"""
Starlette middleware that emits one structured log entry per HTTP
request/response in a format compatible with Google Cloud Logging.

Trace context is extracted from the incoming request headers
(``X-Cloud-Trace-Context`` for GCP-native, ``traceparent`` for W3C) and
stored in module-level ContextVars so that *every* logger used anywhere
inside the request handler automatically includes the correct trace and
span IDs — no manual propagation needed.

The request log entry includes an ``httpRequest`` payload that Cloud Logging
can parse and surface in the Logs Explorer HTTP-request view.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match
from starlette.types import ASGIApp

from . import _metadata, formatter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trace-context helpers
# ---------------------------------------------------------------------------


def _parse_xctc(header: str) -> tuple[str, str, bool]:
    """Parse ``X-Cloud-Trace-Context: TRACE_ID[/SPAN_ID][;o=FLAG]``.

    SPAN_ID in this header is a uint64 decimal; GCP logging expects a
    16-character lowercase hex string, so we convert it.
    """
    trace_id = ""
    span_id = ""
    sampled = False

    slash = header.find("/")
    if slash == -1:
        trace_id = header.strip()
        return trace_id, span_id, sampled

    trace_id = header[:slash]
    remainder = header[slash + 1 :]

    semicolon = remainder.find(";")
    if semicolon == -1:
        span_raw = remainder
    else:
        span_raw = remainder[:semicolon]
        opts = remainder[semicolon + 1 :]
        for opt in opts.split(";"):
            if opt.startswith("o="):
                sampled = opt[2:] == "1"

    # Convert decimal span to 16-char hex expected by Cloud Logging
    if span_raw:
        try:
            span_id = format(int(span_raw), "016x")
        except ValueError:
            span_id = span_raw  # pass through if already hex / unexpected format

    return trace_id, span_id, sampled


def _parse_traceparent(header: str) -> tuple[str, str, bool]:
    """Parse W3C ``traceparent: VER-TRACE_ID-SPAN_ID-FLAGS``."""
    parts = header.split("-")
    if len(parts) < 4:
        return "", "", False
    try:
        sampled = (int(parts[3], 16) & 0x01) == 1
    except ValueError:
        sampled = False
    return parts[1], parts[2], sampled


def _extract_iap_user_email(request: Request) -> str:
    """Return the authenticated user email from IAP headers, or ``""`` if absent.

    Two header sources are supported:

    * ``x-goog-authenticated-user-email`` — set by IAP when Cloud Run is behind
      a load balancer.  Values are prefixed with ``accounts.google.com:``, which
      is stripped before returning.
    * ``x-serverless-authorization`` — set by IAP when accessed directly through
      Cloud Run (without a load balancer).  The raw value is returned as-is since
      it is an opaque JWT bearer token.

    The first header found in the priority order above is used.
    """
    email_header = request.headers.get("x-goog-authenticated-user-email")
    if email_header:
        # Strip the identity-provider prefix, e.g. "accounts.google.com:user@example.com"
        _, _, email = email_header.partition(":")
        return email or email_header

    serverless_auth = request.headers.get("x-serverless-authorization")
    if serverless_auth:
        return serverless_auth

    return ""


def _extract_trace_context(request: Request) -> tuple[str, str, bool]:
    """Return ``(trace_id, span_id, sampled)`` from the best available header.

    Priority:
    1. ``traceparent`` (W3C / OpenTelemetry standard) — used when an upstream
       service propagates its own trace context.
    2. ``X-Cloud-Trace-Context`` — injected by GCP infrastructure when no
       upstream trace is present.
    """
    tp = request.headers.get("traceparent")
    if tp:
        return _parse_traceparent(tp)

    xctc = request.headers.get("x-cloud-trace-context")
    if xctc:
        return _parse_xctc(xctc)

    return "", "", False


# ---------------------------------------------------------------------------
# Route-template helpers
# ---------------------------------------------------------------------------


def _find_route_template(
    router: Any, scope: dict[str, Any]
) -> str | None:
    """Recursively search *router*'s route table for the path template that
    matches *scope*, e.g. ``'/api/user/{user_id}/task/{task_id}'``.

    Returns ``None`` when no route matches (e.g. a 404 request).

    ``route.matches()`` is a pure query — it does not mutate *scope* — so
    calling it before the actual routing happens in ``call_next`` is safe.
    When a :class:`starlette.routing.Mount` matches, its ``child_scope``
    carries an updated ``root_path`` that trims the mount prefix; we merge
    this into a copy of *scope* before recursing so that inner routes see
    only their own path segment.
    """
    for route in getattr(router, "routes", []):
        match, child_scope = route.matches(scope)
        if match == Match.NONE:
            continue
        if hasattr(route, "endpoint"):
            # Leaf Route / WebSocketRoute — return its path template.
            return route.path
        # Mount — recurse with the child scope so that get_route_path trims
        # the mount prefix before inner routes try to match.
        sub = _find_route_template(
            getattr(route, "app", None), {**scope, **child_scope}
        )
        if sub is not None:
            return route.path + sub
    return None


def _extract_route_path(request: Request) -> str:
    """Return the matched route path template including any ``root_path``.

    For example, given a request to ``/api/user/123/task/abc`` matched by the
    route ``/api/user/{user_id}/task/{task_id}``, returns
    ``"/api/user/{user_id}/task/{task_id}"`` (or
    ``"/prefix/api/user/{user_id}/task/{task_id}"`` when ``root_path`` is set).
    Returns ``""`` when no route matches.
    """
    app = request.scope.get("app")
    template = _find_route_template(app, request.scope)
    root_path = request.scope.get("root_path", "")
    return root_path + (template or "")


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class GCPRequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log one structured entry per request/response to Google Cloud Logging.

    Parameters
    ----------
    app:
        The ASGI application to wrap.
    project_id:
        GCP project ID, used to build the full trace resource name stored in
        ``request_trace`` so that ``GCPFormatter`` can emit it verbatim.
    logger_name:
        Name of the Python logger to use. Defaults to this module's name.
    default_level:
        Log level used for 1xx/2xx/3xx responses. 4xx responses use WARNING;
        5xx responses use ERROR.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        project_id: str = "",
        logger_name: str = __name__,
        default_level: int = logging.INFO,
    ) -> None:
        super().__init__(app)
        # Resolve at startup (one blocking metadata call at most).
        self._project_id = project_id or _metadata.get_project_id()
        self._logger = logging.getLogger(logger_name)
        self._default_level = default_level

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        trace_id, span_id, sampled = _extract_trace_context(request)
        user_email = _extract_iap_user_email(request)
        route_path = _extract_route_path(request)

        # Store the bare trace ID; GCPFormatter is responsible for building
        # the full "projects/<id>/traces/<trace_id>" resource name when it has
        # a project_id configured.
        trace_tok = formatter.request_trace.set(trace_id)
        span_tok = formatter.request_span.set(span_id)
        sampled_tok = formatter.request_trace_sampled.set(sampled)
        email_tok = formatter.request_user_email.set(user_email)
        route_tok = formatter.request_route.set(route_path)

        status_code = 500
        start = time.perf_counter()
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            self._logger.exception(
                "Unhandled exception processing %s %s",
                request.method,
                request.url.path,
            )
            raise
        finally:
            latency_s = time.perf_counter() - start
            self._emit(request, status_code, latency_s)
            formatter.request_trace.reset(trace_tok)
            formatter.request_span.reset(span_tok)
            formatter.request_trace_sampled.reset(sampled_tok)
            formatter.request_user_email.reset(email_tok)
            formatter.request_route.reset(route_tok)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _level_for_status(self, status_code: int) -> int:
        if status_code >= 500:
            return logging.ERROR
        if status_code >= 400:
            return logging.WARNING
        return self._default_level

    def _emit(
        self,
        request: Request,
        status_code: int,
        latency_s: float,
    ) -> None:
        level = self._level_for_status(status_code)

        # Latency as a proto Duration string (e.g. "0.123456s")
        latency_str = f"{latency_s:.6f}s"

        remote_ip = ""
        if request.client:
            remote_ip = request.client.host

        # Prefer the de-facto forwarded-for header when behind a load balancer
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            remote_ip = forwarded_for.split(",")[0].strip()

        http_version = request.scope.get("http_version", "1.1")

        self._logger.log(
            level,
            "%s %s %d (%.3fs)",
            request.method,
            request.url.path,
            status_code,
            latency_s,
            extra={
                # Cloud Logging parses this key specially in structured logs
                "httpRequest": {
                    "requestMethod": request.method,
                    "requestUrl": str(request.url),
                    "status": status_code,
                    "userAgent": request.headers.get("user-agent", ""),
                    "remoteIp": remote_ip,
                    "protocol": f"HTTP/{http_version}",
                    "latency": latency_str,
                    "referer": request.headers.get("referer", ""),
                },
            },
        )
