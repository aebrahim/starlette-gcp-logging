"""
GCP-structured JSON log formatter for Python's logging module.

Produces JSON output compatible with Google Cloud Logging's structured log
ingestion, including correct severity mapping, source location, and
W3C / X-Cloud-Trace-Context trace correlation.

Trace / span context is propagated via module-level ContextVars so that any
logger used inside a request handler automatically inherits the values set by
GCPRequestLoggingMiddleware without any explicit plumbing.
"""

import datetime
import json
import logging
import traceback
import typing
from contextvars import ContextVar

from . import _metadata

# ---------------------------------------------------------------------------
# Per-request context variables (set by middleware, read by formatter)
# ---------------------------------------------------------------------------

#: Full trace resource name, e.g. "projects/my-project/traces/<TRACE_ID>".
#: Set to the bare TRACE_ID when project_id is unknown.
request_trace: ContextVar[str] = ContextVar("gcp_trace", default="")

#: 16-character lowercase hex span ID, e.g. "00f067aa0ba902b7".
request_span: ContextVar[str] = ContextVar("gcp_span", default="")

#: Whether the trace is sampled.
request_trace_sampled: ContextVar[bool] = ContextVar("gcp_trace_sampled", default=False)

#: Authenticated user email extracted from IAP headers
#: (``x-goog-authenticated-user-email`` or ``x-serverless-authorization``).
request_user_email: ContextVar[str] = ContextVar("gcp_user_email", default="")

#: Starlette route path template, e.g. ``"/api/user/{user_id}/task/{task_id}"``.
#: Includes ``root_path`` when one is present.  Set to ``""`` when the route
#: cannot be resolved (e.g. for 404 responses).
request_route: ContextVar[str] = ContextVar("starlette_route", default="")

# ---------------------------------------------------------------------------
# Python log-level → GCP severity mapping
# ---------------------------------------------------------------------------

_SEVERITY: dict[int, str] = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARNING",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "CRITICAL",
}

# ---------------------------------------------------------------------------
# LogRecord fields that belong to the logging machinery, not the payload
# ---------------------------------------------------------------------------

_INTERNAL_ATTRS: frozenset[str] = frozenset(
    {
        # Standard LogRecord attributes
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",  # Python 3.12+
    }
)


class GCPFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object understood by GCP.

    Parameters
    ----------
    project_id:
        GCP project ID used to build the full trace resource name
        ``projects/<project_id>/traces/<trace_id>``.  When ``None`` (the
        default) it is fetched from the GCP instance metadata server on
        demand and cached for the lifetime of the process via
        ``_metadata.get_project_id()``.
    """

    def __init__(self, project_id: str | None = None) -> None:
        super().__init__()
        self._project_id = project_id

    @property
    def project_id(self) -> str:
        return self._project_id or _metadata.get_project_id()

    # ------------------------------------------------------------------
    # Core formatting
    # ------------------------------------------------------------------

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        payload: dict[str, typing.Any] = {
            "severity": _SEVERITY.get(record.levelno, record.levelname),
            "message": record.message,
            # RFC 3339 timestamp with UTC offset
            "time": (
                datetime.datetime.fromtimestamp(
                    record.created, tz=datetime.timezone.utc
                ).isoformat()
            ),
            "logging.googleapis.com/sourceLocation": {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            },
            # logger name surfaced as a label for easy filtering
            "logger": record.name,
        }

        # -- Trace / span correlation ----------------------------------
        trace = request_trace.get()
        span = request_span.get()
        sampled = request_trace_sampled.get()

        if trace:
            if self.project_id:
                payload["logging.googleapis.com/trace"] = (
                    f"projects/{self.project_id}/traces/{trace}"
                )
            else:
                payload["logging.googleapis.com/trace"] = trace
            payload["logging.googleapis.com/traceSampled"] = sampled

        if span:
            payload["logging.googleapis.com/spanId"] = span

        # -- IAP authenticated user -----------------------------------
        user_email = request_user_email.get()
        if user_email:
            payload.setdefault("logging.googleapis.com/labels", {})
            payload["logging.googleapis.com/labels"]["authenticated_user_email"] = (
                user_email
            )

        # -- Starlette route template ---------------------------------
        route = request_route.get()
        if route:
            payload.setdefault("logging.googleapis.com/labels", {})
            payload["logging.googleapis.com/labels"]["starlette.dev/route"] = route

        # -- Exception / stack info -----------------------------------
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
            # Also surface as GCP's "error" structure for Error Reporting
            payload["@type"] = (
                "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
            )

        if record.stack_info:
            payload["stackInfo"] = self.formatStack(record.stack_info)

        # -- Extra fields passed via extra={...} ----------------------
        for key, value in record.__dict__.items():
            if key not in _INTERNAL_ATTRS and not key.startswith("_"):
                payload[key] = value

        return json.dumps(payload, default=_json_default)


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------


def _json_default(obj: typing.Any) -> typing.Any:
    """Fallback serialiser for types not handled by the standard encoder."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if isinstance(obj, BaseException):
        return "".join(traceback.format_exception(type(obj), obj, obj.__traceback__))
    return str(obj)
