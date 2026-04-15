"""
Unit tests for starlette_gcp_logging.
"""

from __future__ import annotations

import io
import json
import logging
import unittest
import unittest.mock

from starlette_gcp_logging import _metadata, formatter, middleware


class TestGCPFormatter(unittest.TestCase):
    def _make_handler(
        self, project_id: str | None = None
    ) -> tuple[logging.Logger, io.StringIO]:
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(formatter.GCPFormatter(project_id=project_id))
        log = logging.getLogger(f"test_{id(buf)}")
        log.handlers = [handler]
        log.propagate = False
        log.setLevel(logging.DEBUG)
        return log, buf

    def test_basic_fields(self):
        log, buf = self._make_handler()
        log.info("hello world")
        payload = json.loads(buf.getvalue())

        self.assertEqual(payload["severity"], "INFO")
        self.assertEqual(payload["message"], "hello world")
        self.assertIn("time", payload)
        self.assertEqual(
            payload["logging.googleapis.com/sourceLocation"]["function"],
            "test_basic_fields",
        )

    def test_severity_mapping(self):
        log, buf = self._make_handler()
        log.debug("d")
        log.warning("w")
        log.error("e")
        log.critical("c")
        lines = [json.loads(line) for line in buf.getvalue().strip().splitlines()]
        self.assertEqual(
            [line["severity"] for line in lines],
            ["DEBUG", "WARNING", "ERROR", "CRITICAL"],
        )

    def test_extra_fields(self):
        log, buf = self._make_handler()
        log.info("msg", extra={"user_id": "u123", "region": "us-east1"})
        payload = json.loads(buf.getvalue())
        self.assertEqual(payload["user_id"], "u123")
        self.assertEqual(payload["region"], "us-east1")

    def test_exception_included(self):
        log, buf = self._make_handler()
        try:
            raise ValueError("boom")
        except ValueError:
            log.exception("caught it")
        payload = json.loads(buf.getvalue())
        self.assertIn("ValueError", payload["exception"])
        self.assertTrue(payload["@type"].endswith("ReportedErrorEvent"))

    def test_trace_context(self):
        log, buf = self._make_handler(project_id="my-project")

        tok_t = formatter.request_trace.set("abc123")
        tok_s = formatter.request_span.set("00f067aa0ba902b7")
        tok_m = formatter.request_trace_sampled.set(True)
        try:
            log.info("traced message")
        finally:
            formatter.request_trace.reset(tok_t)
            formatter.request_span.reset(tok_s)
            formatter.request_trace_sampled.reset(tok_m)

        payload = json.loads(buf.getvalue())
        self.assertEqual(
            payload["logging.googleapis.com/trace"],
            "projects/my-project/traces/abc123",
        )
        self.assertEqual(payload["logging.googleapis.com/spanId"], "00f067aa0ba902b7")
        self.assertTrue(payload["logging.googleapis.com/traceSampled"])

    def test_project_id_auto_detected(self):
        _metadata.get_project_id.cache_clear()
        try:
            with unittest.mock.patch(
                "starlette_gcp_logging._metadata.get_project_id",
                return_value="auto-project",
            ):
                log, buf = self._make_handler()
                tok = formatter.request_trace.set("deadbeef")
                try:
                    log.info("auto project test")
                finally:
                    formatter.request_trace.reset(tok)

                payload = json.loads(buf.getvalue())
                self.assertEqual(
                    payload["logging.googleapis.com/trace"],
                    "projects/auto-project/traces/deadbeef",
                )
        finally:
            _metadata.get_project_id.cache_clear()


class TestTraceHeaderParsing(unittest.TestCase):
    def test_parse_xctc_full(self):
        trace, span, sampled = middleware._parse_xctc(
            "105445aa7843bc8bf206b120001000/1;o=1"
        )
        self.assertEqual(trace, "105445aa7843bc8bf206b120001000")
        self.assertEqual(span, format(1, "016x"))
        self.assertTrue(sampled)

    def test_parse_xctc_no_span(self):
        trace, span, sampled = middleware._parse_xctc("105445aa7843bc8bf206b120001000")
        self.assertEqual(trace, "105445aa7843bc8bf206b120001000")
        self.assertEqual(span, "")
        self.assertFalse(sampled)

    def test_parse_traceparent_sampled(self):
        trace, span, sampled = middleware._parse_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        self.assertEqual(trace, "4bf92f3577b34da6a3ce929d0e0e4736")
        self.assertEqual(span, "00f067aa0ba902b7")
        self.assertTrue(sampled)

    def test_parse_traceparent_unsampled(self):
        _, _, sampled = middleware._parse_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        )
        self.assertFalse(sampled)


class TestMetadata(unittest.TestCase):
    def setUp(self):
        _metadata.get_project_id.cache_clear()

    def tearDown(self):
        _metadata.get_project_id.cache_clear()

    def test_fallback_outside_gcp(self):
        result = _metadata.get_project_id()
        self.assertIsInstance(result, str)

    def test_result_is_cached(self):
        first = _metadata.get_project_id()
        second = _metadata.get_project_id()
        self.assertIs(first, second)


class TestGCPRequestLoggingMiddleware(unittest.TestCase):
    def setUp(self):
        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.responses import JSONResponse, Response
        from starlette.routing import Route

        async def homepage(request: Request):
            logging.getLogger("app").info("inside handler")
            return JSONResponse({"ok": True})

        async def bad(request: Request):
            return Response(status_code=503)

        self.app = Starlette(routes=[Route("/", homepage), Route("/bad", bad)])
        self.app.add_middleware(
            middleware.GCPRequestLoggingMiddleware,
            project_id="my-project",
        )

        self.buf = io.StringIO()
        self.handler = logging.StreamHandler(self.buf)
        self.handler.setFormatter(formatter.GCPFormatter(project_id="my-project"))
        self.root = logging.getLogger()
        self.root.addHandler(self.handler)
        self.root.setLevel(logging.DEBUG)

    def tearDown(self):
        self.root.removeHandler(self.handler)

    def _lines(self) -> list[dict]:
        return [
            json.loads(line)
            for line in self.buf.getvalue().strip().splitlines()
            if line.strip()
        ]

    def test_request_log_entry(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        resp = client.get(
            "/",
            headers={
                "X-Cloud-Trace-Context": "105445aa7843bc8bf206b120001000/1;o=1",
                "User-Agent": "test-agent/1.0",
            },
        )
        self.assertEqual(resp.status_code, 200)

        lines = self._lines()
        self.assertGreaterEqual(len(lines), 2)

        mw_entry = next(line for line in lines if "httpRequest" in line)
        self.assertEqual(mw_entry["httpRequest"]["requestMethod"], "GET")
        self.assertEqual(mw_entry["httpRequest"]["status"], 200)
        self.assertIn("latency", mw_entry["httpRequest"])
        self.assertEqual(mw_entry["httpRequest"]["userAgent"], "test-agent/1.0")

    def test_trace_propagated_to_all_loggers(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        client.get(
            "/",
            headers={"X-Cloud-Trace-Context": "105445aa7843bc8bf206b120001000/1;o=1"},
        )

        traced = [
            line for line in self._lines() if "logging.googleapis.com/trace" in line
        ]
        self.assertGreaterEqual(len(traced), 2)
        for entry in traced:
            self.assertIn(
                "projects/my-project/traces/",
                entry["logging.googleapis.com/trace"],
            )

    def test_5xx_logged_as_error(self):
        from starlette.testclient import TestClient

        self.root.removeHandler(self.handler)
        buf = io.StringIO()
        h = logging.StreamHandler(buf)
        h.setFormatter(formatter.GCPFormatter())
        mw_logger = logging.getLogger("starlette_gcp_logging.middleware")
        mw_logger.addHandler(h)
        mw_logger.setLevel(logging.DEBUG)
        mw_logger.propagate = False

        try:
            client = TestClient(self.app, raise_server_exceptions=False)
            client.get("/bad")
            entry = json.loads(buf.getvalue().strip())
            self.assertEqual(entry["severity"], "ERROR")
            self.assertEqual(entry["httpRequest"]["status"], 503)
        finally:
            mw_logger.removeHandler(h)
            mw_logger.propagate = True


class TestIAPHeaderParsing(unittest.TestCase):
    def _make_request(self, headers: dict):
        from starlette.requests import Request

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
        return Request(scope)

    def test_xgoog_authenticated_user_email(self):
        req = self._make_request(
            {"x-goog-authenticated-user-email": "accounts.google.com:user@example.com"}
        )
        self.assertEqual(middleware._extract_iap_user_email(req), "user@example.com")

    def test_xgoog_authenticated_user_email_no_prefix(self):
        req = self._make_request(
            {"x-goog-authenticated-user-email": "user@example.com"}
        )
        self.assertEqual(middleware._extract_iap_user_email(req), "user@example.com")

    def test_serverless_authorization_fallback(self):
        req = self._make_request({"x-serverless-authorization": "Bearer eyJtoken"})
        self.assertEqual(middleware._extract_iap_user_email(req), "Bearer eyJtoken")

    def test_xgoog_takes_priority_over_serverless(self):
        req = self._make_request(
            {
                "x-goog-authenticated-user-email": "accounts.google.com:user@example.com",
                "x-serverless-authorization": "Bearer eyJtoken",
            }
        )
        self.assertEqual(middleware._extract_iap_user_email(req), "user@example.com")

    def test_no_iap_headers(self):
        req = self._make_request({})
        self.assertEqual(middleware._extract_iap_user_email(req), "")


class TestGCPFormatterUserEmail(unittest.TestCase):
    def _make_handler(self) -> tuple[logging.Logger, io.StringIO]:
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(formatter.GCPFormatter(project_id="test-project"))
        log = logging.getLogger(f"test_email_{id(buf)}")
        log.handlers = [handler]
        log.propagate = False
        log.setLevel(logging.DEBUG)
        return log, buf

    def test_user_email_in_labels(self):
        log, buf = self._make_handler()
        tok = formatter.request_user_email.set("user@example.com")
        try:
            log.info("request with iap user")
        finally:
            formatter.request_user_email.reset(tok)

        payload = json.loads(buf.getvalue())
        self.assertEqual(
            payload["logging.googleapis.com/labels"]["authenticated_user_email"],
            "user@example.com",
        )

    def test_no_labels_when_no_email(self):
        log, buf = self._make_handler()
        log.info("request without iap user")
        payload = json.loads(buf.getvalue())
        self.assertNotIn("logging.googleapis.com/labels", payload)


class TestMiddlewareIAPPropagation(unittest.TestCase):
    def setUp(self):
        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        async def homepage(request: Request):
            logging.getLogger("app").info("inside handler")
            return JSONResponse({"ok": True})

        self.app = Starlette(routes=[Route("/", homepage)])
        self.app.add_middleware(
            middleware.GCPRequestLoggingMiddleware,
            project_id="my-project",
        )

        self.buf = io.StringIO()
        self.handler = logging.StreamHandler(self.buf)
        self.handler.setFormatter(formatter.GCPFormatter(project_id="my-project"))
        self.root = logging.getLogger()
        self.root.addHandler(self.handler)
        self.root.setLevel(logging.DEBUG)

    def tearDown(self):
        self.root.removeHandler(self.handler)

    def _lines(self) -> list[dict]:
        return [
            json.loads(line)
            for line in self.buf.getvalue().strip().splitlines()
            if line.strip()
        ]

    def test_iap_email_propagated_to_all_log_entries(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        client.get(
            "/",
            headers={
                "x-goog-authenticated-user-email": "accounts.google.com:user@example.com"
            },
        )

        # Only examine entries produced inside the request context (app and middleware
        # loggers).  httpx logs its own entries outside the context so they won't
        # carry the IAP label.
        app_entries = [
            entry
            for entry in self._lines()
            if entry.get("logger", "").startswith(("app", "starlette_gcp_logging"))
        ]
        self.assertGreaterEqual(len(app_entries), 2)
        for entry in app_entries:
            self.assertEqual(
                entry.get("logging.googleapis.com/labels", {}).get(
                    "authenticated_user_email"
                ),
                "user@example.com",
            )

    def test_no_iap_headers_no_labels(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        client.get("/")

        for entry in self._lines():
            self.assertNotIn(
                "authenticated_user_email",
                entry.get("logging.googleapis.com/labels", {}),
            )


class TestGCPFormatterRoute(unittest.TestCase):
    def _make_handler(self) -> tuple[logging.Logger, io.StringIO]:
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(formatter.GCPFormatter(project_id="test-project"))
        log = logging.getLogger(f"test_route_{id(buf)}")
        log.handlers = [handler]
        log.propagate = False
        log.setLevel(logging.DEBUG)
        return log, buf

    def test_route_in_labels(self):
        log, buf = self._make_handler()
        tok = formatter.request_route.set("/api/user/{user_id}/task/{task_id}")
        try:
            log.info("request with route")
        finally:
            formatter.request_route.reset(tok)

        payload = json.loads(buf.getvalue())
        self.assertEqual(
            payload["logging.googleapis.com/labels"]["starlette.dev/route"],
            "/api/user/{user_id}/task/{task_id}",
        )

    def test_no_labels_when_no_route(self):
        log, buf = self._make_handler()
        log.info("request without route")
        payload = json.loads(buf.getvalue())
        self.assertNotIn("logging.googleapis.com/labels", payload)

    def test_route_and_email_share_labels_dict(self):
        log, buf = self._make_handler()
        route_tok = formatter.request_route.set("/items/{item_id}")
        email_tok = formatter.request_user_email.set("user@example.com")
        try:
            log.info("combined labels")
        finally:
            formatter.request_route.reset(route_tok)
            formatter.request_user_email.reset(email_tok)

        payload = json.loads(buf.getvalue())
        labels = payload["logging.googleapis.com/labels"]
        self.assertEqual(labels["starlette.dev/route"], "/items/{item_id}")
        self.assertEqual(labels["authenticated_user_email"], "user@example.com")


class TestFindRouteTemplate(unittest.TestCase):
    def _make_scope(self, path: str, root_path: str = "") -> dict:
        return {
            "type": "http",
            "method": "GET",
            "path": path,
            "root_path": root_path,
            "path_params": {},
        }

    def test_simple_route_no_params(self):
        from starlette.routing import Route, Router

        router = Router(routes=[Route("/health", lambda r: None)])
        result = middleware._find_route_template(router, self._make_scope("/health"))
        self.assertEqual(result, "/health")

    def test_simple_route_with_params(self):
        from starlette.routing import Route, Router

        router = Router(routes=[Route("/user/{user_id}", lambda r: None)])
        result = middleware._find_route_template(router, self._make_scope("/user/42"))
        self.assertEqual(result, "/user/{user_id}")

    def test_nested_mount(self):
        from starlette.routing import Mount, Route, Router

        inner = Router(routes=[Route("/task/{task_id}", lambda r: None)])
        router = Router(routes=[Mount("/api/user/{user_id}", app=inner)])
        result = middleware._find_route_template(
            router, self._make_scope("/api/user/123/task/abc")
        )
        self.assertEqual(result, "/api/user/{user_id}/task/{task_id}")

    def test_no_match_returns_none(self):
        from starlette.routing import Route, Router

        router = Router(routes=[Route("/exists", lambda r: None)])
        result = middleware._find_route_template(
            router, self._make_scope("/does-not-exist")
        )
        self.assertIsNone(result)

    def test_extract_route_path_includes_root_path(self):
        from starlette.requests import Request
        from starlette.routing import Route, Router

        router = Router(routes=[Route("/items/{item_id}", lambda r: None)])
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/items/99",
            "root_path": "/v1",
            "query_string": b"",
            "headers": [],
            "path_params": {},
            "app": router,
        }
        request = Request(scope)
        self.assertEqual(
            middleware._extract_route_path(request), "/v1/items/{item_id}"
        )

    def test_extract_route_path_no_match_returns_empty(self):
        from starlette.requests import Request
        from starlette.routing import Route, Router

        router = Router(routes=[Route("/exists", lambda r: None)])
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/missing",
            "root_path": "",
            "query_string": b"",
            "headers": [],
            "path_params": {},
            "app": router,
        }
        request = Request(scope)
        self.assertEqual(middleware._extract_route_path(request), "")


class TestMiddlewareRoutePropagation(unittest.TestCase):
    def setUp(self):
        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        async def user_task(request: Request):
            logging.getLogger("app").info("inside handler")
            return JSONResponse({"ok": True})

        self.app = Starlette(
            routes=[Route("/api/user/{user_id}/task/{task_id}", user_task)]
        )
        self.app.add_middleware(
            middleware.GCPRequestLoggingMiddleware,
            project_id="my-project",
        )

        self.buf = io.StringIO()
        self.handler = logging.StreamHandler(self.buf)
        self.handler.setFormatter(formatter.GCPFormatter(project_id="my-project"))
        self.root = logging.getLogger()
        self.root.addHandler(self.handler)
        self.root.setLevel(logging.DEBUG)

    def tearDown(self):
        self.root.removeHandler(self.handler)

    def _lines(self) -> list[dict]:
        return [
            json.loads(line)
            for line in self.buf.getvalue().strip().splitlines()
            if line.strip()
        ]

    def test_route_label_on_all_log_entries(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        client.get("/api/user/123/task/abc")

        app_entries = [
            entry
            for entry in self._lines()
            if entry.get("logger", "").startswith(("app", "starlette_gcp_logging"))
        ]
        self.assertGreaterEqual(len(app_entries), 2)
        for entry in app_entries:
            self.assertEqual(
                entry.get("logging.googleapis.com/labels", {}).get(
                    "starlette.dev/route"
                ),
                "/api/user/{user_id}/task/{task_id}",
            )

    def test_no_route_label_on_unmatched_path(self):
        from starlette.testclient import TestClient

        client = TestClient(self.app, raise_server_exceptions=False)
        client.get("/does-not-exist")

        mw_entries = [
            entry
            for entry in self._lines()
            if entry.get("logger", "").startswith("starlette_gcp_logging")
        ]
        self.assertGreaterEqual(len(mw_entries), 1)
        for entry in mw_entries:
            self.assertNotIn(
                "starlette.dev/route",
                entry.get("logging.googleapis.com/labels", {}),
            )


if __name__ == "__main__":
    unittest.main()
