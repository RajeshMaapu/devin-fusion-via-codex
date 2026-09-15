"""Redirect-rejection transport tests: loopback HTTP only, dummy creds.

Both provider paths (relay upstream forward, Codex inference) must treat
any redirect as a hard failure — a credential-bearing request must never
be re-issued to a redirect target, even same-origin.
"""
from __future__ import annotations

import json
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

from fusion_relay import relay, translate
from fusion_relay.transport import (RedirectBlocked, RejectRedirects,
                                    open_request)

REDIRECT_CODES = (301, 302, 303, 307, 308)


class _Target(BaseHTTPRequestHandler):
    """Records every request it receives; must never see one."""
    seen: list = []

    def _record(self):
        type(self).seen.append(self.headers.get("Authorization"))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = _record
    do_POST = _record

    def log_message(self, *args):
        pass


class _Source(BaseHTTPRequestHandler):
    """Redirects /r/<code> to the target and /local/<code> to /landed."""
    landed: list = []

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self):
        if self.path == "/landed":
            type(self).landed.append(self.headers.get("Authorization"))
        elif self.path.startswith("/r/"):
            code = int(self.path.rsplit("/", 1)[-1])
            self.send_response(code)
            self.send_header("Location", self.server.target_url)
            self.send_header("Content-Length", "0")
        elif self.path.startswith("/local/"):
            code = int(self.path.rsplit("/", 1)[-1])
            self.send_response(code)
            self.send_header("Location", "/landed")
            self.send_header("Content-Length", "0")
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class RedirectFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _Target.seen = []
        _Source.landed = []
        cls.target = ThreadingHTTPServer(("127.0.0.1", 0), _Target)
        cls.source = ThreadingHTTPServer(("127.0.0.1", 0), _Source)
        cls.source.target_url = "http://127.0.0.1:%d/" % \
            cls.target.server_address[1]
        cls.threads = []
        for server in (cls.target, cls.source):
            thread = threading.Thread(target=server.serve_forever,
                                      daemon=True)
            thread.start()
            cls.threads.append(thread)

    @classmethod
    def tearDownClass(cls):
        for server in (cls.target, cls.source):
            server.shutdown()
            server.server_close()
        for thread in cls.threads:
            thread.join(timeout=2)

    def setUp(self):
        translate.reset()
        self.addCleanup(translate.reset)
        _Target.seen.clear()
        _Source.landed.clear()

    def source_url(self, path):
        return "http://127.0.0.1:%d%s" % (self.source.server_address[1],
                                          path)


class ForwardRedirectTest(RedirectFixture):
    def test_forward_blocks_every_redirect_status(self):
        for code in REDIRECT_CODES:
            with self.subTest(code=code):
                with patch.object(relay, "UPSTREAM",
                                  self.source_url("")):
                    with self.assertRaises(RuntimeError) as ctx:
                        relay._forward(b"{}", {"Authorization":
                                               "Bearer dummy-relay"},
                                       "/r/%d" % code)
                self.assertIn("redirect", str(ctx.exception))
                self.assertEqual(_Target.seen, [])

    def test_forward_blocks_same_origin_redirect(self):
        for code in REDIRECT_CODES:
            with self.subTest(code=code):
                with patch.object(relay, "UPSTREAM",
                                  self.source_url("")):
                    with self.assertRaises(RuntimeError):
                        relay._forward(b"{}", {"Authorization":
                                               "Bearer dummy-relay"},
                                       "/local/%d" % code)
                self.assertEqual(_Source.landed, [])


class CodexRedirectTest(RedirectFixture):
    def test_call_codex_blocks_every_redirect_status(self):
        for code in REDIRECT_CODES:
            with self.subTest(code=code):
                rec = {}
                with patch.object(translate, "CODEX_RESPONSES_URL",
                                  self.source_url("/r/%d" % code)):
                    with self.assertRaises(RuntimeError):
                        translate.call_codex(
                            {"prompt_cache_key": "redirect-test"}, rec,
                            credentials=("dummy", "dummy"))
                self.assertEqual(_Target.seen, [])
                self.assertEqual(rec["codex_http_status"], code)
                self.assertEqual(rec["codex_status"], "failed")
                self.assertEqual(rec["codex_usage"]["unknown_calls"], 1)

    def test_call_codex_blocks_same_origin_redirect(self):
        for code in REDIRECT_CODES:
            with self.subTest(code=code):
                with patch.object(translate, "CODEX_RESPONSES_URL",
                                  self.source_url("/local/%d" % code)):
                    with self.assertRaises(RuntimeError):
                        translate.call_codex(
                            {"prompt_cache_key": "redirect-test"}, {},
                            credentials=("dummy", "dummy"))
                self.assertEqual(_Source.landed, [])


class HandlerMethodTest(unittest.TestCase):
    """Direct handler-level checks: every redirect shape is refused and
    the blocked response object is still closeable."""

    def test_all_redirect_methods_raise(self):
        handler = RejectRedirects()
        req = urllib.request.Request("https://source.example/x")
        for code in REDIRECT_CODES:
            with self.subTest(code=code):
                method = getattr(handler, "http_error_%d" % code)
                fp, headers = Mock(), Mock()
                with self.assertRaises(RedirectBlocked) as ctx:
                    method(req, fp, code, "redirect", headers)
                self.assertEqual(ctx.exception.code, code)
                ctx.exception.close()
                fp.close.assert_called()

    def test_https_to_http_downgrade_rejected(self):
        handler = RejectRedirects()
        req = urllib.request.Request("https://source.example/x")
        with self.assertRaises(RedirectBlocked):
            handler.redirect_request(req, Mock(), 302, "Found", Mock(),
                                     "http://downgrade.example/")

    def test_relative_location_rejected(self):
        handler = RejectRedirects()
        req = urllib.request.Request("https://source.example/x")
        with self.assertRaises(RedirectBlocked):
            handler.redirect_request(req, Mock(), 302, "Found", Mock(),
                                     "/elsewhere")

    def test_no_second_request_issued(self):
        handler = RejectRedirects()
        handler.parent = Mock()
        req = urllib.request.Request("https://source.example/x")
        with self.assertRaises(RedirectBlocked):
            handler.http_error_302(req, Mock(), 302, "Found", Mock())
        handler.parent.open.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
