"""Loopback HTTP tests for approval_ui — real sockets, fake manager use.

No browser, no external network: everything binds 127.0.0.1 ephemeral
ports and uses http.client.
"""

import hashlib
import http.client
import json
import unittest
from unittest.mock import patch

from fusion_relay import approval_ui
from fusion_relay.approvals import ApprovalManager, ApprovalScope


def _scope(**kw) -> ApprovalScope:
    base = dict(
        principal="user-1", session="sess-1", operation_id="op-1",
        app="com.example.App", capability="observe",
        binding_revision=1,
        action_digest=hashlib.sha256(b"a").hexdigest())
    base.update(kw)
    return ApprovalScope(**base)


class UIFixture(unittest.TestCase):
    def setUp(self):
        self.mgr = ApprovalManager()
        self.server = approval_ui.ApprovalServer(self.mgr)
        self.server.start()
        self.addCleanup(self.server.close)
        self.addCleanup(self.mgr.close)
        self.host = f"{self.server.host}:{self.server.port}"
        self.origin = f"http://{self.host}"

    def _conn(self):
        return http.client.HTTPConnection(
            self.server.host, self.server.port, timeout=5)

    def _req(self, method, path, body=None, headers=None):
        c = self._conn()
        try:
            c.request(method, path, body=body, headers=headers or {})
            r = c.getresponse()
            data = r.read()
            return r.status, dict(r.getheaders()), data
        finally:
            c.close()

    def _json(self, obj):
        return json.dumps(obj).encode()

    def _post_json(self, path, obj, cookie=None, csrf=None,
                   origin=None, extra=None):
        h = {"Content-Type": "application/json",
             "Origin": origin if origin is not None else self.origin}
        if cookie:
            h["Cookie"] = cookie
        if csrf:
            h["X-CSRF-Token"] = csrf
        h.update(extra or {})
        return self._req("POST", path, self._json(obj), h)

    def _bootstrap(self, token=None, **kw):
        if token is None:
            token = self.server._control_token
        status, hdrs, data = self._post_json("/bootstrap",
                                             {"token": token}, **kw)
        cookie = hdrs.get("Set-Cookie", "").split(";")[0]
        return status, hdrs, data, cookie

    def _session(self, cookie=None):
        if cookie is None:
            st, _, _, cookie = self._bootstrap()
            self.assertEqual(st, 200)
        st, _, data = self._req("GET", "/session",
                                headers={"Cookie": cookie})
        self.assertEqual(st, 200)
        return cookie, json.loads(data)["csrf"]


class PageTest(UIFixture):
    def test_index_headers_and_no_secret(self):
        status, hdrs, data = self._req("GET", "/")
        self.assertEqual(status, 200)
        csp = hdrs.get("Content-Security-Policy", "")
        self.assertIn("default-src 'none'", csp)
        self.assertIn("frame-ancestors 'none'", csp)
        self.assertIn("script-src 'nonce-", csp)
        self.assertIn("connect-src 'self'", csp)
        self.assertEqual(hdrs.get("Cache-Control"), "no-store")
        self.assertEqual(hdrs.get("X-Frame-Options"), "DENY")
        self.assertEqual(hdrs.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(hdrs.get("Referrer-Policy"), "no-referrer")
        # the control token is never embedded in served markup
        self.assertNotIn(self.server._control_token.encode(), data)
        # markup builds DOM via textContent/createElement only
        self.assertNotIn(b"innerHTML", data)

    def test_xss_task_text_not_in_page(self):
        payload = '<script>alert(1)</script><img src=x onerror=alert(2)>'
        self.mgr.request(_scope(), task=payload,
                         action_summary="s")
        _, _, page = self._req("GET", "/")
        self.assertNotIn(payload.encode(), page)
        # the JSON API escapes it — delivered as data, never markup
        cookie, _ = self._session()
        st, hdrs, data = self._req("GET", "/api/pending",
                                   headers={"Cookie": cookie})
        self.assertEqual(st, 200)
        self.assertIn("json", hdrs.get("Content-Type", ""))
        # delivered as JSON data only; the page renders via textContent
        self.assertIn(payload.encode(), data)

    def test_get_routes_do_not_mutate(self):
        t = self.mgr.request(_scope(), "t", "s")
        self._req("GET", "/api/decide")
        self._req("GET", "/api/revoke")
        self._req("GET", "/bootstrap")
        self.assertEqual(len(self.mgr.pending()), 1)
        self.assertEqual(t.scope.operation_id, "op-1")


class AuthGuardTest(UIFixture):
    def test_unauthenticated_endpoints_deny(self):
        for path in ("/api/pending", "/session"):
            st, _, _ = self._req("GET", path)
            self.assertEqual(st, 403, path)
        st, _, _ = self._post_json("/api/decide", {
            "request_id": "x", "decision": "deny", "revision": 1})
        self.assertEqual(st, 403)

    def test_stolen_request_id_insufficient(self):
        t = self.mgr.request(_scope(), "t", "s")
        spy = patch.object(self.mgr, "decide",
                           wraps=self.mgr.decide)
        with spy as m:
            st, _, _ = self._post_json("/api/decide", {
                "request_id": t.request_id,
                "decision": "allow_once", "revision": 1})
        self.assertEqual(st, 403)
        self.assertEqual(m.call_count, 0)
        self.assertEqual(len(self.mgr.pending()), 1)

    def test_wrong_host_denied(self):
        st, _, _ = self._req("GET", "/api/pending",
                             headers={"Host": "evil.example"})
        self.assertEqual(st, 403)
        st, _, _ = self._req("GET", "/",
                             headers={"Host": "evil.example"})
        self.assertEqual(st, 403)

    def test_wrong_origin_denied(self):
        st, _, _ = self._post_json(
            "/bootstrap", {"token": self.server._control_token},
            origin="http://evil.example")
        self.assertEqual(st, 403)

    def test_empty_cookie_and_absent_csrf_never_authorize(self):
        t = self.mgr.request(_scope(), "t", "s")
        with patch.object(self.mgr, "decide",
                          wraps=self.mgr.decide) as m:
            for h in (
                    # empty cookie header, no csrf, correct Host+Origin
                    {"Content-Type": "application/json",
                     "Origin": self.origin, "Cookie": ""},
                    # no cookie at all, forged-looking csrf
                    {"Content-Type": "application/json",
                     "Origin": self.origin,
                     "X-CSRF-Token": ""},
                    {"Content-Type": "application/json",
                     "Origin": self.origin,
                     "X-CSRF-Token": "A" * 43}):
                st, _, _ = self._req("POST", "/api/decide", self._json(
                    {"request_id": t.request_id,
                     "decision": "allow_once", "revision": 1}), h)
                self.assertEqual(st, 403)
        self.assertEqual(m.call_count, 0)
        self.assertEqual(len(self.mgr.pending()), 1)

    def test_duplicate_headers_rejected(self):
        c = self._conn()
        try:
            c.putrequest("POST", "/bootstrap")
            c.putheader("Host", self.host)
            c.putheader("Host", self.host)  # duplicate
            c.putheader("Origin", self.origin)
            c.putheader("Content-Type", "application/json")
            body = self._json({"token": self.server._control_token})
            c.putheader("Content-Length", str(len(body)))
            c.endheaders(body)
            r = c.getresponse()
            r.read()
            self.assertEqual(r.status, 403)
        finally:
            c.close()
        # duplicate Origin likewise
        c = self._conn()
        try:
            c.putrequest("POST", "/bootstrap")
            c.putheader("Host", self.host)
            c.putheader("Origin", self.origin)
            c.putheader("Origin", self.origin)
            c.putheader("Content-Type", "application/json")
            body = self._json({"token": self.server._control_token})
            c.putheader("Content-Length", str(len(body)))
            c.endheaders(body)
            r = c.getresponse()
            r.read()
            self.assertEqual(r.status, 403)
        finally:
            c.close()

    def test_host_constructor_loopback_only(self):
        with self.assertRaises(ValueError):
            approval_ui.ApprovalServer(self.mgr, host="0.0.0.0")
        with self.assertRaises(ValueError):
            approval_ui.ApprovalServer(self.mgr, host="10.0.0.5")

    def test_transfer_encoding_denied(self):
        c = self._conn()
        try:
            c.putrequest("POST", "/bootstrap")
            c.putheader("Host", self.host)
            c.putheader("Origin", self.origin)
            c.putheader("Content-Type", "application/json")
            c.putheader("Transfer-Encoding", "chunked")
            c.putheader("Content-Length", "16")
            c.endheaders(b'{"token":"x"}')
            r = c.getresponse()
            r.read()
            self.assertIn(r.status, (400, 403))
        finally:
            c.close()


class BootstrapTest(UIFixture):
    def test_token_single_use(self):
        st, hdrs, _, cookie = self._bootstrap()
        self.assertEqual(st, 200)
        self.assertIn("HttpOnly", hdrs.get("Set-Cookie", ""))
        self.assertIn("SameSite=Strict", hdrs.get("Set-Cookie", ""))
        self.assertTrue(
            cookie.startswith(self.server._cookie_name + "="))
        # consumed: replay fails
        st2, _, _, _ = self._bootstrap()
        self.assertEqual(st2, 403)

    def test_wrong_token_denied(self):
        st, _, _, _ = self._bootstrap(token="wrong")
        self.assertEqual(st, 403)
        # real token still usable (not burned by failures)
        st, _, _, _ = self._bootstrap()
        self.assertEqual(st, 200)

    def test_concurrent_bootstrap_single_use(self):
        import threading
        token = self.server._control_token
        results = []
        barrier = threading.Barrier(2)

        def go():
            barrier.wait(5)
            st, hdrs, _, cookie = self._bootstrap(token=token)
            results.append((st, cookie))
        threads = [threading.Thread(target=go) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        codes = sorted(st for st, _ in results)
        self.assertEqual(codes, [200, 403])
        good = [c for st, c in results if st == 200][0]
        # the accepted session remains valid
        st, _, data = self._req("GET", "/session",
                                headers={"Cookie": good})
        self.assertEqual(st, 200)
        self.assertIn("csrf", json.loads(data))

    def test_restart_rotates_secrets(self):
        cookie, csrf = self._session()
        old_token = self.server._control_token
        self.server.close()
        self.server.start()
        # new port => new origin; cookie name also changed
        self.origin = f"http://{self.server.host}:{self.server.port}"
        self.host = f"{self.server.host}:{self.server.port}"
        self.assertNotEqual(self.server._control_token, old_token)
        st, _, _ = self._req("GET", "/session",
                             headers={"Cookie": cookie})
        self.assertEqual(st, 403)

    def test_bootstrap_body_guards(self):
        st, _, _ = self._post_json("/bootstrap",
                                   {"token": "x", "extra": 1})
        self.assertEqual(st, 400)
        c = self._conn()
        try:
            c.request("POST", "/bootstrap",
                      body=b"x" * 2000,
                      headers={"Content-Type": "application/json",
                               "Origin": self.origin})
            r = c.getresponse()
            r.read()
            self.assertEqual(r.status, 413)
        finally:
            c.close()
        st, _, _ = self._req("POST", "/bootstrap",
                             body=b'{"token":"x"}',
                             headers={"Origin": self.origin,
                                      "Content-Type": "text/plain"})
        self.assertEqual(st, 400)


class DecisionFlowTest(UIFixture):
    def _ticket(self, **kw):
        return self.mgr.request(_scope(**kw), "Do it", "Read window")

    def test_authenticated_decide_once(self):
        t = self._ticket()
        cookie, csrf = self._session()
        h = {"Content-Type": "application/json", "Origin": self.origin,
             "Cookie": cookie, "X-CSRF-Token": csrf}
        st, _, data = self._req("POST", "/api/decide", self._json(
            {"request_id": t.request_id, "decision": "allow_once",
             "revision": 1}), h)
        self.assertEqual(st, 200)
        self.assertEqual(json.loads(data)["ok"], True)
        self.mgr.authorize(t, consume=True)
        # decided tickets reject further decisions
        st, _, _ = self._req("POST", "/api/decide", self._json(
            {"request_id": t.request_id, "decision": "deny",
             "revision": 1}), h)
        self.assertEqual(st, 409)

    def test_decide_requires_csrf(self):
        t = self._ticket()
        cookie, _ = self._session()
        for csrf in (None, "forged"):
            h = {"Content-Type": "application/json",
                 "Origin": self.origin, "Cookie": cookie}
            if csrf:
                h["X-CSRF-Token"] = csrf
            st, _, _ = self._req("POST", "/api/decide", self._json(
                {"request_id": t.request_id, "decision": "deny",
                 "revision": 1}), h)
            self.assertEqual(st, 403)
        self.assertEqual(len(self.mgr.pending()), 1)

    def test_decide_unknown_fields_rejected(self):
        t = self._ticket()
        cookie, csrf = self._session()
        h = {"Content-Type": "application/json", "Origin": self.origin,
             "Cookie": cookie, "X-CSRF-Token": csrf}
        st, _, _ = self._req("POST", "/api/decide", self._json(
            {"request_id": t.request_id, "decision": "deny",
             "revision": 1, "admin": True}), h)
        self.assertEqual(st, 400)
        self.assertEqual(len(self.mgr.pending()), 1)

    def test_pending_lists_ticket_for_authed(self):
        t = self._ticket()
        cookie, _ = self._session()
        st, _, data = self._req("GET", "/api/pending",
                                headers={"Cookie": cookie})
        self.assertEqual(st, 200)
        items = json.loads(data)
        self.assertEqual([i["request_id"] for i in items],
                         [t.request_id])
        self.assertEqual(items[0]["capability"], "observe")

    def test_revoke_invalidates(self):
        t = self._ticket()
        cookie, csrf = self._session()
        h = {"Content-Type": "application/json", "Origin": self.origin,
             "Cookie": cookie, "X-CSRF-Token": csrf}
        st, _, _ = self._req("POST", "/api/revoke", self._json(
            {"session": "sess-1", "app": "com.example.App"}), h)
        self.assertEqual(st, 200)
        self.assertEqual(self.mgr.pending(), [])
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t)

    def test_unknown_path_404(self):
        st, _, _ = self._req("GET", "/nope")
        self.assertEqual(st, 404)
        cookie, csrf = self._session()
        st, _, _ = self._post_json("/api/nope", {"x": 1},
                                   cookie=cookie, csrf=csrf)
        self.assertEqual(st, 404)

    def test_decide_invalid_value_400(self):
        t = self._ticket()
        cookie, csrf = self._session()
        h = {"Content-Type": "application/json", "Origin": self.origin,
             "Cookie": cookie, "X-CSRF-Token": csrf}
        st, _, _ = self._req("POST", "/api/decide", self._json(
            {"request_id": t.request_id, "decision": "allow_always",
             "revision": 1}), h)
        self.assertEqual(st, 400)
        self.assertEqual(len(self.mgr.pending()), 1)

    def test_bad_content_lengths_rejected(self):
        cookie, csrf = self._session()
        for cl in ("1x", "+5", "9" * 40, "-1", "1 2", "0x10"):
            c = self._conn()
            try:
                c.putrequest("POST", "/api/decide", skip_host=True)
                c.putheader("Host", self.host)
                c.putheader("Origin", self.origin)
                c.putheader("Content-Type", "application/json")
                c.putheader("Cookie", cookie)
                c.putheader("X-CSRF-Token", csrf)
                c.putheader("Content-Length", cl)
                c.endheaders(b"{}")
                r = c.getresponse()
                r.read()
                self.assertIn(r.status, (400, 413), msg=cl)
            finally:
                c.close()
        # raw socket: non-ASCII digits that http.client can't emit
        import socket as _s
        raw = _s.create_connection(
            (self.server.host, self.server.port), timeout=5)
        try:
            raw.sendall((
                "POST /bootstrap HTTP/1.1\r\n"
                f"Host: {self.host}\r\n"
                f"Origin: {self.origin}\r\n"
                "Content-Type: application/json\r\n"
                "Content-Length: １２\r\n"
                "\r\n{}").encode("utf-8"))
            data = raw.recv(65536)
            self.assertIn(b" 400 ", data.split(b"\r\n")[0])
        finally:
            raw.close()

    def test_grants_listing_and_revoke(self):
        t = self._ticket()
        self.mgr.decide(t.request_id, "allow_task", 1)
        cookie, csrf = self._session()
        st, _, data = self._req("GET", "/api/grants",
                                headers={"Cookie": cookie})
        self.assertEqual(st, 200)
        grants = json.loads(data)
        self.assertEqual(len(grants), 1)
        self.assertEqual(grants[0]["app"], "com.example.App")
        self.assertGreater(grants[0]["remaining_s"], 0)
        h = {"Content-Type": "application/json", "Origin": self.origin,
             "Cookie": cookie, "X-CSRF-Token": csrf}
        st, _, _ = self._req("POST", "/api/revoke", self._json(
            {"session": "sess-1", "app": "com.example.App"}), h)
        self.assertEqual(st, 200)
        self.assertEqual(self.mgr.grants(), [])

    def test_close_terminates_idle_keepalive(self):
        import threading
        c = self._conn()
        c.request("GET", "/", headers={})
        r = c.getresponse()
        r.read()  # leaves the keep-alive connection idle/open
        self.server.close()
        # handler socket closed: further reads hit EOF or reset
        try:
            c.request("GET", "/api/pending", headers={})
            r2 = c.getresponse()
            r2.read()
            alive_body = r2.status
        except (http.client.HTTPException, OSError):
            alive_body = None
        self.assertNotEqual(alive_body, 200)
        c.close()
        self.assertEqual(
            [t for t in threading.enumerate()
             if t.name == "fusion-approval-ui"], [])

    def test_no_orphan_threads(self):
        self.server.close()
        import threading
        alive = [t.name for t in threading.enumerate()
                 if t.name == "fusion-approval-ui"]
        self.assertEqual(alive, [])


if __name__ == "__main__":
    unittest.main()
