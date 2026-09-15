"""Accounting integration: real handler loopback, dummy providers only."""
from __future__ import annotations

import contextlib
import http.client
import io
import json
import pathlib
import socket
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

from fusion_relay import catalog, relay, translate, wire
from fusion_relay.accounting import (
    AccountingLedger, AccountingUnavailable, reference)



def _ldb1(led, sql, params=()):
    """Locked single-row access — /usr/bin/python3 3.9 sqlite3 crashes
    on concurrent statements on one connection; hold the ledger lock."""
    with led._lock:
        return led._db.execute(sql, params).fetchone()


def _ldba(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchall()


def _ldbw(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params)


class FakeSSE:
    """urllib response stand-in yielding SSE data: lines."""

    def __init__(self, events, status=200):
        self._lines = [b"data: " + json.dumps(e).encode() + b"\n"
                       for e in events]
        self._pending = b""
        self.status = status
        self.headers = {}
        self.closed = False

    def readline(self, limit=-1):
        if not self._pending:
            if not self._lines:
                return b""
            self._pending = self._lines.pop(0)
        line = self._pending
        if limit < 0 or len(line) <= limit:
            self._pending = b""
            return line
        out, self._pending = line[:limit], line[limit:]
        return out

    def __iter__(self):
        return iter(self._lines)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        self.closed = True


class Upstream:
    """Scripted one-shot loopback upstream for the native path."""

    def __init__(self, script):
        self._script = script
        self._stop = threading.Event()
        self.conn = None
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        conn = None
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = self.sock.accept()
                    break
                except socket.timeout:
                    continue
                except OSError:
                    return
            if conn is None:
                return
            self.conn = conn
            conn.settimeout(10)
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(4096)
            head, _, rest = data.partition(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    length = int(line.split(b":", 1)[1])
            while len(rest) < length:
                rest += conn.recv(4096)
            self._script(conn, rest)
        finally:
            if conn is not None:
                conn.close()

    def close(self):
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        conn = self.conn
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        self.thread.join(10)


class QuietServer(relay._BoundedServer):
    def handle_error(self, request, client_address):
        err = __import__("sys").exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError,
                            http.client.IncompleteRead)):
            return
        raise err


def completed(usage):
    return {"type": "response.completed", "response": {
        "id": "r-1", "status": "completed", "output": [],
        "usage": usage}}


class AccountingIntegrationTest(unittest.TestCase):
    TOKEN = "acct-test-token-0000"
    RPC = "/xexerra.chat.v1.ChatService/GetChatMessage"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = pathlib.Path(self._tmp.name).resolve()
        for attr, val in (
                ("DATA_DIR", self.dir),
                ("TOKEN_PATH", self.dir / "relay-token"),
                ("REQUESTS_LOG", self.dir / "requests.jsonl"),
                ("STATS_PATH", self.dir / "stats.json"),
                ("_RELAY_TOKEN", self.TOKEN),
                ("STREAM_MODE", "buffer")):
            p = patch.object(relay, attr, val)
            self.addCleanup(p.stop)
            p.start()
        self.ledger = AccountingLedger(self.dir / "accounting.sqlite3")
        self.addCleanup(self.ledger.close)
        for attr, val in (("_accounting", self.ledger),
                          ("_accounting_required", True),
                          ("_accounting_export_degraded", False)):
            p = patch.object(relay, attr, val)
            self.addCleanup(p.stop)
            p.start()
        p = patch.object(relay.auth, "get_token",
                         return_value=("tok", "acct-1"))
        self.addCleanup(p.stop)
        p.start()
        catalog.reset()
        self.addCleanup(catalog.reset)
        catalog.attach_store(self.dir / "routes.json")
        self.server = QuietServer(("127.0.0.1", 0), relay.Handler)
        self.server.identity = types.SimpleNamespace(secret=b"k" * 32)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05})
        self._thread.start()
        self.addCleanup(
            lambda: self.assertFalse(self._thread.is_alive()))
        self.addCleanup(self._thread.join, 10)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    # -- helpers ----------------------------------------------------------
    def _conn(self):
        return http.client.HTTPConnection("127.0.0.1", self.port,
                                          timeout=15)

    def _chat(self, model, session="s1"):
        inner = wire.field(2, 1) + wire.field(3, "hi")
        return wire.frame(
            wire.field(3, inner) + wire.field(16, session)
            + wire.field(21, model)) + wire.end_stream()

    def _post(self, body, ctype="application/connect+proto",
              authorization=None):
        conn = self._conn()
        headers = {"Content-Type": ctype}
        if authorization is not None:
            headers["Authorization"] = authorization
        conn.request("POST", f"/t/{self.TOKEN}{self.RPC}",
                     body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        status = resp.status
        conn.close()
        return status, data

    def _post_codex(self, events, calls=None):
        """POST an astra chat with a fake SSE provider; return response."""
        fake = FakeSSE(events)
        with patch.object(translate, "open_request",
                          return_value=fake) as send:
            status, data = self._post(self._chat("gpt-6-astra-high"))
        if calls is not None:
            calls.append(send.call_count)
        return status, data

    def _post_native(self, body, authorization="cred-A"):
        usage = wire.field(2, 11) + wire.field(3, 7)
        msg = wire.field(3, "answer") + wire.field(7, usage)
        raw = wire.frame(msg) + wire.end_stream()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Content-Length: %d\r\n\r\n" % len(raw))
            conn.sendall(raw)

        up = Upstream(script)
        self.addCleanup(up.close)
        with patch.object(relay, "UPSTREAM",
                          f"http://127.0.0.1:{up.port}"):
            return self._post(body, authorization=authorization)

    def _records(self):
        # the record lands in the handler's finally, just after the
        # last byte reaches the client — poll briefly rather than race it
        log = self.dir / "requests.jsonl"
        import time
        for _ in range(200):
            if log.exists() and log.read_text().strip():
                return [json.loads(l)
                        for l in log.read_text().splitlines()]
            time.sleep(0.01)
        return []

    def _wait(self, predicate):
        import time
        for _ in range(200):
            if predicate():
                return True
            time.sleep(0.01)
        return False

    def _snapshot(self):
        return self.ledger.snapshot()

    # -- tests ------------------------------------------------------------
    def test_codex_usage_lands_in_ledger(self):
        status, data = self._post_codex([completed(
            {"input_tokens": 7, "output_tokens": 3})])
        self.assertEqual(status, 200)
        frames = list(wire.iter_frames(data))
        self.assertEqual(frames[-1][0], 2)
        self.assertNotIn("error", json.loads(frames[-1][1]))
        self.assertEqual(len(self._records()), 1)
        codex = self._snapshot()["providers"]["codex"]
        self.assertEqual(codex["responses"], 1)
        self.assertEqual(codex["known"]["input_tokens"], 7)
        self.assertEqual(codex["known"]["output_tokens"], 3)
        # durable accounting active: legacy totals file is not written
        self.assertFalse((self.dir / "stats.json").exists())

    def test_native_usage_recorded_separately(self):
        status, data = self._post_native(self._chat("swe-2-medium"))
        self.assertEqual(status, 200)
        self.assertTrue(self._records())
        snap = self._snapshot()
        native = snap["providers"]["native"]
        self.assertEqual(native["responses"], 1)
        self.assertEqual(native["known"]["input_tokens"], 11)
        self.assertEqual(native["known"]["output_tokens"], 7)
        self.assertEqual(snap["providers"]["codex"]["responses"], 0)

    def test_admission_failure_never_calls_provider(self):
        with patch.object(translate, "open_request") as send, \
                patch.object(relay, "_admit_accounting",
                             side_effect=AccountingUnavailable("x")), \
                patch.object(relay.Handler, "_stream_native") as sn:
            status, data = self._post(self._chat("gpt-6-astra-high"))
            nstatus, ndata = self._post(self._chat("swe-2-medium"))
        send.assert_not_called()
        sn.assert_not_called()
        for payload in (data, ndata):
            frames = list(wire.iter_frames(payload))
            meta = json.loads(frames[-1][1])
            self.assertEqual(meta["error"]["code"], "failed_precondition")

    def test_completion_failure_client_unaffected_next_blocked(self):
        calls = []
        # hold the fault until the handler's finally has logged — the
        # response reaches the client before the record is durable
        with patch.object(self.ledger, "complete",
                          side_effect=AccountingUnavailable("lost")):
            status, data = self._post_codex(
                [completed({"input_tokens": 7, "output_tokens": 3})],
                calls)
            recs = self._records()
        self.assertEqual(status, 200)
        frames = list(wire.iter_frames(data))
        self.assertNotIn("error", json.loads(frames[-1][1]))
        self.assertEqual(calls, [1])          # exactly one provider call
        rec = recs[-1]
        self.assertTrue(rec.get("accounting_gap"))
        # completion failure degrades the ledger: nothing new is admitted
        with patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat("gpt-6-astra-high"))
        send.assert_not_called()
        meta = json.loads(list(wire.iter_frames(data))[-1][1])
        self.assertEqual(meta["error"]["code"], "failed_precondition")

    def test_request_log_failure_sets_export_degraded(self):
        real = relay.append_private
        log = self.dir / "requests.jsonl"

        def flaky(path, data):
            if pathlib.Path(path) == log:
                raise OSError("read-only log")
            return real(path, data)

        # the handler's finally runs after the client is answered — keep
        # the fault injected until the record attempt has happened
        with patch.object(relay, "append_private", flaky):
            status, _ = self._post_codex([completed(
                {"input_tokens": 7, "output_tokens": 3})])
            self.assertTrue(self._wait(
                lambda: relay._accounting_export_degraded))
        self.assertEqual(status, 200)
        snap = relay.accounting_status()
        self.assertTrue(snap["export_degraded"])
        # request-log failure must not alter durable SQLite totals
        self.assertEqual(snap["providers"]["codex"]["known"]
                         ["input_tokens"], 7)
        self.assertEqual(snap["pending_exports"], 0)

    def test_duplicate_log_record_keeps_single_receipt(self):
        op = reference("op-x")
        acct = reference("acct-a")
        self.ledger.admit(op, "codex", acct)
        rec = {"route": "codex",
               "_accounting": (op, "codex", acct),
               "codex_usage_calls": [
                   {"status": "completed", "input_tokens": 7,
                    "output_tokens": 3, "cached_tokens": None,
                    "reasoning_tokens": None}]}
        relay._log_record(rec)
        relay._log_record(rec)
        self.assertEqual(
            self._snapshot()["providers"]["codex"]["responses"], 1)
        receipts = (self.dir / "receipts.jsonl").read_text().splitlines()
        self.assertEqual(len(receipts), 1)

    def test_native_binding_fingerprints_separate(self):
        self._post_native(self._chat("swe-2-medium"),
                          authorization="cred-A")
        self._post_native(self._chat("swe-2-medium"),
                          authorization="cred-B")
        self.assertTrue(self._wait(
            lambda: _ldb1(self.ledger, 
                "SELECT COUNT(*) FROM accounting_events")[0]
            == 2))
        rows = _ldba(self.ledger, 
            "SELECT payload FROM accounting_events")
        self.assertEqual(len(rows), 2)
        refs = {json.loads(p)["account_ref"] for (p,) in rows}
        self.assertEqual(len(refs), 2)
        for ref in refs:
            self.assertTrue(len(ref) == 64)
        for (p,) in rows:
            self.assertNotIn("cred-A", p)
            self.assertNotIn("cred-B", p)

    def test_startup_dirty_ledger_degrades_and_blocks(self):
        # a second ledger on a separate store with an unclosed run is
        # degraded at construction; bound to the relay it blocks service
        dirty_db = self.dir / "other.sqlite3"
        first = AccountingLedger(dirty_db)
        first.close(clean=False)
        led2 = AccountingLedger(dirty_db)
        self.addCleanup(led2.close)
        self.assertTrue(led2.degraded)
        with patch.object(relay, "_accounting", led2):
            conn = self._conn()
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            body = json.loads(resp.read())
            conn.close()
            self.assertEqual(resp.status, 503)
            self.assertFalse(body["ok"])
            self.assertTrue(body["accounting"]["degraded"])
            # public healthz never exposes totals, paths, or timestamps
            self.assertEqual(
                set(body["accounting"]),
                {"degraded", "coverage", "reconciliation_required",
                 "export_degraded"})
            with patch.object(translate, "open_request") as send:
                status, data = self._post(
                    self._chat("gpt-6-astra-high"))
            send.assert_not_called()
            self.assertEqual(status, 200)
            meta = json.loads(list(wire.iter_frames(data))[-1][1])
            self.assertEqual(meta["error"]["code"], "failed_precondition")

    def test_ledger_init_failure_never_binds_server(self):
        called = []

        @contextlib.contextmanager
        def owner(path):
            yield

        class FakePrivate:
            def __init__(self, path, create=False):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        class BoomLedger:
            def __init__(self, path):
                raise AccountingUnavailable("store unavailable")

        marks = [
            patch.object(relay, "PrivateDirectory", FakePrivate),
            patch.object(relay, "store_owner", owner),
            patch.object(relay, "relay_token", lambda: "t" * 20),
            patch.object(relay.catalog, "attach_store", lambda p: None),
            patch.object(relay, "_load_stats", lambda: None),
            patch.object(relay.auth, "get_token",
                         lambda: ("t", "a")),
            patch.object(relay, "AccountingLedger", BoomLedger),
            patch.object(relay, "_BoundedServer",
                         lambda *a: called.append("server")),
        ]
        for p in marks:
            p.start()
        try:
            self.assertRaises(AccountingUnavailable, relay.serve, 0)
        finally:
            for p in marks:
                p.stop()
        self.assertEqual(called, [])

    def test_export_failure_receipt_stays_pending(self):
        real = relay.append_private
        receipts = self.dir / "receipts.jsonl"

        def flaky(path, data):
            if pathlib.Path(path) == receipts:
                raise OSError("readonly receipts")
            return real(path, data)

        with patch.object(relay, "append_private", flaky):
            status, _ = self._post_codex([completed(
                {"input_tokens": 7, "output_tokens": 3})])
            self.assertTrue(self._wait(
                lambda: relay._accounting_export_degraded))
        self.assertEqual(status, 200)
        snap = self._snapshot()
        # the receipt is durable in SQLite even though export failed
        self.assertEqual(snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(snap["pending_exports"], 1)
        conn = self._conn()
        conn.request("GET", "/healthz")
        body = json.loads(conn.getresponse().read())
        conn.close()
        self.assertTrue(body["accounting"]["export_degraded"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
