"""ExperimentalHost tests: offline loopback only — fake provider, fake
auth, injected ledger key; no Keychain, no real ports, no real devin."""
from __future__ import annotations

import http.client
import json
import pathlib
import socket
import sys
import time
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from fusion_relay import catalog, relay, translate, wire
from fusion_relay.accounting import AccountingLedger
from fusion_relay.continuation import (ContinuationLedger, digest)
from fusion_relay.experimental_host import ExperimentalHost, main
from fusion_relay.host_binding import (AuthenticatedContext,
                                       CapabilityStore,
                                       HEADER_CAPABILITY,
                                       HEADER_OPERATION,
                                       HEADER_PROOF,
                                       PROVENANCE_LAUNCHER)
from fusion_relay.translate import parse_routed_model

KEY = b'k' * 32
TOKEN = "acct-test-token-0000"
RPC = "/xexerra.chat.v1.ChatService/GetChatMessage"


def _ldb1(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchone()


def _ldba(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchall()


def _ldbw(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params)


class FakeSSE:
    def __init__(self, events, status=200):
        self._lines = [b"data: " + json.dumps(e).encode() + b"\n"
                       for e in events]
        self._pending = b""
        self.status = status
        self.headers = {}

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

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass


def completed(usage, output=None):
    return {"type": "response.completed", "response": {
        "id": "r-1", "status": "completed",
        "output": output or [], "usage": usage}}


class QuietServer(relay._BoundedServer):
    def handle_error(self, request, client_address):
        err = sys.exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError,
                            http.client.IncompleteRead)):
            return
        raise err


def msg(source, text_body=""):
    inner = wire.field(2, source)
    if text_body:
        inner += wire.field(3, text_body)
    return wire.field(3, inner)


def chat(msgs, session="s1", model="gpt-6-astra-high"):
    return wire.frame(
        b"".join(msgs) + wire.field(16, session)
        + wire.field(21, model)) + wire.end_stream()


def packet(msgs, session="s1", model="gpt-6-astra-high"):
    body = chat(msgs, session, model)
    packets = list(relay._decode_packets(body, framed=True))
    assert len(packets) == 1
    return packets[0]


class _HostBase(unittest.TestCase):
    """Host with an injected ledger/capability store; fake clock shared
    by host and store so expiry/retry windows are deterministic."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = pathlib.Path(self._tmp.name).resolve()
        self.now = [1000.0]
        self.cled = ContinuationLedger(
            self.dir / "continuation.sqlite3", KEY)
        self.addCleanup(self.cled.close)
        self.caps = CapabilityStore(clock=lambda: self.now[0])
        self.journal = self.dir / "host-journal.jsonl"
        p = patch.object(relay.auth, "get_token",
                         return_value=("tok", "acct-1"))
        self.addCleanup(p.stop)
        p.start()
        catalog.reset()
        self.addCleanup(catalog.reset)
        catalog.attach_store(self.dir / "routes.json")
        self.addCleanup(relay.set_continuation_coordinator, None)
        self.host = ExperimentalHost(
            data_dir=self.dir, relay_port=1, proxy_port=0,
            ledger=self.cled, capabilities=self.caps,
            journal_path=self.journal,
            clock=lambda: self.now[0])

    def _journal_events(self):
        if not self.journal.exists():
            return []
        return [json.loads(l) for l in
                self.journal.read_text().splitlines() if l.strip()]

    def _verify_ctx(self, bound, body_digest):
        h = bound["headers"]
        return AuthenticatedContext(
            capability_id=h[HEADER_CAPABILITY],
            operation_id=h[HEADER_OPERATION],
            body_digest=body_digest,
            proof=h[HEADER_PROOF], now=self.now[0])


class BindRequestTest(_HostBase):
    def test_astra_issues_lead_launcher_capability(self):
        pkt = packet([msg(1, "hi")])
        bound = self.host.bind_request(pkt, "gpt-6-astra-high")
        self.assertIsNotNone(bound)
        routed = parse_routed_model("gpt-6-astra-high")
        body = translate.packet_to_responses_body(
            pkt, routed, {}, continuity_scope="")
        host = self.caps.verify(self._verify_ctx(bound, digest(body)))
        self.assertEqual(host.lane, "lead")
        self.assertEqual(host.provenance, PROVENANCE_LAUNCHER)
        self.assertEqual(host.native_session_id, "s1")
        self.assertEqual(host.account_reference, "acct-1")
        self.assertEqual(host.model_profile, "gpt-6-astra:high")
        self.assertEqual(host.continuation_epoch, "launcher-genesis")
        self.assertFalse(bound["reused_operation"])

    def test_operation_id_retry_window(self):
        pkt = packet([msg(1, "hi")])
        b1 = self.host.bind_request(pkt, "gpt-6-astra-high")
        b2 = self.host.bind_request(pkt, "gpt-6-astra-high")
        self.assertEqual(b1["headers"][HEADER_OPERATION],
                         b2["headers"][HEADER_OPERATION])
        self.assertTrue(b2["reused_operation"])
        other = packet([msg(1, "different")])
        b3 = self.host.bind_request(other, "gpt-6-astra-high")
        self.assertNotEqual(b1["headers"][HEADER_OPERATION],
                            b3["headers"][HEADER_OPERATION])
        self.now[0] += 301.0  # beyond retry_window_s
        b4 = self.host.bind_request(pkt, "gpt-6-astra-high")
        self.assertNotEqual(b1["headers"][HEADER_OPERATION],
                            b4["headers"][HEADER_OPERATION])
        self.assertFalse(b4["reused_operation"])

    def test_non_codex_route_binds_nothing(self):
        self.assertIsNone(
            self.host.bind_request(packet([msg(1, "hi")]),
                                   "swe-2"))

    def test_profile_change_transitions_epoch(self):
        pkt = packet([msg(1, "hi")])
        b1 = self.host.bind_request(pkt, "gpt-6-astra-high")
        old_cap = b1["headers"][HEADER_CAPABILITY]
        b2 = self.host.bind_request(pkt, "gpt-6-astra-low")
        self.assertNotEqual(old_cap, b2["headers"][HEADER_CAPABILITY])
        rows = _ldba(self.cled,
                     "SELECT reason FROM epoch_transitions")
        self.assertEqual([r[0] for r in rows], ["model_switch"])
        with self.assertRaises(Exception) as cm:
            self.caps.verify(self._verify_ctx(b1, "0" * 64))
        self.assertIn("revoked", str(cm.exception))
        routed = parse_routed_model("gpt-6-astra-low")
        body = translate.packet_to_responses_body(
            pkt, routed, {}, continuity_scope="")
        host = self.caps.verify(self._verify_ctx(b2, digest(body)))
        self.assertEqual(host.model_profile, "gpt-6-astra:low")
        self.assertNotEqual(host.continuation_epoch, "launcher-genesis")
        events = self._journal_events()
        self.assertEqual([e["event"] for e in events],
                         ["epoch_transition"])
        self.assertEqual(events[0]["reason"], "model_switch")


class StopTest(_HostBase):
    def test_stop_joins_relay_thread(self):
        release = threading.Event()

        def serve():
            release.wait(10)

        t = threading.Thread(target=serve)
        t.start()
        self.host._relay_thread = t
        self.host._owns_relay = True
        with patch.object(self.host, "_post_shutdown",
                          side_effect=release.set):
            self.host.stop()
        self.assertFalse(t.is_alive())

    def test_stop_warns_when_relay_thread_hangs(self):
        import contextlib, io
        release = threading.Event()

        def serve():
            release.wait(10)

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        self.addCleanup(release.set)
        self.host._relay_thread = t
        self.host._owns_relay = True
        self.host._join_timeout_s = 0.2
        err = io.StringIO()
        started = time.monotonic()
        with patch.object(self.host, "_post_shutdown"), \
                contextlib.redirect_stderr(err):
            self.host.stop()
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn("relay thread did not exit", err.getvalue())


class ProxyTest(_HostBase):
    """Loopback: real relay Handler in-thread (durable) + the host's
    proxy. Provider and auth are fakes; ledgers are real."""

    def setUp(self):
        super().setUp()
        # The relay verifies capability expiry against real time — the
        # fake clock is only for the pure bind_request unit tests.
        import time
        self.caps._clock = time.time
        self.host._clock = time.time
        for attr, val in (
                ("DATA_DIR", self.dir),
                ("TOKEN_PATH", self.dir / "relay-token"),
                ("REQUESTS_LOG", self.dir / "requests.jsonl"),
                ("STATS_PATH", self.dir / "stats.json"),
                ("_RELAY_TOKEN", TOKEN),
                ("STREAM_MODE", "buffer"),
                ("CONTINUATION_MODE", "durable")):
            p = patch.object(relay, attr, val)
            self.addCleanup(p.stop)
            p.start()
        self.acct = AccountingLedger(self.dir / "accounting.sqlite3")
        self.addCleanup(self.acct.close)
        for attr, val in (("_accounting", self.acct),
                          ("_accounting_required", True),
                          ("_accounting_export_degraded", False)):
            p = patch.object(relay, attr, val)
            self.addCleanup(p.stop)
            p.start()
        self.server = QuietServer(("127.0.0.1", 0), relay.Handler)
        self.server.identity = types.SimpleNamespace(secret=b"k" * 32)
        self.host._relay_port = self.server.server_address[1]
        self.host._start_proxy()
        self.addCleanup(self.host._stop_proxy)
        self.proxy_port = self.host._proxy.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05})
        self._thread.start()
        self.addCleanup(
            lambda: self.assertFalse(self._thread.is_alive()))
        self.addCleanup(self._thread.join, 10)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def _post(self, body, path=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.proxy_port,
                                          timeout=15)
        conn.request("POST", path or f"/t/{TOKEN}{RPC}", body=body,
                     headers={"Content-Type": "application/connect+proto"})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, data, dict(resp.getheaders())

    def _trailer(self, data):
        frames = list(wire.iter_frames(data))
        self.assertEqual(frames[-1][0], 2)
        return json.loads(frames[-1][1])

    def _wait(self, predicate):
        for _ in range(200):
            if predicate():
                return True
            time.sleep(0.01)
        return False

    def _records(self):
        log = self.dir / "requests.jsonl"
        for _ in range(200):
            if log.exists() and log.read_text().strip():
                return [json.loads(l)
                        for l in log.read_text().splitlines()]
            time.sleep(0.01)
        return []

    def test_end_to_end_bound_and_replayed(self):
        body = chat([msg(1, "one")])
        events = [completed({"input_tokens": 1, "output_tokens": 1})]
        with patch.object(translate, "open_request",
                          return_value=FakeSSE(events)) as send:
            s1, d1, h1 = self._post(body)
            s2, d2, h2 = self._post(body)
        self.assertEqual((s1, s2), (200, 200))
        self.assertNotIn("error", self._trailer(d1))
        self.assertEqual(d1, d2)
        send.assert_called_once()  # second POST replayed from the ledger
        rec = self._records()[-1]
        self.assertEqual(rec["binding_status"], "verified")
        self.assertEqual(rec["continuation_status"], "durable_host_bound")
        self.assertEqual(rec["continuation_role"], "lead")
        self.assertIn("x-fusion-continuation-epoch",
                      {k.lower() for k in h2})
        events = self._journal_events()
        bound = [e for e in events if e["event"] == "request_bound"]
        self.assertEqual(len(bound), 2)
        self.assertEqual([e["reused_operation"] for e in bound],
                         [False, True])
        self.assertEqual(bound[0]["relay_status"], 200)
        self.assertIn("epoch_ref", bound[0])
        raw = self.journal.read_text()
        self.assertNotIn("X-Fusion", raw)
        self.assertNotIn(TOKEN, raw)
        proof = self.host._sessions["s1"]["binding"].capability_id
        self.assertNotIn(proof, raw)

    def test_resumed_session_adopts_legacy_history(self):
        # Turn under session s1 commits normally.
        events = [completed({"input_tokens": 1, "output_tokens": 1})]
        with patch.object(translate, "open_request",
                          return_value=FakeSSE(events)):
            s, d, _ = self._post(chat([msg(1, "one")], session="s1"))
        self.assertNotIn("error", self._trailer(d))
        # devin -r rewrote field 16: new seed, assistant-bearing history
        # that does NOT extend s1's committed turn (edited first message)
        # -> no carry-over candidate, plain genesis adoption.
        resumed = chat([msg(1, "one-edited"), msg(2, "answer1"),
                        msg(1, "two")], session="s2")
        with patch.object(translate, "open_request",
                          return_value=FakeSSE(events)) as send:
            s, d, _ = self._post(resumed)
        # the first forward failed pre-dispatch; the retry is the only
        # provider call
        self.assertEqual(send.call_count, 1)
        self.assertEqual(s, 200)
        self.assertNotIn("error", self._trailer(d))
        self.assertEqual(_ldb1(
            self.cled,
            "SELECT COUNT(*) FROM continuation_turns")[0], 2)
        self.assertEqual(_ldba(
            self.cled, "SELECT reason FROM epoch_transitions"),
            [("legacy_history",)])
        entries = self._journal_events()
        kinds = [e["event"] for e in entries]
        self.assertIn("legacy_adoption_retry", kinds)
        trans = [e for e in entries if e["event"] == "epoch_transition"]
        self.assertEqual(trans[-1]["reason"], "legacy_history")
        self.assertEqual(trans[-1]["prior_reasoning"], "unavailable")
        retry = [e for e in entries
                 if e["event"] == "legacy_adoption_retry"][-1]
        self.assertEqual(retry["relay_status"], 200)
        self.assertFalse(trans[-1]["carried"])

    def test_resumed_session_carries_stored_reasoning(self):
        """The 'resume recovers stored continuation' proof: a new seed
        whose history extends the old scope's turns exactly gets those
        turns carried over, and the stored reasoning item is reinserted
        into the provider body of the resumed turn."""
        reasoning = {"type": "reasoning", "id": "r1",
                     "encrypted_content": "opaque-1"}
        out1 = [reasoning, {"type": "message", "id": "m1", "content": [
            {"type": "output_text", "text": "answer1"}]}]
        with patch.object(translate, "open_request",
                          return_value=FakeSSE([completed(
                              {"input_tokens": 1, "output_tokens": 1},
                              output=out1)])):
            s, d, _ = self._post(chat([msg(1, "one")], session="s1"))
        self.assertNotIn("error", self._trailer(d))
        resumed = chat([msg(1, "one"), msg(2, "answer1"), msg(1, "two")],
                       session="s2")
        with patch.object(translate, "open_request",
                          return_value=FakeSSE([completed(
                              {"input_tokens": 1, "output_tokens": 1})])
                          ) as send:
            s, d, _ = self._post(resumed)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(s, 200)
        self.assertNotIn("error", self._trailer(d))
        provider_body = json.loads(send.call_args[0][0].data)
        self.assertIn(reasoning, provider_body["input"])
        trans = [e for e in self._journal_events()
                 if e["event"] == "epoch_transition"][-1]
        self.assertTrue(trans["carried"])
        self.assertEqual(trans["prior_reasoning"], "carried")
        row = _ldb1(self.cled, "SELECT carried_from_scope FROM "
                    "epoch_transitions")
        self.assertIsNotNone(row[0])
        # new scope: carried turn (rev 1) + resumed turn (rev 2)
        self.assertEqual(sorted(r[0] for r in _ldba(
            self.cled, "SELECT revision FROM continuation_heads")),
            [1, 2])

    def _diverged_history_test(self, with_note):
        # Turn under s1 commits normally.
        events = [completed({"input_tokens": 1, "output_tokens": 1})]
        with patch.object(translate, "open_request",
                          return_value=FakeSSE(events)):
            s, d, _ = self._post(chat([msg(1, "one")], session="s1"))
        self.assertNotIn("error", self._trailer(d))
        if with_note:
            # The hook's session_id is a display name — structurally
            # unmatched against the wire seed — but recent.
            from fusion_relay.accounting import reference
            self.host._coordinator.note_compaction(
                reference('session', 'generated-voyage'))
        # Same seed, history that does not extend the anchors.
        diverged = chat([msg(1, "summary"), msg(2, "answer1"),
                         msg(1, "two")], session="s1")
        with patch.object(translate, "open_request",
                          return_value=FakeSSE(events)) as send:
            s, d, _ = self._post(diverged)
        self.assertEqual(send.call_count, 1)  # first forward never dispatched
        self.assertEqual(s, 200)
        self.assertNotIn("error", self._trailer(d))
        self.assertEqual(_ldb1(
            self.cled,
            "SELECT COUNT(*) FROM continuation_turns")[0], 2)
        self.assertEqual(_ldba(
            self.cled, "SELECT reason FROM epoch_transitions"),
            [("history_divergence",)])
        entries = self._journal_events()
        trans = [e for e in entries
                 if e["event"] == "epoch_transition"][-1]
        self.assertEqual(trans["reason"], "history_divergence")
        self.assertEqual(trans["compaction_note_correlation"],
                         "unmatched")
        self.assertIs(trans["recent_compaction_notice"], with_note)
        retry = [e for e in entries
                 if e["event"] == "legacy_adoption_retry"][-1]
        self.assertEqual(retry["reason"], "history_divergence")
        self.assertEqual(retry["relay_status"], 200)

    def test_diverged_history_adopted_without_compaction_note(self):
        self._diverged_history_test(with_note=False)

    def test_diverged_history_journals_recent_compaction_notice(self):
        self._diverged_history_test(with_note=True)

    def test_client_disconnect_cancels_upstream(self):
        release = threading.Event()

        class BlockingSSE:
            status = 200
            headers = {}
            closed = False

            def readline(self, limit=-1):
                release.wait(10)
                if self.closed:
                    return b""
                return (b'data: {"type":"response.in_progress",'
                        b'"response":{"id":"r1"}}\n\n')

            def close(self):
                self.closed = True
                release.set()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                self.close()
                return False

        fake = BlockingSSE()
        with patch.object(translate, "open_request", return_value=fake):
            sock = socket.create_connection(
                ("127.0.0.1", self.proxy_port), timeout=5)
            body = chat([msg(1, "one")], session="s1")
            sock.sendall(
                (f"POST /t/{TOKEN}{RPC} HTTP/1.1\r\n"
                 "Host: 127.0.0.1\r\n"
                 "Content-Type: application/connect+proto\r\n"
                 f"Content-Length: {len(body)}\r\n\r\n").encode() + body)
            time.sleep(0.5)
            sock.close()
            # the proxy must observe the dead client and abort its
            # upstream request so the relay sees a disconnect
            deadline = time.time() + 5
            while time.time() < deadline:
                if any(e["event"] == "client_disconnected"
                       for e in self._journal_events()):
                    break
                time.sleep(0.05)
            self.assertTrue(any(
                e["event"] == "client_disconnected"
                and e["phase"] == "upstream_in_flight"
                for e in self._journal_events()))
            release.set()
            self.assertTrue(self._wait(lambda: bool(self._records())))
        rec = self._records()[-1]
        self.assertEqual(rec.get("error_category"), "cancelled")
        self.assertTrue(rec.get("client_gone"))
        self.assertTrue(fake.closed)  # provider stream released
        self.assertIn(("cancel_unconfirmed",), _ldba(
            self.cled, "SELECT status FROM operations"))

    def test_binding_refused_forwards_without_headers(self):
        pkt = packet([msg(1, "one")])
        self.host.bind_request(pkt, "gpt-6-astra-high")
        scope = self.host._sessions["s1"]["binding"] \
            .continuation_binding().scope()
        _ldbw(self.cled,
              "INSERT INTO operations(scope,operation_id,fingerprint,"
              "status) VALUES(?,?,?,'executing')",
              (scope, "op-block", "fp"))
        body = chat([msg(1, "two")], model="gpt-6-astra-low")
        with patch.object(translate, "open_request") as send:
            status, data, _ = self._post(body)
        send.assert_not_called()
        self.assertEqual(status, 200)
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "failed_precondition")
        events = [e["event"] for e in self._journal_events()]
        self.assertIn("binding_refused", events)
        self.assertIn("bind_error", events)
        rec = self._records()[-1]
        self.assertEqual(rec["binding_status"], "unavailable")


class MainTest(unittest.TestCase):
    def test_help(self):
        with self.assertRaises(SystemExit) as cm:
            main(["--help"])
        self.assertEqual(cm.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
