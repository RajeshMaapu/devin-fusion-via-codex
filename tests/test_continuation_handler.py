"""Durable continuation handler integration: real loopback, fake
provider, real ContinuationLedger, fake trusted binding resolver."""
from __future__ import annotations

import http.client
import json
import pathlib
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

import dataclasses

from fusion_relay import catalog, payload_budget, relay, translate, wire
from fusion_relay.accounting import AccountingLedger
from fusion_relay.continuation import (ContinuationBinding,
                                       ContinuationError,
                                       ContinuationLedger)
from fusion_relay.continuation_host import ContinuationCoordinator
from fusion_relay.host_binding import (CapabilityStore,
                                       PROVENANCE_LAUNCHER,
                                       PROVENANCE_NATIVE)

KEY = b'k' * 32

PNG = (pathlib.Path(__file__).resolve().parent
       / "fixtures" / "tool-image.png").read_bytes()


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


class QuietServer(relay._BoundedServer):
    def handle_error(self, request, client_address):
        err = __import__("sys").exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError,
                            http.client.IncompleteRead)):
            return
        raise err


def completed(usage, output=None):
    return {"type": "response.completed", "response": {
        "id": "r-1", "status": "completed",
        "output": output or [], "usage": usage}}


OUT1 = [{'type': 'reasoning', 'id': 'r1', 'encrypted_content': 'op1'},
        {'type': 'function_call', 'call_id': 'c1', 'name': 'tool',
         'arguments': '{}'},
        {'type': 'message', 'id': 'm1',
         'content': [{'type': 'output_text', 'text': 'ans'}]}]
OUT2 = [{'type': 'reasoning', 'id': 'r2', 'encrypted_content': 'op2'},
        {'type': 'message', 'id': 'm2',
         'content': [{'type': 'output_text', 'text': 'done'}]}]


class _HandlerFixture:
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
        self.cled = ContinuationLedger(
            self.dir / "continuation.sqlite3", KEY)
        self.addCleanup(self.cled.close)
        self.binding = ContinuationBinding(
            account='acct-1', session='s1', lane='lead',
            profile='gpt-6-astra:high', epoch='e1')
        self.resolver_result = self.binding
        self.coordinator = ContinuationCoordinator(
            self.cled, lambda packet, creds: self.resolver_result)
        p = patch.object(relay, "_continuation_coordinator",
                         self.coordinator)
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

    # -- helpers ---------------------------------------------------------
    def _db1(self, ledger, sql, params=()):
        # sqlite3 on /usr/bin/python3 3.9 crashes on concurrent statement
        # execution on one connection — always hold the ledger's lock
        # while the server thread may be using it.
        with ledger._lock:
            return ledger._db.execute(sql, params).fetchone()

    def _dba(self, ledger, sql, params=()):
        with ledger._lock:
            return ledger._db.execute(sql, params).fetchall()

    def _conn(self):
        return http.client.HTTPConnection("127.0.0.1", self.port,
                                          timeout=15)

    def _msg(self, source, text_body="", tool_calls=(), call_id=""):
        inner = wire.field(2, source)
        if text_body:
            inner += wire.field(3, text_body)
        if call_id:
            inner += wire.field(7, call_id)
        for cid, name, args in tool_calls:
            inner += wire.field(
                6, wire.field(1, cid) + wire.field(2, name)
                + wire.field(3, args))
        return wire.field(3, inner)

    def _chat(self, msgs, session="s1", model="gpt-6-astra-high"):
        return wire.frame(
            b"".join(msgs) + wire.field(16, session)
            + wire.field(21, model)) + wire.end_stream()

    def _post(self, body):
        conn = self._conn()
        conn.request("POST", f"/t/{self.TOKEN}{self.RPC}",
                     body=body,
                     headers={"Content-Type": "application/connect+proto"})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, data

    def _one_turn(self):
        return [self._msg(1, "one")]

    def _post_codex(self, events, body=None):
        fake = FakeSSE(events)
        with patch.object(translate, "open_request",
                          return_value=fake) as send:
            status, data = self._post(body or self._chat(self._one_turn()))
        return status, data, send

    def _records(self):
        import time
        log = self.dir / "requests.jsonl"
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

    def _trailer(self, data):
        frames = list(wire.iter_frames(data))
        self.assertEqual(frames[-1][0], 2)
        return json.loads(frames[-1][1])

class ContinuationHandlerTest(_HandlerFixture, unittest.TestCase):
    # -- tests ------------------------------------------------------------
    def test_no_coordinator_fails_closed(self):
        with patch.object(relay, "_continuation_coordinator", None), \
                patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat(self._one_turn()))
        send.assert_not_called()
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "failed_precondition")

    def test_unqualified_binding_no_fallback(self):
        self.resolver_result = None
        with patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat(self._one_turn()))
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "failed_precondition")

    def test_committed_turn_buffered_and_replayed(self):
        sent_at = []
        real_send = relay.Handler._send

        def observing_send(handler, code, payload, ctype,
                           extra_headers=None):
            sent_at.append(self._db1(
                self.cled,
                "SELECT COUNT(*) FROM continuation_turns")[0])
            return real_send(handler, code, payload, ctype,
                             extra_headers=extra_headers)

        with patch.object(relay.Handler, "_send", observing_send):
            status, data, send = self._post_codex(
                [completed({"input_tokens": 7, "output_tokens": 3},
                           output=OUT1)])
        self.assertEqual(status, 200)
        self.assertNotIn("error", self._trailer(data))
        self.assertEqual(send.call_count, 1)
        self.assertEqual(sent_at, [1])   # commit durable before _send
        # the handler's finally logs after the client is answered — wait
        # for the durable receipt to land rather than racing it
        self.assertTrue(self._wait(lambda: self._db1(self.acct,
                      "SELECT COUNT(*) FROM accounting_events")[0] == 1))

        # exact retry: replay, no second provider call or receipt
        status, data2, send2 = self._post_codex(
            [completed({"input_tokens": 9, "output_tokens": 9})])
        self.assertEqual(send2.call_count, 0)
        self.assertEqual(data2, data)
        self.assertNotIn("error", self._trailer(data2))
        self.assertTrue(self._wait(lambda: len(self._records()) >= 2))
        self.assertEqual(self._db1(self.acct,
                      "SELECT COUNT(*) FROM accounting_events")[0], 1)

    def test_empty_terminal_output_recovers_streamed_call(self):
        # Live backend shape: function_call fully streamed, then
        # response.completed with output: [] — must not fail closed.
        args = '{"file_path":"./imgs/img_00.png"}'
        call_item = {'id': 'fc1', 'type': 'function_call',
                     'call_id': 'c1', 'name': 'read', 'arguments': args}
        events = [
            {'type': 'response.output_item.added', 'output_index': 0,
             'item_id': 'fc1',
             'item': {'id': 'fc1', 'type': 'function_call',
                      'call_id': 'c1', 'name': 'read', 'arguments': ''}},
            {'type': 'response.function_call_arguments.delta',
             'output_index': 0, 'item_id': 'fc1', 'delta': args[:20]},
            {'type': 'response.function_call_arguments.delta',
             'output_index': 0, 'item_id': 'fc1', 'delta': args[20:]},
            {'type': 'response.function_call_arguments.done',
             'output_index': 0, 'item_id': 'fc1', 'arguments': args},
            {'type': 'response.output_item.done', 'output_index': 0,
             'item_id': 'fc1', 'item': call_item},
            completed({"input_tokens": 7, "output_tokens": 3},
                      output=[])]
        status, data, send = self._post_codex(events)
        self.assertEqual(status, 200)
        self.assertNotIn("error", self._trailer(data))
        calls = []
        for flags, payload in wire.iter_frames(data):
            if flags & 2:
                continue
            msg = wire.decode(payload)
            for raw in msg.get(6, []):
                calls.append(wire.decode(raw))
        self.assertEqual(len(calls), 1)
        self.assertEqual(wire.text(calls[0], 2), 'read')
        self.assertEqual(wire.text(calls[0], 1), 'c1')
        self.assertTrue(self._wait(lambda: bool(self._records())))
        rec = self._records()[-1]
        self.assertEqual(rec["codex_status"], "completed")
        self.assertNotIn("error_category", rec)
        self.assertEqual(rec["tool_call_names"], ["read"])
        # durable turn committed with the streamed call in its output
        row = self._db1(
            self.cled,
            "SELECT scope, operation_id, sealed FROM "
            "continuation_turns")
        turn = json.loads(self.cled._open(row[0], row[1], row[2]))
        from fusion_relay.continuation import visible_items
        self.assertEqual(visible_items(turn['output']),
                         [{'type': 'function_call', 'call_id': 'c1',
                           'name': 'read', 'arguments': args}])

    def test_tool_history_reinserts_ordered_items(self):
        status, data, send = self._post_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)])
        self.assertNotIn("error", self._trailer(data))
        msgs = [self._msg(1, "one"),
                self._msg(2, "ans",
                          tool_calls=[("c1", "tool", "{}")]),
                self._msg(4, "res", call_id="c1"),
                self._msg(1, "two")]
        status, data2, send2 = self._post_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT2)],
            body=self._chat(msgs))
        self.assertNotIn("error", self._trailer(data2))
        req = send2.call_args[0][0]
        provider_body = json.loads(req.data)
        fc = {'type': 'function_call', 'call_id': 'c1', 'name': 'tool',
              'arguments': '{}'}
        self.assertEqual(provider_body["input"], [
            {'role': 'user', 'content': 'one'},
            OUT1[0], fc, OUT1[2],
            {'type': 'function_call_output', 'call_id': 'c1',
             'output': 'res'},
            {'role': 'user', 'content': 'two'}])
        # legacy in-memory reinjection is disabled under durable mode
        self.assertTrue(self._wait(lambda: len(self._records()) >= 2))
        rec = self._records()[-1]
        self.assertNotIn("reasoning_echoed", rec)
        self.assertNotIn("relay_items_reinjected", rec)
        # turn 1's projection came back in the client's history: that is
        # history evidence for turn 1 — recorded as its own level, while
        # the new turn stays pending
        self.assertEqual(rec["turns_history_evidenced"], 1)
        self.assertEqual(rec["acceptance"], "pending")
        states = [r[0] for r in self._dba(
            self.cled, "SELECT state FROM continuation_turns "
            "ORDER BY revision")]
        self.assertEqual(states, ["history_evidenced", "offered"])
        from fusion_relay import diagnostics
        from fusion_relay.accounting import reference
        snap = diagnostics.snapshot(reference("session", "s1"))
        self.assertEqual(snap["acceptance_prior"], "history_evidenced")
        self.assertEqual(snap["acceptance"], "pending")

    def test_commit_failure_no_success_then_outcome_unknown(self):
        with patch.object(self.cled, "commit",
                          side_effect=ContinuationError("lost")):
            status, data, send = self._post_codex(
                [completed({"input_tokens": 7, "output_tokens": 3},
                           output=OUT1)])
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertIn("error", meta)
        self.assertNotEqual(meta["error"]["code"], "failed_precondition")
        # same operation replays as outcome-unknown, not a new inference
        status, data2, send2 = self._post_codex(
            [completed({"input_tokens": 1, "output_tokens": 1})])
        self.assertEqual(send2.call_count, 0)
        self.assertEqual(self._trailer(data2)["error"]["code"],
                         "failed_precondition")

    def test_delta_mode_commit_failure_valid_error_trailer(self):
        # durable is always buffered; with STREAM_MODE=delta the error
        # must still be a complete HTTP response, not raw chunks
        with patch.object(relay, "STREAM_MODE", "delta"), \
                patch.object(self.cled, "commit",
                             side_effect=ContinuationError("lost")):
            status, data, send = self._post_codex(
                [completed({"input_tokens": 7, "output_tokens": 3},
                           output=OUT1)])
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "internal")

    def test_delta_mode_incomplete_valid_error_trailer(self):
        incomplete = {"type": "response.incomplete", "response": {
            "id": "r-1", "status": "incomplete", "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1}}}
        with patch.object(relay, "STREAM_MODE", "delta"):
            status, data, send = self._post_codex([incomplete])
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertIn("error", meta)
        self.assertTrue(self._wait(lambda: bool(self._records())))
        rec = self._records()[-1]
        self.assertIn("incomplete_detail", rec)
        self.assertRegex(rec["incomplete_detail"],
                         r"^[A-Za-z0-9 _:.%/-]{1,96}$")

    def test_delta_mode_cancellation_valid_error_trailer(self):
        with patch.object(relay, "STREAM_MODE", "delta"), \
                patch.object(relay, "call_codex",
                             side_effect=relay.RequestCancelled()):
            status, data = self._post(self._chat(self._one_turn()))
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "cancelled")

    def test_computer_tool_never_dispatchable(self):
        bad_out = [{'type': 'function_call', 'call_id': 'x1',
                    'name': 'codex_computer', 'arguments': '{}'},
                   {'type': 'message', 'id': 'm1',
                    'content': [{'type': 'output_text', 'text': 'hi'}]}]
        status, data, send = self._post_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=bad_out)])
        self.assertEqual(send.call_count, 1)
        meta = self._trailer(data)
        self.assertIn("error", meta)
        # nothing committed: the unoffered tool turn is not durable
        self.assertEqual(self._db1(self.cled,
                      "SELECT COUNT(*) FROM continuation_turns")[0], 0)

    def test_resolver_error_sanitized(self):
        def leaking(packet, creds):
            raise ContinuationError("internal-secret-detail")
        self.coordinator._resolver = leaking
        with patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat(self._one_turn()))
        send.assert_not_called()
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "failed_precondition")
        self.assertNotIn("internal-secret-detail", data.decode(
            errors="replace"))

    # -- payload budget ------------------------------------------------
    def _img_msg(self, text_body=""):
        inner = wire.field(2, 1) + wire.field(9, PNG)
        if text_body:
            inner += wire.field(3, text_body)
        return wire.field(3, inner)

    def test_preflight_image_count_rejects_before_reservation(self):
        body = self._chat([self._img_msg() for _ in range(41)])
        with patch.object(translate, "open_request") as send:
            status, data = self._post(body)
        send.assert_not_called()
        self.assertEqual(status, 200)
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "resource_exhausted")
        self.assertIn("compact", meta["error"]["message"])
        self.assertNotIn("send a message to retry",
                         meta["error"]["message"])
        rec = self._records()[-1]
        self.assertEqual(rec["error_category"], "payload_budget")
        self.assertEqual(rec["rejection_origin"], "local_image_count")
        self.assertEqual(rec["payload"]["image_occurrences"], 41)
        # no image content reaches the request log
        self.assertNotIn("base64", self.dir.joinpath(
            "requests.jsonl").read_text())
        # nothing reserved, nothing admitted, no turn appended
        self.assertEqual(self._db1(self.cled,
                      "SELECT COUNT(*) FROM continuation_turns")[0], 0)
        self.assertEqual(self._db1(self.cled, "SELECT COUNT(*) FROM operations WHERE "
                      "status='executing'")[0], 0)

    def test_post_merge_preflight_rejects_and_retry_rereserves(self):
        status, data, send = self._post_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)])
        self.assertNotIn("error", self._trailer(data))
        self.assertEqual(send.call_count, 1)

        msgs = [self._msg(1, "one"),
                self._msg(2, "ans",
                          tool_calls=[("c1", "tool", "{}")]),
                self._msg(4, "res", call_id="c1"),
                self._msg(1, "two")]
        chat_body = self._chat(msgs)
        # unmerged size = the client's native history; merged adds the
        # turn-1 output reinsertion and must exceed the tight limit
        frames = wire.iter_frames(chat_body)
        packet = wire.decode(frames[0][1])
        req_body = translate.packet_to_responses_body(
            packet, translate.parse_routed_model("gpt-6-astra-high"), {})
        unmerged = len(json.dumps(req_body).encode())
        merged_body = dict(req_body, input=req_body["input"][:1] + OUT1
                           + req_body["input"][3:])
        merged = len(json.dumps(merged_body).encode())
        limit = max((unmerged + merged) // 2,
                    len(chat_body) * 4 // 3 + 1)
        self.assertLess(limit, merged)
        tight = dataclasses.replace(payload_budget.CODEX_PROFILE,
                                    max_serialized_bytes=limit)
        with patch.object(payload_budget, "profile_for",
                          return_value=tight), \
                patch.object(translate, "open_request") as send2:
            status, data2 = self._post(chat_body)
        send2.assert_not_called()
        meta = self._trailer(data2)
        self.assertEqual(meta["error"]["code"], "resource_exhausted")
        self.assertIn("compact", meta["error"]["message"])
        statuses = [r[0] for r in self._dba(
            self.cled, "SELECT status FROM operations WHERE "
            "operation_id != 'key-check'")]
        self.assertIn('preflight_rejected', statuses)
        self.assertNotIn('executing', statuses)
        self.assertEqual(self._db1(self.cled,
                      "SELECT COUNT(*) FROM continuation_turns")[0], 1)
        self.assertTrue(self._wait(lambda: len(self._records()) >= 2))
        rec = self._records()[-1]
        self.assertEqual(rec["error_category"], "payload_budget")
        self.assertEqual(rec["rejection_origin"],
                         "local_translated_bytes")

        # identical retry after relaxing the limit re-reserves and
        # succeeds — no 'continuation turn already reserved'
        status, data3, send3 = self._post_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT2)], body=chat_body)
        self.assertEqual(send3.call_count, 1)
        self.assertNotIn("error", self._trailer(data3))
        self.assertEqual(self._db1(self.cled,
                      "SELECT COUNT(*) FROM continuation_turns")[0], 2)

    def test_admission_failure_abandons_reservation_and_retry_succeeds(self):
        from fusion_relay.accounting import AccountingUnavailable
        with patch.object(relay, "_admit_accounting",
                          side_effect=AccountingUnavailable("down")), \
                patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat(self._one_turn()))
        send.assert_not_called()
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "failed_precondition")
        self.assertIn("accounting", meta["error"]["message"])
        statuses = [r[0] for r in self._dba(
            self.cled, "SELECT status FROM operations WHERE "
            "operation_id != 'key-check'")]
        self.assertEqual(statuses, ["admission_rejected"])
        self.assertTrue(self._wait(lambda: len(self._records()) >= 1))
        rec = self._records()[-1]
        self.assertEqual(rec["error_category"], "request_error")
        self.assertEqual(rec["continuation_status"], "admission_rejected")
        # identical retry re-reserves cleanly and commits
        status, data2, send2 = self._post_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT1)])
        self.assertEqual(send2.call_count, 1)
        self.assertNotIn("error", self._trailer(data2))
        self.assertEqual(self._db1(self.cled,
                      "SELECT COUNT(*) FROM continuation_turns")[0], 1)

    def test_upstream_image_rejection_classified_and_sanitized(self):
        import io as _io
        import urllib.error
        err = urllib.error.HTTPError(
            "u", 400, "Bad Request", hdrs=None,
            fp=_io.BytesIO(json.dumps({"error": {
                "message": "Too many images in the conversation",
                "code": "invalid_request_error"}}).encode()))
        with patch.object(translate, "open_request",
                          side_effect=err) as send:
            status, data = self._post(self._chat(self._one_turn()))
        self.assertEqual(send.call_count, 1)
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "resource_exhausted")
        self.assertIn("image_count", meta["error"]["message"])
        self.assertIn("compact", meta["error"]["message"])
        rec = self._records()[-1]  # waits for the log line to land
        log = self.dir.joinpath("requests.jsonl").read_text()
        self.assertNotIn("Too many images", log)
        self.assertEqual(rec["codex_http_status"], 400)
        self.assertEqual(rec["error_category"], "upstream_error")
        self.assertEqual(rec["rejection_origin"],
                         "upstream_image_count")

    def test_upstream_unknown_rejection_unavailable(self):
        import io as _io
        import urllib.error
        err = urllib.error.HTTPError(
            "u", 400, "Bad Request", hdrs=None,
            fp=_io.BytesIO(json.dumps(
                {"error": {"message": "unrelated failure"}}).encode()))
        with patch.object(translate, "open_request",
                          side_effect=err):
            status, data = self._post(self._chat(self._one_turn()))
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "unavailable")
        self.assertNotIn("unrelated failure",
                         meta["error"]["message"])
        rec = self._records()[-1]
        self.assertEqual(rec["rejection_origin"], "upstream_unknown")

    def test_capabilities_labels(self):
        conn = self._conn()
        conn.request("GET", f"/t/{self.TOKEN}/capabilities")
        caps = json.loads(conn.getresponse().read())
        conn.close()
        self.assertEqual(caps["continuation"], "durable_host_bound")
        self.assertEqual(caps["native_ack_contract"], "unavailable")
        with patch.object(relay, "_continuation_coordinator", None):
            conn = self._conn()
            conn.request("GET", f"/t/{self.TOKEN}/capabilities")
            caps = json.loads(conn.getresponse().read())
            conn.close()
        self.assertEqual(caps["continuation"],
                         "durable_host_binding_required")
        self.assertEqual(caps["host_ack_endpoint"],
                         "local_contract_only")
        self.assertEqual(caps["binding_issuer"], "none_native")


class CapabilityHandlerTest(_HandlerFixture, unittest.TestCase):
    """Durable mode with an authenticated capability binding."""

    def setUp(self):
        super().setUp()
        self.caps = CapabilityStore()
        self.coordinator = ContinuationCoordinator(
            self.cled, capabilities=self.caps,
            accept_provenance=frozenset(
                {PROVENANCE_NATIVE, PROVENANCE_LAUNCHER}))
        p = patch.object(relay, "_continuation_coordinator",
                         self.coordinator)
        self.addCleanup(p.stop)
        p.start()
        self._host = None
        self._secret = None

    def _issue(self, **over):
        args = dict(client_instance_id='c' * 64,
                    account_reference='acct-1', native_session_id='s1',
                    lane='lead', model_profile='gpt-6-astra:high',
                    continuation_epoch='e1',
                    provenance=PROVENANCE_NATIVE, ttl_s=3600)
        args.update(over)
        self._host, self._secret = self.caps.issue(**args)
        return self._host, self._secret

    def _req_body(self, body):
        frames = wire.iter_frames(body)
        packet = wire.decode(frames[0][1])
        return translate.packet_to_responses_body(
            packet, translate.parse_routed_model("gpt-6-astra-high"),
            {})

    def _headers(self, secret, capability_id, operation_id, req_body):
        from fusion_relay.continuation import digest
        from fusion_relay.host_binding import CapabilityStore
        body_digest = digest(req_body)
        return {"Content-Type": "application/connect+proto",
                "X-Fusion-Capability": capability_id,
                "X-Fusion-Operation": operation_id,
                "X-Fusion-Proof": CapabilityStore.prove(
                    secret, capability_id, operation_id, body_digest)}

    def _post_bound(self, body, operation_id="op-1", headers=None):
        if headers is None:
            self._issue()
            headers = self._headers(self._secret,
                                    self._host.capability_id,
                                    operation_id, self._req_body(body))
        conn = self._conn()
        conn.request("POST", f"/t/{self.TOKEN}{self.RPC}",
                     body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        self._last_resp_headers = {k.lower(): v
                                   for k, v in resp.getheaders()}
        conn.close()
        return resp.status, data

    def _post_bound_codex(self, events, body=None,
                          operation_id="op-1", headers=None):
        fake = FakeSSE(events)
        with patch.object(translate, "open_request",
                          return_value=fake) as send:
            status, data = self._post_bound(
                body or self._chat(self._one_turn()),
                operation_id=operation_id, headers=headers)
        return status, data, send

    # -- capability enforcement -------------------------------------
    def test_valid_lead_capability_succeeds(self):
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)])
        self.assertEqual(send.call_count, 1)
        self.assertNotIn("error", self._trailer(data))
        self.assertTrue(self._wait(lambda: self._records()))
        rec = self._records()[-1]
        self.assertEqual(rec["binding_status"], "verified")
        self.assertEqual(rec["continuation_role"], "lead")
        self.assertEqual(rec["acceptance"], "pending")

    def test_no_headers_binding_unavailable(self):
        with patch.object(translate, "open_request") as send:
            status, data = self._post(self._chat(self._one_turn()))
        send.assert_not_called()
        meta = self._trailer(data)
        self.assertEqual(meta["error"]["code"], "failed_precondition")
        self.assertIn("trusted continuation binding unavailable",
                      meta["error"]["message"])

    def test_sidekick_capability_denied(self):
        self._issue(lane='sidekick')
        headers = self._headers(
            self._secret, self._host.capability_id, "op-1",
            self._req_body(self._chat(self._one_turn())))
        with patch.object(translate, "open_request") as send:
            status, data = self._post_bound(
                self._chat(self._one_turn()), headers=headers)
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")
        rec = self._records()[-1]
        self.assertEqual(rec["binding_status"], "invalid")

    def test_unknown_expired_revoked_bumped(self):
        import secrets as _s
        # unknown capability id
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        headers = self._headers(_s.token_bytes(32), 'a' * 64, "op-1",
                                req)
        with patch.object(translate, "open_request") as send:
            _, data = self._post_bound(body, headers=headers)
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")
        # revoked
        self._issue()
        self.caps.revoke(self._host.capability_id)
        headers = self._headers(self._secret, self._host.capability_id,
                                "op-1", req)
        with patch.object(translate, "open_request") as send:
            _, data = self._post_bound(body, headers=headers)
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")
        # bumped generation
        self._issue()
        self.caps.bump_generation('c' * 64)
        headers = self._headers(self._secret, self._host.capability_id,
                                "op-1", req)
        with patch.object(translate, "open_request") as send:
            _, data = self._post_bound(body, headers=headers)
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")
        # expired
        clock = [1000.0]
        store = type(self.caps)(clock=lambda: clock[0])
        host, secret = store.issue(
            client_instance_id='c' * 64, account_reference='acct-1',
            native_session_id='s1', lane='lead',
            model_profile='gpt-6-astra:high', continuation_epoch='e1',
            provenance='native_hook_verified', ttl_s=10)
        coord = ContinuationCoordinator(self.cled, capabilities=store)
        with patch.object(relay, "_continuation_coordinator", coord):
            clock[0] += 100
            headers = self._headers(secret, host.capability_id, "op-1",
                                    req)
            with patch.object(translate, "open_request") as send:
                _, data = self._post_bound(body, headers=headers)
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")

    def test_cross_session_account_profile_reject_before_reserve(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        for over in ({'native_session_id': 'other'},
                     {'account_reference': 'acct-2'},
                     {'model_profile': 'gpt-6-astra:low'}):
            self._issue(**over)
            headers = self._headers(
                self._secret, self._host.capability_id, "op-1", req)
            with patch.object(translate, "open_request") as send:
                _, data = self._post_bound(body, headers=headers)
            send.assert_not_called()
            self.assertEqual(self._trailer(data)["error"]["code"],
                             "permission_denied")
        self.assertEqual(self._db1(self.cled, "SELECT COUNT(*) FROM operations WHERE "
                      "scope != 'key-check'")[0], 0)

    def test_malformed_headers_denied(self):
        body = self._chat(self._one_turn())
        with patch.object(translate, "open_request") as send:
            _, data = self._post_bound(
                body, headers={"Content-Type": "application/connect+proto",
                               "X-Fusion-Capability": "a" * 64})
        send.assert_not_called()
        self.assertEqual(self._trailer(data)["error"]["code"],
                         "permission_denied")

    def test_replay_and_conflicts(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)], body=body, headers=headers)
        self.assertEqual(send.call_count, 1)
        # same capability + op + body: replay, no provider call
        status, data2, send2 = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1})],
            body=body, headers=headers)
        self.assertEqual(send2.call_count, 0)
        self.assertEqual(data2, data)
        # same op id, changed body: conflict -> failed_precondition
        other_body = self._chat(self._one_turn(), session="s1")
        other_req = self._req_body(other_body)
        # make it genuinely different
        other_body = self._chat(
            [self._msg(1, "different text")])
        other_req = self._req_body(other_body)
        headers2 = self._headers(secret, host.capability_id, "op-1",
                                 other_req)
        with patch.object(translate, "open_request") as send3:
            _, data3 = self._post_bound(other_body, headers=headers2)
        send3.assert_not_called()
        self.assertEqual(self._trailer(data3)["error"]["code"],
                         "failed_precondition")
        # distinct op id, identical body: anchor/history check fails —
        # an intentional repeat needs the client history to include the
        # first answer; a bare identical body never silently replays.
        headers3 = self._headers(secret, host.capability_id, "op-2", req)
        with patch.object(translate, "open_request") as send4:
            _, data4 = self._post_bound(body, headers=headers3)
        send4.assert_not_called()
        meta = self._trailer(data4)
        self.assertEqual(meta["error"]["code"], "failed_precondition")
        self.assertIn("epoch", meta["error"]["message"])

    # -- /host/ack ----------------------------------------------------
    def _ack(self, host, secret, ack):
        conn = self._conn()
        body = json.dumps(ack).encode()
        from fusion_relay.continuation import digest
        from fusion_relay.host_binding import CapabilityStore
        body_digest = digest(ack)
        conn.request("POST", f"/t/{self.TOKEN}/host/ack", body=body,
                     headers={"Content-Type": "application/json",
                              "X-Fusion-Capability": host.capability_id,
                              "X-Fusion-Operation": ack["operation_id"],
                              "X-Fusion-Proof": CapabilityStore.prove(
                                  secret, host.capability_id,
                                  ack["operation_id"], body_digest)})
        resp = conn.getresponse()
        out = resp.status, json.loads(resp.read())
        conn.close()
        return out

    def _ack_payload(self, host, operation_id="op-1", **over):
        from fusion_relay.continuation import ContinuationBinding
        binding = host.continuation_binding()
        turn = self._db1(self.cled,
            'SELECT revision, response_digest FROM continuation_turns '
            'WHERE scope=? AND operation_id=?',
            (binding.scope(), operation_id))
        ack = {"protocol_version": 1,
               "client_instance_id": host.client_instance_id,
               "session_id": host.native_session_id, "lane": host.lane,
               "operation_id": operation_id,
               "continuation_epoch": host.continuation_epoch,
               "revision": turn[0] if turn else 1,
               "response_digest": turn[1] if turn else '0' * 64,
               "consumer_commit_reference": "ref-1"}
        ack.update(over)
        return ack

    def test_ack_lifecycle(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)], body=body, headers=headers)
        self.assertNotIn("error", self._trailer(data))
        self.assertTrue(self._wait(lambda: self._records()))
        from fusion_relay import diagnostics
        from fusion_relay.accounting import reference
        session_ref = reference("session", "s1")
        self.assertEqual(diagnostics.snapshot(session_ref)
                         ["acceptance"], "pending")
        ack = self._ack_payload(host)
        status, out = self._ack(host, secret, ack)
        self.assertEqual(status, 200)
        self.assertEqual(out, {"acceptance": "acknowledged",
                               "idempotent": False})
        self.assertEqual(diagnostics.snapshot(session_ref)
                         ["acceptance"], "acknowledged")
        status, out = self._ack(host, secret, ack)
        self.assertEqual((status, out["idempotent"]), (200, True))
        changed = dict(ack, response_digest='0' * 64)
        self.assertEqual(self._ack(host, secret, changed)[0], 409)
        stale = dict(ack, revision=99)
        self.assertEqual(self._ack(host, secret, stale)[0], 409)
        never = self._ack_payload(host, operation_id="op-x",
                                  revision=1, response_digest='0' * 64)
        self.assertEqual(self._ack(host, secret, never)[0], 409)
        side, side_secret = self._issue(lane='sidekick')
        side_ack = dict(ack, client_instance_id=side.client_instance_id,
                        lane='sidekick')
        status, out = self._ack(side, side_secret, side_ack)
        self.assertEqual(status, 403)

    # -- /host/compaction ----------------------------------------------
    def _post_compaction(self, payload):
        conn = self._conn()
        conn.request("POST", f"/t/{self.TOKEN}/host/compaction",
                     body=json.dumps(payload).encode(),
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        out = resp.status, resp.read()
        conn.close()
        return out

    def test_compaction_endpoint_validation(self):
        self.assertEqual(self._post_compaction(
            {"protocol_version": 1, "session_id": "s1",
             "summary_present": True})[0], 200)
        # missing key -> 400
        self.assertEqual(self._post_compaction(
            {"protocol_version": 1, "session_id": "s1"})[0], 400)
        # a summary key is always rejected
        self.assertEqual(self._post_compaction(
            {"protocol_version": 1, "session_id": "s1",
             "summary_present": True, "summary": "SECRET"})[0], 400)
        log = self.dir.joinpath("requests.jsonl")
        if log.exists():
            self.assertNotIn("SECRET", log.read_text())

    def test_compaction_enables_epoch_transition(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)], body=body, headers=headers)
        self.assertNotIn("error", self._trailer(data))
        # client history no longer matches stored anchors (compacted)
        new_msgs = [self._msg(1, "compacted summary"), self._msg(1, "two")]
        body2 = self._chat(new_msgs)
        req2 = self._req_body(body2)
        headers2 = self._headers(secret, host.capability_id, "op-2",
                                 req2)
        # without a compaction note: fail closed
        with patch.object(translate, "open_request") as send2:
            _, data2 = self._post_bound(body2, headers=headers2)
        send2.assert_not_called()
        self.assertEqual(self._trailer(data2)["error"]["code"],
                         "failed_precondition")
        # note the compaction (hook session id correlates here — in
        # production the correlation stays unverified)
        self.assertEqual(self._post_compaction(
            {"protocol_version": 1, "session_id": "s1",
             "summary_present": True})[0], 200)
        status, data3, send3 = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT2)], body=body2, headers=headers2)
        self.assertEqual(send3.call_count, 1)
        self.assertNotIn("error", self._trailer(data3))
        # recs: turn 1, fail-closed turn 2, compaction note, turn 3
        self.assertTrue(self._wait(lambda: len(self._records()) >= 4))
        row = self._db1(self.cled, 'SELECT reason FROM epoch_transitions')
        self.assertEqual(row[0], 'history_compaction')
        # new epoch reference differs from the old
        from fusion_relay.accounting import reference
        recs = self._records()
        codex_recs = [r for r in recs if r.get("route") == "codex"]
        refs = {r.get("continuation_epoch_ref") for r in codex_recs
                if r.get("continuation_epoch_ref")}
        self.assertEqual(len(refs), 2)

    def test_compaction_correlated_through_verified_marker(self):
        """The hook's session_id is the session NAME, not the wire seed;
        a verified UserPromptSubmit marker carries that name inside the
        packet, so the compaction note can be matched honestly."""
        from fusion_relay import marker as marker_mod
        secret_key = self.server.identity.secret
        m1 = marker_mod.render(secret_key, "frost-dust", "11111111-aaaa")
        body = self._chat([self._msg(1, m1), self._msg(1, "one")])
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)], body=body, headers=headers)
        self.assertNotIn("error", self._trailer(data))
        self.assertTrue(self._wait(lambda: len(self._records()) >= 1))
        self.assertEqual(self._records()[-1]["marker_status"], "verified")
        # compaction note keyed by the session NAME (as the hook sends)
        self.assertEqual(self._post_compaction(
            {"protocol_version": 1, "session_id": "frost-dust",
             "summary_present": True})[0], 200)
        # compacted history under the same seed, new turn's marker
        m2 = marker_mod.render(secret_key, "frost-dust", "22222222-bbbb")
        body2 = self._chat([self._msg(1, "compacted summary"),
                            self._msg(1, m2), self._msg(1, "two")])
        req2 = self._req_body(body2)
        headers2 = self._headers(secret, host.capability_id, "op-2", req2)
        status, data2, send2 = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT2)], body=body2, headers=headers2)
        self.assertEqual(send2.call_count, 1)
        self.assertNotIn("error", self._trailer(data2))
        self.assertEqual(self._db1(
            self.cled, 'SELECT reason FROM epoch_transitions')[0],
            'history_compaction')
        self.assertTrue(self._wait(lambda: len(self._records()) >= 3))
        rec = [r for r in self._records() if r.get("route") == "codex"][-1]
        self.assertEqual(rec["epoch_transition"], "history_compaction")
        self.assertEqual(rec["compaction_correlation"], "matched_marker")
        from fusion_relay import diagnostics
        from fusion_relay.accounting import reference
        snap = diagnostics.snapshot(reference("session", "s1"))
        self.assertEqual(snap["compaction_correlation"], "matched_marker")
        self.assertEqual(snap["marker"], "verified")
        # a forged marker (wrong key) is reported invalid, never verified
        forged = marker_mod.render(b"z" * 32, "frost-dust", "33333333-cccc")
        body3 = self._chat([self._msg(1, forged), self._msg(1, "x")],
                           session="s-forged")
        req3 = self._req_body(body3)
        host3, secret3 = self._issue(native_session_id="s-forged")
        headers3 = self._headers(secret3, host3.capability_id, "op-3",
                                 req3)
        status, data3, _ = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT1)], body=body3, headers=headers3)
        self.assertTrue(self._wait(lambda: len(self._records()) >= 4))
        self.assertEqual(self._records()[-1]["marker_status"], "invalid")

    def test_compaction_chain_continues_and_ack_new_epoch(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        status, data, send = self._post_bound_codex(
            [completed({"input_tokens": 7, "output_tokens": 3},
                       output=OUT1)], body=body, headers=headers)
        self.assertNotIn("error", self._trailer(data))
        epoch1 = self._last_resp_headers["x-fusion-continuation-epoch"]
        self.assertEqual(epoch1, "e1")

        self._post_compaction({"protocol_version": 1,
                               "session_id": "s1",
                               "summary_present": True})
        # compacted client history, same capability (old epoch)
        body2 = self._chat([self._msg(1, "compacted summary"),
                            self._msg(1, "two")])
        headers2 = self._headers(secret, host.capability_id, "op-2",
                                 self._req_body(body2))
        status, data2, send2 = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=OUT2)], body=body2, headers=headers2)
        self.assertNotIn("error", self._trailer(data2))
        epoch2 = self._last_resp_headers[
            "x-fusion-continuation-epoch"]
        self.assertNotEqual(epoch2, epoch1)
        self.assertEqual(self._last_resp_headers[
            "x-fusion-continuation-revision"], "1")

        # third request under the SAME capability follows the epoch
        # chain: compacted history + visible projection + new user msg
        body3 = self._chat([self._msg(1, "compacted summary"),
                            self._msg(1, "two"),
                            self._msg(2, "done"),
                            self._msg(1, "three")])
        headers3 = self._headers(secret, host.capability_id, "op-3",
                                 self._req_body(body3))
        status, data3, send3 = self._post_bound_codex(
            [completed({"input_tokens": 1, "output_tokens": 1},
                       output=[{'type': 'message', 'id': 'm3',
                                'content': [{'type': 'output_text',
                                             'text': 'three-ans'}]}])],
            body=body3, headers=headers3)
        self.assertEqual(send3.call_count, 1)
        self.assertNotIn("error", self._trailer(data3))
        self.assertEqual(self._last_resp_headers[
            "x-fusion-continuation-epoch"], epoch2)
        self.assertEqual(self._last_resp_headers[
            "x-fusion-continuation-revision"], "2")
        # exactly one transition was recorded
        self.assertEqual(self._db1(self.cled, 'SELECT COUNT(*) FROM epoch_transitions')[0], 1)

        # ack the transitioned turn with the NEW epoch -> 200
        binding = host.continuation_binding()
        effective = self.cled.resolve_epoch(binding)
        self.assertEqual(effective.epoch, epoch2)
        turn = self._db1(self.cled,
            'SELECT revision, response_digest FROM continuation_turns '
            'WHERE scope=? AND operation_id=?',
            (effective.scope(), 'op-2'))
        ack = {"protocol_version": 1,
               "client_instance_id": host.client_instance_id,
               "session_id": host.native_session_id, "lane": host.lane,
               "operation_id": "op-2", "continuation_epoch": epoch2,
               "revision": turn[0], "response_digest": turn[1],
               "consumer_commit_reference": "ref-1"}
        # ack with the OLD epoch is never accepted
        old_ack = dict(ack, continuation_epoch="e1")
        status, _ = self._ack(host, secret, old_ack)
        self.assertNotEqual(status, 200)
        # op-2 was later extended by the third request, so it carries
        # history evidence — but a rejected ack must never make it
        # 'acknowledged'
        self.assertEqual(
            self.cled.acceptance_state(effective, 'op-2'),
            'history_evidenced')
        status, out = self._ack(host, secret, ack)
        self.assertEqual(status, 200)
        self.assertEqual(
            self.cled.acceptance_state(effective, 'op-2'),
            'acknowledged')

    def test_offer_failure_keeps_commit_and_succeeds(self):
        body = self._chat(self._one_turn())
        req = self._req_body(body)
        host, secret = self._issue()
        headers = self._headers(secret, host.capability_id, "op-1", req)
        with patch.object(self.cled, "offer_result",
                          side_effect=ContinuationError("offer lost")):
            status, data, send = self._post_bound_codex(
                [completed({"input_tokens": 7, "output_tokens": 3},
                           output=OUT1)], body=body, headers=headers)
        self.assertEqual(send.call_count, 1)
        self.assertNotIn("error", self._trailer(data))
        row = self._db1(self.cled,
            "SELECT state FROM continuation_turns WHERE operation_id="
            "'op-1'")
        self.assertEqual(row[0], 'result_committed')
        self.assertEqual(self._db1(self.cled, "SELECT status FROM operations WHERE "
                      "operation_id='op-1'")[0], 'succeeded')

    def test_post_compaction_hook_script(self):
        import os
        import subprocess
        script = (pathlib.Path(__file__).resolve().parent.parent
                  / "bin" / "fusion-post-compaction")
        token_file = self.dir / "relay-token"
        token_file.write_text(self.TOKEN + "\n")
        os.chmod(token_file, 0o600)
        env = dict(os.environ,
                   FUSION_RELAY_DATA_DIR=str(self.dir),
                   FUSION_RELAY_PORT=str(self.port))
        proc = subprocess.run(
            ["python3", str(script)],
            input=json.dumps({"session_id": "hook-session-1",
                              "prompt_id": "p1",
                              "summary": "FAKE-SUMMARY-SECRET"}),
            capture_output=True, text=True, env=env, timeout=10)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout, "")
        # the relay saw a valid note for the hashed session only — the
        # summary text never left the hook payload
        self.assertTrue(self._wait(lambda: any(
            r.get("rpc") == "HostCompaction" for r in self._records())))
        log = self.dir.joinpath("requests.jsonl").read_text()
        self.assertNotIn("FAKE-SUMMARY-SECRET", log)
        from fusion_relay import diagnostics
        from fusion_relay.accounting import reference
        entry = diagnostics.snapshot(
            reference("session", "hook-session-1"))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["compaction_events"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
