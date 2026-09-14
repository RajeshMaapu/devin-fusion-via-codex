"""Remediation slice 2: durable operation journal, request cancellation,
usage recording on terminal events, and continuity-scope partitioning."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from fusion_relay import cua, lifecycle, operations, relay, translate, wire
from fusion_relay.translate import (IncompleteResponse, UnsupportedRequest,
                                    packet_to_responses_body,
                                    parse_routed_model)


def _cua_call(cid="c1", args="{}"):
    return {"type": "function_call", "call_id": cid,
            "name": translate.CUA_TOOL_NAME, "arguments": args}


def _tail_for(items):
    msg = wire.field(1, "r1")
    for it in items:
        if it.get("type") == "function_call":
            msg += wire.field(6, wire.field(1, it["call_id"])
                              + wire.field(2, it["name"])
                              + wire.field(3, it.get("arguments", "")))
    msg += wire.field(5, 10 if any(
        i.get("type") == "function_call" for i in items) else 1)
    return wire.frame(msg) + wire.end_stream()


class _Queued:
    """Patch translate.call_codex with per-iteration canned outputs."""

    def __init__(self, test, outputs):
        self.calls = []
        self.outputs = outputs
        self._orig = translate.call_codex
        test.addCleanup(self._restore)

    def fake(self, body, rec, on_delta=None, timeout=0, _items_out=None,
             **_ignored):
        self.calls.append(json.loads(json.dumps(body)))
        items = self.outputs[len(self.calls) - 1]
        if _items_out is not None:
            _items_out.extend(items)
        return _tail_for(items)

    def __enter__(self):
        translate.call_codex = self.fake
        return self

    def __exit__(self, *a):
        return False

    def _restore(self):
        translate.call_codex = self._orig


class OperationJournalTest(unittest.TestCase):
    def setUp(self):
        translate.reset()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.db = pathlib.Path(self._tmpdir.name) / "ops.db"
        self.journal = operations.OperationJournal(self.db)
        self.addCleanup(lambda: self.journal.close())
        self._ctx = {"journal": self.journal, "operation_scope": "s1"}

    def test_permissions_and_schema(self):
        import stat
        self.assertEqual(stat.S_IMODE(os.stat(self.db).st_mode), 0o600)
        self.assertEqual(
            stat.S_IMODE(os.stat(self.db.parent).st_mode), 0o700)

    def test_same_scope_and_id_executes_once_across_calls(self):
        ran = []
        ctx = self._ctx
        for _ in range(2):
            with _Queued(self, [[_cua_call()], []]):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(1) or "OK", **ctx)
        self.assertEqual(ran, [1])
        self.assertEqual(self.journal.lookup("s1", "c1")["result"], "OK")

    def test_journal_survives_restart(self):
        ran = []
        with _Queued(self, [[_cua_call()], []]):
            translate.call_codex_with_tools(
                {"prompt_cache_key": "k", "input": []}, {},
                executor=lambda c, r: ran.append(1) or "OK", **self._ctx)
        self.journal.close()
        self.journal = operations.OperationJournal(self.db)
        with _Queued(self, [[_cua_call()], []]):
            translate.call_codex_with_tools(
                {"prompt_cache_key": "k", "input": []}, {},
                executor=lambda c, r: ran.append(1) or "OK",
                journal=self.journal, operation_scope="s1")
        self.assertEqual(ran, [1])

    def test_different_scope_isolated(self):
        ran = []
        for scope in ("s1", "s2"):
            with _Queued(self, [[_cua_call()], []]):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(scope) or "OK",
                    journal=self.journal, operation_scope=scope)
        self.assertEqual(ran, ["s1", "s2"])

    def test_conflicting_fingerprint_refused_after_restart(self):
        with _Queued(self, [[_cua_call()], []]):
            translate.call_codex_with_tools(
                {"prompt_cache_key": "k", "input": []}, {},
                executor=lambda c, r: "OK", **self._ctx)
        self.journal.close()
        self.journal = operations.OperationJournal(self.db)
        with _Queued(self, [[_cua_call("c1", '{"other":1}')], []]):
            with self.assertRaises(UnsupportedRequest):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: "OK",
                    journal=self.journal, operation_scope="s1")

    def test_thrown_executor_marks_outcome_unknown_and_no_retry(self):
        def boom(c, r):
            raise RuntimeError("executor exploded")
        with _Queued(self, [[_cua_call()], []]):
            with self.assertRaises(IncompleteResponse):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=boom, **self._ctx)
        row = self.journal.lookup("s1", "c1")
        self.assertEqual(row["status"], "outcome_unknown")
        ran = []
        with _Queued(self, [[_cua_call()], []]):
            with self.assertRaises(IncompleteResponse):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(1) or "OK",
                    **self._ctx)
        self.assertEqual(ran, [])

    def test_executing_row_reports_outcome_unknown(self):
        import hashlib
        fp = hashlib.sha256(json.dumps(["n", "{}"]).encode()).hexdigest()
        self.journal._db.execute(
            "INSERT INTO operations(scope,operation_id,fingerprint,status) "
            "VALUES('s1','cx',?,'executing')", (fp,))
        self.assertEqual(
            self.journal.lookup("s1", "cx")["status"], "outcome_unknown")
        with self.assertRaises(IncompleteResponse):
            self.journal.run("s1", {"call_id": "cx", "name": "n",
                                    "arguments": "{}"},
                             lambda: "x", lambda: None)

    def test_repeated_id_next_iteration_uses_cached_result(self):
        ran = []
        calls = []
        with _Queued(self, [[_cua_call("c1")], [_cua_call("c1")], []]) as q:
            translate.call_codex_with_tools(
                {"prompt_cache_key": "k", "input": []}, {},
                executor=lambda c, r: ran.append(1) or "OK",
                max_loops=4, **self._ctx)
        self.assertEqual(ran, [1])  # one action total
        third_input = q.calls[2]["input"]
        outputs = [i.get("output") for i in third_input
                   if i.get("type") == "function_call_output"
                   and i.get("call_id") == "c1"]
        self.assertEqual(outputs, ["OK"])  # exactly one pair per call_id

    def test_cancel_after_execution_keeps_result_no_new_action(self):
        ctx = lifecycle.RequestContext()
        ran = []

        def execute_then_cancel(c, r):
            ran.append(1)
            ctx.cancel()
            return "OK"

        with _Queued(self, [[_cua_call()], []]):
            with self.assertRaises(lifecycle.RequestCancelled):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=execute_then_cancel,
                    journal=self.journal, operation_scope="s1",
                    check_cancelled=ctx.check)
        self.assertEqual(ran, [1])
        self.assertEqual(self.journal.lookup("s1", "c1")["status"],
                         "succeeded")

    def test_intent_persistence_failure_blocks_action(self):
        self.journal._db.close()
        self.db.unlink()
        ran = []
        with self.assertRaises(Exception):
            self.journal.run("s1", _cua_call(),
                             lambda: ran.append(1) or "OK", lambda: None)
        self.assertEqual(ran, [])

    def test_failed_outcome_write_leaves_executing(self):
        class BadJournal(operations.OperationJournal):
            def _finish(self, *a):
                raise OSError("disk gone")
        self.journal.close()
        self.journal = BadJournal(self.db.parent / "ops2.db")
        ran = []
        with self.assertRaises(OSError):
            self.journal.run("s1", _cua_call(),
                             lambda: ran.append(1) or "OK", lambda: None)
        self.assertEqual(ran, [1])
        self.journal.close()
        self.journal = operations.OperationJournal(self.db.parent
                                                   / "ops2.db")
        self.assertEqual(self.journal.lookup("s1", "c1")["status"],
                         "outcome_unknown")

    def test_oversized_result_marks_outcome_unknown(self):
        with self.assertRaises(IncompleteResponse):
            self.journal.run("s1", _cua_call(),
                             lambda: "x" * (operations.MAX_RESULT_BYTES + 1),
                             lambda: None)
        self.assertEqual(self.journal.lookup("s1", "c1")["status"],
                         "outcome_unknown")

    def test_malformed_arguments_rejected_pre_actuator(self):
        ran = []
        for bad in ("not json{", '"[1]"', '"null"'):
            with _Queued(self, [[_cua_call("c1", bad)], []]):
                with self.assertRaises(UnsupportedRequest):
                    translate.call_codex_with_tools(
                        {"prompt_cache_key": "k", "input": []}, {},
                        executor=lambda c, r: ran.append(1) or "OK",
                        **self._ctx)
        self.assertEqual(ran, [])
        # Journal-level validation matches for direct callers.
        for bad in ("not json{", '"[1]"', '"null"'):
            self.assertRaises(
                UnsupportedRequest, self.journal.run, "s1",
                {"call_id": "cx", "name": "n", "arguments": bad},
                lambda: "x", lambda: None)

    def test_changed_args_later_iteration_rejected(self):
        ran = []
        with _Queued(self, [[_cua_call("c1")],
                            [_cua_call("c1", '{"code":"other"}')], []]):
            with self.assertRaises(UnsupportedRequest):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(1) or "OK",
                    max_loops=4, **self._ctx)
        self.assertEqual(ran, [1])  # one prior effect, none additional

    def test_native_collision_with_executed_id_rejected(self):
        ran = []
        native = {"type": "function_call", "call_id": "c1",
                  "name": "native_computer", "arguments": "{}"}
        with _Queued(self, [[_cua_call("c1")], [native], []]):
            with self.assertRaises(UnsupportedRequest):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(1) or "OK",
                    max_loops=4, **self._ctx)
        self.assertEqual(ran, [1])

    def test_concurrent_same_id_single_dispatch(self):
        import threading as _t
        ran, outcomes, errors = [], [], []
        entered = _t.Event()
        release = _t.Event()
        call = {"call_id": "c9", "name": translate.CUA_TOOL_NAME,
                "arguments": "{}"}

        def execute():
            entered.set()
            release.wait(5)
            ran.append(1)
            return "OK"

        def attempt():
            try:
                self.journal.run("s1", call, execute, lambda: None)
                outcomes.append("ok")
            except IncompleteResponse:
                outcomes.append("unknown")
            except Exception as e:
                errors.append(e)

        owner = _t.Thread(target=attempt)
        owner.start()
        self.assertTrue(entered.wait(5))
        contenders = [_t.Thread(target=attempt) for _ in range(3)]
        for t in contenders:
            t.start()
        try:
            for t in contenders:
                t.join(10)
            for t in contenders:
                self.assertFalse(t.is_alive())
            self.assertEqual(outcomes, ["unknown"] * 3)
        finally:
            release.set()
        owner.join(10)
        self.assertFalse(owner.is_alive())
        self.assertFalse(errors)
        self.assertEqual(sorted(outcomes), ["ok", "unknown", "unknown",
                                            "unknown"])
        self.assertEqual(ran, [1])

    def test_many_scopes_isolated(self):
        for i in range(70):
            scope = f"scope-{i}"
            self.journal.run(scope, _cua_call("c1"),
                             lambda: f"r{i}", lambda: None)
        for i in range(70):
            self.assertEqual(
                self.journal.lookup(f"scope-{i}", "c1")["result"], f"r{i}")

    def test_missing_scope_blocks_before_inference(self):
        with _Queued(self, [[_cua_call()], []]) as q:
            for kwargs in ({"journal": self.journal},
                           {"operation_scope": "s1"},
                           {}):
                with self.assertRaises(UnsupportedRequest):
                    translate.call_codex_with_tools(
                        {"prompt_cache_key": "k", "input": []}, {},
                        executor=lambda c, r: "OK", **kwargs)
        self.assertEqual(q.calls, [])

    def test_cancel_after_inference_zero_actions(self):
        ctx = lifecycle.RequestContext()
        ran = []

        with _Queued(self, [[_cua_call()], []]) as q:
            def fake(body, rec, on_delta=None, timeout=0, _items_out=None,
                     **kw):
                out = q.fake(body, rec, on_delta, timeout, _items_out)
                ctx.cancel()
                return out
            translate.call_codex = fake
            with self.assertRaises(lifecycle.RequestCancelled):
                translate.call_codex_with_tools(
                    {"prompt_cache_key": "k", "input": []}, {},
                    executor=lambda c, r: ran.append(1) or "OK",
                    journal=self.journal, operation_scope="s1",
                    check_cancelled=ctx.check)
        self.assertEqual(ran, [])
        self.assertIsNone(self.journal.lookup("s1", "c1"))

    def test_journal_cancel_before_dispatch_marks_cancelled(self):
        calls = [0]

        def check():
            calls[0] += 1
            if calls[0] >= 2:
                raise lifecycle.RequestCancelled("request_cancelled")

        ran = []
        with self.assertRaises(lifecycle.RequestCancelled):
            self.journal.run("s1", _cua_call(),
                             lambda: ran.append(1) or "OK", check)
        self.assertEqual(ran, [])
        self.assertEqual(self.journal.lookup("s1", "c1")["status"],
                         "cancelled")


class RequestContextTest(unittest.TestCase):
    def test_cancel_flag(self):
        ctx = lifecycle.RequestContext()
        ctx.check()
        ctx.cancel()
        with self.assertRaises(lifecycle.RequestCancelled):
            ctx.check()

    def test_deadline(self):
        ctx = lifecycle.RequestContext(timeout=-1)
        with self.assertRaises(lifecycle.RequestCancelled):
            ctx.check()

    def test_disconnect_probe(self):
        ctx = lifecycle.RequestContext(disconnected=lambda: True)
        with self.assertRaises(lifecycle.RequestCancelled):
            ctx.check()

    def test_client_gone_is_cancellation(self):
        self.assertTrue(issubclass(translate.ClientGone,
                                   lifecycle.RequestCancelled))


class CuaBlockedTest(unittest.TestCase):
    def _provider(self, d):
        return cua.CuaProvider(pathlib.Path(d))

    def test_execute_blocked_without_spawn(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._provider(d)
            p._ensure = Mock()
            with self.assertRaises(cua.CuaUnavailable) as ctx:
                p.execute("code", "t", 1000, {})
            self.assertIn("computer_policy_denied", str(ctx.exception))
            p._ensure.assert_not_called()

    def test_unknown_elicitation_method_gets_protocol_error(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._provider(d)
            p._send = Mock()
            p._answer_elicitation({"jsonrpc": "2.0", "id": 7,
                                   "method": "elicitation/unknown"})
        sent = p._send.call_args[0][0]
        self.assertEqual(sent["error"]["code"], -32601)

    def test_repeated_elicitations_all_declined_no_cross_approval(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._provider(d)
            p._send = Mock()
            for rid in (1, 2):
                p._answer_elicitation({"jsonrpc": "2.0", "id": rid,
                                       "method": "elicitation/create",
                                       "params": {}})
            for call in p._send.call_args_list:
                self.assertEqual(call[0][0]["result"],
                                 {"action": "decline"})

    def test_kill_retains_proc_when_still_alive(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._provider(d)
            proc = Mock()
            proc.terminate.side_effect = OSError("gone fd")
            proc.poll.return_value = None  # still running
            p._proc = proc
            with self.assertRaises(cua.CuaUnavailable):
                p._kill()
            self.assertIs(p._proc, proc)

    def test_kill_escalates_on_terminate_timeout(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._provider(d)
            proc = Mock()
            proc.wait.side_effect = [subprocess.TimeoutExpired("x", 2), None]
            proc.poll.return_value = 0
            p._proc = proc
            p._kill()
            proc.kill.assert_called_once()
            self.assertIsNone(p._proc)


class UsageRecordingTest(unittest.TestCase):
    def setUp(self):
        translate.reset()
        self._auth = translate.auth.get_token
        translate.auth.get_token = lambda: ("tok", "acct")
        self.addCleanup(self._restore)

    def _restore(self):
        translate.auth.get_token = self._auth

    def _run_into(self, rec, events):
        import urllib.request
        from test_relay import _FakeSSE
        fake = _FakeSSE(events)
        orig = urllib.request.urlopen
        urllib.request.urlopen = lambda *a, **k: fake
        try:
            return translate.call_codex({"prompt_cache_key": "k"}, rec)
        finally:
            urllib.request.urlopen = orig

    def test_unknown_usage_omits_wire_field7(self):
        msg = translate._final_message(
            {"id": "r", "output": []}, [], {})
        self.assertNotIn(7, wire.decode(msg))

    def test_wire_usage_is_final_response_not_aggregate(self):
        rec = {}
        first = {"id": "one", "usage": {"input_tokens": 10,
                                        "output_tokens": 2}}
        second = {"id": "two", "usage": {"input_tokens": 20,
                                         "output_tokens": 3}}
        translate._record_usage(first, rec)
        translate._record_usage(second, rec)
        self.assertEqual(rec["codex_usage"]["input_tokens"], 30)
        decoded = wire.decode(translate._final_message(second, [], {}))
        usage = wire.decode(decoded[7][0])
        self.assertEqual((usage[2][0], usage[3][0]), (20, 3))

    def test_incomplete_and_failed_record_usage(self):
        rec_inc = {}
        with self.assertRaises(IncompleteResponse):
            self._run_into(rec_inc, [{"type": "response.incomplete",
                                      "response": {
                                          "id": "r", "status": "incomplete",
                                          "usage": {"input_tokens": 9,
                                                    "output_tokens": 1}}}])
        self.assertEqual(rec_inc["codex_usage_calls"][0]["status"],
                         "incomplete")
        self.assertEqual(rec_inc["codex_usage"]["input_tokens"], 9)
        rec2 = {}
        try:
            self._run_into(rec2, [{"type": "response.failed",
                                   "response": {"id": "f",
                                                "status": "failed"}}])
        except RuntimeError:
            pass
        self.assertEqual(rec2["codex_usage"]["response_count"], 1)
        self.assertEqual(rec2["codex_usage_calls"][0]["status"], "failed")

    def test_aborted_stream_records_failed_call(self):
        rec = {}
        with self.assertRaises(RuntimeError):
            self._run_into(rec, [{"type": "response.output_text.delta",
                                  "delta": "x"}])
        self.assertEqual(rec["codex_usage"]["response_count"], 1)
        self.assertEqual(rec["codex_usage_calls"][0]["status"], "failed")

    def test_stats_accumulate_aggregate(self):
        rec = {}
        translate._record_usage({"id": "a", "usage": {
            "input_tokens": 10, "output_tokens": 2}}, rec)
        translate._record_usage({"id": "b", "usage": {
            "input_tokens": 20, "output_tokens": 3}}, rec)
        totals = {}
        from fusion_relay import usage
        usage.add_to_totals(totals, rec["codex_usage"])
        self.assertEqual((totals["input"], totals["output"]), (30, 5))

    def test_log_record_routes_usage_to_stats(self):
        with tempfile.TemporaryDirectory() as d:
            orig = (relay.REQUESTS_LOG, relay.DATA_DIR, relay._save_stats,
                    dict(relay._stats["tokens"]["codex"]))
            relay.REQUESTS_LOG = pathlib.Path(d) / "r.jsonl"
            relay.DATA_DIR = pathlib.Path(d)
            relay._save_stats = lambda: None
            relay._stats["tokens"]["codex"] = {}
            try:
                rec = {}
                translate._record_usage({"id": "a", "usage": {
                    "input_tokens": 10, "output_tokens": 2}}, rec)
                translate._record_usage({"id": "b", "usage": {
                    "input_tokens": 20, "output_tokens": 3}}, rec)
                relay._log_record(rec)
                self.assertEqual(
                    (relay._stats["tokens"]["codex"]["input"],
                     relay._stats["tokens"]["codex"]["output"]), (30, 5))
            finally:
                (relay.REQUESTS_LOG, relay.DATA_DIR, relay._save_stats,
                 relay._stats["tokens"]["codex"]) = orig


class ContinuityScopeTest(unittest.TestCase):
    def setUp(self):
        translate.reset()

    def _packet(self, seed="s"):
        asst = (wire.field(2, 2) + wire.field(3, "working")
                + wire.field(6, wire.field(1, "n1") + wire.field(2, "shell")
                             + wire.field(3, "{}")))
        return wire.decode(wire.field(3, asst) + wire.field(16, seed)
                           + wire.field(21, "gpt-6-astra-high"))

    def test_missing_seed_rejected(self):
        packet = wire.decode(wire.field(3, wire.field(2, 1)
                                        + wire.field(3, "hi"))
                             + wire.field(21, "gpt-6-astra-high"))
        with self.assertRaises(UnsupportedRequest):
            packet_to_responses_body(packet, parse_routed_model("x"), {})

    def test_scopes_partition_replay_cache(self):
        translate.stash_relay_items(
            translate._cache_key("s", "scope-a"),
            [{"type": "function_call", "call_id": "cx",
              "name": translate.CUA_TOOL_NAME, "arguments": "{}"}])
        other = packet_to_responses_body(
            self._packet(), parse_routed_model("x"), {},
            continuity_scope="scope-b")
        self.assertFalse(any(i.get("call_id") == "cx"
                             for i in other["input"]))
        same = packet_to_responses_body(
            self._packet(), parse_routed_model("x"), {},
            continuity_scope="scope-a")
        self.assertTrue(any(i.get("call_id") == "cx"
                            for i in same["input"]))

    def test_no_scope_disables_caches(self):
        translate.stash_relay_items(
            translate._cache_key("s"),
            [{"type": "function_call", "call_id": "cx",
              "name": translate.CUA_TOOL_NAME, "arguments": "{}"}])
        body = packet_to_responses_body(
            self._packet(), parse_routed_model("x"), {})
        self.assertFalse(any(i.get("call_id") == "cx"
                             for i in body["input"]))
        with translate._relay_items_lock:
            self.assertTrue(translate._relay_items_cache)  # not consumed


class DeliveryTest(unittest.TestCase):
    """Durable per-consumer delivery of terminal journal results."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db = pathlib.Path(self._tmp.name) / "ops.db"
        self.journal = operations.OperationJournal(self.db)
        self.addCleanup(self.journal.close)

    def _succeed(self, op="op1", result="RES"):
        self.journal.run("s1", _cua_call(op), lambda: result,
                         lambda: None)

    def test_offer_stable_id_and_ack(self):
        self._succeed()
        a = self.journal.offer("s1", "op1", "ui")
        b = self.journal.offer("s1", "op1", "ui")
        self.assertEqual(a["delivery_id"], b["delivery_id"])
        self.assertEqual(a["status"], "succeeded")
        self.assertEqual(a["result"], "RES")
        # offer alone does not acknowledge
        self.assertEqual(
            len(self.journal.pending_deliveries("s1", "ui")), 1)
        self.assertTrue(
            self.journal.acknowledge("s1", "ui", a["delivery_id"]))
        self.assertTrue(  # idempotent
            self.journal.acknowledge("s1", "ui", a["delivery_id"]))
        self.assertEqual(
            self.journal.pending_deliveries("s1", "ui"), [])

    def test_offer_survives_restart_same_id(self):
        self._succeed()
        first = self.journal.offer("s1", "op1", "ui")["delivery_id"]
        self.journal.close()
        self.journal = operations.OperationJournal(self.db)
        again = self.journal.offer("s1", "op1", "ui")["delivery_id"]
        self.assertEqual(first, again)

    def test_offer_refuses_nonterminal(self):
        # a crashed run leaves outcome_unknown — not offerable
        def boom():
            raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.journal.run("s1", _cua_call("bad"), boom, lambda: None)
        with self.assertRaises(IncompleteResponse):
            self.journal.offer("s1", "bad", "ui")
        with self.assertRaises(UnsupportedRequest):
            self.journal.offer("s1", "missing", "ui")

    def test_wrong_scope_consumer_ack_false(self):
        self._succeed()
        offer = self.journal.offer("s1", "op1", "ui")
        self.assertFalse(self.journal.acknowledge(
            "other", "ui", offer["delivery_id"]))
        self.assertFalse(self.journal.acknowledge(
            "s1", "other", offer["delivery_id"]))
        self.assertFalse(self.journal.acknowledge(
            "s1", "ui", "f" * 32))
        self.assertEqual(
            len(self.journal.pending_deliveries("s1", "ui")), 1)

    def test_failed_result_offerable(self):
        def boom():
            raise RuntimeError("nope")
        with self.assertRaises(RuntimeError):
            self.journal.run("s1", _cua_call("f1"), boom, lambda: None)
        # crashed -> outcome_unknown, not offerable
        with self.assertRaises(IncompleteResponse):
            self.journal.offer("s1", "f1", "ui")
        # a failed persisted result is offerable: mark via finish
        self.journal._finish("s1", "f1", "failed", "executor error")
        offer = self.journal.offer("s1", "f1", "ui")
        self.assertEqual(offer["status"], "failed")

    def test_concurrent_offer_single_id(self):
        import threading
        self._succeed()
        ids, errs = [], []
        def offer():
            try:
                ids.append(
                    self.journal.offer("s1", "op1", "ui")
                    ["delivery_id"])
            except Exception as e:
                errs.append(e)
        ts = [threading.Thread(target=offer) for _ in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertFalse(errs)
        self.assertEqual(len(set(ids)), 1)

    def test_concurrent_offer_two_instances(self):
        import threading
        self._succeed()
        other = operations.OperationJournal(self.db)
        self.addCleanup(other.close)
        ids, errs = [], []
        barrier = threading.Barrier(4)
        def offer(j):
            try:
                barrier.wait(10)
                ids.append(j.offer("s1", "op1", "ui")["delivery_id"])
            except Exception as e:
                errs.append(e)
        ts = [threading.Thread(target=offer, args=(j,))
              for j in (self.journal, other, self.journal, other)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertFalse(errs)
        self.assertEqual(len(set(ids)), 1)

    def test_many_scopes_deliveries(self):
        for i in range(70):
            self.journal.run(f"sc{i}", _cua_call("c1"),
                             lambda i=i: f"r{i}", lambda: None)
            o = self.journal.offer(f"sc{i}", "c1", "ui")
            self.assertEqual(o["result"], f"r{i}")


if __name__ == "__main__":
    unittest.main()
