"""Remediation regressions: fail-closed computer dispatch, tool-loop
terminal correctness, usage accounting, and wire frame validation.

Each test pins one defect from the remediation handoff. They fail against
the pre-remediation implementation.
"""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import cua, operations, translate, wire


class ElicitationFailClosedTest(unittest.TestCase):
    """The relay must never auto-accept a consent elicitation."""

    def test_elicitation_is_not_auto_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            provider = cua.CuaProvider(pathlib.Path(d))
            provider._send = Mock()  # type: ignore[method-assign]
            provider._answer_elicitation({
                "jsonrpc": "2.0", "id": 41,
                "method": "elicitation/create", "params": {}})
        sent = provider._send.call_args[0][0]
        result = sent.get("result") or {}
        self.assertNotEqual(result.get("action"), "accept")
        self.assertNotEqual((result.get("content") or {}).get("confirmed"),
                            True)


class ComputerToolInjectionTest(unittest.TestCase):
    """No trusted role binding or consent UI exists: injection must
    reject, not hide competing native tools or pretend authorization."""

    def test_inject_computer_tool_rejected_without_trust(self) -> None:
        body = {"tools": [{"type": "function", "name": "native_computer",
                           "parameters": {}}]}
        with self.assertRaises(translate.UnsupportedRequest):
            translate.inject_computer_tool(body)
        self.assertEqual(body, {"tools": [{"type": "function",
                                           "name": "native_computer",
                                           "parameters": {}}]})


class ToolLoopBudgetTest(unittest.TestCase):
    """Exhausting the tool budget raises tool_budget_exhausted and the
    executed call/result pair remains recoverable from the stash."""

    def setUp(self) -> None:
        translate.reset()
        self._orig = translate.call_codex

    def tearDown(self) -> None:
        translate.call_codex = self._orig  # type: ignore[assignment]

    def test_budget_exhausted_raises_and_stashes_result(self) -> None:
        def fake(body, rec, on_delta=None, timeout=0, _items_out=None,
                 **_ignored):
            if _items_out is not None:
                _items_out.append({
                    "type": "function_call", "call_id": "c1",
                    "name": "codex_computer", "arguments": "{}"})
            msg = (wire.field(1, "r1")
                   + wire.field(6, wire.field(1, "c1")
                                + wire.field(2, "codex_computer")
                                + wire.field(3, "{}"))
                   + wire.field(5, 10))
            return wire.frame(msg) + wire.end_stream()

        translate.call_codex = fake  # type: ignore[assignment]
        executor = Mock(return_value="DONE")
        with tempfile.TemporaryDirectory() as d:
            journal = operations.OperationJournal(
                pathlib.Path(d) / "ops.db")
            try:
                with self.assertRaises(translate.IncompleteResponse) as ctx:
                    translate.call_codex_with_tools(
                        {"prompt_cache_key": "audit-session", "input": []},
                        {}, executor=executor, max_loops=1,
                        journal=journal, operation_scope="audit-scope")
            finally:
                journal.close()
        self.assertIn("tool_budget_exhausted", str(ctx.exception))
        executor.assert_called_once()
        with translate._relay_items_lock:
            stashed = translate._relay_items_cache["audit-session"]
        self.assertIn({"type": "function_call_output", "call_id": "c1",
                       "output": "DONE"}, stashed)


class UsageAccountingTest(unittest.TestCase):
    """Usage from every internal inference response must aggregate."""

    def test_usage_accumulates_across_responses(self) -> None:
        rec: dict = {}
        translate._record_usage(
            {"id": "r1",
             "usage": {"input_tokens": 10, "output_tokens": 2}}, rec)
        translate._record_usage(
            {"id": "r2",
             "usage": {"input_tokens": 20, "output_tokens": 3}}, rec)
        self.assertEqual(rec["codex_usage"]["input_tokens"], 30)
        self.assertEqual(rec["codex_usage"]["output_tokens"], 5)


class FrameValidationTest(unittest.TestCase):
    """iter_frames must reject a frame whose declared length exceeds the
    bytes actually present."""

    def test_iter_frames_rejects_truncated_payload(self) -> None:
        with self.assertRaises(ValueError):
            wire.iter_frames(b"\x00\x00\x00\x00\x09abc")


if __name__ == "__main__":
    unittest.main()
