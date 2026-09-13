"""Unit tests for the fusion-codex relay: wire codec, routing, translation."""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import wire
from fusion_relay.relay import route_for_model
from fusion_relay.translate import (UnsupportedRequest,
                                    packet_to_responses_body,
                                    parse_routed_model)
from fusion_relay import translate


class WireCodecTest(unittest.TestCase):
    """Round-trip protobuf fields and Connect frames."""

    def test_varint_roundtrip(self) -> None:
        for n in (0, 1, 127, 128, 300, 1 << 32):
            msg = wire.decode(wire.field(1, n))
            self.assertEqual(msg[1][0], n)

    def test_string_field(self) -> None:
        msg = wire.decode(wire.field(21, "gpt-6-astra-high"))
        self.assertEqual(wire.text(msg, 21), "gpt-6-astra-high")

    def test_nested_message_field(self) -> None:
        inner = wire.field(2, 1) + wire.field(3, "hi")
        outer = wire.decode(wire.field(3, inner))
        nested = wire.decode(outer[3][0])  # type: ignore[arg-type]
        self.assertEqual(nested[2][0], 1)
        self.assertEqual(wire.text(nested, 3), "hi")

    def test_frame_roundtrip(self) -> None:
        framed = wire.frame(b"payload")
        flags, payload = wire.unframe(framed)
        self.assertEqual((flags, payload), (0, b"payload"))
        frames = wire.iter_frames(framed + wire.frame(b"{}", wire.FLAG_TRAILER))
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[1][0], wire.FLAG_TRAILER)

    def test_truncated_frame_rejected(self) -> None:
        with self.assertRaises(ValueError):
            wire.unframe(b"\x00\x00\x00\x00")

    def test_error_frame_shape(self) -> None:
        flags, payload = wire.unframe(wire.error_frame("internal", "boom"))
        self.assertEqual(flags, wire.FLAG_TRAILER)
        self.assertEqual(json.loads(payload)["error"]["code"], "internal")


class RoutingTest(unittest.TestCase):
    """Model-id routing policy."""

    def test_astra_routes_to_codex(self) -> None:
        for model in ("gpt-6-astra", "gpt-6-astra-high", "gpt-6-astra-xhigh",
                      "gpt-6-astra-max", "gpt-6-astra-high-fast"):
            self.assertEqual(route_for_model(model), "codex", model)

    def test_swe_family_routes_to_cognition(self) -> None:
        # swe-* models are Cognition-native: forwarding preserves the route
        # they would have taken without the relay (sidekick, title-gen aux).
        for model in ("swe-2-medium", "swe-2-high", "swe-1-6-fast"):
            self.assertEqual(route_for_model(model), "forward", model)

    def test_unknown_model_rejected_by_default(self) -> None:
        # Non-Cognition-family models (other gpt-*, kimi, etc.) must never
        # silently fall through to a paid route.
        for model in ("gpt-5.6-sol-high", "kimi-k3", "claude-opus", ""):
            self.assertEqual(route_for_model(model), "reject", model)


class RoutedModelTest(unittest.TestCase):
    """Suffix parsing: routed id -> upstream model + effort."""

    def test_effort_suffix(self) -> None:
        r = parse_routed_model("gpt-6-astra-high")
        self.assertEqual((r.model, r.effort), ("gpt-6-astra", "high"))

    def test_bare_model_defaults_high(self) -> None:
        r = parse_routed_model("gpt-6-astra")
        self.assertEqual((r.model, r.effort), ("gpt-6-astra", "high"))

    def test_max_maps_to_xhigh_with_note(self) -> None:
        r = parse_routed_model("gpt-6-astra-max")
        self.assertEqual((r.model, r.effort), ("gpt-6-astra", "xhigh"))
        self.assertTrue(r.notes)

    def test_fast_is_stripped_with_note(self) -> None:
        r = parse_routed_model("gpt-6-astra-high-fast")
        self.assertEqual(r.model, "gpt-6-astra")
        self.assertTrue(r.notes)


class TranslateTest(unittest.TestCase):
    """packet -> Responses body translation."""

    def _packet(self, *messages: bytes, tools: bool = True) -> wire.Message:
        body = wire.field(2, "SESSION INSTRUCTIONS")
        for m in messages:
            body += wire.field(3, m)
        if tools:
            params = json.dumps({"type": "object",
                                 "properties": {"p": {"type": "string"}}})
            body += wire.field(10, wire.field(1, "tool_a")
                               + wire.field(2, "desc") + wire.field(3, params))
        body += wire.field(16, "session-seed")
        body += wire.field(21, "gpt-6-astra-high")
        return wire.decode(body)

    def test_roles_and_instructions(self) -> None:
        user = wire.field(2, 1) + wire.field(3, "hello")
        asst = wire.field(2, 2) + wire.field(3, "hi")
        sys_m = wire.field(2, 5) + wire.field(3, "SYS")
        packet = self._packet(sys_m, user, asst)
        rec: dict = {}
        body = packet_to_responses_body(packet, parse_routed_model("gpt-6-astra-high"), rec)
        self.assertEqual(body["model"], "gpt-6-astra")
        self.assertEqual(body["reasoning"], {"effort": "high"})
        self.assertIn("SESSION INSTRUCTIONS", body["instructions"])
        self.assertIn("SYS", body["instructions"])
        self.assertEqual(body["input"][:2],
                         [{"role": "user", "content": "hello"},
                          {"role": "assistant", "content": "hi"}])
        self.assertTrue(body["stream"] and body["store"] is False)
        self.assertEqual(body["tools"][0]["name"], "tool_a")
        self.assertTrue(body["prompt_cache_key"].startswith("fusion-relay-"))

    def test_tool_calls_and_outputs(self) -> None:
        tc = wire.field(6, wire.field(1, "c1") + wire.field(2, "tool_a")
                        + wire.field(3, "{}"))
        asst_with_call = wire.field(2, 2) + tc
        out = wire.field(2, 4) + wire.field(3, "result") + wire.field(7, "c1")
        packet = self._packet(asst_with_call, out)
        body = packet_to_responses_body(packet, parse_routed_model("x"), {})
        kinds = [i["type"] for i in body["input"]]
        self.assertEqual(kinds, ["function_call", "function_call_output"])
        self.assertEqual(body["input"][0]["call_id"], "c1")
        self.assertEqual(body["input"][1]["output"], "result")

    def test_unknown_source_rejected(self) -> None:
        weird = wire.field(2, 9) + wire.field(3, "mystery")
        packet = self._packet(weird)
        with self.assertRaises(UnsupportedRequest):
            packet_to_responses_body(packet, parse_routed_model("x"), {})


class FinalMessageTest(unittest.TestCase):
    """Codex response -> wire message translation."""

    def test_text_and_usage(self) -> None:
        complete = {
            "id": "resp_1", "model": "gpt-6-astra", "status": "completed",
            "output": [{"type": "message", "content": [
                {"type": "output_text", "text": "answer"}]}],
            "usage": {"input_tokens": 10, "output_tokens": 5,
                      "input_tokens_details": {"cached_tokens": 4}},
        }
        msg = translate._final_message(complete, [], {})
        decoded = wire.decode(msg)
        self.assertEqual(wire.text(decoded, 1), "resp_1")
        self.assertEqual(decoded[5][0], translate.FINISH_STOP)
        usage = wire.decode(decoded[7][0])  # type: ignore[arg-type]
        self.assertEqual((usage[2][0], usage[3][0], usage[5][0]), (10, 5, 4))

    def test_tool_call_finish_reason(self) -> None:
        complete = {
            "id": "r", "output": [{"type": "function_call", "call_id": "c",
                                   "name": "t", "arguments": "{}"}],
            "usage": {},
        }
        rec: dict = {}
        decoded = wire.decode(translate._final_message(complete, [], rec))
        self.assertEqual(decoded[5][0], translate.FINISH_TOOL_CALLS)
        tc = wire.decode(decoded[6][0])  # type: ignore[arg-type]
        self.assertEqual(wire.text(tc, 2), "t")


if __name__ == "__main__":
    unittest.main()
