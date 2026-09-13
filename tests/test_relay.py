"""Unit tests for the fusion-codex relay: wire codec, routing, translation."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
import pathlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import wire
from fusion_relay import catalog, relay, translate
from fusion_relay.relay import route_for_model
from fusion_relay.translate import (IncompleteResponse, UnsupportedRequest,
                                    packet_to_responses_body,
                                    parse_routed_model)


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
        for model in ("swe-2-medium", "swe-2-high", "swe-1-6-fast"):
            self.assertEqual(route_for_model(model), "forward", model)

    def test_unknown_model_forwards_native_by_default(self) -> None:
        # Non-astra models keep their native Cognition route — identical to
        # running without the relay; nothing silently re-bills.
        for model in ("gpt-5.6-sol-high", "kimi-k3", "claude-opus"):
            self.assertEqual(route_for_model(model), "forward", model)

    def test_reject_policy_still_available(self) -> None:
        saved = relay.AUX_POLICY
        try:
            relay.AUX_POLICY = "reject"
            self.assertEqual(route_for_model("claude-opus"), "reject")
        finally:
            relay.AUX_POLICY = saved


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

    def setUp(self) -> None:
        translate.reset()

    def _packet(self, *messages: bytes, tools: bool = True,
                seed: str = "session-seed") -> wire.Message:
        body = wire.field(2, "SESSION INSTRUCTIONS")
        for m in messages:
            body += wire.field(3, m)
        if tools:
            params = json.dumps({"type": "object",
                                 "properties": {"p": {"type": "string"}}})
            body += wire.field(10, wire.field(1, "tool_a")
                               + wire.field(2, "desc") + wire.field(3, params))
        body += wire.field(16, seed)
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
        self.assertEqual(body["include"], ["reasoning.encrypted_content"])
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

    def test_parallel_tool_calls_preserved(self) -> None:
        tcs = b"".join(
            wire.field(6, wire.field(1, cid) + wire.field(2, "t")
                       + wire.field(3, "{}"))
            for cid in ("c1", "c2", "c3"))
        asst = wire.field(2, 2) + wire.field(3, "calling") + tcs
        body = packet_to_responses_body(self._packet(asst),
                                        parse_routed_model("x"), {})
        calls = [i for i in body["input"] if i.get("type") == "function_call"]
        self.assertEqual([c["call_id"] for c in calls], ["c1", "c2", "c3"])

    def test_unknown_source_rejected(self) -> None:
        weird = wire.field(2, 9) + wire.field(3, "mystery")
        packet = self._packet(weird)
        with self.assertRaises(UnsupportedRequest):
            packet_to_responses_body(packet, parse_routed_model("x"), {})

    def test_image_field_becomes_input_image(self) -> None:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        user = wire.field(2, 1) + wire.field(3, "look") + wire.field(9, png)
        body = packet_to_responses_body(self._packet(user),
                                        parse_routed_model("x"), {})
        content = body["input"][0]["content"]
        self.assertEqual(content[0], {"type": "input_text", "text": "look"})
        self.assertEqual(content[1]["type"], "input_image")
        self.assertTrue(content[1]["image_url"].startswith("data:image/png;base64,"))

    def test_large_unknown_field_rejected(self) -> None:
        blob = b"\x01" * 500  # not an image, too big to ignore
        user = wire.field(2, 1) + wire.field(3, "hi") + wire.field(11, blob)
        with self.assertRaises(UnsupportedRequest):
            packet_to_responses_body(self._packet(user),
                                     parse_routed_model("x"), {})

    def test_small_unknown_field_ignored_not_dropped_silently(self) -> None:
        user = wire.field(2, 1) + wire.field(3, "hi") + wire.field(8, b"msgid")
        rec: dict = {}
        body = packet_to_responses_body(self._packet(user),
                                        parse_routed_model("x"), rec)
        self.assertEqual(body["input"][0]["content"], "hi")
        self.assertIn("src1.f8", rec["ignored_fields"])

    def test_reasoning_items_echoed_before_last_assistant_turn(self) -> None:
        seed = "sess-42"
        key = translate._cache_key(seed)
        reasoning = [{"type": "reasoning", "encrypted_content": "BLOB",
                      "summary": []}]
        translate._stash_reasoning(key, reasoning + [
            {"type": "message"},  # non-reasoning dropped
            {"type": "reasoning"},  # no encrypted_content dropped
        ])
        user1 = wire.field(2, 1) + wire.field(3, "first")
        asst = wire.field(2, 2) + wire.field(3, "answer")
        user2 = wire.field(2, 1) + wire.field(3, "followup")
        body = packet_to_responses_body(
            self._packet(user1, asst, user2, seed=seed),
            parse_routed_model("x"), {})
        kinds = [(i.get("type"), i.get("role")) for i in body["input"]]
        # reasoning inserted immediately before the last assistant turn
        self.assertEqual(kinds, [
            (None, "user"), ("reasoning", None), (None, "assistant"),
            (None, "user")])

    def test_no_reasoning_echo_when_cache_empty(self) -> None:
        asst = wire.field(2, 2) + wire.field(3, "a")
        body = packet_to_responses_body(self._packet(asst),
                                        parse_routed_model("x"), {})
        self.assertFalse(any(i.get("type") == "reasoning"
                             for i in body["input"]))


class _FakeSSE:
    """Minimal urllib response stand-in yielding SSE lines."""

    def __init__(self, events: list[dict], status: int = 200) -> None:
        self._lines = [b"data: " + json.dumps(e).encode() + b"\n"
                       for e in events]
        self.status = status
        self.headers: dict = {}
        self.closed = False

    def __iter__(self):
        return iter(self._lines)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        self.closed = True


class CallCodexTest(unittest.TestCase):
    """Response-stream handling: buffered text, incomplete, cancellation."""

    def setUp(self) -> None:
        translate.reset()
        self._auth = translate.auth.get_token
        translate.auth.get_token = lambda: ("tok", "acct")  # type: ignore

    def tearDown(self) -> None:
        translate.auth.get_token = self._auth  # type: ignore

    def _run(self, events, on_delta=None):
        import urllib.request
        fake = _FakeSSE(events)
        orig = urllib.request.urlopen
        urllib.request.urlopen = lambda *a, **k: fake  # type: ignore
        try:
            return translate.call_codex({"prompt_cache_key": "k"}, {},
                                        on_delta=on_delta), fake
        finally:
            urllib.request.urlopen = orig  # type: ignore

    def test_buffered_mode_emits_full_text_then_terminal(self) -> None:
        events = [
            {"type": "response.output_text.delta", "delta": "Hel"},
            {"type": "response.output_text.delta", "delta": "lo"},
            {"type": "response.completed", "response": {
                "id": "r1", "status": "completed", "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 2}}},
        ]
        out, _ = self._run(events)  # on_delta=None -> buffered
        frames = wire.iter_frames(out)
        texts = [wire.text(wire.decode(p), 3)
                 for f, p in frames if not f & 0x02]
        self.assertEqual(texts[0], "Hello")  # cumulative frame first
        last = wire.decode(frames[-2][1])
        self.assertEqual(last[5][0], translate.FINISH_STOP)

    def test_delta_mode_streams_and_terminal_has_no_text(self) -> None:
        events = [
            {"type": "response.output_text.delta", "delta": "Hi"},
            {"type": "response.completed", "response": {
                "id": "r", "status": "completed", "output": [], "usage": {}}},
        ]
        got = []
        out, _ = self._run(events, on_delta=lambda f: got.append(f) or True)
        self.assertEqual(len(got), 1)
        frames = wire.iter_frames(out)
        msg = wire.decode(frames[0][1])
        self.assertNotIn(3, msg)  # terminal carries no text in delta mode

    def test_incomplete_raises_not_silent_stop(self) -> None:
        events = [{"type": "response.incomplete", "response": {
            "id": "r", "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "usage": {"input_tokens": 9}}}]
        with self.assertRaises(IncompleteResponse):
            self._run(events)

    def test_client_gone_aborts_upstream(self) -> None:
        events = [{"type": "response.output_text.delta", "delta": "x"}] * 5
        events.append({"type": "response.completed", "response": {
            "id": "r", "status": "completed", "output": [], "usage": {}}})
        fake_holder = {}
        import urllib.request
        orig = urllib.request.urlopen
        def fake_open(*a, **k):
            fake_holder["r"] = _FakeSSE(events)
            return fake_holder["r"]
        urllib.request.urlopen = fake_open  # type: ignore
        try:
            with self.assertRaises(translate.ClientGone):
                translate.call_codex({}, {}, on_delta=lambda f: False)
            self.assertTrue(fake_holder["r"].closed)  # upstream read stopped
        finally:
            urllib.request.urlopen = orig  # type: ignore

    def test_reasoning_items_stashed_for_next_turn(self) -> None:
        events = [{"type": "response.completed", "response": {
            "id": "r", "status": "completed",
            "output": [{"type": "reasoning", "encrypted_content": "E"}],
            "usage": {}}}]
        self._run(events)
        with translate._reasoning_lock:
            self.assertEqual(translate._reasoning_cache["k"][0]
                             ["encrypted_content"], "E")


class LogPrivacyTest(unittest.TestCase):
    """Regression: no user/assistant content or tool args may reach rec dicts."""

    MARKER = "SECRET_MARKER_7X9Q"

    def test_request_path_record_carries_no_content(self) -> None:
        tc = wire.field(6, wire.field(1, "c1") + wire.field(2, "tool_a")
                        + wire.field(3, '{"k":"%s"}' % self.MARKER))
        user = wire.field(2, 1) + wire.field(3, self.MARKER + " prompt")
        asst = wire.field(2, 2) + wire.field(3, "asst " + self.MARKER) + tc
        tout = wire.field(2, 4) + wire.field(3, "out " + self.MARKER) \
            + wire.field(7, "c1")
        body = (wire.field(3, user) + wire.field(3, asst) + wire.field(3, tout)
                + wire.field(16, "seed") + wire.field(21, "gpt-6-astra-high"))
        rec: dict = {}
        packet_to_responses_body(wire.decode(body), parse_routed_model("x"), rec)
        self.assertNotIn(self.MARKER, json.dumps(rec))

    def test_response_path_record_carries_no_content(self) -> None:
        complete = {
            "id": "r", "status": "completed",
            "output": [{"type": "function_call", "call_id": "c",
                        "name": "t", "arguments": '{"k":"%s"}' % self.MARKER}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
        rec: dict = {}
        translate._final_message(complete, [], rec)
        translate._record_usage(complete, rec)
        self.assertNotIn(self.MARKER, json.dumps(rec))

    def test_delta_records_chars_not_text(self) -> None:
        rec: dict = {}
        events = [{"type": "response.output_text.delta",
                   "delta": self.MARKER},
                  {"type": "response.completed", "response": {
                      "id": "r", "status": "completed", "output": [],
                      "usage": {}}}]
        import urllib.request
        fake = _FakeSSE(events)
        orig = urllib.request.urlopen
        orig_auth = translate.auth.get_token
        urllib.request.urlopen = lambda *a, **k: fake  # type: ignore
        translate.auth.get_token = lambda: ("t", "a")  # type: ignore
        try:
            translate.call_codex({"prompt_cache_key": "k"}, rec,
                                 on_delta=lambda f: True)
        finally:
            urllib.request.urlopen = orig  # type: ignore
            translate.auth.get_token = orig_auth  # type: ignore
        self.assertNotIn(self.MARKER, json.dumps(rec))
        self.assertEqual(rec["delta_chars"], len(self.MARKER))


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

    def test_tool_call_logs_name_only_never_arguments(self) -> None:
        complete = {
            "id": "r", "output": [{"type": "function_call", "call_id": "c",
                                   "name": "t", "arguments": "{\"secret\":1}"}],
            "usage": {},
        }
        rec: dict = {}
        decoded = wire.decode(translate._final_message(complete, [], rec))
        self.assertEqual(decoded[5][0], translate.FINISH_TOOL_CALLS)
        self.assertEqual(rec["tool_call_names"], ["t"])
        self.assertNotIn("secret", json.dumps(rec))


class CatalogTest(unittest.TestCase):
    """Catalog injection + AssignModel rewrite + session pinning."""

    def setUp(self) -> None:
        catalog.reset()

    def _entry(self, model_id: str, name: str = "A Model") -> bytes:
        inner = wire.field(17, model_id)
        badges = wire.field(1, "Fam") + wire.field(2, wire.field(1, "Effort")
                                                 + wire.field(2, wire.field(2, "High")
                                                              + wire.field(3, 1)))
        e = (wire.field(1, name) + wire.field(22, model_id)
             + wire.field(23, inner) + wire.field(30, badges))
        return wire.field(1, e)  # top-level repeated field 1

    def test_inject_relabels_base_and_adds_native_clones(self) -> None:
        body = self._entry("gpt-6-astra-high", "GPT-6 Astra High Thinking")
        body += self._entry("fusion-gpt-6-astra-high-sidekick-swe-2-medium", "Fusion A/S")
        body += self._entry("claude-opus-5-medium", "Opus")  # not cloneable
        out, n, warnings = catalog.inject_route_entries(body)
        self.assertEqual(n, 2)
        self.assertFalse(warnings)
        top = wire.decode_typed(out)
        pairs = [(wire.get_string(wire.decode_typed(v), 22),
                  wire.get_string(wire.decode_typed(v), 1))
                 for v, w in top[1] if w == 2]
        ids = [p[0] for p in pairs]
        names = [p[1] for p in pairs]
        self.assertIn("gpt-6-astra-high", ids)
        self.assertIn("GPT-6 Astra High Thinking · Codex sub", names)
        self.assertIn("Fusion A/S · Codex sub", names)
        self.assertIn("gpt-6-astra-high-native", ids)
        self.assertIn("fusion-gpt-6-astra-high-sidekick-swe-2-medium-native", ids)
        self.assertNotIn("claude-opus-5-medium-native", ids)

    def test_astra_like_drift_warns(self) -> None:
        body = self._entry("astra-9-xhigh", "Future Astra")
        _, _, warnings = catalog.inject_route_entries(body)
        self.assertTrue(any("astra-9-xhigh" in w for w in warnings))

    def test_inject_on_garbage_returns_input_with_warning(self) -> None:
        out, n, warnings = catalog.inject_route_entries(b"\xff\xff")
        self.assertEqual((out, n), (b"\xff\xff", 0))
        self.assertTrue(warnings)

    def test_rewrite_assign_native_returns_pending_pin(self) -> None:
        req = wire.field(2, "gpt-6-astra-high-native") + wire.field(3, "u2")
        new_body, session, route = catalog.rewrite_assign(req)
        self.assertEqual((session, route), ("u2", "native"))
        self.assertEqual(wire.text(wire.decode(new_body), 2), "gpt-6-astra-high")
        # not pinned yet — caller commits only on upstream success
        packet = wire.decode(wire.field(16, "u2"))
        self.assertIsNone(catalog.session_route(packet))
        catalog.pin_route(session, route)
        self.assertEqual(catalog.session_route(packet), "native")

    def test_unsuffixed_selector_untouched(self) -> None:
        req = wire.field(2, "gpt-6-astra-high") + wire.field(3, "u3")
        new_body, session, route = catalog.rewrite_assign(req)
        self.assertEqual((session, route), ("", ""))
        self.assertEqual(wire.decode(new_body)[2], wire.decode(req)[2])

    def test_pins_persist_across_restart(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / "routes.json"
            catalog.attach_store(path)
            catalog.pin_route("sess-1", "native")
            catalog.reset()
            catalog.attach_store(path)  # simulate restart
            packet = wire.decode(wire.field(16, "sess-1"))
            self.assertEqual(catalog.session_route(packet), "native")
        catalog.attach_store(pathlib.Path("/nonexistent/routes.json"))
        catalog.reset()

    def test_encode_typed_roundtrip(self) -> None:
        raw = (wire.field(1, "x") + wire.field(5, 42)
               + wire.varint(3 << 3 | 5) + b"ABCD")  # fixed32
        typed = wire.decode_typed(raw)
        self.assertEqual(wire.encode_typed(typed), raw)


if __name__ == "__main__":
    unittest.main()
