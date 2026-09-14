"""Tool-result field-10 PNG envelope translation tests."""

from __future__ import annotations

import base64
import binascii
import json
import os
import struct
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import artifacts, translate, wire
from fusion_relay.translate import (UnsupportedRequest,
                                    packet_to_responses_body,
                                    parse_routed_model)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
CAPTURED_MSG = os.path.join(FIXTURES, "tool-image-message.bin")
FIXTURE_PNG = os.path.join(FIXTURES, "tool-image.png")

HELPER_ERR = ("tool image field 10 invalid or unsupported "
              "(PNG/JPEG required)")


def _chunk(kind: bytes, data: bytes) -> bytes:
    return (struct.pack(">I", len(data)) + kind + data
            + struct.pack(">I", binascii.crc32(kind + data) & 0xffffffff))


def _envelope(png: bytes, mime: bytes = b"image/png") -> bytes:
    return wire.field(1, base64.b64encode(png)) + wire.field(2, mime)


def _fixture() -> bytes:
    with open(FIXTURE_PNG, "rb") as f:
        return f.read()


def _padded_png(pad: int) -> bytes:
    png = _fixture()
    return png[:-12] + _chunk(b"tEXt", b"note\0" + b"x" * pad) + png[-12:]


def _src4(*extra: bytes, text: str = "result",
          call_id: str = "c1") -> bytes:
    msg = wire.field(2, 4)
    if text:
        msg += wire.field(3, text)
    if call_id:
        msg += wire.field(7, call_id)
    for e in extra:
        msg += wire.field(10, e)
    return msg


def _translate(*msgs: bytes):
    body = wire.field(16, "synthetic-session")
    for m in msgs:
        body += wire.field(3, m)
    rec: dict = {}
    out = packet_to_responses_body(wire.decode(body),
                                   parse_routed_model("x"), rec)
    return out, rec


def _last_output(body: dict):
    return body["input"][-1]


class ToolImageTest(unittest.TestCase):

    def setUp(self) -> None:
        translate.reset()

    def test_captured_message_decodes_verbatim(self) -> None:
        with open(CAPTURED_MSG, "rb") as f:
            msg = f.read()
        fixture = _fixture()
        body, _ = _translate(msg)
        last = _last_output(body)
        self.assertEqual(last["type"], "function_call_output")
        self.assertEqual(last["call_id"], "synthetic-call-1")
        self.assertEqual(last["output"], [
            {"type": "input_text", "text": "[Image 1]"},
            {"type": "input_image",
             "image_url": "data:image/png;base64,"
                          + base64.b64encode(fixture).decode()}])
        body2 = json.loads(json.dumps(body))
        out = body2["input"][-1]["output"]
        self.assertIsInstance(out, list)
        self.assertEqual(out[0]["text"], "[Image 1]")
        self.assertEqual(base64.b64decode(
            out[1]["image_url"].split(",", 1)[1]), fixture)

    def test_oversized_envelope_image_preserved(self) -> None:
        for pad, minimum_size in ((18000, 22588), (45000, 59107)):
            with self.subTest(pad=pad):
                png = _padded_png(pad)
                env = _envelope(png)
                self.assertGreater(len(env), minimum_size)
                body, _ = _translate(_src4(env))
                out = _last_output(body)["output"]
                self.assertEqual(out[0], {"type": "input_text", "text": "result"})
                self.assertEqual(base64.b64decode(
                    out[1]["image_url"].split(",", 1)[1]), png)

    def test_multiple_images_order_and_empty_text(self) -> None:
        png_a, png_b = _fixture(), _padded_png(64)
        body, _ = _translate(_src4(_envelope(png_a), _envelope(png_b)))
        out = _last_output(body)["output"]
        self.assertEqual(out[0]["text"], "result")
        self.assertEqual(
            [base64.b64decode(p["image_url"].split(",", 1)[1])
             for p in out[1:]], [png_a, png_b])
        body, _ = _translate(
            _src4(_envelope(png_a), _envelope(png_b), text=""))
        out = _last_output(body)["output"]
        self.assertEqual([p["type"] for p in out],
                         ["input_image", "input_image"])

    def test_invalid_envelopes_rejected(self) -> None:
        fixture = _fixture()
        b64png = base64.b64encode(fixture)
        cases = [
            b"x" * 22588,
            b"\x0a\xff",
            wire.field(2, b"image/png"),
            wire.field(1, b64png),
            wire.field(1, b64png) + wire.field(1, b64png)
            + wire.field(2, b"image/png"),
            _envelope(fixture) + wire.field(3, b"z"),
            b"\x08\x05" + wire.field(2, b"image/png"),
            b"\x0d\x00\x00\x00\x00" + wire.field(2, b"image/png"),
            wire.field(1, b"###") + wire.field(2, b"image/png"),
            _envelope(fixture, b"image/jpeg"),
            wire.field(1, base64.b64encode(b"not png"))
            + wire.field(2, b"image/png"),
            _envelope(fixture[:-8] + b"\xde\xad\xbe\xef"
                      + fixture[-4:]),
            _envelope(fixture) + wire.field(2, b"image/png"),
            wire.field(1, b"") + wire.field(2, b"image/png"),
        ]
        for payload in cases:
            with self.subTest(payload=payload[:20]):
                with self.assertRaises(UnsupportedRequest) as caught:
                    _translate(_src4(payload))
                self.assertEqual(str(caught.exception), HELPER_ERR)

    def test_too_many_images_rejected(self) -> None:
        env = _envelope(_fixture())
        with self.assertRaises(UnsupportedRequest) as caught:
            _translate(_src4(*([env] * 5)))
        self.assertEqual(str(caught.exception), "too many tool images")

    def test_image_without_call_id_rejected(self) -> None:
        with self.assertRaises(UnsupportedRequest) as caught:
            _translate(_src4(_envelope(_fixture()), call_id=""))
        self.assertEqual(str(caught.exception),
                         "tool image call_id required")

    def test_raw_png_under_field9_rejected_generically(self) -> None:
        fixture = _fixture()
        msg = (wire.field(2, 4) + wire.field(3, "x") + wire.field(7, "c1")
               + wire.field(9, fixture))
        with self.assertRaises(UnsupportedRequest) as caught:
            _translate(msg)
        self.assertEqual(
            str(caught.exception),
            "message field 9 (source 4) carries a %d-byte payload the "
            "translator cannot represent" % len(fixture))

    def test_envelope_size_limit_before_base64(self) -> None:
        env = _envelope(_fixture())
        with mock.patch.object(artifacts, "MAX_ARTIFACT_BYTES", 32), \
                mock.patch.object(translate.base64, "b64decode") as m:
            with self.assertRaises(UnsupportedRequest) as caught:
                _translate(_src4(env))
        self.assertEqual(str(caught.exception), HELPER_ERR)
        m.assert_not_called()

    def test_user_field10_envelope_still_rejected(self) -> None:
        env = _envelope(_fixture())
        msg = (wire.field(2, 1) + wire.field(3, "hi")
               + wire.field(10, env))
        with self.assertRaises(UnsupportedRequest):
            _translate(msg)

    def test_text_only_tool_result_unchanged(self) -> None:
        body, _ = _translate(_src4())
        last = _last_output(body)
        self.assertEqual(last, {"type": "function_call_output",
                                "call_id": "c1", "output": "result"})

    def test_no_payload_leaks_into_rec_or_errors(self) -> None:
        fixture = _fixture()
        marker = base64.b64encode(fixture).decode()[:64]
        body, rec = _translate(_src4(_envelope(fixture)))
        self.assertNotIn(marker, json.dumps(rec))
        self.assertNotIn("PNG", json.dumps(rec))
        bad = wire.field(1, b"###") + wire.field(2, b"image/png")
        with self.assertRaises(UnsupportedRequest) as caught:
            _translate(_src4(bad))
        self.assertNotIn(marker, str(caught.exception))
        self.assertNotIn("###", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
