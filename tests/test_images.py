"""JPEG/PNG media compatibility: images.py, translator field 10, runtime."""

from __future__ import annotations

import base64
import hashlib
import io
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from fusion_relay import artifacts, images, translate, wire
from fusion_relay.approvals import ApprovalManager
from fusion_relay.broker import ExecutionBinding
from fusion_relay.runtime import (CuaRuntimeAdapter, RuntimeUnavailable)
from fusion_relay.translate import (UnsupportedRequest,
                                    packet_to_responses_body,
                                    parse_routed_model)
import test_runtime

try:
    from PIL import Image
    HAVE_PIL = True
except ImportError:
    HAVE_PIL = False

JPEG_REASON = "Pillow (requirements-image.txt) required for JPEG tests"
ERR = "tool image field 10 invalid or unsupported (PNG/JPEG required)"


def _jpeg(w: int = 16, h: int = 16, exif=None) -> bytes:
    img = Image.new("RGB", (w, h), (200, 30, 30))
    buf = io.BytesIO()
    kwargs = {"format": "JPEG"}
    if exif is not None:
        kwargs["exif"] = exif
    img.save(buf, **kwargs)
    return buf.getvalue()


def _env(data: bytes, mime: bytes = b"image/png") -> bytes:
    return wire.field(1, base64.b64encode(data)) + wire.field(2, mime)


def _src4(*envs: bytes) -> bytes:
    msg = wire.field(2, 4) + wire.field(3, "r") + wire.field(7, "c1")
    for e in envs:
        msg += wire.field(10, e)
    return msg


def _translate(msg: bytes) -> dict:
    body = wire.field(16, "synthetic-session") + wire.field(3, msg)
    return packet_to_responses_body(wire.decode(body),
                                    parse_routed_model("x"), {})


@unittest.skipUnless(HAVE_PIL, JPEG_REASON)
class TranslatorJpegTest(unittest.TestCase):

    def setUp(self) -> None:
        translate.reset()

    def test_jpeg_declared_png_keeps_original_bytes(self) -> None:
        jpeg = _jpeg()
        body = _translate(_src4(_env(jpeg, b"image/png")))
        out = body["input"][-1]["output"]
        self.assertEqual(out[0], {"type": "input_text", "text": "r"})
        self.assertEqual(out[1], {
            "type": "input_image",
            "image_url": "data:image/jpeg;base64,"
                         + base64.b64encode(jpeg).decode()})

    def test_jpeg_declared_jpeg(self) -> None:
        jpeg = _jpeg()
        body = _translate(_src4(_env(jpeg, b"image/jpeg")))
        out = body["input"][-1]["output"]
        self.assertTrue(out[1]["image_url"].startswith("data:image/jpeg;"))

    def test_png_still_png(self) -> None:
        png = test_runtime.make_png(4, 4)
        body = _translate(_src4(_env(png, b"image/png")))
        out = body["input"][-1]["output"]
        self.assertTrue(out[1]["image_url"].startswith("data:image/png;"))

    def test_jpeg_rejections(self) -> None:
        jpeg = _jpeg()
        png = test_runtime.make_png(4, 4)
        cases = [
            _env(jpeg[:-10]),
            _env(b"\xff\xd8\xff" + b"junk" * 16 + b"\xff\xd9"),
            _env(png, b"image/jpeg"),
            _env(png, b"image/svg+xml"),
            _env(jpeg, b"image/svg+xml"),
        ]
        for payload in cases:
            with self.subTest(payload=payload[:20]):
                with self.assertRaises(UnsupportedRequest) as caught:
                    _translate(_src4(payload))
                self.assertEqual(str(caught.exception), ERR)


@unittest.skipUnless(HAVE_PIL, JPEG_REASON)
class CheckedImageTest(unittest.TestCase):

    def test_dimension_limit_before_load(self) -> None:
        jpeg = _jpeg()
        with mock.patch.object(images, "MAX_DIMENSION", 8), \
                mock.patch.object(Image.Image, "load") as m:
            with self.assertRaises(artifacts.ArtifactError):
                images.checked_image(jpeg, "image/jpeg")
        m.assert_not_called()

    def test_byte_bound(self) -> None:
        jpeg = _jpeg()
        with mock.patch.object(images, "MAX_ARTIFACT_BYTES", 64):
            with self.assertRaises(artifacts.ArtifactError):
                images.checked_image(jpeg, "image/jpeg")

    def test_checked_image_reports_jpeg_dimensions(self) -> None:
        mime, size = images.checked_image(_jpeg(16, 24), "image/jpeg")
        self.assertEqual((mime, size), ("image/jpeg", (16, 24)))

    def test_conversion_validates_and_preserves_pixels(self) -> None:
        jpeg = _jpeg()
        png, mime = images.image_as_png(jpeg, "image/jpeg")
        self.assertEqual(mime, "image/jpeg")
        self.assertEqual(artifacts.validate_png(png), (16, 16))
        with Image.open(io.BytesIO(png)) as a, \
                Image.open(io.BytesIO(jpeg)) as b:
            self.assertEqual(list(a.convert("RGB").getdata()),
                             list(b.convert("RGB").getdata()))

    def test_exif_orientation_not_transposed(self) -> None:
        exif = Image.Exif()
        exif[0x0112] = 6
        jpeg = _jpeg(16, 24, exif=exif)
        png, _ = images.image_as_png(jpeg, "image/jpeg")
        self.assertEqual(artifacts.validate_png(png), (16, 24))
        with Image.open(io.BytesIO(png)) as a, \
                Image.open(io.BytesIO(jpeg)) as b:
            self.assertEqual(list(a.convert("RGB").getdata()),
                             list(b.convert("RGB").getdata()))


@unittest.skipUnless(HAVE_PIL, JPEG_REASON)
class RuntimeImageTest(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = os.path.join(self._tmp.name)
        self.fake = os.path.join(root, "fake_runtime.py")
        with open(self.fake, "w") as f:
            f.write(test_runtime.FAKE)
        self.outdir = os.path.join(root, "out")
        os.mkdir(self.outdir)
        self.approvals = ApprovalManager()
        self.addCleanup(self.approvals.close)
        self.binding = ExecutionBinding("user-1", "sess-1", "lead", 1)

    def _adapter(self, data_b64: str) -> CuaRuntimeAdapter:
        return CuaRuntimeAdapter(
            self.approvals, self.binding, "com.example.FusionRelayFixture",
            command=[sys.executable, self.fake, "good", data_b64,
                     self.outdir])

    def _observe(self, ad):
        out = []

        def run():
            try:
                out.append(ad.observe("com.example.FusionRelayFixture"))
            except Exception as e:
                out.append(e)

        th = threading.Thread(target=run)
        th.start()
        deadline = time.monotonic() + 10
        ticket = None
        while time.monotonic() < deadline:
            pend = self.approvals.pending()
            if pend:
                ticket = pend[0]
                break
            if not th.is_alive():
                return out[0]
            time.sleep(0.02)
        self.assertIsNotNone(ticket)
        self.assertTrue(self.approvals.decide(
            ticket["request_id"], "allow_once", ticket["revision"]))
        th.join(10)
        return out[0]

    def test_jpeg_declared_png_converted_and_counted(self) -> None:
        jpeg = _jpeg()
        ad = self._adapter(base64.b64encode(jpeg).decode())
        ad.start()
        self.addCleanup(ad.close)
        res = self._observe(ad)
        self.assertIsInstance(res, dict)
        self.assertEqual(artifacts.validate_png(res["png"]), (16, 16))
        self.assertEqual(res["source_mime_type"], "image/jpeg")
        self.assertEqual(res["source_image_sha256"],
                         hashlib.sha256(jpeg).hexdigest())
        self.assertEqual(ad.image_mime_corrections, 1)
        c = ad.compatibility()
        self.assertEqual(c["image_mime_corrections"], 1)
        self.assertEqual(c["image_formats"], ["png", "jpeg"])
        self.assertEqual(c["image_transform"],
                         "JPEG decoded to RGB PNG without resizing")

    def test_png_unchanged_path_no_correction(self) -> None:
        png = test_runtime.make_png()
        ad = self._adapter(base64.b64encode(png).decode())
        ad.start()
        self.addCleanup(ad.close)
        res = self._observe(ad)
        self.assertIsInstance(res, dict)
        self.assertEqual(res["png"], png)
        self.assertEqual(res["source_mime_type"], "image/png")
        self.assertEqual(res["source_image_sha256"],
                         hashlib.sha256(png).hexdigest())
        self.assertEqual(ad.image_mime_corrections, 0)


if __name__ == "__main__":
    unittest.main()
