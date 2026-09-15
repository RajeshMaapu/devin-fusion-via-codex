"""ArtifactStore + PNG-subset validation tests — temp dirs, stdlib only."""

import base64
import binascii
import os
import pathlib
import stat
import struct
import tempfile
import threading
import unittest
import zlib

from fusion_relay import artifacts
from fusion_relay.artifacts import ArtifactStore, ToolResult


def _chunk(ctype: bytes, body: bytes, bad_crc=False) -> bytes:
    crc = binascii.crc32(ctype + body) & 0xFFFFFFFF
    if bad_crc:
        crc ^= 1
    return struct.pack(">I", len(body)) + ctype + body \
        + struct.pack(">I", crc)


def make_png(w=2, h=2, color=6, filt=0, extra_raw=b"",
             bitdepth=8, interlace=0) -> bytes:
    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, bitdepth,
                                       color, 0, 0, interlace))
    channels = {0: 1, 2: 3, 6: 4}[color]
    row = bytes([filt]) + b"\x10" * (w * channels)
    raw = row * h + extra_raw
    idat = _chunk(b"IDAT", zlib.compress(raw))
    return sig + ihdr + idat + _chunk(b"IEND", b"")



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


class _Clock:
    def __init__(self):
        self.t = 5000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class PngValidationTest(unittest.TestCase):
    def test_valid_roundtrip_dims(self):
        self.assertEqual(artifacts.validate_png(make_png(3, 4)), (3, 4))
        self.assertEqual(artifacts.validate_png(make_png(2, 2, 2)),
                         (2, 2))

    def test_rejects(self):
        good = make_png()
        cases = [
            b"",                                  # empty
            b"not a png at all",                  # bad signature
            good[:8],                             # signature only
            good[:-4],                            # truncated crc
            good + b"tail",                       # trailing data
            good[:-16],                           # missing IEND
            _chunk(b"IHDR", struct.pack(          # bad CRC
                ">IIBBBBB", 2, 2, 8, 6, 0, 0, 0), bad_crc=True),
            make_png(color=0),                    # grayscale unsupported
            make_png(bitdepth=16),                # depth unsupported
            make_png(interlace=1),                # interlaced
            make_png(filt=5),                     # bad filter byte
            make_png(extra_raw=b"\x00" + b"\x10" * 18),  # extra row
        ]
        for i, data in enumerate(cases):
            with self.assertRaises(artifacts.ArtifactError, msg=i):
                artifacts.validate_png(data)

    def test_pixel_bomb_dims(self):
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(make_png(8192, 8192))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(make_png(0, 2))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(make_png(9000, 2))

    def test_unknown_critical_chunk_rejected(self):
        sig = b"\x89PNG\r\n\x1a\n"
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        bad = sig + ihdr + _chunk(b"XYZW", b"x") \
            + _chunk(b"IEND", b"")
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)

    def test_ancillary_chunk_allowed(self):
        sig = b"\x89PNG\r\n\x1a\n"
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        row = b"\x00" + b"\x10" * 4
        ok = (sig + ihdr + _chunk(b"tEXt", b"k\x00v")
              + _chunk(b"IDAT", zlib.compress(row))
              + _chunk(b"IEND", b""))
        self.assertEqual(artifacts.validate_png(ok), (1, 1))

    def test_corrupt_zlib_rejected(self):
        sig = b"\x89PNG\r\n\x1a\n"
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        bad = (sig + ihdr + _chunk(b"IDAT", zlib.compress(b"\x00")[:-4])
               + _chunk(b"IEND", b""))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)

    def test_oversize_rejected(self):
        big = make_png() + b"x" * (artifacts.MAX_ARTIFACT_BYTES)
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(big)

    def _minimal(self, *chunks):
        return b"\x89PNG\r\n\x1a\n" + b"".join(chunks)

    def test_short_zlib_output_rejected(self):
        # 1x1 RGBA expects 5 bytes of scanlines; give 1
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        bad = self._minimal(
            ihdr, _chunk(b"IDAT", zlib.compress(b"\x00")),
            _chunk(b"IEND", b""))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)

    def test_noncontiguous_idat_rejected(self):
        row = zlib.compress(b"\x00" + b"\x10" * 4)
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        half = len(row) // 2
        bad = self._minimal(
            ihdr, _chunk(b"IDAT", row[:half]),
            _chunk(b"tEXt", b"k\x00v"), _chunk(b"IDAT", row[half:]),
            _chunk(b"IEND", b""))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)

    def test_ancillary_before_ihdr_rejected(self):
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        bad = self._minimal(
            _chunk(b"tEXt", b"k\x00v"), ihdr,
            _chunk(b"IDAT", zlib.compress(b"\x00" + b"\x10" * 4)),
            _chunk(b"IEND", b""))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)

    def test_reserved_bit_chunk_name_rejected(self):
        ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6,
                                           0, 0, 0))
        bad = self._minimal(
            ihdr, _chunk(b"ABcD", b"x"),
            _chunk(b"IDAT", zlib.compress(b"\x00" + b"\x10" * 4)),
            _chunk(b"IEND", b""))
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.validate_png(bad)


class StoreTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = pathlib.Path(self._tmp.name) / "art"
        self.clock = _Clock()
        self.store = ArtifactStore(self.root, clock=self.clock)
        self.addCleanup(self.store.close)

    def test_put_get_roundtrip_and_part(self):
        png = make_png()
        art = self.store.put_png("s1", "op1", png)
        self.assertEqual((art.width, art.height), (2, 2))
        got, data = self.store.get("s1", art.ref)
        self.assertEqual(data, png)
        self.assertEqual(got.sha256, art.sha256)
        part = self.store.image_part("s1", art.ref)
        self.assertEqual(part["type"], "input_image")
        self.assertEqual(part["detail"], "original")
        raw = part["image_url"].split(",", 1)[1]
        self.assertEqual(base64.b64decode(raw), png)

    def test_permissions_umask0(self):
        old = os.umask(0)
        try:
            store = ArtifactStore(pathlib.Path(self._tmp.name) / "p",
                                  clock=self.clock)
            art = store.put_png("s", "o", make_png())
            for p in (store._root,
                      store._root / "artifacts.db",
                      store._root / f"{art.ref}.png"):
                mode = stat.S_IMODE(os.stat(p).st_mode)
                self.assertEqual(
                    mode, 0o700 if p.is_dir() else 0o600, str(p))
            store.close()
        finally:
            os.umask(old)

    def test_wrong_scope_and_bad_ref(self):
        art = self.store.put_png("s1", "o", make_png())
        with self.assertRaises(artifacts.ArtifactError):
            self.store.get("other", art.ref)
        with self.assertRaises(artifacts.ArtifactError):
            self.store.get("s1", "zz")
        with self.assertRaises(artifacts.ArtifactError):
            self.store.get("s1", "../../etc/passwd")

    def test_expired_fails(self):
        art = self.store.put_png("s1", "o", make_png(), ttl=10)
        self.clock.advance(11)
        with self.assertRaises(artifacts.ArtifactExpired):
            self.store.get("s1", art.ref)

    def test_tampered_bytes_fail(self):
        art = self.store.put_png("s1", "o", make_png())
        path = self.root / f"{art.ref}.png"
        path.write_bytes(path.read_bytes()[:-4] + b"\x00" * 4)
        with self.assertRaises(artifacts.ArtifactError):
            self.store.get("s1", art.ref)

    def test_symlink_target_rejected(self):
        art = self.store.put_png("s1", "o", make_png())
        path = self.root / f"{art.ref}.png"
        target = pathlib.Path(self._tmp.name) / "real.png"
        target.write_bytes(make_png())
        path.unlink()
        os.symlink(target, path)
        with self.assertRaises(OSError):
            self.store.get("s1", art.ref)

    def test_quota_enforced_no_eviction(self):
        first = make_png(2, 2)
        small = ArtifactStore(pathlib.Path(self._tmp.name) / "q",
                              clock=self.clock,
                              max_bytes=len(first) + 10)
        self.addCleanup(small.close)
        small.put_png("s", "o1", first)
        with self.assertRaises(artifacts.ResourceExhausted):
            small.put_png("s", "o2", make_png(4, 4))
        # existing artifact untouched
        self.assertTrue((small._root / "artifacts.db").exists())
        rows = _ldb1(
            small, "SELECT COUNT(*) FROM artifacts")[0]
        self.assertEqual(rows, 1)

    def test_concurrent_puts_unique_refs(self):
        refs, errs = [], []
        def put(i):
            try:
                refs.append(
                    self.store.put_png("s", f"op{i}", make_png()).ref)
            except Exception as e:
                errs.append(e)
        ts = [threading.Thread(target=put, args=(i,)) for i in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertFalse(errs)
        self.assertEqual(len(set(refs)), 6)

    def test_bytearray_snapshot(self):
        buf = bytearray(make_png())
        art = self.store.put_png("s", "o", buf)
        buf[:] = b"\x00" * len(buf)  # mutate caller buffer
        _, data = self.store.get("s", art.ref)
        self.assertEqual(data, make_png())
        with self.assertRaises(artifacts.ArtifactError):
            self.store.put_png("s", "o", "not bytes")

    def test_zero_write_rejected(self):
        import unittest.mock as mock
        with mock.patch.object(artifacts.os, "write",
                               return_value=0):
            with self.assertRaises(OSError):
                self.store.put_png("s", "o", make_png())
        self.assertFalse(list(self.root.glob("*.png")))

    def test_uuid_collision_preserves_existing(self):
        import unittest.mock as mock
        art = self.store.put_png("s", "o1", make_png())
        before = (self.root / f"{art.ref}.png").read_bytes()
        with mock.patch.object(artifacts.uuid, "uuid4") as m:
            m.return_value.hex = art.ref  # force collision
            with self.assertRaises(artifacts.ArtifactError):
                self.store.put_png("s", "o2", make_png())
        self.assertEqual(
            (self.root / f"{art.ref}.png").read_bytes(), before)
        got, _ = self.store.get("s", art.ref)
        self.assertEqual(got.operation_id, "o1")

    def test_ambiguous_commit_poisons_store(self):
        import sqlite3
        import unittest.mock as mock
        real = self.store._db

        class _FailCommit:
            def execute(self, sql, *a):
                if sql == "COMMIT":
                    raise sqlite3.OperationalError("disk I/O")
                return real.execute(sql, *a)

            def __getattr__(self, n):
                return getattr(real, n)
        self.store._db = _FailCommit()
        with self.assertRaises(sqlite3.OperationalError):
            self.store.put_png("s", "o", make_png())
        self.assertTrue(self.store._failed)
        self.store._db = real
        # the store stays failed; the ambiguous file is preserved
        with self.assertRaises(artifacts.ArtifactError):
            self.store.put_png("s", "o2", make_png())
        orphans = list(self.root.glob("*.png"))
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0].read_bytes(), make_png())

    def test_directory_fsync_before_commit(self):
        import unittest.mock as mock
        fds = []
        real_fsync = os.fsync
        with mock.patch.object(artifacts.os, "fsync",
                               side_effect=lambda fd: (
                                   fds.append(fd), real_fsync(fd))):
            self.store.put_png("s", "o", make_png())
        self.assertGreaterEqual(len(fds), 2)  # file + directory


class EncodeResultTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = ArtifactStore(pathlib.Path(self._tmp.name),
                                   clock=_Clock())
        self.addCleanup(self.store.close)

    def test_whitelist_and_shape(self):
        art = self.store.put_png("s1", "op1", make_png())
        res = ToolResult("op1", "succeeded",
                         ("did it",), (art.ref,))
        parts = artifacts.encode_result(res, self.store, "s1")
        self.assertEqual(parts[0]["type"], "input_text")
        self.assertIn('"succeeded"', parts[0]["text"])
        self.assertEqual(parts[1]["text"], "did it")
        self.assertEqual(parts[2]["type"], "input_image")

    def test_rejects(self):
        art = self.store.put_png("s1", "op1", make_png())
        art2 = self.store.put_png("s1", "op2", make_png())
        for res in (
                ToolResult("op1", "weird"),
                ToolResult("op1", "succeeded", ("x" * 70000,)),
                # aggregate bound: two 40 KiB blocks exceed 64 KiB total
                ToolResult("op1", "succeeded",
                           ("x" * 40000, "y" * 40000)),
                ToolResult("op1", "succeeded", ("",)),
                ToolResult("op1", "succeeded", "not-a-tuple"),
                ToolResult("", "succeeded", ("t",)),
                ToolResult("op1", "succeeded", (), (123,)),
                ToolResult("op1", "succeeded", (),
                           tuple([art.ref] * 5)),
                ToolResult("op1", "succeeded", (), (art2.ref,))):
            with self.assertRaises(artifacts.ArtifactError, msg=res):
                artifacts.encode_result(res, self.store, "s1")
        with self.assertRaises(artifacts.ArtifactError):
            artifacts.encode_result(
                ToolResult("op1", "succeeded", (), (art.ref,)),
                self.store, "other-scope")

    def test_max_bytes_validated(self):
        for bad in (0, -5, True, "big"):
            with self.assertRaises(artifacts.ArtifactError, msg=bad):
                ArtifactStore(pathlib.Path(self._tmp.name) / str(bad),
                              max_bytes=bad)


if __name__ == "__main__":
    unittest.main()
