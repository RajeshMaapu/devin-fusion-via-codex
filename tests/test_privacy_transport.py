"""Privacy, bounded-transport, storage, and read-only auth tests.

Everything here uses fake sockets/responses/credentials and temporary
directories — no live auth, network, or service.
"""

import base64
import contextlib
import gzip
import io
import json
import os
import pathlib
import stat
import tempfile
import threading
import unittest
from unittest.mock import patch

from fusion_relay import auth, catalog, relay, storage, translate, wire


class _Hdrs:
    """Minimal email.message stand-in for _read_request_body."""

    def __init__(self, pairs):
        self._pairs = list(pairs)

    def get(self, key, default=None):
        for k, v in self._pairs:
            if k.lower() == key.lower():
                return v
        return default

    def get_all(self, key):
        vals = [v for k, v in self._pairs if k.lower() == key.lower()]
        return vals or None  # email.message semantics: absent -> None


class _Reader:
    def __init__(self, data: bytes):
        self._b = io.BytesIO(data)

    def read1(self, n=-1):
        return self._b.read(n)


def _handler(pairs, data=b""):
    from unittest.mock import Mock
    h = relay.Handler.__new__(relay.Handler)
    h.headers = _Hdrs(pairs)
    h.rfile = _Reader(data)
    h.connection = Mock()
    return h


class RequestBodyTest(unittest.TestCase):
    def test_exact_length_read(self):
        status, body = _handler(
            [("Content-Length", "5")], b"hello")._read_request_body()
        self.assertEqual((status, body), (0, b"hello"))

    def test_transfer_encoding_rejected(self):
        status, _ = _handler(
            [("Transfer-Encoding", "chunked"),
             ("Content-Length", "0")])._read_request_body()
        self.assertEqual(status, 400)

    def test_empty_transfer_encoding_rejected(self):
        status, _ = _handler(
            [("Transfer-Encoding", ""),
             ("Content-Length", "0")])._read_request_body()
        self.assertEqual(status, 400)

    def test_unicode_digits_rejected(self):
        status, _ = _handler(
            [("Content-Length", "١٢٣")], b"")._read_request_body()
        self.assertEqual(status, 400)

    def test_absurd_content_length_rejected(self):
        status, _ = _handler(
            [("Content-Length", "9" * 10000)],
            b"")._read_request_body()
        self.assertEqual(status, 400)

    def test_stalled_read_is_408(self):
        import socket as _s

        class Stall:
            def read1(self, n=-1):
                raise _s.timeout()

        from unittest.mock import Mock
        h = relay.Handler.__new__(relay.Handler)
        h.headers = _Hdrs([("Content-Length", "10")])
        h.rfile = Stall()
        h.connection = Mock()
        status, _ = h._read_request_body()
        self.assertEqual(status, 408)

    def test_content_encoding_rejected(self):
        status, _ = _handler(
            [("Content-Encoding", "gzip"),
             ("Content-Length", "0")])._read_request_body()
        self.assertEqual(status, 400)

    def test_duplicate_content_length_rejected(self):
        status, _ = _handler(
            [("Content-Length", "1"), ("Content-Length", "1")],
            b"x")._read_request_body()
        self.assertEqual(status, 400)

    def test_non_numeric_content_length_rejected(self):
        status, _ = _handler(
            [("Content-Length", "1x")], b"x")._read_request_body()
        self.assertEqual(status, 400)

    def test_missing_content_length_rejected(self):
        status, _ = _handler([])._read_request_body()
        self.assertEqual(status, 400)

    def test_oversize_rejected(self):
        status, _ = _handler(
            [("Content-Length", str(relay.MAX_REQUEST_BYTES + 1))],
            b"")._read_request_body()
        self.assertEqual(status, 413)

    def test_short_body_rejected(self):
        status, _ = _handler(
            [("Content-Length", "10")], b"abc")._read_request_body()
        self.assertEqual(status, 400)


class ForwardBoundsTest(unittest.TestCase):
    def test_capped_read_rejects_oversize(self):
        big = io.BytesIO(b"x" * (64 << 20 | 1))
        self.assertRaises(ValueError, relay._read_capped, big)

    def test_response_headers_reject_trailer(self):
        class R:
            headers = {"Content-Type": "application/proto",
                       "Trailer": "grpc-status"}
        self.assertRaises(ValueError, relay._response_headers, R())

    def test_response_headers_reject_content_encoding(self):
        class R:
            headers = {"Content-Type": "application/proto",
                       "Content-Encoding": "gzip"}
        self.assertRaises(ValueError, relay._response_headers, R())

    def test_response_headers_pass_end_to_end_only(self):
        class R:
            headers = {"Connect-Protocol-Version": "1",
                       "Grpc-Status": "0",
                       "X-Internal-Debug": "secret",
                       "Content-Length": "3"}
        out = relay._response_headers(R())
        self.assertEqual(out, {"connect-protocol-version": "1",
                               "grpc-status": "0"})


class BoundedDecompressTest(unittest.TestCase):
    def _gz(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def test_roundtrip(self):
        self.assertEqual(wire.bounded_decompress(self._gz(b"hi")), b"hi")

    def test_over_limit_rejected(self):
        self.assertRaises(ValueError, wire.bounded_decompress,
                          self._gz(b"x" * 100), 10)

    def test_truncated_rejected(self):
        self.assertRaises(ValueError, wire.bounded_decompress,
                          self._gz(b"hello")[:-4])

    def test_concatenated_members_rejected(self):
        self.assertRaises(ValueError, wire.bounded_decompress,
                          self._gz(b"a") + self._gz(b"b"))

    def test_trailing_garbage_rejected(self):
        self.assertRaises(ValueError, wire.bounded_decompress,
                          self._gz(b"a") + b"junk")

    def test_not_gzip_rejected(self):
        self.assertRaises(ValueError, wire.bounded_decompress, b"plain")


class DecodePacketsTest(unittest.TestCase):
    def test_raw_protobuf_explicit(self):
        body = wire.field(21, "m")
        self.assertEqual(list(relay._decode_packets(
            body, framed=False)), [wire.decode(body)])

    def test_framed_with_trailer(self):
        body = wire.frame(wire.field(21, "m")) + wire.end_stream()
        msgs = list(relay._decode_packets(body, framed=True))
        self.assertEqual(wire.text(msgs[0], 21), "m")

    def test_framed_parse_error_no_fallback(self):
        self.assertRaises(ValueError, list,
                          relay._decode_packets(b"\x00\x00\x00\x00\x09abc"))

    def test_sniffed_framed(self):
        body = wire.frame(wire.field(21, "m")) + wire.end_stream()
        msgs = list(relay._decode_packets(body))
        self.assertEqual(len(msgs), 1)

    def test_malformed_trailer_rejected(self):
        body = wire.frame(wire.field(1, "x")) + wire.frame(b"[1]", 0x02)
        self.assertRaises(ValueError, list,
                          relay._decode_packets(body, framed=True))

    def test_shared_budget_across_frames(self):
        small = 64
        payload = wire.field(1, "x" * 40)
        gz = gzip.compress(payload)
        body = wire.frame(payload) + wire.frame(gz, 0x01) \
            + wire.end_stream()
        with patch.object(relay, "MAX_REQUEST_BYTES", small):
            self.assertRaises(ValueError, list,
                              relay._decode_packets(body, framed=True))
        # under the real cap it decodes both frames
        msgs = list(relay._decode_packets(body, framed=True))
        self.assertEqual(len(msgs), 2)

    def test_trailer_error_rejected(self):
        err = json.dumps({"error": {"code": "x"}}).encode()
        body = wire.frame(wire.field(1, "x")) + wire.frame(err, 0x02)
        self.assertRaises(ValueError, list,
                          relay._decode_packets(body, framed=True))


class _FakeSSE:
    def __init__(self, lines):
        self._lines = list(lines)
        self._pending = b""
        self.status = 200
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

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.closed = True

    def close(self):
        self.closed = True


def _sse(events):
    return _FakeSSE([b"data: " + json.dumps(e).encode() + b"\n"
                     for e in events])


class CodexTransportTest(unittest.TestCase):
    def setUp(self):
        translate.reset()
        self.addCleanup(translate.reset)

    def test_oversized_sse_line_rejected(self):
        fake = _FakeSSE([b"x" * (1 << 20 | 1) + b"\n"])
        with patch.object(translate.auth, "get_token",
                          return_value=("t", "acct")), \
                patch("urllib.request.urlopen", return_value=fake):
            self.assertRaises(RuntimeError,
                              translate.call_codex, {"input": []}, {})

    def test_pre_request_cancellation_no_usage(self):
        ctx = translate.RequestCancelled
        calls = []

        def check():
            raise ctx("request_cancelled")

        rec = {}
        with patch.object(translate.auth, "get_token",
                          side_effect=AssertionError), \
                patch("urllib.request.urlopen",
                      side_effect=lambda *a, **k: calls.append(1)):
            self.assertRaises(ctx, translate.call_codex,
                              {"input": []}, rec, check_cancelled=check)
        self.assertEqual(calls, [])
        self.assertNotIn("codex_usage", rec)

    def test_midstream_cancellation_records_cancelled(self):
        class CancelAfterOne(_FakeSSE):
            def __init__(self):
                super().__init__([b"data: " + json.dumps(
                    {"type": "response.output_text.delta",
                     "delta": "hi"}).encode() + b"\n",
                    b"data: [DONE]\n"])

        rec = {}
        ctx_holder = {"n": 0}

        def check():
            ctx_holder["n"] += 1
            if ctx_holder["n"] > 2:
                raise translate.RequestCancelled("request_cancelled")

        with patch.object(translate.auth, "get_token",
                          return_value=("t", "acct")), \
                patch("urllib.request.urlopen",
                      return_value=CancelAfterOne()):
            self.assertRaises(translate.RequestCancelled,
                              translate.call_codex, {"input": []}, rec,
                              check_cancelled=check)
        self.assertTrue(rec.get("client_gone"))
        self.assertEqual(rec["codex_status"], "cancelled")
        self.assertEqual(rec["codex_usage"]["unknown_calls"], 1)

    def test_quota_headers_allowlist_only(self):
        fake = _sse([{"type": "response.completed", "response": {
            "id": "r1", "status": "completed",
            "output": [{"type": "message", "id": "m", "role": "assistant",
                        "content": [{"type": "output_text", "text": "hi"}]}],
            "usage": {"input_tokens": 1, "output_tokens": 1}}}])
        fake.headers = {
            "x-codex-primary-used-percent": "42.5",
            "x-codex-secondary-window-minutes": "300",
            "x-secret-header": "leak",
            "x-codex-primary-reset-at": "not-a-number"}
        rec = {}
        with patch.object(translate.auth, "get_token",
                          return_value=("t", "acct")), \
                patch("urllib.request.urlopen", return_value=fake):
            translate.call_codex({"input": []}, rec)
        self.assertEqual(rec["codex_quota_snapshot"],
                         {"x-codex-primary-used-percent": 42.5,
                          "x-codex-secondary-window-minutes": 300.0})

    def test_http_error_no_body_read(self):
        class Guard(io.BytesIO):
            def read(self, *a):
                raise AssertionError("error body must not be read")

            def close(self):
                pass

        err = translate.urllib.error.HTTPError(
            "u", 429, "rate", hdrs=None, fp=Guard(b"secrets"))
        rec = {}
        with patch.object(translate.auth, "get_token",
                          return_value=("t", "acct")), \
                patch("urllib.request.urlopen", side_effect=err):
            self.assertRaises(RuntimeError,
                              translate.call_codex, {"input": []}, rec)
        self.assertEqual(rec["codex_http_status"], 429)
        self.assertEqual(rec["codex_status"], "failed")


class SafeRecordTest(unittest.TestCase):
    def test_allowlist_projection(self):
        rec = {
            "route": "codex",
            "rpc": "/xexerra.chat.v1.ChatService/GetChatMessage",
            "model": "gpt-6-astra",
            "prompt": "user prompt text",
            "request_head": "deadbeef",
            "request_numbers": [1, 2],
            "error": "provider said secret things",
            "exception": "traceback-y details",
            "ms": 12.5, "bytes": 100, "n_messages": 2,
            "upstream_status": 200, "delta_chars": 7,
            "relay_tool_calls": 1,
            "client_gone": False, "relay_tool_loop_bound": True,
            "codex_status": "completed",
            "codex_usage": {"input_tokens": 10, "output_tokens": 2,
                            "cached_tokens": 1, "reasoning_tokens": 0},
            "codex_usage_calls": [{
                "input_tokens": 10, "output_tokens": 2,
                "response_ref": "a" * 64, "role": "lead",
                "status": "completed", "identified": True,
                "secret_field": "drop-me"},
                {"response_ref": "raw-response-id", "role": "superuser"}],
        }
        out = relay.safe_record(rec)
        for forbidden in ("model", "prompt", "request_head",
                          "request_numbers", "error", "exception",
                          "effort", "session_route"):
            self.assertNotIn(forbidden, out)
        self.assertEqual(out["rpc"], "GetChatMessage")
        self.assertEqual(out["codex_usage"]["input_tokens"], 10)
        calls = out["codex_usage_calls"]
        self.assertEqual(calls[0]["response_ref"], "a" * 64)
        self.assertNotIn("secret_field", calls[0])
        self.assertIsNone(calls[1]["response_ref"])
        self.assertEqual(calls[1]["role"], "unverified")

    def test_unknown_rpc_other_and_enums_enforced(self):
        out = relay.safe_record({"rpc": "/weird/Path",
                                 "route": "bogus",
                                 "codex_status": "weird",
                                 "error_category": "weird",
                                 "ms": float("nan"), "bytes": -1})
        self.assertEqual(out["rpc"], "other")
        self.assertEqual(out["route"], "other")
        self.assertNotIn("ms", out)
        self.assertNotIn("bytes", out)
        self.assertNotIn("codex_status", out)
        self.assertNotIn("error_category", out)


class LogPrivacyTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.log = pathlib.Path(self._tmpdir.name) / "requests.jsonl"
        self.stats = pathlib.Path(self._tmpdir.name) / "stats.json"
        p1 = patch.object(relay, "REQUESTS_LOG", self.log)
        p2 = patch.object(relay, "STATS_PATH", self.stats)
        p3 = patch.object(relay, "DATA_DIR",
                          pathlib.Path(self._tmpdir.name))
        self.addCleanup(p1.stop)
        self.addCleanup(p2.stop)
        self.addCleanup(p3.stop)
        p1.start()
        p2.start()
        p3.start()
        import copy
        self._saved_stats = copy.deepcopy(relay._stats)
        self.addCleanup(
            lambda: relay._stats.update(self._saved_stats))

    def test_log_line_is_sanitized(self):
        relay._log_record({
            "route": "codex", "rpc": "/s/GetChatMessage",
            "prompt": "SECRET-PROMPT", "error": "SECRET-ERR",
            "model": "SECRET-MODEL",
            "codex_usage": {"input_tokens": 3, "output_tokens": 1}})
        line = self.log.read_text()
        for secret in ("SECRET-PROMPT", "SECRET-ERR", "SECRET-MODEL"):
            self.assertNotIn(secret, line)
        rec = json.loads(line)
        self.assertEqual(rec["rpc"], "GetChatMessage")
        self.assertIn("ts", rec)
        st = json.loads(self.stats.read_text())
        self.assertEqual(st["tokens"]["codex"]["input"], 3)

    def test_stats_write_failure_marks_unavailable(self):
        self.log.touch()
        with patch.object(relay, "atomic_write",
                          side_effect=OSError("disk gone")):
            relay._log_record({"route": "codex", "rpc": "/s/GetChatMessage"})
        self.assertEqual(relay._stats["persistence"], "unavailable")


class StorageTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.root = pathlib.Path(self._tmpdir.name)

    def test_ensure_private_dir_modes(self):
        old = os.umask(0)
        try:
            d = storage.ensure_private_dir(self.root / "leaf")
        finally:
            os.umask(old)
        self.assertEqual(stat.S_IMODE(os.stat(d).st_mode), 0o700)

    def test_ensure_private_dir_rejects_symlink(self):
        target = self.root / "real"
        target.mkdir()
        link = self.root / "link"
        link.symlink_to(target)
        self.assertRaises(OSError, storage.ensure_private_dir, link)

    def test_ensure_private_dir_rejects_file(self):
        f = self.root / "file"
        f.write_text("x")
        self.assertRaises(OSError, storage.ensure_private_dir, f)

    def test_atomic_write_mode_and_replace(self):
        storage.ensure_private_dir(self.root)
        f = self.root / "data"
        storage.atomic_write(f, b"one")
        storage.atomic_write(f, b"two")
        self.assertEqual(f.read_bytes(), b"two")
        self.assertEqual(stat.S_IMODE(os.stat(f).st_mode), 0o600)
        self.assertFalse([p for p in self.root.iterdir()
                          if p.name.startswith(".relay-")])

    def test_atomic_write_failure_cleans_temp(self):
        storage.ensure_private_dir(self.root)
        f = self.root / "data"
        with patch("os.replace", side_effect=OSError("boom")):
            self.assertRaises(OSError, storage.atomic_write, f, b"x")
        self.assertFalse([p for p in self.root.iterdir()
                          if p.name.startswith(".relay-")])
        self.assertFalse(f.exists())

    def test_read_private_bounds_and_symlink(self):
        storage.ensure_private_dir(self.root)
        f = self.root / "data"
        storage.atomic_write(f, b"0123456789")
        self.assertEqual(storage.read_private(f, 64), b"0123456789")
        # Oversized content raises — a truncated prefix is never returned.
        self.assertRaises(OSError, storage.read_private, f, 4)
        outside = self.root / "target"
        outside.write_text("x")
        link = self.root / "link"
        link.symlink_to(outside)
        self.assertRaises(OSError, storage.read_private, link, 10)

    def test_append_private_appends_and_modes(self):
        storage.ensure_private_dir(self.root)
        f = self.root / "log"
        storage.append_private(f, b"a\n")
        storage.append_private(f, b"b\n")
        self.assertEqual(f.read_bytes(), b"a\nb\n")
        self.assertEqual(stat.S_IMODE(os.stat(f).st_mode), 0o600)
        link = self.root / "link2"
        link.symlink_to(f)
        self.assertRaises(OSError, storage.append_private, link, b"x")

    def test_append_private_partial_writes(self):
        storage.ensure_private_dir(self.root)
        f = self.root / "log"
        real_write = os.write
        calls = {"n": 0}

        def short(fd, data):
            calls["n"] += 1
            return real_write(fd, data[:1])

        with patch("os.write", side_effect=short):
            storage.append_private(f, b"abc")
        self.assertGreater(calls["n"], 1)
        self.assertEqual(f.read_bytes(), b"abc")


class TokenAndAuthTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.dir = pathlib.Path(self._tmpdir.name)
        self.token_path = self.dir / "relay-token"
        p1 = patch.object(relay, "TOKEN_PATH", self.token_path)
        p2 = patch.object(relay, "DATA_DIR", self.dir)
        p3 = patch.object(relay, "_RELAY_TOKEN", None)
        self.addCleanup(p1.stop)
        self.addCleanup(p2.stop)
        self.addCleanup(p3.stop)
        p1.start()
        p2.start()
        p3.start()

    def test_invalid_token_not_overwritten(self):
        for bad in ("short", "", "   \n", "x" * 300, "tok with spaces!!"):
            self.token_path.write_text(bad)
            self.assertRaises(RuntimeError, relay.relay_token)
            self.assertEqual(self.token_path.read_text(), bad)
            relay._RELAY_TOKEN = None

    def test_concurrent_creators_same_token(self):
        results, errors = [], []

        def create():
            try:
                results.append(relay.relay_token())
            except Exception as e:
                errors.append(e)
        threads = [threading.Thread(target=create) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        for t in threads:
            self.assertFalse(t.is_alive())
        self.assertFalse(errors)
        self.assertEqual(len(results), 8)
        self.assertEqual(len(set(results)), 1)
        self.assertGreaterEqual(len(results[0]), 16)

    def test_token_file_mode(self):
        relay.relay_token()
        self.assertEqual(stat.S_IMODE(os.stat(self.token_path).st_mode),
                         0o600)


class ReadOnlyAuthTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.auth_file = pathlib.Path(self._tmpdir.name) / "auth.json"
        patcher = patch.object(auth, "CODEX_AUTH", self.auth_file)
        self.addCleanup(patcher.stop)
        patcher.start()

    def _jwt(self, exp: float) -> str:
        seg = base64.urlsafe_b64encode(
            json.dumps({"exp": exp}).encode()).decode().rstrip("=")
        return f"h.{seg}.s"

    def _write(self, exp: float):
        self.auth_file.write_text(json.dumps({
            "auth_mode": "chatgpt",
            "tokens": {"access_token": self._jwt(exp),
                       "account_id": "acct-1"}}))

    def test_valid_token_returned(self):
        import time
        self._write(time.time() + 3600)
        token, account = auth.get_token()
        self.assertEqual(account, "acct-1")
        self.assertTrue(token)

    def test_expired_safe_error(self):
        import time
        self._write(time.time() - 10)
        with self.assertRaises(auth.AuthError) as cm:
            auth.get_token()
        self.assertIn("renew the login in Codex", str(cm.exception))
        self.assertNotIn(str(self.auth_file), str(cm.exception))

    def test_malformed_safe_error(self):
        self.auth_file.write_text("{oops")
        with self.assertRaises(auth.AuthError) as cm:
            auth.get_token()
        self.assertNotIn(str(self.auth_file), str(cm.exception))

    def test_missing_tokens_safe_error(self):
        self.auth_file.write_text(json.dumps({"auth_mode": "chatgpt"}))
        self.assertRaises(auth.AuthError, auth.get_token)

    def test_nonfinite_expiry_rejected(self):
        import time
        self._write(float("nan"))
        self.assertRaises(auth.AuthError, auth.get_token)
        self._write(float("inf"))
        self.assertRaises(auth.AuthError, auth.get_token)

    def test_no_writes_to_canonical_file(self):
        import time
        self._write(time.time() + 3600)
        before = self.auth_file.read_bytes()
        names_before = set(os.listdir(self._tmpdir.name))
        auth.get_token()
        self.assertEqual(self.auth_file.read_bytes(), before)
        self.assertEqual(set(os.listdir(self._tmpdir.name)), names_before)

    def test_refresh_always_fails(self):
        self.assertRaises(auth.AuthError, auth._refresh, {})


class _QuietServer(relay._BoundedServer):
    """Test server that records handler exceptions instead of printing."""

    def __init__(self, *a, **kw):
        self.handler_errors = []
        super().__init__(*a, **kw)

    def handle_error(self, request, client_address):
        import sys
        err = sys.exc_info()[1]
        # A client-closed/reset socket surfaces here after the response
        # was already sent — transport noise, not a handler defect.
        if isinstance(err, (ConnectionResetError, BrokenPipeError)):
            return
        self.handler_errors.append(err)


class ServerFixtureTest(unittest.TestCase):
    """Real loopback HTTP against _BoundedServer; all upstreams faked."""

    TOKEN = "test-token-123456789"
    RPC_BASE = "/xexerra.chat.v1.ChatService"

    def setUp(self):
        import copy
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        d = pathlib.Path(self._tmpdir.name)
        self._saved_stats = copy.deepcopy(relay._stats)
        self.addCleanup(lambda: relay._stats.update(self._saved_stats))
        patches = [
            patch.object(relay, "DATA_DIR", d),
            patch.object(relay, "TOKEN_PATH", d / "relay-token"),
            patch.object(relay, "REQUESTS_LOG", d / "requests.jsonl"),
            patch.object(relay, "STATS_PATH", d / "stats.json"),
            patch.object(relay, "_RELAY_TOKEN", self.TOKEN),
            patch.object(relay, "STREAM_MODE", "buffer"),
        ]
        for p in patches:
            self.addCleanup(p.stop)
            p.start()
        catalog.reset()
        self.addCleanup(catalog.reset)
        catalog.attach_store(d / "routes.json")
        auth_patch = patch.object(
            relay.auth, "get_token", return_value=("tok", "acct-1"))
        self.addCleanup(auth_patch.stop)
        auth_patch.start()
        self.server = _QuietServer(("127.0.0.1", 0), relay.Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05})
        self._thread.start()
        self.addCleanup(self._thread.join, 10)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def tearDown(self):
        self.assertEqual(self.server.handler_errors, [])

    def _conn(self):
        import http.client
        return http.client.HTTPConnection("127.0.0.1", self.port,
                                          timeout=10)

    def _post(self, rpc, body, ctype="application/proto", token=None):
        conn = self._conn()
        conn.request("POST", f"/t/{token or self.TOKEN}{rpc}",
                     body=body, headers={"Content-Type": ctype})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, data, resp.getheader("Content-Type")

    def _chat_body(self, model="gpt-6-astra-high", session="s1"):
        inner = wire.field(2, 1) + wire.field(3, "hi")
        return (wire.field(3, inner) + wire.field(16, session)
                + wire.field(21, model))

    def test_healthz_unauthenticated(self):
        conn = self._conn()
        conn.request("GET", "/healthz")
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        conn.close()

    def test_capabilities_explicitly_blocked(self):
        conn = self._conn()
        conn.request("GET", f"/t/{self.TOKEN}/capabilities")
        resp = conn.getresponse()
        caps = json.loads(resp.read())
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(caps["status"], "blocked")
        self.assertFalse(caps["computer_enabled"])
        self.assertEqual(caps["native_dispatch_enforcement"], "unsupported")
        self.assertEqual(caps["consent_ui"], "unavailable")
        self.assertFalse(caps["detached_tasks"])
        self.assertEqual(caps["operation_journal"], "local_contract_only")
        self.assertEqual(caps["image_feedback"], "vision_unavailable")

    def test_unauthorized_paths_rejected(self):
        conn = self._conn()
        conn.request("GET", "/capabilities")
        self.assertIn(conn.getresponse().status, (403, 404))
        conn.close()
        status, data, _ = self._post(f"{self.RPC_BASE}/GetUserStatus",
                                     b"", token="wrong-token-00000000")
        self.assertEqual(status, 403)

    def test_codex_inference_buffered(self):
        tail = wire.frame(wire.field(1, "r1")
                          + wire.field(6, wire.field(3, "hello"))) \
            + wire.end_stream()
        framed = wire.frame(self._chat_body()) + wire.end_stream()
        with patch.object(relay, "call_codex_with_tools",
                          return_value=tail) as m:
            status, data, ctype = self._post(
                f"{self.RPC_BASE}/GetChatMessage", framed,
                ctype="application/connect+proto")
        self.assertEqual(status, 200)
        self.assertEqual(data, tail)
        self.assertEqual(ctype, "application/connect+proto")
        self.assertEqual(m.call_count, 1)

    def test_native_forward_passthrough(self):
        body = wire.frame(self._chat_body(model="swe-2-medium")) \
            + wire.end_stream()
        echo = relay.ForwardResponse(
            200, b"upstream-bytes", "application/connect+proto", {})
        with patch.object(relay, "_forward", return_value=echo) as m:
            status, data, _ = self._post(
                f"{self.RPC_BASE}/GetChatMessage", body,
                ctype="application/connect+proto")
        self.assertEqual(status, 200)
        self.assertEqual(data, b"upstream-bytes")
        self.assertEqual(m.call_args[0][0], body)

    def test_assign_success_commits(self):
        req = wire.field(2, "gpt-6-astra-high-native") \
            + wire.field(3, "u-assign-1")
        ok = relay.ForwardResponse(200, wire.field(1, "done"),
                                   "application/proto", {})
        with patch.object(relay, "_forward", return_value=ok):
            status, _, _ = self._post(f"{self.RPC_BASE}/AssignModel", req)
        self.assertEqual(status, 200)
        packet = wire.decode(wire.field(16, "u-assign-1"))
        self.assertEqual(catalog.session_route(packet), "native")

    def test_assign_terminal_error_no_commit(self):
        req = wire.field(2, "gpt-6-astra-high-native") \
            + wire.field(3, "u-assign-2")
        err = relay.ForwardResponse(500, b"", "application/proto", {})
        with patch.object(relay, "_forward", return_value=err):
            status, _, _ = self._post(f"{self.RPC_BASE}/AssignModel", req)
        packet = wire.decode(wire.field(16, "u-assign-2"))
        self.assertIsNone(catalog.session_route(packet))

    def test_malformed_assign_never_forwarded(self):
        with patch.object(relay, "_forward") as m:
            status, data, _ = self._post(
                f"{self.RPC_BASE}/AssignModel", b"\xff\xff\xff")
        self.assertEqual(m.call_count, 0)
        self.assertIn(b"invalid_argument", data)

    def test_unknown_outcome_pending_blocks_inference(self):
        req = wire.field(2, "gpt-6-astra-high-native") \
            + wire.field(3, "u-assign-3")
        # Connect-framed response with no trailer -> outcome unknown
        odd = relay.ForwardResponse(200, wire.frame(wire.field(1, "x")),
                                    "application/connect+proto", {})
        with patch.object(relay, "_forward", return_value=odd):
            status, data, _ = self._post(f"{self.RPC_BASE}/AssignModel",
                                         req)
        self.assertIn(b"selection_unconfirmed", data)
        # The session stays pending — inference for it must not run.
        with patch.object(relay, "call_codex_with_tools") as m:
            framed = wire.frame(
                self._chat_body(session="u-assign-3")) + wire.end_stream()
            status, data, _ = self._post(
                f"{self.RPC_BASE}/GetChatMessage", framed,
                ctype="application/connect+proto")
        self.assertIn(b"selection_unconfirmed", data)
        self.assertEqual(m.call_count, 0)

    def test_route_store_failure_blocks_forward(self):
        req = wire.field(2, "gpt-6-astra-high-native") \
            + wire.field(3, "u-assign-4")
        with patch.object(catalog, "atomic_write",
                          side_effect=OSError("disk gone")), \
                patch.object(relay, "_forward") as m:
            status, data, _ = self._post(f"{self.RPC_BASE}/AssignModel",
                                         req)
        self.assertEqual(m.call_count, 0)
        self.assertIn(b"selection_unconfirmed", data)


class StoreOwnerTest(unittest.TestCase):
    def test_nested_acquisition_refused_and_released(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d)
            with storage.store_owner(p):
                with self.assertRaises(OSError):
                    with storage.store_owner(p):
                        pass
            # released: reacquisition succeeds
            with storage.store_owner(p):
                pass
            lock_mode = stat.S_IMODE(
                os.stat(p / ".owner.lock").st_mode)
            self.assertEqual(lock_mode, 0o600)


class ServeOwnershipTest(unittest.TestCase):
    """serve() must hold the data-dir owner lock before touching state."""

    def _patches(self, order):
        @contextlib.contextmanager
        def owner(path):
            order.append("owner")
            yield
            order.append("owner-exit")

        class FakeServer:
            def __init__(self, addr, handler):
                order.append("server")

            def __enter__(self):
                return self

            def __exit__(self, *a):
                order.append("server-close")

            def serve_forever(self):
                order.append("serve")

        return [
            patch.object(relay, "store_owner", owner),
            patch.object(relay, "relay_token",
                         lambda: order.append("token") or "t" * 20),
            patch.object(relay.catalog, "attach_store",
                         lambda p: order.append("load")),
            patch.object(relay, "_load_stats",
                         lambda: order.append("stats")),
            patch.object(relay.auth, "get_token",
                         lambda: order.append("auth") or ("t", "a")),
            patch.object(relay, "_BoundedServer", FakeServer),
        ]

    def test_owner_acquired_before_any_state_load(self):
        order = []
        patches = self._patches(order)
        for p in patches:
            p.start()
        try:
            relay.serve(0)
        finally:
            for p in patches:
                p.stop()
        self.assertEqual(order, ["owner", "token", "load", "stats",
                                 "auth", "server", "serve",
                                 "server-close", "owner-exit"])

    def test_ownership_refusal_blocks_startup(self):
        called = []

        @contextlib.contextmanager
        def owner(path):
            raise OSError("another relay process owns this data directory")
            yield

        marks = [patch.object(relay, "store_owner", owner),
                 patch.object(relay, "relay_token",
                              lambda: called.append("token")),
                 patch.object(relay.catalog, "attach_store",
                              lambda p: called.append("load")),
                 patch.object(relay.auth, "get_token",
                              lambda: called.append("auth")),
                 patch.object(relay, "_BoundedServer",
                              lambda *a: called.append("server"))]
        for p in marks:
            p.start()
        try:
            self.assertRaises(OSError, relay.serve, 0)
        finally:
            for p in marks:
                p.stop()
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
