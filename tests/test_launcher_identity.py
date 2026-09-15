"""Launcher identity and private-directory tests: loopback/tempdir only."""
from __future__ import annotations

import contextlib
import http.client
import io
import json
import os
import secrets
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from fusion_relay import identity, launcher, relay
from fusion_relay.identity import (PrivateDirectory, ServiceIdentity,
                                   build_identity, service_proof,
                                   verify_service)


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


ROOT = Path(__file__).resolve().parent.parent
H64 = secrets.token_hex(32)
TOKEN = "dummytoken_0123456789"


def tmpdir():
    return Path(tempfile.mkdtemp()).resolve()


class VerifyServiceTest(unittest.TestCase):
    def setUp(self):
        self.secret = secrets.token_bytes(32)
        self.nonce = secrets.token_hex(32)
        self.endpoint = "http://127.0.0.1:8931"
        self.release = build_identity()
        self.proof = service_proof(self.secret, self.nonce, self.endpoint,
                                   secrets.token_hex(32), self.release)

    def verify(self, proof=..., **kw):
        args = dict(proof=self.proof if proof is ... else proof,
                    secret=self.secret, nonce=self.nonce,
                    endpoint=self.endpoint, allowed_release=self.release)
        args.update(kw)
        return verify_service(**args)

    def test_valid_proof(self):
        self.assertTrue(self.verify())

    def test_wrong_nonce_rejected(self):
        self.assertFalse(self.verify(nonce=secrets.token_hex(32)))

    def test_replay_nonce_rejected(self):
        other = service_proof(self.secret, secrets.token_hex(32),
                              self.endpoint, secrets.token_hex(32),
                              self.release)
        self.assertFalse(self.verify(proof=other))

    def test_wrong_endpoint_rejected(self):
        self.assertFalse(self.verify(endpoint="http://127.0.0.1:9999"))

    def test_wrong_secret_rejected(self):
        self.assertFalse(self.verify(secret=secrets.token_bytes(32)))

    def test_wrong_release_rejected(self):
        self.assertFalse(self.verify(allowed_release=secrets.token_hex(32)))

    def test_bool_protocol_rejected(self):
        proof = {"body": dict(self.proof["body"], protocol=True),
                 "mac": self.proof["mac"]}
        self.assertFalse(self.verify(proof=proof))

    def test_bad_shapes_rejected(self):
        for bad in (None, "x", {}, {"body": self.proof["body"]},
                    {"body": self.proof["body"], "mac": "zz" * 32},
                    {"body": dict(self.proof["body"], instance="x"),
                     "mac": "0" * 64},
                    {"body": self.proof["body"], "mac": "0" * 64,
                     "extra": 1}):
            with self.subTest(bad=bad):
                self.assertFalse(self.verify(proof=bad))

    def test_bad_endpoint_forms_rejected(self):
        for ep in ("https://127.0.0.1:8931", "http://127.0.0.1:0",
                   "http://127.0.0.1:99999", "http://localhost:8931",
                   "http://127.0.0.1:8931/x", 8931):
            with self.subTest(endpoint=ep):
                self.assertFalse(self.verify(endpoint=ep))


class PrivateDirectoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tmpdir()
        self.addCleanup(lambda: __import__("shutil").rmtree(
            self.tmp, ignore_errors=True))

    def test_create_and_file_roundtrip(self):
        target = self.tmp / "data" / "inner"
        with PrivateDirectory(target, create=True) as d:
            d.write_new("identity.key", b"k" * 32)
            self.assertEqual(d.read("identity.key", 64), b"k" * 32)
        with PrivateDirectory(target) as d:
            self.assertEqual(d.read("identity.key", 64), b"k" * 32)

    def test_missing_without_create_fails(self):
        with self.assertRaises(FileNotFoundError):
            PrivateDirectory(self.tmp / "nope")

    def test_relative_path_rejected(self):
        with self.assertRaises(ValueError):
            PrivateDirectory(Path("relative/dir"))

    def test_traversal_component_rejected(self):
        base = self.tmp / "a"
        base.mkdir()
        with self.assertRaises(ValueError):
            PrivateDirectory(base / ".." / "a")

    def test_final_symlink_rejected(self):
        real = self.tmp / "real"
        real.mkdir()
        (self.tmp / "link").symlink_to(real, target_is_directory=True)
        with self.assertRaises(OSError):
            PrivateDirectory(self.tmp / "link")

    def test_intermediate_symlink_rejected(self):
        real = self.tmp / "real"
        (real / "inner").mkdir(parents=True)
        (self.tmp / "link").symlink_to(real, target_is_directory=True)
        with self.assertRaises(OSError):
            PrivateDirectory(self.tmp / "link" / "inner")

    def test_group_writable_dir_rejected(self):
        target = self.tmp / "loose"
        target.mkdir()
        target.chmod(0o770)
        with self.assertRaises(OSError):
            PrivateDirectory(target)

    def test_unsafe_files_refused(self):
        with PrivateDirectory(self.tmp, create=True) as d:
            d.write_new("ok", b"x")
        path = self.tmp / "ok"
        path.chmod(0o644)
        with PrivateDirectory(self.tmp) as d:
            with self.assertRaises(OSError):
                d.read("ok", 10)
        path.chmod(0o600)
        os.link(path, self.tmp / "hard")
        with PrivateDirectory(self.tmp) as d:
            with self.assertRaises(OSError):
                d.read("hard", 10)
        (self.tmp / "hard").unlink()
        (self.tmp / "sym").symlink_to(path)
        with PrivateDirectory(self.tmp) as d:
            with self.assertRaises(OSError):
                d.read("sym", 10)

    def test_oversized_read_rejected(self):
        with PrivateDirectory(self.tmp, create=True) as d:
            d.write_new("big", b"x" * 100)
            with self.assertRaises(OSError):
                d.read("big", 10)

    def test_write_new_refuses_overwrite(self):
        with PrivateDirectory(self.tmp, create=True) as d:
            d.write_new("k", b"1")
            with self.assertRaises(FileExistsError):
                d.write_new("k", b"2")

    def test_bad_names_rejected(self):
        with PrivateDirectory(self.tmp) as d:
            for name in ("", ".", "..", "a/b", "../x", None):
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        d.read(name, 10)

    def test_append_and_lock(self):
        with PrivateDirectory(self.tmp, create=True) as d:
            fd = d.append_fd("relay.log")
            os.write(fd, b"line\n")
            os.close(fd)
            fd = d.append_fd("relay.log")
            os.write(fd, b"more\n")
            os.close(fd)
            self.assertEqual(d.read("relay.log", 100), b"line\nmore\n")
            lock_fd = d.lock(".launch.lock")
            os.close(lock_fd)

    def test_root_never_valid_final(self):
        with self.assertRaises(OSError):
            PrivateDirectory(Path("/"))

    def test_dot_slash_path_rejected(self):
        with self.assertRaises(ValueError):
            PrivateDirectory(str(self.tmp) + "/./x", create=True)

    def test_closed_directory_refuses_access(self):
        d = PrivateDirectory(self.tmp)
        d.close()
        with self.assertRaises(OSError):
            d.read("anything", 10)
        with self.assertRaises(OSError):
            d.write_new("x", b"1")
        with self.assertRaises(OSError):
            d.append_fd("x")
        with self.assertRaises(OSError):
            d.lock("x")

    def test_concurrent_create(self):
        target = self.tmp / "raced"
        errors = []

        def open_dir():
            try:
                PrivateDirectory(target, create=True).close()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=open_dir) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(errors, [])

    def test_owner_mismatch_rejected(self):
        real_fstat = os.fstat

        def fake_fstat(fd):
            st = real_fstat(fd)
            vals = list(st)
            vals[4] = st.st_uid + 1
            return os.stat_result(tuple(vals))

        with patch.object(identity.os, "fstat", fake_fstat):
            with self.assertRaises(OSError):
                PrivateDirectory(self.tmp)


class BuildIdentityTest(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(build_identity(), build_identity())

    def test_symlink_code_file_rejected(self):
        tmp = tmpdir()
        self.addCleanup(lambda: __import__("shutil").rmtree(
            tmp, ignore_errors=True))
        (tmp / "fusion_relay").mkdir()
        (tmp / "bin").mkdir()
        (tmp / "fusion_relay" / "a.py").write_text("x = 1")
        (tmp / "fusion_relay" / "b.py").symlink_to(
            tmp / "fusion_relay" / "a.py")
        for name in ("devin-fusion", "fusion-relay"):
            (tmp / "bin" / name).write_text("#!/bin/sh\n")
        with self.assertRaises(RuntimeError):
            build_identity(tmp)


class HandlerIdentityTest(unittest.TestCase):
    """Loopback Handler tests with a deterministic fake identity."""

    @classmethod
    def setUpClass(cls):
        cls.secret = secrets.token_bytes(32)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), relay.Handler)
        port = cls.server.server_address[1]
        cls.server.identity = ServiceIdentity(
            secret=cls.secret, endpoint="http://127.0.0.1:%d" % port,
            instance=secrets.token_hex(32), release=build_identity())
        cls.server.shutdown = Mock()
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()
        cls.port = port
        cls.old_token = relay._RELAY_TOKEN
        relay._RELAY_TOKEN = TOKEN

    @classmethod
    def tearDownClass(cls):
        relay._RELAY_TOKEN = cls.old_token
        ThreadingHTTPServer.shutdown(cls.server)
        cls.server.server_close()
        cls.thread.join(timeout=2)
        assert not cls.thread.is_alive()

    def get(self, path):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            return resp.status, dict(resp.headers), resp.read()
        finally:
            conn.close()

    def post(self, path, body=b""):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            conn.request("POST", path, body=body)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    def test_identity_proof_roundtrip(self):
        nonce = secrets.token_hex(32)
        status, headers, body = self.get("/identity?nonce=" + nonce)
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        proof = json.loads(body)
        self.assertTrue(verify_service(
            proof, self.secret, nonce,
            "http://127.0.0.1:%d" % self.port, build_identity()))

    def test_identity_bad_nonce_rejected(self):
        for path in ("/identity", "/identity?nonce=short",
                     "/identity?nonce=" + "0" * 65,
                     "/identity?other=" + "0" * 64):
            with self.subTest(path=path):
                status, _, _ = self.get(path)
                self.assertEqual(status, 400)

    def test_identity_strict_query_rejected(self):
        n = "0" * 64
        for path in ("/identity?nonce=%s&nonce=%s" % (n, n),
                     "/identity?nonce=%s&extra=1" % n,
                     "/identity?nonce=",
                     "/identity?nonce",
                     "/identity?nonce=%s#frag" % n,
                     "/identity?nonce=%s&" % n):
            with self.subTest(path=path):
                status, _, _ = self.get(path)
                self.assertEqual(status, 400)

    def test_healthz_unaffected(self):
        status, _, _ = self.get("/healthz")
        self.assertEqual(status, 200)

    def test_shutdown_requires_token_and_empty_body(self):
        self.server.shutdown.reset_mock()
        status, _ = self.post("/shutdown")
        self.assertEqual(status, 403)
        status, _ = self.post("/t/wrong/shutdown")
        self.assertEqual(status, 403)
        status, _ = self.post("/t/%s/shutdown" % TOKEN, body=b"x")
        self.assertEqual(status, 400)
        self.server.shutdown.assert_not_called()

    def test_shutdown_authorized_calls_server_shutdown(self):
        status, body = self.post("/t/%s/shutdown" % TOKEN)
        self.assertEqual(status, 200)
        for _ in range(50):
            if self.server.shutdown.called:
                break
            __import__("time").sleep(0.02)
        self.server.shutdown.assert_called_once()


class IdentityUnsetTest(unittest.TestCase):
    def test_identity_503_without_server_identity(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), relay.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection(
                "127.0.0.1", server.server_address[1], timeout=3)
            conn.request("GET", "/identity?nonce=" + "0" * 64)
            resp = conn.getresponse()
            resp.read()
            self.assertEqual(resp.status, 503)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ManagerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tmpdir()
        self.addCleanup(lambda: __import__("shutil").rmtree(
            self.tmp, ignore_errors=True))
        with PrivateDirectory(self.tmp, create=True) as d:
            d.write_new("identity.key", secrets.token_bytes(32))
            d.write_new("relay-token", (TOKEN + "\n").encode())

    def test_refused_spawns_then_reuses(self):
        proc = Mock()
        proc.poll.return_value = None
        handshakes = iter([None, True, True])
        with patch.object(launcher, "_handshake",
                          side_effect=lambda *a: next(handshakes)), \
                patch.object(launcher.subprocess, "Popen",
                             return_value=proc) as popen:
            endpoint, token = launcher.ensure_service(18931, self.tmp)
            self.assertEqual(endpoint, "http://127.0.0.1:18931")
            self.assertEqual(token, TOKEN)
            popen.assert_called_once()
            endpoint2, _ = launcher.ensure_service(18931, self.tmp)
            self.assertEqual(endpoint2, endpoint)
            popen.assert_called_once()

    def test_wrong_build_never_spawns(self):
        with patch.object(launcher, "_handshake",
                          side_effect=RuntimeError("verify failed")), \
                patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(RuntimeError):
                launcher.ensure_service(18931, self.tmp)
            popen.assert_not_called()

    def test_concurrent_callers_spawn_once(self):
        proc = Mock()
        proc.poll.return_value = None
        lock = threading.Lock()
        calls = [None, True, True]

        def handshake(*a):
            with lock:
                return calls.pop(0)

        results, errors = [], []
        def work():
            try:
                results.append(launcher.ensure_service(18931, self.tmp))
            except Exception as e:
                errors.append(e)

        with patch.object(launcher, "_handshake",
                          side_effect=handshake), \
                patch.object(launcher.subprocess, "Popen",
                             return_value=proc) as popen:
            threads = [threading.Thread(target=work) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        popen.assert_called_once()

    def test_malicious_listener_gets_no_token_request(self):
        seen = []

        class Bad(__import__("http.server", fromlist=["x"])
                  .BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                proof = service_proof(secrets.token_bytes(32),
                                      "0" * 64, "http://127.0.0.1:1",
                                      "0" * 64, "0" * 64)
                body = json.dumps(proof).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Bad)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with patch.object(launcher.subprocess, "Popen") as popen:
                with self.assertRaises(RuntimeError):
                    launcher.ensure_service(port, self.tmp)
                popen.assert_not_called()
            self.assertTrue(seen)
            self.assertFalse(any(p.startswith("/t/") for p in seen))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_launch_devin_execs_with_endpoint(self):
        captured = {}

        def fake_exec(file, argv, env):
            captured.update(file=file, argv=argv, env=env)

        with patch.object(launcher, "ensure_service",
                          return_value=("http://127.0.0.1:1", "tok123")), \
                patch.object(launcher.os, "execvpe", fake_exec), \
                contextlib.redirect_stdout(io.StringIO()) as out:
            launcher.launch_devin(["acp", "--flag"])
        self.assertEqual(captured["argv"], ["devin", "acp", "--flag"])
        self.assertEqual(captured["env"]["WINDSURF_API_SERVER_URL"],
                         "http://127.0.0.1:1/t/tok123")
        self.assertEqual(out.getvalue(), "")

    def test_port_validation(self):
        for bad in ("x", "0", "65536", "-1"):
            with self.assertRaises((ValueError, RuntimeError)):
                launcher.ensure_service(_parse(bad), self.tmp)
        for bad in (True, False, 1.5, 8931.0):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    launcher._check_port(bad)

    def test_main_usage(self):
        self.assertEqual(launcher.main(["bogus"]), 2)

    def test_token_missing_closes_verified_conn(self):
        (self.tmp / "relay-token").unlink()
        conn = Mock()
        for cmd in (launcher._cmd_status, launcher._cmd_stats,
                    launcher._cmd_stop):
            with self.subTest(cmd=cmd.__name__):
                conn.reset_mock()
                with patch.object(launcher, "DATA_DIR", self.tmp), \
                        patch.object(launcher, "_verified_conn",
                                     return_value=conn):
                    self.assertEqual(cmd(1), 1)
                conn.close.assert_called_once()

    def test_missing_key_occupied_port_no_write_no_spawn(self):
        (self.tmp / "identity.key").unlink()

        class Any(__import__("http.server", fromlist=["x"])
                  .BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Any)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.object(launcher.subprocess, "Popen") as popen:
                with self.assertRaises(RuntimeError):
                    launcher.ensure_service(server.server_address[1],
                                            self.tmp)
                popen.assert_not_called()
            self.assertFalse((self.tmp / "identity.key").exists())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_nonpersistent_proof_connection_gets_no_token_request(self):
        import http.server
        seen = []
        secret = (self.tmp / "identity.key").read_bytes()

        class CloseAfterProof(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                nonce = self.path.split("nonce=", 1)[1]
                body = json.dumps(service_proof(
                    secret, nonce,
                    "http://127.0.0.1:%d" % self.server.server_address[1],
                    secrets.token_hex(32), build_identity())).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), CloseAfterProof)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with patch.object(launcher, "DATA_DIR", self.tmp):
                self.assertEqual(launcher._cmd_status(port), 1)
            self.assertTrue(seen)
            self.assertFalse(any(p.startswith("/t/") for p in seen))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def _parse(value):
    try:
        return int(value)
    except ValueError:
        return value


DUMMY_SERVER = '''
import http.server, json, sys, threading
sys.path.insert(0, {root!r})
from fusion_relay.identity import service_proof, build_identity

SECRET = bytes.fromhex(sys.argv[1])
TOKEN = sys.argv[2]

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path.startswith("/identity?"):
            nonce = self.path.split("nonce=", 1)[1]
            body = json.dumps(service_proof(
                SECRET, nonce,
                "http://127.0.0.1:%d" % self.server.server_address[1],
                "{instance}", build_identity())).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/t/" + TOKEN + "/stats":
            body = b"{{\\"requests\\": 3}}"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404); self.send_header("Content-Length","0")
            self.end_headers()

    def do_POST(self):
        if self.path == "/t/" + TOKEN + "/shutdown":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            threading.Thread(target=self.server.shutdown, daemon=True).start()
        else:
            self.send_response(403); self.send_header("Content-Length","0")
            self.end_headers()

    def log_message(self, *a):
        pass

srv = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[3])), H)
print(srv.server_address[1], flush=True)
srv.serve_forever()
'''


class ManagerSubprocessTest(unittest.TestCase):
    """Launch the real manager entrypoint against a dummy proof server."""

    def setUp(self):
        self.tmp = tmpdir()
        self.addCleanup(lambda: __import__("shutil").rmtree(
            self.tmp, ignore_errors=True))
        self.secret = secrets.token_bytes(32)
        with PrivateDirectory(self.tmp, create=True) as d:
            d.write_new("identity.key", self.secret)
            d.write_new("relay-token", (TOKEN + "\n").encode())
        script = self.tmp / "dummy_server.py"
        script.write_text(DUMMY_SERVER.format(
            root=str(ROOT), instance=secrets.token_hex(32)))
        script.chmod(0o600)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT)
        self.proc = subprocess.Popen(
            [sys.executable, str(script), self.secret.hex(), TOKEN, "0"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env,
            text=True)
        self.addCleanup(self._stop_dummy)
        self.port = int(self.proc.stdout.readline().strip())
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = str(ROOT)
        self.env["FUSION_RELAY_DATA_DIR"] = str(self.tmp)
        self.env["FUSION_RELAY_PORT"] = str(self.port)

    def _stop_dummy(self):
        self.proc.kill()
        self.proc.wait(timeout=5)
        self.proc.stdout.close()

    def run_manager(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "fusion_relay.launcher", *args],
            env=self.env, capture_output=True, text=True, timeout=15)

    def test_status_and_stats_against_verified_dummy(self):
        r = self.run_manager("status")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("running", r.stdout)
        r = self.run_manager("stats")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("requests", r.stdout)
        self.assertNotIn(TOKEN, r.stdout + r.stderr)

    def test_stop_shuts_down_dummy(self):
        r = self.run_manager("stop")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.proc.wait(timeout=10)
        self.assertIsNotNone(self.proc.returncode)

    def test_status_not_running_on_refused_port(self):
        import socket as _socket
        sock = _socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        self.env["FUSION_RELAY_PORT"] = str(port)
        r = self.run_manager("status")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("not running", r.stdout)


class ReconcileCmdTest(unittest.TestCase):
    """Offline reconcile subcommand: argv gate, owner lock, real ledger."""

    def setUp(self):
        self.tmp = tmpdir()
        self.addCleanup(lambda: __import__("shutil").rmtree(
            self.tmp, ignore_errors=True))
        self._p = patch.object(launcher, "DATA_DIR", self.tmp)
        self.addCleanup(self._p.stop)
        self._p.start()

    def _dirty_ledger(self):
        from fusion_relay.accounting import AccountingLedger, reference
        led = AccountingLedger(self.tmp / "accounting.sqlite3")
        led.admit(reference("op-p"), "codex", reference("acct-a"))
        led.close(clean=False)

    def test_usage_rejects_before_opening_ledger(self):
        with patch("fusion_relay.accounting.AccountingLedger") as al:
            for argv in (["reconcile"],
                         ["reconcile", "--acknowledge-unknown"],
                         ["reconcile", "--evidence-ref", "e" * 64],
                         ["reconcile", "--acknowledge-unknown",
                          "--evidence-ref", "badref"],
                         ["reconcile", "--acknowledge-unknown",
                          "--evidence-ref", "e" * 64, "extra"]):
                with self.subTest(argv=argv):
                    self.assertEqual(launcher.main(argv), 2)
            al.assert_not_called()

    def test_owner_lock_blocks_reconcile(self):
        self._dirty_ledger()
        from fusion_relay.storage import store_owner
        with store_owner(self.tmp):
            self.assertEqual(launcher.main(
                ["reconcile", "--acknowledge-unknown",
                 "--evidence-ref", "e" * 64]), 1)
        # nothing was reconciled while the store was owned
        from fusion_relay.accounting import AccountingLedger
        led = AccountingLedger(self.tmp / "accounting.sqlite3")
        self.addCleanup(led.close)
        snap = led.snapshot()
        self.assertEqual(snap["providers"]["codex"]["pending"], 1)
        self.assertEqual(_ldbw(led, 
            "SELECT COUNT(*) FROM accounting_reconciliations")
            .fetchone()[0], 0)

    def test_reconcile_acks_and_prints_snapshot(self):
        self._dirty_ledger()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = launcher.main(
                ["reconcile", "--acknowledge-unknown",
                 "--evidence-ref", "e" * 64])
        self.assertEqual(rc, 0)
        snap = json.loads(out.getvalue())
        self.assertFalse(snap["degraded"])
        self.assertEqual(
            snap["providers"]["codex"]["responses"], 1)
        self.assertIsNone(
            snap["providers"]["codex"]["known"]["input_tokens"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
