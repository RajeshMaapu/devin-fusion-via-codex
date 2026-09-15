"""Manage the local fusion-codex relay over authenticated loopback HTTP.

Replaces shell discovery/PID signaling: the relay is reached only by
verified HMAC identity proof on 127.0.0.1; the only auto-start trigger is
an outright connection refusal. Tokens and keys live under a
dirfd-pinned private directory and are never printed.
"""
from __future__ import annotations

import http.client
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path

from .identity import PrivateDirectory, build_identity, verify_service

DATA_DIR = Path(os.environ.get(
    "FUSION_RELAY_DATA_DIR",
    Path.home() / ".local" / "share" / "fusion-codex-relay"))
DEFAULT_PORT = 8931
_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{16,}")
_HANDSHAKE_WAIT_S = 6
_PROBE_BYTES = 4096


def _endpoint(port: int) -> str:
    return "http://127.0.0.1:%d" % port


def _check_port(port) -> int:
    if isinstance(port, (bool, float)):
        raise ValueError("invalid port")
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("invalid port")
    if not 1 <= port <= 65535:
        raise ValueError("invalid port")
    return port


def _env_port() -> int:
    return _check_port(os.environ.get("FUSION_RELAY_PORT", DEFAULT_PORT))


def _read_token(private: PrivateDirectory) -> str:
    try:
        raw = private.read("relay-token", 256)
    except FileNotFoundError:
        raise RuntimeError("fusion-relay: relay token missing")
    try:
        token = raw.decode().strip()
    except UnicodeDecodeError:
        token = ""
    if not _TOKEN_RE.fullmatch(token):
        raise RuntimeError("fusion-relay: invalid relay token")
    return token


def _handshake(port: int, secret: bytes, release: str):
    """Probe /identity on one short-lived connection.

    Returns None on connection refusal (safe to auto-start), True on a
    verified proof; anything else raises.
    """
    nonce = secrets.token_hex(32)
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        try:
            conn.request("GET", "/identity?nonce=" + nonce)
        except ConnectionRefusedError:
            return None
        resp = conn.getresponse()
        body = resp.read(_PROBE_BYTES + 1)
        if resp.status != 200 or len(body) > _PROBE_BYTES:
            raise RuntimeError("unexpected service response")
        ctype = (resp.headers.get("Content-Type") or "").split(
            ";", 1)[0].strip().lower()
        if ctype != "application/json":
            raise RuntimeError("unexpected service response")
        try:
            proof = json.loads(body)
        except ValueError:
            raise RuntimeError("unexpected service response")
        if not verify_service(proof, secret, nonce, _endpoint(port),
                              release):
            raise RuntimeError("service identity verification failed")
        return True
    finally:
        conn.close()


def _load_secret(private: PrivateDirectory):
    try:
        raw = private.read("identity.key", 64)
    except FileNotFoundError:
        return None
    if len(raw) != 32:
        raise RuntimeError("fusion-relay: invalid identity key")
    return raw


def _spawn(port: int, data_dir: Path, private: PrivateDirectory):
    log_fd = private.append_fd("relay.log")
    env = dict(os.environ)
    env["FUSION_RELAY_DATA_DIR"] = str(data_dir)
    try:
        return subprocess.Popen(
            [sys.executable, "-m", "fusion_relay.relay", str(port)],
            stdin=subprocess.DEVNULL, stdout=log_fd,
            stderr=subprocess.STDOUT, start_new_session=True,
            close_fds=True, env=env)
    finally:
        os.close(log_fd)


def _await_handshake(proc, port: int, secret: bytes, release: str) -> None:
    deadline = time.monotonic() + _HANDSHAKE_WAIT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("fusion-relay: relay process exited "
                               "during startup")
        if _handshake(port, secret, release):
            return
        time.sleep(0.1)
    raise RuntimeError("fusion-relay: relay identity handshake timed out")


def ensure_service(port: int, data_dir: Path) -> tuple[str, str]:
    """Return (endpoint, token) for a verified running relay.

    Auto-starts only on connection refusal under the private launch
    lock; an occupied port that cannot prove the expected identity is a
    hard error.
    """
    port = _check_port(port)
    data_dir = Path(data_dir)
    with PrivateDirectory(data_dir, create=True) as private:
        lock_fd = private.lock(".launch.lock")
        try:
            secret = _load_secret(private)
            release = build_identity()
            if _handshake(port, secret or b"", release):
                return _endpoint(port), _read_token(private)
            if secret is None:
                secret = secrets.token_bytes(32)
                private.write_new("identity.key", secret)
            proc = _spawn(port, data_dir, private)
            _await_handshake(proc, port, secret, release)
            return _endpoint(port), _read_token(private)
        finally:
            os.close(lock_fd)


def _verified_conn(port: int, private: PrivateDirectory):
    """Verified connection; raises RuntimeError on any identity failure.

    ConnectionRefusedError propagates — the only 'not running' signal.
    """
    secret = _load_secret(private)
    if secret is None:
        raise RuntimeError("fusion-relay: no identity key")
    nonce = secrets.token_hex(32)
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", "/identity?nonce=" + nonce)
        resp = conn.getresponse()
        body = resp.read(_PROBE_BYTES + 1)
        if resp.status != 200 or len(body) > _PROBE_BYTES:
            raise RuntimeError("unexpected service response")
        ctype = (resp.headers.get("Content-Type") or "").split(
            ";", 1)[0].strip().lower()
        if ctype != "application/json":
            raise RuntimeError("unexpected service response")
        try:
            proof = json.loads(body)
        except ValueError:
            raise RuntimeError("unexpected service response")
        if not verify_service(proof, secret, nonce, _endpoint(port),
                              build_identity()):
            raise RuntimeError("service identity verification failed")
        if resp.will_close or conn.sock is None:
            raise RuntimeError(
                "service identity connection is not persistent")
        conn.auto_open = 0
    except BaseException:
        conn.close()
        raise
    return conn


def _control_connection(port, private):
    conn = _verified_conn(port, private)
    try:
        return conn, _read_token(private)
    except BaseException:
        conn.close()
        raise


def _cmd_start(port: int) -> int:
    endpoint, _ = ensure_service(port, DATA_DIR)
    print("fusion-relay running on %s" % endpoint)
    return 0


def _cmd_fg(port: int) -> int:
    from . import relay
    relay.serve(port)
    return 0


MAX_STATS_BYTES = 1 << 20


def _cmd_status(port: int) -> int:
    try:
        with PrivateDirectory(DATA_DIR) as private:
            conn, token = _control_connection(port, private)
    except ConnectionRefusedError:
        print("not running")
        return 0
    except (OSError, RuntimeError) as e:
        print("fusion-relay: %s" % e, file=sys.stderr)
        return 1
    try:
        conn.request("GET", "/t/%s/stats" % token)
        resp = conn.getresponse()
        body = resp.read(MAX_STATS_BYTES + 1)
        if resp.status != 200 or len(body) > MAX_STATS_BYTES:
            print("fusion-relay: stats unavailable", file=sys.stderr)
            return 1
        print("running (127.0.0.1:%d)" % port)
        try:
            print(json.dumps(json.loads(body), indent=2))
        except ValueError:
            print("fusion-relay: stats unavailable", file=sys.stderr)
            return 1
    finally:
        conn.close()
    return 0


def _cmd_stats(port: int) -> int:
    try:
        with PrivateDirectory(DATA_DIR) as private:
            conn, token = _control_connection(port, private)
    except (OSError, RuntimeError) as e:
        print("fusion-relay: %s" % e, file=sys.stderr)
        return 1
    try:
        conn.request("GET", "/t/%s/stats" % token)
        resp = conn.getresponse()
        body = resp.read(MAX_STATS_BYTES + 1)
        if resp.status != 200 or len(body) > MAX_STATS_BYTES:
            print("fusion-relay: stats unavailable", file=sys.stderr)
            return 1
        try:
            print(json.dumps(json.loads(body), indent=2))
        except ValueError:
            print("fusion-relay: stats unavailable", file=sys.stderr)
            return 1
    finally:
        conn.close()
    return 0


def _cmd_stop(port: int) -> int:
    try:
        with PrivateDirectory(DATA_DIR) as private:
            conn, token = _control_connection(port, private)
    except ConnectionRefusedError:
        print("not running")
        return 0
    except (OSError, RuntimeError) as e:
        print("fusion-relay: %s" % e, file=sys.stderr)
        return 1
    try:
        conn.request("POST", "/t/%s/shutdown" % token, body=b"")
        resp = conn.getresponse()
        body = resp.read(_PROBE_BYTES + 1)
        if resp.status != 200 or len(body) > _PROBE_BYTES:
            print("fusion-relay: shutdown rejected", file=sys.stderr)
            return 1
        print("stopped")
    finally:
        conn.close()
    return 0


def _cmd_reconcile(argv: list) -> int:
    """Offline accounting reconciliation — never touches a live relay.

    The data-directory owner lock is nonblocking-exclusive, so this
    refuses outright while a relay owns the store.
    """
    if (len(argv) != 3 or argv[0] != "--acknowledge-unknown"
            or argv[1] != "--evidence-ref"
            or not re.fullmatch(r"[0-9a-f]{64}", argv[2])):
        print("usage: fusion-relay reconcile --acknowledge-unknown "
              "--evidence-ref <64hex>", file=sys.stderr)
        return 2
    from .accounting import AccountingLedger
    from .storage import store_owner
    with PrivateDirectory(DATA_DIR) as private:
        with store_owner(DATA_DIR):
            ledger = AccountingLedger(DATA_DIR / "accounting.sqlite3")
            try:
                ledger.acknowledge_unknown(argv[2], confirmed=True)
                print(json.dumps(ledger.snapshot(), indent=2))
            finally:
                ledger.close()
    return 0


def _cmd_qualify(argv: list) -> int:
    """Record an evidence-bound qualification receipt for the CURRENT
    code tree in the data directory (offline; the relay reads it at
    startup). Usage: qualify --level L [--level L2] --evidence PATH...
    """
    from . import qualification
    levels, paths, note = [], [], ""
    i = 0
    while i < len(argv):
        if argv[i] == "--level" and i + 1 < len(argv):
            levels.append(argv[i + 1])
            i += 2
        elif argv[i] == "--evidence" and i + 1 < len(argv):
            paths.append(argv[i + 1])
            i += 2
        elif argv[i] == "--note" and i + 1 < len(argv):
            note = argv[i + 1]
            i += 2
        else:
            print("usage: fusion-relay qualify --level LEVEL "
                  "[--level LEVEL] --evidence PATH [--evidence PATH...] "
                  "[--note TEXT]", file=sys.stderr)
            return 2
    if not levels or not paths:
        print("usage: fusion-relay qualify --level LEVEL --evidence PATH",
              file=sys.stderr)
        return 2
    refs = [qualification.evidence_ref(p) for p in paths]
    with PrivateDirectory(DATA_DIR) as private:
        secret = private.read("identity.key", 64)
        receipt = qualification.record(private, secret, levels, refs, note)
    print(json.dumps({"build_identity": receipt["body"]["build_identity"],
                      "levels": receipt["body"]["levels"],
                      "evidence_count": len(refs)}, indent=2))
    return 0


def launch_devin(args: list) -> None:
    endpoint, token = ensure_service(_env_port(), DATA_DIR)
    env = dict(os.environ)
    env["WINDSURF_API_SERVER_URL"] = endpoint + "/t/" + token
    os.execvpe("devin", ["devin", *args], env)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    command = argv[0] if argv else "status"
    try:
        if command == "start":
            return _cmd_start(_check_port(argv[1] if len(argv) > 1
                                          else _env_port()))
        if command == "fg":
            return _cmd_fg(_check_port(argv[1] if len(argv) > 1
                                       else _env_port()))
        if command == "status":
            return _cmd_status(_env_port())
        if command == "stats":
            return _cmd_stats(_env_port())
        if command == "stop":
            return _cmd_stop(_env_port())
        if command == "reconcile":
            return _cmd_reconcile(argv[1:])
        if command == "qualify":
            return _cmd_qualify(argv[1:])
        if command == "devin":
            launch_devin(argv[1:])
            return 0
    except http.client.HTTPException:
        print("fusion-relay: relay control request failed",
              file=sys.stderr)
        return 1
    except (OSError, RuntimeError, ValueError) as e:
        print("fusion-relay: %s" % e, file=sys.stderr)
        return 1
    print("usage: fusion-relay {start [PORT]|stop|status|fg [PORT]|stats"
          "|devin [ARGS...]|reconcile --acknowledge-unknown "
          "--evidence-ref <64hex>}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
