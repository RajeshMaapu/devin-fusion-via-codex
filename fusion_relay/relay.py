"""The local inference relay.

Binds 127.0.0.1 only and requires a per-install bearer path token: every
request must arrive under ``/t/<token>/`` where the token lives in
``$DATA_DIR/relay-token`` (mode 0600, generated on first start). The wrapper
exports ``WINDSURF_API_SERVER_URL=http://127.0.0.1:<port>/t/<token>`` so only
processes that can read the token file can use the relay — localhost alone
is not an authenticator.

Routing policy for ``GetChatMessage`` (the only inference RPC):

- ``gpt-6-astra*``   → translated and sent to the ChatGPT Codex backend
- everything else    → forwarded byte-for-byte to Cognition — the same route
  it would take without the relay (``FUSION_RELAY_AUX=reject|codex`` changes
  this; ``reject`` restores the original fail-closed posture)

Every other RPC (control plane, assignments, search, captions, usage) is
forwarded to Cognition verbatim so the session behaves natively. There is no
fallback: a failed Codex call returns a Connect error to the CLI rather than
rerouting to paid Cognition Astra.

Privacy: request records contain route/status/usage/field-shape data only.
Assistant text is counted (``delta_chars``), never stored; tool calls are
logged by name only, never arguments; prompt content never enters the log.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import secrets
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import auth, catalog
from .translate import (ClientGone, IncompleteResponse, UnsupportedRequest,
                        call_codex, packet_to_responses_body,
                        parse_routed_model)
from .wire import Message, decode, error_frame, iter_frames, text, unframe

UPSTREAM = os.environ.get("WINDSURF_API_UPSTREAM", "https://server.codeium.com")
DEFAULT_PORT = 8931
MAX_REQUEST_BYTES = 64 << 20
# Non-astra models keep their native Cognition route by default — identical
# to running without the relay. "reject" restores fail-closed posture.
AUX_POLICY = os.environ.get("FUSION_RELAY_AUX", "forward")  # forward|reject|codex
# Comma-separated RPC-name substrings whose REQUEST bodies get numeric-field
# capture (for quota-forensics only; string fields are never recorded).
INSPECT_REQUESTS = {s for s in os.environ.get(
    "FUSION_RELAY_INSPECT", "").split(",") if s}
STREAM_MODE = os.environ.get("FUSION_RELAY_STREAM", "delta")  # delta|buffer
UPSTREAM_TIMEOUT = int(os.environ.get("FUSION_RELAY_UPSTREAM_TIMEOUT", "120"))
DATA_DIR = pathlib.Path(os.environ.get(
    "FUSION_RELAY_DATA_DIR", pathlib.Path.home() / ".local" / "share" / "fusion-codex-relay"))
REQUESTS_LOG = DATA_DIR / "requests.jsonl"
STATS_PATH = DATA_DIR / "stats.json"
ROUTES_PATH = DATA_DIR / "routes.json"
TOKEN_PATH = DATA_DIR / "relay-token"

ROUTE_TABLE = [
    (re.compile(r"^gpt-6-astra"), "codex"),
]

MODEL_ID_RE = re.compile(rb"[a-z0-9.+-]*(?:astra|swe|sol|luna|fusion|devstral|kimi)[a-z0-9.+-]*")

_stats_lock = threading.Lock()
_stats: dict = {"started": 0, "requests": 0, "by_route": {},
                "tokens": {"codex": {}, "cognition": {}}}

HOP_BY_HOP = {"host", "content-length", "connection", "keep-alive", "te",
              "trailers", "transfer-encoding", "upgrade", "accept-encoding"}

_RELAY_TOKEN = ""


def relay_token() -> str:
    """Return the per-install path token, creating it on first use."""
    global _RELAY_TOKEN
    if _RELAY_TOKEN:
        return _RELAY_TOKEN
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        tok = TOKEN_PATH.read_text().strip()
    except OSError:
        tok = ""
    if len(tok) < 16:
        tok = secrets.token_urlsafe(24)
        TOKEN_PATH.write_text(tok + "\n")
        os.chmod(TOKEN_PATH, 0o600)
    _RELAY_TOKEN = tok
    return tok


def _load_stats() -> None:
    try:
        saved = json.loads(STATS_PATH.read_text())
    except (OSError, ValueError):
        return
    with _stats_lock:
        for key in ("requests", "by_route", "tokens"):
            if isinstance(saved.get(key), type(_stats[key])):
                _stats[key] = saved[key]


def _save_stats() -> None:
    try:
        tmp = STATS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(_stats))
        tmp.replace(STATS_PATH)
    except OSError:
        pass


def _log_record(rec: dict) -> None:
    rec.setdefault("ts", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with REQUESTS_LOG.open("a") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    with _stats_lock:
        _stats["requests"] += 1
        route = rec.get("route", "?")
        _stats["by_route"][route] = _stats["by_route"].get(route, 0) + 1
        usage = rec.get("codex_usage")
        if usage == "unknown":
            tok = _stats["tokens"]["codex"]
            tok["unknown_calls"] = tok.get("unknown_calls", 0) + 1
        elif usage:
            tok = _stats["tokens"]["codex"]
            tok["input"] = tok.get("input", 0) + usage.get("input_tokens", 0)
            tok["output"] = tok.get("output", 0) + usage.get("output_tokens", 0)
            cached = usage.get("input_tokens_details", {}).get("cached_tokens", 0)
            tok["cached"] = tok.get("cached", 0) + cached
        if "cognition_usage" in rec:
            tok = _stats["tokens"]["cognition"]
            cu = rec["cognition_usage"]
            if cu == "unknown":
                tok["unknown_calls"] = tok.get("unknown_calls", 0) + 1
            else:
                tok["input"] = tok.get("input", 0) + cu.get("input", 0)
                tok["output"] = tok.get("output", 0) + cu.get("output", 0)
        _save_stats()


def route_for_model(model: str) -> str:
    """Return 'codex' for astra leads, else the aux (native-forward) policy."""
    for pattern, route in ROUTE_TABLE:
        if pattern.match(model):
            return route
    return AUX_POLICY


def _peek_user_status(raw: bytes) -> dict:
    """Numeric fields from a GetUserStatus response, flattened to scalars.

    Quota/usage counters are numeric; string fields (identity, plan names)
    are skipped so nothing personal lands in the local log.
    """
    return {k: v[0] if len(v) == 1 else v
            for k, v in _peek_numbers(raw).items()}


def _peek_chat_usage(raw: bytes) -> dict:
    """Read the usage submessage from a forwarded GetChatMessage stream."""
    try:
        frames = iter_frames(raw)
    except ValueError:
        return {}
    for flags, payload in reversed(frames):
        if flags & 0x02:
            continue
        try:
            msg = decode(payload)
        except ValueError:
            continue
        usage_field = msg.get(7)
        if not usage_field:
            continue
        try:
            usage = decode(usage_field[0])  # type: ignore[arg-type]
        except ValueError:
            continue
        return {"input": usage.get(2, [0])[0],
                "output": usage.get(3, [0])[0],
                "cached": usage.get(5, [0])[0]}
    return {}


def _peek_numbers(raw: bytes) -> dict:
    """All numeric fields in a protobuf body — for request-forensics logging."""
    try:
        frames = iter_frames(raw)
        payloads = []
        for flags, p in frames:
            if flags & 0x02:
                continue
            if flags & 0x01:
                import gzip
                try:
                    p = gzip.decompress(p)
                except OSError:
                    continue
            payloads.append(p)
        msgs = [decode(p) for p in payloads] or [decode(raw)]
    except (ValueError, IndexError):
        try:
            msgs = [decode(raw)]
        except ValueError:
            return {}
    out: dict = {}
    def walk(m: Message, prefix: str) -> None:
        for num, values in m.items():
            for v in values:
                key = f"{prefix}{num}"
                if isinstance(v, int):
                    out.setdefault(key, []).append(v)
                elif isinstance(v, bytes) and len(v) < 400:
                    try:
                        walk(decode(v), key + ".")
                    except ValueError:
                        pass
    for i, m in enumerate(msgs):
        walk(m, f"{i}:" if len(msgs) > 1 else "")
    return out


def _peek_models(raw: bytes) -> list[str]:
    """Extract model-looking strings from a forwarded response body."""
    found = set()
    for m in MODEL_ID_RE.findall(raw):
        try:
            s = m.decode()
        except UnicodeDecodeError:
            continue
        if len(s) > 5 and "." not in s[:4] or s.startswith("swe"):
            found.add(s)
    return sorted(found)[:10]


def _forward(body: bytes, headers, path: str) -> tuple[int, bytes, str]:
    """Forward a raw Connect request to Cognition; return (status, body, ctype)."""
    out_headers = {k: v for k, v in headers.items()
                   if k.lower() not in HOP_BY_HOP}
    req = urllib.request.Request(UPSTREAM + path, data=body, headers=out_headers,
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=UPSTREAM_TIMEOUT) as resp:
            return resp.status, resp.read(), resp.headers.get("Content-Type", "application/proto")
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers.get("Content-Type", "application/proto")


def _decode_packets(body: bytes):
    """Yield decoded protobuf messages from a possibly-framed request body."""
    import gzip

    def decode_frame(flags: int, payload: bytes):
        if flags & 0x02:
            return None
        if flags & 0x01:
            payload = gzip.decompress(payload)
        return decode(payload)

    try:
        flags, payload = unframe(body)
        msg = decode_frame(flags, payload)
        if msg is not None:
            yield msg
        rest = body[5 + len(payload):]
        for flags, payload in iter_frames(rest):
            msg = decode_frame(flags, payload)
            if msg is not None:
                yield msg
    except ValueError:
        yield decode(body)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # silence default stderr spam
        pass

    # -- auth --------------------------------------------------------------
    def _authorized_path(self) -> str:
        """Strip the /t/<token> prefix; '' means the request is unauthorized."""
        prefix = "/t/" + _RELAY_TOKEN
        if _RELAY_TOKEN and self.path.startswith(prefix + "/"):
            return self.path[len(prefix):]
        return ""

    # -- helpers -----------------------------------------------------------
    def _send(self, status: int, payload: bytes, ctype: str) -> bool:
        """Write a response; False if the client went away."""
        try:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return True
        except (BrokenPipeError, ConnectionResetError, socket.error):
            return False

    def _send_stream_head(self, ctype: str) -> bool:
        try:
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            return True
        except (BrokenPipeError, ConnectionResetError, socket.error):
            return False

    def _write_chunk(self, payload: bytes) -> bool:
        try:
            self.wfile.write(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, socket.error):
            return False

    def _write_last_chunk(self) -> None:
        try:
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, socket.error):
            pass

    # -- routing -----------------------------------------------------------
    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, json.dumps({"ok": True}).encode(), "application/json")
        if self._authorized_path() == "/stats":
            with _stats_lock:
                snap = dict(_stats, by_route=dict(_stats["by_route"]))
            snap["uptime_s"] = round(time.time() - _stats["started"], 1)
            snap["log"] = str(REQUESTS_LOG)
            return self._send(200, json.dumps(snap, indent=2).encode(), "application/json")
        return self._send(404, b"not found", "text/plain")

    def do_POST(self):
        started = time.time()
        path = self._authorized_path()
        if not path:
            return self._send(403, error_frame(
                "unauthenticated", "fusion-relay: invalid or missing relay token"),
                "application/json")
        length = int(self.headers.get("Content-Length", 0))
        if length > MAX_REQUEST_BYTES:
            rec = {"rpc": path, "route": "reject", "reason": "oversize",
                   "bytes": length}
            _log_record(rec)
            return self._send(413, error_frame("resource_exhausted", "request too large"),
                              "application/json")
        body = self.rfile.read(length) if length else b""
        rec: dict = {"rpc": path}

        if not path.endswith("/GetChatMessage"):
            pending_session, pending_route = "", ""
            if path.endswith("/AssignModel") or \
                    path.endswith("/AssignModelStarting"):
                body, pending_session, pending_route = catalog.rewrite_assign(body)
            status, out, ctype = _forward(body, self.headers, path)
            rec.update(route="forward", upstream_status=status,
                       ms=round((time.time() - started) * 1000))
            if pending_route:
                if status < 400:
                    catalog.pin_route(pending_session, pending_route)
                    rec["session_route"] = pending_route
                else:
                    rec["pin_deferred"] = status  # failed assign can't repin
            if any(s in path for s in INSPECT_REQUESTS):
                rec["request_numbers"] = _peek_numbers(body)
                rec["request_head"] = body[:16].hex()
                rec["request_encoding"] = self.headers.get("Content-Encoding")
            if path.endswith("/GetCliModelConfigs"):
                out, n_injected, warnings = catalog.inject_route_entries(out)
                rec["injected_models"] = n_injected
                if warnings:
                    rec["catalog_warnings"] = warnings
            if "AssignModel" in path or "ModelConfig" in path:
                rec["response_models"] = _peek_models(out)
            if path.endswith("/GetUserStatus"):
                rec["user_status_fields"] = _peek_user_status(out)
            _log_record(rec)
            return self._send(status, out, ctype)

        try:
            packets = list(_decode_packets(body))
        except ValueError as e:
            rec.update(route="reject", reason=f"decode: {e}")
            _log_record(rec)
            return self._send(200, error_frame("invalid_argument", str(e)),
                              "application/connect+proto")
        packet = packets[0] if packets else {}
        # Protocol-drift guard: the fields we route on must exist. Absence
        # means the schema changed — fail loudly rather than guess.
        if not packet.get(21) or not packet.get(3):
            missing = [f for f in (3, 21) if not packet.get(f)]
            rec.update(route="reject",
                       reason=f"protocol drift: required fields {missing} absent")
            _log_record(rec)
            return self._send(200, error_frame(
                "failed_precondition",
                "fusion-relay: GetChatMessage schema changed; refusing to guess"),
                "application/connect+proto")
        model = text(packet, 21)
        rec["model"] = model
        rec["n_messages"] = len(packet.get(3, []))
        route = route_for_model(model)
        pinned = catalog.session_route(packet)
        if route == "codex" and pinned == "native":
            # Session explicitly picked a `…-native` selector in /model.
            route = "forward"
            rec["session_route"] = "native"

        if route == "forward":
            status, out, ctype = _forward(body, self.headers, path)
            rec.update(route="cognition-forward", upstream_status=status,
                       ms=round((time.time() - started) * 1000))
            usage = _peek_chat_usage(out)
            rec["cognition_usage"] = usage if usage else "unknown"
            _log_record(rec)
            return self._send(status, out, ctype)

        if route == "reject":
            rec.update(route="reject", reason=f"unrouted model {model!r}")
            _log_record(rec)
            return self._send(200, error_frame(
                "unimplemented",
                f"fusion-relay: model {model!r} is not in the routing table"),
                "application/connect+proto")

        routed = parse_routed_model(model)
        rec["route"] = "codex"
        rec["effort"] = routed.effort
        if routed.notes:
            rec["notes"] = routed.notes
        try:
            req_body = packet_to_responses_body(packet, routed, rec)
        except (UnsupportedRequest, ValueError) as e:
            rec["route"] = "reject"
            rec["reason"] = str(e)
            _log_record(rec)
            return self._send(200, error_frame("invalid_argument", str(e)),
                              "application/connect+proto")

        try:
            if STREAM_MODE == "delta":
                if not self._send_stream_head("application/connect+proto"):
                    rec["client_gone"] = True
                    _log_record(rec)
                    return
                def on_delta(payload: bytes) -> bool:
                    return self._write_chunk(payload)
                tail = call_codex(req_body, rec, on_delta=on_delta)
                for flags, payload in iter_frames(tail):
                    if not self._write_chunk(bytes([flags]) + len(payload).to_bytes(4, "big") + payload):
                        rec["client_gone"] = True
                        break
                self._write_last_chunk()
            else:
                out = call_codex(req_body, rec)
                self._send(200, out, "application/connect+proto")
        except ClientGone:
            rec["client_gone"] = True  # upstream read aborted promptly
        except IncompleteResponse as e:
            # Truncation must never look like success — send an explicit error.
            rec["error"] = str(e)
            if STREAM_MODE == "delta":
                self._write_chunk(error_frame("out_of_range", str(e)))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("out_of_range", str(e)),
                           "application/connect+proto")
        except Exception as e:  # auth failure, HTTP error, stream failure
            rec["error"] = str(e)
            if STREAM_MODE == "delta":
                self._write_chunk(error_frame("internal", str(e)))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("internal", str(e)),
                           "application/connect+proto")
        finally:
            rec["ms"] = round((time.time() - started) * 1000)
            _log_record(rec)


def serve(port: int = DEFAULT_PORT) -> None:
    _stats["started"] = time.time()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    relay_token()
    catalog.attach_store(ROUTES_PATH)
    _load_stats()
    try:
        auth.get_token()  # fail fast if not logged in
    except auth.AuthError as e:
        print(f"fusion-relay: {e}", file=sys.stderr)
        sys.exit(2)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"fusion-relay listening on 127.0.0.1:{port}  "
          f"upstream={UPSTREAM}  stream={STREAM_MODE}  aux={AUX_POLICY}  "
          f"token=/t/{_RELAY_TOKEN[:6]}…")
    server.serve_forever()


if __name__ == "__main__":
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT)
