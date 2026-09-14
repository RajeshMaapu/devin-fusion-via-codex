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

import fcntl
import hashlib
import json
import math
import os
import pathlib
import re
import secrets
import select
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import auth, catalog, cua
from .catalog import RouteStateError
from .lifecycle import RequestCancelled, RequestContext
from .storage import append_private, atomic_write, ensure_private_dir, \
    read_private, store_owner
from .translate import (ClientGone, IncompleteResponse, UnsupportedRequest,
                        call_codex_with_tools, packet_to_responses_body,
                        parse_routed_model)
from .usage import add_to_totals, normalized_usage, token_count
from .wire import (Message, bounded_decompress, decode, error_frame, field,
                   frame, iter_frames, text)

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
              "trailer", "trailers", "transfer-encoding", "upgrade",
              "accept-encoding", "proxy-authenticate", "proxy-authorization"}

# Upstream response headers safe to pass end-to-end.
RESPONSE_PASS = {"connect-protocol-version", "connect-content-encoding",
                 "grpc-status", "grpc-message"}

MAX_UPSTREAM_BYTES = 64 << 20
MAX_HANDLERS = 32
_HANDLER_SLOTS = threading.BoundedSemaphore(MAX_HANDLERS)
_BODY_DEADLINE_S = 15
_SOCKET_IDLE_S = 10

_RELAY_TOKEN = ""


def relay_token() -> str:
    """Return the per-install path token, creating it on first use.

    Creation is serialized under a process-wide flock; an existing file
    that fails validation is never regenerated — only its absence allows
    creation. Token material is never logged.
    """
    global _RELAY_TOKEN
    if _RELAY_TOKEN:
        return _RELAY_TOKEN
    ensure_private_dir(DATA_DIR)
    lock_fd = os.open(DATA_DIR / ".token.lock",
                      os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                      | getattr(os, "O_NONBLOCK", 0), 0o600)
    try:
        import stat as _st
        if not _st.S_ISREG(os.fstat(lock_fd).st_mode):
            raise RuntimeError("fusion-relay: token lock is not a file")
        os.fchmod(lock_fd, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        missing = False
        try:
            raw = read_private(TOKEN_PATH, 256)
        except FileNotFoundError:
            missing = True
            raw = b""
        except OSError:
            raise RuntimeError(
                "fusion-relay: unreadable relay token file; refusing "
                "to overwrite it")
        if missing:
            tok = secrets.token_urlsafe(24)
            atomic_write(TOKEN_PATH, (tok + "\n").encode())
        else:
            try:
                tok = raw.decode().strip()
            except UnicodeDecodeError:
                tok = ""
            if not re.fullmatch(r"[A-Za-z0-9_-]{16,}", tok):
                raise RuntimeError(
                    "fusion-relay: invalid relay token file; refusing "
                    "to overwrite it")
        _RELAY_TOKEN = tok
    finally:
        os.close(lock_fd)
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
        atomic_write(STATS_PATH, json.dumps(_stats).encode())
    except OSError:
        _stats["persistence"] = "unavailable"


_SAFE_ROUTES = {"codex", "forward", "cognition-forward", "reject"}
_SAFE_RPCS = {"GetChatMessage", "AssignModel", "AssignModelStarting",
              "GetCliModelConfigs", "GetUserStatus"}
_QUOTA_HEADERS = {"x-codex-primary-used-percent",
                  "x-codex-secondary-used-percent",
                  "x-codex-primary-window-minutes",
                  "x-codex-secondary-window-minutes",
                  "x-codex-primary-reset-at",
                  "x-codex-secondary-reset-at"}
_SAFE_CATEGORIES = {"decode_error", "request_error", "upstream_error",
                    "selection_unconfirmed", "computer_policy_denied",
                    "cancelled", "internal"}
_SAFE_NUMERIC = ("ms", "bytes", "delta_chars", "n_messages",
                 "relay_tool_calls", "upstream_status", "codex_http_status")
_SAFE_BOOL = ("client_gone", "relay_tool_loop_bound")
_SAFE_STATUSES = {"completed", "incomplete", "failed", "cancelled",
                  "unknown"}
_RESPONSE_REF_RE = re.compile(r"^[0-9a-f]{64}$")


def safe_record(rec: dict) -> dict:
    """Project a request record onto the logged allowlist.

    Only the fields below ever reach disk/stats: no prompts, tool
    arguments, provider error bodies, raw RPC paths, request bytes, or
    header dumps.
    """
    out: dict = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    route = rec.get("route")
    out["route"] = route if route in _SAFE_ROUTES else "other"
    rpc = rec.get("rpc")
    leaf = rpc.rsplit("/", 1)[-1] if isinstance(rpc, str) else ""
    out["rpc"] = leaf if leaf in _SAFE_RPCS else "other"
    for k in _SAFE_NUMERIC:
        v = rec.get(k)
        if type(v) in (int, float) and math.isfinite(v) and v >= 0:
            out[k] = v
    for k in _SAFE_BOOL:
        if type(rec.get(k)) is bool:
            out[k] = rec[k]
    status = rec.get("codex_status")
    if status in _SAFE_STATUSES:
        out["codex_status"] = status
    category = rec.get("error_category")
    if category in _SAFE_CATEGORIES:
        out["error_category"] = category
    usage = rec.get("codex_usage")
    if usage == "unknown":
        out["codex_usage"] = "unknown"
    elif isinstance(usage, dict):
        counts = normalized_usage(usage)
        missing = usage.get("missing_fields")
        out["codex_usage"] = {
            "input_tokens": counts["input_tokens"],
            "output_tokens": counts["output_tokens"],
            "input_tokens_details":
                {"cached_tokens": counts["cached_tokens"]},
            "output_tokens_details":
                {"reasoning_tokens": counts["reasoning_tokens"]},
            "response_count": token_count(usage.get("response_count")),
            "unknown_calls": token_count(usage.get("unknown_calls")),
            "missing_fields": {
                k: token_count(v) for k, v in missing.items()
                if k in ("input_tokens", "output_tokens", "cached_tokens",
                         "reasoning_tokens")
                and token_count(v) is not None}
            if isinstance(missing, dict) else {},
            "partial": usage.get("partial") is True}
        out["codex_usage"] = {k: v for k, v in
                              out["codex_usage"].items() if v is not None}
    calls = rec.get("codex_usage_calls")
    if isinstance(calls, list):
        safe_calls = []
        for c in calls:
            if not isinstance(c, dict):
                continue
            ref = c.get("response_ref")
            safe_calls.append({
                "input_tokens": token_count(c.get("input_tokens")),
                "output_tokens": token_count(c.get("output_tokens")),
                "cached_tokens": token_count(c.get("cached_tokens")),
                "reasoning_tokens": token_count(c.get("reasoning_tokens")),
                "response_ref": ref if isinstance(ref, str)
                and _RESPONSE_REF_RE.match(ref) else None,
                "role": c.get("role") if c.get("role") in
                ("lead", "sidekick", "unverified") else "unverified",
                "status": c.get("status") if c.get("status") in
                _SAFE_STATUSES else "unknown",
                "identified": c.get("identified") is True,
                "conflict": c.get("conflict") is True})
        out["codex_usage_calls"] = safe_calls
    quota = rec.get("codex_quota_snapshot")
    if isinstance(quota, dict):
        out["codex_quota_snapshot"] = {
            k: v for k, v in quota.items()
            if k in _QUOTA_HEADERS and type(v) in (int, float)
            and math.isfinite(v) and v >= 0}
        if not out["codex_quota_snapshot"]:
            del out["codex_quota_snapshot"]
    cu = rec.get("cognition_usage")
    if cu == "unknown":
        out["cognition_usage"] = "unknown"
    elif isinstance(cu, dict):
        out["cognition_usage"] = {
            k: token_count(cu.get(k)) for k in ("input", "output", "cached")}
    return out


def _log_record(rec: dict) -> None:
    safe = safe_record(rec)
    try:
        ensure_private_dir(DATA_DIR)
        append_private(REQUESTS_LOG,
                       (json.dumps(safe, separators=(",", ":")) + "\n")
                       .encode())
    except OSError:
        pass
    with _stats_lock:
        _stats["requests"] += 1
        route = safe.get("route", "?")
        _stats["by_route"][route] = _stats["by_route"].get(route, 0) + 1
        add_to_totals(_stats["tokens"]["codex"], safe.get("codex_usage"))
        if "cognition_usage" in safe:
            tok = _stats["tokens"]["cognition"]
            cu = safe["cognition_usage"]
            if cu == "unknown":
                tok["unknown_calls"] = tok.get("unknown_calls", 0) + 1
            else:
                for k in ("input", "output", "cached"):
                    if cu.get(k) is not None:
                        tok[k] = tok.get(k, 0) + cu[k]
                if cu.get("input") is None or cu.get("output") is None:
                    tok["unknown_calls"] = tok.get("unknown_calls", 0) + 1
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
        if flags & 0x01:
            try:
                payload = bounded_decompress(payload)
            except ValueError:
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
        return {name: token_count(usage.get(number, [None])[0])
                for name, number in (("input", 2), ("output", 3),
                                     ("cached", 5))}
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
                try:
                    p = bounded_decompress(p)
                except ValueError:
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


@dataclass
class ForwardResponse:
    status: int
    body: bytes
    content_type: str
    headers: dict


def _read_capped(resp, limit: int = MAX_UPSTREAM_BYTES) -> bytes:
    """Read a response body in bounded chunks; reject oversize."""
    chunks = []
    total = 0
    while True:
        chunk = resp.read(min(65536, limit + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise ValueError("upstream response too large")
        chunks.append(chunk)
    return b"".join(chunks)


def _response_headers(resp) -> dict:
    """End-to-end upstream headers; transport trailers/encoding unsupported."""
    hdrs = {k.lower(): v for k, v in resp.headers.items()}
    if "trailer" in hdrs:
        raise ValueError("upstream declared transport trailers")
    enc = hdrs.get("content-encoding")
    if enc and enc.lower() != "identity":
        raise ValueError("upstream content encoding unsupported")
    named = {t.strip().lower()
             for t in (hdrs.get("connection") or "").split(",")}
    return {k: v for k, v in hdrs.items()
            if k in RESPONSE_PASS and k not in named}


def _forward(body: bytes, headers, path: str) -> ForwardResponse:
    """Forward a raw Connect request to Cognition (buffered, bounded)."""
    named = {t.strip().lower()
             for t in (headers.get("Connection") or "").split(",")}
    out_headers = {k: v for k, v in headers.items()
                   if k.lower() not in HOP_BY_HOP and k.lower() not in named}
    req = urllib.request.Request(UPSTREAM + path, data=body, headers=out_headers,
                                 method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=UPSTREAM_TIMEOUT)
    except urllib.error.HTTPError as e:
        resp = e
    try:
        with resp:
            hdrs = _response_headers(resp)  # validate before body reads
            return ForwardResponse(
                resp.status, _read_capped(resp),
                resp.headers.get("Content-Type", "application/proto"),
                hdrs)
    except ValueError:
        raise RuntimeError("upstream response unusable")


def _decode_packets(body: bytes, *, framed: bool | None = None):
    """Decode protobuf messages from a request body.

    ``framed``: True for ``application/connect+proto`` (strict Connect
    framing, bounded decompression, validated trailer), False for
    ``application/proto`` (raw protobuf). Unspecified: sniffed — a first
    byte in {0,1,2} is never a valid raw protobuf tag, so it means framed.
    A framed parse error propagates; there is no silent raw fallback.
    """
    if framed is None:
        framed = bool(body) and body[0] in (0, 1, 2)
    if not framed:
        yield decode(body)
        return
    total = 0  # raw + decompressed data bytes share one budget
    for flags, payload in iter_frames(body):
        if flags & 0x02:
            meta = json.loads(payload)  # trailer must be a JSON object
            if not isinstance(meta, dict) or meta.get("error"):
                raise ValueError("invalid trailer")
            continue
        total += len(payload)
        if total > MAX_REQUEST_BYTES:
            raise ValueError("body too large")
        if flags & 0x01:
            payload = bounded_decompress(payload, MAX_REQUEST_BYTES - total)
            total += len(payload)
            if total > MAX_REQUEST_BYTES:
                raise ValueError("decompressed body too large")
        yield decode(payload)


def _singleton_text(msg, number: int) -> str:
    """Exactly one UTF-8 string value for *number* — nonempty."""
    vals = msg.get(number)
    if not vals or len(vals) != 1 or not isinstance(vals[0], bytes):
        raise ValueError("missing or non-singleton field")
    s = vals[0].decode()
    if not s:
        raise ValueError("empty field")
    return s


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        self.request.settimeout(_SOCKET_IDLE_S)
        super().setup()

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
    def _send(self, status: int, payload: bytes, ctype: str,
              extra_headers: dict | None = None) -> bool:
        """Write a response; False if the client went away."""
        try:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(payload)))
            for k, v in (extra_headers or {}).items():
                kl = k.lower()
                if kl in HOP_BY_HOP or kl in ("content-length",
                                              "content-type"):
                    continue
                self.send_header(k, v)
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

    def _client_disconnected(self) -> bool:
        """Peek the socket: read-ready + empty peek means the client hung up.

        Never consumes bytes; errors are treated as gone.
        """
        try:
            ready, _, _ = select.select([self.connection], [], [], 0)
            if not ready:
                return False
            return self.connection.recv(
                1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
        except (OSError, ValueError):
            return True

    def _read_request_body(self) -> tuple[int, bytes | None]:
        """Read exactly Content-Length bytes under a total deadline.

        Returns ``(status, body)`` — status 0 means success; otherwise
        the HTTP status to send (400/408/413); the caller stops there.
        The deadline bounds the body read; the socket idle timeout bounds
        header inactivity only — there is no total header deadline.
        """
        if self.headers.get_all("Transfer-Encoding") is not None:
            return 400, None
        encodings = self.headers.get_all("Content-Encoding") or []
        if len(encodings) > 1 \
                or (encodings and encodings[0].lower() != "identity"):
            return 400, None
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 \
                or not re.fullmatch(r"[0-9]{1,20}", lengths[0]):
            return 400, None
        length = int(lengths[0])
        if length > MAX_REQUEST_BYTES:
            return 413, None
        deadline = time.monotonic() + _BODY_DEADLINE_S
        chunks = []
        remaining = length
        try:
            while remaining:
                now = time.monotonic()
                if now > deadline:
                    return 408, None
                self.connection.settimeout(
                    min(_SOCKET_IDLE_S, max(0.001, deadline - now)))
                try:
                    chunk = self.rfile.read1(min(65536, remaining))
                except (socket.timeout, TimeoutError):
                    return 408, None
                except (OSError, ValueError):
                    return 400, None
                if not chunk:
                    return 400, None
                chunks.append(chunk)
                remaining -= len(chunk)
            return 0, b"".join(chunks)
        finally:
            try:
                self.connection.settimeout(_SOCKET_IDLE_S)
            except OSError:
                pass

    # -- routing -----------------------------------------------------------
    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, json.dumps({"ok": True}).encode(), "application/json")
        if self._authorized_path() == "/capabilities":
            caps = cua.CuaProvider(DATA_DIR).compatibility()
            caps.update({
                "native_dispatch_enforcement": "unsupported",
                "consent_ui": "unavailable",
                "detached_tasks": False,
                "operation_journal": "local_contract_only",
                "image_feedback": "vision_unavailable"})
            return self._send(200, json.dumps(caps, indent=2).encode(),
                              "application/json")
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
            self.close_connection = True  # body left unread
            return self._send(403, error_frame(
                "unauthenticated", "fusion-relay: invalid or missing relay token"),
                "application/json")
        status, body = self._read_request_body()
        if status:
            self.close_connection = True
            if status == 413:
                rec = {"rpc": path, "route": "reject", "bytes":
                       int(self.headers.get("Content-Length") or 0),
                       "error_category": "request_error"}
                _log_record(rec)
            return self._send(status, error_frame(
                "resource_exhausted" if status == 413 else "invalid_argument",
                "request body rejected"), "application/connect+proto")
        rec: dict = {"rpc": path}
        req_ctype = self.headers.get("Content-Type", "").split(
            ";", 1)[0].strip().lower()

        if not path.endswith("/GetChatMessage"):
            pending_session, pending_route, revision = "", "", 0
            fwd_headers = self.headers
            if path.endswith("/AssignModel") or \
                    path.endswith("/AssignModelStarting"):
                if req_ctype == "application/connect+proto":
                    try:
                        frames = iter_frames(body)
                        data = [(f, p) for f, p in frames if not f & 0x02]
                        trailers = [(f, p) for f, p in frames if f & 0x02]
                        if len(data) != 1:
                            raise ValueError("one data frame required")
                        flags, payload = data[0]
                        if flags & 0x01:
                            payload = bounded_decompress(payload)
                        for _, t in trailers:
                            meta = json.loads(t)
                            if not isinstance(meta, dict) \
                                    or not set(meta) <= {"metadata", "error"} \
                                    or meta.get("error"):
                                raise ValueError("invalid trailer")
                        newp, pending_session, pending_route = \
                            catalog.rewrite_assign(payload)
                    except (ValueError, TypeError, UnicodeError):
                        rec.update(route="reject",
                                   error_category="decode_error")
                        _log_record(rec)
                        return self._send(200, error_frame(
                            "invalid_argument", "malformed assign request"),
                            "application/connect+proto")
                    if pending_route:
                        body = frame(newp) + b"".join(
                            frame(p, f) for f, p in trailers)
                        # rewritten output is uncompressed — don't claim
                        # compression upstream
                        fwd_headers = {k: v for k, v in self.headers.items()
                                       if k.lower() not in (
                                           "connect-content-encoding",
                                           "content-encoding")}
                elif req_ctype == "application/proto":
                    try:
                        body, pending_session, pending_route = \
                            catalog.rewrite_assign(body)
                    except (ValueError, TypeError, UnicodeError):
                        rec.update(route="reject",
                                   error_category="decode_error")
                        _log_record(rec)
                        return self._send(200, error_frame(
                            "invalid_argument", "malformed assign request"),
                            "application/connect+proto")
                else:
                    rec.update(route="reject",
                               error_category="decode_error")
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "invalid_argument", "unsupported content type"),
                        "application/connect+proto")
                if pending_route and not pending_session:
                    rec.update(route="reject",
                               error_category="decode_error")
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "invalid_argument",
                        "route selection requires a session"),
                        "application/connect+proto")
            if pending_route:
                try:
                    revision = catalog.begin_selection(
                        pending_session, pending_route)
                except RouteStateError:
                    rec.update(route="reject",
                               error_category="selection_unconfirmed")
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "failed_precondition",
                        "selection_unconfirmed"),
                        "application/connect+proto")
            try:
                resp = _forward(body, fwd_headers, path)
            except Exception:
                if pending_route:
                    try:
                        catalog.finish_selection(
                            pending_session, revision, None)
                    except RouteStateError:
                        pass
                rec.update(route="forward", error_category="upstream_error")
                _log_record(rec)
                return self._send(200, error_frame(
                    "unavailable", "upstream request failed"),
                    "application/connect+proto")
            rec.update(route="forward", upstream_status=resp.status,
                       ms=round((time.time() - started) * 1000))
            if pending_route:
                outcome = catalog.assignment_outcome(resp)
                try:
                    catalog.finish_selection(
                        pending_session, revision, outcome)
                except RouteStateError:
                    rec["error_category"] = "selection_unconfirmed"
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "failed_precondition", "selection_unconfirmed"),
                        "application/connect+proto")
                if outcome is True:
                    rec["session_route"] = pending_route
                elif outcome is None:
                    rec["error_category"] = "selection_unconfirmed"
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "failed_precondition", "selection_unconfirmed"),
                        "application/connect+proto")
            resp_ctype = resp.content_type.split(";", 1)[0].strip().lower()
            if path.endswith("/GetCliModelConfigs") \
                    and 200 <= resp.status < 300 \
                    and resp_ctype == "application/proto" \
                    and not resp.headers.get("content-encoding"):
                out, n_injected, warnings = catalog.inject_route_entries(
                    resp.body)
                rec["injected_models"] = n_injected
                if warnings:
                    rec["catalog_warnings"] = warnings
            else:
                out = resp.body
            _log_record(rec)
            return self._send(resp.status, out, resp.content_type,
                              extra_headers=resp.headers)

        framed: bool | None = None
        if req_ctype == "application/connect+proto":
            framed = True
        elif req_ctype == "application/proto":
            framed = False
        try:
            packets = list(_decode_packets(body, framed=framed))
        except ValueError:
            rec.update(route="reject", error_category="decode_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "invalid_argument", "request decode failed"),
                "application/connect+proto")
        if len(packets) != 1:
            rec.update(route="reject", error_category="decode_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "invalid_argument",
                "expected exactly one inference packet"),
                "application/connect+proto")
        packet = packets[0]
        # Protocol-drift guard: the fields we route on must exist. Absence
        # means the schema changed — fail loudly rather than guess.
        if not packet.get(21) or not packet.get(3):
            rec.update(route="reject", error_category="decode_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "failed_precondition",
                "fusion-relay: GetChatMessage schema changed; refusing to guess"),
                "application/connect+proto")
        try:
            model = _singleton_text(packet, 21)
            _singleton_text(packet, 16)  # session id, required for routing
        except (ValueError, TypeError, UnicodeError):
            rec.update(route="reject", error_category="decode_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "invalid_argument", "request decode failed"),
                "application/connect+proto")
        rec["model"] = model
        rec["n_messages"] = len(packet.get(3, []))
        route = route_for_model(model)
        try:
            pinned = catalog.session_route(packet)
        except RouteStateError:
            rec.update(route="reject",
                       error_category="selection_unconfirmed")
            _log_record(rec)
            return self._send(200, error_frame(
                "failed_precondition", "selection_unconfirmed"),
                "application/connect+proto")
        if route == "codex" and pinned == "native":
            # Session explicitly picked a `…-native` selector in /model.
            route = "forward"
            rec["session_route"] = "native"

        if route == "forward":
            try:
                resp = _forward(body, self.headers, path)
            except Exception:
                rec.update(route="cognition-forward",
                           error_category="upstream_error")
                _log_record(rec)
                return self._send(200, error_frame(
                    "unavailable", "upstream request failed"),
                    "application/connect+proto")
            rec.update(route="cognition-forward", upstream_status=resp.status,
                       ms=round((time.time() - started) * 1000))
            usage = _peek_chat_usage(resp.body)
            rec["cognition_usage"] = usage if usage else "unknown"
            _log_record(rec)
            return self._send(resp.status, resp.body, resp.content_type,
                              extra_headers=resp.headers)

        if route == "reject":
            rec.update(route="reject", error_category="request_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "unimplemented",
                "fusion-relay: model is not in the routing table"),
                "application/connect+proto")

        routed = parse_routed_model(model)
        rec["route"] = "codex"
        rec["effort"] = routed.effort
        # One auth read covers the continuity scope and the inference
        # request so the cache account and the billed account cannot race.
        # The scope is "unverified": no trusted role binding exists, so it
        # is never a role claim.
        continuity_scope = ""
        try:
            credentials = auth.get_token()
        except auth.AuthError:
            rec.update(route="reject", error_category="request_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "unauthenticated",
                "Codex login unavailable or expired; renew it in Codex"),
                "application/connect+proto")
        continuity_scope = hashlib.sha256("\0".join(
            (credentials[1], routed.model, routed.effort, "unverified")
        ).encode()).hexdigest()
        try:
            req_body = packet_to_responses_body(
                packet, routed, rec, continuity_scope=continuity_scope)
        except (UnsupportedRequest, ValueError):
            rec["route"] = "reject"
            rec["error_category"] = "request_error"
            _log_record(rec)
            return self._send(200, error_frame(
                "invalid_argument", "request not translatable"),
                "application/connect+proto")

        # Computer dispatch fails closed: no trusted dispatcher, consent
        # UI, or qualified runtime — see GET /capabilities.
        executor = None
        rec["computer_status"] = "blocked"

        # Disconnect policy: cancel, never detached. The blocking upstream
        # read is timeout-bounded; cancellation is observed at boundaries.
        context = RequestContext(disconnected=self._client_disconnected)
        try:
            if STREAM_MODE == "delta":
                if not self._send_stream_head("application/connect+proto"):
                    rec["client_gone"] = True
                    _log_record(rec)
                    return
                def on_delta(payload: bytes) -> bool:
                    return self._write_chunk(payload)
                tail = call_codex_with_tools(req_body, rec, on_delta=on_delta,
                                             executor=executor,
                                             check_cancelled=context.check,
                                             credentials=credentials)
                for flags, payload in iter_frames(tail):
                    if not self._write_chunk(bytes([flags]) + len(payload).to_bytes(4, "big") + payload):
                        rec["client_gone"] = True
                        break
                self._write_last_chunk()
            else:
                out = call_codex_with_tools(req_body, rec, executor=executor,
                                            check_cancelled=context.check,
                                            credentials=credentials)
                self._send(200, out, "application/connect+proto")
        except RequestCancelled:
            rec["client_gone"] = True
            rec["error_category"] = "cancelled"
            if self._client_disconnected():
                self.close_connection = True
            elif STREAM_MODE == "delta":
                self._write_chunk(error_frame("cancelled",
                                              "request cancelled"))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("cancelled",
                                            "request cancelled"),
                           "application/connect+proto")
        except IncompleteResponse as e:
            # Truncation must never look like success — send an explicit error.
            rec["error_category"] = "request_error"
            if STREAM_MODE == "delta":
                self._write_chunk(error_frame("out_of_range", str(e)))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("out_of_range", str(e)),
                           "application/connect+proto")
        except Exception:  # auth failure, HTTP error, stream failure
            rec["error_category"] = "internal"
            if STREAM_MODE == "delta":
                self._write_chunk(error_frame("internal",
                                              "upstream request failed"))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("internal",
                                            "upstream request failed"),
                           "application/connect+proto")
        finally:
            rec["ms"] = round((time.time() - started) * 1000)
            _log_record(rec)


class _BoundedServer(ThreadingHTTPServer):
    """ThreadingHTTPServer capped at MAX_HANDLERS concurrent requests."""

    def process_request(self, request, client_address):
        if not _HANDLER_SLOTS.acquire(blocking=False):
            try:
                request.close()
            except OSError:
                pass
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            _HANDLER_SLOTS.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            _HANDLER_SLOTS.release()


def serve(port: int = DEFAULT_PORT) -> None:
    with store_owner(DATA_DIR):
        _stats["started"] = time.time()
        relay_token()
        catalog.attach_store(ROUTES_PATH)
        _load_stats()
        try:
            auth.get_token()  # fail fast if not logged in
        except auth.AuthError as e:
            print(f"fusion-relay: {e}", file=sys.stderr)
            sys.exit(2)
        with _BoundedServer(("127.0.0.1", port), Handler) as server:
            print(f"fusion-relay listening on 127.0.0.1:{port}  "
                  f"upstream={UPSTREAM}  stream={STREAM_MODE}  "
                  f"aux={AUX_POLICY}")
            server.serve_forever()


if __name__ == "__main__":
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT)
