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

import asyncio
import contextlib
import fcntl
import hashlib
import json
import math
import os
import pathlib
import re
import secrets
import select
import signal
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import auth, catalog, cua, marker, payload_budget, \
    qualification
from .accounting import AccountingLedger, AccountingUnavailable, \
    reference
from .catalog import RouteStateError
from .identity import PrivateDirectory, ServiceIdentity
from .lifecycle import RequestCancelled, RequestContext
from .storage import append_private, atomic_write, ensure_private_dir, \
    read_private, store_owner
from .receipts import events_for_record
from .transport import RedirectBlocked, open_request
from .continuation import ContinuationError, digest
from .host_binding import BindingError, context_from_headers
from . import diagnostics
from .payload_budget import (BUDGET_POLICY_VERSION, BudgetExceeded,
                             PayloadReport, connect_code_for)
from .translate import (ClientGone, CUA_TOOL_NAME, IncompleteResponse,
                        UnsupportedRequest, UpstreamRejected, call_codex,
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
# Durable continuation is opt-in: a host application must attach a
# ContinuationCoordinator; without one durable mode fails closed.
CONTINUATION_MODE = os.environ.get(
    "FUSION_RELAY_CONTINUATION", "legacy")  # legacy|durable
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

_accounting = None
_accounting_required = False
_accounting_export_degraded = False
_accounting_lock = threading.RLock()

# Attached only by the hosting application — never via HTTP or a guessed
# role binding. See fusion_relay.continuation_host.
_continuation_coordinator = None


def set_continuation_coordinator(coordinator):
    global _continuation_coordinator
    _continuation_coordinator = coordinator


def accounting_status():
    if _accounting is None:
        return {'degraded': True, 'partial': True,
                'coverage': 'unavailable', 'reconciliation_required': True}
    snap = _accounting.snapshot()
    snap['export_degraded'] = _accounting_export_degraded
    snap['account_binding'] = {'codex': 'provider_account',
                               'native': 'credential_fingerprint'}
    return snap


def _admit_accounting(rec, provider, account_ref):
    if _accounting is None:
        if _accounting_required:
            raise AccountingUnavailable('accounting unavailable')
        return
    operation_id = reference(provider, account_ref,
                             secrets.token_hex(32))
    _accounting.admit(operation_id, provider, account_ref)
    rec['_accounting'] = (operation_id, provider, account_ref)


def _finish_accounting(rec):
    global _accounting_export_degraded
    context = rec.get('_accounting')
    if context is None or _accounting is None:
        return
    operation_id, provider, account_ref = context
    try:
        _accounting.complete(
            operation_id,
            events_for_record(rec, operation_id, provider, account_ref))
        rec['accounting_gap'] = False
    except (AccountingUnavailable, ValueError, OSError,
            TypeError, KeyError):
        # _transaction already degraded+gapped on AccountingUnavailable;
        # only record once for mapping/other failures
        if not _accounting.degraded:
            _accounting.record_gap()
        rec['accounting_gap'] = True
        return
    try:
        with _accounting_lock:
            for event_id, payload in _accounting.unexported():
                append_private(DATA_DIR / 'receipts.jsonl',
                               (payload + '\n').encode())
                _accounting.mark_exported(event_id)
    except (OSError, AccountingUnavailable, sqlite3.Error):
        # sticky for the process lifetime — there is no durable
        # request-log/receipt export reconciliation pass
        _accounting_export_degraded = True

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
              "GetCliModelConfigs", "GetUserStatus", "HostAck",
              "HostCompaction"}
_QUOTA_HEADERS = {"x-codex-primary-used-percent",
                  "x-codex-secondary-used-percent",
                  "x-codex-primary-window-minutes",
                  "x-codex-secondary-window-minutes",
                  "x-codex-primary-reset-at",
                  "x-codex-secondary-reset-at"}
_SAFE_CATEGORIES = {"decode_error", "request_error", "upstream_error",
                    "selection_unconfirmed", "computer_policy_denied",
                    "cancelled", "internal", "payload_budget"}
_SAFE_NUMERIC = ("ms", "bytes", "delta_chars", "n_messages",
                 "relay_tool_calls", "upstream_status", "codex_http_status",
                 "incoming_wire_bytes", "final_serialized_bytes",
                 "image_occurrences", "unique_image_count",
                 "image_bytes_total", "historical_image_count",
                 "current_turn_image_count", "budget_policy_version")
_SAFE_REJECTION_ORIGINS = {
    'local_wire_bytes', 'local_image_count', 'local_image_bytes',
    'local_translated_bytes', 'upstream_image_count',
    'upstream_image_format', 'upstream_context_limit',
    'upstream_payload_too_large', 'upstream_unknown',
    'native_or_pre_relay_unknown'}
_SAFE_PAYLOAD_NUMERIC = ("incoming_wire_bytes", "decompressed_bytes",
                         "final_serialized_bytes", "image_occurrences",
                         "unique_image_count", "image_bytes_total",
                         "historical_image_count",
                         "current_turn_image_count",
                         "budget_policy_version")
_SAFE_BOOL = ("client_gone", "relay_tool_loop_bound", "response_refused",
              "termination_confirmed", "accounting_gap")
_SAFE_STATUSES = {"completed", "incomplete", "failed", "cancelled",
                  "unknown"}
_RESPONSE_REF_RE = re.compile(r"^[0-9a-f]{64}$")
# IncompleteResponse messages are fixed relay literals (never provider
# content); the allowlist regex is a second guard before they are logged.
_INCOMPLETE_DETAIL_RE = re.compile(r"^[A-Za-z0-9 _:.%/-]{1,96}$")


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
    origin = rec.get("rejection_origin")
    if origin in _SAFE_REJECTION_ORIGINS:
        out["rejection_origin"] = origin
    detail = rec.get("incomplete_detail")
    if isinstance(detail, str) and _INCOMPLETE_DETAIL_RE.match(detail):
        out["incomplete_detail"] = detail
    names = rec.get("tool_call_names")
    if isinstance(names, list):
        safe_names = [n for n in names if isinstance(n, str)
                      and _INCOMPLETE_DETAIL_RE.match(n)]
        if safe_names:
            out["tool_call_names"] = safe_names[:32]
    binding = rec.get("binding_status")
    if binding in ("verified", "unavailable", "invalid"):
        out["binding_status"] = binding
    acceptance = rec.get("acceptance")
    if acceptance in ("acknowledged", "history_evidenced", "pending",
                      "uncertain"):
        out["acceptance"] = acceptance
    evidenced = rec.get("turns_history_evidenced")
    if type(evidenced) is int and evidenced >= 0:
        out["turns_history_evidenced"] = evidenced
    marker_status = rec.get("marker_status")
    if marker_status in ("verified", "absent", "invalid"):
        out["marker_status"] = marker_status
    correlation = rec.get("compaction_correlation")
    if correlation in ("matched_marker", "matched_seed"):
        out["compaction_correlation"] = correlation
    key_store = rec.get("key_store_status")
    if key_store in ("ready", "unavailable"):
        out["key_store_status"] = key_store
    transition = rec.get("epoch_transition")
    if transition in ("model_switch", "history_compaction", "fork",
                      "account_change", "legacy_history",
                      "history_divergence", "operator_recovery"):
        out["epoch_transition"] = transition
    cont = rec.get("continuation_status")
    if cont in ("durable_host_bound", "durable_host_binding_required",
                "legacy_memory_only_unqualified", "preflight_rejected",
                "admission_rejected"):
        out["continuation_status"] = cont
    role = rec.get("continuation_role")
    if role in ("lead", "sidekick"):
        out["continuation_role"] = role
    revision = rec.get("continuation_revision")
    if type(revision) is int and revision >= 0:
        out["continuation_revision"] = revision
    epoch_ref = rec.get("continuation_epoch_ref")
    if isinstance(epoch_ref, str) and _RESPONSE_REF_RE.match(epoch_ref):
        out["continuation_epoch_ref"] = epoch_ref
    cancellation = rec.get("cancellation")
    if cancellation in ("requested", "uncertain"):
        out["cancellation"] = cancellation
    payload = rec.get("payload")
    if isinstance(payload, dict):
        safe_payload = {k: payload[k] for k in _SAFE_PAYLOAD_NUMERIC
                        if type(payload.get(k)) is int and payload[k] >= 0}
        if payload.get("image_bytes_partial") is True:
            safe_payload["image_bytes_partial"] = True
        if safe_payload:
            out["payload"] = safe_payload
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
    global _accounting_export_degraded
    _finish_accounting(rec)
    if rec.get("_session_ref"):
        diagnostics.record(rec.get("model", ""), rec["_session_ref"],
                           rec.get("route", ""), rec)
    safe = safe_record(rec)
    try:
        ensure_private_dir(DATA_DIR)
        append_private(REQUESTS_LOG,
                       (json.dumps(safe, separators=(",", ":")) + "\n")
                       .encode())
    except OSError:
        _accounting_export_degraded = True
    if _accounting is not None:
        return
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


# The CLI issues AssignModel and the first GetChatMessage of a turn back to
# back (observed live: a sidekick spawn's AssignModel overlapped the
# lead's inference). A selection that is merely in flight is not a
# conflict — wait briefly for it to settle, then fail closed as before.
SELECTION_GRACE_S = float(os.environ.get("FUSION_RELAY_SELECTION_GRACE",
                                         "2.0"))


def _session_route_settled(packet):
    deadline = time.monotonic() + SELECTION_GRACE_S
    while True:
        try:
            return catalog.session_route(packet)
        except RouteStateError as e:
            if str(e) != "selection_unconfirmed" \
                    or time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


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
        resp = open_request(req, timeout=UPSTREAM_TIMEOUT)
    except RedirectBlocked as e:
        e.close()
        raise RuntimeError('upstream redirect blocked') from None
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

    def _send_stream_head(self, ctype: str, status: int = 200,
                          extra_headers: dict | None = None) -> bool:
        try:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Transfer-Encoding", "chunked")
            for k, v in (extra_headers or {}).items():
                kl = k.lower()
                if kl in HOP_BY_HOP or kl in ("content-length",
                                              "content-type"):
                    continue
                self.send_header(k, v)
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
        """Probe the socket: nonzero SO_ERROR means the client is gone.

        A half-close (client SHUT_WR) or an empty peek is not a
        disconnect — a clean FIN only means the client finished sending;
        a full close surfaces on the next write. Never consumes bytes.
        """
        try:
            if self.connection.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR):
                return True
            ready, _, _ = select.select([self.connection], [], [], 0)
            if not ready:
                return False
            self.connection.recv(
                1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
            return False
        except BlockingIOError:
            return False
        except (OSError, ValueError):
            return True

    def _stream_native(self, body: bytes, path: str, rec: dict) -> None:
        """Stream a native Cognition response through, byte-for-byte.

        Raw upstream bytes are relayed as HTTP chunks as they arrive —
        no whole-response buffering. The Connect envelope is observed
        incrementally for usage only when the upstream content type is
        ``application/connect+proto`` with identity/no HTTP encoding.
        """
        from .native_stream import ConnectObserver, StreamFailure, \
            stream_native
        started = time.time()
        named = {t.strip().lower()
                 for t in (self.headers.get("Connection") or "").split(",")}
        out_headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in HOP_BY_HOP
                       and k.lower() not in named}
        context = RequestContext(timeout=UPSTREAM_TIMEOUT)
        state = {"head": False, "observe": False}

        def observe_frame(raw):
            usage = _peek_chat_usage(raw)
            if usage:
                rec['cognition_usage'] = usage

        observer = ConnectObserver(observe_frame)

        async def on_headers(status: int, headers: dict):
            if status in (204, 304):
                raise StreamFailure(
                    "bodyless status incompatible with stream")
            rec["upstream_status"] = status
            ctype = headers.get("content-type", "application/proto")
            named = {t.strip().lower()
                     for t in (headers.get("connection") or "").split(",")}
            extras = {k: v for k, v in headers.items()
                      if (k.lower() in RESPONSE_PASS
                          or k.lower() == "content-encoding")
                      and k.lower() not in named}
            if not self._send_stream_head(ctype, status=status,
                                          extra_headers=extras):
                raise RequestCancelled("request_cancelled")
            state["head"] = True
            self.wfile.flush()
            self.connection.setblocking(False)
            enc = headers.get("content-encoding")
            state["observe"] = (
                ctype.split(";", 1)[0].strip().lower()
                == "application/connect+proto"
                and (enc is None or enc.lower() == "identity"))

        def observe(chunk: bytes):
            if state["observe"]:
                observer.feed(chunk)

        def check():
            context.check()
            try:
                if self.connection.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR):
                    raise RequestCancelled("request_cancelled")
            except OSError:
                raise RequestCancelled("request_cancelled")

        def on_cleanup(confirmed):
            rec["termination_confirmed"] = confirmed

        async def pump():
            loop = asyncio.get_running_loop()
            deadline = loop.time() + float(UPSTREAM_TIMEOUT)

            async def write(chunk: bytes):
                await asyncio.wait_for(
                    loop.sock_sendall(
                        self.connection,
                        b"%x\r\n" % len(chunk) + chunk + b"\r\n"),
                    min(30.0, float(UPSTREAM_TIMEOUT)))

            await stream_native(
                UPSTREAM + path, body, out_headers,
                on_headers=on_headers, write=write, check=check,
                observe=observe, on_cleanup=on_cleanup,
                idle_timeout=min(30.0, float(UPSTREAM_TIMEOUT)),
                total_timeout=float(UPSTREAM_TIMEOUT),
                max_bytes=MAX_UPSTREAM_BYTES)
            if state["observe"]:
                observer.finish()
            context.check()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RequestCancelled("request_cancelled")
            await asyncio.wait_for(
                loop.sock_sendall(self.connection, b"0\r\n\r\n"),
                min(float(_SOCKET_IDLE_S), remaining))

        failure = None
        try:
            asyncio.run(asyncio.wait_for(
                pump(), float(UPSTREAM_TIMEOUT) + 1.2))
        except RequestCancelled:
            rec["client_gone"] = True
            failure = "cancelled"
        except Exception:
            failure = "upstream_error"
        finally:
            try:
                self.connection.setblocking(True)
                self.connection.settimeout(_SOCKET_IDLE_S)
            except OSError:
                failure = failure or "upstream_error"
            rec["route"] = "cognition-forward"
            rec["ms"] = round((time.time() - started) * 1000)
            rec.setdefault("cognition_usage", "unknown")
            if failure:
                rec["error_category"] = failure
            _log_record(rec)
        if not failure:
            return
        if state["head"]:
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_WR)
            except OSError:
                pass
        else:
            self._send(200, error_frame(
                "cancelled" if failure == "cancelled" else "unavailable",
                "request cancelled" if failure == "cancelled"
                else "upstream request failed"),
                "application/connect+proto")

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

    def _host_compaction(self, body: bytes) -> None:
        """POST /host/compaction — native /compact hook receiver.

        The hook carries only a session id and a presence flag; the
        summary text never enters the relay. Correlation between the
        hook's session_id and the wire seed (field 16) is unverified —
        a matching note merely permits a 'history_compaction' epoch
        transition; a mismatch fails closed.
        """
        rec = {"rpc": "HostCompaction"}
        ctype = self.headers.get("Content-Type", "").split(
            ";", 1)[0].strip().lower()
        payload = None
        if len(body) <= (4 << 10) and ctype == "application/json":
            try:
                payload = json.loads(body)
            except ValueError:
                payload = None
        if not isinstance(payload, dict) \
                or set(payload) != {"protocol_version", "session_id",
                                    "summary_present"} \
                or payload["protocol_version"] != 1 \
                or not isinstance(payload["session_id"], str) \
                or not payload["session_id"] \
                or len(payload["session_id"]) > 512 \
                or not isinstance(payload["summary_present"], bool):
            rec.update(route="reject", error_category="request_error")
            _log_record(rec)
            return self._send(400, b'{"error":"compaction rejected"}',
                              "application/json")
        session_ref = reference("session", payload["session_id"])
        diagnostics.record_compaction(session_ref)
        coordinator = _continuation_coordinator
        if coordinator is not None:
            coordinator.note_compaction(session_ref)
        rec["route"] = "codex"
        rec["_session_ref"] = session_ref
        _log_record(rec)
        return self._send(
            200, b'{"recorded":true,"correlation":"unverified"}',
            "application/json")

    def _host_ack(self, body: bytes) -> None:
        """POST /host/ack — durable-result acknowledgement.

        Requires the three binding headers; the ack body itself is the
        signed payload (body_digest = digest(parsed ack)).
        """
        rec = {"rpc": "HostAck"}
        coordinator = _continuation_coordinator
        if coordinator is None:
            _log_record(rec)
            return self._send(503, b'{"error":"acknowledgement unavailable"}',
                              "application/json")
        ctype = self.headers.get("Content-Type", "").split(
            ";", 1)[0].strip().lower()
        ack = None
        if len(body) <= (16 << 10) and ctype == "application/json":
            try:
                ack = json.loads(body)
            except ValueError:
                ack = None
        if not isinstance(ack, dict):
            rec.update(route="reject", error_category="request_error")
            _log_record(rec)
            return self._send(400, b'{"error":"acknowledgement rejected"}',
                              "application/json")
        try:
            context = context_from_headers(
                self.headers, digest(ack), time.time())
        except BindingError:
            context = "invalid"
        if context is None or context == "invalid":
            rec["binding_status"] = "invalid" if context else "unavailable"
            _log_record(rec)
            return self._send(403, b'{"error":"binding rejected"}',
                              "application/json")
        try:
            result = coordinator.acknowledge(context, ack)
        except BindingError:
            rec["binding_status"] = "invalid"
            _log_record(rec)
            return self._send(403, b'{"error":"binding rejected"}',
                              "application/json")
        except (ContinuationError, sqlite3.Error):
            _log_record(rec)
            return self._send(409, b'{"error":"acknowledgement rejected"}',
                              "application/json")
        rec["binding_status"] = "verified"
        rec["acceptance"] = result["acceptance"]
        if isinstance(ack.get("session_id"), str):
            rec["_session_ref"] = reference("session", ack["session_id"])
        _log_record(rec)
        return self._send(200, json.dumps(result).encode(),
                          "application/json")

    # -- routing -----------------------------------------------------------
    def do_GET(self):
        if self.path == "/identity" or self.path.startswith("/identity?"):
            ident = getattr(self.server, "identity", None)
            if ident is None:
                return self._send(503, b"identity unavailable",
                                  "text/plain")
            split = urllib.parse.urlsplit(self.path)
            if split.fragment:
                return self._send(400, b"invalid nonce", "text/plain")
            try:
                params = urllib.parse.parse_qs(
                    split.query, keep_blank_values=True,
                    strict_parsing=True, max_num_fields=1)
            except ValueError:
                return self._send(400, b"invalid nonce", "text/plain")
            if set(params) != {"nonce"} or len(params["nonce"]) != 1:
                return self._send(400, b"invalid nonce", "text/plain")
            nonce = params["nonce"][0]
            if not re.fullmatch(r"[0-9a-f]{64}", nonce):
                return self._send(400, b"invalid nonce", "text/plain")
            return self._send(200, json.dumps(ident.proof(nonce)).encode(),
                              "application/json")
        if self.path == "/healthz":
            acct = accounting_status()
            ok = not acct["degraded"]
            code = 503 if _accounting_required and not ok else 200
            public = {k: acct.get(k, False) for k in (
                "degraded", "coverage", "reconciliation_required",
                "export_degraded")}
            return self._send(code, json.dumps(
                {"ok": ok, "accounting": public}).encode(),
                "application/json")
        if self._authorized_path() == "/capabilities":
            caps = cua.CuaProvider(DATA_DIR).compatibility()
            caps.update({
                "native_dispatch_enforcement": "unsupported",
                "consent_ui": "unavailable",
                "detached_tasks": False,
                "operation_journal": "local_contract_only",
                "continuation": (
                    "durable_host_bound" if _continuation_coordinator
                    is not None else
                    "durable_host_binding_required"
                    if CONTINUATION_MODE == "durable" else
                    "legacy_memory_only_unqualified"),
                "native_ack_contract": "unavailable",
                "host_ack_endpoint": "local_contract_only",
                "native_marker_hook": "user_prompt_submit_additional_context",
                "qualification": diagnostics.qualification(),
                "binding_issuer": "none_native",
                "native_transport": "streaming",
                "image_feedback": "vision_unavailable",
                "payload_budget": {
                    "policy_version": BUDGET_POLICY_VERSION,
                    "codex": {
                        "max_image_occurrences":
                            payload_budget.CODEX_PROFILE
                            .max_image_occurrences,
                        "max_image_bytes_total":
                            payload_budget.CODEX_PROFILE
                            .max_image_bytes_total,
                        "max_serialized_bytes":
                            payload_budget.CODEX_PROFILE
                            .max_serialized_bytes,
                        "provenance":
                            payload_budget.CODEX_PROFILE.provenance,
                        "upstream_limit_status": "unverified"}}})
            return self._send(200, json.dumps(caps, indent=2).encode(),
                              "application/json")
        if self._authorized_path() == "/stats":
            with _stats_lock:
                snap = dict(_stats, by_route=dict(_stats["by_route"]))
            snap["uptime_s"] = round(time.time() - _stats["started"], 1)
            snap["log"] = str(REQUESTS_LOG)
            snap["accounting"] = accounting_status()
            snap["legacy_totals_unverified"] = True
            return self._send(200, json.dumps(snap, indent=2).encode(), "application/json")
        if self._authorized_path().split("?", 1)[0] == "/diagnostics":
            split = urllib.parse.urlsplit(self.path)
            try:
                params = urllib.parse.parse_qs(split.query,
                                               max_num_fields=1)
            except ValueError:
                return self._send(404, b"not found", "text/plain")
            if split.fragment or set(params) != {"session"} \
                    or len(params["session"]) != 1:
                return self._send(404, b"not found", "text/plain")
            entry = diagnostics.snapshot(params["session"][0])
            if entry is None:
                return self._send(404, b"unknown session", "text/plain")
            return self._send(200, json.dumps(
                {"session": entry,
                 "accounting": accounting_status()}, indent=2).encode(),
                "application/json")
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
        if path == "/shutdown":
            if body != b"":
                return self._send(400, error_frame(
                    "invalid_argument", "shutdown takes no body"),
                    "application/json")
            self._send(200, b'{"ok": true}', "application/json")
            threading.Thread(target=self.server.shutdown,
                             daemon=True).start()
            return
        if path == "/host/compaction":
            return self._host_compaction(body)
        if path == "/host/ack":
            return self._host_ack(body)
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
            session_seed = _singleton_text(packet, 16)
        except (ValueError, TypeError, UnicodeError):
            rec.update(route="reject", error_category="decode_error")
            _log_record(rec)
            return self._send(200, error_frame(
                "invalid_argument", "request decode failed"),
                "application/connect+proto")
        rec["model"] = model
        # diagnostics are keyed only by the hashed session reference —
        # the raw seed is never stored
        rec["_session_ref"] = reference("session", session_seed)
        rec["n_messages"] = len(packet.get(3, []))
        # Native-hook marker (UserPromptSubmit additionalContext): verified
        # against the relay identity key; correlates hook session names
        # with this wire session. Never a lane or acceptance claim.
        marker_result = marker.extract(packet, getattr(
            getattr(self.server, "identity", None), "secret", None))
        rec["marker_status"] = marker_result.status
        if marker_result.status == "verified":
            rec["_marker_session_ref"] = marker_result.session_name_ref
        route = route_for_model(model)
        try:
            pinned = _session_route_settled(packet)
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
                binding_secret = getattr(
                    getattr(self.server, 'identity', None), 'secret', None)
                if _accounting is not None and binding_secret is None:
                    raise AccountingUnavailable(
                        'native account binding unavailable')
                import hmac
                binding = hmac.new(
                    binding_secret or b'',
                    self.headers.get('Authorization', '').encode(),
                    hashlib.sha256).hexdigest()
                _admit_accounting(rec, 'native',
                                  reference('native', binding))
            except AccountingUnavailable:
                return self._send(200, error_frame(
                    'failed_precondition',
                    'durable accounting unavailable'),
                    'application/connect+proto')
            return self._stream_native(body, path, rec)

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
        # Local payload budget (policy v1, not a vendor limit): a coarse
        # wire-size bound runs before translation so an image-heavy
        # request is rejected before the expensive encode.
        profile = payload_budget.profile_for('codex')
        report = PayloadReport(route='codex', profile=profile.profile,
                               budget_policy_version=BUDGET_POLICY_VERSION)
        rec['_payload_report'] = report
        try:
            payload_budget.coarse_incoming_check(len(body), profile, report)
        except BudgetExceeded as e:
            rec['error_category'] = 'payload_budget'
            rec['rejection_origin'] = e.report.rejection_origin
            rec['payload'] = report.safe_dict()
            _log_record(rec)
            return self._send(200, error_frame(
                'resource_exhausted', e.user_message()),
                'application/connect+proto')
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
        # Durable continuation replaces the in-memory caches: they stay
        # disabled (empty scope) so no unqualified reinjection can mix
        # with the ledger-owned history merge.
        if CONTINUATION_MODE != "durable":
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
        # Image budget on the translated body — before any continuation
        # reservation or accounting admission, so a preflight failure
        # reserves nothing and admits nothing.
        try:
            payload_budget.measure_body(req_body, profile, report)
        except BudgetExceeded as e:
            rec['error_category'] = 'payload_budget'
            rec['rejection_origin'] = e.report.rejection_origin
            rec['payload'] = report.safe_dict()
            _log_record(rec)
            return self._send(200, error_frame(
                'resource_exhausted', e.user_message()),
                'application/connect+proto')
        rec['payload'] = report.safe_dict()

        # Durable continuation: the host coordinator owns history merge,
        # replay, and commit. A replay never admits accounting or calls
        # the provider; a fresh reservation stays conservative-pending if
        # admission below fails.
        coordinator = _continuation_coordinator  # pinned for this request
        continuation_binding = None
        continuation_reservation = None
        if CONTINUATION_MODE == "durable":
            if coordinator is None:
                rec["error_category"] = "request_error"
                rec["continuation_status"] = "durable_host_binding_required"
                _log_record(rec)
                return self._send(200, error_frame(
                    "failed_precondition",
                    "trusted continuation binding unavailable"),
                    "application/connect+proto")
            rec["key_store_status"] = coordinator.key_store_status
            context = None
            if coordinator.binding_mode == 'capabilities':
                try:
                    context = context_from_headers(
                        self.headers, digest(req_body), time.time())
                except BindingError:
                    context = "malformed"
            try:
                if context == "malformed":
                    raise BindingError('malformed binding headers')
                continuation_binding, continuation_reservation = \
                    coordinator.prepare(
                        packet, req_body, credentials, context,
                        marker_session_ref=rec.get("_marker_session_ref"))
            except BindingError:
                rec["error_category"] = "request_error"
                rec["binding_status"] = "invalid"
                rec["continuation_status"] = "durable_host_binding_required"
                _log_record(rec)
                return self._send(200, error_frame(
                    "permission_denied",
                    "continuation binding rejected"),
                    "application/connect+proto")
            except ContinuationError as e:
                rec["error_category"] = "request_error"
                rec["binding_status"] = \
                    "unavailable" if context is None else "verified"
                rec["continuation_status"] = "durable_host_binding_required"
                _log_record(rec)
                # Store/coordinator messages are sanitized literals; a
                # legacy resolver's exception text is untrusted.
                message = str(e) if (
                    coordinator.binding_mode == 'capabilities'
                    and isinstance(e, ContinuationError)) else \
                    "trusted continuation binding unavailable"
                return self._send(200, error_frame(
                    "failed_precondition", message),
                    "application/connect+proto")
            except (sqlite3.Error, UnicodeError, ValueError):
                rec["error_category"] = "request_error"
                rec["binding_status"] = \
                    "unavailable" if context is None else "verified"
                rec["continuation_status"] = "durable_host_binding_required"
                _log_record(rec)
                return self._send(200, error_frame(
                    "failed_precondition",
                    "trusted continuation binding unavailable"),
                    "application/connect+proto")
            rec["continuation_status"] = "durable_host_bound"
            rec["binding_status"] = "verified" if (
                context is not None
                and coordinator.binding_mode == 'capabilities') \
                else "unavailable"
            rec["continuation_role"] = continuation_binding.lane
            if continuation_reservation.get("epoch_transition"):
                rec["epoch_transition"] = \
                    continuation_reservation["epoch_transition"]
            if continuation_reservation.get("compaction_correlation"):
                rec["compaction_correlation"] = \
                    continuation_reservation["compaction_correlation"]
            rec["continuation_epoch_ref"] = reference(
                "epoch", continuation_binding.epoch)
            if type(continuation_reservation.get("evidenced")) is int:
                rec["turns_history_evidenced"] = \
                    continuation_reservation["evidenced"]
            if continuation_reservation.get("replay") is not None:
                # Route replay through execute so the binding check runs
                # before stored wire bytes are released.
                rec["continuation_revision"] = \
                    continuation_reservation["revision"]
                try:
                    out = coordinator.execute(
                        continuation_binding, continuation_reservation,
                        lambda b, o, s: (_ for _ in ()).throw(
                            AssertionError("replay must not invoke")))
                except (ContinuationError, sqlite3.Error, ValueError):
                    rec["error_category"] = "request_error"
                    _log_record(rec)
                    return self._send(200, error_frame(
                        "failed_precondition",
                        "trusted continuation binding unavailable"),
                        "application/connect+proto")
                rec["acceptance"] = coordinator.acceptance(
                    continuation_binding, continuation_reservation)
                _log_record(rec)
                return self._send(
                    200, out, "application/connect+proto",
                    extra_headers={
                        "X-Fusion-Continuation-Epoch":
                            continuation_binding.epoch,
                        "X-Fusion-Continuation-Revision":
                            str(continuation_reservation["revision"])})

        # Credentials are pinned and the request translated; admission
        # must precede the stream head so no unbilled bytes reach the
        # client. Pre-admission failure means no provider call at all.
        try:
            _admit_accounting(rec, 'codex',
                              reference('codex', credentials[1]))
        except AccountingUnavailable:
            if continuation_reservation is not None:
                # Reserved but provably never dispatched: release the
                # lane instead of stranding it as outcome-unknown. If
                # the settle itself fails the row stays 'executing' and
                # is operator-resolvable.
                try:
                    coordinator.abandon(continuation_reservation,
                                        'admission_rejected')
                    rec["continuation_status"] = "admission_rejected"
                except (ContinuationError, sqlite3.Error):
                    pass
            rec["error_category"] = "request_error"
            _log_record(rec)
            return self._send(200, error_frame(
                'failed_precondition', 'durable accounting unavailable'),
                'application/connect+proto')

        # Computer dispatch fails closed: no trusted dispatcher, consent
        # UI, or qualified runtime — see GET /capabilities.
        executor = None
        rec["computer_status"] = "blocked"

        # Disconnect policy: cancel, never detached. The blocking upstream
        # read is timeout-bounded; cancellation is observed at boundaries.
        context = RequestContext(disconnected=self._client_disconnected)
        # Only after a successful stream head may errors be written as
        # Connect trailers; before it, failures need a full HTTP response.
        stream_started = False
        try:
            if continuation_reservation is not None:
                # Buffered invoke: no deltas reach the client until the
                # turn is durably committed; a commit failure surfaces as
                # an error frame, never a successful trailer.
                def invoke(native_body, items, serialized):
                    result = call_codex(
                        native_body, rec, _items_out=items,
                        check_cancelled=context.check,
                        credentials=credentials,
                        serialized=serialized)
                    # No trusted dispatcher or consent UI exists; a
                    # provider-emitted computer call is never deliverable.
                    if any(item.get("type") == "function_call"
                           and item.get("name") == CUA_TOOL_NAME
                           for item in items):
                        raise UnsupportedRequest(
                            "computer_policy_denied")
                    return result

                def preflight(merged):
                    # Measure + serialize the merged body (continuation
                    # reinsertion included); the exact returned bytes are
                    # what invoke sends.
                    payload_budget.measure_body(merged, profile, report)
                    try:
                        return payload_budget.serialize_and_check(
                            merged, profile, report)
                    finally:
                        rec['payload'] = report.safe_dict()

                out = coordinator.execute(
                    continuation_binding, continuation_reservation, invoke,
                    preflight=preflight)
                rec["continuation_revision"] = \
                    continuation_reservation["revision"] + 1
                try:
                    rec["acceptance"] = coordinator.acceptance(
                        continuation_binding, continuation_reservation)
                except (ContinuationError, sqlite3.Error):
                    pass
                self._send(200, out, "application/connect+proto",
                           extra_headers={
                               "X-Fusion-Continuation-Epoch":
                                   continuation_binding.epoch,
                               "X-Fusion-Continuation-Revision":
                                   str(continuation_reservation
                                       ["revision"] + 1)})
            elif STREAM_MODE == "delta":
                if not self._send_stream_head("application/connect+proto"):
                    rec["client_gone"] = True
                    return
                stream_started = True
                def on_delta(payload: bytes) -> bool:
                    return self._write_chunk(payload)
                tail = call_codex_with_tools(req_body, rec, on_delta=on_delta,
                                             executor=executor,
                                             check_cancelled=context.check,
                                             credentials=credentials,
                                             budget_profile=profile)
                for flags, payload in iter_frames(tail):
                    if not self._write_chunk(bytes([flags]) + len(payload).to_bytes(4, "big") + payload):
                        rec["client_gone"] = True
                        break
                self._write_last_chunk()
            else:
                out = call_codex_with_tools(req_body, rec, executor=executor,
                                            check_cancelled=context.check,
                                            credentials=credentials,
                                            budget_profile=profile)
                self._send(200, out, "application/connect+proto")
        except RequestCancelled:
            rec["client_gone"] = True
            rec["error_category"] = "cancelled"
            if continuation_reservation is not None:
                # Only a socket close was observed — the provider never
                # confirmed cancellation of the inference.
                rec["cancellation"] = "requested"
            if self._client_disconnected():
                self.close_connection = True
            elif stream_started:
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
            rec["incomplete_detail"] = str(e)
            if stream_started:
                self._write_chunk(error_frame("out_of_range", str(e)))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("out_of_range", str(e)),
                           "application/connect+proto")
        except BudgetExceeded as e:
            # Post-merge preflight rejection (durable path): the
            # reservation was abandoned, the lane is free to retry.
            rec["error_category"] = "payload_budget"
            rec["rejection_origin"] = e.report.rejection_origin
            rec["payload"] = e.report.safe_dict()
            if continuation_reservation is not None:
                rec["continuation_status"] = "preflight_rejected"
            if stream_started:
                self._write_chunk(error_frame("resource_exhausted",
                                              e.user_message()))
                self._write_last_chunk()
            else:
                self._send(200, error_frame("resource_exhausted",
                                            e.user_message()),
                           "application/connect+proto")
        except UpstreamRejected as e:
            # Structured, sanitized provider rejection — the raw error
            # body was classified and discarded in call_codex.
            code = connect_code_for(e.rejection.classification)
            if stream_started:
                self._write_chunk(error_frame(code, e.user_message()))
                self._write_last_chunk()
            else:
                self._send(200, error_frame(code, e.user_message()),
                           "application/connect+proto")
        except Exception:  # auth failure, HTTP error, stream failure
            rec["error_category"] = "internal"
            if stream_started:
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

    daemon_threads = False   # server_close waits for in-flight requests

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


def _qualification_status(private, identity) -> dict:
    """Verified qualification receipt for this tree, or the weakest level.
    Any problem reading it degrades the label — never startup."""
    try:
        return qualification.status(private, getattr(identity, "secret",
                                                     None))
    except Exception:
        return {"level": "local_tests", "receipt": "invalid",
                "evidence_count": 0}


@contextlib.contextmanager
def _graceful_signals(server):
    """Turn SIGTERM/SIGINT into ``server.shutdown()`` so ``serve()``'s
    ``finally`` runs and the accounting run is closed clean.

    launchd stops the service with SIGTERM; without this Python dies
    mid-run and the next start comes up degraded. Handlers are installed
    only from the main thread (an in-thread ``serve()`` is unaffected)
    and restored afterwards. ``shutdown()`` must not be called from the
    serving thread, so it runs on a short-lived daemon thread.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = {}

    def _handler(signum, frame):
        print(f"fusion-relay: shutting down (signal {signum})",
              file=sys.stderr, flush=True)
        threading.Thread(target=server.shutdown, daemon=True).start()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, _handler)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def serve(port: int = DEFAULT_PORT) -> None:
    if CONTINUATION_MODE not in ("legacy", "durable"):
        print(f"fusion-relay: unsupported FUSION_RELAY_CONTINUATION "
              f"{CONTINUATION_MODE!r}", file=sys.stderr)
        sys.exit(2)
    with PrivateDirectory(DATA_DIR, create=True) as private:
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
            global _accounting, _accounting_required, \
                _accounting_export_degraded
            _accounting_required = True
            _accounting = None
            _accounting_export_degraded = False
            try:
                # Durable accounting must exist before the service can
                # admit billable work; construction failure never opens
                # the port and leaves no sticky globals behind.
                _accounting = AccountingLedger(
                    DATA_DIR / 'accounting.sqlite3')
                with _BoundedServer(("127.0.0.1", port), Handler) \
                        as server:
                    server.identity = ServiceIdentity.create(
                        private, server.server_address[1])
                    diagnostics.set_qualification(
                        _qualification_status(private, server.identity))
                    print(f"fusion-relay listening on 127.0.0.1:{port}  "
                          f"upstream={UPSTREAM}  stream={STREAM_MODE}  "
                          f"aux={AUX_POLICY}")
                    with _graceful_signals(server):
                        server.serve_forever()
            finally:
                try:
                    if _accounting is not None:
                        _accounting.close(clean=True)
                finally:
                    _accounting = None
                    _accounting_required = False


if __name__ == "__main__":
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT)
