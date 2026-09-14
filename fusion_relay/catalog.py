"""Catalog injection and per-session route pinning.

Relabels every ``gpt-6-astra*`` / ``fusion-gpt-6-astra*`` entry in the
``GetCliModelConfigs`` response with a ``· Codex sub`` marker (the canonical
id IS the Codex route under the relay) and appends one ``-native`` clone per
entry as the in-picker escape hatch back to Cognition-billed Astra.

Selection flow:

1. ``/model`` shows e.g. ``GPT-6 Astra High Thinking · Codex sub``.
2. ``AssignModel`` arrives with a ``-native`` selector in field 2; the relay
   strips the suffix before Cognition sees it (Cognition only knows canonical
   ids). The selection is journaled as ``pending`` BEFORE forwarding and
   committed only if the upstream response confirms success — a failed or
   ambiguous assignment leaves the previous route intact and the session
   blocked as ``selection_unconfirmed`` rather than guessed.
3. ``GetChatMessage`` carries the same session uuid in field 16; an astra
   packet consults the map — ``native`` forwards to Cognition, anything else
   takes the Codex route (the relay's default anyway).

State persists to a versioned ``routes.json`` (v2: routes + revisions +
pending intents) under the data dir so a relay restart neither silently
reroutes a session nor drops an unconfirmed selection. A corrupt or
unwritable store sets ``_store_error`` and fails closed — the relay never
infers routing state it could not durably record.
"""

from __future__ import annotations

import json
import pathlib
import re
import threading

from .storage import atomic_write, read_private
from .wire import (bounded_decompress, decode, decode_typed, encode_typed,
                   field, get_string, iter_frames, set_string)

# Selectors that get the Codex-route relabel + a ``-native`` clone.
CLONEABLE_RE = re.compile(rb"^(?:fusion-)?gpt-6-astra")
# Any astra-family id not covered above is a protocol-drift warning.
ASTRA_LIKE_RE = re.compile(rb"astra")
NATIVE_SUFFIX = "-native"

# Catalog entry layout (decoded from a live GetCliModelConfigs response):
F_ENTRY_ID = 22        # model id sent back in AssignModel field 2
F_ENTRY_NAME = 1       # display name
F_ENTRY_CONFIG = 23    # nested config; sub-field 17 echoes the id
F_CONFIG_ID = 17
F_ENTRY_BADGES = 30    # badge group; sub-field 2 is a repeated badge
F_BADGE_LIST = 2

# AssignModel request layout:
F_ASSIGN_SELECTOR = 2  # requested model selector
F_ASSIGN_SESSION = 3   # session uuid — equals GetChatMessage field 16

_lock = threading.Lock()
_session_routes: dict[str, str] = {}  # session uuid -> "codex" | "native"
_revisions: dict[str, int] = {}       # session uuid -> last selection revision
_pending: dict[str, dict] = {}        # session uuid -> {revision, route, status}
_store_path: pathlib.Path | None = None
_store_error = False

_ROUTES = ("codex", "native")
_PENDING_STATUSES = ("pending", "selection_unconfirmed")


class RouteStateError(RuntimeError):
    """The durable route store is unreadable, unwritable, or ambiguous."""


def _write_state(routes: dict, revisions: dict, pending: dict) -> None:
    """Persist a candidate state; raises RouteStateError on any failure."""
    global _store_error
    if _store_path is None:
        return
    doc = {"version": 2, "routes": routes, "revisions": revisions,
           "pending": pending}
    try:
        atomic_write(_store_path, json.dumps(doc).encode())
    except OSError:
        _store_error = True
        raise RouteStateError("route store write failed")


def _validate_saved(saved) -> tuple[dict, dict, dict]:
    """Parse a loaded store document into (routes, revisions, pending)."""
    if not isinstance(saved, dict):
        raise ValueError("route store is not an object")
    if "version" not in saved:
        # legacy: flat {session: route}; every entry must be a valid route
        routes = {}
        for k, v in saved.items():
            if not isinstance(k, str) or not k or v not in _ROUTES:
                raise ValueError("invalid legacy route entry")
            routes[k] = v
        return routes, {}, {}
    if saved.get("version") != 2:
        raise ValueError("unsupported route store version")
    routes = saved.get("routes")
    revisions = saved.get("revisions")
    pending = saved.get("pending")
    if not isinstance(routes, dict) or not isinstance(revisions, dict) \
            or not isinstance(pending, dict):
        raise ValueError("route store fields malformed")
    for k, v in routes.items():
        if not isinstance(k, str) or not k or v not in _ROUTES:
            raise ValueError("invalid route entry")
    for k, v in revisions.items():
        if not isinstance(k, str) or not k or type(v) is not int or v < 0:
            raise ValueError("invalid revision entry")
    for k, v in pending.items():
        if not isinstance(k, str) or not k or not isinstance(v, dict) \
                or type(v.get("revision")) is not int \
                or v["revision"] <= 0 \
                or revisions.get(k) != v["revision"] \
                or v.get("route") not in _ROUTES \
                or v.get("status") not in _PENDING_STATUSES:
            raise ValueError("invalid pending entry")
    return dict(routes), dict(revisions), dict(pending)


def attach_store(path: pathlib.Path) -> None:
    """Persist route state under *path*; loads state from a prior run.

    Only FileNotFoundError means "empty". Any malformed or unreadable
    existing state sets ``_store_error`` and raises RouteStateError —
    the relay refuses to guess routing from ambiguous storage.
    """
    global _store_path, _store_error
    with _lock:
        _store_path = path
        _session_routes.clear()
        _revisions.clear()
        _pending.clear()
        try:
            raw = read_private(path, 1 << 20)
        except FileNotFoundError:
            _store_error = False
            return
        except OSError:
            _store_error = True
            raise RouteStateError("route store unreadable")
        try:
            routes, revisions, pending = _validate_saved(json.loads(raw))
        except ValueError:
            _store_error = True
            raise RouteStateError("route store malformed")
        _session_routes.update(routes)
        _revisions.update(revisions)
        _pending.update(pending)
        _store_error = False


def begin_selection(session: str, route: str) -> int:
    """Journal a selection intent for *session*; return its revision.

    The pending intent is persisted BEFORE the caller forwards the
    assignment upstream. A session with an unconfirmed selection is
    rejected immediately — two assignments can never race.
    """
    if not isinstance(session, str) or not session \
            or route not in _ROUTES:
        raise RouteStateError("invalid selection request")
    with _lock:
        if _store_path is None:
            raise RouteStateError("durable route store required")
        if _store_error:
            raise RouteStateError("route store state unknown")
        if session in _pending:
            raise RouteStateError("selection already pending")
        revision = _revisions.get(session, 0) + 1
        pending = dict(_pending)
        pending[session] = {"revision": revision, "route": route,
                            "status": "pending"}
        revisions = dict(_revisions)
        revisions[session] = revision
        _write_state(_session_routes, revisions, pending)
        _pending[session] = pending[session]
        _revisions[session] = revision
        return revision


def finish_selection(session: str, revision: int,
                     accepted: bool | None) -> None:
    """Resolve a pending selection.

    ``True`` commits the new route, ``False`` clears the intent keeping
    the old committed route, ``None`` leaves the session blocked as
    ``selection_unconfirmed``. All transitions persist before publish.
    """
    with _lock:
        if _store_error:
            raise RouteStateError("route store state unknown")
        pend = _pending.get(session)
        if pend is None or pend["revision"] != revision:
            raise RouteStateError("no matching pending selection")
        routes = dict(_session_routes)
        pending = dict(_pending)
        if accepted is True:
            routes[session] = pend["route"]
            pending.pop(session)
        elif accepted is False:
            pending.pop(session)
        else:
            pending[session] = dict(pend, status="selection_unconfirmed")
        _write_state(routes, _revisions, pending)
        _session_routes.clear()
        _session_routes.update(routes)
        _pending.clear()
        _pending.update(pending)


def selection_status(session: str) -> dict:
    """Snapshot a session's committed route, revision, and pending state."""
    with _lock:
        return {"route": _session_routes.get(session),
                "revision": _revisions.get(session, 0),
                "pending": dict(_pending[session]) if session in _pending
                else None,
                "store_error": _store_error}


def _badge(label: str, value: str) -> bytes:
    """Build a badge submessage {f1: label, f2: {f2: value, f3: 1}}."""
    return field(1, label) + field(2, field(2, value) + field(3, 1))


def _clone_entry(raw: bytes, suffix: str, badge_text: str) -> bytes | None:
    """Clone one catalog entry with a new id suffix + display label."""
    entry = decode_typed(raw)
    model_id = get_string(entry, F_ENTRY_ID)
    if not model_id:
        return None
    set_string(entry, F_ENTRY_ID, model_id + suffix)
    name = get_string(entry, F_ENTRY_NAME)
    set_string(entry, F_ENTRY_NAME, name + " · " + badge_text)
    # keep the nested config's echoed id in sync
    cfg = entry.get(F_ENTRY_CONFIG)
    if cfg:
        inner = decode_typed(cfg[0][0])  # type: ignore[arg-type]
        set_string(inner, F_CONFIG_ID, model_id + suffix)
        entry[F_ENTRY_CONFIG] = [(encode_typed(inner), 2)]
    # append a Route badge to the badge group
    badges = entry.get(F_ENTRY_BADGES)
    if badges:
        group = decode_typed(badges[0][0])  # type: ignore[arg-type]
        group.setdefault(F_BADGE_LIST, []).append(
            (_badge("Route", badge_text), 2))
        entry[F_ENTRY_BADGES] = [(encode_typed(group), 2)]
    return encode_typed(entry)


def inject_route_entries(body: bytes) -> tuple[bytes, int, list[str]]:
    """Relabel astra entries as Codex-routed; append ``-native`` clones.

    Returns ``(new_body, injected_count, warnings)`` — warnings flag
    astra-family ids the relabel didn't cover and per-entry clone failures,
    i.e. protocol drift surfacing before it silently misroutes.
    """
    warnings: list[str] = []
    try:
        top = decode_typed(body)
    except ValueError:
        return body, 0, ["catalog body did not decode"]
    injected = 0
    extras = bytearray()
    for i, (raw, wtype) in enumerate(top.get(1, [])):
        if wtype != 2 or not isinstance(raw, bytes):
            continue
        try:
            entry = decode_typed(raw)
        except ValueError:
            warnings.append("catalog entry failed to decode")
            continue
        id_vals = entry.get(F_ENTRY_ID)
        if not id_vals:
            continue
        model_id = id_vals[0][0]
        if not isinstance(model_id, bytes):
            continue
        if not CLONEABLE_RE.match(model_id):
            if ASTRA_LIKE_RE.search(model_id):
                warnings.append(
                    f"astra-like id not relabeled: {model_id.decode(errors='replace')}")
            continue
        # relabel the base entry in place: it IS the codex route
        _relabel(entry, "Codex sub")
        top[1][i] = (encode_typed(entry), 2)
        clone = _clone_entry(raw, NATIVE_SUFFIX, "Native")
        if clone:
            extras += field(1, clone)
            injected += 1
        else:
            warnings.append(f"clone failed for {model_id.decode(errors='replace')}")
    if injected:
        body = encode_typed(top) + bytes(extras)
    return body, injected, warnings


def _relabel(entry, label: str) -> None:
    """Append ``· <label>`` to an entry's display name and badge list."""
    name = get_string(entry, F_ENTRY_NAME)
    if name and label not in name:
        set_string(entry, F_ENTRY_NAME, name + " · " + label)
    badges = entry.get(F_ENTRY_BADGES)
    if badges:
        group = decode_typed(badges[0][0])  # type: ignore[arg-type]
        group.setdefault(F_BADGE_LIST, []).append((_badge("Route", label), 2))
        entry[F_ENTRY_BADGES] = [(encode_typed(group), 2)]


def rewrite_assign(body: bytes) -> tuple[bytes, str, str]:
    """Strip a route suffix from an AssignModel selector.

    Returns ``(new_body, session_uuid, requested_route)``. The route is
    NOT committed here — the caller journals the intent with
    :func:`begin_selection` before forwarding and resolves it via
    :func:`finish_selection`. Malformed bodies or non-singleton
    selector/session fields raise ``ValueError`` — the caller must not
    forward them.
    """
    msg = decode_typed(body)
    if len(msg.get(F_ASSIGN_SELECTOR, [])) > 1 \
            or len(msg.get(F_ASSIGN_SESSION, [])) > 1:
        raise ValueError("non-singleton assign selector/session")
    selector = get_string(msg, F_ASSIGN_SELECTOR)
    session = get_string(msg, F_ASSIGN_SESSION)
    if selector.endswith(NATIVE_SUFFIX):
        set_string(msg, F_ASSIGN_SELECTOR, selector[: -len(NATIVE_SUFFIX)])
        return encode_typed(msg), session, "native"
    if CLONEABLE_RE.match(selector.encode()):
        # Canonical `· Codex sub` row picked while a native pin may exist —
        # an explicit re-pin so the stale route cannot silently survive.
        return body, session, "codex"
    return body, "", ""


def pin_route(session: str, route: str) -> None:
    """Commit a session route directly (trusted local pin).

    The candidate state is persisted first; the in-memory map is
    published only after the write succeeds.
    """
    if not session or route not in _ROUTES:
        return
    with _lock:
        if _store_error:
            raise RouteStateError("route store state unknown")
        routes = dict(_session_routes)
        routes[session] = route
        pending = dict(_pending)
        pending.pop(session, None)
        _write_state(routes, _revisions, pending)
        _session_routes[session] = route
        _pending.pop(session, None)


def session_route(packet) -> str | None:
    """Look up the pinned route for a decoded GetChatMessage packet.

    Raises RouteStateError when the store state is unknown or the
    session has a pending/unconfirmed selection — never guesses.
    Corrupt state blocks lookups even when the packet has no session.
    """
    session = ""
    v = packet.get(16)
    if v and isinstance(v[0], bytes):
        session = v[0].decode()
    with _lock:
        if _store_error:
            raise RouteStateError("route store state unknown")
        if not session:
            return None
        if session in _pending:
            raise RouteStateError("selection_unconfirmed")
        return _session_routes.get(session)


def assignment_outcome(response) -> bool | None:
    """Classify an upstream AssignModel response: True / False / None.

    Local protocol assessment only — not a vendor-contract claim, and an
    HTTP failure cannot prove the upstream did not mutate state. Non-2xx
    is False; ``application/proto`` with a decodable protobuf body (empty
    allowed) is True per Connect unary semantics;
    ``application/connect+proto`` requires exactly one data frame plus
    exactly one JSON trailer (``error`` → False, ``{}`` or metadata-only
    → True); a nonzero ``grpc-status`` header is False; anything unknown
    or malformed is None — never a guess.
    """
    status = getattr(response, "status", 0)
    headers = {str(k).lower(): v
               for k, v in (getattr(response, "headers", {}) or {}).items()}
    grpc = headers.get("grpc-status")
    if grpc is not None:
        try:
            if int(grpc) != 0:
                return False
        except (TypeError, ValueError):
            return None
    if not (200 <= status < 300):
        return False
    ctype = (getattr(response, "content_type", "") or "").split(
        ";", 1)[0].strip().lower()
    body = getattr(response, "body", b"")
    if ctype == "application/connect+proto":
        try:
            frames = iter_frames(body)
        except ValueError:
            return None
        data = [(f, p) for f, p in frames if not f & 0x02]
        trailer = [p for f, p in frames if f & 0x02]
        if len(data) != 1 or len(trailer) != 1:
            return None
        flags, payload = data[0]
        if flags & 0x01:
            try:
                payload = bounded_decompress(payload)
            except ValueError:
                return None
        try:
            meta = json.loads(trailer[0])
        except ValueError:
            return None
        if not isinstance(meta, dict) \
                or not set(meta) <= {"metadata", "error"}:
            return None
        if "error" in meta:
            err = meta["error"]
            if not isinstance(err, dict) \
                    or not isinstance(err.get("code"), str):
                return None
            return False
        md = meta.get("metadata")
        if md is not None and (
                not isinstance(md, dict)
                or any(not isinstance(k, str)
                       or not isinstance(v, list)
                       or any(not isinstance(x, str) for x in v)
                       for k, v in md.items())):
            return None
        try:
            decode(payload)
        except ValueError:
            return None
        return True
    if ctype == "application/proto":
        try:
            decode(body)
            return True
        except ValueError:
            return None
    if ctype == "application/json":
        try:
            env = json.loads(body)
        except ValueError:
            return None
        if isinstance(env, dict) and isinstance(env.get("code"), str) \
                and env["code"]:
            return False
        return None
    return None


def reset() -> None:
    """Clear maps, pending intents, store path, and error (tests)."""
    global _store_path, _store_error
    with _lock:
        _session_routes.clear()
        _revisions.clear()
        _pending.clear()
        _store_path = None
        _store_error = False
