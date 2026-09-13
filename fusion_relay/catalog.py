"""Catalog injection and per-session route pinning.

Relabels every ``gpt-6-astra*`` / ``fusion-gpt-6-astra*`` entry in the
``GetCliModelConfigs`` response with a ``· Codex sub`` marker (the canonical
id IS the Codex route under the relay) and appends one ``-native`` clone per
entry as the in-picker escape hatch back to Cognition-billed Astra.

Selection flow:

1. ``/model`` shows e.g. ``GPT-6 Astra High Thinking · Codex sub``.
2. ``AssignModel`` arrives with a ``-native`` selector in field 2; the relay
   strips the suffix before Cognition sees it (Cognition only knows canonical
   ids). The pin is committed ONLY after upstream accepts the assignment —
   a failed selection leaves the previous route intact.
3. ``GetChatMessage`` carries the same session uuid in field 16; an astra
   packet consults the map — ``native`` forwards to Cognition, anything else
   takes the Codex route (the relay's default anyway).

Pins persist to ``routes.json`` under the data dir so a relay restart does
not silently reroute an existing session.
"""

from __future__ import annotations

import json
import pathlib
import re
import threading

from .wire import (decode_typed, encode_typed, field, get_string, set_string)

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
_store_path: pathlib.Path | None = None


def attach_store(path: pathlib.Path) -> None:
    """Persist route pins under *path*; loads any pins from a prior run."""
    global _store_path
    with _lock:
        _store_path = path
        try:
            saved = json.loads(path.read_text())
        except (OSError, ValueError):
            saved = {}
        for k, v in saved.items():
            if v in ("codex", "native"):
                _session_routes[k] = v


def _save_locked() -> None:
    if _store_path is None:
        return
    tmp = _store_path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(_session_routes))
        tmp.replace(_store_path)
    except OSError:
        pass


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

    Returns ``(new_body, session_uuid, requested_route)``. The pin is NOT
    committed here — the caller commits via :func:`pin_route` only after the
    upstream assignment succeeds, so a failed selection cannot repin.
    """
    try:
        msg = decode_typed(body)
    except ValueError:
        return body, "", ""
    selector = get_string(msg, F_ASSIGN_SELECTOR)
    if not selector.endswith(NATIVE_SUFFIX):
        return body, "", ""
    set_string(msg, F_ASSIGN_SELECTOR, selector[: -len(NATIVE_SUFFIX)])
    session = get_string(msg, F_ASSIGN_SESSION)
    return encode_typed(msg), session, "native"


def pin_route(session: str, route: str) -> None:
    """Commit a session route after its assignment succeeded upstream."""
    if not session:
        return
    with _lock:
        _session_routes[session] = route
        _save_locked()


def session_route(packet) -> str | None:
    """Look up the pinned route for a decoded GetChatMessage packet."""
    session = ""
    v = packet.get(16)
    if v and isinstance(v[0], bytes):
        session = v[0].decode()
    if not session:
        return None
    with _lock:
        return _session_routes.get(session)


def reset() -> None:
    """Clear the session map (tests)."""
    with _lock:
        _session_routes.clear()
