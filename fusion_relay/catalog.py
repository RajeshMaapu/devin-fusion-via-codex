"""Catalog injection and per-session route pinning.

Adds labeled ``-codex`` / ``-native`` clones of every ``gpt-6-astra*`` and
``fusion-gpt-6-astra*`` entry to the ``GetCliModelConfigs`` response, so the
CLI's ``/model`` picker shows the Codex-subscription route as a real choice.

Selection flow:

1. ``/model`` shows e.g. ``GPT-6 Astra High Thinking · Codex sub``.
2. ``AssignModel`` arrives with the suffixed selector in field 2; the relay
   strips the suffix before Cognition sees it (Cognition only knows the
   canonical ids) and records ``session_uuid -> route``.
3. ``GetChatMessage`` carries the same session uuid in field 16; an astra
   packet consults the map — ``native`` forwards to Cognition, anything else
   takes the Codex route (the relay's default anyway).
"""

from __future__ import annotations

import re
import threading

from .wire import (decode, decode_typed, encode_typed, field, get_string,
                   set_string)

# Selectors that get labeled route clones in the picker.
CLONEABLE_RE = re.compile(rb"^(?:fusion-)?gpt-6-astra")
CODEX_SUFFIX = "-codex"
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


def inject_route_entries(body: bytes) -> tuple[bytes, int]:
    """Append -codex/-native clones to a raw GetCliModelConfigs response.

    Returns ``(new_body, injected_count)``. Protobuf repeated fields merge on
    concatenation, so new entries are simply appended to the body.
    """
    try:
        top = decode_typed(body)
    except ValueError:
        return body, 0
    injected = 0
    extras = bytearray()
    for raw, wtype in top.get(1, []):
        if wtype != 2 or not isinstance(raw, bytes):
            continue
        id_vals = decode_typed(raw).get(F_ENTRY_ID)
        if not id_vals:
            continue
        model_id = id_vals[0][0]
        if not isinstance(model_id, bytes) or not CLONEABLE_RE.match(model_id):
            continue
        for suffix, label in ((CODEX_SUFFIX, "Codex sub"),
                              (NATIVE_SUFFIX, "Native")):
            clone = _clone_entry(raw, suffix, label)
            if clone:
                extras += field(1, clone)
                injected += 1
    return body + bytes(extras), injected


def rewrite_assign(body: bytes) -> tuple[bytes, str | None]:
    """Strip a route suffix from an AssignModel selector; pin the session.

    Returns ``(new_body, requested_route_or_None)``. The session uuid in
    field 3 is recorded so later GetChatMessage packets can honor a
    ``-native`` pick while leaving the Codex route as the default.
    """
    try:
        msg = decode_typed(body)
    except ValueError:
        return body, None
    selector = get_string(msg, F_ASSIGN_SELECTOR)
    route = None
    if selector.endswith(CODEX_SUFFIX):
        route = "codex"
    elif selector.endswith(NATIVE_SUFFIX):
        route = "native"
    if route is None:
        return body, None
    set_string(msg, F_ASSIGN_SELECTOR, selector[: -len(route) - 1])
    session = get_string(msg, F_ASSIGN_SESSION)
    if session:
        with _lock:
            _session_routes[session] = route
    return encode_typed(msg), route


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
