"""Authenticated per-turn marker carried through a native hook.

The Devin CLI's documented ``UserPromptSubmit`` hook may inject
``additionalContext``; that text arrives in the ``GetChatMessage`` packet
as a user-source message (verified live, CLI 3000.10.21). The hook
(``bin/fusion-prompt-marker``) renders a marker binding the hook's
``session_id`` (the session name) and ``prompt_id`` (per user turn) under
an HMAC keyed with the relay's identity key, which only the same user's
private data directory holds.

What a verified marker proves: the packet belongs to a CLI session in
which the relay's hook ran, for the named turn. What it does NOT prove:
the Fusion lane (lead/sidekick) — that remains a routing label — or that
the response was accepted. The marker is never stripped from the history.
"""
from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass

from .continuation import canonical
from .wire import Message, decode, text

MARKER_VERSION = 1
_PREFIX = 'fusion-relay-marker v1'
_SESSION_RE = r'[A-Za-z0-9._-]{1,64}'
_PROMPT_RE = r'[0-9a-fA-F-]{8,64}'
_MARKER_RE = re.compile(
    r'fusion-relay-marker v1 (?P<session>%s) (?P<prompt>%s) '
    r'(?P<mac>[0-9a-f]{64})' % (_SESSION_RE, _PROMPT_RE))
_SESSION_FULL = re.compile(_SESSION_RE + r'\Z')
_PROMPT_FULL = re.compile(_PROMPT_RE + r'\Z')
SRC_USER = 1


@dataclass(frozen=True)
class MarkerResult:
    status: str                 # 'verified' | 'absent' | 'invalid'
    session_name_ref: str | None = None   # reference('session', name)
    prompt_id: str | None = None


def _mac(secret: bytes, session: str, prompt: str) -> str:
    return hmac.new(secret, canonical(
        {'v': MARKER_VERSION, 'session': session, 'prompt': prompt}),
        hashlib.sha256).hexdigest()


def render(secret: bytes, session: str, prompt: str) -> str:
    """The ``additionalContext`` text the hook injects for one turn."""
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise ValueError('marker key unavailable')
    if not isinstance(session, str) or not _SESSION_FULL.fullmatch(session):
        raise ValueError('marker session unusable')
    if not isinstance(prompt, str) or not _PROMPT_FULL.fullmatch(prompt):
        raise ValueError('marker prompt unusable')
    return '%s %s %s %s' % (_PREFIX, session, prompt,
                            _mac(secret, session, prompt))


def extract(packet: Message, secret) -> MarkerResult:
    """Find the LAST marker in the packet's user-source messages and
    verify it. Absent → 'absent'; present but unverifiable → 'invalid'.
    Only hashed references leave this function."""
    from .accounting import reference
    last = None
    for raw in packet.get(3, []):
        try:
            msg = decode(raw)  # type: ignore[arg-type]
        except ValueError:
            continue
        if msg.get(2, [0])[0] != SRC_USER:
            continue
        body = text(msg, 3)
        if _PREFIX not in body:
            continue
        for match in _MARKER_RE.finditer(body):
            last = match
    if last is None:
        return MarkerResult('absent')
    if not isinstance(secret, bytes) or len(secret) != 32:
        return MarkerResult('invalid')
    session, prompt, mac = last.group('session', 'prompt', 'mac')
    if not hmac.compare_digest(_mac(secret, session, prompt), mac):
        return MarkerResult('invalid')
    return MarkerResult('verified', reference('session', session), prompt)
