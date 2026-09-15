"""Session diagnostics: bounded process-only view keyed by hashed
session references. Nothing here persists raw prompts, outputs, or
credentials."""
from __future__ import annotations

import re
import threading
import time
from collections import OrderedDict

_MAX_SESSIONS = 64
_lock = threading.Lock()
_sessions: OrderedDict = OrderedDict()
# Set by the relay at startup from a verified qualification receipt
# (fusion_relay.qualification.status); defaults to the weakest level.
_qualification = {'level': 'local_tests', 'receipt': 'absent',
                  'evidence_count': 0}
_QUALIFICATION_LEVELS = ('local_tests', 'live_verified',
                         'benchmark_evaluated')


def set_qualification(status: dict) -> None:
    global _qualification
    level = status.get('level') if isinstance(status, dict) else None
    receipt = status.get('receipt') if isinstance(status, dict) else None
    _qualification = {
        'level': level if level in _QUALIFICATION_LEVELS else 'local_tests',
        'receipt': receipt if receipt in ('verified', 'absent', 'invalid',
                                          'stale_build') else 'absent',
        'evidence_count': status.get('evidence_count', 0)
        if isinstance(status, dict)
        and type(status.get('evidence_count')) is int else 0}


def qualification() -> dict:
    return dict(_qualification)

_MODEL_RE = re.compile(
    r'(?:gpt-6-astra|swe-2)(?:-(?:none|low|medium|high|xhigh|max|fast))*\Z')
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')
_ROUTES = {'codex', 'cognition-forward', 'reject'}
_CONTINUATION = {'durable_host_binding_required', 'durable_host_bound',
                 'legacy_memory_only_unqualified', 'preflight_rejected',
                 'admission_rejected', 'blocked'}
# Mapped onto the bounded label set; the raw status is retained under
# 'continuation_detail' when it maps to 'blocked'.
_CONTINUATION_MAP = {
    'durable_host_bound': 'durable',
    'legacy_memory_only_unqualified': 'legacy_memory_only',
    'durable_host_binding_required': 'blocked',
    'preflight_rejected': 'blocked',
    'admission_rejected': 'blocked',
    'durable': 'durable',
    'legacy_memory_only': 'legacy_memory_only',
    'blocked': 'blocked',
    'in_memory_only': 'in_memory_only',
    'disabled': 'disabled',
    'legacy_durable': 'legacy_durable',
}
_BINDING = {'verified', 'unavailable', 'invalid'}
# 'history_evidenced' = the consumer later replayed this turn's visible
# projection in its own history; qualified evidence, not an ack.
_ACCEPTANCE = {'acknowledged', 'history_evidenced', 'pending', 'uncertain'}
_KEY_STORE = {'ready', 'unavailable'}
# There is no provider cancellation-confirmation signal anywhere, so
# 'confirmed' intentionally does not exist.
_CANCELLATION = {'requested', 'uncertain', 'not_observed'}
_EPOCH_TRANSITION = {'model_switch', 'history_compaction', 'fork',
                     'account_change', 'legacy_history',
                     'history_divergence', 'operator_recovery'}
_CORRELATION = {'matched_marker', 'matched_seed'}
_MARKER = {'verified', 'absent', 'invalid'}
_ROLES = {'lead', 'sidekick'}
_OUTCOMES = {'completed', 'incomplete', 'failed', 'cancelled', 'unknown'}


def _continuation_fields(rec):
    continuation = rec.get('continuation_status')
    if not isinstance(continuation, str):
        return 'legacy_memory_only', None
    mapped = _CONTINUATION_MAP.get(continuation)
    if mapped is None:
        return 'blocked', continuation[:64] \
            if len(continuation) <= 64 else 'unverified'
    detail = continuation[:64] if mapped != continuation else None
    return mapped, detail


def record(packet_model: str, session_ref: str, route: str,
           rec: dict) -> None:
    """Stash one whitelisted diagnostic entry for a session."""
    if not isinstance(session_ref, str) \
            or not _HEX64.fullmatch(session_ref):
        return
    revision = rec.get('continuation_revision')
    epoch_ref = rec.get('continuation_epoch_ref')
    role = rec.get('continuation_role')
    outcome = rec.get('codex_status', 'unknown')
    terminated = rec.get('termination_confirmed')
    continuation, continuation_detail = _continuation_fields(rec)
    binding = rec.get('binding_status')
    acceptance = rec.get('acceptance')
    key_store = rec.get('key_store_status')
    cancellation = rec.get('cancellation')
    transition = rec.get('epoch_transition')
    correlation = rec.get('compaction_correlation')
    marker_status = rec.get('marker_status')
    entry = {
        'session_ref': session_ref,
        'model': packet_model
                 if isinstance(packet_model, str)
                 and _MODEL_RE.fullmatch(packet_model)
                 else 'unverified',
        'route': route if isinstance(route, str) and route in _ROUTES
                 else 'unverified',
        'role': role if isinstance(role, str) and role in _ROLES
                else 'unverified',
        'continuation': continuation,
        'continuation_detail': continuation_detail,
        'continuation_revision': revision
                                 if isinstance(revision, int)
                                 and not isinstance(revision, bool)
                                 and revision >= 0 else None,
        'continuation_epoch_ref': epoch_ref
                                  if isinstance(epoch_ref, str)
                                  and _HEX64.fullmatch(epoch_ref)
                                  else None,
        'protocol_version': 1,
        'binding': binding if binding in _BINDING else 'unavailable',
        'acceptance': acceptance
                      if acceptance in _ACCEPTANCE else 'uncertain',
        'acceptance_prior': 'history_evidenced'
                            if type(rec.get('turns_history_evidenced'))
                            is int and rec['turns_history_evidenced'] > 0
                            else 'none',
        'key_store': key_store
                     if key_store in _KEY_STORE else 'unavailable',
        'cancellation': 'requested' if cancellation == 'requested'
                        else 'uncertain'
                        if rec.get('client_gone') is True
                        or cancellation == 'uncertain'
                        else 'not_observed',
        'epoch_transition': transition
                            if transition in _EPOCH_TRANSITION else None,
        'qualification': _qualification['level'],
        'qualification_receipt': _qualification['receipt'],
        # 'matched_marker': a compaction note was correlated through the
        # verified UserPromptSubmit marker (hook session name); the seed
        # path can only match when the hook id equals field 16 (it does
        # not in CLI 3000.10.21). Otherwise the correlation is unverified.
        'compaction_correlation': correlation
                                  if correlation in _CORRELATION
                                  else 'unverified',
        'marker': marker_status if marker_status in _MARKER else 'absent',
        'last_outcome': 'error' if rec.get('error_category')
                        else outcome
                        if isinstance(outcome, str)
                        and outcome in _OUTCOMES else 'unknown',
        'termination_confirmed': terminated
                                 if isinstance(terminated, bool)
                                 else None,
        'computer_provider': 'disabled',
        'native_ack_contract': 'unavailable',
    }
    with _lock:
        prior = _sessions.get(session_ref)
        if prior is not None and 'compaction_events' in prior:
            entry['compaction_events'] = prior['compaction_events']
            entry['compaction_last'] = prior['compaction_last']
        _sessions[session_ref] = entry
        _sessions.move_to_end(session_ref)
        while len(_sessions) > _MAX_SESSIONS:
            _sessions.popitem(last=False)


def record_compaction(session_ref: str) -> None:
    """A /compact hook fired for this session ref.

    Correlation between the hook session_id and the wire seed is
    unverified; only a counter and timestamp are retained.
    """
    if not isinstance(session_ref, str) \
            or not _HEX64.fullmatch(session_ref):
        return
    with _lock:
        entry = _sessions.setdefault(session_ref, {
            'session_ref': session_ref,
            'qualification': _qualification['level'],
            'compaction_correlation': 'unverified'})
        entry['compaction_events'] = entry.get('compaction_events', 0) + 1
        entry['compaction_last'] = time.time()
        _sessions.move_to_end(session_ref)
        while len(_sessions) > _MAX_SESSIONS:
            _sessions.popitem(last=False)


def snapshot(session_ref: str) -> dict | None:
    """Return the stored entry for a session ref, or None."""
    if not isinstance(session_ref, str) \
            or not _HEX64.fullmatch(session_ref):
        return None
    with _lock:
        entry = _sessions.get(session_ref)
        return dict(entry) if entry is not None else None
