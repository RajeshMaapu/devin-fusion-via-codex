from __future__ import annotations

from .accounting import FIELDS, MAX_COUNT, STATUSES, reference


def _count(value):
    return value if type(value) is int and 0 <= value <= MAX_COUNT else None


def events_for_record(rec, operation_id, provider, account_ref):
    base = {'provider': provider, 'account_ref': account_ref}
    if provider == 'codex':
        calls = rec.get('codex_usage_calls')
        if not isinstance(calls, list) or not calls:
            calls = [{}]
        events = []
        for index, call in enumerate(calls):
            status = call.get('status')
            events.append(dict(base,
                event_id=reference(provider, account_ref, operation_id, str(index)),
                status=status if status in STATUSES else 'unknown',
                **{key: _count(call.get(key)) for key in FIELDS}))
        return events
    usage = rec.get('cognition_usage')
    usage = usage if isinstance(usage, dict) else {}
    status = 'cancelled' if rec.get('client_gone') else (
        'failed' if rec.get('error_category') or rec.get('upstream_status', 500) >= 400
        else 'completed')
    return [dict(base, event_id=reference(provider, account_ref, operation_id, '0'),
                 status=status, input_tokens=_count(usage.get('input')),
                 output_tokens=_count(usage.get('output')),
                 cached_tokens=_count(usage.get('cached')), reasoning_tokens=None)]
