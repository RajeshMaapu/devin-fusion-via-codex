from __future__ import annotations

import hashlib


FIELDS = ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens")
ROLES = frozenset({"lead", "sidekick", "unverified"})
STATUSES = frozenset({"completed", "incomplete", "failed", "cancelled", "unknown"})


def token_count(value):
    return value if type(value) is int and value >= 0 else None


def normalized_usage(value) -> dict:
    value = value if isinstance(value, dict) else {}
    inputs = value.get("input_tokens_details")
    outputs = value.get("output_tokens_details")
    return {
        "input_tokens": token_count(value.get("input_tokens")),
        "output_tokens": token_count(value.get("output_tokens")),
        "cached_tokens": token_count(inputs.get("cached_tokens")) if isinstance(inputs, dict) else None,
        "reasoning_tokens": token_count(outputs.get("reasoning_tokens")) if isinstance(outputs, dict) else None,
    }


def aggregate(entries: list[dict]) -> dict:
    totals = {}
    missing = {}
    for key in FIELDS:
        known = [entry[key] for entry in entries if entry.get(key) is not None]
        totals[key] = sum(known) if known else None
        missing[key] = len(entries) - len(known)
    return {
        "input_tokens": totals["input_tokens"],
        "output_tokens": totals["output_tokens"],
        "input_tokens_details": {"cached_tokens": totals["cached_tokens"]},
        "output_tokens_details": {"reasoning_tokens": totals["reasoning_tokens"]},
        "response_count": len(entries),
        "unknown_calls": sum(entry.get("input_tokens") is None or entry.get("output_tokens") is None for entry in entries),
        "missing_fields": missing,
        "partial": any(missing.values()),
    }


def record_usage(complete: dict, rec: dict, *, role: str = "unverified") -> None:
    role = role if role in ROLES else "unverified"
    response_id = complete.get("id")
    entries = rec.setdefault("_codex_response_usage", {})
    identified = isinstance(response_id, str) and bool(response_id)
    identity = response_id if identified else "unidentified-" + str(len(entries))
    key = hashlib.sha256((role + "\0" + identity).encode()).hexdigest()
    status = complete.get("status")
    status = status if status in STATUSES else "unknown"
    item = dict(normalized_usage(complete.get("usage")), response_ref=key,
                role=role, status=status, identified=identified)
    previous = entries.get(key)
    if previous is None:
        entries[key] = item
    else:
        for field in FIELDS:
            if previous[field] is None:
                previous[field] = item[field]
            elif item[field] is not None and previous[field] != item[field]:
                previous["conflict"] = True
        if previous["status"] == "unknown":
            previous["status"] = status
    values = list(entries.values())
    rec["codex_usage_calls"] = [dict(value) for value in values]
    rec["codex_usage"] = aggregate(values)
    rec["codex_usage_by_role"] = {
        current: aggregate([value for value in values if value["role"] == current])
        for current in sorted({value["role"] for value in values})
    }
    rec["codex_status"] = status


def add_to_totals(totals: dict, usage: dict | str) -> None:
    if usage == "unknown":
        totals["unknown_calls"] = totals.get("unknown_calls", 0) + 1
        return
    if not isinstance(usage, dict):
        return
    values = normalized_usage(usage)
    for source, destination in (("input_tokens", "input"), ("output_tokens", "output"),
                                ("cached_tokens", "cached"), ("reasoning_tokens", "reasoning")):
        value = values[source]
        if value is not None:
            totals[destination] = totals.get(destination, 0) + value
    totals["unknown_calls"] = totals.get("unknown_calls", 0) + usage.get("unknown_calls", int(values["input_tokens"] is None or values["output_tokens"] is None))
    totals["responses"] = totals.get("responses", 0) + usage.get("response_count", 1)
    missing = totals.setdefault("missing_fields", {})
    for name, count in usage.get("missing_fields", {key: int(value is None) for key, value in values.items()}).items():
        if name in FIELDS and type(count) is int and count >= 0:
            missing[name] = missing.get(name, 0) + count
