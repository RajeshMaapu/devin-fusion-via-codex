"""Local payload budgets for translated provider requests.

These are LOCAL conservative limits, not vendor limits. They exist so an
image-accumulating conversation is rejected with an actionable error
before an oversized body reaches the provider — the upstream limit is
unverified and this module makes no claim about it.

Nothing here stores image content: measurement counts occurrences and
byte totals and keeps only sha256 digests of base64 text transiently for
uniqueness. Raw URLs, base64, and message text never enter a report.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

BUDGET_POLICY_VERSION = 1

RECOVERY_INSTRUCTION = (
    "compact the conversation (/compact) or start a new session from a "
    "checkpoint; retrying the same request will not succeed.")

# Bounded read of a rejecting upstream's error body for classification;
# the bytes are inspected then discarded, never stored or logged.
MAX_ERROR_BODY_BYTES = 8192

_SAFE_TOKEN_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")


@dataclass(frozen=True)
class BudgetProfile:
    route: str
    profile: str
    max_image_occurrences: int
    max_image_bytes_total: int
    max_serialized_bytes: int
    provenance: str
    verified_on: str | None
    upstream_limit_status: str


# LOCAL conservative limits for the codex route — NOT vendor limits.
# The upstream limit is unverified; these bounds only guarantee the
# relay rejects obviously-hopeless payloads before dispatch.
CODEX_PROFILE = BudgetProfile(
    route='codex',
    profile='codex-responses-chatgpt-backend',
    max_image_occurrences=40,
    max_image_bytes_total=24 << 20,
    max_serialized_bytes=32 << 20,
    provenance='local_conservative_limit',
    verified_on=None,
    upstream_limit_status='unverified')


def profile_for(route: str) -> BudgetProfile | None:
    """Route-specific budget profile; native forwarding has none."""
    if route == 'codex':
        return CODEX_PROFILE
    return None


@dataclass
class PayloadReport:
    """Scalar measurements of one outgoing payload — never content."""
    incoming_wire_bytes: int | None = None
    decompressed_bytes: int | None = None
    final_serialized_bytes: int | None = None
    image_occurrences: int | None = None
    unique_image_count: int | None = None
    image_bytes_total: int | None = None
    historical_image_count: int | None = None
    current_turn_image_count: int | None = None
    # True when some image part's bytes could not be measured, leaving
    # image_bytes_total a lower bound.
    image_bytes_partial: bool = False
    route: str = ''
    profile: str = ''
    budget_policy_version: int = BUDGET_POLICY_VERSION
    rejection_origin: str | None = None

    def safe_dict(self) -> dict:
        """Only the scalar fields above — safe to log as-is."""
        return {
            'incoming_wire_bytes': self.incoming_wire_bytes,
            'decompressed_bytes': self.decompressed_bytes,
            'final_serialized_bytes': self.final_serialized_bytes,
            'image_occurrences': self.image_occurrences,
            'unique_image_count': self.unique_image_count,
            'image_bytes_total': self.image_bytes_total,
            'historical_image_count': self.historical_image_count,
            'current_turn_image_count': self.current_turn_image_count,
            'image_bytes_partial': self.image_bytes_partial,
            'route': self.route,
            'profile': self.profile,
            'budget_policy_version': self.budget_policy_version,
            'rejection_origin': self.rejection_origin,
        }


class BudgetExceeded(ValueError):
    """A local payload budget was exceeded; the request is rejected."""

    def __init__(self, kind: str, report: PayloadReport, limit: int,
                 measured: int):
        super().__init__(kind)
        if kind not in ('wire_bytes', 'image_count', 'image_bytes',
                        'translated_bytes'):
            raise ValueError('unknown budget kind')
        self.kind = kind
        self.report = report
        self.limit = limit
        self.measured = measured
        report.rejection_origin = 'local_' + kind

    def user_message(self) -> str:
        return (
            "payload budget exceeded (%s: %d > %d, policy v%d, local "
            "limit; upstream limit unverified). Recovery: %s" % (
                self.kind, self.measured, self.limit,
                self.report.budget_policy_version, RECOVERY_INSTRUCTION))


def coarse_incoming_check(wire_bytes: int, profile: BudgetProfile,
                          report: PayloadReport) -> None:
    """Reject before translation when even the base64 lower bound blows
    the serialized budget (base64 expansion is >= 4/3 of raw bytes)."""
    report.incoming_wire_bytes = wire_bytes
    if wire_bytes * 4 // 3 > profile.max_serialized_bytes:
        raise BudgetExceeded('wire_bytes', report,
                             profile.max_serialized_bytes,
                             wire_bytes * 4 // 3)


def _image_parts(item: dict) -> list:
    if item.get('role') == 'user' and isinstance(item.get('content'), list):
        return [p for p in item['content']
                if isinstance(p, dict) and p.get('type') == 'input_image']
    if item.get('type') == 'function_call_output' \
            and isinstance(item.get('output'), list):
        return [p for p in item['output']
                if isinstance(p, dict) and p.get('type') == 'input_image']
    return []


def _data_url_bytes(url) -> int | None:
    """Decoded-length arithmetic on a ``data:<mime>;base64,<b64>`` URL.

    Never decodes: returns ``len(b64)*3//4 - padding``. None when the
    part is not a base64 data URL.
    """
    if not isinstance(url, str) or not url.startswith('data:'):
        return None
    head, sep, b64 = url.partition(',')
    if not sep or not head.endswith(';base64'):
        return None
    padding = len(b64) - len(b64.rstrip('='))
    return len(b64) * 3 // 4 - padding


def measure_body(body: dict, profile: BudgetProfile,
                 report: PayloadReport | None = None) -> PayloadReport:
    """Count image occurrences and byte totals in ``body['input']``.

    Every occurrence counts (duplicates included). Historical vs current
    turn splits at the last assistant/function_call/reasoning item; with
    no such boundary everything is current. Enforces the profile's image
    occurrence and byte budgets.
    """
    if report is None:
        report = PayloadReport(route=profile.route, profile=profile.profile,
                               budget_policy_version=BUDGET_POLICY_VERSION)
    items = body.get('input', [])
    boundary = -1
    for idx, item in enumerate(items):
        if isinstance(item, dict) and (
                item.get('role') == 'assistant'
                or item.get('type') in ('function_call', 'reasoning')):
            boundary = idx
    occurrences = 0
    historical = 0
    current = 0
    total_bytes = 0
    unique: set = set()
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        parts = _image_parts(item)
        if not parts:
            continue
        for part in parts:
            occurrences += 1
            if boundary >= 0 and idx <= boundary:
                historical += 1
            else:
                current += 1
            size = _data_url_bytes(part.get('image_url'))
            if size is None:
                report.image_bytes_partial = True
                continue
            total_bytes += size
            unique.add(hashlib.sha256(
                part['image_url'].partition(',')[2].encode()).digest())
    report.image_occurrences = occurrences
    report.unique_image_count = len(unique)
    report.image_bytes_total = total_bytes
    report.historical_image_count = historical
    report.current_turn_image_count = current
    if occurrences > profile.max_image_occurrences:
        raise BudgetExceeded('image_count', report,
                             profile.max_image_occurrences, occurrences)
    if total_bytes > profile.max_image_bytes_total:
        raise BudgetExceeded('image_bytes', report,
                             profile.max_image_bytes_total, total_bytes)
    return report


def serialize_and_check(body: dict, profile: BudgetProfile,
                        report: PayloadReport) -> bytes:
    """Serialize once and validate the exact bytes callers must send."""
    data = json.dumps(body).encode()
    report.final_serialized_bytes = len(data)
    if len(data) > profile.max_serialized_bytes:
        raise BudgetExceeded('translated_bytes', report,
                             profile.max_serialized_bytes, len(data))
    return data


@dataclass(frozen=True)
class UpstreamRejection:
    """Bounded, content-free classification of an upstream error body."""
    status: int
    code: str | None
    error_type: str | None
    classification: str
    certainty: str


_CLASSIFICATIONS = {'image_count', 'image_dimensions_or_format',
                    'context_limit', 'payload_too_large',
                    'unknown_upstream_rejection'}


def _classify_message(message: str, status: int) -> tuple[str, bool]:
    """Keyword rules over a lowercase message copy; (class, matched)."""
    if ('too many images' in message or 'image count' in message
            or 'number of images' in message):
        return 'image_count', True
    if 'image' in message and any(
            k in message for k in
            ('dimension', 'resolution', 'format', 'decode',
             'unsupported', 'pixels')):
        return 'image_dimensions_or_format', True
    if ('context_length' in message or 'context length' in message
            or 'maximum context' in message or 'too many tokens' in message
            or 'context window' in message):
        return 'context_limit', True
    if ('too large' in message or 'payload' in message
            or 'request entity' in message):
        return 'payload_too_large', True
    if status == 413:
        return 'payload_too_large', False
    return 'unknown_upstream_rejection', False


def classify_upstream_error(status: int, body: bytes) -> UpstreamRejection:
    """Classify a bounded error body; never retains or returns its text.

    ``body`` is at most ``MAX_ERROR_BODY_BYTES + 1`` bytes; anything over
    the cap (or unparseable) classifies without inspecting content.
    """
    parsed = None
    if len(body) <= MAX_ERROR_BODY_BYTES:
        try:
            candidate = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            candidate = None
        if isinstance(candidate, dict):
            parsed = candidate
    if parsed is None:
        return UpstreamRejection(
            status, None, None,
            'payload_too_large' if status == 413
            else 'unknown_upstream_rejection', 'unknown')
    error = parsed.get('error')
    if not isinstance(error, dict):
        error = {}
    code = error.get('code')
    etype = error.get('type')
    message = error.get('message')
    code = code if isinstance(code, str) and _SAFE_TOKEN_RE.fullmatch(code) \
        else None
    etype = etype if isinstance(etype, str) \
        and _SAFE_TOKEN_RE.fullmatch(etype) else None
    if isinstance(message, str):
        classification, matched = _classify_message(message.lower(), status)
    elif status == 413:
        classification, matched = 'payload_too_large', False
    else:
        classification, matched = 'unknown_upstream_rejection', False
    return UpstreamRejection(status, code, etype, classification,
                             'inferred' if matched else 'unknown')


def connect_code_for(classification: str) -> str:
    if classification in ('image_count', 'payload_too_large',
                          'context_limit', 'image_dimensions_or_format'):
        return 'resource_exhausted'
    return 'unavailable'
