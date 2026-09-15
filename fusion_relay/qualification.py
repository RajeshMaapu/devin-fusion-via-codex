"""Evidence-bound qualification receipt.

Diagnostics must derive ``qualification`` from evidence, not from the
presence of code. An operator records a receipt after a qualification
run: it names the achieved levels, the sha256 of every evidence artifact,
and the exact ``build_identity`` of the code tree that was qualified, all
under an HMAC keyed with the relay's identity key. At startup the relay
verifies the receipt and reports the highest level ONLY if the running
tree's build identity matches; any code change silently drops the
reported level back to ``local_tests`` until re-qualified.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import time

from .continuation import canonical
from .identity import PrivateDirectory, build_identity

RECEIPT_NAME = 'qualification-receipt.json'
LEVELS = ('local_tests', 'live_verified', 'benchmark_evaluated')
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')
MAX_RECEIPT_BYTES = 64 << 10


def _mac(secret: bytes, body: dict) -> str:
    return hmac.new(secret, canonical(body), hashlib.sha256).hexdigest()


def evidence_ref(path) -> str:
    """sha256 of an evidence file (the receipt stores refs, not paths)."""
    digest = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def record(private: PrivateDirectory, secret: bytes, levels, evidence_refs,
           note: str = '') -> dict:
    """Write a receipt for the CURRENT build identity. Replaces any
    existing receipt (the old one is invalid for a changed tree anyway)."""
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise ValueError('identity key unavailable')
    levels = sorted(set(levels), key=LEVELS.index)
    if not levels or any(lv not in LEVELS for lv in levels):
        raise ValueError('unknown qualification level')
    refs = sorted(set(evidence_refs))
    if not refs or any(not _HEX64.fullmatch(r) for r in refs):
        raise ValueError('evidence references required')
    if not isinstance(note, str) or len(note) > 512:
        raise ValueError('note too long')
    body = {'protocol': 1, 'build_identity': build_identity(),
            'levels': levels, 'evidence_refs': refs, 'note': note,
            'recorded_at': time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                         time.gmtime())}
    receipt = {'body': body, 'mac': _mac(secret, body)}
    data = json.dumps(receipt, indent=1, sort_keys=True).encode()
    try:
        private.read(RECEIPT_NAME, MAX_RECEIPT_BYTES)
        exists = True
    except FileNotFoundError:
        exists = False
    if exists:
        # replace atomically via a fresh name is not available on the
        # private-directory API; write-new requires absence, so unlink
        # through the same dirfd-pinned directory.
        import os
        os.unlink(RECEIPT_NAME, dir_fd=private._fd)
    private.write_new(RECEIPT_NAME, data)
    return receipt


def status(private: PrivateDirectory, secret) -> dict:
    """Verify the receipt against the running tree.

    Returns {'level': <highest verified level>, 'receipt': 'verified' |
    'absent' | 'invalid' | 'stale_build', 'evidence_count': n}.
    """
    try:
        raw = private.read(RECEIPT_NAME, MAX_RECEIPT_BYTES)
    except FileNotFoundError:
        return {'level': 'local_tests', 'receipt': 'absent',
                'evidence_count': 0}
    except OSError:
        return {'level': 'local_tests', 'receipt': 'invalid',
                'evidence_count': 0}
    try:
        receipt = json.loads(raw)
        body, mac = receipt['body'], receipt['mac']
        if not isinstance(secret, bytes) or len(secret) != 32 \
                or not isinstance(body, dict) or not isinstance(mac, str) \
                or not hmac.compare_digest(_mac(secret, body), mac):
            raise ValueError('mac')
        levels = body['levels']
        refs = body['evidence_refs']
        if body.get('protocol') != 1 or not isinstance(levels, list) \
                or any(lv not in LEVELS for lv in levels) \
                or not isinstance(refs, list) \
                or any(not isinstance(r, str) or not _HEX64.fullmatch(r)
                       for r in refs):
            raise ValueError('shape')
    except (ValueError, KeyError, TypeError):
        return {'level': 'local_tests', 'receipt': 'invalid',
                'evidence_count': 0}
    if body.get('build_identity') != build_identity():
        return {'level': 'local_tests', 'receipt': 'stale_build',
                'evidence_count': len(refs)}
    highest = max(levels, key=LEVELS.index)
    return {'level': highest, 'receipt': 'verified',
            'evidence_count': len(refs)}
