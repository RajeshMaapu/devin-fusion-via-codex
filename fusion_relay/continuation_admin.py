"""Operator recovery for unresolved continuation operations.

``provider_outcome_unknown`` / ``cancel_unconfirmed`` rows block their
lane until an operator explicitly reconciles them. This tool lists
unresolved rows and settles them as 'abandoned' — a claimed success can
never be reconstructed, so no other resolution exists.

Only hashed/opaque identifiers are printed. Keys are loaded from the OS
key store exactly like the experimental host (never created here).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from .continuation import (ContinuationError, ContinuationLedger)

# Lane-blocking states an operator must settle explicitly.
_UNRESOLVED = ('executing', 'provider_outcome_unknown',
               'cancel_unconfirmed')
# Settled-before-dispatch states: not lane-blocking, an identical retry
# re-reserves; listed only with --include-retryable.
_RETRYABLE = ('preflight_rejected', 'admission_rejected')


def _load_key(service: str, account: str) -> bytes:
    from .keychain import MacOSKeychain
    return MacOSKeychain(service=service).load(account, create=False)


def _open_ledger(data_dir: Path, args):
    account = args.key_account or hashlib.sha256(
        str(data_dir).encode()).hexdigest()
    try:
        key = _load_key(args.key_service, account)
    except ContinuationError as e:
        print('fusion-continuation-admin: key_store unavailable: '
              f'{e}', file=sys.stderr)
        return None, 3
    try:
        return ContinuationLedger(
            data_dir / 'continuation.sqlite3', key), 0
    except ContinuationError as e:
        print('fusion-continuation-admin: continuation store '
              f'unavailable: {e} (stop the host first)',
              file=sys.stderr)
        return None, 3


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog='fusion-continuation-admin',
        description='Operator reconciliation for unresolved '
                    'continuation operations')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('list-unresolved', 'resolve-unknown'):
        p = sub.add_parser(name)
        p.add_argument('--data-dir', required=True)
        p.add_argument(
            '--key-service',
            default='ai.fusion-codex-relay.experimental')
        p.add_argument('--key-account', default=None,
                       help='hex64 key account; default sha256 of the '
                            'resolved data dir path')
    sub.choices['list-unresolved'].add_argument(
        '--include-retryable', action='store_true',
        help='also list preflight/admission-rejected rows (not '
             'lane-blocking; an identical retry re-reserves)')
    sub.choices['resolve-unknown'].add_argument('--scope', required=True)
    sub.choices['resolve-unknown'].add_argument('--operation-id',
                                                required=True)
    sub.choices['resolve-unknown'].add_argument('--evidence-ref',
                                                required=True,
                                                help='hex64')
    args = parser.parse_args(argv)
    data_dir = Path(args.data_dir).resolve()

    ledger, code = _open_ledger(data_dir, args)
    if ledger is None:
        return code
    try:
        if args.command == 'list-unresolved':
            statuses = _UNRESOLVED + (
                _RETRYABLE if args.include_retryable else ())
            with ledger._lock:
                rows = ledger._db.execute(
                    'SELECT scope, operation_id, status FROM operations '
                    'WHERE status IN (%s) ORDER BY scope, operation_id'
                    % ','.join('?' * len(statuses)), statuses).fetchall()
            for scope, op, status in rows:
                print(json.dumps({'scope': scope, 'operation_id': op,
                                  'status': status}, sort_keys=True))
            return 0
        try:
            ledger.resolve_unknown(args.scope, args.operation_id,
                                   args.evidence_ref, 'abandoned')
        except ContinuationError as e:
            print(f'fusion-continuation-admin: resolve failed: {e}',
                  file=sys.stderr)
            return 3
        print('resolved abandoned')
        return 0
    finally:
        ledger.close()


if __name__ == '__main__':
    sys.exit(main())
