"""continuation_admin tests: injected ledger, fixed key — no Keychain."""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from fusion_relay import continuation_admin
from fusion_relay.continuation import (ContinuationBinding,
                                       ContinuationError,
                                       ContinuationLedger)

KEY = b'k' * 32
BINDING = ContinuationBinding(account='acct-1', session='s1',
                              lane='lead', profile='gpt-6-astra:high',
                              epoch='e1')


def _body(*roles):
    return {'input': [{'role': r, 'content': 'x'} for r in roles],
            'instructions': 'i', 'model': 'astra',
            'reasoning': {'effort': 'high'}}


def _ldb1(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchone()


class AdminTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = pathlib.Path(self._tmp.name).resolve()
        self.db = self.dir / 'continuation.sqlite3'
        p = patch.object(continuation_admin, '_load_key',
                         return_value=KEY)
        self.addCleanup(p.stop)
        p.start()

    def _run(self, args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), \
                contextlib.redirect_stderr(err):
            code = continuation_admin.main(args)
        return code, out.getvalue(), err.getvalue()

    def _unresolved(self):
        """Open a ledger, inject a provider_outcome_unknown row, close."""
        led = ContinuationLedger(self.db, KEY)
        res = led.reserve(BINDING, 'op-1', _body('user'))
        led.mark_outcome_unknown(res)
        led.close()

    def test_list_and_resolve_unknown(self):
        self._unresolved()
        code, out, err = self._run(
            ['list-unresolved', '--data-dir', str(self.dir)])
        self.assertEqual(code, 0)
        rows = [json.loads(l) for l in out.splitlines() if l.strip()]
        self.assertEqual(rows, [{'scope': BINDING.scope(),
                                 'operation_id': 'op-1',
                                 'status': 'provider_outcome_unknown'}])
        code, out, err = self._run(
            ['resolve-unknown', '--data-dir', str(self.dir),
             '--scope', BINDING.scope(), '--operation-id', 'op-1',
             '--evidence-ref', 'e' * 64])
        self.assertEqual((code, out.strip()), (0, 'resolved abandoned'))
        led = ContinuationLedger(self.db, KEY)
        self.addCleanup(led.close)
        self.assertEqual(_ldb1(
            led, "SELECT status FROM operations WHERE operation_id='op-1'"
            )[0], 'failed')
        # lane unblocked: a fresh reservation succeeds
        res = led.reserve(BINDING, 'op-2', _body('user'))
        self.assertEqual(res['operation_id'], 'op-2')
        self.assertIsNone(res['replay'])

    def test_executing_row_is_listed_and_resolvable(self):
        # crash-before-inference / admission failure leaves 'executing'
        led = ContinuationLedger(self.db, KEY)
        led.reserve(BINDING, 'op-x', _body('user'))
        led.close()
        code, out, err = self._run(
            ['list-unresolved', '--data-dir', str(self.dir)])
        rows = [json.loads(l) for l in out.splitlines() if l.strip()]
        self.assertEqual([r['status'] for r in rows], ['executing'])
        code, out, err = self._run(
            ['resolve-unknown', '--data-dir', str(self.dir),
             '--scope', BINDING.scope(), '--operation-id', 'op-x',
             '--evidence-ref', 'f' * 64])
        self.assertEqual((code, out.strip()), (0, 'resolved abandoned'))
        led = ContinuationLedger(self.db, KEY)
        self.addCleanup(led.close)
        self.assertEqual(_ldb1(
            led, "SELECT status FROM operations WHERE operation_id='op-x'"
            )[0], 'failed')
        res = led.reserve(BINDING, 'op-y', _body('user'))
        self.assertIsNone(res['replay'])

    def test_retryable_rows_only_with_flag(self):
        led = ContinuationLedger(self.db, KEY)
        res = led.reserve(BINDING, 'op-a', _body('user'))
        led.abandon(res, 'admission_rejected')
        led.close()
        code, out, err = self._run(
            ['list-unresolved', '--data-dir', str(self.dir)])
        self.assertEqual((code, out), (0, ''))
        code, out, err = self._run(
            ['list-unresolved', '--data-dir', str(self.dir),
             '--include-retryable'])
        rows = [json.loads(l) for l in out.splitlines() if l.strip()]
        self.assertEqual([r['status'] for r in rows],
                         ['admission_rejected'])

    def test_bad_evidence_ref_rejected(self):
        self._unresolved()
        code, out, err = self._run(
            ['resolve-unknown', '--data-dir', str(self.dir),
             '--scope', BINDING.scope(), '--operation-id', 'op-1',
             '--evidence-ref', 'not-hex'])
        self.assertEqual(code, 3)
        self.assertIn('evidence reference required', err)

    def test_owner_lock_reports_stop_host(self):
        self._unresolved()
        led = ContinuationLedger(self.db, KEY)
        self.addCleanup(led.close)
        code, out, err = self._run(
            ['list-unresolved', '--data-dir', str(self.dir)])
        self.assertEqual(code, 3)
        self.assertIn('stop the host first', err)
        self.assertEqual(out, '')

    def test_help(self):
        with self.assertRaises(SystemExit) as cm:
            continuation_admin.main(['--help'])
        self.assertEqual(cm.exception.code, 0)


if __name__ == '__main__':
    unittest.main()
