import base64
import ctypes
import hashlib
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fusion_relay.continuation import (ContinuationBinding,
                                       ContinuationError,
                                       ContinuationLedger, digest)
from fusion_relay import continuation

KEY = b'k' * 32

def _ldb1(led, sql, params=()):
    """Locked single-row access — /usr/bin/python3 3.9 sqlite3 crashes
    on concurrent statements on one connection; hold the ledger lock."""
    with led._lock:
        return led._db.execute(sql, params).fetchone()


def _ldba(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchall()


def _ldbw(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params)

OTHER_KEY = b'z' * 32

BINDING = dict(account='a', session='s', lane='lead',
               profile='astra-high', epoch='e1')


def binding(**over):
    return ContinuationBinding(**dict(BINDING, **over))


def body(turns):
    return {'input': list(turns), 'instructions': 'i', 'model': 'astra',
            'reasoning': {'effort': 'high'}}


def user(text):
    return {'role': 'user', 'content': text}


def assistant(text):
    return {'role': 'assistant', 'content': text}


def reasoning(rid, blob):
    return {'type': 'reasoning', 'id': rid, 'encrypted_content': blob}


def message(mid, text):
    return {'type': 'message', 'id': mid,
            'content': [{'type': 'output_text', 'text': text}]}


OUTPUT1 = [reasoning('r1', 'opaque1'), message('m1', 'answer1')]
OUTPUT2 = [reasoning('r2', 'opaque2'), message('m2', 'answer2')]


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = (pathlib.Path(self.tmp.name).resolve()
                     / 'continuation.sqlite3')
        self.led = ContinuationLedger(self.path, KEY)
        self.addCleanup(lambda: self.led.close())
        self.b = binding()

    def reserve(self, op, turns, b=None):
        return self.led.reserve(b or self.b, op, body(turns))


class TestBinding(LedgerCase):
    def test_missing_fields_and_lane_rejected(self):
        for over in ({'account': ''}, {'session': ''}, {'epoch': ''},
                     {'lane': 'guest'}, {'lane': ''}):
            with self.assertRaises(ContinuationError):
                binding(**over).scope()
        self.assertIsInstance(self.b.scope(), str)


class TestReserveCommit(LedgerCase):
    def test_first_turn_and_projection(self):
        r1 = self.reserve('q1', [user('one')])
        self.assertEqual(r1['revision'], 0)
        self.assertEqual(r1['body']['input'], [user('one')])
        self.assertIsNone(r1['replay'])
        self.led.commit(r1, OUTPUT1, b'wire1')

        r2 = self.reserve('q2', [user('one'), assistant('answer1'),
                                 user('two')])
        self.assertEqual(r2['revision'], 1)
        self.assertEqual(r2['body']['input'],
                         [user('one'), reasoning('r1', 'opaque1'),
                          message('m1', 'answer1'), user('two')])
        self.led.commit(r2, OUTPUT2, b'wire2')

        r3 = self.reserve('q3', [user('one'), assistant('answer1'),
                                 user('two'), assistant('answer2'),
                                 user('three')])
        self.assertEqual(r3['body']['input'],
                         [user('one')] + OUTPUT1 + [user('two')]
                         + OUTPUT2 + [user('three')])

    def test_replay_same_body_no_new_inference(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        again = self.reserve('q1', [user('one')])
        self.assertEqual(again['replay'], b'wire1')
        self.assertNotIn('body', again)

    def test_same_operation_changed_body_rejects(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        with self.assertRaises(ContinuationError):
            self.reserve('q1', [user('different')])

    def test_concurrent_reservation_rejects(self):
        self.reserve('q1', [user('one')])
        with self.assertRaises(ContinuationError):
            self.reserve('q2', [user('one')])

    def test_independent_scopes_both_reserve(self):
        other = binding(session='s2')
        self.assertIsNotNone(self.reserve('q1', [user('one')]))
        self.assertIsNotNone(self.reserve('q1', [user('x')], b=other))

    def test_commit_stale_and_mismatch_atomic(self):
        r1 = self.reserve('q1', [user('one')])
        bad = dict(r1, native_body=body([user('tampered')]))
        with self.assertRaises(ContinuationError):
            self.led.commit(bad, OUTPUT1, b'wire1')
        self.led.commit(r1, OUTPUT1, b'wire1')
        with self.assertRaises(ContinuationError):
            self.led.commit(r1, OUTPUT2, b'wire2')
        snap = _ldb1(self.led, 
            "SELECT COUNT(*) FROM continuation_turns")
        self.assertEqual(snap[0], 1)

    def test_wrong_key_rejects(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        self.led.close()
        with self.assertRaises(ContinuationError):
            ContinuationLedger(self.path, OTHER_KEY)
        self.led = ContinuationLedger(self.path, KEY)

    def test_corrupted_sealed_fails(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        _ldbw(self.led, 
            "UPDATE continuation_turns SET sealed=? WHERE revision=1",
            ('AAAA' + 'A' * 40,))
        self.led.close()
        # key-check row is intact so reopen succeeds; the corrupted turn
        # surfaces when history is read.
        self.led = ContinuationLedger(self.path, KEY)
        with self.assertRaises(ContinuationError):
            self.reserve('q2', [user('one'), assistant('answer1'),
                                user('two')])

    def test_changed_history_requires_epoch(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        for hist in ([user('one'), assistant('other'), user('two')],
                     [user('rewritten'), user('two')],
                     [user('one'), user('two')]):
            with self.assertRaises(ContinuationError):
                self.reserve('q2', hist)

    def test_legacy_history_rejected(self):
        led = ContinuationLedger(
            pathlib.Path(self.tmp.name).resolve() / 'fresh.sqlite3', KEY)
        self.addCleanup(led.close)
        for hist in ([user('one'), assistant('a'), user('two')],
                     [{'type': 'function_call', 'call_id': 'c',
                       'name': 'n', 'arguments': '{}'}]):
            with self.assertRaises(ContinuationError):
                led.reserve(self.b, 'x', body(hist))

    def test_retention_limit_rejects_next(self):
        with mock.patch.object(continuation, 'MAX_TURNS', 1):
            r1 = self.reserve('q1', [user('one')])
            self.led.commit(r1, OUTPUT1, b'wire1')
            with self.assertRaises(ContinuationError):
                self.reserve('q2', [user('one'), assistant('answer1'),
                                    user('two')])
        count = _ldb1(self.led, 
            "SELECT COUNT(*) FROM continuation_turns")[0]
        self.assertEqual(count, 1)

    def test_function_call_alignment(self):
        call = {'type': 'function_call', 'call_id': 'c1', 'name': 'tool',
                'arguments': '{}'}
        out = [reasoning('r1', 'op1'), call]
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, out, b'wire1')
        r2 = self.reserve('q2', [user('one'), call,
                                 {'type': 'function_call_output',
                                  'call_id': 'c1', 'output': 'res'},
                                 user('two')])
        self.assertEqual(r2['body']['input'],
                         [user('one'), reasoning('r1', 'op1'), call,
                          {'type': 'function_call_output',
                           'call_id': 'c1', 'output': 'res'},
                          user('two')])
        self.led.commit(r2, OUTPUT2, b'wire2')
        changed = dict(call, call_id='cX')
        with self.assertRaises(ContinuationError):
            self.reserve('q3', [user('one'), changed,
                                {'type': 'function_call_output',
                                 'call_id': 'cX', 'output': 'res'},
                                user('two')])

    def test_no_plaintext_marker(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        self.led.close()
        raw = self.path.read_bytes()
        for marker in (b'answer1', b'opaque1', b'wire1', b'one'):
            self.assertNotIn(marker, raw)
        self.led = ContinuationLedger(self.path, KEY)

    def test_malformed_output_rejected_before_commit(self):
        r1 = self.reserve('q1', [user('one')])
        for bad in (None, [None], 'not-a-list',
                    [{'type': 'message', 'content': None}],
                    [{'type': 'message',
                      'content': [{'type': 'output_text'}]}],
                    [{'type': 'function_call', 'call_id': '',
                      'name': 'n', 'arguments': '{}'}],
                    [{'type': 'function_call', 'name': 'n'}],
                    [{'type': 'other'}]):
            with self.assertRaises(ContinuationError):
                self.led.commit(r1, bad, b'wire1')
        self.led.commit(r1, OUTPUT1, b'wire1')

    def test_caller_body_mutation_cannot_corrupt_reservation(self):
        original = body([user('one')])
        r1 = self.led.reserve(self.b, 'q1', original)
        original['input'].append(user('injected'))
        original['model'] = 'changed'
        self.led.commit(r1, OUTPUT1, b'wire1')
        stored = json.loads(self.led._open(
            self.b.scope(), 'q1',
            _ldbw(self.led, 
                "SELECT sealed FROM continuation_turns WHERE revision=1")
            .fetchone()[0]))
        self.assertEqual(stored['input']['input'], [user('one')])
        self.assertEqual(stored['input']['model'], 'astra')

    def test_head_mismatch_rejects(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        _ldbw(self.led, 
            "DELETE FROM continuation_turns WHERE revision=1")
        with self.assertRaises(ContinuationError):
            self.reserve('q2', [user('one'), assistant('answer1'),
                                user('two')])

    def test_missing_keycheck_refuses_existing_turns(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        _ldbw(self.led, 
            "DELETE FROM operations WHERE scope='key-check'")
        self.led.close()
        with self.assertRaises(ContinuationError):
            ContinuationLedger(self.path, KEY)
        # refusal is permanent: turns without a key reference can never
        # be adopted by a fresh key-check

    def test_key_check_row_not_in_scope(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        rows = _ldba(self.led, 
            "SELECT scope FROM continuation_turns")
        self.assertNotIn('key-check', {r[0] for r in rows})
        # key-check operation row does not block reservations
        self.assertIsNotNone(
            self.reserve('q2', [user('one'), assistant('answer1'),
                                user('two')]))


class TestDelivery(LedgerCase):
    def setUp(self):
        super().setUp()
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')

    def test_delivery_id_stable_and_ack_clears_pending(self):
        scope = self.b.scope()
        d1 = self.led.delivery(self.b, 'q1', 'cli')
        d2 = self.led.delivery(self.b, 'q1', 'cli')
        self.assertEqual(d1['delivery_id'], d2['delivery_id'])
        self.assertEqual(d1['result'], b'wire1')
        self.assertEqual(d1['status'], 'succeeded')
        pending = self.led.pending_deliveries(scope, 'cli')
        self.assertEqual(len(pending), 1)  # offer does not implicitly ack
        self.assertTrue(self.led.acknowledge(scope, 'cli',
                                             d1['delivery_id']))
        self.assertTrue(self.led.acknowledge(scope, 'cli',
                                             d1['delivery_id']))
        self.assertEqual(self.led.pending_deliveries(scope, 'cli'), [])
        self.assertFalse(self.led.acknowledge(scope, 'cli', 'unknown'))

    def test_delivery_requires_terminal(self):
        self.reserve('q9', [user('one'), assistant('answer1'),
                            user('two')])
        from fusion_relay import translate
        with self.assertRaises(translate.IncompleteResponse):
            self.led.delivery(self.b, 'q9', 'cli')


class TestReload(LedgerCase):
    def test_separate_process_recovery(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        self.led.close()
        script = (
            "import sys,json\n"
            "sys.path.insert(0,%r)\n"
            "from fusion_relay.continuation import (ContinuationLedger,"
            "ContinuationBinding)\n"
            "key=bytes.fromhex(sys.stdin.read().strip())\n"
            "b=ContinuationBinding(account='a',session='s',lane='lead',"
            "profile='astra-high',epoch='e1')\n"
            "import pathlib\n"
            "led=ContinuationLedger(pathlib.Path(sys.argv[1]),key)\n"
            "r=led.reserve(b,'q2',{'input':[{'role':'user','content':'one'},"
            "{'role':'assistant','content':'answer1'},"
            "{'role':'user','content':'two'}],'instructions':'i',"
            "'model':'astra','reasoning':{'effort':'high'}})\n"
            "print(json.dumps(r['body']['input']))\n" % str(REPO))
        proc = subprocess.run(
            [sys.executable, '-c', script, str(self.path)],
            input=KEY.hex(), capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        merged = json.loads(proc.stdout)
        self.assertEqual(merged, [user('one')] + OUTPUT1 + [user('two')])
        self.led = ContinuationLedger(self.path, KEY)


class TestCoordinator(LedgerCase):
    def setUp(self):
        super().setUp()
        from fusion_relay.continuation_host import ContinuationCoordinator
        self.Coordinator = ContinuationCoordinator
        self.packet = {16: [b's']}
        self.credentials = ('tok', 'a')
        self.body = body([user('one')])
        # coordinator checks profile as '<model>:<effort>'
        self.cb = binding(profile='astra:high')

    def coordinator(self, result):
        return self.Coordinator(self.led, lambda p, c: result)

    def test_resolver_failures_reject_before_invoke(self):
        calls = []

        def invoke(b, out, serialized):
            calls.append(1)
            return b'w'

        for result in (None, 'not-binding',
                       binding(account='wrong', profile='astra:high'),
                       binding(session='other', profile='astra:high'),
                       binding(profile='astra:low')):
            coord = self.coordinator(result)
            with self.assertRaises(ContinuationError):
                coord.prepare(self.packet, self.body, self.credentials)
        self.assertEqual(calls, [])

    def test_prepare_reserves_and_execute_commits(self):
        coord = self.coordinator(self.cb)
        b, res = coord.prepare(self.packet, self.body, self.credentials)
        self.assertIs(b, self.cb)
        self.assertIsNone(res['replay'])
        expected_op = digest({'scope': self.cb.scope(),
                              'native_body': self.body})
        self.assertEqual(res['operation_id'], expected_op)
        seen = []

        def invoke(native_body, output, serialized):
            seen.append(native_body)
            output.extend(OUTPUT1)
            return b'wire1'

        wire_bytes = coord.execute(self.cb, res, invoke)
        self.assertEqual(wire_bytes, b'wire1')
        self.assertEqual(seen, [res['body']])
        # replay path: no invoke
        b2, res2 = coord.prepare(self.packet, self.body, self.credentials)
        self.assertEqual(res2['replay'], b'wire1')
        self.assertEqual(coord.execute(self.cb, res2, invoke), b'wire1')
        self.assertEqual(len(seen), 1)

    def test_lead_and_sidekick_scopes_distinct(self):
        sidekick = binding(lane='sidekick')
        self.assertNotEqual(self.b.scope(), sidekick.scope())

    def test_execute_rejects_mismatched_binding_before_invoke(self):
        coord = self.coordinator(self.cb)
        b, res = coord.prepare(self.packet, self.body, self.credentials)
        calls = []
        for wrong in (None, 'not-binding',
                      binding(account='other', profile='astra:high'),
                      binding(lane='sidekick', profile='astra:high'),
                      binding(profile='astra:low')):
            with self.assertRaises(ContinuationError):
                coord.execute(wrong, res,
                              lambda body, out, s: calls.append(1) or b'w')
        self.assertEqual(calls, [])
        # replay path validates binding before returning wire
        coord.execute(self.cb, res,
                      lambda body, out, s: out.extend(OUTPUT1) or b'wire1')
        b2, res2 = coord.prepare(self.packet, self.body, self.credentials)
        self.assertEqual(res2['replay'], b'wire1')
        with self.assertRaises(ContinuationError):
            coord.execute(binding(account='other', profile='astra:high'),
                          res2, lambda body, out, s: b'x')

    def test_prepare_rejects_malformed_packet_and_body(self):
        coord = self.coordinator(self.cb)
        for pkt in ({16: []}, {16: [b'a', b'b']}, {16: [b'']},
                    {16: ['notbytes']}, {}, 'not-dict'):
            with self.assertRaises(ContinuationError):
                coord.prepare(pkt, self.body, self.credentials)
        for creds in (None, ('tok',), ('tok', ''), ('tok', 1),
                      ['tok', 'a']):
            with self.assertRaises(ContinuationError):
                coord.prepare(self.packet, self.body, creds)
        for bad in ({}, {'model': 'm'}, {'model': '', 'reasoning': {}},
                    dict(self.body, reasoning={}),
                    dict(self.body, reasoning={'effort': ''})):
            with self.assertRaises(ContinuationError):
                coord.prepare(self.packet, bad, self.credentials)

    def test_commit_failure_propagates_no_wire(self):
        coord = self.coordinator(self.cb)
        b, res = coord.prepare(self.packet, self.body, self.credentials)
        with mock.patch.object(self.led, 'commit',
                               side_effect=ContinuationError('commit fail')):
            with self.assertRaises(ContinuationError):
                coord.execute(self.cb, res,
                              lambda body, out, s: out.extend(OUTPUT1)
                              or b'wire1')
        # the provider may have run; the outcome is unproven and the
        # lane fails closed until reconciled
        row = _ldb1(self.led, 
            "SELECT status FROM operations WHERE operation_id=?",
            (res['operation_id'],))
        self.assertEqual(row[0], 'provider_outcome_unknown')
        with self.assertRaisesRegex(ContinuationError,
                                    'outcome unknown'):
            coord.prepare(self.packet, self.body, self.credentials)


class TestSingleOwner(LedgerCase):
    def test_second_ledger_same_process_refused(self):
        with self.assertRaisesRegex(
                ContinuationError,
                'continuation store owned by another process'):
            ContinuationLedger(self.path, KEY)
        # the first ledger is unaffected
        self.reserve('q1', [user('one')])

    def test_subprocess_refused_while_open(self):
        script = (
            "import sys,pathlib\n"
            "sys.path.insert(0,%r)\n"
            "from fusion_relay.continuation import (ContinuationLedger,"
            "ContinuationError)\n"
            "try:\n"
            "    ContinuationLedger(pathlib.Path(sys.argv[1]),b'k'*32)\n"
            "except ContinuationError as e:\n"
            "    print(str(e))\n" % str(REPO))
        proc = subprocess.run(
            [sys.executable, '-c', script, str(self.path)],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('continuation store owned by another process',
                      proc.stdout)
        # after the first closes, ownership is released
        self.led.close()
        self.led = ContinuationLedger(self.path, KEY)


class TestUnresolvedStates(LedgerCase):
    def test_outcome_unknown_blocks_lane_and_same_op(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.mark_outcome_unknown(r1)
        with self.assertRaisesRegex(ContinuationError,
                                    'explicit reconciliation required'):
            self.reserve('q1', [user('one')])
        with self.assertRaisesRegex(ContinuationError,
                                    'unresolved operation'):
            self.reserve('q2', [user('one')])
        # operator reconciliation settles it as failed
        self.led.resolve_unknown(self.b.scope(), 'q1', 'a' * 64,
                                 'abandoned')
        row = _ldb1(self.led, 
            "SELECT status FROM operations WHERE operation_id='q1'"
            )
        self.assertEqual(row[0], 'failed')
        self.assertIsNotNone(self.reserve('q2', [user('one')]))

    def test_cancel_unconfirmed(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.mark_cancel(r1)
        with self.assertRaisesRegex(ContinuationError,
                                    'explicit reconciliation required'):
            self.reserve('q1', [user('one')])

    def test_resolve_unknown_guards(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.mark_outcome_unknown(r1)
        with self.assertRaises(ContinuationError):
            self.led.resolve_unknown(self.b.scope(), 'q1', 'x',
                                     'abandoned')
        with self.assertRaises(ContinuationError):
            self.led.resolve_unknown(self.b.scope(), 'q1', 'a' * 64,
                                     'succeeded')
        self.led.resolve_unknown(self.b.scope(), 'q1', 'a' * 64,
                                 'abandoned')
        with self.assertRaises(ContinuationError):
            self.led.resolve_unknown(self.b.scope(), 'q1', 'a' * 64,
                                     'abandoned')

    def test_crash_before_inference_leaves_executing(self):
        r1 = self.reserve('q1', [user('one')])
        # process "crashes": store closed without commit
        self.led.close()
        self.led = ContinuationLedger(self.path, KEY)
        row = _ldb1(self.led, 
            "SELECT status FROM operations WHERE operation_id='q1'"
            )
        self.assertEqual(row[0], 'executing')
        with self.assertRaisesRegex(ContinuationError,
                                    'continuation outcome unknown'):
            self.reserve('q1', [user('one')])


class TestAckAndAcceptance(LedgerCase):
    def _committed(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        self.led.offer_result(r1, self.b)
        turn = _ldb1(self.led, 
            'SELECT revision, response_digest FROM continuation_turns '
            "WHERE operation_id='q1'")
        return r1, turn

    def test_offer_sets_offered_state(self):
        r1, turn = self._committed()
        state = _ldb1(self.led, 
            "SELECT state FROM continuation_turns WHERE operation_id="
            "'q1'")[0]
        self.assertEqual(state, 'offered')
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'pending')

    def test_acknowledge_result(self):
        r1, turn = self._committed()
        self.assertFalse(self.led.acknowledge_result(
            self.b, 'q1', turn[0], turn[1], 'ref-1'))
        self.assertTrue(self.led.acknowledge_result(
            self.b, 'q1', turn[0], turn[1], 'ref-1'))
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'acknowledged')
        state = _ldb1(self.led, 
            "SELECT state FROM continuation_turns WHERE operation_id="
            "'q1'")[0]
        self.assertEqual(state, 'consumer_acknowledged')
        # delivery row marked acknowledged
        self.assertEqual(_ldb1(self.led, 
            'SELECT acknowledged FROM delivery')[0], 1)

    def test_acknowledge_conflicts(self):
        r1, turn = self._committed()
        with self.assertRaisesRegex(ContinuationError,
                                    'does not match committed result'):
            self.led.acknowledge_result(self.b, 'q1', turn[0],
                                        '0' * 64, 'ref-1')
        with self.assertRaisesRegex(ContinuationError,
                                    'does not match committed result'):
            self.led.acknowledge_result(self.b, 'q1', 99, turn[1],
                                        'ref-1')
        self.assertFalse(self.led.acknowledge_result(
            self.b, 'q1', turn[0], turn[1], 'ref-1'))
        with self.assertRaisesRegex(ContinuationError,
                                    'conflicting acknowledgement'):
            self.led.acknowledge_result(self.b, 'q1', turn[0], turn[1],
                                        'ref-2')
        with self.assertRaisesRegex(ContinuationError,
                                    'does not match committed result'):
            self.led.acknowledge_result(self.b, 'never', 1, '0' * 64,
                                        'ref-1')

    def test_history_evidenced_on_matching_extension(self):
        r1, turn = self._committed()
        history = [user('one'), assistant('answer1'), user('two')]
        r2 = self.reserve('q2', history)
        self.assertEqual(r2['evidenced'], 1)
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'history_evidenced')
        state = _ldb1(self.led, "SELECT state FROM continuation_turns "
                      "WHERE operation_id='q1'")[0]
        self.assertEqual(state, 'history_evidenced')
        # an ack after evidence is still accepted and wins
        self.assertFalse(self.led.acknowledge_result(
            self.b, 'q1', turn[0], turn[1], 'ref-1'))
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'acknowledged')
        # a later offer of q1 must not downgrade the state
        self.led.offer_result(r1, self.b)
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'acknowledged')
        # the new turn itself is merely reserved: evidence is about q1
        self.led.commit(r2, OUTPUT2, b'wire2')
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q2'), 'pending')

    def test_history_mismatch_leaves_state_untouched(self):
        r1, turn = self._committed()
        with self.assertRaises(ContinuationError):
            self.reserve('q2', [user('one'), assistant('edited'),
                                user('two')])
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'pending')
        self.assertEqual(_ldb1(self.led, "SELECT COUNT(*) FROM operations "
                               "WHERE operation_id='q2'")[0], 0)

    def test_acceptance_persists_across_reopen(self):
        r1, turn = self._committed()
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'pending')
        self.led.close()
        self.led = ContinuationLedger(self.path, KEY)
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'pending')
        self.led.acknowledge_result(self.b, 'q1', turn[0], turn[1],
                                    'ref-1')
        self.led.close()
        self.led = ContinuationLedger(self.path, KEY)
        self.assertEqual(
            self.led.acceptance_state(self.b, 'q1'), 'acknowledged')


class TestEpochTransitions(LedgerCase):
    def _commit(self, b, op, turns, output, wire_bytes):
        r = self.led.reserve(b, op, body(turns))
        self.led.commit(r, output, wire_bytes)
        return r

    def test_transition_epoch(self):
        self._commit(self.b, 'q1', [user('one')], OUTPUT1, b'w1')
        new = self.led.transition_epoch(self.b, 'e2', 'history_compaction')
        self.assertEqual(new.epoch, 'e2')
        self.assertNotEqual(new.scope(), self.b.scope())
        row = _ldb1(self.led, 
            'SELECT reason, prior_head FROM epoch_transitions'
            )
        self.assertEqual((row[0], row[1]), ('history_compaction', 1))
        # old scope's turns preserved; new scope starts at revision 0
        self.assertEqual(_ldb1(self.led, 
            'SELECT COUNT(*) FROM continuation_turns WHERE scope=?',
            (self.b.scope(),))[0], 1)
        r = self.led.reserve(new, 'q2', body(
            [user('one'), assistant('answer1'), user('two')]))
        self.assertEqual(r['revision'], 0)

    def test_transition_epoch_guards(self):
        with self.assertRaises(ContinuationError):
            self.led.transition_epoch(self.b, 'e2', 'bogus_reason')
        # 'history_divergence' is accepted — the client history no
        # longer extends the anchors and the cause is not established.
        new = self.led.transition_epoch(self.b, 'e2',
                                        'history_divergence')
        self.assertEqual(new.epoch, 'e2')
        self.assertEqual(_ldb1(
            self.led, 'SELECT reason FROM epoch_transitions')[0],
            'history_divergence')
        with self.assertRaises(ContinuationError):
            self.led.transition_epoch(self.b, 'e1', 'fork')
        r1 = self.reserve('q1', [user('one')])
        with self.assertRaisesRegex(ContinuationError,
                                    'blocked by unresolved'):
            self.led.transition_epoch(self.b, 'e2', 'fork')
        self.led.mark_outcome_unknown(r1)
        with self.assertRaisesRegex(ContinuationError,
                                    'blocked by unresolved'):
            self.led.transition_epoch(self.b, 'e2', 'fork')

    def test_find_carry_candidate(self):
        self._commit(self.b, 'q1', [user('one')], OUTPUT1, b'w1')
        self._commit(self.b, 'q2', [user('one'), assistant('answer1'),
                                    user('two')], OUTPUT2, b'w2')
        resumed = binding(session='s-resumed', epoch='launcher-genesis')
        history = [user('one'), assistant('answer1'), user('two'),
                   assistant('answer2'), user('three')]
        self.assertEqual(self.led.find_carry_candidate(resumed, history),
                         self.b)
        # non-matching history, or a different profile -> None
        self.assertIsNone(self.led.find_carry_candidate(
            resumed, [user('one'), assistant('edited'), user('two')]))
        self.assertIsNone(self.led.find_carry_candidate(
            binding(session='s-resumed', profile='astra-low'), history))
        # a candidate with an unresolved operation is never carried
        r3 = self.led.reserve(self.b, 'q3', body(history))
        self.led.mark_outcome_unknown(r3)
        self.assertIsNone(self.led.find_carry_candidate(resumed, history))

    def test_transition_epoch_carry_over(self):
        self._commit(self.b, 'q1', [user('one')], OUTPUT1, b'w1')
        self._commit(self.b, 'q2', [user('one'), assistant('answer1'),
                                    user('two')], OUTPUT2, b'w2')
        resumed = binding(session='s-resumed', epoch='launcher-genesis')
        new = self.led.transition_epoch(resumed, 'e-carried',
                                        'legacy_history', carry_from=self.b)
        # two turns copied, re-sealed under the new scope, head = 2
        self.assertEqual(_ldb1(self.led,
            'SELECT revision FROM continuation_heads WHERE scope=?',
            (new.scope(),))[0], 2)
        turns = self.led._turns(new.scope())
        self.assertEqual([op for op, _ in turns], ['q1', 'q2'])
        self.assertEqual(turns[1][1]['output'], OUTPUT2)
        row = _ldb1(self.led, 'SELECT carried_from_scope, reason FROM '
                    'epoch_transitions')
        self.assertEqual(row, (self.b.scope(), 'legacy_history'))
        # old scope untouched; operations not copied (fresh reservation)
        self.assertEqual(_ldb1(self.led,
            'SELECT COUNT(*) FROM continuation_turns WHERE scope=?',
            (self.b.scope(),))[0], 2)
        self.assertEqual(_ldb1(self.led,
            'SELECT COUNT(*) FROM operations WHERE scope=?',
            (new.scope(),))[0], 0)
        # the resumed history now extends the carried anchors and the
        # stored reasoning is merged back in
        r = self.led.reserve(new, 'q3', body(
            [user('one'), assistant('answer1'), user('two'),
             assistant('answer2'), user('three')]))
        self.assertEqual(r['revision'], 2)
        self.assertIn(OUTPUT1[0], r['body']['input'])
        self.assertIn(OUTPUT2[0], r['body']['input'])
        with self.assertRaisesRegex(ContinuationError, 'mismatch'):
            self.led.transition_epoch(
                binding(session='x', profile='astra-low'), 'e9',
                'legacy_history', carry_from=self.b)

    def test_resolve_epoch_chain(self):
        # no transitions -> same binding
        self.assertIs(self.led.resolve_epoch(self.b), self.b)
        b2 = self.led.transition_epoch(self.b, 'e2', 'fork')
        self.assertEqual(self.led.resolve_epoch(self.b), b2)
        b3 = self.led.transition_epoch(b2, 'e3', 'model_switch')
        self.assertEqual(self.led.resolve_epoch(self.b), b3)
        self.assertEqual(self.led.resolve_epoch(b2), b3)
        chain = self.led.epoch_chain(self.b)
        self.assertEqual([x.epoch for x in chain], ['e1', 'e2', 'e3'])

    def test_resolve_epoch_cycle_rejected(self):
        self.led.transition_epoch(self.b, 'e2', 'fork')
        scope1, scope2 = self.b.scope(), binding(epoch='e2').scope()
        _ldbw(self.led, 
            'INSERT INTO epoch_transitions'
            '(from_scope,to_scope,to_epoch,reason,prior_head,created_at)'
            ' VALUES(?,?,?,?,?,?)',
            (scope2, scope1, 'e1', 'operator_recovery', 0, 0.0))
        with self.assertRaisesRegex(ContinuationError,
                                    'epoch chain invalid'):
            self.led.resolve_epoch(self.b)

    def test_retire_scope(self):
        r1 = self.reserve('q1', [user('one')])
        self.led.commit(r1, OUTPUT1, b'wire1')
        self.led.offer_result(r1, self.b)
        with self.assertRaisesRegex(ContinuationError,
                                    'pending or uncertain'):
            self.led.retire_scope(self.b.scope())
        turn = _ldb1(self.led, 
            'SELECT revision, response_digest FROM continuation_turns'
            )
        self.led.acknowledge_result(self.b, 'q1', turn[0], turn[1],
                                    'ref-1')
        self.led.retire_scope(self.b.scope())
        self.assertEqual(_ldb1(self.led, 
            'SELECT COUNT(*) FROM continuation_turns WHERE scope=?',
            (self.b.scope(),))[0], 0)
        # retired scope may be reused fresh
        self.assertIsNotNone(self.reserve('q9', [user('x')]))


class TestSchemaMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = (pathlib.Path(self.tmp.name).resolve()
                     / 'continuation.sqlite3')

    def _keycheck(self, db):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from fusion_relay.continuation import canonical
        nonce = b'0' * 12
        sealed = base64.b64encode(nonce + AESGCM(KEY).encrypt(
            nonce, b'continuation-v1',
            canonical(['key-check', 'key-check']))).decode()
        db.execute("INSERT INTO operations VALUES"
                   "('key-check','key-check','v1','succeeded',?)",
                   (sealed,))

    def _v1_store(self):
        db = sqlite3.connect(str(self.path))
        db.executescript('''
            CREATE TABLE operations(
                scope TEXT NOT NULL, operation_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, status TEXT NOT NULL,
                result TEXT, PRIMARY KEY(scope, operation_id));
            CREATE TABLE delivery(
                scope TEXT NOT NULL, operation_id TEXT NOT NULL,
                consumer TEXT NOT NULL, delivery_id TEXT NOT NULL UNIQUE,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(scope, operation_id, consumer));
            CREATE TABLE continuation_heads(
                scope TEXT PRIMARY KEY, revision INTEGER NOT NULL);
            CREATE TABLE continuation_turns(
                scope TEXT NOT NULL, revision INTEGER NOT NULL,
                operation_id TEXT NOT NULL, input_digest TEXT NOT NULL,
                output_digest TEXT NOT NULL, sealed BLOB NOT NULL,
                PRIMARY KEY(scope, revision),
                UNIQUE(scope, operation_id));''')
        self._keycheck(db)
        db.commit()
        db.close()

    def test_v1_migrates_to_v2(self):
        self._v1_store()
        led = ContinuationLedger(self.path, KEY)
        self.addCleanup(led.close)
        version = _ldb1(led, 
            'SELECT version FROM schema_version')[0]
        self.assertEqual(version, 2)
        for table in ('acknowledgements', 'epoch_transitions'):
            self.assertIsNotNone(_ldb1(led, 
                "SELECT 1 FROM sqlite_master WHERE name=?", (table,)
            ))
        cols = {r[1] for r in _ldbw(led, 
            'PRAGMA table_info(continuation_turns)')}
        self.assertIn('response_digest', cols)
        self.assertIn('state', cols)
        epoch_cols = {r[1] for r in _ldbw(led, 
            'PRAGMA table_info(epoch_transitions)')}
        self.assertIn('to_epoch', epoch_cols)
        # key-check row still validates: existing data readable
        led.reserve(binding(), 'q1', body([user('one')]))

    def test_incomplete_and_newer_refused(self):
        db = sqlite3.connect(str(self.path))
        db.execute('CREATE TABLE schema_version(version INTEGER NOT NULL)')
        db.execute('INSERT INTO schema_version VALUES(-2)')
        db.commit()
        db.close()
        with self.assertRaisesRegex(ContinuationError,
                                    'migration incomplete'):
            ContinuationLedger(self.path, KEY)
        db = sqlite3.connect(str(self.path))
        db.execute('UPDATE schema_version SET version=3')
        db.commit()
        db.close()
        with self.assertRaisesRegex(ContinuationError,
                                    'newer than supported'):
            ContinuationLedger(self.path, KEY)


if __name__ == '__main__':
    unittest.main()
