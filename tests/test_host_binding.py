"""Trusted host binding: capability issue/verify, header contexts,
and coordinator-level binding enforcement."""
from __future__ import annotations

import pathlib
import secrets
import sys
import time
import tempfile
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fusion_relay.continuation import (ContinuationError,
                                       ContinuationLedger, digest)
from fusion_relay.continuation_host import ContinuationCoordinator
from fusion_relay.host_binding import (
    HEADER_CAPABILITY, HEADER_OPERATION, HEADER_PROOF,
    PROTOCOL_VERSION, PROVENANCE_LAUNCHER, PROVENANCE_NATIVE,
    PROVENANCE_RESOLVER, AuthenticatedContext, BindingError,
    CapabilityStore, HostBinding, context_from_headers)

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

CLIENT = secrets.token_hex(32)


def issue(store, **over):
    args = dict(client_instance_id=CLIENT, account_reference='acct-1',
                native_session_id='s1', lane='lead',
                model_profile='astra:high', continuation_epoch='e1',
                provenance=PROVENANCE_NATIVE, ttl_s=3600)
    args.update(over)
    return store.issue(**args)


def context_for(secret, capability_id, operation_id, body,
                now=None):
    body_digest = digest(body)
    return AuthenticatedContext(
        capability_id=capability_id, operation_id=operation_id,
        body_digest=body_digest,
        proof=CapabilityStore.prove(secret, capability_id,
                                    operation_id, body_digest),
        now=now if now is not None else time.time())


class Headers(dict):
    def get(self, key, default=None):
        for k, v in self.items():
            if k.lower() == key.lower():
                return v
        return default


class TestCapabilityStore(unittest.TestCase):
    def setUp(self):
        self.clock = [1000.0]
        self.store = CapabilityStore(clock=lambda: self.clock[0])

    def test_issue_validates(self):
        for over in ({'lane': 'guest'}, {'lane': ''},
                     {'provenance': 'made_up'},
                     {'account_reference': ''},
                     {'native_session_id': 'x' * 513},
                     {'model_profile': ''}, {'continuation_epoch': ''},
                     {'client_instance_id': 'not-hex'},
                     {'ttl_s': 0}, {'ttl_s': 86401}, {'ttl_s': 'x'}):
            with self.assertRaises(BindingError, msg=over):
                issue(self.store, **over)
        binding, secret = issue(self.store)
        self.assertIsInstance(binding, HostBinding)
        self.assertEqual(binding.protocol_version, PROTOCOL_VERSION)
        self.assertEqual(len(binding.capability_id), 64)
        self.assertEqual(len(secret), 32)
        cb = binding.continuation_binding()
        self.assertEqual((cb.account, cb.session, cb.lane, cb.profile,
                          cb.epoch),
                         ('acct-1', 's1', 'lead', 'astra:high', 'e1'))

    def test_verify_roundtrip_and_errors(self):
        binding, secret = issue(self.store)
        ctx = context_for(secret, binding.capability_id, 'op-1',
                          {'x': 1}, now=self.clock[0])
        self.assertEqual(self.store.verify(ctx).capability_id,
                         binding.capability_id)
        with self.assertRaisesRegex(BindingError, 'unknown capability'):
            self.store.verify(context_for(secret, secrets.token_hex(32),
                                          'op-1', {'x': 1},
                                          now=self.clock[0]))
        # a wrong secret or tampered proof fails
        ctx_bad = context_for(secrets.token_bytes(32),
                              binding.capability_id, 'op-1', {'x': 1},
                              now=self.clock[0])
        with self.assertRaisesRegex(BindingError,
                                    'capability proof invalid'):
            self.store.verify(ctx_bad)
        good = context_for(secret, binding.capability_id, 'op-1',
                           {'x': 1}, now=self.clock[0])
        tampered = AuthenticatedContext(
            good.capability_id, good.operation_id, good.body_digest,
            '0' * 64, good.now)
        with self.assertRaisesRegex(BindingError,
                                    'capability proof invalid'):
            self.store.verify(tampered)
        # expiry
        self.clock[0] += 3601
        with self.assertRaisesRegex(BindingError, 'capability expired'):
            self.store.verify(context_for(
                secret, binding.capability_id, 'op-1', {'x': 1},
                now=self.clock[0]))
        # revoked
        binding2, secret2 = issue(self.store)
        self.store.revoke(binding2.capability_id)
        with self.assertRaisesRegex(BindingError, 'capability revoked'):
            self.store.verify(context_for(
                secret2, binding2.capability_id, 'op-1', {'x': 1},
                now=self.clock[0]))
        # generation bump invalidates older capabilities
        binding3, secret3 = issue(self.store)
        self.store.bump_generation(CLIENT)
        with self.assertRaisesRegex(BindingError, 'capability revoked'):
            self.store.verify(context_for(
                secret3, binding3.capability_id, 'op-1', {'x': 1},
                now=self.clock[0]))
        # newly issued capabilities carry the current generation
        binding4, secret4 = issue(self.store)
        self.assertEqual(self.store.verify(context_for(
            secret4, binding4.capability_id, 'op-1', {'x': 1},
            now=self.clock[0])).capability_id, binding4.capability_id)

    def test_error_messages_are_sanitized(self):
        binding, secret = issue(self.store)
        ctx = context_for(secret, binding.capability_id, 'op', {'a': 1},
                          now=self.clock[0])
        bad = AuthenticatedContext(ctx.capability_id, ctx.operation_id,
                                   ctx.body_digest, 'f' * 64, ctx.now)
        try:
            self.store.verify(bad)
        except BindingError as e:
            self.assertNotIn(secret.hex(), str(e))
            self.assertNotIn(ctx.proof, str(e))
            self.assertEqual(str(e), 'capability proof invalid')


class TestContextFromHeaders(unittest.TestCase):
    def test_absent_returns_none(self):
        self.assertIsNone(context_from_headers(Headers(), 'a' * 64, 0))

    def test_partial_or_malformed(self):
        good = {HEADER_CAPABILITY: 'a' * 64,
                HEADER_OPERATION: 'op-1', HEADER_PROOF: 'b' * 64}
        for mutant in (
                {k: v for k, v in good.items() if k != HEADER_OPERATION},
                {k: v for k, v in good.items() if k != HEADER_PROOF},
                dict(good, **{HEADER_CAPABILITY: 'zz'}),
                dict(good, **{HEADER_PROOF: 'zz'}),
                dict(good, **{HEADER_OPERATION: ''}),
                dict(good, **{HEADER_OPERATION: 'x' * 513}),
                dict(good, **{HEADER_OPERATION: 'bad\nid'})):
            with self.assertRaisesRegex(BindingError,
                                        'malformed binding headers'):
                context_from_headers(Headers(mutant), 'a' * 64, 0)
        with self.assertRaisesRegex(BindingError,
                                    'malformed binding headers'):
            context_from_headers(Headers(good), 'not-hex', 0)

    def test_valid(self):
        ctx = context_from_headers(Headers({
            HEADER_CAPABILITY: 'a' * 64, HEADER_OPERATION: 'op-1',
            HEADER_PROOF: 'b' * 64}), 'c' * 64, 12.0)
        self.assertEqual((ctx.capability_id, ctx.operation_id,
                          ctx.body_digest, ctx.proof, ctx.now),
                         ('a' * 64, 'op-1', 'c' * 64, 'b' * 64, 12.0))


class CoordinatorCase(unittest.TestCase):
    """Coordinator in capabilities mode over a real ledger."""

    BODY = {'input': [{'role': 'user', 'content': 'one'}],
            'instructions': 'i', 'model': 'astra',
            'reasoning': {'effort': 'high'}}
    PACKET = {16: [b's1']}
    CREDS = ('tok', 'acct-1')

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = ContinuationLedger(
            pathlib.Path(self.tmp.name).resolve() / 'c.sqlite3', KEY)
        self.addCleanup(self.led.close)
        self.store = CapabilityStore()
        self.coordinator = ContinuationCoordinator(
            self.led, capabilities=self.store,
            accept_provenance=frozenset(
                {PROVENANCE_NATIVE, PROVENANCE_LAUNCHER}))

    def ctx(self, body=None, operation_id='op-1', **issue_over):
        binding, secret = issue(self.store, **issue_over)
        return binding, context_for(
            secret, binding.capability_id, operation_id,
            body if body is not None else self.BODY, now=time.time())


class TestCoordinatorBinding(CoordinatorCase):
    def test_requires_exactly_one_mechanism(self):
        with self.assertRaises(ContinuationError):
            ContinuationCoordinator(self.led)
        with self.assertRaises(ContinuationError):
            ContinuationCoordinator(
                self.led, lambda p, c: None, capabilities=self.store)

    def test_no_context_unavailable(self):
        with self.assertRaisesRegex(
                ContinuationError,
                'trusted continuation binding unavailable'):
            self.coordinator.prepare(self.PACKET, self.BODY, self.CREDS)

    def test_valid_lead_binding(self):
        binding, ctx = self.ctx()
        b, r = self.coordinator.prepare(self.PACKET, self.BODY,
                                        self.CREDS, ctx)
        self.assertEqual(b.epoch, 'e1')
        self.assertEqual(r['operation_id'], 'op-1')

    def test_sidekick_lane_rejected(self):
        _, ctx = self.ctx(lane='sidekick')
        with self.assertRaisesRegex(BindingError,
                                    'operation not authorized for lane'):
            self.coordinator.prepare(self.PACKET, self.BODY,
                                     self.CREDS, ctx)

    def test_cross_session_account_profile(self):
        _, ctx = self.ctx(native_session_id='other-session')
        with self.assertRaisesRegex(BindingError,
                                    'continuation session mismatch'):
            self.coordinator.prepare(self.PACKET, self.BODY,
                                     self.CREDS, ctx)
        _, ctx = self.ctx(account_reference='acct-2')
        with self.assertRaisesRegex(BindingError,
                                    'continuation account mismatch'):
            self.coordinator.prepare(self.PACKET, self.BODY,
                                     self.CREDS, ctx)
        _, ctx = self.ctx(model_profile='astra:low')
        with self.assertRaisesRegex(BindingError,
                                    'continuation profile mismatch'):
            self.coordinator.prepare(self.PACKET, self.BODY,
                                     self.CREDS, ctx)
        # nothing reserved on any rejection
        self.assertEqual(_ldb1(self.led, 
            "SELECT COUNT(*) FROM operations WHERE scope != 'key-check'"
            )[0], 0)

    def test_unaccepted_provenance(self):
        coordinator = ContinuationCoordinator(
            self.led, capabilities=self.store)  # native only
        binding, secret = issue(
            self.store, provenance=PROVENANCE_LAUNCHER)
        ctx = context_for(secret, binding.capability_id, 'op-1',
                          self.BODY, now=time.time())
        with self.assertRaisesRegex(BindingError,
                                    'provenance not accepted'):
            coordinator.prepare(self.PACKET, self.BODY,
                                self.CREDS, ctx)

    def test_body_digest_mismatch(self):
        binding, ctx = self.ctx()
        forged = AuthenticatedContext(
            ctx.capability_id, ctx.operation_id, digest({'other': 1}),
            ctx.proof, ctx.now)
        # the proof no longer covers the presented digest
        with self.assertRaisesRegex(BindingError,
                                    'capability proof invalid'):
            self.coordinator.prepare(self.PACKET, self.BODY,
                                     self.CREDS, forged)


class TestAcknowledge(CoordinatorCase):
    @staticmethod
    def _invoke(body, out, s):
        out.append({'type': 'message', 'id': 'm1',
                    'content': [{'type': 'output_text', 'text': 'ans'}]})
        return b'wire'

    def _commit_turn(self):
        binding, ctx = self.ctx()
        b, r = self.coordinator.prepare(self.PACKET, self.BODY,
                                        self.CREDS, ctx)
        self.coordinator.execute(b, r, self._invoke)
        return binding, ctx

    def _ack_body(self, host, ctx, **over):
        turn = _ldb1(self.led, 
            'SELECT revision, response_digest FROM continuation_turns '
            'WHERE operation_id=?', (ctx.operation_id,))
        ack = {'protocol_version': 1,
               'client_instance_id': host.client_instance_id,
               'session_id': host.native_session_id,
               'lane': host.lane, 'operation_id': ctx.operation_id,
               'continuation_epoch': host.continuation_epoch,
               'revision': turn[0], 'response_digest': turn[1],
               'consumer_commit_reference': 'ref-1'}
        ack.update(over)
        return ack

    def _ack_context(self, host, secret, ack):
        body_digest = digest(ack)
        return AuthenticatedContext(
            host.capability_id, ack['operation_id'], body_digest,
            CapabilityStore.prove(secret, host.capability_id,
                                  ack['operation_id'], body_digest),
            now=time.time())

    def test_ack_roundtrip_and_idempotence(self):
        host, _ = self.ctx()
        b, r = self.coordinator.prepare(self.PACKET, self.BODY,
                                        self.CREDS, _)
        self.coordinator.execute(b, r, self._invoke)
        self.assertEqual(self.coordinator.acceptance(b, r), 'pending')
        # re-issue to recover the secret (ctx helper discarded it)
        # — the store's own secret is required; issue a fresh op
        # instead: simplest is to reuse _ack_context with the real
        # secret, so keep it from issue().
        secret = self.store._caps[host.capability_id]['secret']
        ack = self._ack_body(host, _)
        result = self.coordinator.acknowledge(
            self._ack_context(host, secret, ack), ack)
        self.assertEqual(result, {'acceptance': 'acknowledged',
                                  'idempotent': False})
        self.assertEqual(self.coordinator.acceptance(b, r),
                         'acknowledged')
        again = self.coordinator.acknowledge(
            self._ack_context(host, secret, ack), ack)
        self.assertTrue(again['idempotent'])
        changed = dict(ack, response_digest='0' * 64)
        with self.assertRaises(ContinuationError):
            self.coordinator.acknowledge(
                self._ack_context(host, secret, changed), changed)
        stale = dict(ack, revision=99)
        with self.assertRaises(ContinuationError):
            self.coordinator.acknowledge(
                self._ack_context(host, secret, stale), stale)

    def test_ack_sidekick_lane_rejected(self):
        host, _ = self.ctx()
        b, r = self.coordinator.prepare(self.PACKET, self.BODY,
                                        self.CREDS, _)
        self.coordinator.execute(b, r, self._invoke)
        side, side_secret = issue(self.store, lane='sidekick')
        ack = {'protocol_version': 1,
               'client_instance_id': side.client_instance_id,
               'session_id': side.native_session_id, 'lane': 'sidekick',
               'operation_id': 'op-1', 'continuation_epoch': 'e1',
               'revision': 1, 'response_digest': '0' * 64,
               'consumer_commit_reference': 'ref-1'}
        ctx = self._ack_context(side, side_secret, ack)
        # The sidekick binding scopes differently, so the turn lookup
        # fails — the ack never binds across lanes.
        with self.assertRaises(ContinuationError):
            self.coordinator.acknowledge(ctx, ack)

    def test_ack_requires_authenticated_binding(self):
        legacy = ContinuationCoordinator(
            self.led, lambda packet, creds: None)
        with self.assertRaisesRegex(
                ContinuationError,
                'acknowledgement requires authenticated binding'):
            legacy.acknowledge(None, {})


if __name__ == '__main__':
    unittest.main()
