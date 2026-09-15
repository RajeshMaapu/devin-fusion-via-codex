"""Session diagnostics: whitelist-only, hashed refs, bounded LRU."""
import http.client
import json
import pathlib
import sys
import threading
import unittest
from unittest.mock import patch

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fusion_relay import diagnostics, relay

REF = 'a' * 64


class QuietServer(relay._BoundedServer):
    def handle_error(self, request, client_address):
        err = __import__("sys").exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError,
                            http.client.IncompleteRead)):
            return
        raise err


class TestRecord(unittest.TestCase):
    def setUp(self):
        diagnostics._sessions.clear()
        self.addCleanup(diagnostics._sessions.clear)

    def rec(self, **kw):
        return kw

    def test_known_model_and_fields(self):
        diagnostics.record('gpt-6-astra-high', REF, 'codex',
                           self.rec(continuation_status='durable_host_bound',
                                    continuation_role='lead',
                                    continuation_revision=2,
                                    continuation_epoch_ref='b' * 64))
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['model'], 'gpt-6-astra-high')
        self.assertEqual(entry['route'], 'codex')
        self.assertEqual(entry['role'], 'lead')
        self.assertEqual(entry['continuation'], 'durable')
        self.assertEqual(entry['continuation_revision'], 2)
        self.assertEqual(entry['continuation_epoch_ref'], 'b' * 64)
        self.assertEqual(entry['computer_provider'], 'disabled')
        self.assertEqual(entry['native_ack_contract'], 'unavailable')
        self.assertEqual(entry['protocol_version'], 1)

    def test_untrusted_metadata_never_stored(self):
        diagnostics.record('arbitrary-model-name', REF, 'weird-route',
                           self.rec(continuation_status='made_up',
                                    continuation_role='superadmin',
                                    continuation_revision=-1,
                                    continuation_epoch_ref='raw-session!'))
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['model'], 'unverified')
        self.assertEqual(entry['route'], 'unverified')
        self.assertEqual(entry['role'], 'unverified')
        self.assertEqual(entry['continuation'], 'blocked')
        self.assertEqual(entry['continuation_detail'], 'made_up')
        self.assertIsNone(entry['continuation_revision'])
        self.assertIsNone(entry['continuation_epoch_ref'])

    def test_native_standalone_not_falsely_lead(self):
        diagnostics.record('swe-2-medium', REF, 'cognition-forward',
                           self.rec())
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['role'], 'unverified')
        self.assertEqual(entry['route'], 'cognition-forward')

    def test_unhashable_and_unknown_values_no_leak(self):
        # unhashable route/role/continuation inputs must not raise or leak
        diagnostics.record('m', REF, ['codex'],
                           self.rec(continuation_status=['durable'],
                                    continuation_role={'x': 1},
                                    codex_status='arbitrary-str'))
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['route'], 'unverified')
        self.assertEqual(entry['role'], 'unverified')
        self.assertEqual(entry['continuation'], 'legacy_memory_only')
        self.assertEqual(entry['last_outcome'], 'unknown')

    def test_termination_confirmed_field(self):
        diagnostics.record('m', REF, 'codex',
                           self.rec(termination_confirmed=False))
        self.assertIs(
            diagnostics.snapshot(REF)['termination_confirmed'], False)
        diagnostics.record('m', REF, 'codex',
                           self.rec(termination_confirmed='yes'))
        self.assertIsNone(
            diagnostics.snapshot(REF)['termination_confirmed'])

    def test_error_and_cancellation_outcomes(self):
        diagnostics.record('m', REF, 'codex',
                           self.rec(error_category='internal'))
        self.assertEqual(diagnostics.snapshot(REF)['last_outcome'],
                         'error')
        diagnostics.record('m', REF, 'codex',
                           self.rec(client_gone=True))
        # a closed socket is the only signal; the provider never
        # confirms cancellation, so 'confirmed' is never reported
        self.assertEqual(diagnostics.snapshot(REF)['cancellation'],
                         'uncertain')

    def test_invalid_session_ref_rejected(self):
        for ref in ('short', 'g' * 64, '', None, REF + 'x'):
            diagnostics.record('m', ref, 'codex', self.rec())
            self.assertIsNone(diagnostics.snapshot(ref))
        self.assertEqual(len(diagnostics._sessions), 0)

    def test_lru_bounded(self):
        for i in range(80):
            diagnostics.record('swe-2', f'{i:064x}', 'reject', self.rec())
        self.assertLessEqual(len(diagnostics._sessions), 64)
        self.assertIsNone(diagnostics.snapshot(f'{0:064x}'))
        self.assertIsNotNone(diagnostics.snapshot(f'{79:064x}'))

    def test_new_field_defaults(self):
        diagnostics.record('gpt-6-astra', REF, 'codex', self.rec())
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['binding'], 'unavailable')
        self.assertEqual(entry['acceptance'], 'uncertain')
        self.assertEqual(entry['key_store'], 'unavailable')
        self.assertEqual(entry['cancellation'], 'not_observed')
        self.assertIsNone(entry['epoch_transition'])
        self.assertEqual(entry['qualification'], 'local_tests')
        self.assertEqual(entry['compaction_correlation'], 'unverified')
        # the invariant: no 'confirmed' cancellation exists
        self.assertNotEqual(entry['cancellation'], 'confirmed')

    def test_binding_acceptance_cancellation_fields(self):
        diagnostics.record('gpt-6-astra', REF, 'codex', self.rec(
            binding_status='verified', acceptance='acknowledged',
            key_store_status='ready', cancellation='requested',
            epoch_transition='history_compaction'))
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['binding'], 'verified')
        self.assertEqual(entry['acceptance'], 'acknowledged')
        self.assertEqual(entry['key_store'], 'ready')
        self.assertEqual(entry['cancellation'], 'requested')
        self.assertEqual(entry['epoch_transition'],
                         'history_compaction')
        # unverified values are dropped to safe defaults
        diagnostics.record('gpt-6-astra', REF, 'codex', self.rec(
            binding_status='root', acceptance='confirmed',
            key_store_status='plaintext', cancellation='confirmed',
            epoch_transition='rm_rf'))
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['binding'], 'unavailable')
        self.assertEqual(entry['acceptance'], 'uncertain')
        self.assertEqual(entry['key_store'], 'unavailable')
        self.assertNotEqual(entry['cancellation'], 'confirmed')
        self.assertIsNone(entry['epoch_transition'])

    def test_record_compaction(self):
        diagnostics.record_compaction(REF)
        diagnostics.record_compaction(REF)
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['compaction_events'], 2)
        self.assertEqual(entry['compaction_correlation'], 'unverified')
        # a later request record preserves the compaction counters
        diagnostics.record('gpt-6-astra', REF, 'codex', self.rec())
        entry = diagnostics.snapshot(REF)
        self.assertEqual(entry['compaction_events'], 2)
        diagnostics.record_compaction('not-hex')
        self.assertIsNone(diagnostics.snapshot('not-hex'))


class TestEndpoint(unittest.TestCase):
    TOKEN = 'diag-token-0000'

    def setUp(self):
        diagnostics._sessions.clear()
        self.addCleanup(diagnostics._sessions.clear)
        p = patch.object(relay, '_RELAY_TOKEN', self.TOKEN)
        self.addCleanup(p.stop)
        p.start()
        self.server = QuietServer(('127.0.0.1', 0), relay.Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={'poll_interval': 0.05})
        self._thread.start()
        self.addCleanup(
            lambda: self.assertFalse(self._thread.is_alive()))
        self.addCleanup(self._thread.join, 10)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def _get(self, path):
        conn = http.client.HTTPConnection('127.0.0.1', self.port,
                                          timeout=15)
        conn.request('GET', path)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        conn.close()
        return status, body

    def test_snapshot_and_accounting(self):
        diagnostics.record('gpt-6-astra-high', REF, 'codex', {})
        status, body = self._get(
            f'/t/{self.TOKEN}/diagnostics?session={REF}')
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload['session']['session_ref'], REF)
        self.assertIn('accounting', payload)

    def test_unknown_session_404(self):
        status, _ = self._get(
            f'/t/{self.TOKEN}/diagnostics?session={"f" * 64}')
        self.assertEqual(status, 404)

    def test_invalid_query_no_disclosure(self):
        diagnostics.record('gpt-6-astra', REF, 'codex', {})
        for q in ('session=', 'session=short', 'session=' + REF.upper(),
                  'other=' + REF):
            status, body = self._get(
                f'/t/{self.TOKEN}/diagnostics?{q}')
            self.assertEqual(status, 404)
            self.assertNotIn(REF.encode(), body)

    def test_strict_query_rejects_extra_duplicate_fragment(self):
        diagnostics.record('gpt-6-astra', REF, 'codex', {})
        for q in (f'session={REF}&extra=1',
                  f'session={REF}&session={REF}',
                  f'extra=1&session={REF}'):
            status, body = self._get(
                f'/t/{self.TOKEN}/diagnostics?{q}')
            self.assertEqual(status, 404)
            self.assertNotIn(REF.encode(), body)
        status, body = self._get(
            f'/t/{self.TOKEN}/diagnostics?session={REF}%23frag')
        # url-encoded # stays inside the value → not a valid ref
        self.assertEqual(status, 404)

    def test_unauthenticated_404(self):
        diagnostics.record('gpt-6-astra', REF, 'codex', {})
        status, body = self._get(f'/diagnostics?session={REF}')
        self.assertEqual(status, 404)
        self.assertNotIn(REF.encode(), body)

    def test_empty_store_unknown(self):
        status, _ = self._get(
            f'/t/{self.TOKEN}/diagnostics?session={REF}')
        self.assertEqual(status, 404)


if __name__ == '__main__':
    unittest.main(verbosity=2)
