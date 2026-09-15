"""Host-side continuation coordination.

Callable only — no HTTP binding. Production enablement additionally
requires an authenticated native lane and a verified ACK contract; the
binding resolver must supply a trusted ContinuationBinding, or a
CapabilityStore must verify an authenticated host context.
"""
from __future__ import annotations

import sqlite3
import threading

from .continuation import (ContinuationBinding, ContinuationError,
                           ContinuationLedger, EpochRequired, digest)
from .host_binding import (PROVENANCE_NATIVE, PROVENANCE_RESOLVER,
                           AuthenticatedContext, BindingError,
                           CapabilityStore)
from .payload_budget import BudgetExceeded
from .lifecycle import RequestCancelled

_ACK_KEYS = {'protocol_version', 'client_instance_id', 'session_id',
             'lane', 'operation_id', 'continuation_epoch', 'revision',
             'response_digest', 'consumer_commit_reference'}


class ContinuationCoordinator:
    def __init__(self, store, binding_resolver=None, *, capabilities=None,
                 accept_provenance=frozenset({PROVENANCE_NATIVE}),
                 key_store_status='unavailable'):
        if (binding_resolver is None) == (capabilities is None):
            raise ContinuationError(
                'exactly one binding mechanism required')
        self._store = store
        self._resolver = binding_resolver
        self._capabilities = capabilities
        self._accept_provenance = accept_provenance
        self.key_store_status = key_store_status
        self._notes: dict = {}
        self._notes_lock = threading.Lock()
        self._last_compaction_ts: float = 0.0

    @staticmethod
    def from_keychain(path, account_ref, binding_resolver, create=False,
                      **kwargs):
        from .keychain import MacOSKeychain
        key = MacOSKeychain().load(account_ref, create=create)
        return ContinuationCoordinator(
            ContinuationLedger(path, key), binding_resolver,
            key_store_status='ready', **kwargs)

    @property
    def binding_mode(self):
        return 'capabilities' if self._capabilities is not None \
            else 'resolver'

    def note_compaction(self, session_ref):
        """Record that a native /compact was observed for a session ref.

        The correlation between the hook's session_id and the wire seed
        is unverified; the note is consumed once and only ever authorizes
        a 'history_compaction' epoch transition on the matching scope.
        """
        if not isinstance(session_ref, str) or len(session_ref) != 64:
            return
        import time
        with self._notes_lock:
            self._notes[session_ref] = time.time()
            self._last_compaction_ts = time.time()

    def _take_compaction_note(self, session_ref):
        with self._notes_lock:
            return self._notes.pop(session_ref, None)

    def recent_compaction_notice(self, window_s: float) -> bool:
        """True when ANY /host/compaction note arrived within
        ``window_s`` — evidence-only metadata; correlation between the
        hook's session id and a wire seed is unverified, so a recent
        notice never proves a divergence was a compaction."""
        import time
        with self._notes_lock:
            ts = self._last_compaction_ts
        return bool(ts) and time.time() - ts <= window_s

    def _verified_host(self, packet, body, credentials, context):
        """Verify an authenticated context against packet/body/credentials."""
        if context is None:
            raise ContinuationError(
                'trusted continuation binding unavailable')
        host = self._capabilities.verify(context)
        if host.provenance not in self._accept_provenance:
            raise BindingError(
                'continuation binding provenance not accepted')
        values = packet.get(16)
        session = values[0].decode()
        if host.account_reference != credentials[1]:
            raise BindingError('continuation account mismatch')
        if host.native_session_id != session:
            raise BindingError('continuation session mismatch')
        profile = body['model'] + ':' + body['reasoning']['effort']
        if host.model_profile != profile:
            raise BindingError('continuation profile mismatch')
        # This endpoint is a Codex-routed lead inference; a sidekick
        # capability is never authorized here.
        if host.lane != 'lead':
            raise BindingError('operation not authorized for lane')
        if digest(body) != context.body_digest:
            raise BindingError('continuation body digest mismatch')
        return host

    def _reserve_with_compaction(self, binding, operation_id, body,
                                 marker_session_ref=None):
        """reserve() with one compaction-driven epoch-transition retry.

        A compaction note is matched first by the verified hook marker's
        session-name reference (the PostCompaction hook and the
        UserPromptSubmit marker both carry the session name), then by
        the wire seed reference. A transition is reported on the
        reservation dict as ``epoch_transition`` together with which
        reference matched (``compaction_correlation``).
        """
        try:
            return binding, self._store.reserve(
                binding, operation_id, body)
        except EpochRequired as e:
            error = e
        from .accounting import reference
        note_ts, correlation = None, None
        if marker_session_ref:
            note_ts = self._take_compaction_note(marker_session_ref)
            correlation = 'matched_marker'
        if note_ts is None:
            note_ts = self._take_compaction_note(
                reference('session', binding.session))
            correlation = 'matched_seed'
        if note_ts is None:
            raise error
        new_binding = self._store.transition_epoch(
            binding, digest({'epoch': binding.epoch,
                             'compaction': note_ts}),
            'history_compaction')
        reservation = self._store.reserve(new_binding, operation_id, body)
        reservation['epoch_transition'] = 'history_compaction'
        reservation['compaction_correlation'] = correlation
        return new_binding, reservation

    def prepare(self, packet, body, credentials, context=None,
                marker_session_ref=None):
        values = packet.get(16) if isinstance(packet, dict) else None
        if (not isinstance(values, list) or len(values) != 1
                or not isinstance(values[0], bytes) or not values[0]):
            raise ContinuationError('continuation session unavailable')
        try:
            session = values[0].decode()
        except UnicodeError:
            raise ContinuationError(
                'continuation session unavailable') from None
        if (not isinstance(credentials, tuple) or len(credentials) != 2
                or not isinstance(credentials[1], str)
                or not credentials[1]):
            raise ContinuationError('continuation account unavailable')
        if (not isinstance(body, dict)
                or not isinstance(body.get('model'), str)
                or not body['model']
                or not isinstance(body.get('reasoning'), dict)
                or not isinstance(body['reasoning'].get('effort'), str)
                or not body['reasoning']['effort']):
            raise ContinuationError('continuation profile unavailable')
        if self._capabilities is not None:
            host = self._verified_host(packet, body, credentials, context)
            # Follow any recorded epoch transitions (compaction, fork)
            # so a still-valid capability reaches its effective scope.
            binding = self._store.resolve_epoch(
                host.continuation_binding())
            return self._reserve_with_compaction(
                binding, context.operation_id, body,
                marker_session_ref=marker_session_ref)
        binding = self._resolver(packet, credentials)
        if not isinstance(binding, ContinuationBinding):
            raise ContinuationError('trusted continuation binding required')
        if binding.account != credentials[1]:
            raise ContinuationError('continuation account mismatch')
        if binding.session != session:
            raise ContinuationError('continuation session mismatch')
        if binding.profile != body['model'] + ':' + body['reasoning']['effort']:
            raise ContinuationError('continuation profile mismatch')
        binding = self._store.resolve_epoch(binding)
        operation_id = digest({'scope': binding.scope(),
                               'native_body': body})
        return binding, self._store.reserve(binding, operation_id, body)

    def execute(self, binding, reservation, invoke, preflight=None):
        if not isinstance(binding, ContinuationBinding) \
                or binding.scope() != reservation.get('scope'):
            raise ContinuationError('continuation reservation scope mismatch')
        if reservation.get('replay') is not None:
            return reservation['replay']
        output = []
        serialized = None
        if preflight is not None:
            # Measure + serialize the MERGED body (continuation
            # reinsertion included). A budget rejection abandons the
            # reservation instead of leaving it 'executing' forever.
            try:
                serialized = preflight(reservation['body'])
            except BudgetExceeded:
                self._store.abandon(reservation, 'preflight_rejected')
                raise
        try:
            result = invoke(reservation['body'], output, serialized)
        except RequestCancelled:
            try:
                self._store.mark_cancel(reservation)
            except (ContinuationError, sqlite3.Error):
                pass
            raise
        except Exception:
            try:
                self._store.mark_outcome_unknown(reservation)
            except (ContinuationError, sqlite3.Error):
                pass
            raise
        try:
            self._store.commit(reservation, output, result)
        except Exception:
            # The provider responded but the commit failed — the outcome
            # is unproven and must never be silently retried.
            try:
                self._store.mark_outcome_unknown(reservation)
            except (ContinuationError, sqlite3.Error):
                pass
            raise
        try:
            self._store.offer_result(reservation, binding)
        except Exception:
            # The turn is durably committed; offer failure leaves
            # delivery uncertain but never undoes the commit.
            reservation['delivery'] = 'uncertain'
        return result

    def abandon(self, reservation, reason):
        """Settle a reservation that provably never reached the provider
        (preflight or admission rejection) so the lane is not stranded."""
        self._store.abandon(reservation, reason)

    def acceptance(self, binding, reservation):
        return self._store.acceptance_state(
            binding, reservation['operation_id'])

    def acknowledge(self, context, ack):
        if self._capabilities is None:
            raise ContinuationError(
                'acknowledgement requires authenticated binding')
        if context is None:
            raise BindingError('unknown capability')
        host = self._capabilities.verify(context)
        # The ack endpoint binds to the lead lane only; a sidekick
        # capability is never authorized here.
        if host.lane != 'lead':
            raise BindingError('operation not authorized for lane')
        if not isinstance(ack, dict) or set(ack) != _ACK_KEYS \
                or ack.get('protocol_version') != 1:
            raise BindingError('malformed acknowledgement')
        if ack['client_instance_id'] != host.client_instance_id \
                or ack['session_id'] != host.native_session_id \
                or ack['lane'] != host.lane \
                or ack['operation_id'] != context.operation_id:
            raise BindingError('acknowledgement binding mismatch')
        chain = self._store.epoch_chain(host.continuation_binding())
        effective = next(
            (b for b in chain if b.epoch == ack['continuation_epoch']),
            None)
        if effective is None:
            raise BindingError('acknowledgement binding mismatch')
        idempotent = self._store.acknowledge_result(
            effective, ack['operation_id'],
            ack['revision'], ack['response_digest'],
            ack['consumer_commit_reference'])
        return {'acceptance': 'acknowledged', 'idempotent': idempotent}
