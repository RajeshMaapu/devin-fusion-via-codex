"""Trusted host bindings for durable continuation.

A HostBinding is an in-process capability issued by the hosting
application (launcher process ownership today; a verified native hook
issuer does not exist yet). The relay never trusts wire metadata for
lane/account/session claims — it verifies an HMAC proof over
(capability, operation, body digest) against a secret that never leaves
the hosting process except through issue()'s return to the host itself.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from dataclasses import dataclass

from .continuation import (ContinuationBinding, ContinuationError,
                           canonical)

PROTOCOL_VERSION = 1
PROVENANCE_NATIVE = 'native_hook_verified'      # no issuer exists today
PROVENANCE_LAUNCHER = 'launcher_process_ownership'
PROVENANCE_RESOLVER = 'resolver_unverified'     # legacy callable resolver
_PROVENANCES = {PROVENANCE_NATIVE, PROVENANCE_LAUNCHER,
                PROVENANCE_RESOLVER}
LANES = ('lead', 'sidekick')

_HEX64 = re.compile(r'[0-9a-f]{64}\Z')
_OPERATION_ID = re.compile(r'[ -~]{1,512}\Z')

HEADER_CAPABILITY = 'X-Fusion-Capability'
HEADER_OPERATION = 'X-Fusion-Operation'
HEADER_PROOF = 'X-Fusion-Proof'


class BindingError(ContinuationError):
    """A presented binding failed verification or comparison."""


@dataclass(frozen=True)
class HostBinding:
    protocol_version: int
    client_instance_id: str      # hex64
    account_reference: str
    native_session_id: str
    lane: str
    model_profile: str           # 'model:effort'
    continuation_epoch: str
    capability_id: str           # hex64
    expires_at: float
    revocation_generation: int
    provenance: str

    def continuation_binding(self) -> ContinuationBinding:
        return ContinuationBinding(
            account=self.account_reference,
            session=self.native_session_id,
            lane=self.lane,
            profile=self.model_profile,
            epoch=self.continuation_epoch)


@dataclass(frozen=True)
class AuthenticatedContext:
    capability_id: str
    operation_id: str
    body_digest: str             # hex64
    proof: str                   # hex64
    now: float


def _proof_payload(capability_id: str, operation_id: str,
                   body_digest: str) -> bytes:
    return canonical({
        'protocol_version': PROTOCOL_VERSION,
        'capability_id': capability_id,
        'operation_id': operation_id,
        'body_digest': body_digest})


class CapabilityStore:
    """In-process capability registry owned by the hosting application.

    Secrets never leave the process except via issue()'s return to the
    host; the relay only ever sees ids and HMAC proofs.
    """

    def __init__(self, clock=time.time):
        self._clock = clock
        self._caps: dict = {}
        self._generations: dict = {}

    def issue(self, *, client_instance_id, account_reference,
              native_session_id, lane, model_profile, continuation_epoch,
              provenance, ttl_s) -> tuple:
        if lane not in LANES:
            raise BindingError('invalid lane')
        if provenance not in _PROVENANCES:
            raise BindingError('invalid provenance')
        if not isinstance(client_instance_id, str) \
                or not _HEX64.fullmatch(client_instance_id):
            raise BindingError('invalid client instance id')
        for value in (account_reference, native_session_id,
                      model_profile, continuation_epoch):
            if not isinstance(value, str) or not value or len(value) > 512:
                raise BindingError('invalid binding field')
        if isinstance(ttl_s, bool) or not isinstance(ttl_s, (int, float)) \
                or not 1 <= ttl_s <= 86400:
            raise BindingError('invalid capability ttl')
        secret = secrets.token_bytes(32)
        capability_id = secrets.token_hex(32)
        binding = HostBinding(
            protocol_version=PROTOCOL_VERSION,
            client_instance_id=client_instance_id,
            account_reference=account_reference,
            native_session_id=native_session_id,
            lane=lane,
            model_profile=model_profile,
            continuation_epoch=continuation_epoch,
            capability_id=capability_id,
            expires_at=self._clock() + ttl_s,
            revocation_generation=self._generations.get(
                client_instance_id, 0),
            provenance=provenance)
        self._caps[capability_id] = {
            'secret': secret, 'binding': binding, 'revoked': False}
        return binding, secret

    def revoke(self, capability_id) -> None:
        entry = self._caps.get(capability_id)
        if entry is not None:
            entry['revoked'] = True

    def bump_generation(self, client_instance_id) -> None:
        """Invalidate every capability issued to this client so far."""
        self._generations[client_instance_id] = \
            self._generations.get(client_instance_id, 0) + 1

    @staticmethod
    def prove(secret, capability_id, operation_id, body_digest) -> str:
        """Host-side helper computing the presentation proof."""
        return hmac.new(secret, _proof_payload(
            capability_id, operation_id, body_digest),
            hashlib.sha256).hexdigest()

    def verify(self, context: AuthenticatedContext) -> HostBinding:
        if not isinstance(context, AuthenticatedContext):
            raise BindingError('malformed binding context')
        entry = self._caps.get(context.capability_id)
        if entry is None:
            raise BindingError('unknown capability')
        binding = entry['binding']
        if context.now > binding.expires_at:
            raise BindingError('capability expired')
        if entry['revoked'] or binding.revocation_generation < \
                self._generations.get(binding.client_instance_id, 0):
            raise BindingError('capability revoked')
        expected = self.prove(entry['secret'], context.capability_id,
                              context.operation_id, context.body_digest)
        if not hmac.compare_digest(expected, context.proof):
            raise BindingError('capability proof invalid')
        return binding


def context_from_headers(headers, body_digest, now):
    """Build an AuthenticatedContext from request headers.

    Returns None when the capability header is absent — the native CLI
    never sends one. A partially-present or malformed set is rejected.
    """
    capability = headers.get(HEADER_CAPABILITY)
    if capability is None:
        return None
    operation_id = headers.get(HEADER_OPERATION)
    proof = headers.get(HEADER_PROOF)
    if operation_id is None or proof is None \
            or not _HEX64.fullmatch(capability) \
            or not _HEX64.fullmatch(proof) \
            or not isinstance(operation_id, str) \
            or not _OPERATION_ID.fullmatch(operation_id) \
            or not isinstance(body_digest, str) \
            or not _HEX64.fullmatch(body_digest):
        raise BindingError('malformed binding headers')
    return AuthenticatedContext(capability_id=capability,
                                operation_id=operation_id,
                                body_digest=body_digest,
                                proof=proof, now=now)
