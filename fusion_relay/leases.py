"""Single-owner logical desktop lease with FIFO queuing.

This is a logical in-process lease for serializing relay-driven
observation/action pairs against one desktop surface. It is **not** a
platform-global fence: it does not lock out unrelated processes, the
user's own input, or other adapters. There is no renewal — a holder must
release and re-queue; expired leases free the slot once no action is in
flight, but a revoked/expired lease with an in-flight action is never
reassigned until that synchronous operation finishes.
"""

from __future__ import annotations

import contextlib
import math
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Optional

_MAX_WINDOW = 512
_OBS_MAX_AGE = 5.0
_POLL = 0.05
_MAX_WAIT = 30.0


@dataclass(frozen=True)
class DesktopLease:
    lease_id: str
    scope: str
    generation: int
    expires_at: float


class LeaseError(PermissionError):
    """Lease invalid, expired, revoked, or queue-bound failed."""


def _check_finite(v, name: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) \
            or not math.isfinite(float(v)):
        raise ValueError(f"invalid {name}")
    return float(v)


class LeaseManager:
    def __init__(self, clock=time.monotonic, max_queue: int = 8,
                 max_duration: float = 30):
        if type(max_queue) is not int or max_queue <= 0:
            raise ValueError("max_queue must be a positive int")
        md = _check_finite(max_duration, "max_duration")
        if not 1.0 <= md <= _MAX_WAIT:
            raise ValueError("max_duration must be within 1..30")
        self._clock = clock
        self._max_queue = max_queue
        self._max_duration = md
        self._cond = threading.Condition(threading.RLock())
        self._queue = deque()            # FIFO of waiter records
        self._slot = None                # {"lease","dead","in_flight"}
        self._generation = 0
        self._rev = {}                   # scope -> revocation generation
        self._obs = {}                   # obs_id -> obs record (slot-bound)

    @staticmethod
    def _run_check(check) -> None:
        if check is None:
            return
        try:
            if check():
                raise LeaseError("cancelled")
        except LeaseError:
            raise
        except Exception:
            raise LeaseError("cancelled")

    def _reap_locked(self) -> None:
        """Free a dead/expired slot once nothing is in flight."""
        s = self._slot
        if s is not None and not s["in_flight"] \
                and (s["dead"] or self._clock() >= s["lease"].expires_at):
            self._obs = {k: v for k, v in self._obs.items()
                         if v["lease_id"] != s["lease"].lease_id}
            self._slot = None

    def acquire(self, scope: str, check=None, wait_timeout: float = 5,
                duration: float = 15) -> DesktopLease:
        if not isinstance(scope, str) or not scope \
                or len(scope) > _MAX_WINDOW:
            raise ValueError("scope required")
        wt = _check_finite(wait_timeout, "wait_timeout")
        if not 0.0 <= wt <= _MAX_WAIT:
            raise ValueError("wait_timeout must be within 0..30")
        duration = min(self._max_duration,
                       max(1.0, _check_finite(duration, "duration")))
        deadline = time.monotonic() + wt
        me = {"scope": scope, "id": uuid.uuid4().hex}
        with self._cond:
            if len(self._queue) >= self._max_queue:
                raise LeaseError("lease queue full")
            me["rev"] = self._rev.get(scope, 0)
            self._queue.append(me)
            try:
                while True:
                    self._run_check(check)  # cancel before grant
                    if self._rev.get(scope, 0) != me["rev"]:
                        raise LeaseError("scope revoked while queued")
                    self._reap_locked()
                    s = self._slot
                    if s is not None and s["lease"].scope == scope \
                            and not s["dead"]:
                        raise LeaseError(
                            "lease already held by this scope")
                    if self._queue[0] is me and s is None:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise LeaseError("lease wait timed out")
                    self._cond.wait(min(_POLL, remaining))
                self._generation += 1
                lease = DesktopLease(
                    lease_id=uuid.uuid4().hex, scope=scope,
                    generation=self._generation,
                    expires_at=self._clock() + duration)
                self._slot = {"lease": lease, "dead": False,
                              "in_flight": False}
                return lease
            finally:
                try:
                    self._queue.remove(me)
                except ValueError:
                    pass
                self._cond.notify_all()

    def _require_locked(self, lease: DesktopLease):
        s = self._slot
        if s is None or s["dead"] or s["lease"] != lease \
                or self._clock() >= lease.expires_at:
            raise LeaseError("lease not held")
        return s

    def check(self, lease: DesktopLease, check_cancelled=None) -> None:
        """Validate the lease is live, then honor cancellation."""
        with self._cond:
            self._require_locked(lease)
        self._run_check(check_cancelled)

    def observe(self, lease: DesktopLease, window_identity: str) -> str:
        """Record a fresh observation; returns its id."""
        if not isinstance(window_identity, str) or not window_identity \
                or len(window_identity) > _MAX_WINDOW:
            raise ValueError("window_identity required")
        with self._cond:
            self._require_locked(lease)
            self._obs = {k: v for k, v in self._obs.items()
                         if v["lease_id"] != lease.lease_id}
            oid = uuid.uuid4().hex
            self._obs[oid] = {"lease_id": lease.lease_id,
                              "generation": lease.generation,
                              "window": window_identity,
                              "created": self._clock()}
            return oid

    def _validate_obs_locked(self, lease: DesktopLease,
                             observation_id: str,
                             current_window: str) -> None:
        obs = self._obs.get(observation_id)
        if obs is None or obs["lease_id"] != lease.lease_id \
                or obs["generation"] != lease.generation:
            raise LeaseError("unknown or stale observation")
        if self._clock() - obs["created"] > _OBS_MAX_AGE:
            raise LeaseError("observation expired")
        if obs["window"] != current_window:
            raise LeaseError("window changed since observation")

    @staticmethod
    def _check_window(current_window) -> None:
        if not isinstance(current_window, str) or not current_window \
                or len(current_window) > _MAX_WINDOW:
            raise ValueError("current_window required")

    def validate_action(self, lease: DesktopLease, observation_id: str,
                        current_window: str, check_cancelled=None) -> None:
        """Fresh matching observation required immediately before act."""
        self._check_window(current_window)
        with self._cond:
            self._require_locked(lease)
            self._validate_obs_locked(lease, observation_id,
                                      current_window)
        self._run_check(check_cancelled)

    @contextlib.contextmanager
    def action(self, lease: DesktopLease, observation_id=None,
               current_window=None, check=None):
        """Exclusively hold the in-flight mark across a synchronous
        dispatch. Concurrent entries on the same lease serialize; a
        revoked/expired in-flight lease is not reassigned until this
        operation finishes."""
        if observation_id is not None:
            self._check_window(current_window)
        with self._cond:
            while True:
                self._run_check(check)
                s = self._slot
                if s is None or s["dead"] or s["lease"] != lease \
                        or self._clock() >= lease.expires_at:
                    raise LeaseError("lease not held")
                if not s["in_flight"]:
                    break
                self._cond.wait(_POLL)
            # exclusive: validate observation then mark in flight
            if observation_id is not None:
                self._validate_obs_locked(lease, observation_id,
                                          current_window)
            s["in_flight"] = True
        try:
            yield
        finally:
            with self._cond:
                s = self._slot
                if s is not None and s["lease"] == lease:
                    s["in_flight"] = False
                    self._reap_locked()
                self._cond.notify_all()

    def release(self, lease: DesktopLease) -> None:
        """Release the slot; an in-flight release marks the lease dead
        and the slot is freed when the operation finishes."""
        with self._cond:
            s = self._slot
            if s is not None and s["lease"] == lease:
                if s["in_flight"]:
                    s["dead"] = True
                else:
                    self._obs = {
                        k: v for k, v in self._obs.items()
                        if v["lease_id"] != lease.lease_id}
                    self._slot = None
            self._cond.notify_all()

    def revoke(self, scope: str) -> None:
        """Invalidate owner/queued waiters and observations for a scope.

        A revoked in-flight action still finishes; the slot is not
        reassigned until it does. Not platform-global fencing.
        """
        with self._cond:
            self._generation += 1
            self._rev[scope] = self._rev.get(scope, 0) + 1
            s = self._slot
            if s is not None and s["lease"].scope == scope:
                s["dead"] = True
                self._obs = {k: v for k, v in self._obs.items()
                             if v["lease_id"] != s["lease"].lease_id}
            self._cond.notify_all()
