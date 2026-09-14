"""In-process human approval control plane.

A trusted broker caller supplies an :class:`ApprovalScope` describing the
principal, session, operation, app, and capability a human is being asked
to approve. The scope is a local caller contract — it is **not** host
identity proof and says nothing about which model role produced the
request.

This slice is intentionally in-memory: no saved approvals, no
persistence. A restart re-prompts for everything. Tickets are random
UUIDs; nothing about a ticket is guessable from model output.
"""

from __future__ import annotations

import collections
import math
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

_CAPABILITIES = frozenset({"observe", "act"})
_DECISIONS = frozenset({"allow_once", "allow_task", "deny"})
_APP_RE = re.compile(r"[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_MAX_NAME = 512   # principal / session / operation_id
_MAX_APP = 255
_MAX_TASK = 4096
_MAX_SUMMARY = 512
_MAX_TTL = 120.0
_MIN_TTL = 1.0
_TASK_GRANT_TTL = 300.0  # allow_task grant: min 5 minutes from creation
_HISTORY_BOUND = 256
_WAIT_POLL = 0.05


@dataclass(frozen=True)
class ApprovalScope:
    principal: str
    session: str
    operation_id: str
    app: str
    capability: str
    binding_revision: int
    action_digest: str


@dataclass(frozen=True)
class ApprovalTicket:
    request_id: str
    scope: ApprovalScope
    task: str
    action_summary: str
    expires_at: float
    revision: int
    allowed_decisions: tuple = ("allow_once", "allow_task", "deny")


def _check_scope(scope: ApprovalScope) -> None:
    if not isinstance(scope, ApprovalScope):
        raise ValueError("invalid approval scope")
    for name, limit in (("principal", _MAX_NAME), ("session", _MAX_NAME),
                        ("operation_id", _MAX_NAME), ("app", _MAX_APP),
                        ("capability", _MAX_NAME),
                        ("action_digest", 64)):
        v = getattr(scope, name)
        if not isinstance(v, str) or not v or len(v) > limit:
            raise ValueError(f"scope.{name} must be a bounded "
                             "nonempty string")
    if not _APP_RE.fullmatch(scope.app):
        raise ValueError("scope.app must be a dotted bundle identifier")
    if scope.capability not in _CAPABILITIES:
        raise ValueError("scope.capability must be 'observe' or 'act'")
    if type(scope.binding_revision) is not int \
            or scope.binding_revision <= 0:
        raise ValueError("scope.binding_revision must be a positive int")
    if not _DIGEST_RE.fullmatch(scope.action_digest):
        raise ValueError("scope.action_digest must be 64 lowercase hex")


@dataclass
class _Grant:
    created_at: float

    def fresh(self, now: float) -> bool:
        return now - self.created_at <= _TASK_GRANT_TTL


class ApprovalManager:
    """Bounded, revisioned approval state for one process.

    Thread-safe via a single RLock + Condition. All decisions are
    explicit human decisions delivered through :meth:`decide`; there is
    deliberately no "remember always" provider grant.
    """

    def __init__(self, clock=time.monotonic, max_pending: int = 64):
        if type(max_pending) is not int or max_pending <= 0:
            raise ValueError("max_pending must be a positive int")
        self._clock = clock
        self._max_pending = max_pending
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        # request_id -> {"ticket": ApprovalTicket, "status": str}
        self._tickets: dict[str, dict] = {}
        # (principal, session, app, capability, binding_revision) -> _Grant
        self._grants: dict[tuple, _Grant] = {}
        # (session, app) -> revision counter, starting at 1
        self._revisions: dict[tuple[str, str], int] = {}
        self._history: collections.deque = collections.deque(
            maxlen=_HISTORY_BOUND)
        self._closed = False

    # -- helpers (caller holds the lock) --------------------------------

    def _revision(self, session: str, app: str) -> int:
        return self._revisions.get((session, app), 1)

    def _terminal(self, request_id: str, status: str) -> None:
        if request_id in self._tickets:
            del self._tickets[request_id]
            self._history.append(request_id)
        self._cond.notify_all()

    def _grant_key(self, s: ApprovalScope) -> tuple:
        return (s.principal, s.session, s.app, s.capability,
                s.binding_revision)

    def _live_grant(self, scope: ApprovalScope) -> Optional[_Grant]:
        g = self._grants.get(self._grant_key(scope))
        if g is not None and g.fresh(self._clock()):
            return g
        return None

    def _prune_locked(self) -> None:
        """Expire tickets in every status and stale grants."""
        now = self._clock()
        for rid, state in list(self._tickets.items()):
            if now >= state["ticket"].expires_at:
                self._terminal(rid, "expired")
        for key, g in list(self._grants.items()):
            if not g.fresh(now):
                del self._grants[key]

    # -- public API -----------------------------------------------------

    def request(self, scope: ApprovalScope, task: str,
                action_summary: str, ttl: float = 120,
                allowed_decisions=None) -> ApprovalTicket:
        """Enqueue an approval request; returns a random-id ticket.

        ``allowed_decisions`` bounds which decisions a human may take on
        this ticket; it must be a nonempty subset including ``deny``.
        A ticket that excludes ``allow_task`` can never create or reuse
        a task grant. Otherwise, if a current explicit ``allow_task``
        grant covers this exact binding
        (principal/session/app/capability/revision), the ticket is
        created pre-approved -- nothing else auto-approves.
        """
        _check_scope(scope)
        if allowed_decisions is None:
            allowed = ("allow_once", "allow_task", "deny")
        else:
            if not isinstance(allowed_decisions, (tuple, list)) \
                    or not allowed_decisions \
                    or not set(allowed_decisions) <= _DECISIONS \
                    or "deny" not in allowed_decisions:
                raise ValueError(
                    "allowed_decisions must be a nonempty subset of "
                    "allow_once/allow_task/deny including deny")
            allowed = tuple(allowed_decisions)
        if not isinstance(task, str) or not task \
                or len(task) > _MAX_TASK:
            raise ValueError("task label required (<=4096 chars)")
        if not isinstance(action_summary, str) or not action_summary \
                or len(action_summary) > _MAX_SUMMARY:
            raise ValueError("action_summary required (<=512 chars)")
        if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) \
                or not math.isfinite(float(ttl)):
            raise ValueError("invalid ttl")
        ttl = float(ttl)
        ttl = min(_MAX_TTL, max(_MIN_TTL, ttl))
        with self._cond:
            if self._closed:
                raise PermissionError("approval manager closed")
            self._prune_locked()
            if len(self._tickets) >= self._max_pending:
                raise RuntimeError("pending approval capacity reached")
            granted = "allow_task" in allowed \
                and self._live_grant(scope) is not None
            self._revisions.setdefault((scope.session, scope.app), 1)
            ticket = ApprovalTicket(
                request_id=uuid.uuid4().hex,
                scope=scope,
                task=task,
                action_summary=action_summary,
                expires_at=self._clock() + ttl,
                revision=self._revision(scope.session, scope.app),
                allowed_decisions=allowed)
            self._tickets[ticket.request_id] = {
                "ticket": ticket,
                "status": "allow_task" if granted else "pending"}
            self._cond.notify_all()
            return ticket

    def pending(self) -> list[dict]:
        """Snapshot of undecided, unexpired tickets for a UI."""
        out = []
        with self._lock:
            self._prune_locked()
            now = self._clock()
            for state in self._tickets.values():
                t = state["ticket"]
                if state["status"] != "pending":
                    continue
                s = t.scope
                out.append({
                    "request_id": t.request_id,
                    "task": t.task,
                    "action_summary": t.action_summary,
                    "principal": s.principal,
                    "session": s.session,
                    "operation_id": s.operation_id,
                    "app": s.app,
                    "capability": s.capability,
                    "binding_revision": s.binding_revision,
                    "action_digest": s.action_digest,
                    "expires_in": max(0.0, t.expires_at - now),
                    "revision": t.revision,
                    "allowed_decisions": list(t.allowed_decisions)})
        return out

    def grants(self) -> list[dict]:
        """Live task grants with remaining seconds, for a UI."""
        with self._lock:
            self._prune_locked()
            now = self._clock()
            return [{
                "principal": k[0], "session": k[1], "app": k[2],
                "capability": k[3], "binding_revision": k[4],
                "remaining_s": max(
                    0.0, _TASK_GRANT_TTL - (now - g.created_at))}
                for k, g in self._grants.items()]

    def decide(self, request_id: str, decision: str,
               revision: int) -> bool:
        """Record a human decision. False on unknown/stale/terminal id."""
        if decision not in _DECISIONS:
            raise ValueError("decision must be allow_once/allow_task/deny")
        with self._cond:
            self._prune_locked()
            state = self._tickets.get(request_id)
            if state is None or state["status"] != "pending":
                return False
            t = state["ticket"]
            if decision not in t.allowed_decisions:
                return False  # decision type this ticket never permits
            if type(revision) is not int \
                    or revision != self._revision(t.scope.session,
                                                  t.scope.app):
                return False  # stale revision — never act on it
            if self._clock() >= t.expires_at:
                self._terminal(request_id, "expired")
                return False
            if decision == "deny":
                self._terminal(request_id, "denied")
                return True
            state["status"] = decision
            if decision == "allow_task":
                self._grants[self._grant_key(t.scope)] = _Grant(
                    created_at=self._clock())
            self._cond.notify_all()
            return True

    def wait(self, ticket: ApprovalTicket, check_cancelled=None) -> str:
        """Block until the ticket is decided; returns the decision.

        Raises ``PermissionError`` on denial, expiry, revocation,
        cancellation, or manager shutdown — always a generic reason.
        ``check_cancelled`` is polled every ~50 ms; a truthy return or a
        raised exception cancels the wait.
        """
        with self._cond:
            while True:
                if self._closed:
                    self._fail(ticket, "cancelled",
                               "approval manager closed")
                state = self._tickets.get(ticket.request_id)
                if state is None or state["ticket"] != ticket:
                    raise PermissionError("approval request invalid")
                if self._clock() >= ticket.expires_at:
                    self._fail(ticket, "expired",
                               "approval request expired")
                cancelled = False
                if check_cancelled is not None:
                    try:
                        cancelled = bool(check_cancelled())
                    except Exception:
                        cancelled = True
                if cancelled:
                    self._fail(ticket, "cancelled",
                               "approval request cancelled")
                status = state["status"]
                if status == "revoked":
                    raise PermissionError("approval revoked")
                if status in ("allow_once", "allow_task"):
                    s = ticket.scope
                    if ticket.revision != self._revision(s.session,
                                                       s.app):
                        self._fail(ticket, "revoked",
                                   "approval ticket stale")
                    if status == "allow_task" \
                            and self._live_grant(s) is None:
                        self._fail(ticket, "revoked",
                                   "task grant no longer valid")
                    return status
                self._cond.wait(_WAIT_POLL)

    def _fail(self, ticket: ApprovalTicket, status: str,
              reason: str) -> None:
        self._terminal(ticket.request_id, status)
        raise PermissionError(reason)

    def authorize(self, ticket: ApprovalTicket,
                  consume: bool = False) -> None:
        """Validate an approved ticket before acting on it.

        Re-checks ticket identity (immutable equality — a swapped or
        forged ticket fails), expiry, current revision, and grant state.
        ``consume=True`` burns an ``allow_once`` ticket atomically; a
        second consume is refused. ``allow_task`` additionally requires
        the live grant.
        """
        with self._cond:
            self._prune_locked()
            state = self._tickets.get(ticket.request_id)
            if state is None or state["ticket"] != ticket:
                raise PermissionError("approval ticket invalid")
            status = state["status"]
            if status not in ("allow_once", "allow_task"):
                raise PermissionError("approval ticket not approved")
            if self._closed:
                raise PermissionError("approval manager closed")
            if self._clock() >= ticket.expires_at:
                self._fail(ticket, "expired", "approval ticket expired")
            s = ticket.scope
            if ticket.revision != self._revision(s.session, s.app):
                self._fail(ticket, "revoked", "approval ticket stale")
            if status == "allow_task" and self._live_grant(s) is None:
                self._fail(ticket, "revoked",
                           "task grant no longer valid")
            if consume:
                self._terminal(ticket.request_id, "consumed")

    def check_revision(self, ticket: ApprovalTicket) -> None:
        with self._lock:
            s = ticket.scope
            if self._closed or ticket.revision != self._revision(
                    s.session, s.app):
                raise PermissionError('approval scope revoked')

    def request_cancelled(self, ticket: ApprovalTicket) -> None:
        """Retire a ticket whose caller gave up waiting."""
        with self._cond:
            state = self._tickets.get(ticket.request_id)
            if state is not None and state["ticket"] == ticket:
                self._terminal(ticket.request_id, "cancelled")

    def revoke(self, session: str, app=None) -> None:
        """Invalidate tickets and grants for a session (optionally per
        app), bump revisions so stale references fail, and wake waiters."""
        with self._cond:
            targets = {k for k in self._revisions
                       if k[0] == session
                       and (app is None or k[1] == app)}
            targets |= {(s["ticket"].scope.session, s["ticket"].scope.app)
                        for s in self._tickets.values()
                        if s["ticket"].scope.session == session
                        and (app is None
                             or s["ticket"].scope.app == app)}
            targets |= {(k[1], k[2]) for k in self._grants
                        if k[1] == session
                        and (app is None or k[2] == app)}
            if app is not None:
                targets.add((session, app))
            for k in targets:
                self._revisions[k] = self._revisions.get(k, 1) + 1
            for gk in [k for k in self._grants
                       if k[1] == session
                       and (app is None or k[2] == app)]:
                del self._grants[gk]
            for rid, state in list(self._tickets.items()):
                s = state["ticket"].scope
                if s.session == session and (app is None or s.app == app):
                    self._terminal(rid, "revoked")
            self._cond.notify_all()

    def close(self) -> None:
        """Cancel everything; further requests fail."""
        with self._cond:
            self._closed = True
            self._grants.clear()
            for rid in list(self._tickets):
                self._terminal(rid, "cancelled")
            self._cond.notify_all()
