"""Local computer-use broker: approvals + leases + artifacts + journal.

This is a concrete local contract vertical slice for a trusted host
caller. Registration is host-only (no HTTP endpoint); the returned
capability token is an opaque in-process handle and is never given to
the model. Role attestation is **not** solved here: ``role`` is a
caller-supplied local binding — sidekick is always denied — and a
same-user process compromise is explicitly outside the boundary.

Actions are a fixed schema — observe / click / type only. No code,
shell, eval, or arbitrary batches: each adapter boundary call is one
structured action validated against a fresh observation. Every
submitted journal operation that fails mid-dispatch is recorded
``outcome_unknown`` and surfaced as ``IncompleteResponse`` — explicit
reconciliation, never automatic retry.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import threading
from dataclasses import asdict, dataclass

from . import translate
from .approvals import ApprovalScope
from .artifacts import ToolResult

_APP_RE = re.compile(r"[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+")
_OBS_RE = re.compile(r"[0-9a-f]{32}")
_NAME_MAX = 512
_APP_MAX = 255
_TEXT_MAX = 64 << 10
_POLL = 0.05
_ROLES = frozenset({"lead", "sidekick"})
_CALL_OBSERVE = "broker_observe"
_CALL_ACT = "broker_act"


class _WorkFailed(Exception):
    """Wrapper marking an exception raised inside a journaled call."""


@dataclass(frozen=True)
class ExecutionBinding:
    principal: str
    session: str
    role: str
    revision: int


def _check_binding(b: ExecutionBinding) -> None:
    if not isinstance(b, ExecutionBinding):
        raise ValueError("invalid binding")
    for name in ("principal", "session", "role"):
        v = getattr(b, name)
        if not isinstance(v, str) or not v or len(v) > _NAME_MAX:
            raise ValueError(f"binding.{name} must be a bounded "
                             "nonempty string")
    if b.role not in _ROLES:
        raise ValueError("binding.role must be 'lead' or 'sidekick'")
    if type(b.revision) is not int or b.revision <= 0:
        raise ValueError("binding.revision must be a positive int")


def _check_action(action: dict) -> str:
    """Whitelist the fixed action schema; returns the kind."""
    if not isinstance(action, dict):
        raise ValueError("action must be a dict")
    kind = action.get("kind")
    if kind == "observe":
        if set(action) != {"kind"}:
            raise ValueError("observe takes no fields")
    elif kind in ("click", "type"):
        oid = action.get("observation_id")
        if not isinstance(oid, str) or not _OBS_RE.fullmatch(oid):
            raise ValueError("observation_id must be a 32-hex ref")
        extra = set(action) - {"kind", "observation_id"}
        if kind == "click":
            if extra != {"x", "y"} \
                    or type(action["x"]) is not int \
                    or type(action["y"]) is not int \
                    or action["x"] < 0 or action["y"] < 0:
                raise ValueError("invalid click action")
        else:
            if extra != {"text"} \
                    or not isinstance(action["text"], str) \
                    or len(action["text"].encode("utf-8")) > _TEXT_MAX:
                raise ValueError("invalid type action")
    else:
        raise ValueError("unsupported action kind")
    return kind


def _result_json(result: ToolResult) -> str:
    return json.dumps(asdict(result), sort_keys=True)


def _result_from(raw: str) -> ToolResult:
    d = json.loads(raw)
    return ToolResult(operation_id=d["operation_id"],
                      status=d["status"],
                      text_blocks=tuple(d.get("text_blocks") or ()),
                      image_artifact_refs=tuple(
                          d.get("image_artifact_refs") or ()))


def _call(name: str, args: dict, operation_id: str) -> dict:
    return {"call_id": operation_id, "name": name,
            "arguments": json.dumps(args, sort_keys=True)}


def _fingerprint(call: dict) -> str:
    return hashlib.sha256(json.dumps(
        [call["name"], call["arguments"]]).encode()).hexdigest()


class _ExecutionContext:
    def __init__(self, check):
        self.check = check


class ComputerBroker:
    def __init__(self, approvals, leases, artifacts, journal, adapter):
        self._approvals = approvals
        self._leases = leases
        self._artifacts = artifacts
        self._journal = journal
        self._adapter = adapter
        self._adapter_binding = None
        self._lock = threading.RLock()
        self._caps = {}  # token -> {"binding","lock","lease","obs",...}

    # -- host-only registration ----------------------------------------

    def register(self, binding: ExecutionBinding) -> str:
        _check_binding(binding)
        token = secrets.token_urlsafe(32)
        with self._lock:
            if binding.role == 'lead':
                if self._adapter_binding is not None \
                        and self._adapter_binding != binding:
                    raise PermissionError(
                        'adapter already bound to another '
                        'execution scope')
                self._adapter_binding = binding
            self._caps[token] = {"binding": binding,
                                 "lock": threading.Lock(),
                                 "lease": None, "obs": None,
                                 "window": None, "app": None,
                                 "width": None, "height": None}
        return token

    def revoke(self, capability: str) -> None:
        with self._lock:
            entry = self._caps.pop(capability, None)
        if entry is None:
            return
        b = entry["binding"]
        self._approvals.revoke(b.session)
        self._leases.revoke(self._journal_scope(b))

    # -- internals ------------------------------------------------------

    def _entry(self, capability: str) -> dict:
        with self._lock:
            entry = self._caps.get(capability)
        if entry is None:
            raise PermissionError("unknown capability")
        return entry

    def _live(self, entry: dict, capability: str) -> None:
        with self._lock:
            if self._caps.get(capability) is not entry:
                raise PermissionError("capability revoked")

    def _guard(self, entry: dict, capability: str, context,
               ticket=None, lease=None):
        """Combined cancellation + liveness check for every boundary."""
        def check():
            context.check()
            self._live(entry, capability)
            if ticket is not None:
                self._approvals.check_revision(ticket)
            if lease is not None:
                self._leases.check(lease)
        return check

    @staticmethod
    def _journal_scope(b: ExecutionBinding) -> str:
        return hashlib.sha256(json.dumps(
            [b.principal, b.session, b.role, b.revision]).encode()
        ).hexdigest()

    @staticmethod
    def _digest(app: str, action: dict) -> str:
        return hashlib.sha256(json.dumps(
            {"app": app, "action": action}, sort_keys=True).encode()
        ).hexdigest()

    def _bind_adapter(self, binding, check):
        bind = getattr(self._adapter, 'bind_execution', None)
        if not callable(bind):
            raise PermissionError('adapter lacks execution-context binding')
        bind(binding, _ExecutionContext(check))

    def _approve(self, entry, capability, operation_id, app,
                 approval_kind, digest, task, summary, context):
        b = entry["binding"]
        scope = ApprovalScope(
            principal=b.principal, session=b.session,
            operation_id=operation_id, app=app,
            capability=approval_kind, binding_revision=b.revision,
            action_digest=digest)
        ticket = self._approvals.request(
            scope, task=task, action_summary=summary)
        try:
            self._approvals.wait(
                ticket, check_cancelled=self._guard(entry, capability,
                                                    context))
        except BaseException:
            self._approvals.request_cancelled(ticket)
            raise
        return ticket

    def _replay(self, jscope, call, operation_id, entry, capability,
                context):
        """Return the persisted result for an identical prior call.

        Fingerprint mismatch on a known operation id is a conflict;
        non-succeeded or malformed rows fail explicit — no retry.
        """
        self._live(entry, capability)
        context.check()
        row = self._journal.lookup(jscope, operation_id)
        if row is None:
            return None
        if row["fingerprint"] != _fingerprint(call):
            raise translate.UnsupportedRequest(
                "conflicting reuse of operation id")
        if row["status"] == "succeeded" \
                and isinstance(row["result"], str):
            try:
                res = _result_from(row["result"])
            except (ValueError, KeyError, TypeError):
                raise translate.IncompleteResponse(
                    "persisted result malformed; explicit "
                    "reconciliation required")
            if res.operation_id != operation_id:
                raise translate.IncompleteResponse(
                    "persisted result mismatch; explicit "
                    "reconciliation required")
            return res
        raise translate.IncompleteResponse(
            "outcome_unknown: explicit reconciliation required")

    @staticmethod
    def _check_observe_shape(out) -> None:
        if not isinstance(out, dict) \
                or not isinstance(out.get("window_identity"), str) \
                or not out["window_identity"] \
                or len(out["window_identity"]) > 512 \
                or not isinstance(out.get("text"), str) \
                or not isinstance(out.get("png"), (bytes, bytearray)):
            raise RuntimeError("adapter observe returned bad shape")

    # -- operations -----------------------------------------------------

    def execute(self, capability: str, operation_id: str, app: str,
                action: dict, context) -> ToolResult:
        if not isinstance(operation_id, str) or not operation_id \
                or len(operation_id) > _NAME_MAX:
            raise ValueError("operation_id required")
        if not isinstance(app, str) or len(app) > _APP_MAX \
                or not _APP_RE.fullmatch(app):
            raise ValueError("app must be a dotted bundle identifier")
        kind = _check_action(action)
        # immutable snapshot: later mutation cannot change the payload
        action = json.loads(json.dumps(action, allow_nan=False))
        entry = self._entry(capability)
        b = entry["binding"]
        if b.role != "lead":
            raise PermissionError("sidekick role cannot dispatch "
                                  "computer actions")
        context.check()
        lock = entry["lock"]
        while not lock.acquire(timeout=_POLL):
            self._live(entry, capability)
            context.check()
        try:
            self._live(entry, capability)
            context.check()
            if kind == "observe":
                return self._observe(entry, capability, operation_id,
                                     app, action, context)
            return self._act(entry, capability, operation_id, app,
                             action, context)
        finally:
            lock.release()

    def _observe(self, entry, capability, operation_id, app, action,
                 context) -> ToolResult:
        b = entry["binding"]
        jscope = self._journal_scope(b)
        call = _call(_CALL_OBSERVE, {"app": app, "kind": "observe"},
                     operation_id)
        prior = self._replay(jscope, call, operation_id, entry,
                             capability, context)
        if prior is not None:
            return prior
        guard = self._guard(entry, capability, context)
        digest = self._digest(app, action)
        ticket = self._approve(entry, capability, operation_id, app,
                               "observe", digest,
                               "Computer observation",
                               f"Observe {app} window", context)
        old = entry.get("lease")
        if old is not None:
            self._leases.release(old)
            entry.update(lease=None, obs=None, window=None, app=None,
                         width=None, height=None)
        try:
            lease = self._leases.acquire(jscope, guard,
                                         wait_timeout=5, duration=15)
        except BaseException:
            self._approvals.request_cancelled(ticket)
            raise
        guard = self._guard(entry, capability, context, ticket, lease)

        def work():
            guard()
            self._approvals.authorize(ticket, consume=True)
            guard()
            self._bind_adapter(b, guard)
            out = self._adapter.observe(app)
            self._check_observe_shape(out)
            guard()
            art = self._artifacts.put_png(
                jscope, operation_id, bytes(out["png"]))
            obs = self._leases.observe(lease, out["window_identity"])
            entry.update(lease=lease, obs=obs,
                         window=out["window_identity"], app=app,
                         width=art.width, height=art.height)
            meta = json.dumps({"observation_id": obs,
                               "lease_id": lease.lease_id,
                               "window": out["window_identity"],
                               "width": art.width,
                               "height": art.height})
            return _result_json(ToolResult(
                operation_id=operation_id, status="succeeded",
                text_blocks=(meta, out["text"]),
                image_artifact_refs=(art.ref,)))

        def guarded_work():
            try:
                return work()
            except BaseException as e:
                raise _WorkFailed(e) from e

        try:
            with self._leases.action(lease, check=guard):
                raw = self._journal.run(jscope, call, guarded_work,
                                        guard)
        except _WorkFailed as e:
            self._leases.release(lease)
            self._approvals.request_cancelled(ticket)
            raise translate.IncompleteResponse(
                "outcome_unknown: explicit reconciliation required"
            ) from e
        except BaseException:
            self._leases.release(lease)
            self._approvals.request_cancelled(ticket)
            raise
        return _result_from(raw)

    def _act(self, entry, capability, operation_id, app, action,
             context) -> ToolResult:
        b = entry["binding"]
        jscope = self._journal_scope(b)
        call = _call(_CALL_ACT, {"app": app, "action": action},
                     operation_id)
        prior = self._replay(jscope, call, operation_id, entry,
                             capability, context)
        if prior is not None:
            return prior
        # caller-supplied observation must match the live entry, the
        # app must be the observed one, and click coords must fall
        # inside the observed image — all before any approval/adapte
        lease = entry.get("lease")
        obs = entry.get("obs")
        if lease is None or obs is None or entry.get("app") != app:
            raise PermissionError(
                "fresh observation required before acting")
        if action["observation_id"] != obs:
            raise PermissionError(
                "observation_id does not match current observation")
        if action["kind"] == "click" and (
                action["x"] >= entry["width"]
                or action["y"] >= entry["height"]):
            raise PermissionError(
                "click coordinates outside observed image")
        guard = self._guard(entry, capability, context)
        digest = self._digest(app, action)
        ticket = self._approve(entry, capability, operation_id, app,
                               "act", digest, "Computer action",
                               f"{action['kind']} on {app}", context)
        guard = self._guard(entry, capability, context, ticket, lease)

        def work():
            guard()
            current = self._adapter.current_window(app)
            guard()
            self._leases.validate_action(lease, obs, current, guard)
            self._approvals.authorize(ticket, consume=True)
            guard()
            self._adapter.act(app, action)
            guard()
            verify = self._adapter.observe(app)
            self._check_observe_shape(verify)
            guard()
            art = self._artifacts.put_png(
                jscope, operation_id, bytes(verify["png"]))
            new_obs = self._leases.observe(lease,
                                           verify["window_identity"])
            entry.update(obs=new_obs, window=verify["window_identity"],
                         width=art.width, height=art.height)
            meta = json.dumps({"observation_id": new_obs,
                               "prior_observation_id": obs,
                               "lease_id": lease.lease_id})
            return _result_json(ToolResult(
                operation_id=operation_id, status="succeeded",
                text_blocks=(meta, verify["text"]),
                image_artifact_refs=(art.ref,)))

        def guarded_work():
            try:
                return work()
            except BaseException as e:
                raise _WorkFailed(e) from e

        try:
            with self._leases.action(lease, check=guard):
                guard()
                self._bind_adapter(b, guard)
                current = self._adapter.current_window(app)
                self._leases.validate_action(lease, obs, current,
                                             guard)
                raw = self._journal.run(jscope, call, guarded_work,
                                        guard)
        except _WorkFailed as e:
            self._leases.release(lease)
            self._approvals.request_cancelled(ticket)
            raise translate.IncompleteResponse(
                "outcome_unknown: explicit reconciliation required"
            ) from e
        except BaseException:
            self._approvals.request_cancelled(ticket)
            raise
        return _result_from(raw)
