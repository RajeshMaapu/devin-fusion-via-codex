"""ApprovalManager unit tests — in-memory, fake clock, no I/O."""

import hashlib
import threading
import unittest
from dataclasses import replace

from fusion_relay.approvals import (
    ApprovalManager, ApprovalScope, ApprovalTicket)


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def _scope(**kw) -> ApprovalScope:
    base = dict(
        principal="user-1", session="sess-1", operation_id="op-1",
        app="com.example.App", capability="observe",
        binding_revision=1,
        action_digest=hashlib.sha256(b"a").hexdigest())
    base.update(kw)
    return ApprovalScope(**base)


def _req(mgr, **kw):
    return mgr.request(_scope(**{k: v for k, v in kw.items()
                                 if k in ApprovalScope.__dataclass_fields__}),
                       task=kw.get("task", "Do the thing"),
                       action_summary=kw.get("summary", "Read a window"),
                       ttl=kw.get("ttl", 120))


class ScopeValidationTest(unittest.TestCase):
    def setUp(self):
        self.mgr = ApprovalManager(clock=_Clock())

    def test_bad_scopes_rejected(self):
        bad = [dict(principal=""), dict(session=""),
               dict(operation_id=""), dict(app="notadottedid"),
               dict(app=""), dict(capability="admin"),
               dict(capability=""), dict(binding_revision=0),
               dict(binding_revision=-2), dict(binding_revision=True),
               dict(binding_revision="1"),
               dict(action_digest="zz" * 32), dict(action_digest=""),
               dict(action_digest="A" * 64)]
        for kw in bad:
            with self.assertRaises(ValueError, msg=kw):
                self.mgr.request(_scope(**kw), "t", "s")

    def test_label_bounds(self):
        with self.assertRaises(ValueError):
            self.mgr.request(_scope(), "x" * 4097, "s")
        with self.assertRaises(ValueError):
            self.mgr.request(_scope(), "t", "x" * 513)
        with self.assertRaises(ValueError):
            self.mgr.request(_scope(), "", "s")

    def test_ttl_clamped(self):
        lo = _req(self.mgr, ttl=0)
        hi = _req(self.mgr, ttl=99999)
        now = self.mgr._clock()
        self.assertAlmostEqual(lo.expires_at - now, 1.0)
        self.assertAlmostEqual(hi.expires_at - now, 120.0)

    def test_ttl_nonfinite_rejected(self):
        for bad in (float("nan"), float("inf"), -float("inf"),
                    True, "5"):
            with self.assertRaises(ValueError, msg=bad):
                _req(self.mgr, ttl=bad)

    def test_max_pending_validated(self):
        for bad in (0, -1, True, 2.5, "4"):
            with self.assertRaises(ValueError, msg=bad):
                ApprovalManager(max_pending=bad)

    def test_name_length_bounds(self):
        for field, limit in (("principal", 512), ("session", 512),
                             ("operation_id", 512), ("app", 255)):
            kw = {field: ("a." if field == "app" else "a")
                  + "x" * limit}
            with self.assertRaises(ValueError, msg=field):
                self.mgr.request(_scope(**kw), "t", "s")


class RequestDecideTest(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.mgr = ApprovalManager(clock=self.clock)

    def test_pending_snapshot_fields(self):
        t = _req(self.mgr)
        [p] = self.mgr.pending()
        self.assertEqual(p["request_id"], t.request_id)
        self.assertEqual(p["app"], "com.example.App")
        self.assertEqual(p["capability"], "observe")
        self.assertEqual(p["action_digest"],
                         _scope().action_digest)
        self.assertEqual(p["revision"], 1)

    def test_decide_allow_once_and_wait(self):
        t = _req(self.mgr)
        out = []
        th = threading.Thread(
            target=lambda: out.append(self.mgr.wait(t)))
        th.start()
        self.assertTrue(
            self.mgr.decide(t.request_id, "allow_once", 1))
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertEqual(out, ["allow_once"])
        self.assertEqual(self.mgr.pending(), [])

    @staticmethod
    def _waiter(mgr, ticket, out):
        def run():
            try:
                out.append(mgr.wait(ticket))
            except Exception as e:
                out.append(e)
        return threading.Thread(target=run)

    def test_deny_wakes_waiter(self):
        t = _req(self.mgr)
        out = []
        th = self._waiter(self.mgr, t, out)
        th.start()
        self.assertTrue(self.mgr.decide(t.request_id, "deny", 1))
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], PermissionError)

    def test_decide_unknown_and_terminal_ids(self):
        t = _req(self.mgr)
        self.assertFalse(self.mgr.decide("nonexistent", "allow_once", 1))
        self.assertTrue(self.mgr.decide(t.request_id, "deny", 1))
        self.assertFalse(self.mgr.decide(t.request_id, "allow_once", 1))

    def test_decide_bad_decision_value(self):
        t = _req(self.mgr)
        with self.assertRaises(ValueError):
            self.mgr.decide(t.request_id, "allow_always", 1)

    def test_stale_revision_rejected(self):
        t = _req(self.mgr)
        self.assertFalse(self.mgr.decide(t.request_id, "allow_once", 99))
        self.assertFalse(self.mgr.decide(t.request_id, "allow_once", 0))
        self.assertEqual(len(self.mgr.pending()), 1)  # still pending
        self.assertTrue(self.mgr.decide(t.request_id, "allow_once", 1))

    def test_expiry(self):
        t = _req(self.mgr, ttl=5)
        self.clock.advance(6)
        self.assertEqual(self.mgr.pending(), [])
        self.assertFalse(self.mgr.decide(t.request_id, "allow_once", 1))
        with self.assertRaises(PermissionError):
            self.mgr.wait(t)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t)

    def test_two_simultaneous_tickets_independent(self):
        a = _req(self.mgr, operation_id="op-a")
        b = _req(self.mgr, operation_id="op-b")
        self.assertNotEqual(a.request_id, b.request_id)
        self.assertEqual(len(self.mgr.pending()), 2)
        self.assertTrue(self.mgr.decide(a.request_id, "deny", 1))
        self.assertTrue(self.mgr.decide(b.request_id, "allow_once", 1))
        self.mgr.authorize(b)

    def test_cancel_callback(self):
        t = _req(self.mgr)
        with self.assertRaises(PermissionError):
            self.mgr.wait(t, check_cancelled=lambda: True)

    def test_cancel_callback_raising(self):
        t = _req(self.mgr)

        def boom():
            raise RuntimeError("gone")
        with self.assertRaises(PermissionError):
            self.mgr.wait(t, check_cancelled=boom)

    def test_capacity_bounded(self):
        mgr = ApprovalManager(clock=_Clock(), max_pending=2)
        _req(mgr, operation_id="a")
        _req(mgr, operation_id="b")
        with self.assertRaises(RuntimeError):
            _req(mgr, operation_id="c")

    def test_capacity_recovers_after_expiry(self):
        clock = _Clock()
        mgr = ApprovalManager(clock=clock, max_pending=2)
        _req(mgr, operation_id="a", ttl=5)
        _req(mgr, operation_id="b", ttl=5)
        clock.advance(10)
        # pruned on request — no wait/decide needed
        t = _req(mgr, operation_id="c")
        self.assertIn(t.request_id,
                      [p["request_id"] for p in mgr.pending()])

    def test_approved_tickets_count_toward_capacity(self):
        mgr = ApprovalManager(clock=_Clock(), max_pending=2)
        a = _req(mgr, operation_id="a")
        mgr.decide(a.request_id, "allow_task", 1)  # approved, not pending
        _req(mgr, operation_id="b")               # auto-approved ticket
        with self.assertRaises(RuntimeError):
            _req(mgr, operation_id="c")


class AuthorizeTest(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.mgr = ApprovalManager(clock=self.clock)

    def _approved(self, decision="allow_once", **kw):
        t = _req(self.mgr, **kw)
        self.assertTrue(self.mgr.decide(t.request_id, decision, 1))
        return t

    def test_allow_once_consume_exactly_once(self):
        t = self._approved()
        self.mgr.authorize(t)              # no consume: not exhausted
        self.mgr.authorize(t)
        self.mgr.authorize(t, consume=True)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t, consume=True)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t)

    def test_check_revision_consumed_revoked_closed(self):
        t = self._approved()
        self.mgr.authorize(t, consume=True)
        self.mgr.check_revision(t)
        self.mgr.revoke("sess-1", "com.example.App")
        with self.assertRaises(PermissionError):
            self.mgr.check_revision(t)
        mgr2 = ApprovalManager(clock=self.clock)
        t2 = mgr2.request(_scope(), "t", "s")
        mgr2.close()
        with self.assertRaises(PermissionError):
            mgr2.check_revision(t2)

    def test_check_revision_session_wide_revoke(self):
        t = self._approved()
        self.mgr.authorize(t, consume=True)
        self.mgr.check_revision(t)
        self.mgr.revoke("sess-1")
        with self.assertRaises(PermissionError):
            self.mgr.check_revision(t)

    def test_forged_and_swapped_tickets_fail(self):
        t = self._approved()
        forged = replace(t, scope=_scope(operation_id="op-evil"))
        with self.assertRaises(PermissionError):
            self.mgr.authorize(forged)
        other = self._approved(operation_id="op-2")
        swapped = replace(t, request_id=other.request_id)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(swapped)
        # the real tickets are still usable
        self.mgr.authorize(t)
        self.mgr.authorize(other)

    def test_task_grant_auto_approves_same_binding(self):
        t = self._approved("allow_task")
        self.mgr.authorize(t, consume=True)
        t2 = _req(self.mgr, operation_id="op-2")
        self.assertEqual(self.mgr.wait(t2), "allow_task")  # auto
        self.assertEqual(self.mgr.pending(), [])
        self.mgr.authorize(t2)

    def test_task_grant_scope_binding_enforced(self):
        self._approved("allow_task")
        digest = _scope().action_digest
        for kw in (dict(session="sess-2"), dict(principal="user-2"),
                   dict(app="com.example.Other"),
                   dict(capability="act"), dict(binding_revision=2)):
            t = _req(self.mgr, operation_id="x", **kw)
            # not auto-approved -> remains pending
            self.assertIn(t.request_id,
                          [p["request_id"] for p in self.mgr.pending()],
                          msg=kw)
        # same binding except a different digest still matches the grant
        # (grant binds principal/session/app/capability/revision)
        t = _req(self.mgr, operation_id="x2",
                 action_digest=hashlib.sha256(b"other").hexdigest())
        self.assertEqual(self.mgr.wait(t), "allow_task")
        self.assertNotEqual(t.scope.action_digest, digest)

    def test_act_cannot_reuse_observe_approval(self):
        self._approved("allow_task")
        act = _req(self.mgr, operation_id="op-act", capability="act")
        self.assertEqual(
            [p["request_id"] for p in self.mgr.pending()],
            [act.request_id])

    def test_task_grant_expires(self):
        self._approved("allow_task")
        self.clock.advance(301)  # past 5-minute grant ttl
        t = _req(self.mgr, operation_id="late")
        self.assertEqual(
            [p["request_id"] for p in self.mgr.pending()],
            [t.request_id])

    def test_allow_once_bound_to_operation(self):
        t = self._approved("allow_once", operation_id="op-1")
        self.mgr.authorize(t, consume=True)
        # a new ticket for a different op is a fresh prompt
        t2 = _req(self.mgr, operation_id="op-9")
        self.assertEqual(
            [p["request_id"] for p in self.mgr.pending()],
            [t2.request_id])

    def test_task_consume_once_then_fresh_autoapproved(self):
        t = self._approved("allow_task")
        self.mgr.authorize(t, consume=True)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t, consume=True)
        # grant survives ticket retirement: next op auto-approves
        t2 = _req(self.mgr, operation_id="op-2")
        self.assertEqual(self.mgr.wait(t2), "allow_task")
        self.mgr.authorize(t2, consume=True)

    def test_wait_after_expiry_on_approved_rejects(self):
        t = self._approved("allow_once", ttl=5)
        self.clock.advance(10)
        with self.assertRaises(PermissionError):
            self.mgr.wait(t)

    def test_wait_on_cancelled_ticket_rejects(self):
        t = _req(self.mgr)
        self.mgr.request_cancelled(t)
        with self.assertRaises(PermissionError):
            self.mgr.wait(t)

    def test_observe_and_act_grants_coexist(self):
        self._approved("allow_task")
        self._approved("allow_task", operation_id="op-act",
                       capability="act")
        grants = self.mgr.grants()
        caps = sorted(g["capability"] for g in grants)
        self.assertEqual(caps, ["act", "observe"])
        for g in grants:
            self.assertEqual(g["session"], "sess-1")
            self.assertGreater(g["remaining_s"], 0)
            self.assertLessEqual(g["remaining_s"], 300)

    def test_grants_revoked_after_accept(self):
        self._approved("allow_task")
        self.assertEqual(len(self.mgr.grants()), 1)
        self.mgr.revoke("sess-1", "com.example.App")
        self.assertEqual(self.mgr.grants(), [])
        t = _req(self.mgr, operation_id="again")
        self.assertEqual(
            [p["request_id"] for p in self.mgr.pending()],
            [t.request_id])


class RevokeCloseTest(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.mgr = ApprovalManager(clock=self.clock)

    def test_revoke_invalidates_pending_and_grants(self):
        t = _req(self.mgr)
        self.assertTrue(self.mgr.decide(t.request_id, "allow_task", 1))
        t2 = _req(self.mgr, operation_id="op-2")  # auto-approved
        t3 = _req(self.mgr, operation_id="op-3", app="com.example.B")
        self.mgr.revoke("sess-1")
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t2)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t3)
        # new request after revoke is a fresh prompt (grant gone)
        t4 = _req(self.mgr, operation_id="op-4")
        self.assertEqual(
            [p["request_id"] for p in self.mgr.pending()],
            [t4.request_id])
        self.assertEqual(t4.revision, 2)

    def test_revoke_scoped_to_app(self):
        ta = _req(self.mgr, app="com.example.A", operation_id="a")
        tb = _req(self.mgr, app="com.example.B", operation_id="b")
        self.mgr.revoke("sess-1", "com.example.A")
        remaining = [p["request_id"] for p in self.mgr.pending()]
        self.assertEqual(remaining, [tb.request_id])
        self.assertEqual(ta.scope.app, "com.example.A")
        # stale revision decide now fails for the revoked app
        self.assertFalse(
            self.mgr.decide(ta.request_id, "allow_once", 1))

    def test_revoke_wakes_waiter(self):
        t = _req(self.mgr)
        out = []
        th = RequestDecideTest._waiter(self.mgr, t, out)
        th.start()
        self.mgr.revoke("sess-1")
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], PermissionError)

    def test_close_cancels_everything(self):
        t = _req(self.mgr)
        out = []
        th = RequestDecideTest._waiter(self.mgr, t, out)
        th.start()
        self.mgr.close()
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], PermissionError)
        with self.assertRaises(PermissionError):
            self.mgr.authorize(t)
        with self.assertRaises(PermissionError):
            _req(self.mgr)


if __name__ == "__main__":
    unittest.main()
