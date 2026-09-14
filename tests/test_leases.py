"""LeaseManager tests — Events for determinism, fake clock for ageing."""

import threading
import time
import unittest
from dataclasses import replace

from fusion_relay.leases import (DesktopLease, LeaseError, LeaseManager)


class _Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class LeaseTest(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.lm = LeaseManager(clock=self.clock)

    def test_acquire_observe_validate_release(self):
        lease = self.lm.acquire("scope-a")
        obs = self.lm.observe(lease, "win-1")
        self.lm.validate_action(lease, obs, "win-1")
        self.lm.release(lease)
        with self.assertRaises(LeaseError):
            self.lm.check(lease)

    def test_reenter_and_forged_rejected(self):
        lease = self.lm.acquire("scope-a")
        with self.assertRaises(LeaseError):
            self.lm.acquire("scope-a")  # no reenter while held
        forged = replace(lease, generation=lease.generation + 9)
        with self.assertRaises(LeaseError):
            self.lm.check(forged)
        other = DesktopLease("x" * 32, "scope-a", 1, self.clock() + 5)
        with self.assertRaises(LeaseError):
            self.lm.check(other)
        self.lm.release(lease)

    def test_fifo_order(self):
        first = self.lm.acquire("scope-a")
        order = []

        def contender(scope):
            l = self.lm.acquire(scope, wait_timeout=10)
            order.append(scope)
            self.lm.release(l)
        t1 = threading.Thread(target=contender, args=("scope-b",))
        t2 = threading.Thread(target=contender, args=("scope-c",))
        t1.start()
        time.sleep(0.05)  # ensure b queues first
        t2.start()
        self.lm.release(first)
        t1.join(10)
        t2.join(10)
        self.assertEqual(order, ["scope-b", "scope-c"])

    def test_wait_timeout(self):
        self.lm.acquire("scope-a")
        with self.assertRaises(LeaseError):
            self.lm.acquire("scope-b", wait_timeout=0.2)

    def test_queue_bound(self):
        self.lm.acquire("scope-a")
        cancel = threading.Event()
        outcomes, unexpected = [], []

        def wait(i):
            try:
                self.lm.acquire(f"s{i}",
                                check=lambda: cancel.is_set(),
                                wait_timeout=30)
                unexpected.append("acquired")
            except LeaseError:
                outcomes.append("cancelled")
            except Exception as e:
                unexpected.append(e)
        ts = [threading.Thread(target=wait, args=(i,))
              for i in range(8)]
        for t in ts:
            t.start()
        time.sleep(0.2)  # let all eight queue
        with self.assertRaises(LeaseError):
            self.lm.acquire("scope-full", wait_timeout=0.05)
        cancel.set()
        for t in ts:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in ts))
        self.assertEqual(sorted(outcomes), ["cancelled"] * 8)
        self.assertFalse(unexpected)
        self.lm.release(self.lm._slot["lease"])

    def test_expired_lease_frees_slot(self):
        lease = self.lm.acquire("scope-a", duration=5)
        self.clock.advance(10)
        with self.assertRaises(LeaseError):
            self.lm.check(lease)
        nxt = self.lm.acquire("scope-b")
        self.assertEqual(nxt.scope, "scope-b")
        self.assertGreater(nxt.generation, lease.generation)
        self.lm.release(nxt)

    def test_revoke_owner(self):
        lease = self.lm.acquire("scope-a")
        self.lm.revoke("scope-a")
        with self.assertRaises(LeaseError):
            self.lm.check(lease)
        nxt = self.lm.acquire("scope-b")
        self.lm.release(nxt)

    def test_changed_window_and_stale_obs(self):
        lease = self.lm.acquire("scope-a")
        obs = self.lm.observe(lease, "win-1")
        with self.assertRaises(LeaseError):
            self.lm.validate_action(lease, obs, "win-2")
        self.clock.advance(6)  # obs older than 5s
        with self.assertRaises(LeaseError):
            self.lm.validate_action(lease, obs, "win-1")
        with self.assertRaises(LeaseError):
            self.lm.validate_action(lease, "f" * 32, "win-1")
        self.lm.release(lease)

    def test_cancel_while_queued(self):
        self.lm.acquire("scope-a")
        with self.assertRaises(LeaseError):
            self.lm.acquire("scope-b", check=lambda: True,
                            wait_timeout=10)

    def test_inflight_revoked_blocks_reassignment(self):
        lease = self.lm.acquire("scope-a")
        obs = self.lm.observe(lease, "win-1")
        entered = threading.Event()
        done = threading.Event()
        order = []

        def slow_op():
            with self.lm.action(lease, obs, "win-1"):
                entered.set()
                done.wait(10)
                order.append("old-finishes")
        t = threading.Thread(target=slow_op)
        t.start()
        self.assertTrue(entered.wait(5))
        self.lm.revoke("scope-a")  # in-flight: slot must not reassign
        got = []
        t2 = threading.Thread(
            target=lambda: got.append(
                self.lm.acquire("scope-b", wait_timeout=10)))
        t2.start()
        time.sleep(0.1)
        self.assertEqual(got, [])  # still waiting while op in flight
        done.set()
        t.join(10)
        t2.join(10)
        self.assertEqual(len(got), 1)
        order.append("new-acquired")
        self.assertEqual(order, ["old-finishes", "new-acquired"])
        self.lm.release(got[0])

    def test_duration_clamped(self):
        lease = self.lm.acquire("scope-a", duration=9999)
        self.assertLessEqual(lease.expires_at - self.clock(), 30)
        self.lm.release(lease)

    def test_same_lease_actions_serialize(self):
        lease = self.lm.acquire("scope-a")
        obs = self.lm.observe(lease, "win-1")
        first_in = threading.Event()
        release_first = threading.Event()
        overlap = []

        def op(mark, gate=None):
            with self.lm.action(lease, obs, "win-1"):
                overlap.append(mark + "-enter")
                if mark == "first":
                    first_in.set()
                    release_first.wait(10)
                overlap.append(mark + "-exit")
        t1 = threading.Thread(target=op, args=("first",))
        t2 = threading.Thread(target=op, args=("second",))
        t1.start()
        self.assertTrue(first_in.wait(5))
        t2.start()
        time.sleep(0.2)  # second must still be waiting
        self.assertEqual(overlap, ["first-enter"])
        release_first.set()
        t1.join(10)
        t2.join(10)
        self.assertEqual(overlap, ["first-enter", "first-exit",
                                   "second-enter", "second-exit"])
        self.lm.release(lease)

    def test_release_in_flight_blocks_replacement(self):
        lease = self.lm.acquire("scope-a")
        entered = threading.Event()
        done = threading.Event()

        def slow():
            with self.lm.action(lease):
                entered.set()
                done.wait(10)
        t = threading.Thread(target=slow)
        t.start()
        self.assertTrue(entered.wait(5))
        self.lm.release(lease)  # dead while in flight; slot retained
        got = []
        t2 = threading.Thread(
            target=lambda: got.append(
                self.lm.acquire("scope-b", wait_timeout=10)))
        t2.start()
        time.sleep(0.1)
        self.assertEqual(got, [])
        done.set()
        t.join(10)
        t2.join(10)
        self.assertEqual(len(got), 1)
        self.lm.release(got[0])

    def test_revoke_cancels_queued_waiter(self):
        self.lm.acquire("scope-a")
        out = []

        def waiter():
            try:
                self.lm.acquire("scope-b", wait_timeout=30)
                out.append("acquired")
            except LeaseError:
                out.append("revoked")
        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)  # queued
        self.lm.revoke("scope-b")
        t.join(10)
        self.assertEqual(out, ["revoked"])

    def test_cancel_with_free_slot(self):
        with self.assertRaises(LeaseError):
            self.lm.acquire("scope-x", check=lambda: True)
        self.assertIsNone(self.lm._slot)

    def test_expired_same_scope_reacquire(self):
        first = self.lm.acquire("scope-a", duration=5)
        self.clock.advance(10)
        nxt = self.lm.acquire("scope-a")
        self.assertNotEqual(nxt.lease_id, first.lease_id)
        self.lm.release(nxt)

    def test_action_entry_cancelled_no_body(self):
        from fusion_relay.lifecycle import RequestCancelled
        lease = self.lm.acquire("scope-a")
        entered = []

        def raising():
            raise RequestCancelled("cancelled")
        for check in (lambda: True, raising):
            try:
                with self.lm.action(lease, check=check):
                    entered.append(True)
            except LeaseError:
                pass
        self.assertEqual(entered, [])
        self.lm.release(lease)

    def test_bad_durations(self):
        for bad in (0, -1, float("nan"), float("inf"), True, "x", 31):
            with self.assertRaises(ValueError, msg=bad):
                LeaseManager(clock=self.clock, max_duration=bad)
        for bad in (-1, float("nan"), float("inf"), True, "x", 31):
            with self.assertRaises(ValueError, msg=bad):
                self.lm.acquire("scope-w", wait_timeout=bad)
        with self.assertRaises(ValueError):
            self.lm.acquire("scope-w", duration=float("nan"))


if __name__ == "__main__":
    unittest.main()
