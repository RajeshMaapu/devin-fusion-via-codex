"""ComputerBroker vertical-slice tests — fake adapter, fake humans."""

import binascii
import json
import pathlib
import struct
import tempfile
import threading
import time
import unittest
import zlib

from fusion_relay import artifacts, broker, leases, operations, translate
from fusion_relay.approvals import ApprovalManager
from fusion_relay.broker import ComputerBroker, ExecutionBinding


def make_png(w=2, h=2, color=6) -> bytes:
    def chunk(t, body):
        return (struct.pack(">I", len(body)) + t + body
                + struct.pack(">I",
                              binascii.crc32(t + body) & 0xFFFFFFFF))
    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, color,
                                     0, 0, 0))
    row = b"\x00" + b"\x10" * (w * (3 if color == 2 else 4))
    return (sig + ihdr + chunk(b"IDAT", zlib.compress(row * h))
            + chunk(b"IEND", b""))


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class _Ctx:
    def __init__(self):
        self.cancelled = False

    def check(self):
        if self.cancelled:
            raise PermissionError("cancelled")


class _Adapter:
    """Fake runtime adapter — no desktop anywhere near this."""

    def __init__(self, window="win-1"):
        self.window = window
        self.acts = []
        self.observes = 0
        self.act_entered = threading.Event()
        self.act_release = threading.Event()
        self.block_act = False
        self._lock = threading.Lock()
        self.context = None

    def bind_execution(self, binding, context):
        self.context = context
        context.check()

    def observe(self, app):
        with self._lock:
            self.observes += 1
        return {"window_identity": self.window,
                "text": f"text of {app}", "png": make_png()}

    def current_window(self, app):
        return self.window

    def act(self, app, action):
        with self._lock:
            self.acts.append((app, action))
        self.act_entered.set()
        if self.block_act:
            self.act_release.wait(10)


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = pathlib.Path(self._tmp.name)
        self.clock = _Clock()
        self.approvals = ApprovalManager(clock=self.clock)
        self.addCleanup(self.approvals.close)
        self.leases = leases.LeaseManager(clock=self.clock)
        self.artifacts = artifacts.ArtifactStore(root / "art")
        self.addCleanup(self.artifacts.close)
        self.journal = operations.OperationJournal(root / "ops.db")
        self.addCleanup(self.journal.close)
        self.adapter = _Adapter()
        self.broker = ComputerBroker(self.approvals, self.leases,
                                     self.artifacts, self.journal,
                                     self.adapter)
        self.binding = ExecutionBinding(
            principal="user-1", session="sess-1", role="lead",
            revision=1)
        self.cap = self.broker.register(self.binding)

    def _drive(self, fn, ctx, decision="allow_once"):
        """Run fn in a thread; auto-decide pending approvals as a fake
        human. Bounded by a real 5s deadline; ctx always cancelled and
        the thread joined on the way out. Returns (result, error)."""
        out = []

        def run():
            try:
                out.append(("ok", fn()))
            except Exception as e:
                out.append(("err", e))
        th = threading.Thread(target=run)
        th.start()
        deadline = time.monotonic() + 5
        try:
            while th.is_alive() and time.monotonic() < deadline:
                for p in self.approvals.pending():
                    if decision is not None:
                        self.approvals.decide(
                            p["request_id"], decision, p["revision"])
                th.join(0.02)
        finally:
            if th.is_alive():
                ctx.cancelled = True
            th.join(5)
        self.assertFalse(th.is_alive())
        return out[0] if out else ("err", RuntimeError("no result"))

    # -- registration ---------------------------------------------------

    def test_register_validates(self):
        for b in (ExecutionBinding("", "s", "lead", 1),
                  ExecutionBinding("p", "s", "model", 1),
                  ExecutionBinding("p", "s", "lead", 0)):
            with self.assertRaises(ValueError, msg=b):
                self.broker.register(b)

    def test_unknown_and_sidekick_denied_before_runtime(self):
        ctx = _Ctx()
        with self.assertRaises(PermissionError):
            self.broker.execute("nonexistent-cap", "op1",
                                "com.example.App", {"kind": "observe"},
                                ctx)
        sidekick = self.broker.register(ExecutionBinding(
            "user-1", "sess-1", "sidekick", 1))
        kind, err = self._drive(lambda: self.broker.execute(
            sidekick, "op2", "com.example.App", {"kind": "observe"},
            ctx), ctx, decision="allow_once")
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, PermissionError)
        self.assertEqual(self.adapter.observes, 0)
        self.assertEqual(self.adapter.acts, [])
        self.assertEqual(self.approvals.pending(), [])

    def test_unknown_action_no_approval(self):
        for action in ({"kind": "shell"}, {"kind": "eval"},
                       {"kind": "click", "observation_id": "o",
                        "x": "5", "y": 1},
                       {"kind": "click", "observation_id": "f" * 32,
                        "x": -1, "y": 1},
                       {"kind": "type", "observation_id": "f" * 32,
                        "text": "x" * 70000},
                       "observe"):
            with self.assertRaises(ValueError, msg=action):
                self.broker.execute(self.cap, "opx",
                                    "com.example.App", action, _Ctx())
        self.assertEqual(self.approvals.pending(), [])
        self.assertEqual(self.adapter.observes, 0)

    # -- happy path -------------------------------------------------------

    def test_observe_then_click_verified_and_delivered(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        self.assertEqual(res.status, "succeeded")
        obs_meta = json.loads(res.text_blocks[0])
        obs_id = obs_meta["observation_id"]
        self.assertTrue(obs_id)
        self.assertEqual(len(res.image_artifact_refs), 1)

        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-click", "com.example.App",
            {"kind": "click", "observation_id": obs_id,
             "x": 1, "y": 1}, ctx), ctx)
        self.assertEqual(kind, "ok")
        self.assertEqual(res.status, "succeeded")
        self.assertEqual(len(self.adapter.acts), 1)
        self.assertEqual(self.adapter.acts[0][1]["kind"], "click")
        self.assertEqual(self.adapter.acts[0][1]["x"], 1)

        # durable delivery: offer + ack, retry idempotent
        jscope = self.broker._journal_scope(self.binding)
        offer = self.journal.offer(jscope, "op-click", "devin-ui")
        self.assertEqual(offer["status"], "succeeded")
        offer2 = self.journal.offer(jscope, "op-click", "devin-ui")
        self.assertEqual(offer["delivery_id"], offer2["delivery_id"])
        self.assertEqual(
            len(self.journal.pending_deliveries(jscope, "devin-ui")), 1)
        self.assertTrue(self.journal.acknowledge(
            jscope, "devin-ui", offer["delivery_id"]))
        self.assertTrue(self.journal.acknowledge(
            jscope, "devin-ui", offer["delivery_id"]))  # idempotent
        self.assertEqual(
            self.journal.pending_deliveries(jscope, "devin-ui"), [])

    def test_replay_returns_persisted_without_action(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        n = self.adapter.observes
        kind, res2 = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        self.assertEqual(res2, res)
        self.assertEqual(self.adapter.observes, n)
        self.assertEqual(self.approvals.pending(), [])

    def test_observe_twice_live_and_expired(self):
        ctx = _Ctx()
        for opid in ("op-o1", "op-o2"):
            kind, res = self._drive(lambda o=opid: self.broker.execute(
                self.cap, o, "com.example.App",
                {"kind": "observe"}, ctx), ctx)
            self.assertEqual(kind, "ok", opid)
        self.assertEqual(self.adapter.observes, 2)
        # expired lease frees the scope for a fresh observe
        self.clock.advance(20)
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-o3", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        self.assertEqual(self.adapter.observes, 3)

    def test_fingerprint_conflict_rejected(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-x", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        # same operation id, different call -> conflict, no approval/act
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-x", "com.example.App",
            {"kind": "click", "observation_id": obs_id,
             "x": 1, "y": 1}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.UnsupportedRequest)
        self.assertEqual(self.adapter.acts, [])

    def test_mutation_during_approval_keeps_payload(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        action = {"kind": "click", "observation_id": obs_id,
                  "x": 1, "y": 1}
        out = []

        def run():
            try:
                out.append(self.broker.execute(
                    self.cap, "op-mut", "com.example.App", action,
                    ctx))
            except Exception as e:
                out.append(e)
        th = threading.Thread(target=run)
        th.start()
        deadline = time.monotonic() + 5
        decided = False
        try:
            while th.is_alive() and time.monotonic() < deadline:
                pending = self.approvals.pending()
                if pending:
                    # mutate after the ticket exists: the digest and the
                    # dispatched payload were already snapshotted
                    action["x"] = 99
                    for p in pending:
                        decided = self.approvals.decide(
                            p["request_id"], "allow_once",
                            p["revision"]) or decided
                th.join(0.02)
        finally:
            ctx.cancelled = True
            th.join(5)
        self.assertTrue(decided)
        self.assertFalse(th.is_alive())
        self.assertEqual(self.adapter.acts[0][1]["x"], 1)

    # -- failure modes ----------------------------------------------------

    def test_denied_zero_act(self):
        ctx = _Ctx()
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-deny", "com.example.App",
            {"kind": "observe"}, ctx), ctx, decision="deny")
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, PermissionError)
        self.assertEqual(self.adapter.observes, 0)

    def test_cancel_while_waiting(self):
        ctx = _Ctx()
        out = []

        def run():
            try:
                out.append(self.broker.execute(
                    self.cap, "op-cancel", "com.example.App",
                    {"kind": "observe"}, ctx))
            except Exception as e:
                out.append(e)
        th = threading.Thread(target=run)
        th.start()
        deadline = time.monotonic() + 5
        while not self.approvals.pending() \
                and time.monotonic() < deadline:
            time.sleep(0.01)
        ctx.cancelled = True
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertIsInstance(out[0], PermissionError)
        self.assertEqual(self.adapter.observes, 0)

    def test_stale_lease_blocks_click(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "ok")
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        self.clock.advance(20)  # lease (15s) and obs (5s) both stale
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-click", "com.example.App",
            {"kind": "click", "observation_id": obs_id,
             "x": 1, "y": 1}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, PermissionError)
        self.assertEqual(self.adapter.acts, [])

    def test_bad_coords_and_wrong_obs_zero_approval(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        cases = [
            ("com.example.App",
             {"kind": "click", "observation_id": obs_id,
              "x": 2, "y": 1}),                     # x >= width 2
            ("com.example.App",
             {"kind": "click", "observation_id": "f" * 32,
              "x": 0, "y": 0}),                     # obs not current
            ("com.example.Other",                   # not observed app
             {"kind": "click", "observation_id": obs_id,
              "x": 0, "y": 0}),
        ]
        for i, (app, action) in enumerate(cases):
            with self.assertRaises(PermissionError, msg=i):
                self.broker.execute(self.cap, f"op-bad-{i}",
                                    app, action, ctx)
        self.assertEqual(self.approvals.pending(), [])
        self.assertEqual(self.adapter.acts, [])

    def test_revoked_capability_and_role_spoof(self):
        ctx = _Ctx()
        self.broker.revoke(self.cap)
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, PermissionError)
        # a model-supplied "role" in the action is not trusted — rejected
        with self.assertRaises(ValueError):
            self.broker.execute(
                self.broker.register(self.binding), "op-r",
                "com.example.App",
                {"kind": "observe", "role": "lead"}, ctx)

    def test_revoke_during_act_no_verify_and_row_unknown(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        self.adapter.block_act = True
        out = []

        def run():
            try:
                out.append(self.broker.execute(
                    self.cap, "op-act", "com.example.App",
                    {"kind": "click", "observation_id": obs_id,
                     "x": 0, "y": 0}, ctx))
            except Exception as e:
                out.append(e)
        th = threading.Thread(target=run)
        th.start()
        deadline = time.monotonic() + 5
        while not self.approvals.pending() \
                and time.monotonic() < deadline:
            time.sleep(0.01)
        for p in self.approvals.pending():
            self.approvals.decide(p["request_id"], "allow_once",
                                  p["revision"])
        self.assertTrue(self.adapter.act_entered.wait(5))
        self.broker.revoke(self.cap)   # act already dispatched
        self.adapter.act_release.set()
        th.join(5)
        self.assertFalse(th.is_alive())
        self.assertIsInstance(out[0], translate.IncompleteResponse)
        self.assertEqual(len(self.adapter.acts), 1)
        self.assertEqual(self.adapter.observes, 1)  # no verify observe
        row = self.journal.lookup(
            self.broker._journal_scope(self.binding), "op-act")
        self.assertEqual(row["status"], "outcome_unknown")
        # no automatic retry: replay of the same call fails explicit
        kind, err = self._drive(lambda: self.broker.execute(
            self.broker.register(self.binding), "op-act",
            "com.example.App",
            {"kind": "click", "observation_id": obs_id,
             "x": 0, "y": 0}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.IncompleteResponse)
        self.assertEqual(len(self.adapter.acts), 1)

    def test_changed_window_blocks_act(self):
        ctx = _Ctx()
        kind, res = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        obs_id = json.loads(res.text_blocks[0])["observation_id"]
        self.adapter.window = "win-other"  # window changed
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-click", "com.example.App",
            {"kind": "click", "observation_id": obs_id,
             "x": 1, "y": 1}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, PermissionError)
        self.assertEqual(self.adapter.acts, [])

    def test_cancelled_binding_lock_wait(self):
        ctx1, ctx2 = _Ctx(), _Ctx()
        entered = threading.Event()
        release = threading.Event()

        def first():
            with self.broker._entry(self.cap)["lock"]:
                entered.set()
                release.wait(10)
        t1 = threading.Thread(target=first)
        t1.start()
        self.assertTrue(entered.wait(5))
        out = []

        def second():
            try:
                out.append(self.broker.execute(
                    self.cap, "op-wait", "com.example.App",
                    {"kind": "observe"}, ctx2))
            except Exception as e:
                out.append(e)
        t2 = threading.Thread(target=second)
        t2.start()
        time.sleep(0.2)          # second is blocked on the lock
        ctx2.cancelled = True
        t2.join(5)
        self.assertFalse(t2.is_alive())
        self.assertIsInstance(out[0], PermissionError)
        release.set()
        t1.join(5)
        self.assertEqual(self.adapter.observes, 0)

    def test_adapter_binding_isolation(self):
        self.assertTrue(self.broker.register(self.binding))
        for other in (ExecutionBinding("user-2", "sess-1", "lead", 1),
                      ExecutionBinding("user-1", "sess-2", "lead", 1),
                      ExecutionBinding("user-1", "sess-1", "lead", 2)):
            with self.assertRaises(PermissionError, msg=other):
                self.broker.register(other)
        sk = self.broker.register(
            ExecutionBinding("user-1", "sess-1", "sidekick", 1))
        self.assertTrue(sk)

    def test_adapter_without_bind_execution_fails(self):
        self.adapter.bind_execution = None
        ctx = _Ctx()
        kind, err = self._drive(lambda: self.broker.execute(
            self.cap, "op-obs", "com.example.App",
            {"kind": "observe"}, ctx), ctx)
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.IncompleteResponse)
        self.assertEqual(self.adapter.observes, 0)

    def _blocking_observe(self):
        entered = threading.Event()

        def observe(app):
            entered.set()
            while True:
                self.adapter.context.check()
                time.sleep(0.01)
        self.adapter.observe = observe
        return entered

    def _drive_reactive(self, ctx, entered, react):
        out = []

        def run():
            try:
                out.append(("ok", self.broker.execute(
                    self.cap, "op-obs", "com.example.App",
                    {"kind": "observe"}, ctx)))
            except Exception as e:
                out.append(("err", e))
        th = threading.Thread(target=run)
        th.start()
        fired = [False]
        deadline = time.monotonic() + 5
        try:
            while th.is_alive() and time.monotonic() < deadline:
                for p in self.approvals.pending():
                    self.approvals.decide(p["request_id"],
                                          "allow_once", p["revision"])
                if entered.is_set() and not fired[0]:
                    fired[0] = True
                    react()
                th.join(0.02)
        finally:
            if th.is_alive():
                ctx.cancelled = True
            th.join(5)
        self.assertFalse(th.is_alive())
        return out[0] if out else ("err", RuntimeError("no result"))

    def _assert_unknown_no_artifacts(self):
        row = self.journal.lookup(
            self.broker._journal_scope(self.binding), "op-obs")
        self.assertEqual(row["status"], "outcome_unknown")
        art = pathlib.Path(self._tmp.name) / "art"
        self.assertEqual(list(art.glob("*.png")), [])

    def test_cancel_during_active_observation(self):
        entered = self._blocking_observe()
        ctx = _Ctx()
        kind, err = self._drive_reactive(
            ctx, entered, lambda: setattr(ctx, "cancelled", True))
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.IncompleteResponse)
        self.assertEqual(self.adapter.observes, 0)
        self._assert_unknown_no_artifacts()

    def test_revoke_during_active_observation(self):
        entered = self._blocking_observe()
        ctx = _Ctx()
        kind, err = self._drive_reactive(
            ctx, entered, lambda: self.approvals.revoke(
                self.binding.session, "com.example.App"))
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.IncompleteResponse)
        self.assertEqual(self.adapter.observes, 0)
        self._assert_unknown_no_artifacts()

    def test_lease_expiry_during_active_observation(self):
        entered = self._blocking_observe()
        ctx = _Ctx()
        kind, err = self._drive_reactive(
            ctx, entered, lambda: self.clock.advance(31))
        self.assertEqual(kind, "err")
        self.assertIsInstance(err, translate.IncompleteResponse)
        self.assertEqual(self.adapter.observes, 0)
        self._assert_unknown_no_artifacts()


if __name__ == "__main__":
    unittest.main()
