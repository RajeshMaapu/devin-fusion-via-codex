"""CuaRuntimeAdapter tests — fake JSON-RPC child, no real desktop."""

import base64
import binascii
import json
import os
import pathlib
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zlib

from fusion_relay.approvals import (ApprovalManager, ApprovalScope)
from fusion_relay.broker import ExecutionBinding
from fusion_relay.runtime import (CompatibilityError, CuaRuntimeAdapter,
                                  RuntimeUnavailable)


def make_png(w=2, h=2) -> bytes:
    def chunk(t, body):
        return (struct.pack(">I", len(body)) + t + body
                + struct.pack(">I",
                              binascii.crc32(t + body) & 0xFFFFFFFF))
    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0))
    row = b"\x00" + b"\x10" * (w * 4)
    return (sig + ihdr + chunk(b"IDAT", zlib.compress(row * h))
            + chunk(b"IEND", b""))


FAKE = r'''
import json, os, sys, time

MODE, PNG, OUTDIR = sys.argv[1], sys.argv[2], sys.argv[3]
if MODE == "sigterm":
    import signal
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
if MODE == "orphan":
    pid = os.fork()
    if pid == 0:
        os.close(0)
        os.close(1)  # release stdout so parent exit yields EOF
        time.sleep(120)
        os._exit(0)
    with open(os.path.join(OUTDIR, "grandchild.pid"), "w") as f:
        f.write(str(pid))
APP = "com.example.FusionRelayFixture"
DOCS = "## Computer Use\nfixture docs block"
CALLS = os.path.join(OUTDIR, "calls.jsonl")

def send(o):
    sys.stdout.write(json.dumps(o) + "\n")
    sys.stdout.flush()

def record_call(code):
    with open(CALLS, "a") as f:
        f.write(json.dumps(code) + "\n")

def elicit():
    app = "com.example.OtherApp" if MODE == "wrongapp" else APP
    meta = {"codex_approval_kind": "mcp_tool_call",
            "connector_id": "computer-use",
            "connector_name": "Computer Use",
            "persist": ["session", "always"],
            "progressToken": 0, "riskLevel": "low",
            "tool_name": "get_app_state",
            "tool_params": {"app": app},
            "tool_params_display": [{"display_name": "App",
                                     "name": "app",
                                     "value": "Fixture"}]}
    if MODE == "extraelicit":
        meta["surprise"] = True
    send({"jsonrpc": "2.0", "id": 900, "method": "elicitation/create",
          "params": {"requestedSchema": {"type": "object",
                                         "properties": {}},
                     "_meta": meta,
                     "message": 'Allow Computer Use to use "Fixture"?'}})

def surface(app=APP):
    return {"codex/toolSurface": {"app": {"appId": app,
                                          "kind": "appId"},
                                  "kind": "computerUse"}}

def await_elicit(mid):
    while True:
        r = json.loads(sys.stdin.readline())
        if r.get("id") != 900:
            continue
        # wire-level record of exactly what the client sent back
        with open(os.path.join(OUTDIR, "elicit_result.json"), "w") as f:
            json.dump(r, f)
        if "error" in r or (r.get("result") or {}).get(
                "action") != "accept":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "isError": True,
                "content": [{"type": "text",
                             "text": "not approved"},
                            {"type": "text", "text": DOCS}]}})
            return
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "content": [{"type": "text", "text": DOCS},
                        {"type": "text", "text": "AX: fixture bound"}],
            "_meta": surface()}})
        return

while True:
    raw = sys.stdin.readline()
    if not raw:
        break
    try:
        msg = json.loads(raw)
    except ValueError:
        continue
    mid, method = msg.get("id"), msg.get("method")
    if method == "initialize":
        if MODE == "badinit":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": "1999-01-01",
                "serverInfo": {"name": "other", "version": "0"}}})
        else:
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": "2025-03-26",
                "serverInfo": {"name": "rmcp", "version": "1.5.0"},
                "capabilities": {}, "instructions": "fake runtime"}})
    elif method == "tools/list":
        if MODE == "badtools":
            tools = [{"name": "js", "inputSchema": {"type": "object"}},
                     {"name": "js_reset",
                      "inputSchema": {"type": "object"}}]
        else:
            tools = [
                {"name": "js", "description": "run js",
                 "inputSchema": {
                     "type": "object", "additionalProperties": False,
                     "required": ["code"],
                     "properties": {
                         "code": {"type": "string"},
                         "timeout_ms": {"type": "integer",
                                        "minimum": 1},
                         "title": {"type": "string", "minLength": 1,
                                   "maxLength": 80}}}},
                {"name": "js_reset", "inputSchema": {"type": "object"}},
                {"name": "turn_ended",
                 "inputSchema": {"type": "object"}}]
        send({"jsonrpc": "2.0", "id": mid, "result": {"tools": tools}})
    elif method == "tools/call":
        code = msg["params"]["arguments"]["code"]
        record_call(code)
        if MODE in ("hang", "sigterm"):
            time.sleep(120)
            continue
        if MODE == "orphan":
            os._exit(0)  # die mid-call; grandchild stays in the group
        if "cua.getApp" in code:
            elicit()
            await_elicit(mid)
            continue
        if MODE == "garbage":
            sys.stdout.write("THIS IS NOT JSON\n")
            sys.stdout.flush()
            continue
        meta = surface("com.example.OtherApp") if MODE == "badsurface" \
            else surface()
        if "getAXStateAndScreenshot" in code:
            if MODE == "badimg":
                blocks = [{"type": "text", "text": "AX: fixture state"},
                          {"type": "image", "mimeType": "image/png",
                           "data": "not-base64!!!"}]
            elif MODE == "badmime":
                blocks = [{"type": "text", "text": "AX: fixture state"},
                          {"type": "image", "mimeType": "image/jpeg",
                           "data": PNG}]
            elif MODE == "twoimg":
                blocks = [{"type": "text", "text": "AX: fixture state"},
                          {"type": "image", "mimeType": "image/png",
                           "data": PNG},
                          {"type": "image", "mimeType": "image/png",
                           "data": PNG}]
            else:
                blocks = [{"type": "text", "text": "AX: fixture state"},
                          {"type": "image", "mimeType": "image/png",
                           "data": PNG}]
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": blocks, "_meta": meta}})
        elif "getAXState" in code:
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": "AX: fixture state"}],
                "_meta": meta}})
        else:
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": "ok"}],
                "_meta": meta}})
'''


class _Ctx:
    def __init__(self):
        self.cancelled = False

    def check(self):
        if self.cancelled:
            raise PermissionError("cancelled")


class RuntimeTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = pathlib.Path(self._tmp.name)
        self.fake = root / "fake_runtime.py"
        self.fake.write_text(FAKE)
        self.outdir = root / "out"
        self.outdir.mkdir()
        self.approvals = ApprovalManager()
        self.addCleanup(self.approvals.close)
        self.binding = ExecutionBinding("user-1", "sess-1", "lead", 1)
        self.png_b64 = base64.b64encode(make_png()).decode()

    def _adapter(self, mode="good"):
        return CuaRuntimeAdapter(
            self.approvals, self.binding, "com.example.FusionRelayFixture",
            command=[sys.executable, str(self.fake), mode, self.png_b64,
                     str(self.outdir)])

    def _wait_ticket(self, thread, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pend = self.approvals.pending()
            if pend:
                return pend[0]
            if thread is not None and not thread.is_alive():
                return None
            time.sleep(0.02)
        return None

    def _run_observe(self, ad, out):
        try:
            out.append(ad.observe("com.example.FusionRelayFixture"))
        except Exception as e:
            out.append(e)

    def _calls(self):
        f = self.outdir / "calls.jsonl"
        return f.read_text().splitlines() if f.exists() else []

    # -- handshake --------------------------------------------------------

    def test_start_validates_protocol(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        self.assertEqual(ad.server_info["name"], "rmcp")
        self.assertIn("js", ad.tool_descriptions)
        self.assertTrue(ad.tools_schema_hash)
        c = ad.compatibility()
        self.assertEqual(c["server_instructions_sha256"],
                         __import__("hashlib").sha256(
                             b"fake runtime").hexdigest())

    def test_bad_init_and_tools_rejected(self):
        for mode in ("badinit", "badtools"):
            ad = self._adapter(mode)
            with self.assertRaises(CompatibilityError, msg=mode):
                ad.start()
            self.assertTrue(ad._dead)
            with self.assertRaises(RuntimeError):
                ad.start()

    def test_sidekick_binding_denied(self):
        with self.assertRaises(PermissionError):
            CuaRuntimeAdapter(
                self.approvals,
                ExecutionBinding("p", "s", "sidekick", 1),
                "com.example.FusionRelayFixture",
                command=[sys.executable, str(self.fake), "good",
                         self.png_b64, str(self.outdir)])

    def test_no_app_calls_during_handshake(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        self.assertEqual(self._calls(), [])

    # -- consent + observe ------------------------------------------------

    def test_observe_allow_once_sends_exact_accept(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        ad.set_context(_Ctx())
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        p = self._wait_ticket(th)
        self.assertIsNotNone(p)
        self.assertEqual(p["allowed_decisions"], ["allow_once", "deny"])
        self.assertEqual(p["capability"], "observe")
        self.assertIn("/runtime-consent", p["principal"])
        self.assertEqual(p["task"], "Codex Computer Use permission")
        self.assertTrue(self.approvals.decide(
            p["request_id"], "allow_once", p["revision"]))
        th.join(10)
        res = out[0]
        self.assertIsInstance(res, dict)
        self.assertEqual(res["png"], make_png())
        self.assertIn("AX: fixture state", res["text"])
        # exact wire result: no persist fields ever
        wire = json.loads(
            (self.outdir / "elicit_result.json").read_text())
        self.assertEqual(wire["result"],
                         {"action": "accept", "content": {}})
        self.assertIn("## Computer Use", ad.instructions)
        self.assertEqual(self.approvals.grants(), [])

    def test_deny_sends_decline_and_keeps_docs(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        p = self._wait_ticket(th)
        self.assertIsNotNone(p)
        self.assertTrue(self.approvals.decide(
            p["request_id"], "deny", p["revision"]))
        th.join(10)
        self.assertIsInstance(out[0], RuntimeUnavailable)
        wire = json.loads(
            (self.outdir / "elicit_result.json").read_text())
        self.assertEqual(wire["result"], {"action": "decline"})
        self.assertIn("## Computer Use", ad.instructions)
        # denial stops the flow: no observation call was sent
        self.assertEqual(len(self._calls()), 1)

    def test_allow_task_decision_rejected_on_consent_ticket(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        p = self._wait_ticket(th)
        self.assertIsNotNone(p)
        self.assertFalse(self.approvals.decide(
            p["request_id"], "allow_task", p["revision"]))
        self.assertTrue(self.approvals.decide(
            p["request_id"], "deny", p["revision"]))
        th.join(10)
        self.assertIsInstance(out[0], RuntimeUnavailable)
        self.assertEqual(self.approvals.grants(), [])

    def test_prior_task_grant_never_autoapproves_consent(self):
        # a live allow_task grant for the same base principal/app must
        # not satisfy the runtime-consent ticket
        scope = ApprovalScope(
            principal="user-1", session="sess-1",
            operation_id="op-x", app="com.example.FusionRelayFixture",
            capability="observe", binding_revision=1,
            action_digest="d" * 64)
        t = self.approvals.request(scope, "task", "observe fixture")
        pend = self.approvals.pending()[0]
        self.assertTrue(self.approvals.decide(
            pend["request_id"], "allow_task", pend["revision"]))
        self.assertEqual(len(self.approvals.grants()), 1)

        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        # consent still reaches the human despite the live grant
        p = self._wait_ticket(th)
        self.assertIsNotNone(p)
        self.assertEqual(p["capability"], "observe")
        self.assertIn("/runtime-consent", p["principal"])
        self.approvals.decide(p["request_id"], "deny", p["revision"])
        th.join(10)

    def test_unknown_meta_and_wrong_app_declined_no_ticket(self):
        for mode in ("extraelicit", "wrongapp"):
            self.outdir.joinpath("calls.jsonl").unlink(missing_ok=True)
            self.outdir.joinpath(
                "elicit_result.json").unlink(missing_ok=True)
            ad = self._adapter(mode)
            ad.start()
            self.addCleanup(ad.close)
            out = []
            th = threading.Thread(
                target=self._run_observe, args=(ad, out))
            th.start()
            th.join(10)
            self.assertIsInstance(out[0], RuntimeUnavailable, msg=mode)
            self.assertEqual(
                [p for p in self.approvals.pending()
                 if "/runtime-consent" in p["principal"]], [], msg=mode)
            wire = json.loads(
                (self.outdir / "elicit_result.json").read_text())
            self.assertEqual(wire["result"], {"action": "decline"})

    # -- cancellation / failure -------------------------------------------

    def test_cancel_before_send_no_js(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        ctx = _Ctx()
        ad.set_context(ctx)
        ctx.cancelled = True
        with self.assertRaises(PermissionError):
            ad.observe("com.example.FusionRelayFixture")
        self.assertEqual(self._calls(), [])

    def test_cancel_during_consent_kills_child(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        ctx = _Ctx()
        ad.set_context(ctx)
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        p = self._wait_ticket(th)
        self.assertIsNotNone(p)
        ctx.cancelled = True
        th.join(10)
        self.assertIsInstance(out[0], PermissionError)
        self.assertTrue(ad._dead)
        self.assertIsNotNone(ad._proc.poll())

    def test_two_concurrent_ops_second_cancelled(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        ctx = _Ctx()
        ad.set_context(ctx)
        out1, out2 = [], []
        t1 = threading.Thread(target=self._run_observe,
                              args=(ad, out1))
        t1.start()
        p = self._wait_ticket(t1)
        self.assertIsNotNone(p)
        t2 = threading.Thread(target=self._run_observe,
                              args=(ad, out2))
        t2.start()
        time.sleep(0.1)
        ctx.cancelled = True
        t1.join(10)
        t2.join(10)
        self.assertIsInstance(out2[0], PermissionError)
        self.assertTrue(ad._dead)

    def test_timeout_kills_child(self):
        ad = self._adapter("hang")
        ad.start()
        self.addCleanup(ad.close)
        ad._transport_timeout = 0.5
        ad._bind_timeout = 0.5
        with self.assertRaises(TimeoutError):
            ad.observe("com.example.FusionRelayFixture")
        self.assertTrue(ad._dead)
        self.assertIsNotNone(ad._proc.poll())

    def test_sigterm_ignoring_child_killed_in_bound(self):
        ad = self._adapter("sigterm")
        ad.start()
        self.addCleanup(ad.close)
        ad._transport_timeout = 0.5
        ad._bind_timeout = 0.5
        t0 = time.monotonic()
        with self.assertRaises(TimeoutError):
            ad.observe("com.example.FusionRelayFixture")
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 12)  # TERM window + KILL window
        self.assertTrue(ad._dead)
        self.assertIsNotNone(ad._proc.poll())

    def test_eof_descendant_reaped(self):
        ad = self._adapter("orphan")
        ad.start()
        self.addCleanup(ad.close)
        gpid = int((self.outdir / "grandchild.pid").read_text())
        with self.assertRaises(RuntimeUnavailable):
            ad.observe("com.example.FusionRelayFixture")
        self.assertTrue(ad._dead)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(gpid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            self.fail("grandchild still alive")

    def test_malformed_output_kills_child(self):
        ad = self._adapter("garbage")
        ad.start()
        self.addCleanup(ad.close)
        out = []
        th = threading.Thread(target=self._run_observe, args=(ad, out))
        th.start()
        p = self._wait_ticket(th)
        self.assertTrue(self.approvals.decide(
            p["request_id"], "allow_once", p["revision"]))
        th.join(10)
        # bind succeeded; garbage arrives on the observation call
        self.assertIsInstance(out[0], RuntimeError)
        self.assertTrue(ad._dead)
        self.assertIsNotNone(ad._proc.poll())

    def test_bad_image_surface_and_count_fail(self):
        for mode in ("badimg", "badmime", "twoimg", "badsurface"):
            for n in ("calls.jsonl", "elicit_result.json"):
                self.outdir.joinpath(n).unlink(missing_ok=True)
            ad = self._adapter(mode)
            ad.start()
            self.addCleanup(ad.close)
            out = []
            th = threading.Thread(
                target=self._run_observe, args=(ad, out))
            th.start()
            p = self._wait_ticket(th)
            self.assertTrue(self.approvals.decide(
                p["request_id"], "allow_once", p["revision"]))
            th.join(10)
            self.assertIsInstance(out[0], RuntimeUnavailable, msg=mode)

    def test_bind_execution_binding(self):
        ad = self._adapter()
        ctx = _Ctx()
        with self.assertRaises(PermissionError):
            ad.bind_execution(
                ExecutionBinding("u", "s", "lead", 2), ctx)
        ad.bind_execution(self.binding, ctx)
        ctx.cancelled = True
        with self.assertRaises(PermissionError):
            ad.bind_execution(self.binding, ctx)

    def test_act_fails_closed(self):
        ad = self._adapter()
        with self.assertRaises(CompatibilityError):
            ad.act("com.example.FusionRelayFixture", {"kind": "click"})

    def test_wrong_app_rejected(self):
        ad = self._adapter()
        ad.start()
        self.addCleanup(ad.close)
        with self.assertRaises(ValueError):
            ad.observe("com.example.Other")
        with self.assertRaises(ValueError):
            ad.current_window("com.example.Other")


if __name__ == "__main__":
    unittest.main()
