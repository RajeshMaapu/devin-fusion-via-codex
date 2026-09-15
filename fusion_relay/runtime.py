"""Codex Computer Use (cua-repl) runtime adapter — observe only.

This is a real (non-fake) adapter boundary for :class:`ComputerBroker`,
but it is deliberately limited to observation: the runtime's first-use
documentation does not define the coordinate space of
``Target.click(Vec2)`` relative to screenshot pixels versus points, so
``act`` is not implemented and fails closed until that contract is
qualified. The documentation prefers element-index actions; a future
``click_element`` kind is the intended path, not guessed coordinates.

Isolation model: each adapter owns exactly one ``cua-repl`` child
process (its own process group) bound to exactly one app. All
JavaScript sent to the kernel is fixed, generated here — there is no
raw model-JS entrypoint. ``window_identity`` is a conservative scene
fingerprint: sha256 over ``[app, full AX text]``. It is **not** a
trusted OS focus identity — this adapter cannot detect unrelated-app
focus or platform-global interference.

Platform consent: ``elicitation/create`` requests from the runtime are
surfaced as fresh human approval tickets with
``allowed_decisions=("allow_once", "deny")`` under a distinct
``<principal>/runtime-consent`` principal so no broker task grant can
ever auto-approve a platform consent. A local ``allow_once`` yields
exactly the standard MCP result ``{"action": "accept", "content": {}}``
— never ``persist`` fields. Anything unknown (method, schema, app,
risk level, extra properties) is declined at the protocol level without
asking the human.

Failure model: any transport/protocol failure or cancellation marks
the adapter dead and terminates the adapter-owned process group; a
dead adapter is never reused or restarted. There is no negotiated
cancel method — a submitted call whose context is cancelled is left
``outcome_unknown`` for the caller's journal.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import pathlib
import selectors
import signal
import subprocess
import threading
import time

from .approvals import ApprovalScope
from .artifacts import MAX_ARTIFACT_BYTES, ArtifactError
from .images import image_as_png
from .broker import ExecutionBinding, _check_binding

NODE_ROOT = pathlib.Path(
    "/Applications/ChatGPT.app/Contents/Resources/cua_node")
_PROTOCOL = "2025-03-26"
_SERVER_NAME = "rmcp"
_SERVER_VERSION = "1.5.0"
_KNOWN_TOOLS = {"js", "js_add_node_module_dir", "js_reset", "turn_ended"}
_KNOWN_CONSENT_TOOLS = {"get_app_state"}
_CONSENT_META_KEYS = {
    "codex_approval_kind", "connector_id", "connector_name", "persist",
    "progressToken", "riskLevel", "tool_name", "tool_params",
    "tool_params_display"}
_LINE_MAX = 16 << 20
_TOTAL_MAX = 64 << 20
_TRANSPORT_TIMEOUT = 30.0
_BIND_TIMEOUT = 180.0
_KILL_WAIT = 5.0


class CompatibilityError(RuntimeError):
    """Negotiated runtime contract does not match the pinned one."""


class RuntimeUnavailable(PermissionError):
    """Consent denied, child died, transport failed, or adapter dead."""


def _js_literal(v) -> str:
    return json.dumps(v)


class CuaRuntimeAdapter:
    """One child process, one bound app, observe-only."""

    def __init__(self, approvals, binding: ExecutionBinding, app: str,
                 node_root: pathlib.Path = NODE_ROOT, command=None):
        _check_binding(binding)
        if binding.role != "lead":
            raise PermissionError(
                "runtime adapter requires a lead binding")
        if not isinstance(app, str) or "." not in app or len(app) > 255:
            raise ValueError("app must be a dotted bundle identifier")
        self._approvals = approvals
        self._binding = binding
        self._app = app
        self._node_root = pathlib.Path(node_root)
        self._command = command  # test-only override of spawn argv
        self._context = None
        self._proc = None
        self._sel = None
        self._buf = b""
        self._rid = 0
        self._bound = False
        self._dead = False
        self._op_lock = threading.Lock()
        self.instructions = ""        # first-use doc text, preserved
        self.tool_descriptions = {}
        self.tools_schema_hash = ""
        self.server_info = {}
        self.server_instructions = ""
        self.runtime_package_version = None
        self.image_mime_corrections = 0
        self._transport_timeout = _TRANSPORT_TIMEOUT
        self._bind_timeout = _BIND_TIMEOUT

    # -- lifecycle ----------------------------------------------------

    def set_context(self, context) -> None:
        self._context = context

    def bind_execution(self, binding: ExecutionBinding, context) -> None:
        if binding != self._binding:
            raise PermissionError('runtime execution binding mismatch')
        context.check()
        self.set_context(context)

    def start(self) -> None:
        if self._dead or self._proc is not None:
            raise RuntimeError("adapter is dead or already started")
        env = {k: os.environ[k] for k in
               ("HOME", "PATH", "TMPDIR", "USER", "LOGNAME")
               if k in os.environ}
        root = str(self._node_root)
        env.update({
            "CUA_REPL_NODE_REPL_PATH": root + "/bin/node_repl",
            "CUA_REPL_ENABLED_SURFACES": "computer",
            "NODE_REPL_NODE_MODULE_DIRS": root + "/lib/node_modules",
            "NODE_REPL_NODE_PATH": root + "/bin/node",
            "NODE_REPL_TRUSTED_CODE_PATHS": root + "/lib/node_modules",
        })
        argv = self._command or [
            root + "/bin/node",
            root + "/lib/node_modules/@oai/cua-repl/bin/cua-repl.mjs"]
        proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, env=env, start_new_session=True)
        self._proc = proc
        self._sel = selectors.DefaultSelector()
        self._sel.register(proc.stdout, selectors.EVENT_READ)
        try:
            pkg = (self._node_root / "lib/node_modules"
                   / "@oai/cua-repl/package.json")
            if pkg.is_file():
                v = json.loads(pkg.read_text()).get("version")
                if isinstance(v, str):
                    self.runtime_package_version = v
            self._handshake()
        except BaseException:
            self._abort()
            raise

    def _handshake(self) -> None:
        resp = self._rpc("initialize", {
            "protocolVersion": _PROTOCOL, "capabilities":
                {"elicitation": {"form": {}}},
            "clientInfo": {"name": "fusion-relay-runtime", "version": "1"}})
        result = resp.get("result") or {}
        self.server_info = result.get("serverInfo") or {}
        if result.get("protocolVersion") != _PROTOCOL \
                or self.server_info.get("name") != _SERVER_NAME \
                or self.server_info.get("version") != _SERVER_VERSION:
            raise CompatibilityError(
                "runtime protocol/server mismatch")
        instr = result.get("instructions")
        if isinstance(instr, str):
            self.server_instructions = instr
        self._notify("notifications/initialized")
        resp = self._rpc("tools/list", {})
        tools = (resp.get("result") or {}).get("tools")
        if not isinstance(tools, list) \
                or not all(isinstance(t, dict) for t in tools):
            raise CompatibilityError("tools/list malformed")
        names = [t.get("name") for t in tools]
        if len(names) != len(set(names)) \
                or not set(names) <= _KNOWN_TOOLS \
                or "js" not in names or "js_reset" not in names:
            raise CompatibilityError("unexpected runtime tools")
        by_name = {t["name"]: t for t in tools}
        schema = by_name["js"].get("inputSchema") or {}
        props = schema.get("properties") or {}
        tmo = props.get("timeout_ms") or {}
        title = props.get("title") or {}
        if schema.get("type") != "object" \
                or schema.get("additionalProperties") is not False \
                or schema.get("required") != ["code"] \
                or (props.get("code") or {}).get("type") != "string" \
                or not set(props) <= {"code", "timeout_ms", "title"} \
                or tmo.get("type") != "integer" \
                or tmo.get("minimum") != 1 \
                or title.get("type") != "string" \
                or title.get("minLength") != 1 \
                or title.get("maxLength") != 80:
            raise CompatibilityError("js tool schema mismatch")
        reset = by_name["js_reset"].get("inputSchema") or {}
        if reset.get("type") != "object":
            raise CompatibilityError("js_reset schema mismatch")
        for t in tools:
            self.tool_descriptions[t["name"]] = t.get("description")
        self.tools_schema_hash = hashlib.sha256(
            json.dumps(tools, sort_keys=True).encode()).hexdigest()

    def _close_handles(self) -> None:
        """Release local handles even when child termination is uncertain."""
        if self._sel is not None:
            try:
                self._sel.close()
            except Exception:
                pass
            self._sel = None
        proc = self._proc
        if proc is not None:
            for stream in (proc.stdin, proc.stdout):
                if stream is not None:
                    try:
                        stream.close()
                    except Exception:
                        pass

    def close(self) -> None:
        self._dead = True
        confirmed = False
        try:
            self._kill_group()
            confirmed = True
        finally:
            self._close_handles()
            # Retain identity for reconciliation if termination failed.
            # Closing handles is NOT proof that the process/action stopped.
            if confirmed:
                # handles released only after the group is confirmed gone
                self._proc = None

    def _abort(self) -> None:
        """Dead adapter, bounded termination, unconditional handle cleanup."""
        self._dead = True
        try:
            self._kill_group()
        finally:
            self._close_handles()
        # Preserve the reaped process object for existing diagnostic callers.

    def _kill_group(self) -> None:
        """TERM (5s) then KILL (5s) then confirm the group is empty.

        The child is reaped with wait() throughout; any group member
        still present after the KILL window fails closed — the proc
        handle is kept and the adapter stays dead."""
        proc = self._proc
        if proc is None:
            return
        pgid = proc.pid

        def members() -> list:
            r = subprocess.run(["pgrep", "-g", str(pgid)],
                               capture_output=True, text=True)
            if r.returncode not in (0, 1):
                return [pgid]  # cannot enumerate — treat as present
            return [int(x) for x in r.stdout.split()]

        def reap(deadline) -> bool:
            while time.monotonic() < deadline:
                try:
                    if proc.wait(timeout=0.05) is not None:
                        return True
                except subprocess.TimeoutExpired:
                    pass
            return proc.poll() is not None

        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        if not reap(time.monotonic() + _KILL_WAIT):
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            reap(time.monotonic() + _KILL_WAIT)
        # confirm no owned descendants remain in the group
        deadline = time.monotonic() + _KILL_WAIT
        while True:
            left = members()
            if not left:
                return
            for pid in left:
                try:
                    os.kill(pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
            if time.monotonic() > deadline:
                raise RuntimeError(
                    "runtime child process group termination "
                    "unconfirmed")
            time.sleep(0.05)

    # -- JSON-RPC core --------------------------------------------------

    def _check_ctx(self) -> None:
        if self._context is not None:
            self._context.check()

    def _send(self, obj) -> None:
        self._proc.stdin.write(json.dumps(obj).encode() + b"\n")
        self._proc.stdin.flush()

    def _notify(self, method, params=None) -> None:
        obj = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            obj["params"] = params
        self._send(obj)

    def _rpc(self, method: str, params: dict,
             timeout=None) -> dict:
        timeout = self._transport_timeout if timeout is None \
            else timeout
        if self._dead:
            raise RuntimeUnavailable("runtime adapter dead")
        self._check_ctx()
        self._rid += 1
        rid = self._rid
        try:
            self._send({"jsonrpc": "2.0", "id": rid, "method": method,
                        "params": params})
        except BaseException:
            self._abort()
            raise
        deadline = time.monotonic() + timeout
        total = 0
        while True:
            try:
                self._check_ctx()
            except BaseException:
                # no negotiated cancel: terminate our own group; a
                # submitted call is outcome_unknown upstream
                self._abort()
                raise
            if time.monotonic() > deadline:
                self._abort()
                raise TimeoutError("runtime rpc timeout")
            if b"\n" not in self._buf:
                try:
                    events = self._sel.select(min(
                        0.5, max(0.01, deadline - time.monotonic())))
                except BaseException:
                    self._abort()
                    raise
                if not events:
                    if self._proc.poll() is not None:
                        self._abort()
                        raise RuntimeUnavailable("runtime exited")
                    continue
                try:
                    chunk = os.read(self._proc.stdout.fileno(), 65536)
                except BaseException:
                    self._abort()
                    raise
                if not chunk:
                    self._abort()
                    raise RuntimeUnavailable("runtime EOF")
                self._buf += chunk
                if len(self._buf) > _LINE_MAX:
                    self._abort()
                    raise RuntimeError("runtime line too large")
            while b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                total += len(line)
                if total > _TOTAL_MAX:
                    self._abort()
                    raise RuntimeError("runtime output too large")
                try:
                    msg = json.loads(line)
                except ValueError:
                    self._abort()
                    raise RuntimeError("runtime output malformed")
                if not isinstance(msg, dict):
                    self._abort()
                    raise RuntimeError("runtime output malformed")
                if "method" in msg and "id" in msg:
                    try:
                        self._server_request(msg, deadline)
                    except BaseException:
                        self._abort()
                        raise
                elif msg.get("id") == rid:
                    return msg

    def _server_request(self, msg: dict, deadline: float) -> None:
        """Handle a server->client request. Only a fully qualified
        elicitation/create reaches the human; everything else is
        declined or answered with a protocol error. Cancellation and
        unexpected failures propagate — they are never swallowed."""
        if msg.get("method") != "elicitation/create":
            self._send({"jsonrpc": "2.0", "id": msg.get("id"),
                        "error": {"code": -32601,
                                  "message": "unsupported method"}})
            return
        result = self._consent(msg, deadline)
        self._send({"jsonrpc": "2.0", "id": msg.get("id"),
                    "result": result})

    def _consent(self, msg: dict, deadline: float) -> dict:
        params = msg.get("params")
        decline = {"action": "decline"}
        if not isinstance(params, dict):
            return decline
        if params.get("requestedSchema") != {"type": "object",
                                             "properties": {}}:
            return decline
        meta = params.get("_meta")
        if not isinstance(meta, dict) \
                or not set(meta) <= _CONSENT_META_KEYS:
            return decline
        if meta.get("codex_approval_kind") != "mcp_tool_call" \
                or meta.get("connector_id") != "computer-use" \
                or meta.get("tool_name") not in _KNOWN_CONSENT_TOOLS \
                or meta.get("riskLevel") != "low":
            return decline
        tp = meta.get("tool_params")
        if not isinstance(tp, dict) or set(tp) != {"app"} \
                or tp.get("app") != self._app:
            return decline
        persist = meta.get("persist")
        if persist is not None and (not isinstance(persist, list)
                                    or not set(persist)
                                    <= {"session", "always"}):
            return decline
        message = params.get("message")
        if not isinstance(message, str):
            message = ""
        digest = hashlib.sha256(json.dumps(
            {"schema": params.get("requestedSchema"), "meta": meta},
            sort_keys=True).encode()).hexdigest()
        b = self._binding
        scope = ApprovalScope(
            principal=b.principal + "/runtime-consent",
            session=b.session,
            operation_id=f"runtime-consent-{self._rid}",
            app=self._app, capability="observe",
            binding_revision=b.revision, action_digest=digest)

        def cancel_check():
            self._check_ctx()
            if time.monotonic() > deadline:
                raise TimeoutError("runtime rpc deadline")

        ticket = self._approvals.request(
            scope, task="Codex Computer Use permission",
            action_summary=(message or
                            f"Allow computer use of {self._app}")[:512],
            ttl=min(120.0, max(1.0, deadline - time.monotonic())),
            allowed_decisions=("allow_once", "deny"))
        try:
            decision = self._approvals.wait(
                ticket, check_cancelled=cancel_check)
        except BaseException:
            self._approvals.request_cancelled(ticket)
            try:
                self._check_ctx()
            except BaseException:
                self._abort()
                raise  # cancellation propagates; group is dead
            if time.monotonic() > deadline:
                self._abort()
                raise TimeoutError("runtime consent deadline")
            return decline  # human denial or ticket expiry
        if decision == "allow_once":
            self._approvals.authorize(ticket, consume=True)
            return {"action": "accept", "content": {}}
        return decline

    # -- adapter surface ------------------------------------------------

    def _call_js(self, code: str, title: str,
                 timeout=None) -> dict:
        resp = self._rpc("tools/call", {
            "name": "js",
            "arguments": {"code": code, "title": title[:80],
                          "timeout_ms": 30000}}, timeout=timeout)
        result = resp.get("result")
        if resp.get("error") or not isinstance(result, dict):
            self._abort()
            raise RuntimeUnavailable("runtime call failed")
        # preserve first-use docs even when the call itself was denied
        for block in result.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text" \
                    and block.get("text", "").startswith(
                        "## Computer Use"):
                self.instructions = block["text"]
        if result.get("isError"):
            raise RuntimeUnavailable(
                "runtime call denied or failed")  # no raw provider text
        meta = result.get("_meta") or {}
        surface = meta.get("codex/toolSurface")
        if not isinstance(surface, dict) \
                or surface.get("kind") != "computerUse" \
                or (surface.get("app") or {}).get("appId") != self._app:
            raise RuntimeUnavailable(
                "runtime result not scoped to bound app")
        return result

    def _bind(self) -> None:
        if self._bound:
            return
        code = ("const fixture = await cua.getApp(%s);"
                % _js_literal(self._app))
        self._call_js(code, "Select disposable test app",
                      timeout=self._bind_timeout)
        self._bound = True

    @staticmethod
    def _fingerprint(app: str, ax_text: str) -> str:
        return hashlib.sha256(json.dumps(
            [app, ax_text]).encode()).hexdigest()

    def _observation(self, expr: str, title: str) -> dict:
        while not self._op_lock.acquire(timeout=0.05):
            self._check_ctx()
            if self._dead:
                raise RuntimeUnavailable("runtime adapter dead")
        try:
            self._bind()
            result = self._call_js(f"await fixture.{expr};", title)
        finally:
            self._op_lock.release()
        texts, images = [], []
        for block in result.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                texts.append(block.get("text", ""))
            elif block.get("type") == "image":
                images.append(block)
        ax = "\n".join(t for t in texts
                       if not t.startswith("## Computer Use"))
        return {"ax": ax, "images": images}

    def observe(self, app: str) -> dict:
        """Fresh AX state + screenshot for the bound app only."""
        if app != self._app:
            raise ValueError("adapter is bound to a different app")
        self._check_ctx()
        try:
            out = self._observation(
                "getAXStateAndScreenshot({disableDiffing: true})",
                "Observe disposable test app")
        except Exception:
            self._check_ctx()  # surface cancellation over runtime errors
            raise
        if len(out["images"]) != 1:
            raise RuntimeUnavailable(
                "expected exactly one observation image")
        img = out["images"][0]
        if img.get("mimeType") not in ("image/png", "image/jpeg") \
                or not isinstance(img.get("data"), str):
            raise RuntimeUnavailable("unexpected observation image")
        if len(img["data"]) > 4 * ((MAX_ARTIFACT_BYTES + 2) // 3):
            raise RuntimeUnavailable("observation image too large")
        try:
            raw = base64.b64decode(img["data"], validate=True)
        except ValueError:
            raise RuntimeUnavailable("observation image not base64")
        try:
            png, actual = image_as_png(raw, img["mimeType"])
        except ArtifactError:
            raise RuntimeUnavailable("unexpected observation image")
        if actual != img["mimeType"]:
            self.image_mime_corrections += 1
        return {"window_identity": self._fingerprint(app, out["ax"]),
                "text": out["ax"], "png": png,
                "source_image_sha256": hashlib.sha256(raw).hexdigest(),
                "source_mime_type": actual}

    def current_window(self, app: str) -> str:
        """Scene fingerprint for the bound app — not an OS focus id."""
        if app != self._app:
            raise ValueError("adapter is bound to a different app")
        self._check_ctx()
        try:
            out = self._observation(
                "getAXState({disableDiffing: true})",
                "Check disposable test app state")
        except Exception:
            self._check_ctx()
            raise
        return self._fingerprint(app, out["ax"])

    def act(self, app: str, action: dict) -> None:
        raise CompatibilityError(
            "act unqualified: runtime click coordinate space is not "
            "documented; observe-only until element-index actions are "
            "qualified")

    def compatibility(self) -> dict:
        return {
            "available": self._proc is not None and not self._dead,
            "server": self.server_info,
            "tools_schema_hash": self.tools_schema_hash,
            "server_instructions_sha256": hashlib.sha256(
                self.server_instructions.encode()).hexdigest()
                if self.server_instructions else None,
            "runtime_package_version": self.runtime_package_version,
            "bound_app": self._app,
            "observe_only": True,
            "window_identity": "ax-scene-fingerprint-not-os-focus",
            "image_mime_corrections": self.image_mime_corrections,
            "image_formats": ["png", "jpeg"],
            "image_transform":
                "JPEG decoded to RGB PNG without resizing",
        }
