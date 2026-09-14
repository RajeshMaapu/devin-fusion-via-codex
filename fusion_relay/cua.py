"""CodexComputerProvider — a persistent cua_repl MCP session for the relay.

The relay exposes a ``codex_computer`` function tool on Codex-routed turns
and executes it here: a long-lived ``cua-repl.mjs`` child speaks JSON-RPC
over stdio and runs the ``js`` tool against the ``cua`` API (listApps,
getApp -> getAXState/getScreenshot/click/pressKey/typeText/scroll).

Consent: the surface gates first-use per app via MCP form elicitation.
The relay has no trusted dispatcher, authenticated consent UI, or
qualified runtime contract, so the provider is disabled outright:
``available()`` is False, elicitations are declined (unknown methods get
a protocol error), and ``execute()`` raises ``computer_policy_denied``.
A local lock inside this relay cannot fence other processes on the
desktop either way.

Failure policy: a dead or unspawnable provider raises CuaUnavailable,
which the caller turns into an explicit ``computer_unavailable`` tool
result — never a silent fallback to anything else.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import threading
import time
import fcntl as _fcntl

CODEX_HOME = pathlib.Path(os.environ.get("CODEX_HOME", pathlib.Path.home() / ".codex"))
CHATGPT_RESOURCES = pathlib.Path("/Applications/ChatGPT.app/Contents/Resources")
CUA_NODE = CHATGPT_RESOURCES / "cua_node"
CUA_REPL = CUA_NODE / "lib" / "node_modules" / "@oai" / "cua-repl" / "bin" / "cua-repl.mjs"
NODE_REPL = CUA_NODE / "bin" / "node_repl"
SERVICE_APP = CODEX_HOME / "computer-use" / "Codex Computer Use.app"

# No environment flag may enable this: the trusted dispatch/consent
# contracts the provider requires do not exist yet.
CUA_ENABLED = False


class CuaUnavailable(RuntimeError):
    """The provider cannot serve a call — surfaced as computer_unavailable."""


class CuaProvider:
    """One persistent cua_repl child; serialized js executions."""

    def __init__(self, data_dir: pathlib.Path) -> None:
        self._data_dir = data_dir
        self._proc: subprocess.Popen | None = None
        self._buf = b""
        self._lock = threading.Lock()
        self._next_id = 1

    # -- lifecycle ---------------------------------------------------------
    def _env(self) -> dict:
        env = dict(os.environ)
        env.update({
            "NODE_REPL_NODE_MODULE_DIRS": str(CUA_NODE / "lib" / "node_modules"),
            "NODE_REPL_NODE_PATH": str(CUA_NODE / "bin" / "node"),
            "NODE_REPL_TRUSTED_CODE_PATHS":
                f"{CODEX_HOME}:{CUA_NODE / 'lib' / 'node_modules'}",
            "CODEX_HOME": str(CODEX_HOME),
            "NODE_REPL_TRUSTED_SERVICES":
                '{"browser":"@oai/browser-desktop/service","sky":"@oai/sky/service"}',
            "SKY_CUA_SERVICE_PATH": str(SERVICE_APP),
            "CUA_REPL_NODE_REPL_PATH": str(NODE_REPL),
            "CUA_REPL_ENABLED_SURFACES": "computer",
            "BROWSER_USE_TINYSKY_ENABLED": "1",
            "BROWSER_USE_AVAILABLE_BACKENDS": "chrome,iab",
            "BROWSER_USE_CODEX_APP_BUILD_FLAVOR": "prod",
            "CODEX_CLI_PATH": str(CHATGPT_RESOURCES / "codex"),
            "NODE_REPL_NATIVE_PIPE_CONNECT_TIMEOUT_MS": "1000",
        })
        return env

    def available(self) -> bool:
        return False

    def compatibility(self) -> dict:
        return {"status": "blocked",
                "reason": "trusted_dispatch_consent_and_runtime_contract_unavailable",
                "computer_enabled": False,
                "vision": "vision_unavailable"}

    def _ensure(self) -> None:
        if not self.available():
            raise CuaUnavailable("cua_repl or Codex Computer Use.app missing")
        if self._proc and self._proc.poll() is None:
            return
        self._proc = subprocess.Popen(
            [str(CUA_NODE / "bin" / "node"), str(CUA_REPL)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, env=self._env(), bufsize=0)
        _fcntl.fcntl(self._proc.stdout, _fcntl.F_SETFL, os.O_NONBLOCK)
        self._buf = b""
        init = self._rpc("initialize", {
            "protocolVersion": "2025-03-26",
            "capabilities": {"elicitation": {"form": {}}},
            "clientInfo": {"name": "fusion-codex-relay", "version": "0.3"}},
            deadline=time.time() + 30)
        if not init or "result" not in init:
            self._kill()
            raise CuaUnavailable("cua_repl initialize failed")
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _kill(self) -> None:
        """Terminate the child and confirm it exited before clearing _proc.

        A process that will not die stays referenced so no replacement is
        spawned while the old one is unconfirmed.
        """
        proc = self._proc
        if proc is None:
            return
        try:
            proc.terminate()
        except OSError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except OSError:
                pass
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        except OSError:
            pass
        if proc.poll() is None:
            raise CuaUnavailable("cua_repl process did not terminate")
        self._proc = None

    # -- jsonrpc -----------------------------------------------------------
    def _send(self, obj: dict) -> None:
        assert self._proc and self._proc.stdin
        self._proc.stdin.write(json.dumps(obj).encode() + b"\n")
        self._proc.stdin.flush()

    def _rpc(self, method: str, params: dict, deadline: float) -> dict | None:
        """Send a request; answer elicitations; return the matching response."""
        assert self._proc and self._proc.stdout
        rid = self._next_id
        self._next_id += 1
        self._send({"jsonrpc": "2.0", "id": rid, "method": method,
                    "params": params})
        while time.time() < deadline:
            try:
                chunk = self._proc.stdout.read()
                if chunk:
                    self._buf += chunk
            except BlockingIOError:
                pass
            while b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                if "method" in msg and "id" in msg:
                    self._answer_elicitation(msg)
                    continue
                if msg.get("id") == rid:
                    return msg
            if self._proc.poll() is not None:
                return None
            time.sleep(0.03)
        return None

    def _answer_elicitation(self, msg: dict) -> None:
        """Decline every consent request: the relay has no authenticated
        consent UI, so nothing the app or model sends can be trusted as a
        grant. Unknown request methods get a protocol error."""
        rid = msg.get("id")
        if rid is None:
            return
        if msg.get("method") != "elicitation/create":
            self._send({"jsonrpc": "2.0", "id": rid, "error": {
                "code": -32601, "message": "Unsupported elicitation method"}})
            return
        self._send({"jsonrpc": "2.0", "id": rid,
                    "result": {"action": "decline"}})

    # -- public api --------------------------------------------------------
    def execute(self, code: str, title: str, timeout_ms: int,
                rec: dict) -> dict:
        """Run *code* via the js tool. Returns {text: str, images: [paths]}.

        Never raises for tool-level failures — isError results come back as
        text so the model sees the explicit error. Provider-level failures
        raise CuaUnavailable.
        """
        if not self.available():
            raise CuaUnavailable(
                "computer_policy_denied: trusted dispatcher, consent UI, "
                "and qualified runtime required")
        with self._lock:
            self._ensure()
            assert self._proc is not None
            resp = self._rpc("tools/call", {
                "name": "js",
                "arguments": {"code": code, "title": title[:80] or "computer step",
                              "timeout_ms": timeout_ms}},
                deadline=time.time() + max(30, timeout_ms / 1000 + 30))
            if resp is None:
                self._kill()
                raise CuaUnavailable("cua_repl call timed out or exited")
            if "error" in resp:
                err = resp["error"]
                return {"text": f"computer_unavailable: {err.get('message', err)}",
                        "images": []}
            result = resp.get("result") or {}
            texts, images = [], []
            for block in result.get("content") or []:
                btype = block.get("type")
                if btype == "text":
                    texts.append(block.get("text", ""))
                elif btype == "image" and block.get("data"):
                    texts.append("vision_unavailable: runtime image "
                                 "contract is not qualified")
            out = "\n".join(texts)
            if result.get("isError"):
                out = "computer error: " + (out or "unknown")
            return {"text": out or "(no output)", "images": images}


_provider: CuaProvider | None = None
_provider_lock = threading.Lock()


def get_provider(data_dir: pathlib.Path) -> CuaProvider:
    global _provider
    with _provider_lock:
        if _provider is None:
            _provider = CuaProvider(data_dir)
        return _provider


def reset() -> None:
    """Tests: drop the cached provider."""
    global _provider
    with _provider_lock:
        if _provider:
            _provider._kill()
        _provider = None
