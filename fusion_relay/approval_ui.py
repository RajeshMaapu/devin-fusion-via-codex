"""Loopback human-approval web UI for :mod:`fusion_relay.approvals`.

Serves a single self-contained page on ``127.0.0.1``. The control token
is delivered to the browser in the URL **fragment** (never logged or
sent to the server); the page exchanges it once via ``POST /bootstrap``
for an HttpOnly SameSite=Strict session cookie plus a server-held CSRF
token. All mutating endpoints require an exact loopback ``Host``, exact
``Origin``, JSON content type, and the CSRF header. No CORS, no external
resources, no automatic browser launch.

The token and CSRF values live only in this process's memory — never on
disk, in the environment, or in logs. Scope labels shown are local
caller bindings only, not host identity attestation.
"""

from __future__ import annotations

import http.cookies
import http.server
import json
import re
import secrets
import socket
import threading
import time

_MAX_BOOTSTRAP_BODY = 1024
_MAX_API_BODY = 8192
_MAX_HANDLERS = 8
_SOCKET_IDLE_S = 10.0
_BODY_DEADLINE_S = 5.0
_POLL_MS = 2000
_CL_RE = re.compile(r"[0-9]{1,10}")
_TOKEN_CHARS = re.compile(r"[A-Za-z0-9_-]{8,128}")

_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fusion Relay — Approvals</title>
<style nonce="__STYLE_NONCE__">
body{font-family:system-ui,sans-serif;margin:2rem;max-width:52rem}
h1{font-size:1.25rem}h2{font-size:1rem}
.ticket{border:1px solid #8884;border-radius:8px;padding:1rem;
margin:1rem 0}
dl{display:grid;grid-template-columns:9rem 1fr;gap:.25rem 1rem;margin:.5rem 0}
dt{font-weight:600}dd{margin:0;word-break:break-all}
button{margin-right:.5rem;padding:.4rem .9rem}
table{border-collapse:collapse;margin:.5rem 0}
td,th{border:1px solid #8884;padding:.3rem .6rem;text-align:left}
.empty{color:#666}
</style>
</head>
<body>
<main>
<h1>Fusion Relay — pending approvals</h1>
<p>Scope labels are local caller bindings only — not host identity
attestation.</p>
<p id="status" role="status">Authenticating…</p>
<section id="tickets" aria-live="polite"></section>
<section id="grants"><h2>Active task grants</h2>
<div id="grantlist"></div></section>
</main>
<script nonce="__SCRIPT_NONCE__">
"use strict";
const statusEl=document.getElementById("status");
const listEl=document.getElementById("tickets");
const grantEl=document.getElementById("grantlist");
let csrf="";
function setStatus(t){statusEl.textContent=t;}
async function api(path,opts){
  try{
    return await fetch(path,
      Object.assign({credentials:"same-origin"},opts));
  }catch(e){setStatus("Network error.");return {ok:false,status:0};}
}
async function bootstrap(){
  const m=location.hash.match(/^#(.+)$/);
  if(m){
    const token=m[1];
    history.replaceState(null,"",location.pathname);
    const r=await api("/bootstrap",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({token:token})});
    if(!r.ok){setStatus("Bootstrap rejected.");return;}
  }
  const s=await api("/session");
  if(!s.ok){setStatus("Not authenticated.");return;}
  csrf=(await s.json()).csrf;
  setStatus("Connected.");
  schedule();
}
function field(dl,k,v){const dt=document.createElement("dt");
  dt.textContent=k;const dd=document.createElement("dd");
  dd.textContent=String(v);dl.appendChild(dt);dl.appendChild(dd);}
function render(items){
  listEl.textContent="";
  if(!items.length){const p=document.createElement("p");
    p.className="empty";p.textContent="No pending approvals.";
    listEl.appendChild(p);return;}
  for(const it of items){
    const card=document.createElement("div");card.className="ticket";
    const h=document.createElement("h2");h.textContent=it.task;
    card.appendChild(h);
    const dl=document.createElement("dl");
    field(dl,"Action",it.action_summary);
    field(dl,"Principal",it.principal);
    field(dl,"Session",it.session);
    field(dl,"Operation",it.operation_id);
    field(dl,"App",it.app);
    field(dl,"Capability",it.capability);
    field(dl,"Digest",it.action_digest);
    field(dl,"Revision",it.revision);
    field(dl,"Expires in",Math.round(it.expires_in)+"s");
    card.appendChild(dl);
    const buttons=[];
    const mk=(label,decision)=>{
      const b=document.createElement("button");b.textContent=label;
      buttons.push(b);
      b.addEventListener("click",()=>{
        for(const x of buttons)x.disabled=true;
        decide(it.request_id,decision,it.revision);});
      card.appendChild(b);};
    const allowed=Array.isArray(it.allowed_decisions)
      ?it.allowed_decisions:["allow_once","allow_task","deny"];
    if(allowed.includes("allow_once"))mk("Allow Once","allow_once");
    if(allowed.includes("allow_task"))
      mk("Allow For Task (up to 5 minutes)","allow_task");
    if(allowed.includes("deny"))mk("Deny","deny");
    const rv=document.createElement("button");
    rv.textContent="Revoke session/app";
    rv.addEventListener("click",()=>{
      for(const x of buttons)x.disabled=true;rv.disabled=true;
      revoke(it.session,it.app);});
    card.appendChild(rv);
    listEl.appendChild(card);
  }
}
function renderGrants(items){
  grantEl.textContent="";
  if(!items.length){const p=document.createElement("p");
    p.className="empty";p.textContent="No active grants.";
    grantEl.appendChild(p);return;}
  const tb=document.createElement("table");
  const head=document.createElement("tr");
  for(const k of ["Principal","Session","App","Capability",
                  "Binding rev","Remaining",""]){
    const th=document.createElement("th");th.textContent=k;
    head.appendChild(th);}
  tb.appendChild(head);
  for(const g of items){
    const tr=document.createElement("tr");
    for(const v of [g.principal,g.session,g.app,g.capability,
                    g.binding_revision,Math.round(g.remaining_s)+"s"]){
      const td=document.createElement("td");td.textContent=String(v);
      tr.appendChild(td);}
    const td=document.createElement("td");
    const b=document.createElement("button");b.textContent="Revoke";
    b.addEventListener("click",()=>{b.disabled=true;
      revoke(g.session,g.app);});
    td.appendChild(b);tr.appendChild(td);
    tb.appendChild(tr);
  }
  grantEl.appendChild(tb);
}
async function decide(id,decision,rev){
  const r=await api("/api/decide",{method:"POST",
    headers:{"Content-Type":"application/json","X-CSRF-Token":csrf},
    body:JSON.stringify({request_id:id,decision:decision,revision:rev})});
  if(!r.ok)setStatus("Decision rejected.");
  refresh();
}
async function revoke(session,app){
  const r=await api("/api/revoke",{method:"POST",
    headers:{"Content-Type":"application/json","X-CSRF-Token":csrf},
    body:JSON.stringify({session:session,app:app})});
  if(!r.ok)setStatus("Revoke rejected.");
  refresh();
}
async function refresh(){
  const r=await api("/api/pending");
  if(r.ok){render(await r.json());}
  else if(r.status===403){setStatus("Session rejected.");return;}
  const g=await api("/api/grants");
  if(g.ok){renderGrants(await g.json());}
}
async function schedule(){
  await refresh();
  setTimeout(schedule,__POLL_MS__);
}
bootstrap();
</script>
</body>
</html>
"""


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, addr, handler, max_handlers=_MAX_HANDLERS):
        super().__init__(addr, handler)
        self._slots = threading.BoundedSemaphore(max_handlers)
        self._track_lock = threading.Lock()
        self._open_requests = set()
        self._handler_threads = set()

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            try:
                request.close()
            except OSError:
                pass
            return
        with self._track_lock:
            self._open_requests.add(request)
        try:
            super().process_request(request, client_address)
        except Exception:
            with self._track_lock:
                self._open_requests.discard(request)
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        t = threading.current_thread()
        with self._track_lock:
            self._handler_threads.add(t)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._track_lock:
                self._handler_threads.discard(t)
                self._open_requests.discard(request)
            self._slots.release()

    def close_requests(self):
        with self._track_lock:
            reqs = list(self._open_requests)
            threads = list(self._handler_threads)
        for r in reqs:
            try:
                r.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                r.close()
            except OSError:
                pass
        for t in threads:
            t.join(5)

    def handle_error(self, request, client_address):
        pass  # transport noise is expected; never log secrets


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "FusionApprovalUI/1.0"

    def log_message(self, fmt, *args):
        pass

    def setup(self):
        self.request.settimeout(_SOCKET_IDLE_S)
        super().setup()

    # -- request guards -------------------------------------------------

    @property
    def _ctl(self):
        return self.server.approval_server

    def _origin(self) -> str:
        return self._ctl.url

    def _single(self, name: str):
        vals = self.headers.get_all(name)
        if vals is None or len(vals) != 1:
            return None
        return vals[0]

    def _host_ok(self) -> bool:
        return self._single("Host") == \
            f"{self._ctl.host}:{self._ctl.port}"

    def _origin_ok(self) -> bool:
        return self._single("Origin") == self._origin()

    @staticmethod
    def _safe_token(v) -> bool:
        return isinstance(v, str) and bool(_TOKEN_CHARS.fullmatch(v))

    def _session_ok(self) -> bool:
        stored = self._ctl._session_cookie
        if not isinstance(stored, str) or not stored:
            return False
        raw = self._single("Cookie")
        if raw is None:
            return False
        try:
            jar = http.cookies.SimpleCookie(raw)
        except http.cookies.CookieError:
            return False
        morsel = jar.get(self._ctl._cookie_name)
        if morsel is None or not self._safe_token(morsel.value):
            return False
        return secrets.compare_digest(morsel.value, stored)

    def _csrf_ok(self) -> bool:
        stored = self._ctl._csrf
        if not isinstance(stored, str) or not stored:
            return False
        supplied = self._single("X-CSRF-Token")
        if not self._safe_token(supplied):
            return False
        return secrets.compare_digest(supplied, stored)

    def _json_body(self, limit: int):
        """Bounded strict JSON body -> dict or None (error sent)."""
        if self.headers.get_all("Transfer-Encoding") is not None:
            self._fail(400)
            return None
        if self.headers.get("Content-Encoding") is not None:
            self._fail(400)
            return None
        if (self.headers.get("Content-Type") or "").split(";")[0] \
                .strip().lower() != "application/json":
            self._fail(400)
            return None
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or not _CL_RE.fullmatch(lengths[0]):
            self._fail(400)
            return None
        n = int(lengths[0])
        if n > limit:
            self._fail(413)
            return None
        deadline = time.monotonic() + _BODY_DEADLINE_S
        buf = bytearray()
        try:
            while len(buf) < n:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._fail(408)
                    return None
                self.connection.settimeout(
                    min(_SOCKET_IDLE_S, max(0.001, remaining)))
                chunk = self.rfile.read1(min(65536, n - len(buf)))
                if not chunk:
                    break
                buf += chunk
        except socket.timeout:
            self._fail(408)
            return None
        except OSError:
            self._fail(400)
            return None
        finally:
            try:
                self.connection.settimeout(_SOCKET_IDLE_S)
            except OSError:
                pass
        if len(buf) != n:
            self._fail(400)
            return None
        try:
            obj = json.loads(bytes(buf))
        except (ValueError, UnicodeDecodeError):
            self._fail(400)
            return None
        if not isinstance(obj, dict):
            self._fail(400)
            return None
        return obj

    # -- responses ------------------------------------------------------

    def _send(self, status: int, body: bytes = b"",
              content_type: str = "application/json",
              extra: dict = None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _fail(self, status: int):
        self.close_connection = True
        try:
            self._send(status, json.dumps({"error": "rejected"}).encode())
        except OSError:
            pass

    # -- routes ---------------------------------------------------------

    def do_GET(self):
        if not self._host_ok():
            self._fail(403)
            return
        if self.path == "/" or self.path == "/index.html":
            nonce = secrets.token_urlsafe(16)
            style_nonce = secrets.token_urlsafe(16)
            page = (_PAGE
                    .replace("__SCRIPT_NONCE__", nonce)
                    .replace("__STYLE_NONCE__", style_nonce)
                    .replace("__POLL_MS__", str(_POLL_MS)))
            csp = ("default-src 'none'; "
                   f"script-src 'nonce-{nonce}'; "
                   f"style-src 'nonce-{style_nonce}'; "
                   "connect-src 'self'; frame-ancestors 'none'; "
                   "base-uri 'none'; form-action 'self'")
            self._send(200, page.encode(), "text/html; charset=utf-8",
                       {"Content-Security-Policy": csp})
            return
        if self.path == "/session":
            if not self._session_ok():
                self._fail(403)
                return
            self._send(200, json.dumps(
                {"csrf": self._ctl._csrf}).encode())
            return
        if self.path == "/api/pending":
            if not self._session_ok():
                self._fail(403)
                return
            self._send(200, json.dumps(
                self._ctl.manager.pending()).encode())
            return
        if self.path == "/api/grants":
            if not self._session_ok():
                self._fail(403)
                return
            self._send(200, json.dumps(
                self._ctl.manager.grants()).encode())
            return
        self._fail(404)

    def do_POST(self):
        if not (self._host_ok() and self._origin_ok()):
            self._fail(403)
            return
        if self.path == "/bootstrap":
            body = self._json_body(_MAX_BOOTSTRAP_BODY)
            if body is None:
                return
            if set(body) != {"token"} or not isinstance(
                    body["token"], str):
                self._fail(400)
                return
            ctl = self._ctl
            with ctl._bootstrap_lock:
                if ctl._token_used or not self._safe_token(
                        body["token"]) or not secrets.compare_digest(
                        body["token"], ctl._control_token):
                    self._fail(403)
                    return
                ctl._token_used = True  # single-use exchange
                ctl._session_cookie = secrets.token_urlsafe(32)
                ctl._csrf = secrets.token_urlsafe(32)
            cookie = (f"{ctl._cookie_name}={ctl._session_cookie}; "
                      "HttpOnly; SameSite=Strict; Path=/")
            self._send(200, b'{"ok": true}',
                       extra={"Set-Cookie": cookie})
            return
        if self.path in ("/api/decide", "/api/revoke"):
            if not (self._session_ok() and self._csrf_ok()):
                self._fail(403)
                return
            body = self._json_body(_MAX_API_BODY)
            if body is None:
                return
            if self.path == "/api/decide":
                if set(body) != {"request_id", "decision", "revision"} \
                        or not isinstance(body["request_id"], str) \
                        or not isinstance(body["decision"], str) \
                        or type(body["revision"]) is not int:
                    self._fail(400)
                    return
                try:
                    ok = self._ctl.manager.decide(
                        body["request_id"], body["decision"],
                        body["revision"])
                except ValueError:
                    self._fail(400)
                    return
                self._send(200 if ok else 409, json.dumps(
                    {"ok": ok}).encode())
                return
            if set(body) != {"session", "app"} \
                    or not isinstance(body["session"], str) \
                    or not (body["app"] is None
                            or isinstance(body["app"], str)):
                self._fail(400)
                return
            self._ctl.manager.revoke(body["session"], body["app"])
            self._send(200, b'{"ok": true}')
            return
        self._fail(404)


class ApprovalServer:
    """Context-manager wrapper owning the HTTP server thread."""

    def __init__(self, manager, host: str = "127.0.0.1", port: int = 0):
        if host != "127.0.0.1":
            raise ValueError("approval UI binds loopback only")
        self.manager = manager
        self.host = host
        self.port = port
        self._rotate()
        self._server = None
        self._thread = None

    def _rotate(self) -> None:
        self._control_token = secrets.token_urlsafe(32)  # 256 bits
        self._token_used = False
        self._session_cookie = None
        self._csrf = None
        self._bootstrap_lock = threading.Lock()

    @property
    def _cookie_name(self) -> str:
        return f"fusion_approval_{self.port}"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    @property
    def url(self) -> str:
        if self._server is None:
            return ""
        return f"http://{self.host}:{self.port}"

    @property
    def bootstrap_url(self) -> str:
        if self._server is None:
            return ""
        return f"{self.url}/#{self._control_token}"

    def start(self) -> None:
        if self._server is not None:
            return
        self._rotate()  # restart rotates all secrets; old cookies die
        self._server = _Server((self.host, self.port), _Handler)
        self._server.approval_server = self
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True,
            name="fusion-approval-ui")
        self._thread.start()

    def close(self) -> None:
        if self._server is None:
            return
        server, thread = self._server, self._thread
        self._server = None
        self._thread = None
        server.shutdown()
        server.close_requests()
        server.server_close()
        thread.join(10)


def _demo() -> None:
    """Explicit CLI demo: one synthetic ticket, no actuator, no model."""
    import hashlib

    from fusion_relay.approvals import ApprovalManager, ApprovalScope

    manager = ApprovalManager()
    scope = ApprovalScope(
        principal="demo-principal",
        session="demo-session",
        operation_id="demo-operation",
        app="com.example.FusionRelayDemo",
        capability="observe",
        binding_revision=1,
        action_digest=hashlib.sha256(b"demo-observation").hexdigest())
    manager.request(
        scope, task="Disposable approval demonstration",
        action_summary="Read the demo window; no desktop action will "
                       "execute")
    with ApprovalServer(manager) as server:
        # This URL is for the human running the demo only — never log it.
        print("Approval demo (sandbox fixture, no actions execute):")
        print(server.bootstrap_url)
        try:
            while True:
                threading.Event().wait(3600)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    import sys

    if "--demo" in sys.argv:
        _demo()
    else:
        print("usage: python3 -m fusion_relay.approval_ui --demo")
