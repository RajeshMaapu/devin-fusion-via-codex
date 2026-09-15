"""EXPERIMENTAL host wrapper: a real Devin CLI session exercising durable
continuation with launcher-provenance bindings.

This harness owns the host side of the capability contract: it issues
launcher-provenance capabilities, attaches ``X-Fusion-Capability`` /
``X-Fusion-Operation`` / ``X-Fusion-Proof`` headers to codex-routed
GetChatMessage requests, and forwards them to a loopback relay it owns.

UNQUALIFIED: launcher provenance asserts only that this process owns the
local socket path — it is not a native verified hook. No acknowledgement
is ever sent: socket delivery is not Fusion acceptance, and the native
ACK contract remains unavailable. Native (non-codex) traffic is forwarded
untouched; note that this harness buffers even native streaming
responses. The journal records hashed references only — never bodies,
headers, tokens, or proofs.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import http.server
import json
import os
import secrets
import select
import signal
import socket
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from .accounting import reference
from .continuation import ContinuationError, digest
from .continuation_host import ContinuationCoordinator
from .host_binding import (HEADER_CAPABILITY, HEADER_OPERATION,
                           HEADER_PROOF, PROVENANCE_LAUNCHER,
                           CapabilityStore)
from .storage import append_private, atomic_write, read_private

_FORWARD_TIMEOUT_S = 300
_CAPABILITY_TTL_S = 3600
_REISSUE_MARGIN_S = 60

# Response headers copied through to the client in addition to
# relay.RESPONSE_PASS (imported lazily — see _relay()).
_EPOCH_HEADERS = ('X-Fusion-Continuation-Epoch',
                  'X-Fusion-Continuation-Revision')

_LEGACY_REQUIRED = ('legacy continuation unavailable; '
                    'explicit new epoch required')
_EPOCH_REQUIRED = 'native history transition requires explicit epoch'

# Relay error-trailer messages that mean "the client's history cannot
# extend the committed anchors" → the host may adopt the presented
# history under an explicit epoch transition. Maps message → reason.
_ADOPTION_MESSAGES = {
    _LEGACY_REQUIRED: 'legacy_history',
    _EPOCH_REQUIRED: 'history_divergence',
}


def _epoch_adoption_reason(body: bytes):
    """The epoch-transition reason implied by a Connect response body's
    relay error trailer, or None when the trailer is absent/other."""
    try:
        from . import wire
        for flags, payload in wire.iter_frames(body):
            if flags & 0x02:
                meta = json.loads(payload)
                if isinstance(meta, dict):
                    return _ADOPTION_MESSAGES.get(
                        meta.get('error', {}).get('message'))
    except (ValueError, AttributeError):
        return None
    return None


def _relay():
    """Import relay lazily: its module-level constants read
    FUSION_RELAY_* environment variables at import time."""
    from . import relay
    return relay


class ExperimentalHost:
    """Owns a loopback relay plus a binding-injecting proxy in front of
    it. Unit-testable: ``bind_request`` is pure with respect to the wire;
    the proxy can be started against any already-running relay."""

    def __init__(self, *, data_dir: Path, relay_port: int,
                 proxy_port: int, ledger, capabilities: CapabilityStore,
                 journal_path: Path, retry_window_s: float = 300.0,
                 clock=time.time):
        self._data_dir = Path(data_dir)
        self._relay_port = relay_port
        self._proxy_port = proxy_port
        self._ledger = ledger
        self._capabilities = capabilities
        self._journal_path = Path(journal_path)
        self._retry_window = retry_window_s
        self._clock = clock
        self._client_instance = hashlib.sha256(
            f'{self._data_dir}:{os.getpid()}'.encode()).hexdigest()
        self._lock = threading.Lock()
        self._sessions: dict = {}
        self._ops: dict = {}
        self._proxy = None
        self._proxy_thread = None
        self._relay_thread = None
        self._owns_relay = False
        self._join_timeout_s = 15.0
        coordinator = ContinuationCoordinator(
            ledger, capabilities=capabilities,
            accept_provenance=frozenset({PROVENANCE_LAUNCHER}),
            key_store_status='ready')
        _relay().set_continuation_coordinator(coordinator)
        self._coordinator = coordinator

    # -- lifecycle ------------------------------------------------------

    def start(self) -> None:
        """Launch the owned relay, wait for it, write host.env, then
        start the binding proxy."""
        relay = _relay()
        self._owns_relay = True
        self._relay_thread = threading.Thread(
            target=relay.serve, args=(self._relay_port,), daemon=True)
        self._relay_thread.start()
        self.wait_ready()
        token = read_private(
            self._data_dir / 'relay-token', 256).decode().strip()
        atomic_write(
            self._data_dir / 'host.env',
            (f'WINDSURF_API_SERVER_URL='
             f'http://127.0.0.1:{self._proxy_port}/t/{token}\n').encode())
        self._start_proxy()

    def wait_ready(self, timeout_s: float = 10.0) -> None:
        """Poll /healthz until the relay answers; any HTTP response
        counts as up."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                conn = http.client.HTTPConnection(
                    '127.0.0.1', self._relay_port, timeout=1)
                try:
                    conn.request('GET', '/healthz')
                    conn.getresponse().read()
                    return
                finally:
                    conn.close()
            except OSError:
                time.sleep(0.05)
        raise RuntimeError('relay failed to start')

    def _start_proxy(self) -> None:
        self._proxy = http.server.ThreadingHTTPServer(
            ('127.0.0.1', self._proxy_port), _ProxyHandler)
        self._proxy.experimental_host = self
        self._proxy_thread = threading.Thread(
            target=self._proxy.serve_forever,
            kwargs={'poll_interval': 0.05}, daemon=True)
        self._proxy_thread.start()

    def _stop_proxy(self) -> None:
        if self._proxy is not None:
            self._proxy.shutdown()
            self._proxy.server_close()
            self._proxy = None

    def _post_shutdown(self) -> None:
        token = read_private(
            self._data_dir / 'relay-token', 256).decode().strip()
        req = urllib.request.Request(
            f'http://127.0.0.1:{self._relay_port}'
            f'/t/{token}/shutdown', data=b'', method='POST')
        try:
            urllib.request.urlopen(req, timeout=5).close()
        except urllib.error.HTTPError as e:
            e.close()

    def stop(self) -> None:
        self._stop_proxy()
        if self._owns_relay:
            self._owns_relay = False
            try:
                self._post_shutdown()
            except OSError:
                pass
            # serve()'s finally closes the accounting run cleanly; the
            # join must happen before process exit or the run is left
            # dirty and the next start degrades to
            # 'durable accounting unavailable'.
            thread = self._relay_thread
            if thread is not None:
                thread.join(timeout=self._join_timeout_s)
                if thread.is_alive():
                    print('fusion-experimental-host: relay thread did '
                          'not exit; accounting run may be dirty',
                          file=sys.stderr)

    # -- journal --------------------------------------------------------

    def _journal(self, event: str, **fields) -> None:
        """Append one JSON line: ts + event + hashed refs + small enums.
        Never raw bodies, headers, tokens, or proofs."""
        entry = {'ts': self._clock(), 'event': event}
        entry.update(fields)
        try:
            append_private(self._journal_path,
                           (json.dumps(entry, sort_keys=True)
                            + '\n').encode())
        except OSError:
            pass

    # -- binding --------------------------------------------------------

    def bind_request(self, packet, model: str):
        """Decide the binding headers for one decoded GetChatMessage
        packet, or None for native-forward (no headers)."""
        relay = _relay()
        from . import auth, catalog, translate, wire
        if relay.route_for_model(model) != 'codex':
            return None
        if catalog.session_route(packet) == 'native':
            return None
        routed = translate.parse_routed_model(model)
        req_body = translate.packet_to_responses_body(
            packet, routed, {}, continuity_scope='')
        body_digest = digest(req_body)
        session = wire.text(packet, 16)
        account = auth.get_token()[1]
        profile = routed.model + ':' + routed.effort
        now = self._clock()
        with self._lock:
            state = self._sessions.get(session)
            if state is None:
                binding, secret = self._issue(session, account, profile,
                                              'launcher-genesis')
                state = {'binding': binding, 'secret': secret,
                         'profile': profile, 'account': account,
                         'epoch': binding.continuation_epoch}
                self._sessions[session] = state
            elif state['profile'] != profile:
                old = self._ledger.resolve_epoch(
                    state['binding'].continuation_binding())
                try:
                    new = self._ledger.transition_epoch(
                        old,
                        digest({'epoch': old.epoch, 'profile': profile,
                                'ts': now}),
                        'model_switch')
                except Exception:
                    self._journal(
                        'binding_refused',
                        session_ref=reference('session', session),
                        profile=profile)
                    raise
                binding, secret = self._issue(session, account, profile,
                                              new.epoch)
                self._capabilities.revoke(state['binding'].capability_id)
                state = {'binding': binding, 'secret': secret,
                         'profile': profile, 'account': account,
                         'epoch': new.epoch}
                self._sessions[session] = state
                self._journal('epoch_transition',
                              session_ref=reference('session', session),
                              reason='model_switch')
            elif now >= state['binding'].expires_at - _REISSUE_MARGIN_S:
                binding, secret = self._issue(
                    session, account, profile,
                    state['binding'].continuation_epoch)
                state = {'binding': binding, 'secret': secret,
                         'profile': profile, 'account': account,
                         'epoch': binding.continuation_epoch}
                self._sessions[session] = state
            else:
                binding, secret = state['binding'], state['secret']
            key = (session, body_digest)
            prior = self._ops.get(key)
            reused = prior is not None \
                and now - prior[1] <= self._retry_window
            op_id = prior[0] if reused else secrets.token_hex(16)
            self._ops[key] = (op_id, now)
        headers = {
            HEADER_CAPABILITY: binding.capability_id,
            HEADER_OPERATION: op_id,
            HEADER_PROOF: CapabilityStore.prove(
                secret, binding.capability_id, op_id, body_digest)}
        return {'headers': headers,
                'session_ref': reference('session', session),
                'op_ref': reference('op', op_id),
                'profile': profile,
                'reused_operation': reused}

    def translated_history(self, packet, model: str):
        """The Responses-API ``input`` list the relay will see for this
        packet (same translation as the relay, continuity scope empty)."""
        from . import translate
        routed = translate.parse_routed_model(model)
        return translate.packet_to_responses_body(
            packet, routed, {}, continuity_scope='')['input']

    def adopt_legacy_history(self, session: str, history=None) -> None:
        """Adopt a resumed session's wire history under an explicit
        'legacy_history' epoch transition.

        `devin -r` rewrites field 16, so the same conversation arrives as
        a new scope. If a prior scope of the same account/lane/profile
        has committed turns that *history* extends exactly
        (``find_carry_candidate``), those turns are carried into the new
        scope and their stored reasoning items are reinserted on the
        retry — the resumed conversation keeps its continuation. When no
        candidate matches, the history is adopted as a new genesis and
        prior reasoning is not recoverable. Raises ContinuationError when
        the transition is blocked; the caller keeps the relay's original
        failure."""
        with self._lock:
            state = self._sessions.get(session)
            if state is None:
                raise ContinuationError('continuation session unavailable')
            old = self._ledger.resolve_epoch(
                state['binding'].continuation_binding())
            carry = None
            if history is not None:
                carry = self._ledger.find_carry_candidate(old, history)
            new = self._ledger.transition_epoch(
                old, digest({'epoch': old.epoch, 'legacy': self._clock()}),
                'legacy_history', carry_from=carry)
            binding, secret = self._issue(
                session, state['account'], state['profile'], new.epoch)
            self._capabilities.revoke(state['binding'].capability_id)
            self._sessions[session] = {
                'binding': binding, 'secret': secret,
                'profile': state['profile'], 'account': state['account'],
                'epoch': new.epoch}
            self._journal('epoch_transition',
                          session_ref=reference('session', session),
                          reason='legacy_history',
                          carried=carry is not None,
                          carried_from_scope_ref=reference(
                              'scope', carry.scope()) if carry else None,
                          prior_reasoning='carried' if carry
                          else 'unavailable')

    def adopt_divergent_history(self, session: str) -> None:
        """Adopt a diverged wire history under the SAME seed as a new
        continuation genesis via an explicit 'history_divergence' epoch
        transition — the client history no longer extends the committed
        anchors and the cause is NOT established. A recent /host/compaction
        notice is evidence only (the hook's session id is a display name
        that never correlates to the wire seed); it is journaled as
        metadata, never claimed as the cause. Prior turns' reasoning is
        not recoverable — the adopted history is the new genesis. Raises
        ContinuationError when the transition is blocked; the caller
        keeps the relay's original failure."""
        with self._lock:
            state = self._sessions.get(session)
            if state is None:
                raise ContinuationError('continuation session unavailable')
            old = self._ledger.resolve_epoch(
                state['binding'].continuation_binding())
            new = self._ledger.transition_epoch(
                old, digest({'epoch': old.epoch,
                             'divergence': self._clock()}),
                'history_divergence')
            binding, secret = self._issue(
                session, state['account'], state['profile'], new.epoch)
            self._capabilities.revoke(state['binding'].capability_id)
            self._sessions[session] = {
                'binding': binding, 'secret': secret,
                'profile': state['profile'], 'account': state['account'],
                'epoch': new.epoch}
            self._journal(
                'epoch_transition',
                session_ref=reference('session', session),
                reason='history_divergence',
                prior_reasoning='unavailable',
                compaction_note_correlation='unmatched',
                recent_compaction_notice=bool(
                    self._coordinator.recent_compaction_notice(300)))

    def _issue(self, session, account, profile, epoch):
        return self._capabilities.issue(
            client_instance_id=self._client_instance,
            account_reference=account, native_session_id=session,
            lane='lead', model_profile=profile,
            continuation_epoch=epoch,
            provenance=PROVENANCE_LAUNCHER, ttl_s=_CAPABILITY_TTL_S)


class _ProxyHandler(http.server.BaseHTTPRequestHandler):
    """Forwarding proxy: buffers every request/response (durable traffic
    is buffered anyway; this harness also buffers native streams)."""

    def log_message(self, *args):
        pass

    @property
    def _host(self) -> ExperimentalHost:
        return self.server.experimental_host

    def do_GET(self):
        self._proxy_request(b'')

    def do_POST(self):
        relay = _relay()
        try:
            length = int(self.headers.get('Content-Length') or 0)
        except ValueError:
            self.send_error(400)
            return
        if length < 0 or length > relay.MAX_REQUEST_BYTES:
            self.send_error(413)
            return
        self._proxy_request(self.rfile.read(length))

    def _proxy_request(self, body: bytes) -> None:
        host = self._host
        bound = None
        extra = {}
        ctype = self.headers.get('Content-Type', '').split(
            ';', 1)[0].strip().lower()
        is_chat = self.command == 'POST' \
            and self.path.split('?', 1)[0].endswith('/GetChatMessage') \
            and ctype in ('application/connect+proto',
                          'application/proto')
        if is_chat:
            try:
                relay = _relay()
                packets = list(relay._decode_packets(
                    body, framed=(ctype == 'application/connect+proto')))
            except Exception:
                packets = []
            if len(packets) == 1 and 21 in packets[0] \
                    and 16 in packets[0]:
                from . import wire
                model = wire.text(packets[0], 21)
                try:
                    bound = host.bind_request(packets[0], model)
                except Exception as e:
                    host._journal('bind_error',
                                  error=type(e).__name__)
                    bound = None
                if bound:
                    extra = bound['headers']
        forwarded = self._forward_watched(body, extra, bound)
        if forwarded is None:
            return  # client gone — nothing to write
        status, resp_headers, resp_body = forwarded
        if bound and is_chat:
            adoption_reason = _epoch_adoption_reason(resp_body)
        else:
            adoption_reason = None
        if adoption_reason is not None:
            # The wire history cannot extend the old scope: either a
            # resumed session (devin -r rewrites field 16) or a
            # diverged history under the same seed (cause unestablished
            # — a recent compaction notice is evidence, not proof).
            # Adopt it under an explicit epoch transition — prior
            # turns' reasoning is not recoverable — and re-forward the
            # same body once.
            from . import wire
            session = wire.text(packets[0], 16)
            try:
                if adoption_reason == 'legacy_history':
                    host.adopt_legacy_history(
                        session, host.translated_history(packets[0], model))
                else:
                    host.adopt_divergent_history(session)
                bound = host.bind_request(packets[0], model)
            except Exception as e:
                host._journal('bind_error', error=type(e).__name__)
            else:
                forwarded = self._forward_watched(
                    body, bound['headers'], bound)
                if forwarded is None:
                    return
                status, resp_headers, resp_body = forwarded
                host._journal('legacy_adoption_retry',
                              session_ref=bound['session_ref'],
                              op_ref=bound['op_ref'],
                              reason=adoption_reason,
                              relay_status=status)
        self.send_response(status)
        sent_ctype = False
        for name, value in resp_headers:
            if name.lower() == 'content-type':
                sent_ctype = True
            self.send_header(name, value)
        if not sent_ctype:
            self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(len(resp_body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(resp_body)
        if is_chat and bound:
            entry = {'session_ref': bound['session_ref'],
                     'op_ref': bound['op_ref'],
                     'profile': bound['profile'],
                     'reused_operation': bound['reused_operation'],
                     'relay_status': status}
            lowered = {k.lower(): v for k, v in resp_headers}
            epoch = lowered.get('x-fusion-continuation-epoch')
            revision = lowered.get('x-fusion-continuation-revision')
            if epoch:
                entry['epoch_ref'] = reference('epoch', epoch)
            if revision:
                try:
                    entry['revision'] = int(revision)
                except ValueError:
                    pass
            host._journal('request_bound', **entry)

    def _client_gone(self) -> bool:
        """Probe the client socket: nonzero SO_ERROR, or a readable
        socket whose peek is empty, means the client is gone.
        Never consumes bytes."""
        try:
            if self.connection.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR):
                return True
            ready, _, _ = select.select([self.connection], [], [], 0)
            if not ready:
                return False
            return self.connection.recv(
                1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b''
        except BlockingIOError:
            return False
        except (OSError, ValueError):
            return True

    def _abort_upstream(self, sockets, resp) -> None:
        """Abort the in-flight upstream request so the relay sees its
        client (this proxy) disconnect. The relay's probe only counts a
        nonzero SO_ERROR — an abortive close (SO_LINGER 0) delivers an
        RST. shutdown() on the lingering socket sends the RST and wakes
        the worker thread blocked in read(); socket.close() alone would
        defer the real close while the urllib response's makefile holds
        the fd, so the fd is detached and closed directly."""
        for s in sockets:
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             struct.pack('ii', 1, 0))
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                os.close(s.detach())
            except OSError:
                try:
                    s.close()
                except OSError:
                    pass
        if resp is not None:
            try:
                resp.fp.raw._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                resp.close()
            except Exception:
                pass

    def _forward_watched(self, body: bytes, extra: dict, bound):
        """Run the upstream forward in a worker thread while polling the
        client socket every 0.25s. On client-gone the upstream connection
        is aborted so the relay observes a disconnect and follows its own
        cancellation path; cancellation latency is bounded by the
        provider's event cadence, not by this proxy. Returns the
        (status, headers, body) triple, or None when the client is gone.
        """
        host = self._host
        result: dict = {}
        sockets: list = []

        def work():
            try:
                result['out'] = self._forward(body, extra,
                                              tracker=sockets.append)
            except Exception as e:
                result['err'] = e

        worker = threading.Thread(target=work, daemon=True)
        worker.start()
        while worker.is_alive():
            worker.join(0.25)
            if worker.is_alive() and self._client_gone():
                self._abort_upstream(
                    sockets, getattr(self, '_upstream_resp', None))
                # Journal first: the abort has been issued regardless of
                # how long the worker takes to notice the RST.
                entry = {'phase': 'upstream_in_flight'}
                if bound:
                    entry['session_ref'] = bound['session_ref']
                    entry['op_ref'] = bound['op_ref']
                host._journal('client_disconnected', **entry)
                worker.join(5)
                return None
        if 'err' in result:
            return 502, [], b''
        return result['out']

    def _forward(self, body: bytes, extra: dict, tracker=None):
        relay = _relay()
        from .transport import RedirectBlocked, open_request
        named = {t.strip().lower()
                 for t in (self.headers.get('Connection')
                           or '').split(',')}
        out_headers = {
            k: v for k, v in self.headers.items()
            if k.lower() not in relay.HOP_BY_HOP
            and k.lower() not in named
            and k.lower() not in
            (HEADER_CAPABILITY.lower(), HEADER_OPERATION.lower(),
             HEADER_PROOF.lower())}
        out_headers.update(extra)
        url = f'http://127.0.0.1:{self._host._relay_port}{self.path}'
        method = self.command if self.command in ('GET', 'POST') \
            else 'POST'
        req = urllib.request.Request(
            url, data=(body if method == 'POST' else None),
            headers=out_headers, method=method)
        try:
            resp = open_request(req, timeout=_FORWARD_TIMEOUT_S,
                                tracker=tracker)
        except urllib.error.HTTPError as e:
            resp = e
        except (OSError, urllib.error.URLError):
            return 502, [], b''
        self._upstream_resp = resp
        try:
            with resp:
                data = resp.read(relay.MAX_REQUEST_BYTES + 1)
                status = getattr(resp, 'status', 200)
                hdrs = []
                keep = set(relay.RESPONSE_PASS) | {
                    h.lower() for h in _EPOCH_HEADERS}
                for name, value in resp.headers.items():
                    if name.lower() in keep:
                        hdrs.append((name, value))
                ctype = resp.headers.get('Content-Type')
                if ctype:
                    hdrs.append(('Content-Type', ctype))
                return status, hdrs, data
        except (OSError, ValueError):
            return 502, [], b''
        finally:
            self._upstream_resp = None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog='fusion-experimental-host',
        description='EXPERIMENTAL launcher-provenance durable-'
                    'continuation host (UNQUALIFIED)')
    parser.add_argument('--data-dir', required=True)
    parser.add_argument('--relay-port', type=int, default=8940)
    parser.add_argument('--proxy-port', type=int, default=8941)
    parser.add_argument('--key-service',
                        default='ai.fusion-codex-relay.experimental')
    parser.add_argument('--key-account', default=None,
                        help='hex64 key account; default sha256 of the '
                             'resolved data dir path')
    parser.add_argument('--no-create-key', action='store_true')
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir).resolve()
    os.environ['FUSION_RELAY_CONTINUATION'] = 'durable'
    os.environ['FUSION_RELAY_DATA_DIR'] = str(data_dir)

    # Imported only after the environment is pinned: relay's module
    # constants read FUSION_RELAY_* at import time.
    relay = _relay()  # noqa: F841 — import ordering is the point
    from .keychain import MacOSKeychain
    from .continuation import ContinuationLedger

    key_account = args.key_account or hashlib.sha256(
        str(data_dir).encode()).hexdigest()
    try:
        key = MacOSKeychain(service=args.key_service).load(
            key_account, create=not args.no_create_key)
    except ContinuationError as e:
        print(f'fusion-experimental-host: key_store unavailable: {e}',
              file=sys.stderr)
        return 3

    try:
        ledger = ContinuationLedger(
            data_dir / 'continuation.sqlite3', key)
    except ContinuationError as e:
        print('fusion-experimental-host: continuation store '
              f'unavailable: {e}', file=sys.stderr)
        return 3
    host = ExperimentalHost(
        data_dir=data_dir, relay_port=args.relay_port,
        proxy_port=args.proxy_port, ledger=ledger,
        capabilities=CapabilityStore(),
        journal_path=data_dir / 'host-journal.jsonl')
    try:
        host.start()
    except Exception:
        ledger.close()
        raise
    print('fusion-experimental-host: proxy on '
          f'127.0.0.1:{args.proxy_port}, relay on '
          f'127.0.0.1:{args.relay_port}, env file '
          f'{data_dir}/host.env, provenance=launcher_process_ownership '
          '(UNQUALIFIED experimental binding)', flush=True)

    stop = threading.Event()

    def _signal(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)
    stop.wait()
    host.stop()
    ledger.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
