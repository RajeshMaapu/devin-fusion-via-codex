"""Native streaming tests: loopback HTTP only, no real providers."""
from __future__ import annotations

import asyncio
import gzip
import http.client
import json
import pathlib
import socket
import tempfile
import threading
import time
import unittest

import httpx
from unittest.mock import patch

from fusion_relay import catalog, relay, wire
from fusion_relay.lifecycle import RequestContext
from fusion_relay.native_stream import (
    ConnectObserver, StreamCleanupError, StreamFailure, stream_native)


class Upstream:
    """Scripted one-shot loopback server; close() always reaps it."""

    def __init__(self, script):
        self._script = script
        self.requests = []
        self._stop = threading.Event()
        self.conn = None
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        conn = None
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = self.sock.accept()
                    break
                except socket.timeout:
                    continue
                except OSError:
                    return
            if conn is None:
                return
            self.conn = conn
            conn.settimeout(10)
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(4096)
            head, _, rest = data.partition(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    length = int(line.split(b":", 1)[1])
            while len(rest) < length:
                rest += conn.recv(4096)
            self.requests.append(rest)
            self._script(conn, rest)
        finally:
            if conn is not None:
                conn.close()

    def close(self):
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        conn = self.conn
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        self.thread.join(10)


def simple_response(body=b"", status=200, ctype="application/proto",
                    extra=()):
    head = (b"HTTP/1.1 %d X\r\nContent-Type: %s\r\n"
            b"Content-Length: %d\r\n" % (status, ctype.encode(),
                                        len(body)))
    return head + b"".join(extra) + b"\r\n" + body


def drive(url, body=b"{}", check=None, observe=None, write=None,
          on_headers=None, on_cleanup=None, idle=5.0, total=10.0,
          max_bytes=64 << 20):
    """Run stream_native; return (result, got) or raise the error."""
    got = {"status": None, "headers": None, "chunks": [],
           "observed": 0}
    obs = observe
    if obs is None:
        def obs(chunk):
            got["observed"] += 1

    async def _on_headers(status, headers):
        got["status"] = status
        got["headers"] = headers

    async def _write(chunk):
        got["chunks"].append(chunk)

    async def main():
        return await stream_native(
            url, body, {},
            on_headers=on_headers or _on_headers,
            write=write or _write, check=check or (lambda: None),
            observe=obs, on_cleanup=on_cleanup,
            idle_timeout=idle, total_timeout=total,
            max_bytes=max_bytes)

    return asyncio.run(main()), got


def drive_bg(*a, **kw):
    """Run drive() on a thread; return (thread, box)."""
    box = {}

    def go():
        try:
            box["result"] = drive(*a, **kw)
        except BaseException as e:
            box["error"] = e
    t = threading.Thread(target=go, daemon=True)
    t.start()
    return t, box


def connect_response(frames, status=200, ctype="application/connect+proto",
                     extra=()):
    body = b"".join(frames)
    head = (b"HTTP/1.1 %d X\r\nContent-Type: %s\r\n"
            b"Content-Length: %d\r\n" % (status, ctype.encode(),
                                        len(body)))
    return head + b"".join(extra) + b"\r\n" + body


class ObserverTest(unittest.TestCase):
    def test_frames_split_across_feeds(self):
        got = []
        obs = ConnectObserver(got.append)
        f1 = wire.frame(b"one")
        f2 = wire.frame(b"two")
        obs.feed(f1[:3])
        obs.feed(f1[3:] + f2 + wire.end_stream())
        self.assertEqual(got, [f1, f2])
        obs.finish()

    def test_error_trailer_is_valid_terminal(self):
        got = []
        obs = ConnectObserver(got.append)
        trailer = wire.frame(
            json.dumps({"error": {"code": "x"}}).encode(), 2)
        obs.feed(wire.frame(b"a") + trailer)
        self.assertEqual(got, [wire.frame(b"a")])
        obs.finish()

    def test_compressed_data_frame_passthrough(self):
        got = []
        obs = ConnectObserver(got.append)
        comp = wire.frame(gzip.compress(b"payload"), 1)
        obs.feed(comp + wire.end_stream())
        self.assertEqual(got, [comp])
        obs.finish()

    def test_bytes_after_trailer_rejected(self):
        obs = ConnectObserver(lambda f: None)
        obs.feed(wire.end_stream())
        with self.assertRaises(StreamFailure):
            obs.feed(b"x")
        obs = ConnectObserver(lambda f: None)
        with self.assertRaises(StreamFailure):
            obs.feed(wire.end_stream() + b"x")

    def test_bad_flags_and_oversize(self):
        obs = ConnectObserver(lambda f: None)
        with self.assertRaises(StreamFailure):
            obs.feed(b"\x09" + b"\x00" * 4)
        obs = ConnectObserver(lambda f: None, limit=4)
        with self.assertRaises(StreamFailure):
            obs.feed(b"\x00" + (100).to_bytes(4, "big") + b"x" * 5)

    def test_invalid_trailer_json(self):
        obs = ConnectObserver(lambda f: None)
        with self.assertRaises(StreamFailure):
            obs.feed(wire.frame(b"not json", 2))

    def test_finish_requires_exactly_one_trailer(self):
        obs = ConnectObserver(lambda f: None)
        with self.assertRaises(StreamFailure):
            obs.finish()
        obs.feed(wire.frame(b"a"))
        with self.assertRaises(StreamFailure):
            obs.finish()
        obs.feed(wire.end_stream())
        obs.finish()


class StreamNativeTest(unittest.TestCase):
    def upstream(self, script):
        up = Upstream(script)
        self.addCleanup(self._stop, up)
        return up

    def _stop(self, up):
        up.close()
        self.assertFalse(up.thread.is_alive(),
                         "upstream helper thread did not exit")

    def test_frame_delivered_before_upstream_completes(self):
        p1, p2 = b"part-one", b"part-two"
        order = []
        release = threading.Event()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: "
                         b"application/proto\r\nTransfer-Encoding: chunked"
                         b"\r\n\r\n")
            conn.sendall(b"%x\r\n%s\r\n" % (len(p1), p1))
            order.append("s1")
            release.wait(5)
            conn.sendall(b"%x\r\n%s\r\n" % (len(p2), p2))
            order.append("s2")
            conn.sendall(b"0\r\n\r\n")

        up = self.upstream(script)

        got_chunks = []

        async def write(chunk):
            got_chunks.append(chunk)
            if len(order) == 1:
                order.append("w1")
                release.set()

        total, got = drive(f"http://127.0.0.1:{up.port}/x", write=write)
        self.assertEqual(got_chunks, [p1, p2])
        self.assertEqual(order, ["s1", "w1", "s2"])
        self.assertEqual(total, len(p1) + len(p2))

    def test_byte_exact_frames_and_trailer(self):
        payload = wire.frame(wire.field(3, "hello"))
        body = payload + wire.end_stream()
        up = self.upstream(
            lambda c, r: c.sendall(connect_response(
                [payload, wire.end_stream()])))
        total, got = drive(f"http://127.0.0.1:{up.port}/x")
        self.assertEqual(b"".join(got["chunks"]), body)

    def test_http_content_encoding_preserved_raw(self):
        raw = gzip.compress(b"encoded-body")
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(raw, ctype="application/connect+proto",
                            extra=[b"Content-Encoding: gzip\r\n"])))
        total, got = drive(f"http://127.0.0.1:{up.port}/x")
        self.assertEqual(b"".join(got["chunks"]), raw)

    def test_error_status_and_body_preserved(self):
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(b"boom", status=500)))
        total, got = drive(f"http://127.0.0.1:{up.port}/x")
        self.assertEqual(got["status"], 500)
        self.assertEqual(b"".join(got["chunks"]), b"boom")

    def test_redirect_never_reaches_target(self):
        hits = []
        target = self.upstream(lambda c, r: hits.append(1))
        up = self.upstream(lambda c, r: c.sendall(
            b"HTTP/1.1 302 X\r\nContent-Length: 0\r\n"
            b"Location: http://127.0.0.1:%d/\r\n\r\n" % target.port))
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x")
        self.assertEqual(hits, [])

    def test_trailer_header_rejected(self):
        up = self.upstream(lambda c, r: c.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
            b"Trailer: grpc-status\r\n\r\n"))
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x")

    def test_backpressure_holds_next_read(self):
        # c2 is sent while write(c1) is still blocked: if reads were not
        # gated behind writes, observe would see c2 immediately.
        c1, c2 = b"chunk-1", b"chunk-2"
        observed = []
        send2 = threading.Event()
        release = threading.Event()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\n")
            conn.sendall(c1)
            send2.wait(5)
            conn.sendall(c2)

        up = self.upstream(script)

        def observe(chunk):
            observed.append(chunk)

        async def write(chunk):
            while not release.is_set():
                await asyncio.sleep(0.01)

        thread, box = drive_bg(f"http://127.0.0.1:{up.port}/x",
                               observe=observe, write=write)
        deadline = time.time() + 5
        while not observed and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(observed, [c1])
        send2.set()           # c2 now sits in the socket buffer
        time.sleep(0.3)
        self.assertEqual(observed, [c1])  # write still gates the read
        release.set()
        thread.join(10)
        self.assertNotIn("error", box)
        self.assertEqual(observed, [c1, c2])

    def test_idle_timeout(self):
        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
            time.sleep(3)

        up = self.upstream(script)
        start = time.time()
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x", idle=0.3, total=10)
        self.assertLess(time.time() - start, 3)

    def test_total_timeout_with_trickle(self):
        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked"
                         b"\r\n\r\n")
            for _ in range(10):
                try:
                    conn.sendall(b"1\r\nx\r\n")
                except OSError:
                    return
                time.sleep(0.15)

        up = self.upstream(script)
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x", idle=1.0, total=0.4)

    def test_max_bytes(self):
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(b"x" * 100)))
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x", max_bytes=10)

    def test_cancel_before_request(self):
        # check() fires before any connection is attempted
        ctx = RequestContext(timeout=60)
        ctx.cancel()
        with self.assertRaises(relay.RequestCancelled):
            drive("http://127.0.0.1:1/x", check=ctx.check)

    def test_cancel_during_read(self):
        gate = threading.Event()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n"
                         b"ab")
            gate.wait(5)

        up = self.upstream(script)
        calls = []

        def check():
            calls.append(1)
            if len(calls) > 4:
                raise relay.RequestCancelled("cancelled")

        with self.assertRaises(relay.RequestCancelled):
            drive(f"http://127.0.0.1:{up.port}/x", check=check)
        gate.set()

    def test_cancel_blocked_write_no_orphans(self):
        # second cancel lands inside cleanup; it is absorbed there, the
        # held client close is released, and cancellation is re-raised
        close_started = threading.Event()
        release_close = threading.Event()
        in_write = threading.Event()
        hold = threading.Event()

        async def held_close(client):
            close_started.set()
            while not release_close.is_set():
                await asyncio.sleep(0.01)

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n"
                         b"ab")
            hold.wait(5)

        up = self.upstream(script)
        box = {}
        confirmed = []

        async def write(chunk):
            in_write.set()
            await asyncio.sleep(30)

        async def scen():
            with patch.object(httpx.AsyncClient, "aclose", held_close):
                task = asyncio.ensure_future(stream_native(
                    f"http://127.0.0.1:{up.port}/x", b"{}", {},
                    on_headers=lambda s, h: asyncio.sleep(0),
                    write=write, check=lambda: None,
                    observe=lambda c: None,
                    on_cleanup=confirmed.append,
                    idle_timeout=30, total_timeout=60))
                await asyncio.to_thread(in_write.wait, 5)
                task.cancel()
                self.assertTrue(await asyncio.to_thread(
                    close_started.wait, 5))
                task.cancel()
                release_close.set()
                try:
                    await task
                except asyncio.CancelledError:
                    box["cancelled"] = True
            await asyncio.sleep(0.1)   # let wait_for/httpcore tasks settle
            box["left"] = [x for x in asyncio.all_tasks()
                           if not x.done()
                           and x is not asyncio.current_task()]

        asyncio.run(scen())
        hold.set()
        self.assertTrue(box["cancelled"])
        self.assertEqual(confirmed, [True])
        self.assertEqual(box["left"], [])

    def test_close_error_reports_unconfirmed(self):
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(b"ok")))
        confirmed = []

        async def bad_close(client):
            raise RuntimeError("close boom")

        with patch.object(httpx.AsyncClient, "aclose", bad_close):
            with self.assertRaises(StreamCleanupError):
                drive(f"http://127.0.0.1:{up.port}/x",
                      on_cleanup=confirmed.append)
        self.assertEqual(confirmed, [False])

    def test_hung_close_cancellable_still_unconfirmed(self):
        # closer honours cancellation, but only after the 1s deadline —
        # a cancelled close is not proof of completed termination
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(b"ok")))
        confirmed = []
        box = {}

        async def hung_close(client):
            await asyncio.sleep(30)

        async def scen():
            with patch.object(httpx.AsyncClient, "aclose", hung_close):
                try:
                    await stream_native(
                        f"http://127.0.0.1:{up.port}/x", b"{}", {},
                        on_headers=lambda s, h: asyncio.sleep(0),
                        write=lambda c: asyncio.sleep(0),
                        check=lambda: None, observe=lambda c: None,
                        on_cleanup=confirmed.append,
                        idle_timeout=30, total_timeout=60)
                except StreamCleanupError as e:
                    box["err"] = e
            await asyncio.sleep(0.1)
            box["left"] = [x for x in asyncio.all_tasks()
                           if not x.done()
                           and x is not asyncio.current_task()]

        asyncio.run(scen())
        self.assertIs(box["err"].termination_confirmed, False)
        self.assertEqual(confirmed, [False])
        self.assertEqual(box["left"], [])

    def test_stalled_close_reports_unconfirmed(self):
        up = self.upstream(lambda c, r: c.sendall(
            simple_response(b"ok")))
        confirmed = []
        box = {}

        async def stall_close(client):
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                await asyncio.sleep(30)   # resists the first cancel only

        async def scen():
            with patch.object(httpx.AsyncClient, "aclose", stall_close):
                try:
                    await stream_native(
                        f"http://127.0.0.1:{up.port}/x", b"{}", {},
                        on_headers=lambda s, h: asyncio.sleep(0),
                        write=lambda c: asyncio.sleep(0),
                        check=lambda: None, observe=lambda c: None,
                        on_cleanup=confirmed.append,
                        idle_timeout=30, total_timeout=60)
                except StreamCleanupError as e:
                    box["err"] = e
            # unconfirmed closer is still pending: cancel it again and
            # let it die on this loop so nothing outlives asyncio.run
            for t in asyncio.all_tasks():
                if t is not asyncio.current_task():
                    t.cancel()
            await asyncio.sleep(0.1)
            box["left"] = [x for x in asyncio.all_tasks()
                           if not x.done()
                           and x is not asyncio.current_task()]

        asyncio.run(scen())
        self.assertIs(box["err"].termination_confirmed, False)
        self.assertEqual(confirmed, [False])
        self.assertEqual(box["left"], [])

    def test_cancel_before_headers_closes_upstream(self):
        # upstream holds the response head; cancellation must close the
        # client socket so the peer sees EOF promptly and tasks join
        eof = {}
        got_eof = threading.Event()

        def script(conn, req):
            try:
                eof["data"] = conn.recv(1)
            except OSError as e:
                eof["data"] = e
            got_eof.set()

        up = self.upstream(script)
        box = {}

        async def scen():
            task = asyncio.ensure_future(stream_native(
                f"http://127.0.0.1:{up.port}/x", b"{}", {},
                on_headers=lambda s, h: asyncio.sleep(0),
                write=lambda c: asyncio.sleep(0),
                check=lambda: None, observe=lambda c: None,
                idle_timeout=30, total_timeout=60))
            await asyncio.sleep(0.5)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                box["cancelled"] = True
            await asyncio.sleep(0.1)
            box["left"] = [x for x in asyncio.all_tasks()
                           if not x.done()
                           and x is not asyncio.current_task()]

        asyncio.run(scen())
        self.assertTrue(box["cancelled"])
        self.assertEqual(box["left"], [])
        self.assertTrue(got_eof.wait(5))
        self.assertEqual(eof["data"], b"")

    def test_malformed_stream_aborts(self):
        up = self.upstream(lambda c, r: c.sendall(
            connect_response([b"\x09" + b"\x00" * 4 + b"junk"])))
        obs = ConnectObserver(lambda f: None)
        with self.assertRaises(StreamFailure):
            drive(f"http://127.0.0.1:{up.port}/x", observe=obs.feed)

    def test_truncated_tail_detected_by_finish(self):
        partial = wire.end_stream()[:3]
        up = self.upstream(lambda c, r: c.sendall(
            connect_response([wire.frame(b"a"), partial])))
        obs = ConnectObserver(lambda f: None)
        total, got = drive(f"http://127.0.0.1:{up.port}/x",
                           observe=obs.feed)
        with self.assertRaises(StreamFailure):
            obs.finish()


class HalfCloseTest(unittest.TestCase):
    def test_peer_write_shutdown_is_not_disconnect(self):
        s1, s2 = socket.socketpair()
        try:
            handler = object.__new__(relay.Handler)
            handler.connection = s1
            s2.shutdown(socket.SHUT_WR)
            self.assertFalse(handler._client_disconnected())
        finally:
            s1.close()
            s2.close()


class QuietServer(relay._BoundedServer):
    def handle_error(self, request, client_address):
        err = __import__("sys").exc_info()[1]
        if isinstance(err, (ConnectionResetError, BrokenPipeError,
                            http.client.IncompleteRead)):
            return
        raise err


class HandlerE2ETest(unittest.TestCase):
    """Real relay + real upstream over loopback; native model streams."""

    TOKEN = "native-test-token-000"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        d = pathlib.Path(self._tmp.name)
        for attr, val in (
                ("DATA_DIR", d), ("TOKEN_PATH", d / "relay-token"),
                ("REQUESTS_LOG", d / "requests.jsonl"),
                ("STATS_PATH", d / "stats.json"),
                ("_RELAY_TOKEN", self.TOKEN)):
            p = patch.object(relay, attr, val)
            self.addCleanup(p.stop)
            p.start()
        catalog.reset()
        self.addCleanup(catalog.reset)
        catalog.attach_store(d / "routes.json")
        self.server = QuietServer(("127.0.0.1", 0), relay.Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05})
        self._thread.start()
        self.addCleanup(
            lambda: self.assertFalse(self._thread.is_alive()))
        self.addCleanup(self._thread.join, 10)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.upstreams = []

    def _stop(self, up):
        up.close()
        self.assertFalse(up.thread.is_alive(),
                         "upstream helper thread did not exit")

    def add_upstream(self, script):
        up = Upstream(script)
        self.addCleanup(self._stop, up)
        self.upstreams.append(up)
        return up.port

    def _chat(self, model="swe-2-medium"):
        inner = wire.field(2, 1) + wire.field(3, "hi")
        packet = (wire.field(3, inner) + wire.field(16, "s-nat")
                  + wire.field(21, model))
        return wire.frame(packet) + wire.end_stream()

    def _post(self, body, ctype="application/connect+proto"):
        conn = http.client.HTTPConnection("127.0.0.1", self.port,
                                          timeout=15)
        conn.request("POST", f"/t/{self.TOKEN}"
                     "/xexerra.chat.v1.ChatService/GetChatMessage",
                     body=body, headers={"Content-Type": ctype})
        return conn

    def _records(self):
        # the record lands in the handler's finally, just after the
        # last chunk — poll briefly rather than race it
        log = pathlib.Path(self._tmp.name) / "requests.jsonl"
        for _ in range(200):
            if log.exists() and log.read_text().strip():
                return [json.loads(l)
                        for l in log.read_text().splitlines()]
            time.sleep(0.01)
        return []

    def test_native_stream_first_bytes_early(self):
        f1 = wire.frame(wire.field(3, "early"))
        f2 = wire.frame(wire.field(3, "late"))
        release = threading.Event()
        order = []

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Transfer-Encoding: chunked\r\n\r\n")
            conn.sendall(b"%x\r\n%s\r\n" % (len(f1), f1))
            order.append("s1")
            release.wait(5)
            conn.sendall(b"%x\r\n%s\r\n" % (len(f2), f2))
            conn.sendall(b"%x\r\n%s\r\n" % (len(wire.end_stream()),
                                           wire.end_stream()))
            conn.sendall(b"0\r\n\r\n")
            order.append("s2")

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        first = resp.read(len(f1))
        order.append("c1")
        release.set()
        rest = resp.read()
        conn.close()
        self.assertEqual(order, ["s1", "c1", "s2"])
        self.assertEqual(first + rest, f1 + f2 + wire.end_stream())
        rec = [r for r in self._records()
               if r.get("route") == "cognition-forward"]
        self.assertEqual(len(rec), 1)
        self.assertEqual(rec[0]["upstream_status"], 200)

    def test_native_half_close_serves_reply(self):
        # client sends the full request then FINs its write side; the
        # half-close must not cancel the in-flight native stream
        body = wire.frame(wire.field(3, "ok")) + wire.end_stream()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Content-Length: %d\r\n\r\n" % len(body))
            conn.sendall(body)

        port = self.add_upstream(script)
        req = (b"POST /t/%s/xexerra.chat.v1.ChatService/GetChatMessage "
               b"HTTP/1.1\r\nHost: 127.0.0.1\r\n"
               b"Content-Type: application/connect+proto\r\n"
               b"Content-Length: %d\r\n\r\n"
               % (self.TOKEN.encode(), len(self._chat()))) + self._chat()
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            s = socket.create_connection(("127.0.0.1", self.port),
                                         timeout=15)
            s.sendall(req)
            s.shutdown(socket.SHUT_WR)
            data = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            s.close()
        self.assertIn(b"200", data.split(b"\r\n", 1)[0])
        self.assertTrue(data.endswith(b"0\r\n\r\n"))
        self.assertIn(body, data)

    def test_native_header_allowlist(self):
        raw = gzip.compress(b"opaque")
        body = raw

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Content-Length: %d\r\n"
                         b"Content-Encoding: gzip\r\n"
                         b"Set-Cookie: secret=1\r\n"
                         b"X-Internal-Token: s3cret\r\n"
                         b"Connection: grpc-status\r\n"
                         b"grpc-status: 0\r\n\r\n" % len(body))
            conn.sendall(body)

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
        self.assertEqual(data, raw)
        self.assertEqual(resp.getheader("content-encoding"), "gzip")
        self.assertEqual(resp.getheader("transfer-encoding"), "chunked")
        self.assertIsNone(resp.getheader("set-cookie"))
        self.assertIsNone(resp.getheader("x-internal-token"))
        # connection-nominated field is stripped despite RESPONSE_PASS
        self.assertIsNone(resp.getheader("grpc-status"))
        self.assertIsNone(resp.getheader("content-length"))

    def test_native_usage_observed(self):
        usage = wire.field(2, 11) + wire.field(3, 7)
        msg = wire.field(3, "answer") + wire.field(7, usage)
        body = wire.frame(msg) + wire.end_stream()

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Content-Length: %d\r\n\r\n" % len(body))
            conn.sendall(body)

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(data, body)
        rec = [r for r in self._records()
               if r.get("route") == "cognition-forward"]
        self.assertEqual(rec[0]["cognition_usage"]["input"], 11)
        self.assertEqual(rec[0]["cognition_usage"]["output"], 7)

    def test_native_encoded_body_raw_usage_unknown(self):
        raw = gzip.compress(b"opaque")

        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Content-Encoding: gzip\r\n"
                         b"Content-Length: %d\r\n\r\n" % len(raw))
            conn.sendall(raw)

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
        self.assertEqual(data, raw)
        rec = [r for r in self._records()
               if r.get("route") == "cognition-forward"]
        self.assertEqual(rec[0]["cognition_usage"], "unknown")

    def test_native_upstream_error_status_preserved(self):
        def script(conn, req):
            conn.sendall(b"HTTP/1.1 503 X\r\nContent-Type: text/plain\r\n"
                         b"Content-Length: 4\r\n\r\ndead")

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
        self.assertEqual(resp.status, 503)
        self.assertEqual(data, b"dead")
        rec = [r for r in self._records()
               if r.get("route") == "cognition-forward"]
        self.assertEqual(rec[0]["upstream_status"], 503)

    def test_native_upstream_failure_no_success_trailer(self):
        def script(conn, req):
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: application/connect+proto\r\n"
                         b"Transfer-Encoding: chunked\r\n\r\n")
            conn.sendall(b"%x\r\n%s\r\n" % (len(b"junk"), b"junk"))
            conn.shutdown(socket.SHUT_RDWR)

        port = self.add_upstream(script)
        with patch.object(relay, "UPSTREAM", f"http://127.0.0.1:{port}"):
            conn = self._post(self._chat())
            resp = conn.getresponse()
            with self.assertRaises(http.client.IncompleteRead):
                resp.read()
            conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
