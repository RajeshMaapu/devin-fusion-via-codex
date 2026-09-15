"""Raw native (Cognition) streaming pump over httpx.

``stream_native`` POSTs a request body and forwards raw response bytes
to an async ``write`` callback with read-behind-write backpressure,
per-50ms cancellation checks, and bounded shielded cleanup that survives
repeated task cancellation. ``ConnectObserver`` incrementally parses
Connect envelope frames for usage/metadata without buffering the whole
response.
"""
from __future__ import annotations

import asyncio
import json

import httpx

_REDIRECTS = frozenset((301, 302, 303, 307, 308))


class StreamFailure(RuntimeError):
    pass


class StreamCleanupError(StreamFailure):
    """Upstream termination could not be confirmed within bounds."""

    def __init__(self, msg="upstream termination unconfirmed"):
        super().__init__(msg)
        self.termination_confirmed = False


class ConnectObserver:
    """Incremental Connect-envelope parser over streamed raw bytes.

    ``on_frame`` receives each complete non-trailer frame verbatim —
    the 5-byte envelope header plus payload. Exactly one trailer frame
    (flag 2, JSON object payload) must terminate the stream; any bytes
    after it or a truncated tail at ``finish()`` is a StreamFailure.
    Compressed data frames (flag 1) are passed through unchanged.
    """

    def __init__(self, on_frame, limit=16 << 20):
        self._on_frame = on_frame
        self._limit = limit
        self._buf = bytearray()
        self._trailer = False

    def feed(self, chunk):
        if self._trailer:
            raise StreamFailure("bytes after end-of-stream trailer")
        self._buf += chunk
        while True:
            if len(self._buf) < 5:
                return
            flags = self._buf[0]
            if flags not in (0, 1, 2):
                raise StreamFailure("invalid frame flags")
            length = int.from_bytes(self._buf[1:5], "big")
            if length > self._limit:
                raise StreamFailure("frame payload too large")
            if len(self._buf) < 5 + length:
                return
            raw = bytes(self._buf[:5 + length])
            del self._buf[:5 + length]
            if flags & 2:
                try:
                    meta = json.loads(raw[5:])
                except ValueError:
                    raise StreamFailure("invalid trailer") from None
                if not isinstance(meta, dict):
                    raise StreamFailure("invalid trailer")
                self._trailer = True
                if self._buf:
                    raise StreamFailure("bytes after end-of-stream trailer")
                return
            self._on_frame(raw)

    def finish(self):
        if self._buf or not self._trailer:
            raise StreamFailure("incomplete stream")


async def stream_native(url: str, body: bytes, headers: dict, *,
                        on_headers, write, check, observe,
                        on_cleanup=None,
                        idle_timeout=30.0, total_timeout=120.0,
                        max_bytes=64 << 20):
    """Stream a raw upstream POST response into ``write``.

    ``check`` runs before the request and around every header, read, and
    write boundary. ``on_headers(status, headers)`` is awaited once.
    ``observe(chunk)`` sees every raw chunk before it is written.
    Redirects, declared transport trailers, over-limit bodies, transport
    errors, and the total deadline all raise StreamFailure; check()
    exceptions (RequestCancelled) and task cancellation propagate.
    Returns total raw bytes written.
    """
    check()
    out_headers = {k: v for k, v in headers.items()
                   if k.lower() != "accept-encoding"}
    out_headers["Accept-Encoding"] = "identity"
    timeout = httpx.Timeout(
        idle_timeout, connect=min(10.0, idle_timeout),
        pool=min(10.0, idle_timeout), write=idle_timeout)
    client = httpx.AsyncClient(
        follow_redirects=False, trust_env=False,
        timeout=timeout,
        transport=httpx.AsyncHTTPTransport(
            retries=0, trust_env=False,
            limits=httpx.Limits(max_connections=1,
                                max_keepalive_connections=0)))
    total = 0

    async def transfer():
        nonlocal total
        async with client.stream("POST", url, headers=out_headers,
                                 content=body) as resp:
            if resp.status_code in _REDIRECTS:
                raise StreamFailure("upstream redirect blocked")
            if "trailer" in resp.headers:
                raise StreamFailure("upstream declared transport trailers")
            check()
            await on_headers(resp.status_code, dict(resp.headers))
            async for chunk in resp.aiter_raw():
                check()
                total += len(chunk)
                if total > max_bytes:
                    raise StreamFailure("upstream response too large")
                observe(chunk)
                await write(chunk)
                check()
        return total

    async def _cleanup(task):
        """Cancel the transfer, close the client, confirm termination.

        Bounded to ~1s via ``asyncio.wait`` (never abandons the shield);
        absorbs repeated outer cancellation during the wait, then
        re-raises it. A closer that errors, resists cancellation, or
        overruns the bounds reports ``on_cleanup(False)`` and raises
        StreamCleanupError — termination is never claimed unconfirmed.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 1.0
        if not task.done():
            task.cancel()
        closer = asyncio.ensure_future(client.aclose())
        pending = {task, closer}
        cancelled = None
        confirmed = True
        while pending:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                done, pending = await asyncio.wait(
                    pending, timeout=remaining)
            except asyncio.CancelledError as e:
                cancelled = cancelled if cancelled is not None else e
                continue
            for d in done:
                if d.cancelled():
                    if d is closer:
                        # a cancelled close did not run to completion
                        confirmed = False
                    continue
                exc = d.exception()
                if exc is not None and d is closer:
                    confirmed = False
        if pending:
            # deadline expired with work outstanding; cancelling it now
            # does not prove termination completed
            confirmed = False
            for p in pending:
                p.cancel()
            try:
                done, pending = await asyncio.wait(pending, timeout=0.1)
            except asyncio.CancelledError as e:
                cancelled = cancelled if cancelled is not None else e
                done = set()
            for d in done:
                if not d.cancelled():
                    d.exception()
        if on_cleanup is not None:
            on_cleanup(confirmed)
        if not confirmed:
            raise StreamCleanupError() from cancelled
        if cancelled is not None:
            raise cancelled

    async def pump():
        task = asyncio.ensure_future(transfer())
        try:
            while True:
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task), 0.05)
                except asyncio.TimeoutError:
                    check()
        finally:
            await _cleanup(task)

    try:
        return await asyncio.wait_for(pump(), total_timeout)
    except asyncio.TimeoutError:
        raise StreamFailure("stream deadline exceeded") from None
    except httpx.HTTPError as e:
        raise StreamFailure("upstream transport failed") from e
