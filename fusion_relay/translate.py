"""Translate Devin's protobuf inference request to a Codex Responses call.

Request packet layout (observed on the wire, GetChatMessage):

- field 2  (str)        : session instructions, prepended to the system block
- field 3  (repeated)   : messages; each has
      field 2 (varint)  : source — 1=user, 2=assistant, 4=tool output, 5=instructions
      field 3 (str)     : body text / tool output body
      field 6 (repeated): tool calls {1: call_id, 2: name, 3: arguments}
      field 7 (str)     : call_id this message answers (tool outputs)
      other fields      : image payloads are detected by magic bytes and sent
                          as input_image parts; anything else consequential is
                          rejected explicitly (small scalars are ignored)
- field 10 (repeated)   : tool definitions {1: name, 2: description, 3: params JSON}
- field 16 (str)        : stable session seed (used to derive prompt_cache_key)
- field 21 (str)        : routed model id, e.g. "gpt-6-astra-high"

Response packet layout emitted back to the CLI:

- field 1 (str)         : response id
- field 3 (str)         : assistant text (per-delta in stream mode; one
                          cumulative frame in buffered mode)
- field 6 (repeated)    : tool calls {1: call_id, 2: name, 3: arguments}
- field 7 (message)     : usage {2: input, 3: output, 5: cached}
- field 5 (varint)      : finish reason — 1=stop, 10=tool_calls

Context continuity: the Codex backend returns opaque ``reasoning`` output
items (``encrypted_content`` requested via ``include``). We cache each
session's latest reasoning items keyed by ``prompt_cache_key`` and echo them
back immediately before the most recent assistant turn on the next request —
the documented store:false multi-turn mechanism.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import queue
import socket
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass, field as dc_field
from typing import Callable, Iterator, Optional

from . import auth
from .lifecycle import RequestCancelled
from .payload_budget import (MAX_ERROR_BODY_BYTES, PayloadReport,
                             RECOVERY_INSTRUCTION, UpstreamRejection,
                             classify_upstream_error, measure_body,
                             serialize_and_check)
from .response_assembly import ResponseAssembly
from .transport import open_request
from .usage import normalized_usage, record_usage
from .wire import Message, decode, end_stream, field, frame, iter_frames, text

CODEX_RESPONSES_URL = "https://chatgpt.com/backend-api/codex/responses"

# Routed-model suffix -> Codex reasoning effort. `max` is not a Responses-API
# effort; it is mapped to xhigh and flagged in the request record.
EFFORTS = {"none", "low", "medium", "high", "xhigh"}
MAX_EFFORT = "xhigh"

# Message sources observed on the wire.
SRC_USER, SRC_ASSISTANT, SRC_TOOL_OUT, SRC_INSTRUCTIONS = 1, 2, 4, 5

# Wire message fields we fully understand; anything else is inspected for
# image payloads, ignored if trivially small, or rejected.
KNOWN_MSG_FIELDS = {2, 3, 6, 7}
# Ignorable unknown fields: varints, and short len-delim values that are
# either tiny binary (<= 24 bytes) or printable text (ids like 36-char UUIDs).
# Anything larger that isn't a recognized image is rejected, not dropped.
IGNORABLE_FIELD_BYTES = 24
IGNORABLE_TEXT_BYTES = 128

FINISH_STOP, FINISH_TOOL_CALLS = 1, 10
MAX_SSE_LINE = 1 << 20
MAX_RESPONSE_BYTES = 64 << 20

# Provider quota snapshot: exact names only, numeric values only, never
# treated as an authoritative subscription balance.
QUOTA_HEADERS = (
    "x-codex-primary-used-percent", "x-codex-secondary-used-percent",
    "x-codex-primary-window-minutes", "x-codex-secondary-window-minutes",
    "x-codex-primary-reset-at", "x-codex-secondary-reset-at")
_SAFE_INCOMPLETE = {"max_output_tokens", "content_filter"}

_REASONING_CACHE_SIZE = 64

# Relay-owned tool surface (CodexComputerProvider). Injected only on
# Codex-routed turns; its calls are executed inside the relay and never
# reach the Devin client.
CUA_TOOL_NAME = "codex_computer"
CUA_TOOL = {
    "type": "function",
    "name": CUA_TOOL_NAME,
    "description": (
        "Control macOS apps and browsers through Codex Computer Use. Runs "
        "JavaScript in a persistent REPL exposing the `cua` API: "
        "await cua.listApps(); const app = await cua.getApp(bundleIdOrName); "
        "then app.getAXState() (accessibility-tree diff with element ids — "
        "prefer it), app.getScreenshot() (saved to a file; the path is "
        "returned), app.click(idOrXY), app.pressKey('Return'|'a'|'super+c'|"
        "'Tab'), app.typeText(text), app.scroll(idOrXY,'down',pages). "
        "Use console.log(...) to return values — expression results are "
        "not echoed. Batch actions, then getAXState() to observe results."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "code": {"type": "string",
                     "description": "JavaScript using the cua API; "
                                    "console.log() returns values"},
            "title": {"type": "string",
                      "description": "Short description of this step"},
            "timeout_ms": {"type": "integer",
                           "description": "Execution timeout (default 45000)"},
        },
        "required": ["code"],
    },
    "strict": False,
}

_relay_items_lock = threading.Lock()
# cache_key -> verbatim replay items (our calls + their outputs) that must
# be re-injected into the next request's input because the client never saw
# the relay-owned tool round trip.
_relay_items_cache: dict[str, list[dict]] = {}
_RELAY_ITEMS_CACHE_SIZE = 64
MAX_TOOL_LOOPS = 16


class UnsupportedRequest(ValueError):
    """The packet carries content the translator cannot represent safely."""


class IncompleteResponse(RuntimeError):
    """Codex ended the turn incomplete (e.g. max_output_tokens).

    Raised instead of emitting a normal finish so truncated output can never
    be mistaken for a completed answer downstream.
    """


class ClientGone(RequestCancelled):
    """The CLI disconnected mid-stream; abort the upstream read."""


_REJECTION_ORIGIN = {
    'image_count': 'upstream_image_count',
    'image_dimensions_or_format': 'upstream_image_format',
    'context_limit': 'upstream_context_limit',
    'payload_too_large': 'upstream_payload_too_large',
    'unknown_upstream_rejection': 'upstream_unknown',
}


class UpstreamRejected(RuntimeError):
    """The provider rejected the request with a classified HTTP error.

    Carries only a bounded, content-free ``UpstreamRejection`` — the raw
    error body is inspected for classification then discarded.
    """

    def __init__(self, status: int, rejection: UpstreamRejection):
        super().__init__(f"codex HTTP {status}")
        self.status = status
        self.rejection = rejection

    def user_message(self) -> str:
        r = self.rejection
        if r.classification != 'unknown_upstream_rejection':
            return ("provider rejected request (HTTP %d, classification "
                    "%s, certainty %s). %s" % (
                        self.status, r.classification, r.certainty,
                        RECOVERY_INSTRUCTION))
        return ("upstream request failed (HTTP %d); the rejecting layer "
                "is not established" % self.status)


@dataclass
class RoutedModel:
    """A Devin routed-model id split into upstream model + effort."""

    raw: str
    model: str
    effort: str
    notes: list[str] = dc_field(default_factory=list)


def parse_routed_model(raw: str) -> RoutedModel:
    """Split ``gpt-6-astra-high`` / ``-max`` / ``-fast`` into model + effort."""
    model, effort, notes = raw, "high", []
    changed = True
    while changed:
        changed = False
        for suffix in ("-none", "-low", "-medium", "-high", "-xhigh", "-max",
                       "-fast"):
            if not model.endswith(suffix):
                continue
            model = model[: -len(suffix)]
            changed = True
            if suffix == "-fast":
                notes.append("priority tier not translatable; standard tier")
            elif suffix == "-max":
                effort = MAX_EFFORT
                notes.append("effort max mapped to xhigh")
            else:
                effort = suffix[1:]
            break
    return RoutedModel(raw=raw, model=model, effort=effort, notes=notes)


def _image_mime(raw: bytes) -> str:
    """Return the MIME type if *raw* starts with a known image magic."""
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if raw.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if raw.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    return ""


def _is_printable(raw: bytes) -> bool:
    """True if *raw* decodes as printable text (ids, labels, uuids)."""
    try:
        return raw.decode().isprintable()
    except UnicodeDecodeError:
        return False


_reasoning_lock = threading.Lock()
_reasoning_cache: dict[str, list[dict]] = {}


def _cache_key(seed: str, scope: str = "") -> str:
    return "fusion-relay-" + hashlib.sha256(
        (scope + "\0" + seed).encode()).hexdigest()[:32]


def _stash_reasoning(cache_key: str, items: list[dict]) -> None:
    """Keep the turn's reasoning items for echo-back on the next request."""
    keep = [it for it in items
            if it.get("type") == "reasoning" and it.get("encrypted_content")]
    if not keep:
        return
    with _reasoning_lock:
        _reasoning_cache[cache_key] = keep
        while len(_reasoning_cache) > _REASONING_CACHE_SIZE:
            _reasoning_cache.pop(next(iter(_reasoning_cache)))


def _tool_image_part(raw: bytes) -> dict:
    from .artifacts import MAX_ARTIFACT_BYTES
    from .images import checked_image
    from .wire import decode_typed
    error = 'tool image field 10 invalid or unsupported (PNG/JPEG required)'
    try:
        limit = 4 * ((MAX_ARTIFACT_BYTES + 2) // 3)
        if not isinstance(raw, bytes) or len(raw) > limit + 64:
            raise ValueError('envelope size')
        image = decode_typed(raw)
        if set(image) != {1, 2} or any(
                len(values) != 1 or values[0][1] != 2
                for values in image.values()):
            raise ValueError('envelope schema')
        encoded, mime = image[1][0][0], image[2][0][0]
        if mime not in (b'image/png', b'image/jpeg') or not encoded \
                or len(encoded) > limit:
            raise ValueError('encoding')
        data = base64.b64decode(encoded, validate=True)
        actual_mime, _ = checked_image(data, mime.decode('ascii'))
        return {'type': 'input_image',
                'image_url': 'data:%s;base64,%s' % (
                    actual_mime, base64.b64encode(data).decode('ascii'))}
    except (ValueError, TypeError):
        raise UnsupportedRequest(error) from None


def packet_to_responses_body(packet: Message, routed: RoutedModel,
                             rec: dict,
                             continuity_scope: str = "") -> dict:
    """Convert a decoded GetChatMessage packet into a Responses-API body."""
    instructions: list[str] = []
    inputs: list[dict] = []
    rec["message_sources"] = []
    last_assistant_start: Optional[int] = None

    def scan_extra_fields(msg: Message, source: int) -> list[dict]:
        """Classify fields outside the known set: image / trivial / reject."""
        images = []
        for num, values in msg.items():
            if num in KNOWN_MSG_FIELDS:
                continue
            if source == SRC_TOOL_OUT and num == 10:
                if len(values) > 4:
                    raise UnsupportedRequest('too many tool images')
                images.extend(_tool_image_part(val) for val in values)
                continue
            for val in values:
                if isinstance(val, bytes):
                    mime = _image_mime(val)
                    if mime and source != SRC_TOOL_OUT:
                        images.append({
                            "type": "input_image",
                            "image_url": "data:%s;base64,%s" % (
                                mime, base64.b64encode(val).decode()),
                        })
                        continue
                    if (len(val) <= IGNORABLE_FIELD_BYTES or
                            (len(val) <= IGNORABLE_TEXT_BYTES
                             and _is_printable(val))):
                        rec.setdefault("ignored_fields", []).append(
                            f"src{source}.f{num}")
                        continue
                elif isinstance(val, int):
                    rec.setdefault("ignored_fields", []).append(
                        f"src{source}.f{num}")
                    continue
                raise UnsupportedRequest(
                    f"message field {num} (source {source}) carries a "
                    f"{len(val) if isinstance(val, bytes) else 'varint'}-byte "
                    "payload the translator cannot represent")
        return images

    for raw_msg in packet.get(3, []):
        msg = decode(raw_msg)  # type: ignore[arg-type]
        source = msg.get(2, [0])[0]
        body_text = text(msg, 3)
        rec["message_sources"].append(source)
        images = scan_extra_fields(msg, source)
        if source == SRC_INSTRUCTIONS:
            if images:
                raise UnsupportedRequest("instruction images not translatable")
            instructions.append(body_text)
            continue
        if source == SRC_TOOL_OUT:
            if images and not text(msg, 7):
                raise UnsupportedRequest('tool image call_id required')
            output = body_text
            if images:
                output = ([{'type': 'input_text', 'text': body_text}]
                          if body_text else []) + images
            inputs.append({'type': 'function_call_output',
                           'call_id': text(msg, 7), 'output': output})
            continue
        if source not in (SRC_USER, SRC_ASSISTANT):
            raise UnsupportedRequest(f"message source {source} not supported")
        if source == SRC_ASSISTANT:
            last_assistant_start = len(inputs)
            if images:
                raise UnsupportedRequest("assistant images not translatable")
        if source == SRC_USER and images:
            content = ([{"type": "input_text", "text": body_text}]
                       if body_text else []) + images
            inputs.append({"role": "user", "content": content})
        elif body_text:
            role = "user" if source == SRC_USER else "assistant"
            inputs.append({"role": role, "content": body_text})
        for raw_tc in msg.get(6, []):
            tc = decode(raw_tc)  # type: ignore[arg-type]
            inputs.append({"type": "function_call", "call_id": text(tc, 1),
                           "name": text(tc, 2), "arguments": text(tc, 3)})

    seed = text(packet, 16)
    if not seed:
        raise UnsupportedRequest("missing stable session seed (field 16)")
    # Continuity caches are partitioned by a trusted caller-supplied
    # scope (account/model/effort/role binding). Without one they stay
    # disabled — an unscoped cache could leak across sessions.
    cache_key = _cache_key(seed, continuity_scope)
    prior: list[dict] = []
    if continuity_scope:
        with _reasoning_lock:
            prior = _reasoning_cache.get(cache_key, [])
    if prior and last_assistant_start is not None:
        # Echo the previous turn's reasoning items ahead of the assistant
        # turn they generated — order must mirror the original output.
        inputs[last_assistant_start:last_assistant_start] = prior
        rec["reasoning_echoed"] = len(prior)

    # Re-inject relay-owned tool items the client never saw (calls the
    # relay answered itself last turn). They belong after the assistant
    # turn that emitted them — before the tool outputs that follow. The
    # stash is read, not consumed: delivery is not acknowledged here.
    pending: list[dict] = []
    if continuity_scope:
        with _relay_items_lock:
            pending = copy.deepcopy(_relay_items_cache.get(cache_key, []))
    if pending:
        insert_at = len(inputs)
        if last_assistant_start is not None:
            for idx in range(last_assistant_start, len(inputs)):
                if inputs[idx].get("type") == "function_call_output":
                    insert_at = idx
                    break
        inputs[insert_at:insert_at] = pending
        rec["relay_items_reinjected"] = len(pending)

    prefix = text(packet, 2)
    if prefix:
        instructions.insert(0, prefix)
    tools = []
    for raw_tool in packet.get(10, []):
        t = decode(raw_tool)  # type: ignore[arg-type]
        tools.append({"type": "function", "name": text(t, 1),
                      "description": text(t, 2),
                      "parameters": json.loads(text(t, 3) or "{}"),
                      "strict": False})
    return {
        "model": routed.model,
        "instructions": "\n\n".join(instructions),
        "input": inputs,
        "tools": tools,
        "reasoning": {"effort": routed.effort},
        "include": ["reasoning.encrypted_content"],
        "stream": True,
        "store": False,
        "prompt_cache_key": cache_key,
    }


def inject_computer_tool(body: dict) -> None:
    """Offer the codex_computer tool on a Codex-bound request body.

    Blocked: there is no trusted Fusion role binding or authenticated
    consent UI, so production relay-owned computer dispatch fails closed
    instead of pretending authorization. The body is never mutated.
    """
    raise UnsupportedRequest(
        "computer_policy_denied: trusted Fusion role binding and "
        "consent UI unavailable")


def stash_relay_items(cache_key: str, items: list[dict]) -> None:
    """Remember executed relay-tool items for re-injection next request."""
    with _relay_items_lock:
        _relay_items_cache[cache_key] = items
        while len(_relay_items_cache) > _RELAY_ITEMS_CACHE_SIZE:
            _relay_items_cache.pop(next(iter(_relay_items_cache)))


def _record_usage(complete: dict, rec: dict) -> None:
    record_usage(complete, rec)


def _final_message(complete: dict, items: list[dict], rec: dict,
                   drop_names: frozenset = frozenset()) -> bytes:
    """Build the final wire message (usage + finish + any tool calls).

    ``drop_names`` suppresses relay-owned tool calls the client never
    dispatched (mixed-call turns where native calls still go to Devin).
    """
    if not complete.get("output"):
        complete["output"] = items
    result = field(1, complete["id"])
    has_tool = False
    for item in complete.get("output", []):
        if item["type"] == "function_call":
            if item.get("name") in drop_names:
                continue
            # Tool names are logged; arguments never are (privacy) — they go
            # to the wire only.
            rec.setdefault("tool_call_names", []).append(item["name"])
            has_tool = True
            result += field(6, field(1, item["call_id"]) + field(2, item["name"])
                            + field(3, item["arguments"]))
    counts = normalized_usage(complete.get("usage"))
    usage_payload = b"".join(
        field(number, counts[key])
        for number, key in ((2, "input_tokens"), (3, "output_tokens"),
                            (5, "cached_tokens"))
        if counts[key] is not None)
    if usage_payload:
        result += field(7, usage_payload)
    result += field(5, FINISH_TOOL_CALLS if has_tool else FINISH_STOP)
    return result


def _visible_terminal_content(items: list[dict]) -> tuple:
    """Project supported completed message content without losing refusals."""
    if not isinstance(items, list):
        raise IncompleteResponse("invalid terminal output")
    chunks = []
    refused = False
    present = False
    for item in items:
        if not isinstance(item, dict):
            raise IncompleteResponse("invalid terminal output item")
        if item.get("type") != "message":
            continue
        content = item.get("content", [])
        if not isinstance(content, list):
            raise IncompleteResponse("invalid message content")
        for part in content:
            if not isinstance(part, dict):
                raise IncompleteResponse("invalid message content")
            kind = part.get("type")
            if kind not in ("output_text", "refusal"):
                raise IncompleteResponse("unsupported message content")
            value = part.get("refusal" if kind == "refusal" else "text")
            if not isinstance(value, str):
                raise IncompleteResponse("invalid message content")
            present = True
            refused = refused or kind == "refusal"
            chunks.append(value)
    return present, "".join(chunks), refused


# How often a blocked SSE read yields to the cancellation callback. The
# provider may emit nothing for tens of seconds mid-reasoning; without
# this a dead client is only noticed at the next event boundary.
CANCEL_POLL_S = 0.5


class _LineReader:
    """Read SSE lines on a daemon thread so the request thread can poll
    ``check_cancelled`` every ``CANCEL_POLL_S`` while the provider is
    silent. Line semantics (limits, EOF, errors) are unchanged: EOF is
    delivered as ``b""`` and a reader exception is re-raised in the
    consumer. ``abort()`` shuts the socket down so the blocked reader
    exits promptly; provider-side cancellation is still unconfirmed.
    """

    _EOF = object()

    def __init__(self, response):
        self._response = response
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="fusion-sse-reader")
        self._thread.start()

    def _run(self):
        try:
            while True:
                line = self._response.readline(MAX_SSE_LINE + 1)
                if not line:
                    self._queue.put(self._EOF)
                    return
                self._queue.put(line)
        except BaseException as e:  # delivered to the consumer
            self._queue.put(e)

    def next(self, check) -> bytes:
        while True:
            try:
                item = self._queue.get(timeout=CANCEL_POLL_S)
            except queue.Empty:
                check()
                continue
            if item is self._EOF:
                return b""
            if isinstance(item, BaseException):
                raise item
            return item

    def abort(self) -> None:
        """Best-effort: unblock the reader and reclaim the thread."""
        sock = getattr(getattr(getattr(self._response, "fp", None),
                               "raw", None), "_sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        try:
            self._response.close()
        except Exception:
            pass
        self._thread.join(timeout=2.0)


def call_codex(body: dict, rec: dict,
               on_delta: Optional[Callable[[bytes], bool]] = None,
               timeout: int = 300,
               _items_out: Optional[list] = None,
               check_cancelled: Optional[Callable[[], None]] = None,
               credentials: Optional[tuple] = None,
               serialized: Optional[bytes] = None) -> bytes:
    """Call the Codex Responses endpoint; return the complete wire stream.

    If *on_delta* is given it is invoked once per assistant text delta with a
    ready-to-send wire frame; returning ``False`` aborts the upstream read
    (client disconnected — no point burning the rest of the response).
    Without *on_delta* the full text is emitted as one cumulative frame
    before the terminal message, so buffered and streamed modes deliver
    equivalent completed content.

    The returned bytes always contain the terminal message (usage, tool
    calls, finish reason) followed by the end-of-stream trailer.

    ``_items_out`` (internal) receives the completed response's output items
    so the tool-loop wrapper can inspect calls without re-decoding wire bytes.

    ``check_cancelled`` is a request-lifecycle callback invoked before the
    request, per stream event, and before the terminal message.
    ``credentials`` is an optional ``(token, account_id)`` pair so a caller
    can pin one auth read across cache scope and inference. Usage is
    recorded for every terminal event (completed/incomplete/failed); a
    started request that dies without one is tracked once in ``finally``
    as an unknown call — a pre-request failure is not a provider response.
    """
    check = check_cancelled or (lambda: None)
    usage_recorded = False
    request_started = False
    try:
        check()
        if credentials is not None:
            token, account_id = credentials
        else:
            token, account_id = auth.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "ChatGPT-Account-Id": account_id,
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "User-Agent": "fusion-codex-relay/0.2",
        }
        data = serialized if serialized is not None \
            else json.dumps(body).encode()
        rec["final_serialized_bytes"] = len(data)
        req = urllib.request.Request(CODEX_RESPONSES_URL,
                                     data=data,
                                     headers=headers)
        assembly = ResponseAssembly(IncompleteResponse)
        text_parts: list[str] = []
        total_bytes = 0
        try:
            request_started = True
            with open_request(req, timeout=timeout) as response:
                rec["codex_http_status"] = response.status
                quota = {}
                for k in QUOTA_HEADERS:
                    raw = response.headers.get(k)
                    try:
                        v = float(raw)  # type: ignore[arg-type]
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(v) and v >= 0:
                        quota[k] = v
                if quota:
                    rec["codex_quota_snapshot"] = quota
                lines = _LineReader(response)
                try:
                    while True:
                        check()
                        line = lines.next(check)
                        total_bytes += len(line)
                        if len(line) > MAX_SSE_LINE or \
                                total_bytes > MAX_RESPONSE_BYTES:
                            raise RuntimeError("codex stream oversized")
                        if not line:
                            break
                        if not line.startswith(b"data: "):
                            continue
                        if not line.endswith(b"\n"):
                            raise IncompleteResponse("unterminated event")
                        data = line[6:].strip()
                        if data == b"[DONE]":
                            continue
                        try:
                            event = json.loads(data)
                        except (ValueError, UnicodeDecodeError):
                            raise IncompleteResponse("invalid event data")
                        if not isinstance(event, dict):
                            raise IncompleteResponse("invalid event data")
                        etype = event.get("type")
                        if etype == "response.incomplete":
                            partial = event.get("response")
                            if not isinstance(partial, dict):
                                raise IncompleteResponse("invalid event data")
                            details = partial.get("incomplete_details")
                            if details is not None \
                                    and not isinstance(details, dict):
                                raise IncompleteResponse("invalid event data")
                            reason = (details or {}).get("reason")
                            detail = reason if reason in _SAFE_INCOMPLETE \
                                else "unknown"
                            rec["incomplete_reason"] = detail
                            if not usage_recorded:
                                _record_usage(partial, rec)
                                usage_recorded = True
                            raise IncompleteResponse(
                                f"codex response incomplete: {detail}")
                        elif etype in ("error", "response.failed"):
                            partial = event.get("response")
                            if partial is not None \
                                    and not isinstance(partial, dict):
                                raise IncompleteResponse("invalid event data")
                            if not usage_recorded:
                                _record_usage(partial or {}, rec)
                                usage_recorded = True
                            raise RuntimeError("codex stream failed")
                        else:
                            if etype == "response.completed" \
                                    and not usage_recorded:
                                partial = event.get("response")
                                if not isinstance(partial, dict):
                                    raise IncompleteResponse(
                                        "invalid event data")
                                _record_usage(partial, rec)
                                usage_recorded = True
                            delta = assembly.accept(event)
                            if assembly.refused:
                                rec["response_refused"] = True
                            if delta:
                                text_parts.append(delta)
                                rec["delta_chars"] = rec.get("delta_chars", 0) \
                                    + len(delta)
                                if on_delta and on_delta(
                                        frame(field(3, delta))) is False:
                                    response.close()
                                    raise ClientGone()
                finally:
                    lines.abort()
        except urllib.error.HTTPError as e:
            try:
                raw = e.read(MAX_ERROR_BODY_BYTES + 1)
            except (OSError, ValueError, AttributeError):
                raw = b""
            rejection = classify_upstream_error(e.code, raw)
            rec["codex_http_status"] = e.code
            rec["error_category"] = "upstream_error"
            rec["rejection_origin"] = _REJECTION_ORIGIN.get(
                rejection.classification, 'upstream_unknown')
            e.close()
            raise UpstreamRejected(e.code, rejection)
        complete, output_items = assembly.finish()
        check()
        if assembly.refused:
            rec["response_refused"] = True
        _stash_reasoning(body.get("prompt_cache_key", ""), output_items)
        if _items_out is not None:
            _items_out.extend(output_items)
        out = b""
        if on_delta is None and text_parts:
            out += frame(field(3, "".join(text_parts)))
        out += frame(_final_message(complete, output_items, rec)) + end_stream()
        return out
    except RequestCancelled:
        rec["client_gone"] = True
        raise
    finally:
        if request_started and not usage_recorded:
            _record_usage(
                {"status": "cancelled" if rec.get("client_gone") else "failed"},
                rec)


def call_codex_with_tools(body: dict, rec: dict,
                          on_delta: Optional[Callable[[bytes], bool]] = None,
                          executor: Optional[Callable[[dict, dict], str]] = None,
                          max_loops: int = MAX_TOOL_LOOPS,
                          *, journal=None,
                          operation_scope: str = "",
                          check_cancelled: Optional[Callable[[], None]] = None,
                          credentials: Optional[tuple] = None,
                          budget_profile=None) -> bytes:
    """Like call_codex, but executes relay-owned tool calls internally.

    ``executor(call_item, rec) -> result_text`` runs a relay-owned call.
    An executor requires a durable ``journal`` and trusted
    ``operation_scope`` — without them dispatch fails closed before any
    inference. ``check_cancelled`` is invoked before/after each inference
    and around each dispatch.

    Turns loop while every function_call is relay-owned. If a response mixes
    relay-owned and native calls, the relay-owned ones are executed, their
    call+output items are stashed for re-injection on the next request, and
    only native calls are emitted downstream. A stale relay-owned call with
    no executor gets ``computer_policy_denied`` — never executed.

    ``max_loops`` is the iteration budget; invalid budgets are rejected
    before any inference. Each relay-owned call is dispatched at most once
    per ``call_id`` — a repeated id with identical name/arguments reuses
    its result, a conflicting reuse raises ``UnsupportedRequest``.
    Exhausting the budget sets ``relay_tool_loop_bound`` and raises
    ``IncompleteResponse``; an already-executed call is never returned as
    dispatchable.
    """
    if type(max_loops) is not int or not 1 <= max_loops <= MAX_TOOL_LOOPS:
        raise ValueError("invalid tool iteration budget")
    if executor is not None and (journal is None or not operation_scope):
        raise UnsupportedRequest(
            "computer_policy_denied: durable operation context required")
    check = check_cancelled or (lambda: None)
    call_kwargs: dict = {}
    if check_cancelled is not None:
        call_kwargs["check_cancelled"] = check
    if credentials is not None:
        call_kwargs["credentials"] = credentials
    cache_key = body.get("prompt_cache_key", "")
    executed: dict[str, tuple] = {}  # call_id -> (name, arguments, output)
    executed_pairs: list[dict] = []

    def run_call(call: dict) -> str:
        cid = call["call_id"]
        prior = executed.get(cid)
        if prior is not None:
            return prior[2]  # identical call id + args: cached, no redispatch
        if executor is None:
            output = _exec_relay_call(call, None, rec)
        else:
            try:
                output = journal.run(operation_scope, call,
                                     lambda: executor(call, rec), check)
            except (RequestCancelled, IncompleteResponse,
                    UnsupportedRequest):
                raise
            except Exception:
                raise IncompleteResponse(
                    "outcome_unknown: explicit reconciliation required")
        check()
        executed[cid] = (call.get("name"), call.get("arguments"), output)
        return output

    for _ in range(max_loops):
        check()
        serialized = None
        if budget_profile is not None:
            # Re-measure every iteration: the body grows as relay-owned
            # call+output pairs are appended, so a budget that fit on
            # iteration 1 can be exceeded by iteration 2.
            report = rec.get('_payload_report')
            if report is None:
                report = PayloadReport(
                    route='codex', profile=budget_profile.profile)
                rec['_payload_report'] = report
            measure_body(body, budget_profile, report)
            try:
                serialized = serialize_and_check(
                    body, budget_profile, report)
            finally:
                rec['payload'] = report.safe_dict()
        items: list[dict] = []
        tail = call_codex(body, rec, on_delta=on_delta, _items_out=items,
                          serialized=serialized, **call_kwargs)
        check()
        calls = [it for it in items if it.get("type") == "function_call"]
        # Every call id in the response must map to one fingerprint before
        # anything dispatches — a collision (ours or native, differing
        # name/arguments) rejects the whole response with zero actions.
        fingerprints: dict[str, tuple] = {}
        for c in calls:
            cid = c.get("call_id")
            if not isinstance(cid, str) or not cid:
                raise UnsupportedRequest("tool call missing call_id")
            fp = (c.get("name"), c.get("arguments"))
            prior = fingerprints.get(cid)
            if prior is not None and prior != fp:
                raise UnsupportedRequest("conflicting reuse of tool call id")
            fingerprints[cid] = fp
            # A later-issued call id that conflicts with an already-
            # executed fingerprint (ours or native) rejects the whole
            # response before any current-iteration action.
            seen = executed.get(cid)
            if seen is not None and seen[:2] != fp:
                raise UnsupportedRequest("conflicting reuse of tool call id")
        ours = [c for c in calls if c.get("name") == CUA_TOOL_NAME]
        if not ours:
            return tail
        for c in ours:
            if not isinstance(c.get("arguments"), str):
                raise UnsupportedRequest(
                    "relay-owned tool call arguments not a string")
            try:
                parsed_args = json.loads(c["arguments"])
            except ValueError:
                raise UnsupportedRequest(
                    "relay-owned tool call arguments not valid JSON")
            if not isinstance(parsed_args, dict):
                raise UnsupportedRequest(
                    "relay-owned tool call arguments not a JSON object")
        native = [c for c in calls if c.get("name") != CUA_TOOL_NAME]
        rec["relay_tool_calls"] = rec.get("relay_tool_calls", 0) + len(ours)
        # Replay context: raw output items plus an output right after each
        # of our calls. Identical call items within this response emit one
        # pair — duplicate outputs for one call_id are provider-invalid;
        # the same call re-issued in a later iteration re-emits with the
        # cached result. For loop-continuation the whole turn replays; for
        # a mixed finish only OUR pairs are stashed (the client's own
        # history already carries the native call it is about to answer).
        emitted: set = set()
        replay: list[dict] = []
        our_pairs: list[dict] = []
        ours_by_id = {id(c) for c in ours}
        for it in items:
            if it.get("type") == "function_call" and id(it) in ours_by_id:
                signature = (it["call_id"], it.get("name"),
                             it.get("arguments"))
                if signature in emitted:
                    continue
                emitted.add(signature)
                replay.append(it)
                output_item = {"type": "function_call_output",
                               "call_id": it["call_id"],
                               "output": run_call(it)}
                replay.append(output_item)
                our_pairs.extend((it, output_item))
            else:
                replay.append(it)
        our_ids = {c["call_id"] for c in our_pairs}
        executed_pairs[:] = [p for p in executed_pairs
                             if p.get("call_id") not in our_ids]
        executed_pairs.extend(our_pairs)
        if cache_key:
            stash_relay_items(cache_key, executed_pairs)
        if not native:
            # A call id re-issued in a later iteration re-emits its
            # cached result; drop the older pair so the accumulated
            # input carries exactly one call+output per call_id.
            ours_ids = {c["call_id"] for c in ours}
            existing = body.setdefault("input", [])
            body["input"] = [e for e in existing if not (
                isinstance(e, dict)
                and e.get("type") in ("function_call",
                                      "function_call_output")
                and e.get("call_id") in ours_ids)]
            body["input"].extend(replay)
            continue  # all calls were ours — keep the turn going
        # Mixed: emit only native calls; ours are re-injected next request.
        return _drop_tool_calls(tail, {CUA_TOOL_NAME}, rec)
    rec["relay_tool_loop_bound"] = True
    raise IncompleteResponse("tool_budget_exhausted")


def _drop_tool_calls(tail: bytes, names: set, rec: dict) -> bytes:
    """Rebuild the terminal frame minus tool calls named in ``names``.

    Keeps id/usage/finish fields intact and preserves any earlier frames
    (e.g. buffered text) and the original trailer bytes verbatim.
    Malformed framing or an undecodable terminal payload raises
    ``UnsupportedRequest`` rather than passing tool calls downstream.
    """
    try:
        frames = iter_frames(tail)
    except ValueError:
        raise UnsupportedRequest("malformed response framing")
    body_frames = [(f, p) for f, p in frames if not f & 0x02]
    trailer_raw = b"".join(bytes([f]) + len(p).to_bytes(4, "big") + p
                           for f, p in frames if f & 0x02)
    if not body_frames:
        return tail
    last_flags, last_payload = body_frames[-1]
    try:
        msg = decode(last_payload)
    except ValueError:
        raise UnsupportedRequest("undecodable response payload")
    kept: list[tuple[int, object]] = []
    for num, values in msg.items():
        for v in values:
            if num == 6:
                if not isinstance(v, bytes):
                    raise UnsupportedRequest(
                        "undecodable tool call payload")
                try:
                    name = text(decode(v), 2)
                except ValueError:
                    raise UnsupportedRequest(
                        "undecodable tool call payload")
                if name in names:
                    continue
            kept.append((num, v))
    rebuilt = b"".join(field(num, v) for num, v in kept)
    head = b"".join(bytes([f]) + len(p).to_bytes(4, "big") + p
                    for f, p in body_frames[:-1])
    return head + bytes([last_flags]) + len(rebuilt).to_bytes(4, "big") + rebuilt \
        + trailer_raw


def _exec_relay_call(call: dict, executor: Optional[Callable], rec: dict) -> str:
    if executor is None:
        return ("computer_policy_denied: codex_computer is not available "
                "in this execution profile")
    try:
        return executor(call, rec)
    except (RequestCancelled, IncompleteResponse):
        raise
    except Exception:  # executor details stay out of tool results
        return "computer_unavailable: provider execution failed"


def reset() -> None:
    """Clear the reasoning + relay-item caches (tests)."""
    with _reasoning_lock:
        _reasoning_cache.clear()
    with _relay_items_lock:
        _relay_items_cache.clear()
