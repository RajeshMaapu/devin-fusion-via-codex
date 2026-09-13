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
import hashlib
import json
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass, field as dc_field
from typing import Callable, Iterator, Optional

from . import auth
from .wire import Message, decode, end_stream, field, frame, text

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

_REASONING_CACHE_SIZE = 64


class UnsupportedRequest(ValueError):
    """The packet carries content the translator cannot represent safely."""


class IncompleteResponse(RuntimeError):
    """Codex ended the turn incomplete (e.g. max_output_tokens).

    Raised instead of emitting a normal finish so truncated output can never
    be mistaken for a completed answer downstream.
    """


class ClientGone(RuntimeError):
    """The CLI disconnected mid-stream; abort the upstream read."""


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


def _cache_key(seed: str) -> str:
    return "fusion-relay-" + hashlib.sha256(seed.encode()).hexdigest()[:32]


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


def packet_to_responses_body(packet: Message, routed: RoutedModel,
                             rec: dict) -> dict:
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
            for val in values:
                if isinstance(val, bytes):
                    mime = _image_mime(val)
                    if mime:
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
            if images:
                raise UnsupportedRequest("tool-output images not translatable")
            inputs.append({"type": "function_call_output",
                           "call_id": text(msg, 7), "output": body_text})
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
    cache_key = _cache_key(seed)
    with _reasoning_lock:
        prior = _reasoning_cache.get(cache_key, [])
    if prior and last_assistant_start is not None:
        # Echo the previous turn's reasoning items ahead of the assistant
        # turn they generated — order must mirror the original output.
        inputs[last_assistant_start:last_assistant_start] = prior
        rec["reasoning_echoed"] = len(prior)

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


def _record_usage(complete: dict, rec: dict) -> None:
    usage = complete.get("usage")
    if usage:
        rec["codex_usage"] = {k: v for k, v in usage.items() if k != "attribution"}
    else:
        rec["codex_usage"] = "unknown"
    rec["codex_model"] = complete.get("model")
    rec["codex_status"] = complete.get("status")


def _final_message(complete: dict, items: list[dict], rec: dict) -> bytes:
    """Build the final wire message (usage + finish + any tool calls)."""
    if not complete.get("output"):
        complete["output"] = items
    result = field(1, complete["id"])
    has_tool = False
    for item in complete.get("output", []):
        if item["type"] == "function_call":
            # Tool names are logged; arguments never are (privacy) — they go
            # to the wire only.
            rec.setdefault("tool_call_names", []).append(item["name"])
            has_tool = True
            result += field(6, field(1, item["call_id"]) + field(2, item["name"])
                            + field(3, item["arguments"]))
    usage = complete.get("usage", {})
    result += field(7, field(2, usage.get("input_tokens", 0))
                    + field(3, usage.get("output_tokens", 0))
                    + field(5, usage.get("input_tokens_details", {}).get("cached_tokens", 0)))
    result += field(5, FINISH_TOOL_CALLS if has_tool else FINISH_STOP)
    return result


def call_codex(body: dict, rec: dict,
               on_delta: Optional[Callable[[bytes], bool]] = None,
               timeout: int = 300) -> bytes:
    """Call the Codex Responses endpoint; return the complete wire stream.

    If *on_delta* is given it is invoked once per assistant text delta with a
    ready-to-send wire frame; returning ``False`` aborts the upstream read
    (client disconnected — no point burning the rest of the response).
    Without *on_delta* the full text is emitted as one cumulative frame
    before the terminal message, so buffered and streamed modes deliver
    equivalent completed content.

    The returned bytes always contain the terminal message (usage, tool
    calls, finish reason) followed by the end-of-stream trailer.
    """
    token, account_id = auth.get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "ChatGPT-Account-Id": account_id,
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": "fusion-codex-relay/0.2",
    }
    req = urllib.request.Request(CODEX_RESPONSES_URL,
                                 data=json.dumps(body).encode(), headers=headers)
    complete: Optional[dict] = None
    items: list[dict] = []
    text_parts: list[str] = []
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            rec["codex_http_status"] = response.status
            rec["codex_rate_headers"] = {
                k: v for k, v in response.headers.items()
                if "limit" in k.lower() or "rate" in k.lower() or "codex" in k.lower()}
            for line in response:
                if not line.startswith(b"data: "):
                    continue
                data = line[6:].strip()
                if data == b"[DONE]":
                    continue
                event = json.loads(data)
                etype = event.get("type")
                if etype == "response.output_item.done":
                    items.append(event["item"])
                elif etype == "response.output_text.delta":
                    delta = event.get("delta", "")
                    if delta:
                        text_parts.append(delta)
                        rec["delta_chars"] = rec.get("delta_chars", 0) + len(delta)
                        if on_delta and on_delta(frame(field(3, delta))) is False:
                            response.close()
                            raise ClientGone()
                elif etype == "response.completed":
                    complete = event["response"]
                elif etype == "response.incomplete":
                    partial = event.get("response") or {}
                    detail = (partial.get("incomplete_details") or {}).get(
                        "reason", "unknown")
                    rec["codex_status"] = "incomplete"
                    rec["incomplete_reason"] = detail
                    _record_usage(partial, rec)
                    raise IncompleteResponse(
                        f"codex response incomplete: {detail}")
                elif etype in ("error", "response.failed"):
                    rec["codex_error_type"] = etype
                    raise RuntimeError("codex stream failed")
    except urllib.error.HTTPError as e:
        rec["codex_http_status"] = e.code
        detail = e.read().decode(errors="replace")
        rec["codex_error"] = detail[:600]
        raise RuntimeError(f"codex HTTP {e.code}")
    if complete is None:
        raise RuntimeError("codex stream ended without response.completed")
    _record_usage(complete, rec)
    _stash_reasoning(body.get("prompt_cache_key", ""),
                     complete.get("output", []) or items)
    out = b""
    if on_delta is None and text_parts:
        out += frame(field(3, "".join(text_parts)))
    out += frame(_final_message(complete, items, rec)) + end_stream()
    return out


def reset() -> None:
    """Clear the reasoning cache (tests)."""
    with _reasoning_lock:
        _reasoning_cache.clear()
