"""Translate Devin's protobuf inference request to a Codex Responses call.

Request packet layout (observed on the wire, GetChatMessage):

- field 2  (str)        : session instructions, prepended to the system block
- field 3  (repeated)   : messages; each has
      field 2 (varint)  : source — 1=user, 2=assistant, 4=tool output, 5=instructions
      field 3 (str)     : body text / tool output body
      field 6 (repeated): tool calls {1: call_id, 2: name, 3: arguments}
      field 7 (str)     : call_id this message answers (tool outputs)
- field 10 (repeated)   : tool definitions {1: name, 2: description, 3: params JSON}
- field 16 (str)        : stable session seed (used to derive prompt_cache_key)
- field 21 (str)        : routed model id, e.g. "gpt-6-astra-high"

Response packet layout emitted back to the CLI:

- field 1 (str)         : response id
- field 3 (str)         : assistant text (per-delta in stream mode)
- field 6 (repeated)    : tool calls {1: call_id, 2: name, 3: arguments}
- field 7 (message)     : usage {2: input, 3: output, 5: cached}
- field 5 (varint)      : finish reason — 1=stop, 10=tool_calls
"""

from __future__ import annotations

import hashlib
import json
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

FINISH_STOP, FINISH_TOOL_CALLS = 1, 10


class UnsupportedRequest(ValueError):
    """The packet carries content the translator cannot represent safely."""


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


def packet_to_responses_body(packet: Message, routed: RoutedModel,
                             rec: dict) -> dict:
    """Convert a decoded GetChatMessage packet into a Responses-API body."""
    instructions: list[str] = []
    inputs: list[dict] = []
    rec["message_sources"] = []
    for raw_msg in packet.get(3, []):
        msg = decode(raw_msg)  # type: ignore[arg-type]
        source = msg.get(2, [0])[0]
        body_text = text(msg, 3)
        rec["message_sources"].append(source)
        if source == SRC_INSTRUCTIONS:
            instructions.append(body_text)
            continue
        if source == SRC_TOOL_OUT:
            inputs.append({"type": "function_call_output",
                           "call_id": text(msg, 7), "output": body_text})
            continue
        if source not in (SRC_USER, SRC_ASSISTANT):
            raise UnsupportedRequest(f"message source {source} not supported")
        if body_text:
            role = "user" if source == SRC_USER else "assistant"
            inputs.append({"role": role, "content": body_text})
        for raw_tc in msg.get(6, []):
            tc = decode(raw_tc)  # type: ignore[arg-type]
            inputs.append({"type": "function_call", "call_id": text(tc, 1),
                           "name": text(tc, 2), "arguments": text(tc, 3)})
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
    seed = text(packet, 16)
    return {
        "model": routed.model,
        "instructions": "\n\n".join(instructions),
        "input": inputs,
        "tools": tools,
        "reasoning": {"effort": routed.effort},
        "stream": True,
        "store": False,
        "prompt_cache_key": "fusion-relay-" + hashlib.sha256(seed.encode()).hexdigest()[:32],
    }


def _record_usage(complete: dict, rec: dict) -> None:
    usage = complete.get("usage", {})
    rec["codex_usage"] = {k: v for k, v in usage.items() if k != "attribution"}
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
            rec.setdefault("tool_calls", []).append(
                {"name": item["name"], "arguments": item["arguments"]})
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
               on_delta: Optional[Callable[[bytes], None]] = None,
               timeout: int = 300) -> bytes:
    """Call the Codex Responses endpoint; return the complete wire stream.

    If *on_delta* is given it is invoked once per assistant text delta with a
    ready-to-send wire frame, giving the CLI incremental output. The returned
    bytes always contain the terminal message (usage, tool calls, finish
    reason) followed by the end-of-stream trailer.
    """
    token, account_id = auth.get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "ChatGPT-Account-Id": account_id,
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": "fusion-codex-relay/0.1",
    }
    req = urllib.request.Request(CODEX_RESPONSES_URL,
                                 data=json.dumps(body).encode(), headers=headers)
    complete: Optional[dict] = None
    items: list[dict] = []
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
                elif etype == "response.output_text.delta" and on_delta:
                    delta = event.get("delta", "")
                    if delta:
                        rec.setdefault("visible_text", []).append(delta)
                        on_delta(frame(field(3, delta)))
                elif etype == "response.completed":
                    complete = event["response"]
                elif etype in ("error", "response.failed", "response.incomplete"):
                    if etype == "response.incomplete":
                        complete = event.get("response")
                        rec["incomplete"] = True
                        continue
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
    return frame(_final_message(complete, items, rec)) + end_stream()
