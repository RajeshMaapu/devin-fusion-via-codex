"""Indexed response-stream assembler for Codex Responses events.

Replaces aggregate text concatenation: every streamed piece is keyed by
response id, output index, item id, and content index; the terminal
``response.completed`` output is authoritative and must reconcile with
what was streamed. Visible emission is a stable prefix over sorted
indexes — never interleaved arrival order.

Events carrying no item_id/output_index/content_index at all run in
legacy unindexed mode for protocol compatibility; mixing modes or
partial indices is an error.
"""
from __future__ import annotations

MAX_INDEX = 2048

_TEXT_KINDS = ("output_text", "refusal")
_ITEM_TYPES = ("message", "function_call", "reasoning")
_ITEM_EVENTS = ("response.output_item.added", "response.output_item.done")
_CONTENT_EVENTS = (
    "response.content_part.added", "response.content_part.done",
    "response.output_text.delta", "response.output_text.done",
    "response.refusal.delta", "response.refusal.done")
_FUNC_EVENTS = ("response.function_call_arguments.delta",
                "response.function_call_arguments.done")
_REASONING_STREAM = (
    "response.reasoning_summary_part.added",
    "response.reasoning_summary_part.done",
    "response.reasoning_summary_text.delta",
    "response.reasoning_summary_text.done",
    "response.reasoning_text.delta", "response.reasoning_text.done")
_IGNORED = ("response.queued",) + _REASONING_STREAM
_INDEX_KEYS = ("item_id", "output_index", "content_index")


def _index(value):
    if type(value) is not int or not 0 <= value < MAX_INDEX:
        return None
    return value


class ResponseAssembly:
    def __init__(self, error_cls):
        self.error_cls = error_cls
        self.response_id = None
        self.terminal = None
        self.items = {}
        self.parts = {}
        self.item_ids = {}
        self.emitted = ''
        self.refused = False
        self.indexed = None
        self.legacy = ''
        self._legacy_items = []
        self._fargs = {}
        self._fargs_done = set()
        self._complete = None
        self._output = None

    # -- validation helpers -------------------------------------------

    def _fail(self, msg):
        raise self.error_cls(msg)

    def _bind_response(self, rid):
        if rid is None:
            return
        if not isinstance(rid, str) or not rid:
            self._fail("invalid response identity")
        if self.response_id is None:
            self.response_id = rid
        elif self.response_id != rid:
            self._fail("conflicting response identity")

    def _event_identities(self, event):
        self._bind_response(event.get("response_id"))
        resp = event.get("response")
        if resp is not None:
            if not isinstance(resp, dict):
                self._fail("invalid response identity")
            self._bind_response(resp.get("id"))

    def _index_of(self, event, *names):
        out = []
        for name in names:
            v = _index(event.get(name))
            if v is None:
                self._fail("missing or invalid %s" % name)
            out.append(v)
        return out

    def _item_entry(self, oi):
        entry = self.items.get(oi)
        if entry is None:
            entry = {"item": None, "id": None, "type": None,
                     "done": False, "call_id": None, "name": None}
            self.items[oi] = entry
        return entry

    def _bind_item(self, item_id, oi):
        if item_id is None:
            return
        if not isinstance(item_id, str) or not item_id:
            self._fail("invalid item identity")
        bound = self.item_ids.get(item_id)
        if bound is not None and bound != oi:
            self._fail("item identity bound to two indexes")
        self.item_ids[item_id] = oi
        entry = self._item_entry(oi)
        if entry["id"] is not None and entry["id"] != item_id:
            self._fail("conflicting item identity")
        entry["id"] = item_id

    def _mode(self, event):
        has = any(k in event for k in _INDEX_KEYS)
        if has:
            if self.indexed is False:
                self._fail("mixed indexed and unindexed events")
            self.indexed = True
            return True
        if self.indexed is True:
            self._fail("mixed indexed and unindexed events")
        self.indexed = False
        return False

    # -- shared output validation --------------------------------------

    def _check_call_identity(self, entry, item):
        for key in ("call_id", "name"):
            v = item.get(key)
            if v is None:
                continue
            if not isinstance(v, str) or not v:
                self._fail("invalid function call item")
            prev = entry[key]
            if prev is not None and prev != v:
                self._fail("conflicting function call identity")
            entry[key] = v

    def _check_call_arguments(self, oi, args):
        if not isinstance(args, str):
            self._fail("invalid function call item")
        if oi in self._fargs_done:
            if args != self._fargs[oi]:
                self._fail("conflicting function arguments")
        elif not args.startswith(self._fargs.get(oi, "")):
            self._fail("conflicting function arguments")

    def _check_part(self, part):
        if not isinstance(part, dict):
            self._fail("invalid message content")
        kind = part.get("type")
        if kind not in _TEXT_KINDS:
            self._fail("unsupported message content")
        value = part.get("refusal" if kind == "refusal" else "text")
        if not isinstance(value, str):
            self._fail("invalid message content")
        if kind == "refusal":
            self.refused = True
        return kind, value

    # -- indexed event handlers ----------------------------------------

    def _apply_item(self, event, done):
        oi, = self._index_of(event, "output_index")
        item = event.get("item")
        if not isinstance(item, dict):
            self._fail("invalid output item")
        itype = item.get("type")
        if itype not in _ITEM_TYPES:
            self._fail("unsupported output item")
        item_id = event.get("item_id")
        if item_id is not None and item.get("id") is not None \
                and item["id"] != item_id:
            self._fail("item identity disagrees with item.id")
        self._bind_item(item_id if item_id is not None
                        else item.get("id"), oi)
        entry = self._item_entry(oi)
        if entry["type"] is not None and entry["type"] != itype:
            self._fail("conflicting item type")
        if entry["id"] is not None and item.get("id") is not None \
                and entry["id"] != item["id"]:
            self._fail("conflicting item identity")
        if itype == "message":
            content = item.get("content", [])
            if not isinstance(content, list) or len(content) > MAX_INDEX:
                self._fail("invalid message content")
            for part in content:
                if not isinstance(part, dict):
                    self._fail("invalid message content")
                kind = part.get("type")
                if kind not in _TEXT_KINDS:
                    self._fail("unsupported message content")
                value = part.get("refusal" if kind == "refusal"
                                 else "text")
                if not isinstance(value, str) and \
                        (done or value is not None):
                    self._fail("invalid message content")
                if not done and value:
                    self._fail("nonempty added content unsupported")
                if done and kind == "refusal":
                    self.refused = True
        elif itype == "function_call":
            self._check_call_identity(entry, item)
            args = item.get("arguments")
            if args is not None:
                self._check_call_arguments(oi, args)
                if not done and args:
                    self._fargs[oi] = args
        if done and entry["done"]:
            if entry["item"] != item:
                self._fail("conflicting completed item")
            return
        entry["item"] = item
        entry["type"] = itype
        if item.get("id") is not None:
            entry["id"] = item["id"]
        if done:
            self._seal_item(oi, entry, item)
        elif entry["done"]:
            self._fail("item modified after done")

    def _seal_item(self, oi, entry, item):
        if entry["done"]:
            if entry["item"] != item:
                self._fail("conflicting completed item")
            return
        itype = item["type"]
        if itype == "message":
            content = item.get("content", [])
            if not isinstance(content, list) or len(content) > MAX_INDEX:
                self._fail("invalid message content")
            for ci, part in enumerate(content):
                kind, value = self._check_part(part)
                state = self.parts.get((oi, ci))
                if state is not None:
                    if state["kind"] != kind:
                        self._fail("conflicting content kind")
                    if state["done"]:
                        if value != state["text"]:
                            self._fail("conflicting terminal content")
                    elif not value.startswith(state["text"]):
                        self._fail("conflicting terminal content")
                    state["text"] = value
                    state["done"] = True
                else:
                    self.parts[(oi, ci)] = {
                        "kind": kind, "text": value, "done": True}
            streamed = [ci for (o, ci) in self.parts if o == oi]
            if streamed and max(streamed) >= len(content):
                self._fail("streamed content omitted by terminal item")
        elif itype == "function_call":
            for key in ("call_id", "name"):
                if not isinstance(item.get(key), str) \
                        or not item[key]:
                    self._fail("invalid function call item")
            self._check_call_identity(entry, item)
            self._check_call_arguments(oi, item.get("arguments"))
            self._fargs[oi] = item["arguments"]
            self._fargs_done.add(oi)
        entry["done"] = True

    def _part_state(self, event, kind=None):
        oi, ci = self._index_of(event, "output_index", "content_index")
        item_id = event.get("item_id")
        if item_id is not None:
            self._bind_item(item_id, oi)
        entry = self._item_entry(oi)
        if entry["done"]:
            self._fail("content event after item done")
        if entry["type"] is None:
            entry["type"] = "message"
            entry["item"] = {"type": "message",
                             "id": item_id or entry["id"]}
        elif entry["type"] != "message":
            self._fail("content event on non-message item")
        if item_id is not None:
            entry["id"] = item_id
        state = self.parts.get((oi, ci))
        if state is None:
            state = {"kind": kind, "text": "", "done": False}
            self.parts[(oi, ci)] = state
        elif kind is not None and state["kind"] is not None \
                and state["kind"] != kind:
            self._fail("conflicting content kind")
        elif kind is not None:
            state["kind"] = kind
        return oi, ci, state

    def _apply_part(self, event, done):
        part = event.get("part")
        if not isinstance(part, dict):
            self._fail("invalid content part")
        kind = part.get("type")
        if kind not in _TEXT_KINDS:
            self._fail("unsupported message content")
        oi, ci, state = self._part_state(event, kind)
        value = part.get("refusal" if kind == "refusal" else "text")
        if done:
            if not isinstance(value, str):
                self._fail("invalid content part")
            if state["done"]:
                if value != state["text"]:
                    self._fail("conflicting content part")
            elif not value.startswith(state["text"]):
                self._fail("conflicting content part")
            state["text"] = value
            state["done"] = True
        else:
            if value is not None:
                if not isinstance(value, str):
                    self._fail("invalid content part")
                if value:
                    self._fail("nonempty added content unsupported")
            if state["done"]:
                self._fail("content modified after done")
        if kind == "refusal":
            self.refused = True

    def _apply_delta(self, event, done):
        kind = "refusal" if ".refusal." in event["type"] else "output_text"
        oi, ci, state = self._part_state(event, kind)
        key = "refusal" if kind == "refusal" else "text"
        if done:
            value = event.get(key)
            if not isinstance(value, str):
                self._fail("invalid terminal content")
            if state["done"]:
                if value != state["text"]:
                    self._fail("conflicting terminal content")
            elif not value.startswith(state["text"]):
                self._fail("conflicting terminal content")
            state["text"] = value
            state["done"] = True
        else:
            delta = event.get("delta", "")
            if not isinstance(delta, str):
                self._fail("invalid delta")
            if state["done"] and delta:
                self._fail("delta after content done")
            state["text"] += delta
        if kind == "refusal":
            self.refused = True

    def _apply_fargs(self, event, done):
        oi, = self._index_of(event, "output_index")
        item_id = event.get("item_id")
        if item_id is not None:
            self._bind_item(item_id, oi)
        entry = self._item_entry(oi)
        if entry["done"]:
            self._fail("argument event after item done")
        if entry["type"] is not None \
                and entry["type"] != "function_call":
            self._fail("argument event on non-call item")
        entry["type"] = "function_call"
        if done:
            args = event.get("arguments")
            if not isinstance(args, str):
                self._fail("invalid function arguments")
            if oi in self._fargs_done:
                if args != self._fargs[oi]:
                    self._fail("conflicting function arguments")
            elif not args.startswith(self._fargs.get(oi, "")):
                self._fail("conflicting function arguments")
            self._fargs[oi] = args
            self._fargs_done.add(oi)
        else:
            delta = event.get("delta", "")
            if not isinstance(delta, str):
                self._fail("invalid delta")
            if oi in self._fargs_done and delta:
                self._fail("delta after arguments done")
            self._fargs[oi] = self._fargs.get(oi, "") + delta

    def _apply_ignored(self, event):
        etype = event["type"]
        if etype == "response.queued":
            return
        oi, = self._index_of(event, "output_index")
        for extra in ("content_index", "summary_index"):
            if extra in event:
                self._index_of(event, extra)
        item_id = event.get("item_id")
        if not isinstance(item_id, str) or not item_id:
            self._fail("missing item identity")
        self._mode(event)
        self._bind_item(item_id, oi)
        entry = self._item_entry(oi)
        if entry["type"] is not None and entry["type"] != "reasoning":
            self._fail("reasoning event on non-reasoning item")
        entry["type"] = "reasoning"

    # -- visible prefix -------------------------------------------------

    def _prefix(self, items_done):
        """Stable visible text over sorted output indexes."""
        chunks = []
        oi = 0
        while True:
            entry = self.items.get(oi)
            if entry is None:
                break
            if entry["type"] == "message":
                ci = 0
                while True:
                    state = self.parts.get((oi, ci))
                    if state is None:
                        if entry["done"]:
                            break
                        return "".join(chunks)
                    chunks.append(state["text"])
                    if not state["done"]:
                        return "".join(chunks)
                    ci += 1
                if not entry["done"] and not items_done:
                    break
            else:
                if not entry["done"] and not items_done:
                    break
            oi += 1
        return "".join(chunks)

    def _emit(self, items_done=False):
        prefix = self._prefix(items_done)
        if not prefix.startswith(self.emitted):
            self._fail("emitted content contradicted")
        suffix = prefix[len(self.emitted):]
        self.emitted = prefix
        return suffix

    # -- public API ------------------------------------------------------

    def accept(self, event: dict) -> str:
        if not isinstance(event, dict):
            self._fail("invalid event")
        etype = event.get("type")
        if not isinstance(etype, str):
            self._fail("invalid event type")
        self._event_identities(event)
        if self.terminal is not None:
            if etype == "response.completed" and event == self.terminal:
                return ''
            self._fail("event after terminal response")

        if etype in ("response.created", "response.in_progress"):
            resp = event.get("response")
            if resp is not None and not isinstance(resp, dict):
                self._fail("invalid response identity")
            return ''
        if etype in _IGNORED:
            self._apply_ignored(event)
            return ''
        if etype == "response.completed":
            return self._terminal(event)

        indexed = self._mode(event)
        if not indexed:
            return self._legacy(event)
        if etype in _ITEM_EVENTS:
            self._apply_item(event, etype.endswith(".done"))
        elif etype in _CONTENT_EVENTS:
            if etype.startswith("response.content_part."):
                self._apply_part(event, etype.endswith(".done"))
            else:
                self._apply_delta(event, etype.endswith(".done"))
        elif etype in _FUNC_EVENTS:
            self._apply_fargs(event, etype.endswith(".done"))
        else:
            self._fail("unsupported event")
        return self._emit()

    # -- terminal ---------------------------------------------------------

    def _terminal(self, event):
        resp = event.get("response")
        if not isinstance(resp, dict):
            self._fail("invalid terminal response")
        rid = resp.get("id")
        if not isinstance(rid, str) or not rid:
            self._fail("invalid response identity")
        self._bind_response(rid)
        status = resp.get("status")
        if status is not None and status != "completed":
            self._fail("invalid terminal status")
        self.terminal = event
        if "output" in resp:
            output = resp["output"]
            if not isinstance(output, list) or len(output) > MAX_INDEX:
                self._fail("invalid terminal output")
        else:
            output = None
        if output:
            self._project(output)
        if self.indexed:
            if not output and self.items:
                # The backend may complete with an empty/absent output
                # after fully streaming every item; reconstruct it,
                # failing closed on any gap or unfinished item.
                if sorted(self.items) != list(range(len(self.items))) \
                        or any(e["done"] is not True
                               or not isinstance(e["item"], dict)
                               for e in self.items.values()):
                    self._fail("terminal output omits streamed content")
                output = [self.items[oi]["item"]
                          for oi in range(len(self.items))]
                self._project(output)
                resp = dict(resp, output=output)
            sealed = self._reconcile(output if output is not None else [])
            self._complete = resp
            self._output = sealed
            return self._emit(items_done=True)
        if output:
            sealed, final_text = self._project(output)
        elif self._legacy_items:
            sealed, final_text = self._project(self._legacy_items)
        else:
            sealed, final_text = [], self.legacy
        if not final_text.startswith(self.emitted):
            self._fail("conflicting terminal content")
        suffix = final_text[len(self.emitted):]
        self.emitted = final_text
        self._complete = resp
        self._output = sealed
        return suffix

    def _reconcile(self, output):
        for i, item in enumerate(output):
            if not isinstance(item, dict):
                self._fail("invalid terminal output item")
            if item.get("type") not in _ITEM_TYPES:
                self._fail("unsupported terminal output item")
            entry = self._item_entry(i)
            if entry["item"] is None:
                if entry["type"] is not None and \
                        entry["type"] != item.get("type"):
                    self._fail("conflicting terminal item type")
                entry["item"] = item
                entry["type"] = item.get("type")
            self._bind_item(item.get("id"), i)
        for oi in sorted(self.items):
            if oi >= len(output):
                self._fail("streamed item omitted by terminal output")
            entry = self.items[oi]
            final = output[oi]
            if entry["id"] is not None:
                if final.get("id") is None:
                    self._fail("terminal item omits identity")
                if final["id"] != entry["id"]:
                    self._fail("conflicting terminal item identity")
            if entry["type"] is not None and \
                    final.get("type") != entry["type"]:
                self._fail("conflicting terminal item type")
            self._seal_item(oi, entry, final)
        if not output and self.emitted:
            self._fail("terminal output omits streamed content")
        return output

    def _project(self, items):
        """Validate item list and return (items, visible text)."""
        text = []
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                self._fail("invalid terminal output item")
            iid = item.get("id")
            if iid is not None:
                if not isinstance(iid, str) or not iid or iid in seen:
                    self._fail("invalid terminal item identity")
                seen.add(iid)
            itype = item.get("type")
            if itype == "message":
                content = item.get("content", [])
                if not isinstance(content, list) \
                        or len(content) > MAX_INDEX:
                    self._fail("invalid message content")
                for part in content:
                    text.append(self._check_part(part)[1])
            elif itype == "function_call":
                for key in ("call_id", "name", "arguments"):
                    v = item.get(key)
                    if not isinstance(v, str) \
                            or (key != "arguments" and not v):
                        self._fail("invalid function call item")
            elif itype != "reasoning":
                self._fail("unsupported terminal output item")
        return list(items), "".join(text)

    def finish(self):
        if self.terminal is None:
            self._fail("missing terminal response")
        return self._complete, self._output

    # -- legacy unindexed mode -------------------------------------------

    def _legacy(self, event):
        etype = event["type"]
        if etype == "response.output_text.delta" or \
                etype == "response.refusal.delta":
            delta = event.get("delta", "")
            if not isinstance(delta, str):
                self._fail("invalid delta")
            if etype == "response.refusal.delta":
                self.refused = True
            self.legacy += delta
            self.emitted += delta
            return delta
        if etype == "response.output_text.done" or \
                etype == "response.refusal.done":
            key = "refusal" if etype == "response.refusal.done" \
                else "text"
            value = event.get(key)
            if not isinstance(value, str):
                self._fail("invalid terminal content")
            if not value.startswith(self.legacy):
                self._fail("conflicting terminal content")
            suffix = value[len(self.emitted):]
            self.legacy = value
            self.emitted = value
            if key == "refusal":
                self.refused = True
            return suffix
        if etype == "response.output_item.done":
            item = event.get("item")
            if not isinstance(item, dict):
                self._fail("invalid output item")
            if len(self._legacy_items) >= MAX_INDEX:
                self._fail("too many output items")
            self._legacy_items.append(item)
            return ''
        if etype == "response.output_item.added":
            item = event.get("item")
            if not isinstance(item, dict):
                self._fail("invalid output item")
            itype = item.get("type")
            if itype not in _ITEM_TYPES:
                self._fail("unsupported output item")
            if itype == "message":
                content = item.get("content", [])
                if not isinstance(content, list) \
                        or len(content) > MAX_INDEX:
                    self._fail("invalid message content")
                for part in content:
                    if not isinstance(part, dict):
                        self._fail("invalid message content")
                    kind = part.get("type")
                    if kind not in _TEXT_KINDS:
                        self._fail("unsupported message content")
                    value = part.get("refusal" if kind == "refusal"
                                     else "text")
                    if value is not None:
                        if not isinstance(value, str):
                            self._fail("invalid message content")
                        if value:
                            self._fail(
                                "nonempty added content unsupported")
            elif itype == "function_call":
                for key in ("call_id", "name"):
                    v = item.get(key)
                    if v is not None \
                            and (not isinstance(v, str) or not v):
                        self._fail("invalid function call item")
                args = item.get("arguments")
                if args is not None and not isinstance(args, str):
                    self._fail("invalid function call item")
            return ''
        if etype == "response.content_part.added":
            part = event.get("part")
            if not isinstance(part, dict):
                self._fail("invalid content part")
            kind = part.get("type")
            if kind not in _TEXT_KINDS:
                self._fail("unsupported message content")
            value = part.get("refusal" if kind == "refusal" else "text")
            if value is not None:
                if not isinstance(value, str):
                    self._fail("invalid content part")
                if value:
                    self._fail("nonempty added content unsupported")
            return ''
        if etype == "response.content_part.done":
            self._fail("unindexed content part done unsupported")
        self._fail("unsupported event")
