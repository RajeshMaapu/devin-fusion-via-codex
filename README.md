# fusion-codex-relay

Route Devin **Fusion's Astra lead** inference through your local
**ChatGPT-authenticated Codex** subscription, while the SWE-2 sidekick and all
control-plane traffic keep their native Cognition routes.

```
devin / devin acp ── WINDSURF_API_SERVER_URL ──> fusion-relay (127.0.0.1:8931)
                                                    │
          GetChatMessage, model = gpt-6-astra-* ────┼──> chatgpt.com/backend-api/codex/responses
          GetChatMessage, model = swe-* ────────────┼──> server.codeium.com (verbatim)
          every other RPC (assign, status, usage) ──┴──> server.codeium.com (verbatim)
```

Only `GetChatMessage` carries model-routed inference; routing keys off the
routed-model id in request field 21. A failed Codex call returns an explicit
error to the CLI — **there is no fallback to paid Cognition Astra**.

## Quick start

```bash
codex login                          # ChatGPT account, if not already
~/projects/fusion-codex-relay/bin/devin-fusion          # interactive TUI
~/projects/fusion-codex-relay/bin/devin-fusion -p "hi"  # print mode
~/projects/fusion-codex-relay/bin/devin-fusion acp      # ACP (IDE/Workshop)
```

`devin-fusion` starts the relay if needed, sets the endpoint override, and
execs `devin` unchanged — select any `fusion-gpt-6-astra-*-sidekick-swe-2-*`
model as usual (`--model`, `/model`, or `DEVIN_MODEL`).

```bash
~/projects/fusion-codex-relay/bin/fusion-relay stats    # route counters
~/projects/fusion-codex-relay/bin/fusion-relay stop
```

## What is verified (this build, devin 3000.10.21)

| Behavior | Result |
| --- | --- |
| Fusion selector `fusion-gpt-6-astra-high-sidekick-swe-2-medium` | `AssignModel` honored natively |
| Lead turns (`gpt-6-astra-high`) | Translated → Codex Responses API, HTTP 200, real `usage` returned |
| Tool calls through Codex route | `exec`, `write`, and the native `sidekick` tool all work |
| Sidekick dispatch | `swe-2-medium` forwarded to Cognition — native path preserved |
| Aux model `swe-1-6-fast` (title-gen) | Cognition (native family) |
| Streaming | Incremental deltas to the CLI (`FUSION_RELAY_STREAM=buffer` for whole-message mode) |
| Session resume | `session/load` + new-process recall verified |
| `devin -p` print path | Works (same override) |
| Prompt cache | `cached_tokens` observed flowing Codex → CLI `cachedReadTokens` |
| Usage display | CLI `usage_update` mirrors the Codex token counts exactly |

## Usage accounting — what actually happens

- **Codex side (authoritative):** every response carries `x-codex-*` headers —
  plan type, `x-codex-primary-used-percent`, window size, reset times. The
  relay logs them per request (`requests.jsonl`). Observed: ChatGPT Pro,
  premium limit, single-digit % used of a 7-day window.
- **Cognition side:** `GetUserStatus` numeric fields are identical before and
  after bridged turns; the analytics batch RPC carries product events, not
  per-token counts; and `GetChatMessage` for Astra never reaches Cognition, so
  their inference meter cannot count it. **There is nothing to "force
  update"** — Devin's displayed quota comes from Cognition's replies, which
  the relay forwards verbatim (forging them would be wrong).
- **Relay side:** `~/.local/share/fusion-codex-relay/requests.jsonl` records
  per-request route, model, HTTP status, token usage, latency — sanitized,
  no credentials, no message bodies.

## Environment knobs

| Var | Default | Effect |
| --- | --- | --- |
| `FUSION_RELAY_PORT` | 8931 | Listen port |
| `FUSION_RELAY_STREAM` | `delta` | `delta` streams text; `buffer` returns one frame |
| `FUSION_RELAY_AUX` | `reject` | Policy for models matching no route: `reject`/`forward`/`codex` |
| `FUSION_RELAY_INSPECT` | — | RPC substrings to numerically inspect (forensics) |
| `WINDSURF_API_UPSTREAM` | `https://server.codeium.com` | Cognition upstream |

## Files

- `fusion_relay/wire.py` — protobuf/Connect codec
- `fusion_relay/translate.py` — packet ⇄ Responses-API translation
- `fusion_relay/auth.py` — `~/.codex/auth.json` reader + locked token refresh
- `fusion_relay/relay.py` — HTTP server, routing, accounting
- `bin/fusion-relay`, `bin/devin-fusion` — process manager + launch wrapper
- `tests/test_relay.py` — `python3 -m unittest tests.test_relay`

## Known limitations

- Opaque reasoning items are not threaded between turns (Responses
  `include`/`encrypted_content`); long sessions re-derive context from message
  history. No quality regression observed on probe turns, but a real A/B
  benchmark suite is still owed.
- Cancellation mid-Codex-stream is best-effort (client disconnect stops
  writes; upstream read unwinds on timeout).
- `max` effort maps to `xhigh`; `-fast` priority tier is not translatable.
- Message sources outside user/assistant/tool-output/instructions are
  rejected (fail-closed).
- Compatibility depends on undocumented Devin↔server and Codex-backend
  protocol details; a Devin update can change wire fields without notice.
- ToS note: ChatGPT-subscription inference is intended for Codex products;
  this relays a local login for local use, like the codex-as-api bridge family.

## Benchmark-parity note

"Same model name on both ends" (`gpt-6-astra` + effort) makes parity plausible,
and token/usage/finish semantics round-trip correctly — but parity is only
provable by A/B runs on a real task battery. The relay preserves everything
client-side (session store, sidekick orchestration, tool harness), so the
delta is confined to the translation layer: prompt layout, tool-schema
conversion, reasoning-item continuity. Run your real workload on both paths
before treating the routed path as a drop-in replacement.
