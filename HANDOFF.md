# Implementation Handoff — fusion-codex-relay

## What this is

A local Connect-RPC translation relay that sits between the Devin CLI and
Cognition's api-server. Devin talks to it via the `WINDSURF_API_SERVER_URL`
endpoint override. The relay routes per-request:

| Traffic | Route |
| --- | --- |
| `GetChatMessage` with model `gpt-6-astra-*` | Translated → ChatGPT Codex backend (`chatgpt.com/backend-api/codex/responses`), billed to the user's ChatGPT subscription |
| `GetChatMessage` with model `swe-*` / `devstral` | Forwarded byte-for-byte to Cognition (native route — sidekick and aux calls keep their normal billing/free path) |
| Every other RPC (`AssignModel`, `GetUserStatus`, `GetCliModelConfigs`, analytics, plugins, …) | Forwarded byte-for-byte to Cognition — session control plane is untouched |
| Any other model id | Rejected with a Connect error (fail-closed; `FUSION_RELAY_AUX=forward|codex` to change) |

There is **no fallback**: a Codex failure returns an explicit error to the CLI;
it never reroutes to paid Cognition Astra.

## How to invoke (no new model name)

The routing decision is **per-process**, made by the endpoint the CLI talks to.
The model selector is unchanged — keep using `/fusion` or
`fusion-gpt-6-astra-*-sidekick-swe-2-*`.

```bash
# Canonical entry point — auto-starts the relay, sets the override, execs devin:
~/projects/fusion-codex-relay/bin/devin-fusion            # interactive TUI
~/projects/fusion-codex-relay/bin/devin-fusion -p "task"  # non-interactive
~/projects/fusion-codex-relay/bin/devin-fusion acp        # ACP (IDE/Workshop)

# Manual equivalent:
~/projects/fusion-codex-relay/bin/fusion-relay start
export WINDSURF_API_SERVER_URL=http://127.0.0.1:8931
devin
```

**The trap the first user test hit:** running `/fusion` inside a plain `devin`
session stays on the native route — Cognition returns `Quota exhausted` (their
daily-quota enforcement lives on the `GetChatMessage` path we bypass).
Verified: on the same exhausted-quota account, a relayed Fusion turn completed
normally (Codex HTTP 200).

### Selecting the route in `/model`

Under `devin-fusion` the relay rewrites `GetCliModelConfigs` so the picker
shows the route honestly on the entries you already know:

- Canonical `gpt-6-astra*` / `fusion-gpt-6-astra*` entries are **relabeled in
  place** with a `· Codex sub` suffix — picking the normal entry IS the
  subscription route.
- One `…-native` clone per entry is appended (`… · Native` label) — a
  per-session escape hatch back to Cognition-billed Astra.

Mechanics: picking a `-native` id sends it in `AssignModel` field 2 → the
relay strips the suffix (Cognition only knows canonical ids), pins
`session_uuid → route` (AssignModel field 3 == GetChatMessage field 16), and
honors `native` for that session. Caveat: `--model`/`DEVIN_MODEL` can't
resolve `-native` ids — fuzzy matching runs before the catalog loads; use
the `/model` picker or canonical names on the CLI.

### Global default model

`~/.config/devin/config.json` now sets
`agent.model = fusion-gpt-6-astra-high-sidekick-swe-2-medium` (backup:
`config.json.fusion-relay-backup`). Consequence: **plain `devin` also
defaults to fusion-astra but bypasses the relay** — it will hit the native
Cognition quota. Run sessions through `devin-fusion` while quota is
exhausted.

### Always-on relay

`~/Library/LaunchAgents/ai.maapu.fusion-relay.plist` — RunAtLoad + KeepAlive
on crash, logs to `~/.local/share/fusion-codex-relay/launchd.log`.
Manage: `launchctl kickstart -k gui/$(id -u)/ai.maapu.fusion-relay` (restart),
`launchctl bootout gui/$(id -u)/ai.maapu.fusion-relay` (unload).

## File map

| Path | Role |
| --- | --- |
| `fusion_relay/wire.py` | Protobuf + Connect frame codec (varint/len/32/64-bit; frame = 1-byte flags + 4-byte length + payload; `0x02` trailer, `0x01` gzip) |
| `fusion_relay/translate.py` | `GetChatMessage` packet → Responses-API body; SSE stream → wire messages. Model-id suffix → `reasoning.effort` (`-max`→`xhigh`, `-fast` stripped with note) |
| `fusion_relay/auth.py` | Reads `~/.codex/auth.json` (ChatGPT mode only); refreshes via `auth.openai.com/oauth/token` under flock + atomic rename; never logs tokens |
| `fusion_relay/relay.py` | `ThreadingHTTPServer` on 127.0.0.1; routing table, verbatim forwarder, delta/buffer streaming, JSONL accounting, `/healthz` `/stats` |
| `bin/fusion-relay` | start/stop/status/stats/fg process manager |
| `bin/devin-fusion` | devin launcher with the override |
| `fusion_relay/catalog.py` | `GetCliModelConfigs` rewrite: relabel astra/fusion-astra entries `· Codex sub`, append `-native` clones; `AssignModel` suffix strip + `session_uuid → route` pinning |
| `tests/test_relay.py` | 24 unit tests: codec, routing table, translation, catalog injection/relabel, assign rewrite, session pinning, finish reasons |

Runtime state (never committed): `~/.local/share/fusion-codex-relay/` —
`requests.jsonl` (per-request sanitized records), `relay.log`, `relay.pid`.

## Wire protocol facts (reverse-engineered, verified live)

Request `GetChatMessage` packet (protobuf):
- f2 str — session instructions prefix
- f3 repeated msg — {f2 source: 1=user,2=assistant,4=tool-output,5=instructions; f3 text; f6 tool calls {f1 call_id, f2 name, f3 args}; f7 call_id}
- f10 repeated — tool defs {f1 name, f2 desc, f3 params-JSON}
- f16 str — session seed (hashed → `prompt_cache_key`)
- f21 str — **routed model id** (routing key)

Response message: f1 id, f3 delta text, f6 tool calls, f7 usage
{2:in,3:out,5:cached}, f5 finish (1=stop, 10=tool_calls). Stream ends with a
`0x02` trailer frame `{}`.

Codex call: POST `…/codex/responses`, headers `Authorization: Bearer <access>`,
`ChatGPT-Account-Id`, SSE accept; body `{model, instructions, input, tools,
reasoning.effort, stream, store:false, prompt_cache_key}`.

## Verified live (devin 3000.10.21)

- `fusion-gpt-6-astra-high-sidekick-swe-2-medium` assigned natively (`AssignModel` → `['fusion','gpt-6-astra-high']`, 200)
- Astra lead turns → Codex 200; real `usage` returned; CLI `usage_update` mirrors it exactly
- Tool calls incl. the native `sidekick` tool → sidekick ran as `swe-2-medium` on Cognition
- `devin acp` and `devin -p` both work; `session/load` resume + recall verified
- Prompt cache hits observed (`cached_tokens` → CLI `cachedReadTokens`)
- Quota-exhausted Cognition account: relayed astra turn still completes
- LaunchAgent (launchd `ai.maapu.fusion-relay`) keeps relay up across logins
- Config default `fusion-…-swe-2-medium` honored with no `--model` flag;
  lead `gpt-6-astra-high` → Codex ×6, sidekick `swe-2-medium` → Cognition ×2,
  aux `swe-1-6-fast` → Cognition — real task (fizzbuzz + tests + sidekick
  review) completed green end-to-end

## Usage accounting — the honest picture

- **Codex ledger (authoritative):** `x-codex-*` response headers — plan type,
  `primary-used-percent`, window minutes, reset times. Logged per request.
- **Cognition ledger:** `GetUserStatus` numeric fields unchanged across
  bridged turns; analytics events carry no token counts; astra inference
  never reaches Cognition so its meter can't count it. Nothing to "force
  update" — Devin's quota UI reads Cognition's replies, forwarded verbatim.
- **Relay ledger:** `requests.jsonl` + `/stats` — per-route tokens/latency.

## Known limitations / ops notes

- Opaque reasoning items are not threaded between turns; long sessions
  re-derive context from message history.
- Mid-stream cancellation is best-effort; upstream unwinds on timeout.
- The TUI `/model` picker may filter catalog entries by entitlement; the
  `· Codex sub` relabel keeps canonical ids so picker filtering is unaffected
  — only display names change.
- Compatibility depends on undocumented Devin and Codex protocol details;
  a Devin update can change wire fields without notice.
- ChatGPT-subscription inference via a local login is the same model the
  codex-as-api bridge family uses; ToS gray-zone noted in the source report.

## Benchmark-parity assessment

Model id and effort round-trip unchanged (`gpt-6-astra`/`high`); everything
client-side (session store, sidekick orchestration, tool harness) is native.
The only delta is the translation layer: prompt layout, tool-schema
conversion, reasoning-item continuity. Prove parity with an A/B battery of
real tasks before treating routed Fusion as a drop-in — not claimed yet.

## Workshop integration path (pending)

`ACPHarness` already injects per-process env. An opt-in Workshop Devin profile
needs: launch `devin acp --model fusion-…` with
`WINDSURF_API_SERVER_URL=http://127.0.0.1:8931` + ensure the relay is running
(health check, spawn if down). One profile toggle — no global config change.

## Tests

```bash
cd ~/projects/fusion-codex-relay && python3 -m unittest tests.test_relay   # 24 tests
```
