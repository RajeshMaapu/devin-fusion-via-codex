# fusion-codex-relay

> **Remediation state (2026-09):** this tree contains an uncommitted
> hardening pass on top of the prototype described below. Computer use is
> **disabled** — there is no trusted dispatcher, consent UI, or role
> binding, and native (non-relay) tool dispatch is **not** enforced by
> this relay. Credentials are read-only (no automatic refresh). The
> "verified" table below describes the **historical** prototype state;
> current verified coverage is in [validation.md](validation.md). Nothing
> here has been qualified against live services.

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

`devin-fusion` starts the relay if needed, sets the endpoint override
(including the per-install auth token — see below), and execs `devin`
unchanged — select any `fusion-gpt-6-astra-*-sidekick-swe-2-*` model as usual
(`--model`, `/model`, or `DEVIN_MODEL`).

### Local client authentication

The relay requires every request under a `/t/<token>/` path prefix. The token
lives in `~/.local/share/fusion-codex-relay/relay-token` (mode 0600,
generated on first start) and the wrapper embeds it in
`WINDSURF_API_SERVER_URL`. Localhost binding alone is not treated as
authentication — any local process without file access to the token gets a
403. `/healthz` stays open (liveness only); `/stats` requires the token.

### `/model` picker entries

Under the relay, every `gpt-6-astra*` / `fusion-gpt-6-astra*` catalog entry
is **relabeled in place** with a `· Codex sub` suffix + Route badge — the
models you already pick ARE the subscription route. A `…-native` clone per
entry (labeled `· Native`) provides the per-session escape hatch back to
Cognition-billed Astra.

Mechanics: picking a `-native` id sends it in `AssignModel` field 2 → the
relay strips the suffix (Cognition only knows canonical ids), pins the
session uuid → `GetChatMessage` field 16 carries the same uuid and honors the
pin. Caveat: `--model`/`DEVIN_MODEL` can't resolve `-native` ids (fuzzy match
runs without catalog) — the picker is the way to reach it.

### Default model

`~/.config/devin/config.json` `agent.model` is set to
`fusion-gpt-6-astra-high-sidekick-swe-2-medium` (historically verified in
the prototype: a fresh session picked it up and routed the lead to Codex —
not re-qualified after this hardening). In a plain `devin` session the
same default hits the native Cognition path — and its quota — so run
everything through `devin-fusion`. Backup: `config.json.fusion-relay-backup`.

```bash
~/projects/fusion-codex-relay/bin/fusion-relay stats    # route counters
~/projects/fusion-codex-relay/bin/fusion-relay stop
```

## What was verified historically (prototype, devin 3000.10.21)

These results predate the remediation hardening — see
[validation.md](validation.md) for the current measured gate.

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
| `/model` route entries | Canonical entries relabeled `· Codex sub`; `-native` clones added; pins commit only on upstream 2xx |
| Pin persistence | `routes.json` survives relay restart — post-restart `-native` pin honored live |
| Quota wall bypass | Relayed astra turn completes on a Cognition-quota-exhausted account |
| Always-on | launchd agent `ai.maapu.fusion-relay` (RunAtLoad + KeepAlive) |
| Cancellation | Client killed mid-stream → `client_gone`, upstream Codex read aborted |
| Full cycle | lead→tool→sidekick→lead resume→final answer recorded end-to-end (session `infrequent-menu`) |
| Incomplete output | `response.incomplete` → explicit `out_of_range` error, never a silent stop |
| Buffered mode | Cumulative text frame + terminal — equivalent content to delta mode |

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
  per-request route, HTTP status, token usage, latency — allowlist
  sanitized, no credentials, no message bodies. Note the boundary:
  `/stats` and the log carry **aggregates** (which may be partial —
  `unknown_calls`/`missing_fields`), while the wire response carries
  **per-response** usage only and omits unknown counts rather than
  reporting zero.

## Environment knobs

| Var | Default | Effect |
| --- | --- | --- |
| `FUSION_RELAY_PORT` | 8931 | Listen port |
| `FUSION_RELAY_STREAM` | `delta` | `delta` streams text; `buffer` returns one frame |
| `FUSION_RELAY_AUX` | `forward` | Policy for non-astra models: `forward` (native-identical) / `reject` / `codex` |
| `FUSION_RELAY_INSPECT` | — | No-op retained for compatibility — request inspection was removed with request-head capture |
| `WINDSURF_API_UPSTREAM` | `https://server.codeium.com` | Cognition upstream |

## Files

- `fusion_relay/wire.py` — protobuf/Connect codec
- `fusion_relay/translate.py` — packet ⇄ Responses-API translation, image
  detection/reject, reasoning-continuity echo
- `fusion_relay/auth.py` — read-only `~/.codex/auth.json` reader; refresh is
  owned by Codex (`codex login`), never by this process
- `fusion_relay/catalog.py` — picker relabel/injection, versioned durable
  route state with pending selections
- `fusion_relay/storage.py` — private-dir/atomic-write/bounded-read/flock
  helpers; `store_owner` single-process data-dir lock
- `fusion_relay/operations.py` — SQLite operation journal for explicit
  actuator callers (local contract only — not a desktop lease)
- `fusion_relay/lifecycle.py` — request cancellation context
- `fusion_relay/usage.py` — usage accounting (per-response + aggregate)
- `fusion_relay/cua.py` — computer-use provider, currently **blocked**;
  `GET /t/<token>/capabilities` reports the explicit unsupported surface
- `fusion_relay/relay.py` — HTTP server, token auth, routing, accounting
- `bin/fusion-relay`, `bin/devin-fusion` — process manager + launch wrapper
- `tests/` — `python3 -m unittest discover -s tests -v` (see
  validation.md for the measured run)

## Known limitations

- Reasoning items are echoed between turns via `encrypted_content`
  (`store:false` multi-turn mechanism); pre-relay-restart turns have no
  cached items, so cross-restart context still re-derives from message
  history.
- `max` effort maps to `xhigh`; `-fast` priority tier is not translatable
  (both flagged in the request record).
- Message sources outside user/assistant/tool-output/instructions are
  rejected (fail-closed); unknown message fields are classified
  image/ignore/reject — a new consequential field shape will reject loudly.
- Compatibility depends on undocumented Devin↔server and Codex-backend
  protocol details; drift guards (`f3`/`f21` presence, catalog warnings)
  detect schema changes rather than guess.
- ToS note: ChatGPT-subscription inference is intended for Codex products;
  this relays a local login for local use, like the codex-as-api bridge family.
- Computer use is disabled (fail-closed): no trusted dispatcher, consent UI,
  or runtime contract. `/capabilities` reports this explicitly.
- Transport is bounded (64 MiB bodies, capped frames/SSE, 32 concurrent
  handlers) but buffered — not streaming backpressure — and header reads
  have an idle timeout only, not a total deadline.
- Auth is read-only: an expired Codex login returns an explicit error
  telling the operator to renew it in Codex; the relay never refreshes.
- Route state is single-writer (data-dir flock in `serve`); the v2
  `routes.json` format is not readable by older builds — see
  validation.md before rolling anything back.

## Benchmark-parity note

"Same model name on both ends" (`gpt-6-astra` + effort) makes parity plausible,
and token/usage/finish semantics round-trip correctly — but parity is only
provable by A/B runs on a real task battery. The relay preserves everything
client-side (session store, sidekick orchestration, tool harness), so the
delta is confined to the translation layer: prompt layout, tool-schema
conversion, reasoning-item continuity. Run your real workload on both paths
before treating the routed path as a drop-in replacement.

## License

[MIT](LICENSE) — free to use, modify, and distribute, including commercially,
with the copyright and license notice retained. Provider subscriptions and
service terms still apply.
