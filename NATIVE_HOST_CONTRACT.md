# Native host contract — verified hooks, trust boundary, vendor gaps

Date: 2026-09-14. Installed client: Devin CLI `3000.10.21 (611c1cba)`
(`/Users/mappu/.local/bin/devin`). Evidence source for every "verified"
row below is the CLI's own bundled documentation at
`~/.local/share/devin/cli/_versions/3000.10.21/share/devin/docs/`
(`extensibility/hooks/overview.mdx`, `extensibility/hooks/lifecycle-hooks.mdx`,
`reference/commands.mdx`, `changelog/stable.mdx`). No protocol traffic was
captured or persisted for this document; no live session was run.

This document separates three things that are easy to conflate:

1. What the native client verifiably exposes (documented hooks/commands).
2. What the relay implements locally (`fusion_relay/host_binding.py`,
   `continuation_host.py`, `continuation.py`, `relay.py`).
3. What only Cognition can supply (Section 4).

Nothing in (2) makes a claim in (1) true. A local capability store, HMAC
proof, or SQLite journal establishes *who presented a request to the
relay*, never *which internal Fusion role produced it*.

## 1. Capability matrix

| # | Capability | Native status | Evidence | Relay consequence |
|---|---|---|---|---|
| 1 | Native session identity + lifecycle notifications | **Verified (partial)** | Hooks `SessionStart`, `SessionEnd`; every hook stdin payload carries a stable `session_id` and per-turn `prompt_id` (`hooks/overview.mdx` "Command Hooks"; changelog: "Command hooks now receive the agent's session id"). **Live (2026-09-15):** the hook `session_id` is the human-readable session *name* (e.g. `enchanted-beryllium`, `qualification/live/row7_hook_ids.jsonl`), while `GetChatMessage` field 16 is a different per-process seed that **changes on `--resume`** (`qualification/live/session_c_host_journal.jsonl`: the resumed session arrived under a new `session_ref`). | Hook `session_id` and wire field 16 are **different identifiers**. Without the marker (row 3) hash correlation is structurally unmatched and the relay fails closed (`EpochRequired`); with the verified marker the session name travels inside the packet and compaction notes correlate (`matched_marker`, verified live). A resumed conversation is a new scope: the experimental host records an explicit `legacy_history` transition and, when a prior scope's committed anchors are exactly extended by the resumed history, **carries its turns over** so stored reasoning is kept (verified live); otherwise prior reasoning is not recoverable. |
| 2 | Lead vs sidekick identity per request | **Absent** | No hook field, header, or documented protocol element distinguishes the lead lane from the SWE-2 sidekick lane. `subagents.mdx` describes subagents but exposes no per-request lane identity to external processes. Fusion itself is not described in the bundled docs (grep for `fusion`/`sidekick` in docs: no hits). | `HostBinding.lane` can only be asserted by a hosting process. Provenance `native_hook_verified` has **no issuer**; the default coordinator accepts only that provenance, so durable admission fails closed for real traffic. |
| 3 | Client extension point able to authenticate identities to the relay | **Partial → usable for session/turn identity (verified live)** | Hooks are shell commands with JSON stdin; `UserPromptSubmit` may return `additionalContext`, and that text **arrives inside the `GetChatMessage` packet as a user-source message** (probe + sessions, `qualification/live/session_d_*`). No hook can attach headers to the inference RPC. | `bin/fusion-prompt-marker` injects `fusion-relay-marker v1 <session-name> <prompt_id> <HMAC(identity key)>`; `fusion_relay/marker.py` verifies it. A verified marker proves the packet came from a CLI session running the relay's hook, for that turn — it correlates hook events (compaction) with the wire session and yields a per-turn id. It says nothing about the Fusion lane; requests on the sidekick's native route arrived **without** the marker (observation, not contract). Capability headers remain the binding for durable admission. |
| 4 | Stable native request/operation identity across retry and reconnect | **Turn-level available via the marker** | `prompt_id` is per user prompt, shared by all hooks in a turn; through the marker it is now visible on the inference RPC. It does not distinguish a retried inference from a second inference within the same turn. | Relay operation id: host-issued `X-Fusion-Operation` when a capability is presented; otherwise (resolver mode) the body digest. Body digest is stored as a **conflict check**, not as the definition of intent: same id + different body is refused; identical bodies under distinct ids are distinct operations. |
| 5 | Callback / persistent state proving response acceptance | **Absent** (evidence path available) | No hook fires on model-response acceptance; `PostToolUse` fires on tool completion only; `Stop` fires when the agent decides to stop. | Local `POST /t/<token>/host/ack` exists as a **local contract only**. `/capabilities` reports `native_ack_contract: unavailable`, `host_ack_endpoint: local_contract_only`. Per spec §3 the relay now records **`history_evidenced`** when a later request's history reproduces a turn's exact visible projection — a qualified, distinct level; `acknowledged` still requires an explicit consumer ack that nothing native sends. |
| 6 | Native tool-call deduplication and cancellation semantics | **Partially observable, not contractual** | `PreToolUse` can block (exit 2 / `decision: block`) and `PostToolUse` reports `tool_response.success`. No documented dedup key for a repeated tool call; no cancellation event. | The relay cannot guarantee exactly-once native tool effects. On disconnect it marks `cancel_unconfirmed`; diagnostics `cancellation` is `requested`/`uncertain`, never `confirmed`. Replay of a committed result is only offered when the operation id + body match exactly. |
| 7 | Model switch, history compaction, fork, restart | **Compaction: verified post-hoc. Others: commands only** | `PostCompaction` hook (fires **after** compaction, with `summary`); `/compact`, `/fork [step]`, `/steps`, `/revert`, `--continue`, `--resume <id>` commands; changelog: automatic background compaction exists and is not surfaced in the transcript. No pre-compaction hook, no model-switch event, no fork event. **Live:** `/compact` (a) makes a native Cognition inference to produce the summary, (b) immediately sends the lead a request with the compacted history *before* `PostCompaction` fires, and (c) the next user turn's history did not extend that request's anchors either — two divergences per compaction (`session_c_host_journal.jsonl`). | The relay cannot request or veto compaction. It receives a compaction *notice* via `bin/fusion-post-compaction` → `POST /host/compaction` (session id only; summary discarded); because the hook id never equals field 16 (row 1), the notice only serves as evidence metadata (`recent_compaction_notice`). Under the default relay a compacted history fails closed; the experimental host records an explicit `history_divergence` transition (cause not established) and retries once. Model switch/fork require a new capability with a new profile/epoch. |

### 1a. Two further native facts established live (2026-09-15)

- **The "Too many images in the conversation" text is a client-side
  string.** The CLI binary contains
  `Request payload is too large. Too many images in the conversation.` and
  renders it for a Connect `out_of_range` error from the endpoint,
  regardless of the error's cause. In the reproduced incident the relay
  returned `out_of_range` because the Codex backend's `response.completed`
  now carries `output: []` after streaming every item; the response
  contained **zero images**. Evidence: `qualification/live/probe_event_shape.log`,
  `qualification/live/session_a_requests.jsonl` (`incomplete_detail:
  "streamed item omitted by terminal output"`, `image_occurrences: 0`).
  The client then retried the identical request (5×, `retryable: true`).
- **The client prunes conversation images itself.** Over 45 sequential
  `read` calls on PNGs the translated image count rose to 19, then dropped
  to 10 and cycled (`session_a_requests.jsonl`), i.e. the CLI keeps at most
  ~19 images and trims to the newest ~10. The relay's local budget (40) is
  therefore above the native cap for tool-result images.

## 2. Trust boundary as implemented

```
native CLI ──(GetChatMessage, no binding headers)──▶ relay
                                                   binding: unavailable
                                                   durable admission: refused (failed_precondition)
                                                   legacy in-memory mode: allowed, labelled unqualified

host process ──(CapabilityStore.issue in-process)──▶ secret stays in host memory
host client ──(headers: capability id, op id, HMAC(secret, {v, cap, op, body_digest}))──▶ relay
                                                   binding: verified  ⇐ only if provenance ∈ accept_provenance
                                                   default accept_provenance = {native_hook_verified} (no issuer exists)
```

- The capability proof binds the request body digest, so a replayed proof
  cannot authorize a different body. Expiry and a per-client revocation
  generation are enforced on every request.
- The relay path token (`/t/<token>/`) remains the transport authenticator
  for *reaching* the relay; it is not a lane identity.
- The continuation ledger key comes from the macOS Keychain only
  (`keychain.py`); there is no env/disk fallback. Key states are
  distinguished as missing / access denied / unavailable / record invalid.
- `provenance='launcher_process_ownership'` may be explicitly accepted by a
  host (`accept_provenance`) for experiments; it establishes process
  ownership, **not** internal lane identity, and diagnostics still report
  `qualification: local_tests`.

## 3. What the relay does *not* claim

- That a `verified` binding proves the request came from Fusion's lead
  rather than any process the host chose to hand a capability to.
- That an offered/delivered result was durably accepted by Fusion — only an
  explicit ack through the local endpoint moves `acceptance` to
  `acknowledged`, and no native component sends one today.
- That closing the client socket cancelled upstream inference or billing.
- That the hook `session_id` and wire field 16 are the same identifier.
- That the local payload budget (`payload_budget.CODEX_PROFILE`, policy
  v1: 40 image occurrences / 24 MiB image bytes / 32 MiB serialized) is a
  vendor limit. It is a conservative local bound with
  `upstream_limit_status: unverified`.

## 4. Minimal contract request for Cognition

To qualify durable continuation beyond local tests, the relay needs a
native, authenticated extension providing:

1. **Connection-bound session/lane/profile/epoch binding** — an
   authenticated declaration, per client connection, of
   `{session_id, lane ∈ {lead, sidekick}, model_profile, continuation_epoch}`
   whose origin is the Fusion harness itself (not a wrapper).
2. **Stable operation id** on each inference request, unchanged across
   retry and reconnect, distinct for an intentionally repeated identical
   request.
3. **Durable response-acceptance callback** carrying
   `{operation_id, revision, response_digest}` only after Fusion has
   retained the result.
4. **Partial-stream and tool-call acceptance semantics** — whether
   partial tool-call frames can trigger native execution, and the dedup
   key Fusion applies to repeated tool calls.
5. **Cancellation and model-switch/compaction events** with defined
   reconciliation behaviour (what state the client keeps, what it discards).

These are proposed requirements, not existing API names. Until they exist,
`FUSION_RELAY_CONTINUATION=durable` remains host-attached and
fail-closed, and the default relay profile stays the explicitly labelled
`legacy_memory_only_unqualified` path.
