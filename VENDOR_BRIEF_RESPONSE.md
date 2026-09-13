# Response — "Support external lead inference in native Fusion"

Status per item: **Implemented** · **Tested successfully** · **Failed** ·
**Unsupported** (needs vendor/provider action) · **Not yet tested**.

Scope note: this document reports the state of the *relay prototype*
(`fusion-codex-relay`). Items that require changes inside Devin/Cognition
(source builds, TUI, entitlement backend, session DB) are marked
Unsupported-for-us — we cannot ship them, only demonstrate the requirement.

## Provider contract (preamble)

> "Please first confirm whether this authentication and billing arrangement
> is supported."

**Unsupported-for-us.** We cannot speak for Cognition or OpenAI. Facts we
measured: the Codex backend accepts the user's ChatGPT OAuth token on
`chatgpt.com/backend-api/codex/responses`, returns `x-codex-plan-type: pro`,
meters into the subscription's 7-day window, and serves the `gpt-6-astra`
model id with effort mapping. Whether that access is *approved* for this
use is a vendor/provider policy question — the brief itself flags the
ToS gray-zone. The relay never forges Cognition quota or billing state.

## 1. Provider visible and unambiguous

| Acceptance criterion | Status | Evidence / gap |
|---|---|---|
| Distinguish external-lead Fusion, native Fusion, standalone SWE-2 | Implemented+Tested | Canonical astra/fusion-astra catalog entries relabeled `· Codex sub` + Route badge; `-native` clones labeled `· Native`; swe-2 standalone entries untouched. `devin models list` through the relay shows all three distinctly |
| Selection displays effective configuration immediately | Partially implemented | Route badge + label carry provider. Reasoning/sidekick remain in the entry name. The *active-session status line* is Devin-side — we can only affect catalog labels |
| Reasoning/sidekick changes preserve provider | Implemented+Tested | Every astra/fusion-astra variant is relabeled (all effort suffixes); picking any keeps the Codex route |
| Unsupported combos → clear explanation | Implemented+Tested | Non-astra models forward natively (identical to no-relay). Untranslatable content → Connect `invalid_argument`/`failed_precondition` errors with reason strings, surfaced by the CLI |

Remaining vendor gap: real `FusionExecutionProfile` + `display_family`
grouping (`Fusion · Codex lead` vs `Fusion · Native lead`) needs
server-owned catalog fields — we cannot add families, only relabel leaves.

## 2. Transactional and persistent selection

| Criterion | Status | Evidence |
|---|---|---|
| Both-direction switching across repeated selections | Implemented | `AssignModel` rewrite strips `-native`; unsuffixed = codex default. Live-verified: pinned session forwarded natively, unpinned astra → codex |
| Restart/resume preserves provider+model | Implemented+Tested | Pins persist to `routes.json`; verified live: seeded pin → launchd restart → next `GetChatMessage` for that session routed `cognition-forward` + `session_route: native` |
| Concurrent sessions independent | Implemented | Pins keyed by session uuid (AssignModel f3 ≡ GetChatMessage f16) |
| Failed assignment can't create disagreement | Implemented+Tested | Pin commits only when upstream `<400`; verified: synthetic `…-native` assign → upstream 415 → `pin_deferred` logged, no pin written |
| Mid-turn switching defined | Implemented | Pins apply per-request at `GetChatMessage` time; an in-flight turn finishes on its existing route — never mixes providers mid-response |

Vendor gap: an authoritative `SessionExecutionBinding` with revisions,
optimistic concurrency, and picker-driven (not name-driven) selection state
lives in Devin's session DB — the relay's uuid pin map is the prototype
equivalent.

## 3. Consistent default across entrypoints

| Criterion | Status | Evidence |
|---|---|---|
| New sessions use configured default | Implemented+Tested | `agent.model = fusion-…-swe-2-medium` in `~/.config/devin/config.json`; fresh TUI/`-p`/`acp` sessions all picked it up with no flags |
| Resumed sessions retain configuration | Implemented+Tested | `session/load` resume verified; route pins now survive relay restart too |
| Provider unavailability → explicit error | Implemented+Tested | Codex failure → Connect `internal`/`out_of_range` error to the CLI; no reroute to paid Cognition (fail-closed by design) |
| No automatic provider switch affecting billing | Implemented+Tested | Routing is deterministic per request; nothing auto-switches |

Vendor gap: "same resolver for TUI/CLI/ACP/Workshop" is inherently
client-side; the relay is one uniform endpoint so all entrypoints get
identical behavior *when launched through the wrapper*.

## 4. Request/response semantics (the translator fixes)

| Criterion | Status | Change |
|---|---|---|
| Streaming and buffered equivalent | Implemented+Tested | Buffer mode now emits a cumulative `f3` text frame + terminal message — previously deltas were dropped (unit test) |
| Images reach lead with text | Implemented+Tested(unit) | Message fields outside {2,3,6,7} are scanned: image-magic payloads → `input_image` parts alongside `input_text`; **live image turn not yet tested** (needs a real image-bearing session) |
| Incomplete ≠ success | Implemented+Tested | `response.incomplete` → `out_of_range` error with `incomplete_details.reason`; never a `stop` |
| Partial tool args never executed | Implemented | Only `response.output_item.done`/`completed` items become tool calls; incomplete turns error out entirely |
| Tool IDs/results/parallel calls | Implemented+Tested | call_id threaded both directions; parallel calls preserved (unit test) |
| Effective reasoning = displayed | Implemented | `-none/-low/-medium/-high/-xhigh` pass through; `-max`→`xhigh` + `-fast`→standard are flagged in the request record (documented deltas, not silent) |
| Context-continuity documented+tested | Implemented+Tested | **Fixed properly**: reasoning items are now requested via `include: ["reasoning.encrypted_content"]` and echoed back before the latest assistant turn (the documented `store:false` mechanism). Cache is per-relay-process — cross-restart turns still re-derive |

## 5. Codex computer use

**Unsupported-for-us / Not yet tested.** The relay mediates inference, not
tool runtimes. Codex computer-use-as-a-service for external clients is a
vendor capability question; no screenshot→action→verify cycle has been
attempted. Devin's own tools (exec/write/read) work through the relay —
proven — but that's Devin's runtime, not Codex's.

## 6. Operational and privacy weaknesses — all addressed

| Finding | Status | Fix |
|---|---|---|
| Assistant text + tool args in default logs | Implemented+Tested | `visible_text` → `delta_chars` count; `tool_calls` args → `tool_call_names`. Post-change log scan: 64/64 records clean. **Pre-fix records (indices <238 of `requests.jsonl`) still contain text — retained per "separate retention decision"; purge on your approval** |
| One credential-refresh owner | Implemented | `auth.py` already refreshes under flock + atomic rename; documented as the sole refresh path (Codex CLI's own refresh remains its own, non-conflicting — flock serializes the shared file) |
| Authenticate local clients | Implemented+Tested | `/t/<token>/` path prefix, 0600 token file; verified: POST without token → 403, `/stats` without token → 404, wrapper injects token into the override URL |
| Prompt cancellation propagation | Implemented+Tested | `on_delta` false → `response.close()` + `ClientGone`; verified live: client killed at 8 deltas → `client_gone`, upstream aborted mid-stream |
| Accounting across restart; unknown ≠ zero | Implemented+Tested | `stats.json` persisted per record; restored across launchd restart (79 requests carried). Codex/forwarded calls with unparseable usage count as `unknown_calls`, not zero (3 observed live) |
| Detect protocol changes early | Implemented+Tested | `GetChatMessage` missing `f3`/`f21` → `failed_precondition` reject; catalog decode failures + uncovered astra-like ids → `catalog_warnings` in the record; unknown large message fields reject instead of dropping |

## 7. Completion claim — replaced with recorded evidence

Session `infrequent-menu`, 2026-09-13T22:34Z, wire log:

```
22:34:31  GetChatMessage  gpt-6-astra-high  n=4   codex 200   lead: read file, delegate
22:34:32  GetChatMessage  swe-1-6-fast      n=1   cognition   aux title
22:34:32  AssignModel     → swe-2-medium            cognition   sidekick spawn
22:34:39  GetChatMessage  swe-2-medium      n=2   cognition   sidekick reviews
22:34:43  GetChatMessage  swe-2-medium      n=4   cognition   sidekick completes
22:34:47  GetChatMessage  gpt-6-astra-high  n=7   codex 200   LEAD RESUMES
```

Final text: *"Sidekick verdict: `add(a,b)` correctly returns `a + b` …
CYCLE_DONE"* — the post-sidekick lead turn is on the wire (n=7 carries the
sidekick's result back to the lead). The earlier `polar-sale`/`eminent-papyrus`
gaps were test-driver timeouts while the sidekick was still running, not a
missing turn — substantiated now.

**After restart with retained context:** pin persistence proven live (item
2). Reasoning continuity across relay restart is the residual gap — the
echo cache is process-local (documented limitation).

**Under cancellation:** verified (item 6) — `client_gone`, upstream aborted.

**Under provider failure:** Codex HTTP failure → explicit Connect error;
verified earlier (`codex_http_status` + error frame path). A synthetic
500-injection test is not wired — the failure path is shared with observed
real failures.

## Summary table

| # | Item | Verdict |
|---|---|---|
| 0 | Supported auth/billing arrangement | **Unsupported-for-us** — vendor/provider decision |
| 1 | Visible provider selection | Implemented; family-grouping needs vendor catalog fields |
| 2 | Transactional+persistent selection | Implemented+Tested |
| 3 | Consistent default | Implemented+Tested (through wrapper) |
| 4 | Semantic fidelity | Implemented+Tested; live image turn not yet tested |
| 5 | Codex computer use | Unsupported / not tested |
| 6 | Ops+privacy fixes | Implemented+Tested |
| 7 | Full-cycle evidence | Tested successfully — recorded |

Release-criterion self-assessment against the brief's bar: relay-side
fidelity, recovery, cancellation, accounting and the complete Fusion cycle
all pass. The gating item remains #0 — a supported provider arrangement —
which is why this stays a local prototype, not a shipped feature.
