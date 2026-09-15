# Qualification results — durable continuation handoff (2026-09-14/15)

Tree under test: HEAD `f310fc71f67e5aa3aa2568b98eb99425634f1a7a` plus the
uncommitted working tree. Exact identification of the tree (tracked-diff
sha256 and per-file hashes of every untracked file, before and after this
work) is in `qualification/BASELINE_SNAPSHOT.txt`. Full verbose gate output
with exit code: `qualification/FINAL_GATE.log`.

Interpreter: `/usr/bin/python3` (Python 3.9.6, macOS Darwin 24.6.0).
Gate command:

```sh
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -X faulthandler -W error::ResourceWarning \
  -m unittest discover -s tests -v
```

Result on the final tree: **`Ran 799 tests — OK (skipped=1)`, exit 0**;
`git diff --check` clean. Baseline before this work: 696 tests OK.
The single skip is `tests/test_keychain_live.py` (opt-in; run separately,
Section 5). No commit or push was made. The **production relay (launchd
`ai.maapu.fusion-relay`, port 8931, PID 75991) was never restarted or
stopped** — it still runs the pre-change code until an approved restart.

Live qualification (user-approved, 2026-09-15 UTC) used isolated relays on
ports 8940 (legacy mode) and 8950/8951 (experimental durable host) with
separate data directories (`~/.local/share/fusion-codex-relay-live-{a,c}`),
real Codex/Cognition inference through the user's existing logins, model
`fusion-gpt-6-astra-high-sidekick-swe-2-medium`, CLI 3000.10.21. Sanitized
evidence (allowlisted request records, host journals, TUI/hook captures,
admin logs) is under `qualification/live/`; nothing there contains prompts,
images, tokens or proofs.

Status vocabulary: **PASS** (automated, offline, in the gate),
**PASS-LIVE** (observed in a real session with recorded evidence),
**BLOCKED** (cannot be qualified without a missing native/vendor contract),
**NOT-RUN** (implemented and runnable but not executed).

## Step 0 — image accumulation / oversized requests

| ID | Case | Status | Evidence |
|---|---|---|---|
| S0-1 | Aggregate overflow across many individually valid messages detected | PASS | `test_payload_budget` (41 one-image user messages → `image_count`, historical=40, current=1) |
| S0-2 | Below / at / above limit; duplicates count as occurrences | PASS | 39/40 pass, 41 rejected; identical PNG ×41 → `unique_image_count=1`, still rejected |
| S0-3 | Final JSON/base64 overflow when inbound binary fits | PASS | coarse check passes, `serialize_and_check` raises `translated_bytes` |
| S0-4 | Mixed user/tool images + text counted; no content logged | PASS | `safe_dict()` and `requests.jsonl` asserted free of `data:`/base64 |
| S0-5 | Mocked 400/413, malformed/oversized error body classified without false certainty | PASS | `classify_upstream_error`: keyword match → `inferred`; unrelated 400 → `unknown_upstream_rejection/unknown`; 413 empty → `payload_too_large/unknown`; body > 8192 B never inspected |
| S0-6 | Preflight failure: no provider call, no retry, no stranded reservation | PASS | `test_continuation_handler`: `open_request` not called; pre-merge rejection leaves no `operations` row; post-merge rejection → `preflight_rejected`, identical retry re-reserves |
| S0-7 | Continuation reinsertion / tool-loop growth included in final budget | PASS | coordinator `preflight` runs on the merged body; `call_codex_with_tools` re-measures each iteration (`call_codex` called once when iteration 2 exceeds) |
| S0-8 | Compaction preserves comparisons/ordering, records epoch, resumes after restart | PASS (local) / BLOCKED (native) | Epoch transition + `epoch_transitions` row + resume tested offline; the relay cannot request or verify native compaction — `PostCompaction` fires only after the fact |
| S0-9 | Native history unchanged on rejected request | PASS | `continuation_turns` count unchanged; translator never drops images |
| S0-10 | Real image-heavy Fusion session; incident attribution | **PASS-LIVE (attributed and fixed)** | Live session A (45 sequential PNG `read`s, `qualification/live/session_a_*`): the incident reproduced on the *first tool-call turn* with **0 images** — Codex returned HTTP 200 `completed` with `output: []` after streaming the function_call; `ResponseAssembly` raised `streamed item omitted by terminal output`; the relay answered Connect `out_of_range`; the CLI rendered its canned string "Request payload is too large. Too many images in the conversation." (string present in the CLI binary) and retried the identical request 5×. Rejecting layer: **relay translation (ResponseAssembly), not images, not Cognition, not the Codex endpoint**. Fixed (streamed-item reconstruction, fail-closed on gaps); re-run: 47/47 Codex turns completed, `DONE 45`. Also observed: the CLI itself caps conversation images (19 → prunes to 10), so the 40-occurrence local budget never fired live. |

Local budget policy v1 (`payload_budget.CODEX_PROFILE`): 40 image
occurrences, 24 MiB image bytes, 32 MiB serialized;
`provenance=local_conservative_limit`, `upstream_limit_status=unverified`.
Exact outgoing bytes are serialized once and validated before send.

## Step 1 — native integration facts

Documented in `NATIVE_HOST_CONTRACT.md` with per-row evidence from the
installed CLI 3000.10.21 docs. Summary: hooks + `session_id`/`prompt_id`
verified; lane identity, operation id, acceptance callback, model-switch
event **absent**; `PostCompaction` verified (post-hoc only). Decision gate
outcome: no usable hook for lane/acceptance → local contract implemented,
vendor request written (contract §4).

## Steps 2–4 — binding, ACK/replay, epoch/recovery

| ID | Case | Status | Evidence |
|---|---|---|---|
| S2-1 | Forged lane (sidekick capability on lead inference) | PASS | `permission_denied`, `binding_status=invalid`, no reserve |
| S2-2 | Unknown / expired / revoked / generation-bumped capability | PASS | `test_host_binding`, `test_continuation_handler.CapabilityHandlerTest` |
| S2-3 | Cross-session / account / profile reuse | PASS | rejected before reserve; `operations` empty |
| S2-4 | Reconnect: same capability + op id + body → replay, no provider call | PASS | identical bytes returned |
| S2-5 | Same op id, changed body → conflict | PASS | `conflicting continuation operation` |
| S2-6 | Identical bodies, distinct op ids | PASS (documented) | second reserve fails `EpochRequired` (history did not advance) rather than replaying — intentional repeat needs the client history to include the first answer |
| S2-7 | No headers → `binding: unavailable`, fail closed | PASS | native CLI path |
| S2-8 | Provenance gate: only `native_hook_verified` accepted by default; **no issuer exists** | PASS / BLOCKED | verified locally; real traffic cannot obtain a verified binding |
| S3-1 | ACK: first ack, idempotent repeat, changed digest, stale revision, unknown op, sidekick capability | PASS | 200/200(idempotent)/409/409/409/403 |
| S3-2 | Failure injection: crash before inference; after provider response before commit; after commit before offer; after delivery before ack; after ack | PASS | `provider_outcome_unknown`, `result_committed`, `pending`, `consumer_acknowledged` persisted; provider call counts asserted; separate-process reopen covered |
| S3-3 | Unknown outcome never produces a second billable inference | PASS | lane blocked with `continuation lane has an unresolved operation` |
| S3-4 | Streaming acceptance semantics | PASS (by construction) / BLOCKED (native) | durable mode is buffered — nothing reaches the client before commit; whether partial tool frames trigger native execution is a vendor question |
| S4-1 | Single owner: second opener same process and subprocess refused | PASS | flock on `<store>.owner` |
| S4-2 | Epoch transitions: reason allowlist, prior epoch preserved, blocked by unresolved op, chain resolution, cycle refused | PASS | `epoch_chain`/`resolve_epoch`; compaction chain continues across three turns under one capability |
| S4-3 | Schema v1→v2 migration, incomplete marker, newer-version refusal | PASS | `to_epoch` column present post-migration |
| S4-4 | Retention: `MAX_SCOPES`, `retire_scope` refuses pending/uncertain | PASS | |
| S4-5 | Pre-provider admission failure has safe retry/abandon transition | PASS | `preflight_rejected` (see S0-6) |

## Step 5 — real Keychain adapter

| ID | Case | Status | Evidence |
|---|---|---|---|
| S5-1 | Error-state mapping: missing / access denied / unavailable / record invalid; delete helper | PASS (mocked) | `test_keychain` (19) |
| S5-2 | Cross-process create/load/decrypt, missing item, duplicate concurrent creation, delete/cleanup under a test-only service | **PASS-LIVE** | `FUSION_RELAY_KEYCHAIN_LIVE=1` run in the interactive shell: OK, no prompt (`qualification/live/keychain_interactive.{log,json}`: macOS 15.6.1 arm64, `/Applications/Xcode.app/…/usr/bin/python3` = `/usr/bin/python3`). First run exposed a test bug (unresolved `/var` tmp path refused by `PrivateDirectory`) — fixed, re-run OK. `security dump-keychain` afterwards: 0 `ai.fusion-codex-relay.test.*` items. |
| S5-3 | launchd/background context access | **PASS-LIVE** | Same test submitted as a one-shot launchd job under the user's GUI launchd (`ppid=1`, uid 501, same interpreter as the production plist): OK, no prompt (`qualification/live/keychain_launchd.{log,json}`). Caveat: each run creates and reads its *own* item; access to an item created by a different executable identity was not exercised. |

The experimental host additionally created and used a real key under
service `ai.fusion-codex-relay.experimental` for the whole Step 6 run
(`key_store: ready` on every durable record) and the key was deleted with
`MacOSKeychain.delete()` at teardown (verified `continuation key missing`).

## Step 6 — live Fusion qualification matrix

Harness: `bin/fusion-experimental-host` (built for this step) = durable-mode
relay on 8950 + header-injecting proxy on 8951 issuing
`launcher_process_ownership` capabilities (accepted only because the host
was started with `accept_provenance={launcher}` — **this provenance is a
process-ownership label, not verified lane identity; nothing here upgrades
`qualification` beyond `local_tests`/live-observed**). Rows needing the
plain relay used the legacy-mode relay on 8940. Evidence:
`qualification/live/session_c_requests.jsonl`, `session_c_host_journal.jsonl`,
`session_c_final_ledger.txt`, `row*.log`.

| Row | Test | Result | Measured |
|---|---|---|---|
| 1 | Native lead → tool → SWE-2 → lead | **PASS-LIVE** | Codex (lead, `binding_status: verified`, `durable_host_bound`, revisions 1→2→3) → `read` → 3× `cognition-forward` (sidekick, native route) → lead. Answer `RESULT 9 9` correct. |
| 2 | Relay restart after commit | **PASS-LIVE** | Host SIGTERM'd and restarted; ledger reopened from Keychain with 14 offered turns / 7 scopes intact; resumed conversation continued with exactly 1 provider call for the new turn, none for committed turns. First attempt exposed two harness defects (see Defects 5, 6), both fixed and re-verified. |
| 3 | Disconnect before ACK | **PASS-LIVE** | Every committed turn shows `acceptance: pending` (no native ack exists); after a client kill the committed-but-undelivered turn stays `offered`/`pending`, never `acknowledged`; no replay was issued. |
| 4 | Lead cancellation | **PASS-LIVE (uncertainty preserved)** | Plain relay: client SIGKILL at 8 s → relay `cancelled`, `client_gone: true` after 58 s (detected at the next provider event boundary), `codex_usage: unknown`. Durable host: kill at 9 s → proxy RST → relay `cancelled` at 18 s, `cancellation: requested`, operation `cancel_unconfirmed`, lane blocked (`unresolved operation`), resolved via `fusion-continuation-admin resolve-unknown` with an evidence ref (`row4_admin.log`). Provider-side cancellation is **not** confirmed by any signal; billing of the partial inference is possible. |
| 5 | Sidekick cancellation | **PASS-LIVE** | Kill during the SWE-2 stream: `cognition-forward` record `cancelled`, `client_gone: true`, `termination_confirmed: true` (native stream closed), `cognition_usage: unknown`, 4.3 s. |
| 6 | Concurrent sessions | **PASS-LIVE** | Two parallel sessions: interleaved revisions `[1,1,2,2,3,3]`, two distinct scopes each at head 3, correct answers `WORDS 3` / `WORDS 4`, 0 errors, no scope/ack crossover. |
| 7 | Model switch / compaction | **PASS-LIVE (explicit transitions)** | TUI `/compact` with the `PostCompaction` hook installed: hook fired (`row7_hook_ids.jsonl`), `/host/compaction` received (summary never transmitted); hook id ≠ field 16 so no auto-correlation; the default relay failed closed (`EpochRequired`, shown in the TUI); the experimental host recorded **two** explicit `history_divergence` transitions (`recent_compaction_notice` False then True, `compaction_note_correlation: unmatched`) and the session continued (`RECALL 3` correct). Model switch: `model_switch` transition path unit-tested; not exercised live. |
| 8 | Keychain unavailable | **PASS-LIVE** | Host with `--no-create-key` on a fresh dir: `key_store unavailable: continuation key missing`, exit 3, **no continuation store created, no memory fallback**. Second host on a live store: `continuation store owned by another process`, exit 3. Plain relay in durable mode without a coordinator: `/capabilities` `durable_host_binding_required`. (`row8_keystore.log`) |
| 9 | Repeated same request | **PASS-LIVE** | Synthetic client posted identical bytes twice through the proxy: host reused the operation id; second response byte-identical, `X-Fusion-Continuation-Revision: 1`, **0 provider calls** (`row9_repeat_request.log`). Intentional distinct operation with identical body: fails `EpochRequired` (documented S2-6). |
| 10 | TUI resume | **PASS-LIVE** | `devin -r <session>` after host restart: field 16 changed → new scope → `legacy_history` transition recorded → `RECALL 9 9` matched the saved conversation; ledger head for the new epoch = 1. Prior reasoning items for the resumed turns are not recoverable and were not claimed. |

Not exercised live: `model_switch` transition (unit-tested only); acks
(no native acker exists — the host deliberately never acks).

## Step 7 — benchmark / performance

**NOT-RUN.** No matched evaluation was performed; no claim of parity or
regression margin is made.

## Step 8 — defaults, diagnostics, rollout

- Default continuation remains `legacy_memory_only_unqualified`; durable
  mode is host-attached and fail-closed. Not enabled globally, not marked
  production-qualified.
- Diagnostics expose independently: `binding`, `continuation`
  (+`continuation_detail`), `acceptance`, `key_store`, `cancellation`
  (never `confirmed`), `qualification=local_tests`,
  `compaction_correlation=unverified` — PASS (`test_diagnostics`, 17).
- `/capabilities`: `native_ack_contract: unavailable`,
  `host_ack_endpoint: local_contract_only`, `binding_issuer: none_native`,
  `payload_budget` profile with provenance — PASS.
- No canary or default change performed.

## Defects found and fixed during qualification

1. Epoch-chain defect (Phase 2): after a compaction-driven
   `transition_epoch`, the capability still carried the old epoch, so the
   next request would fail closed and new-epoch acks would be rejected.
   Fixed with `epoch_transitions.to_epoch`, `resolve_epoch`/`epoch_chain`,
   and `X-Fusion-Continuation-Epoch`/`-Revision` response headers; covered
   by `test_compaction_chain_continues_and_ack_new_epoch`.
2. Intermittent segfault (exit 139, ~50% of runs) in
   `test_committed_turn_buffered_and_replayed`: test threads executed
   statements on a live ledger's single sqlite3 connection concurrently
   with the handler thread (pre-existing latent race in the test fixture,
   exposed by the added post-commit work). Fixed in tests only by holding
   the ledger lock; 10/10 clean loops on three affected modules and two
   full-suite runs, plus this final gate.
3. `test_http_error_no_body_read` asserted the provider error body is never
   read, which contradicts the bounded-classification requirement; rewritten
   as `test_http_error_body_read_bounded_and_discarded` (reads exactly
   8193 bytes, nothing lands in the record).
4. **Live root cause of the incident** — `ResponseAssembly` rejected a
   completed Codex response whose terminal `output` is `[]` after all
   items were streamed (new backend behaviour, `probe_event_shape.log`).
   Every tool-calling Codex turn failed as `out_of_range`, which the CLI
   renders as "Too many images". Fixed by reconstructing the output from
   fully-streamed, contiguous, done items (any gap still fails closed);
   `incomplete_detail` (fixed literal) is now logged so this class of
   failure is attributable from `requests.jsonl`.
5. Harness: the proxy masked client disconnects (relay ran a 77 s inference
   for a dead client). Fixed with a watched forward + RST on client loss.
6. Harness: `stop()` did not join the relay thread, so the accounting run
   was never closed clean → next start `degraded` → `durable accounting
   unavailable`. Fixed (join + warning); the dirty runs were reconciled
   through the existing `fusion-relay reconcile` path with an evidence ref
   (`accounting_reconcile.log`).
7. Relay: an accounting admission failure *after* a durable reservation
   left the row `executing` (lane-blocking, and the request was not even
   logged). Now settled as `admission_rejected` (retryable) and logged;
   `resolve_unknown` additionally accepts `executing` rows for operators.
8. Live test: `test_keychain_live` used an unresolved `/var/...` temp path
   that `PrivateDirectory` rightly refuses (symlink) — fixed in the test.
9. Live: a resumed session arrives under a new field-16 seed and a
   compacted session no longer extends anchors; both correctly failed
   closed, and the experimental host now records explicit
   `legacy_history` / `history_divergence` transitions (new allowlisted
   reason) instead of silently re-baselining.

## Round 3 (2026-09-15, user-approved): closing the open items

Each item below names the mechanism that closed (or narrowed) it, its
offline tests, and the live re-verification (`qualification/live/session_d_*`,
experimental host on 8950/8951, fresh store, hooks installed in the
workspace: `bin/fusion-prompt-marker` on `UserPromptSubmit`,
`bin/fusion-post-compaction` on `PostCompaction`).

| Open item | Closed by | Offline | Live |
|---|---|---|---|
| Hook `session_id` ≠ wire field 16 → compaction never correlated | **Authenticated native marker.** The documented `UserPromptSubmit` hook injects `additionalContext` that arrives inside `GetChatMessage` as a user-source message (verified live first: `qualification/live` probe). `bin/fusion-prompt-marker` renders `fusion-relay-marker v1 <session-name> <prompt_id> <HMAC(identity key)>`; `fusion_relay/marker.py` extracts and verifies the last marker; the coordinator matches compaction notes by the marker's session-name reference first. | `test_marker` (4), handler test `test_compaction_correlated_through_verified_marker` (forged marker → `invalid`) | `marker=verified` on every lead turn, `absent` on the sidekick's native forward; after `/compact` the next user turn recorded `epoch_transition=history_compaction`, `compaction_correlation=matched_marker`; `RECALL 5` correct |
| Acceptance always `pending` (no native acker) | **History-evidenced acceptance** (spec §3, qualified): when a later request's history reproduces a turn's exact visible projection, `reserve()` marks it `history_evidenced` inside the same transaction — a distinct level, never `acknowledged`; ack still wins; offer never downgrades. | `test_continuation` (+2), handler test (record `turns_history_evidenced`, diagnostics `acceptance_prior`) | 38 turns `history_evidenced` in the session-d ledger; `evid=1` on turn 2 of the TUI session |
| Every epoch transition lost prior reasoning | **Carry-over transitions.** `scope_bindings` table + `find_carry_candidate()` (same account/lane/profile, all committed anchors extended by the new history) + `transition_epoch(..., carry_from=)` re-sealing the turns under the new scope (operations never copied). Host uses it for `legacy_history`. | `test_continuation` (+2), host loopback asserts the stored reasoning item is reinserted into the resumed turn's provider body | `devin -r`: `carried: True`, resumed turn committed as revision 2 with 1 provider call, `AGAIN 5`; 11 of 12 benchmark resumes carried (the one without was a profile change, correctly not carried) |
| Cancellation latency bounded only by provider event cadence (58 s) | **Reader thread + 0.5 s cancellation poll** in `call_codex`; socket shutdown unblocks the reader. Provider-side cancellation is still unconfirmed by any signal. | `test_silent_provider_cancellation_bounded_by_poll` | kill at 04:03:46.9 → relay `cancelled` at 04:03:47 (≈8.5 s request, <1 s after the kill; previously 9→18 s and 8→58 s) |
| Production restart risk (SIGTERM left accounting dirty) | `serve()` turns SIGTERM/SIGINT into a clean shutdown (`_graceful_signals`). | `test_graceful_shutdown` (subprocess; 0 dirty runs after SIGTERM) | used for the production restart below |
| `qualification` label hard-coded `local_tests` | **Evidence-bound receipt** (`fusion_relay/qualification.py`, `fusion-relay qualify`): levels + sha256 evidence refs + `build_identity`, HMAC-signed with the identity key; diagnostics report the level only while the running tree's build identity matches (`qualification_receipt: verified|stale_build|invalid|absent`). | `test_qualification` (5) | receipt recorded for the final tree (see below) |
| Step 7 benchmark not run | `qualification/bench/run_bench.py` + `summarize.py`: 3 profiles × 6 tasks × 2 repeats, seeded interleaving, resume-recall step per run, Wilson/Newcombe/bootstrap intervals. | harness authored and reviewed by the lead | `qualification/bench/BENCHMARK_RESULTS.md`: task success 10/12 on **all three** profiles (identical), restart recall 10/12 on all three, CLI errors 0; relay−native difference **INCONCLUSIVE** (+0 pp [−31, +31]); the failing task failed natively too (SWE-2 sidekick rate-limited by Cognition at run time) |
| Live-found relay defect: `selection_unconfirmed` when `AssignModel` overlaps the turn's `GetChatMessage` | bounded grace wait (`SELECTION_GRACE_S`=2 s) before failing closed | `test_in_flight_selection_settles_within_grace` | observed in 3 benchmark runs before the fix |

Also observed live in this round: a store whose Keychain key had been
deleted refused to open (`continuation authentication failed`) instead of
accepting a freshly created key — the no-key-replacement rule holds.

## Limitations that remain true

- `verified` binding still proves possession of a host-issued capability;
  a verified **marker** proves the packet came from a CLI session running
  the relay's hook for a named turn — neither is Fusion-internal lane
  identity. The `lead` lane remains a routing label. The marker's absence
  on sidekick-originated requests is an observation, not a contract.
- `acknowledged` acceptance still requires a consumer ack that no native
  component sends; `history_evidenced` is the strongest evidence available
  and is labelled as such.
- `/compact` sends the lead a compacted-history request *before*
  `PostCompaction` fires, so that first request still diverges without a
  correlated note (`history_divergence`); the following user turn is
  correlated (`history_compaction`). Reasoning items summarised away by
  compaction are not recoverable by anyone.
- Provider-side cancellation and billing of a cancelled inference are
  never confirmed; detection latency is now ≤0.5 s + RST propagation.
- The benchmark is small (12 runs per profile) and approximate by design;
  it cannot show zero degradation. Native token usage is unobservable.
- Cross-profile carry-over is deliberately refused (a different model or
  effort must not inherit another's encrypted reasoning).
