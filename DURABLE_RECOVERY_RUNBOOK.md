# Durable continuation — recovery runbook

Scope: the host-attached durable continuation store
(`fusion_relay/continuation.py`, schema v2), its Keychain-held key, and the
relay's admission/recovery behaviour around it. Everything here is
operator-driven; the relay never auto-repairs, auto-replays after an
unknown outcome, or auto-restarts an installed service.

Hard rules (from `AGENTS.md` and the handoff spec):

- Do not restart or stop a running production relay without explicit
  approval.
- Never print, log, or copy key material, prompts, reasoning, or tool
  arguments. Diagnostics carry hashed references and allowlisted labels only.
- A reservation whose provider outcome is unknown is **never** retried
  automatically. Resolution requires an explicit evidence reference.

## 0. Read the state first

```sh
# capabilities: continuation label, ack contract, payload budget, key store
curl -s http://127.0.0.1:8931/t/$(cat ~/.local/share/fusion-codex-relay/relay-token)/capabilities
# per-session diagnostics (session ref = sha256 reference, from requests.jsonl)
curl -s "http://127.0.0.1:8931/t/<token>/diagnostics?session=<hex64>"
```

Diagnostics fields and what they are derived from:

| Field | Values | Derived from |
|---|---|---|
| `binding` | `verified` / `unavailable` / `invalid` | capability verification result on the last request; `unavailable` for native CLI traffic and for resolver-mode hosts |
| `continuation` | `durable` / `legacy_memory_only` / `blocked` | admission outcome (`continuation_detail` keeps the raw label, e.g. `preflight_rejected`) |
| `acceptance` | `acknowledged` / `pending` / `uncertain` | `continuation_turns.state`; only an explicit `/host/ack` yields `acknowledged` |
| `key_store` | `ready` / `unavailable` | whether the coordinator was constructed from a Keychain-loaded key |
| `cancellation` | `requested` / `uncertain` / `not_observed` | client disconnect observed; never `confirmed` (no provider confirmation signal exists) |
| `qualification` | `local_tests` | constant until live/benchmark evidence exists |

## 1. Payload budget rejection (`resource_exhausted`, `error_category: payload_budget`)

Symptom: the client receives
`payload budget exceeded (<kind>: <measured> > <limit>, policy v1, local limit; upstream limit unverified). Recovery: compact the conversation (/compact) or start a new session from a checkpoint; retrying the same request will not succeed.`

`rejection_origin` in `requests.jsonl` tells you which check fired:
`local_wire_bytes` (before translation), `local_image_count` /
`local_image_bytes` (translated body), `local_translated_bytes` (exact
outgoing bytes, includes continuation reinsertion and tool-loop growth).

Recovery:

1. Do **not** resend the same history. Adding text does not remove images.
2. In the CLI run `/compact` (verified command). If the
   `PostCompaction` hook is installed (Section 6) the relay receives a
   compaction notice and allows one `history_compaction` epoch transition
   for that session hash on the next request.
3. If `/compact` is unavailable or insufficient, `/fork` or start a new
   session carrying a written checkpoint (decisions, files, unresolved
   work, only the screenshots still needed). This is a new epoch; the
   original session is preserved, not deleted.
4. In durable mode a post-merge rejection leaves the operation row in
   `preflight_rejected`; the lane is free — an identical retry re-reserves
   cleanly, a changed body is a new operation. Nothing to repair.

`upstream_*` origins mean the provider rejected the request after
dispatch (bounded error body classified, then discarded). In durable mode
that operation is `provider_outcome_unknown` — see Section 3.

## 2. `trusted continuation binding unavailable` / `binding: invalid`

- `unavailable`: no capability headers. Expected for native CLI traffic;
  durable mode is host-attached only. Either run the qualified host that
  issues capabilities (none exists for native Fusion — see
  `NATIVE_HOST_CONTRACT.md`) or use legacy mode knowingly.
- `invalid` (`permission_denied`): capability unknown / expired / revoked /
  proof mismatch / lane-account-session-profile mismatch / body digest
  mismatch. Nothing was reserved or dispatched. The host must issue a
  fresh capability with matching fields; the relay never relaxes the check.

## 3. Unknown provider outcome (`continuation outcome unknown; explicit reconciliation required`)

Operation statuses that block the lane: `executing` (reservation in
flight or process died before any transition), `provider_outcome_unknown`
(dispatch may have happened; no committed result), `cancel_unconfirmed`
(client disconnected; upstream cancellation unconfirmed).

Any new reservation on that scope fails with
`continuation lane has an unresolved operation`. This is intentional: a
second inference on top of an unknown outcome is a possible second
billable call.

Resolution (operator, evidence-backed) — exercised live on 2026-09-15
(`qualification/live/row4_admin.log`):

1. Establish what happened out-of-band: provider dashboard, the
   `codex_http_status` / `codex_usage` / `client_gone` fields of the request
   record, `accounting.sqlite3` events. Write that evidence to a file and
   take its sha256 as the evidence reference.
2. Stop the host/relay only with approval (or use a separate test relay).
   The ledger is single-owner (`<store>.owner` flock); the admin tool
   refuses with `… (stop the host first)` while a host owns it.
3. `bin/fusion-continuation-admin list-unresolved --data-dir D` lists the
   lane-blocking rows (`executing`, `provider_outcome_unknown`,
   `cancel_unconfirmed`); `--include-retryable` adds the non-blocking
   `preflight_rejected` / `admission_rejected` rows.
4. `bin/fusion-continuation-admin resolve-unknown --data-dir D --scope S
   --operation-id OP --evidence-ref HEX64` settles the row to `failed` with
   the sealed evidence reference. Only `abandoned` exists: a claimed success
   cannot be reconstructed because no provider output was committed.
5. The client history now diverges from the ledger (the user saw an error,
   not an answer). The next request either extends the last committed
   revision (normal) or needs an explicit epoch transition.

`executing` rows left by a crash before inference or by an accounting
admission failure are resolvable the same way; since this change the relay
settles a post-reservation admission failure itself as
`admission_rejected` (retryable, not lane-blocking).

Accounting side: a dirty `accounting_runs` row (process died before the
clean close) makes the next start `degraded` and every admission fails
`durable accounting unavailable`. Reconcile offline with
`bin/fusion-relay reconcile --acknowledge-unknown --evidence-ref HEX64`
(`FUSION_RELAY_DATA_DIR` set; store must be unowned) — see
`qualification/live/accounting_reconcile.log`.

## 4. Epoch transitions (model switch, compaction, fork, account change, legacy history)

`reserve()` refuses a history that no longer extends the committed anchors
with `native history transition requires explicit epoch` (typed
`EpochRequired`). Silent re-baselining never happens.

- `history_compaction`: automatic **only** when a compaction notice for the
  same session hash arrived via `/host/compaction` and is unconsumed;
  exactly one transition, recorded in `epoch_transitions` with
  `prior_head`. Otherwise the request fails closed.
- `model_switch`, `fork`, `account_change`, `legacy_history`,
  `operator_recovery`: the host must call
  `ContinuationLedger.transition_epoch(binding, new_epoch, reason)` and
  issue a capability for the new epoch (or rely on `resolve_epoch`, which
  follows the recorded chain from the capability's epoch to the current
  one). Transitions are blocked while the old scope has an unresolved
  operation.
- The effective epoch and revision are returned on every durable response
  as `X-Fusion-Continuation-Epoch` / `X-Fusion-Continuation-Revision`;
  acks must reference the epoch of the turn being acknowledged.
- Old scopes are preserved for reconciliation. `retire_scope(scope)` is
  manual and refuses while any turn is not `consumer_acknowledged` or any
  operation is unresolved.

## 5. Key recovery and Keychain states

The AES-GCM ledger key lives only in the macOS Keychain
(service `ai.fusion-codex-relay.continuation`, account = hex64 account
reference). Error mapping from `MacOSKeychain.load()`:

| Error | Meaning | Action |
|---|---|---|
| `continuation key missing` | item not found and `create=False` | Do **not** create a new key for an existing ledger: `ContinuationLedger` refuses (`continuation key reference missing` / `continuation key mismatch`). Locate the original key (other user context, restored Keychain) or treat the ledger as unrecoverable and start a new epoch with a new store path. |
| `OS key store access denied` | `errSecAuthFailed` / `errSecInteractionNotAllowed` | Background/launchd contexts cannot answer Keychain prompts. Grant access interactively once under the same executable identity, or run the host in a context allowed to prompt. Never fall back to env/disk keys. |
| `OS key store unavailable` | keychain locked / not available / non-darwin | Unlock the login Keychain; verify `platform.mac_ver()` and `sys.executable` match the tested context. |
| `OS key store record invalid` | wrong length | The item was tampered with or written by something else. Do not overwrite; investigate. |

Cross-process qualification is opt-in:
`FUSION_RELAY_KEYCHAIN_LIVE=1 python3 -m unittest tests.test_keychain_live`
(uses a random `ai.fusion-codex-relay.test.<hex>` service, deletes it on
cleanup, may prompt). It is visibly skipped in the default gate. It was
run on 2026-09-15 in both the interactive shell and a one-shot launchd job
(`ppid=1`, uid 501, macOS 15.6.1 arm64, `/usr/bin/python3`): both passed
without a prompt and left no Keychain residue
(`qualification/live/keychain_*.log`).

## 5a. Experimental host (launcher-provenance bindings, UNQUALIFIED)

`bin/fusion-experimental-host --data-dir D --relay-port P1 --proxy-port P2`
runs a durable-mode relay in-process plus a header-injecting proxy the CLI
talks to (`source D/host.env` exports `WINDSURF_API_SERVER_URL`). It issues
`launcher_process_ownership` capabilities (lane `lead` inferred from the
Codex route — a label, not a verified identity), reuses operation ids for
identical bodies within 300 s, never sends acks, propagates client
disconnects to the relay (RST), and records explicit epoch transitions for
`model_switch`, `legacy_history` (resume under a new seed) and
`history_divergence` (anchors no longer match; cause not established,
e.g. after `/compact`). Prior reasoning items are lost at every transition.
Key: Keychain service `ai.fusion-codex-relay.experimental`, account =
sha256 of the resolved data dir. Stop with SIGTERM (waits for the relay
thread so the accounting run closes clean). Never point it at the
production data directory or port.

## 6. Installing the compaction notice hook (optional, host-side)

Add to the CLI hooks configuration a `PostCompaction` command hook running
`bin/fusion-post-compaction`. The script forwards only
`{protocol_version, session_id, summary_present}`; the summary never
leaves the hook process. The relay endpoint rejects any body carrying a
`summary` key. Correlation of the hook `session_id` with wire field 16 is
unverified; a mismatch simply means no transition is authorised.

## 6a. Installing the turn marker hook (recommended with the compaction hook)

Add a `UserPromptSubmit` command hook running `bin/fusion-prompt-marker`
(with `FUSION_RELAY_DATA_DIR` pointing at the relay's data directory so
it can read `identity.key`). It injects one short authenticated line per
turn into the agent context; the relay verifies it and reports
`marker: verified|absent|invalid` in diagnostics. With both hooks
installed a `/compact` is correlated to the wire session
(`compaction_correlation: matched_marker`) and recorded as an explicit
`history_compaction` transition. Never treat the marker as a lane
identity.

## 6b. Qualification receipt

`fusion-relay qualify --level live_verified [--level benchmark_evaluated]
--evidence PATH...` (with `FUSION_RELAY_DATA_DIR` set) writes an
HMAC-signed `qualification-receipt.json` bound to the current
`build_identity`. Diagnostics and `/capabilities` report the level only
while the running tree matches (`qualification_receipt: verified`); any
code change reports `stale_build` → `local_tests` until re-qualified.
Record the receipt AFTER the final code change and BEFORE restarting.

## 6c. Restarting the installed relay cleanly

1. Check for in-flight work: `lsof -nP -iTCP:8931 -sTCP:ESTABLISHED`.
2. Stop cleanly so the accounting run closes: `bin/fusion-relay stop`
   (posts `/shutdown` after an identity handshake). Since this change
   the relay also handles SIGTERM gracefully, so
   `launchctl kickstart -k gui/$UID/ai.maapu.fusion-relay` is safe for
   the NEW code; the pre-change process must be stopped via
   `fusion-relay stop`.
3. Start: `launchctl kickstart gui/$UID/ai.maapu.fusion-relay`
   (KeepAlive only restarts on failure, not after a clean exit).
4. Verify `/healthz` shows `degraded: false` and `/capabilities` shows the
   expected `qualification`.

## 7. Migration and downgrade

- Opening a v1 store migrates it to v2 (adds `response_digest`, `state`,
  `acknowledgements`, `epoch_transitions` with `to_epoch`). A marker row
  `schema_version = -2` is committed first; a crash mid-migration leaves
  `continuation store migration incomplete` — restore from backup or
  finish by reopening after inspecting the store; the relay will not guess.
- A store newer than v2 refuses to open (`schema newer than supported`).
- Downgrade: v1 code cannot read v2 columns it does not know about but
  SQLite tolerates extra columns for its existing queries; acknowledgement
  and epoch state would simply be ignored — so downgrading loses
  acceptance/epoch visibility but not encrypted turns. Keep pending
  deliveries and reconciliation records; do not delete the store.

## 8. Rollback of this change set

Code-only rollback: check out the previous tree. The v2 store remains
readable as described above. No production relay was restarted for this
work; the running service (if any) still executes the previous code until
an approved restart.
