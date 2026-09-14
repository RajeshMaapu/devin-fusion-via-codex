# Validation — remediation gate

**Scope of this gate.** Everything below was verified only on the local
remediation diff (base `60224b1`, all changes uncommitted) using unit
tests plus real loopback HTTP sockets bound to `127.0.0.1` with faked
upstream/auth. No installed relay, no real Codex auth, no desktop actions,
and no provider inference were exercised — every test uses fake
credentials, fake upstreams, temporary directories, and fake actuators.
This document does **not** claim the work is complete or that the relay
is qualified against live services.

## Measured gate

| Command | Result |
| --- | --- |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v` | 209 tests, **OK** (exit 0) |

Raw output: `/tmp/remediation-evidence-Il10/full_validation.txt`.
Per-module evidence: `final3_test_relay.txt` (62 OK),
`final3_test_remediation_runtime.txt` (38 OK),
`final3_test_privacy_transport.txt` (66 OK),
`final3_test_route_durability.txt` (31 OK),
`final3_test_remediation_regressions.txt` (5 OK) — all under
`/tmp/remediation-evidence-Il10/`. Baseline (pre-fix) regression evidence:
`baseline_regressions.txt` (5/5 expected failures against `60224b1`).

Post-review targeted fixes (serve ownership ordering, deterministic
concurrency, nested tool-payload rejection, decompress bound) were
verified by scoped reruns, not a second full gate:
`review_final_test_relay.txt` (65 OK),
`review_final_test_remediation_runtime.txt` (38 OK),
`review_final_test_privacy_transport.txt` (69 OK).

Environment: Python 3.9.6, macOS; `git diff --check` clean;
`git rev-parse HEAD` = `60224b11de43117f57ae818e4c774ae7a8844ccc`;
working tree contains the full uncommitted remediation diff.

## Remediation item status (R01–R11)

| # | Area | Status | Honest position |
| --- | --- | --- | --- |
| R01 | Consent UI / app grants / revoke | **Blocked** | No authenticated consent UI exists; elicitation is answered `decline`. No approval adapter — disabling is not solving. |
| R02 | Trusted role binding / native dispatcher enforcement | **Blocked** | No role provenance on the wire; the native tool dispatcher and shell paths are outside the relay's control. `native_dispatch_enforcement: unsupported`. |
| R03 | Image / vision path | **Partial** | Adapter disabled; image blocks would report `vision_unavailable`, no new screenshot writes. Typed artifacts/multimodal delivery/live visual test remain blocked. |
| R04 | Tool-loop terminal correctness | **Implemented (local tests)** | Budgets 1/2/16 raise `tool_budget_exhausted` when exhausted; no executed relay call is returned downstream; journal retains outcomes; live provider contract unqualified. |
| R05 | Desktop lease / focus / user-interference fencing | **Blocked** | The operation journal dedups operations; it is explicitly **not** a desktop lease. Logical runtime contexts unavailable. |
| R06 | Cancellation lifecycle | **Partial** | Client disconnect and `RequestContext` cancellation observed at all relay boundaries; blocking upstream reads are timeout-bounded — **no active provider-side cancellation**. |
| R07 | Durable operation state | **Partial** | `OperationJournal` covers explicit fake-actuator callers only (local contract). No production hidden actions, consumer ack, durable reasoning, retention policy — and **no complete recovery claim**: `outcome_unknown` requires explicit reconciliation. |
| R08 | Usage accounting | **Implemented (scoped)** | Per-response wire usage (unknown fields omitted), aggregated stats/log via `usage.py`; unknown calls never coerced to zero. Dedup is per-request / provider response-id only — no cross-request or global billing dedup, and unidentified responses cannot dedup safely (unknown/partial counted explicitly). |
| R09 | Credential ownership / log privacy | **Implemented (scoped)** | Auth is read-only — no refresh/lock/rewrite of the canonical file; expired login fails closed. Logs are allowlist-sanitized with **partial diagnostic capture only**; pre-remediation logs/screenshots were **not purged** — separate retention decision. File-leaf symlinks are rejected at open; parent-symlink or hostile same-user path races are not fenced. |
| R10 | Bounded transport / strict wire | **Partial** | Request/response/SSE/frame bounds enforced; buffering is capped, **not** streaming backpressure; headers are bounded by socket idle timeout only (no total header deadline); upstream "success" qualification is protocol-local, not vendor-verified. Pending-selection durability (revisions, write-error blocking) is tested, but there is no reconciliation UI; unary `application/proto` success assumes Connect semantics, vendor schema unqualified. |
| R11 | Provider/runtime qualification | **Blocked** | CUA runtime disabled; no negotiated protocol or instruction-skill qualification; live Fusion/runtime gates were not run. |

## Change summary (this remediation diff)

- **Transport**: bounded request body / upstream response / SSE line /
  decompression; strict Connect framing with no fallback; explicit failure
  on `Transfer-Encoding`, non-identity `Content-Encoding`, upstream
  `Trailer` headers; 32-handler bound; per-read socket deadlines.
  Evidence: `test_privacy_transport.py` —
  `RequestBodyTest`, `ForwardBoundsTest`, `ServerFixtureTest`.
- **Privacy**: `safe_record` allowlist projection before any log/stat
  write; no prompts, tool args, models, bodies, arbitrary headers, or
  exception strings persisted. Exact allowlisted numeric quota headers are
  retained separately from usage totals. Evidence: `SafeRecordTest`,
  `LogPrivacyTest`.
- **Storage**: 0700 dirs, `O_NOFOLLOW` leaf checks, atomic 0600 writes,
  bounded reads, serialized appends, `store_owner` flock.
  Evidence: `StorageTest`, `TokenAndAuthTest`, `StoreOwnerTest`,
  `ServeOwnershipTest`.
- **Routing durability**: versioned route state, pending selections,
  strict assignment outcome classification, fail-closed lookups.
  Evidence: `test_route_durability.py` — `RouteStoreLoadTest`,
  `RouteStateTest`, `AssignmentOutcomeTest`.
- **Tool loop**: pre-dispatch validation of all call IDs, journal-backed
  once-only execution, `outcome_unknown` never auto-retried, per-iteration
  emission, undecodable nested payloads rejected.
  Evidence: `RelayToolLoopTest` (`test_relay.py`),
  `OperationJournalTest` (`test_remediation_runtime.py`).
- **Cancellation**: `RequestContext` boundaries; cancelled-but-connected
  clients get a `cancelled` trailer; no provider-side cancellation.
  Evidence: `RequestContextTest` (`test_remediation_runtime.py`).
- **CUA**: executor removed (`None`), `computer_status: blocked`,
  `/capabilities` reports unsupported enforcement/consent/detached/image.

## Host-hook finding

Devin 3000.10.21 documents a `PreToolUse` hook payload carrying
`tool_name`, `tool_input`, `session_id`, and `prompt_id` — usable for
policy hooks, but **no documented Fusion lead-role attestation** exists on
the wire: nothing cryptographically or contractually binds a request to
the lead role, so trusted role enforcement cannot be built from the
documented surfaces. R02 remains blocked.

Pending route selections (`selection_unconfirmed`) require an external
reconciliation contract/UI; none exists, so affected sessions stay blocked
rather than guessing a provider.

## Known structural caveats

- v2 `routes.json` state is **not** readable by the pre-remediation code —
  the older reader silently ignores `version` and drops pins. Do not run an
  old binary against v2 state; migration needs separate review, and the
  state file must never be deleted as a "fix".
- Route state is single-writer: `serve` holds `.owner.lock`
  (`store_owner`, nonblocking flock) for the data directory. The module
  APIs themselves are not multi-process safe — only `OperationJournal`
  uses a cross-process unique key.
- `read_private` fails on oversized state rather than returning a
  truncated prefix. Storage helpers reject symlinked target leaves and
  enforce 0600 files / 0700 owned directories; they do not fence parent
  symlinks or hostile same-user path replacement races.
- Trusted-host contract required to ever enable computer dispatch:
  authenticated consent UI, a trusted Fusion role binding on the wire,
  dispatcher/shell enforcement, and a qualified runtime — none of which
  exist in this repository.
- A running installed relay (if any) still runs the old code — services
  were deliberately untouched; deployment is the owner's decision.

## Rollback note

This diff is code-only: review/revert the uncommitted changes before any
deployment. No destructive git operations were used, none should be. If a
relay is already running, it keeps running old code until deliberately
restarted.
