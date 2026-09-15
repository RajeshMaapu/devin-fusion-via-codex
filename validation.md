# Validation — remediation gate

## Post-patch image-result verification (2026-09-14)

This section supersedes the older test counts, PNG-only description, and
installed-service status below. The earlier recovery notes and remediation
matrix remain historical, not claims of current live-provider qualification.
The tested tree is HEAD `d42d4c4bb68bedd000b1a7864c56914922af038b` plus the
existing uncommitted changes and untracked modules/fixtures. Those changes
were preserved; this verification added integration coverage, extended the
large-image regression, and updated this document. No commit or push was made.

### Complete post-patch gate

Commands, run from `/Users/mappu/projects/fusion-codex-relay` with the installed
service's interpreter:

```bash
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover -s tests -p 'test_privacy_transport.py' -v
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover -s tests -v
git diff --check
```

- Scoped transport/integration run: **71 tests, OK, exit 0**.
- Complete suite after the image patch and new integration tests:
  **398 tests, OK, exit 0; no failures, errors, or skips**. JPEG tests ran,
  rather than being skipped for a missing decoder.
- `git diff --check`: **exit 0**, no whitespace errors.
- Evidence: `/tmp/devin-toolimg-verify2/test_privacy_transport.txt` and
  `/tmp/devin-toolimg-verify2/full-suite.txt` (complete output and exit codes).
- The full run emitted non-failing `ResourceWarning`s for unclosed subprocess
  files during runtime tests, including `test_bad_init_and_tools_rejected`
  and `test_cancel_during_consent_kills_child`. The earlier pre-image-repair
  log `/tmp/relay-testgate-1789368495/unittest.log` already contains unclosed
  file warnings during `test_bad_init_and_tools_rejected`. Thus this warning
  class predates the repair; every individual warning has not been separately
  attributed. There are no test failures to classify as pre-existing versus
  regressions. No clean-HEAD comparison is claimed: HEAD alone omits substantial
  pre-existing working-tree work and would not be an equivalent baseline.

### Isolated captured-CLI integration

`ServerFixtureTest.test_captured_tool_image_downstream_serialization` in
`tests/test_privacy_transport.py` reuses the previously captured CLI 3000.10.21
synthetic PNG-read message, `tests/fixtures/tool-image-message.bin`, verbatim.
The companion PNG is `tests/fixtures/tool-image.png`. Historical capture
metadata remains at `/tmp/relay-imgcap-1789369378/tool-result-meta.json`.
This verification reuses that capture; it does not claim a newly launched CLI
session or capture of the original failing conversation.

The test wraps the captured message with a synthetic matching assistant tool
call and session/model fields, sends a Connect-framed request over real
loopback HTTP to an isolated relay handler, and runs the real wire decoding,
translation, tool-call wrapper, and `urllib.request.Request` JSON serialization.
Only the downstream `urlopen` boundary is mocked, returning synthetic SSE;
authentication, storage, and route state are isolated with fake credentials
and temporary directories. No request reaches the private provider endpoint.
Both `buffer` and `delta` modes pass, including a successful Connect trailer.

Assertions verify the actual serialized downstream body preserves:

- PNG bytes exactly, checked by strict base64 decoding against the fixture;
- MIME type `image/png` in the `data:image/png;base64,...` input-image part;
- accompanying text `[Image 1]` as an `input_text` part;
- tool-call ID `synthetic-call-1` on both call and `function_call_output`;
- upstream model `gpt-6-astra` and reasoning `{"effort": "high"}`.

The captured downstream JSON is retained at
`/tmp/devin-toolimg-verify2/downstream-request-body.json`. Its temporary capture
runner and output are `/tmp/devin-toolimg-verify2/capture_runner.py` and
`/tmp/devin-toolimg-verify2/capture-runner.txt`. The durable test does not write
request artifacts. These are synthetic evidence files, not production logs.

`test_invalid_tool_images_never_reach_downstream` additionally verifies that
malformed protobuf, unsupported `image/svg+xml`, and an envelope exceeding a
patched-down byte limit return `invalid_argument` without calling `urlopen`.
Existing unit rejection tests remain intact for invalid base64, duplicate or
missing fields, MIME/content mismatch, malformed image bytes, excessive image
count, absent call ID, unknown fields, and size/dimension/decompression bounds.
The large-image preservation regression now covers envelopes larger than
both 22,588 and 59,107 bytes; these synthetic values are not the original
failure payloads. The integration oversize case uses a reduced limit to test
the guard, not an actual production-limit-sized upload.

Current translation accepts the strict field-10 base64/MIME envelope for
PNG and JPEG, at most four images per tool result and 8 MiB decoded image
bytes per image, with image validators enforcing their additional bounds.
PNG remains limited to the native validator's subset; JPEG validation uses
Pillow (`requirements-image.txt` pins `Pillow==11.3.0`). Valid JPEG bytes
labelled `image/png` are identified as JPEG and serialized as `image/jpeg`;
PNG labelled JPEG is rejected. This is the existing implementation tested
here, not a newly broadened acceptance policy.

### Installed relay versus working tree

Read-only inspection of the launchd job and listener established:

- LaunchAgent: `gui/501/ai.maapu.fusion-relay`, configured by
  `/Users/mappu/Library/LaunchAgents/ai.maapu.fusion-relay.plist`.
- Command: `/usr/bin/python3 -m fusion_relay.relay 8931`. The running executable
  resolves to the Xcode Python framework's Python 3.9 process.
- Both `WorkingDirectory` and `PYTHONPATH` point to
  `/Users/mappu/projects/fusion-codex-relay`; it runs this source tree directly,
  not an installed wheel or separate build output.
- After the explicitly approved restart, PID **92406** started at
  **2026-09-14 09:53:12 local time**, with the expected cwd and listener
  `127.0.0.1:8931`. `/healthz` returned `{"ok": true}`. Restart evidence:
  `/tmp/devin-toolimg-verify/restart-output.txt`.
- The translator/image-validator edits predate that process start
  (`translate.py` 00:21:13, `images.py` 00:26:26, `artifacts.py` on Sep 13).
  The old PID 98989 had started on Sep 13 before these fixes. The new process
  loads the current source at startup; this is launch/source provenance,
  not introspection of the old process's imported bytecode.

**No build or install step is needed on this machine**, and no additional
restart was performed for this follow-up: only tests and documentation changed.
The prior restart was approved after warning about interrupted traffic,
cleared in-memory caches, and loading all existing local changes, including
disabled relay-owned computer dispatch. Future source changes require a
restart to affect already imported modules. Deployment elsewhere must include
the untracked `artifacts.py`, `images.py`, required local dependencies, and
fixtures/tests as appropriate; the Git commit alone does not contain this repair.
No dependencies or global configuration were changed during verification.

### Fusion model selection: configuration versus observations

Read-only inspection of `/Users/mappu/.config/devin/config.json` found both
`agent.model` and `agent.preferred_family_models.fusion` set to exactly:

```text
fusion-gpt-6-astra-high-sidekick-swe-2-medium
```

This config selects an Astra lead at **High** and SWE-2 sidekick at **Medium**.
The separate `preferred_family_models.swe-2 = swe-2-max` value is a standalone
family preference, not the sidekick segment of the configured Fusion selector.
`bin/devin-fusion` sets the relay endpoint and passes CLI arguments unchanged;
it does not select or raise reasoning effort. CLI documentation also supports
`--model`/`DEVIN_MODEL`, `/model`, and session thinking-level changes. The
inspected CLI processes had no `--model` argument, and `DEVIN_MODEL` was unset
in the verification shell; this does not establish every session's overrides.

At the relay boundary, model routing uses GetChatMessage field 21, with
persisted session route pins also able to select native forwarding. The
isolated integration supplies `gpt-6-astra-high` and proves the downstream
serialization is `gpt-6-astra` with `reasoning.effort = high`. The translator
maps `-max` to `xhigh` but does not map `-high` to `xhigh`. SWE requests are
forwarded unchanged under the installed default auxiliary policy; the relay
does not impose a separate Codex reasoning value on the sidekick.

The current chat's supplied runtime label says **GPT-6 Astra High Thinking +
SWE-2 Medium**. That label is an observation distinct from the config and from
a captured provider request; the exact selector and effective settings of the
interrupted conversation have not been extracted or replayed. Sanitized relay
records omit raw model and effort fields and mark role provenance unverified,
so they cannot supply session-specific proof of lead/sidekick settings here.
No High-to-XHigh change, model reassignment, or settings write was performed.

### Separately approved live synthetic check

After the isolated gate, the user's explicit approval for a synthetic live
image check was exercised with **exactly one** POST through the installed
relay on port 8931. No additional restart was needed. A fresh synthetic
session ID, the captured PNG tool-result message, a synthetic matching prior
tool call, and a request to describe the image were used. Field 21 was
`gpt-6-astra-high`; no tool definitions were offered and no tools were executed.
Only the existing local relay access token was read for authentication; no
token, token-bearing URL, request headers, or provider credentials were
printed or persisted in the evidence.

Observed result: **HTTP 200**, a clean Connect end trailer (`{}`), and **zero
tool calls**. The actual response was:

> The image has tightly packed, repeating diagonal rainbow stripes running
> from bottom left to top right. Bright green, yellow, pink, red, cyan, and
> blue bands create a vivid, slightly pixelated pattern.

Direct visual inspection of `tests/fixtures/tool-image.png` confirms the
fixture contains diagonal rainbow stripes consistent with that description.
This establishes successful live acceptance and an image-consistent response
for this synthetic fixture through the installed relay, not general visual
accuracy or recovery of the interrupted conversation. The live request's
routed identifier is observed; the downstream model/effort serialization is
proven separately by the isolated capture and translator, not by logging live
provider request bodies.

Evidence: `/tmp/devin-toolimg-live/live_check.py` (one-shot runner),
`/tmp/devin-toolimg-live/result.json` (sanitized result),
`/tmp/devin-toolimg-live/response.bin` (synthetic response only), and
`/tmp/devin-toolimg-live/stdout.txt` (output and exit 0). The full suite was not
rerun after this check because no implementation or test files changed.

### Remaining qualification boundary

The captured synthetic PNG failed on the pre-fix translator with a 2,002-byte
field-10 rejection (historical evidence:
`/tmp/toolimg-prefix-1789369807/prefix.log`) and now passes local integration.
The original **59,107-byte** failure has not been captured, reproduced, or
replayed against the repaired version. Its content cannot be identified from
its size alone, and **the original protocol error is not claimed resolved**.

No original conversation replay or desktop action was performed. The full
suite and isolated integration used fake upstreams; the separately approved
one-request synthetic live check above is the only live-provider inference
performed for this verification. Neither synthetic check identifies or
reproduces the original 59,107-byte payload. Historical computer/runtime
qualification limitations below are not cleared by these tests.

## Interrupted-session recovery check (2026-09-13)

At HEAD `d42d4c4`, with the existing uncommitted approval, artifact,
broker, lease, runtime, and journal-delivery work preserved, the command
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v`
completed with **366 tests, OK (exit 0)**. Evidence:
`/tmp/relay-testgate-1789368495/unittest.log`. `git diff --check` was clean.
This check used local tests, including fake runtime children and loopback
servers; it did not restart installed services, access real credentials,
run live inference, or perform desktop actions.

The initial synthetic rejection test used 22,588 arbitrary bytes, not the
original failing payload. After user approval, an isolated CLI 3000.10.21
session with fresh HOME/XDG directories, fake credentials, a loopback-only
mock inference server, and a network-restricted child captured real client
encoding without live inference or desktop actions. A plain-text read had
no field 10; a synthetic PNG read produced source-4 field 10 containing
nested protobuf field 1 (base64 PNG) and field 2 (`image/png`). Decoded image
bytes exactly matched the fixture. Evidence:
`/tmp/relay-capture-1789368837/tool-result-meta.json` and
`/tmp/relay-imgcap-1789369378/tool-result-meta.json`.

The PNG capture is retained as `tests/fixtures/tool-image-message.bin` and
`tests/fixtures/tool-image.png`. Its regression failed before the fix with
`message field 10 (source 4) carries a 2002-byte payload the translator
cannot represent`: `/tmp/toolimg-prefix-1789369807/prefix.log`.
The original 22,588-byte request was not captured; the PNG read establishes
one confirmed path to the same error, not the identity of that old payload.

`translate.py` now decodes that verified tool-result envelope, validates
PNG content using the existing `artifacts.validate_png`, and emits typed
`input_text`/`input_image` parts inside `function_call_output.output`,
preserving the call ID and actual image bytes. This format is specified by
https://developers.openai.com/api/docs/guides/function-calling and the local
Codex app-server schema captured at
`/tmp/fusion-relay-protocol-discovery-20260914/v2/ThreadResumeParams.json`
(`FunctionCallOutputBody` and `FunctionCallOutputContentItem`). The private
live inference endpoint has not been qualified by this check.

Supported envelope: exactly one base64 field and one `image/png` MIME field,
at most four images per tool-result message, at most 8 MiB decoded per image,
and the validator's noninterlaced 8-bit RGB/RGBA PNG subset (dimension,
pixel-count, CRC, and decompression bounds enforced). Other MIME types,
malformed envelopes, and unknown consequential fields remain rejected.
This path uses inline bytes, does not open model-supplied paths, and does not
create artifact-store records or enable computer dispatch. Deployment must
include the existing untracked `fusion_relay/artifacts.py` dependency.

After the translator change, scoped checks passed: **11 image tests** and
**66 relay tests**. Commands:
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_tool_images.py' -v`
and
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_relay.py' -v`.
Evidence: `/tmp/toolimg-postfix-1789369830/toolimg.log` and
`/tmp/toolimg-postfix-1789369830/relay.log`.
The full 366-test result above predates this translator repair; it was not
rerun afterward. No installed service was restarted, no global defaults or
canonical credentials changed, and no commit or push was made. To roll back
this repair, reverse only the new image-translation hunk and its new tests;
preserve all preexisting remediation work, route state, and evidence.
Final targeted rerun: `/tmp/toolimg-final-1789369963/toolimg.log`.

The earlier gate and R01–R11 table below are historical evidence for the
previous remediation diff, not an updated assessment of the unfinished
local modules. Passing this recovery gate does not qualify native
Fusion role enforcement, live consent, visual feedback, or full recovery.

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

## 2026-09-14 repair implementation (working tree based on f310fc7)

Implemented in the working tree (uncommitted): bounded and indexed
refusal/terminal response handling; bounded runtime-handle cleanup with
unconfirmed-termination reporting; redirect-blocked upstream transport;
HMAC service identity with dirfd-pinned private storage and a Python
launcher/manager (no shell PID signaling or auto-spawn trust); native
Connect streaming for non-Astra Cognition routes (raw bytes, chunked
downstream framing, bounded cleanup); transactional durable accounting
(SQLite receipts, at-least-once export, sticky degradation); host-only
AES-GCM-sealed continuation ledger with Keychain adapter and durable
mode that fails closed without a trusted binding; session diagnostics
(hashed refs, whitelisted fields, bounded LRU).

### Gate

- Full suite BEFORE the final narrowly tested diagnostic/reconciliation
  additions: **662 tests, OK, 41.039s** (`.scratch/final_full_suite.log`,
  `-W error::ResourceWarning`, no warnings-as-errors tripped).
- The final reconcile/diagnostics edits were covered by targeted runs
  only (`.scratch/verify_test_*.log`); the full-suite number above does
  not claim the post-edit tree was re-run end to end.
- `git diff --check` clean; `bash -n` on launcher scripts clean.

### Qualification NOT done

- Authenticated native lane / ACK binding absent — durable continuation
  is opt-in and requires an attached host coordinator; legacy default
  remains memory-only and unqualified.
- Keychain tests are mocked plus SDK-signature verified — no real
  Keychain integration was exercised.
- Global retention/compaction/model-switch epoch operator path is not
  integrated.
- Native transport: HTTP trailers unsupported (explicit failure);
  gzip-encoded upstream bodies pass through with usage "unknown";
  TLS/proxy env trust disabled (`trust_env=False`) explicitly.
- Snapshot accounting: the service requires the ledger at startup; old
  JSON stats are marked unverified and never mixed into durable totals.
- Dirty runs require explicit offline reconciliation acknowledgment;
  unknown receipts are preserved, never zeroed or deleted.
- Discovery HMAC bootstrap does not authenticate each future Devin TCP
  connection (TOCTOU limitation) — not complete origin pinning.
- The old live relay (if running) is untouched. The new launcher refuses
  to trust that pre-handshake service until an approved restart.
- No real-provider, native-cycle, benchmark, or computer-dispatch
  qualification was run; no parity is claimed.

### Environment note

The earlier httpx install into the user's Python upgraded `certifi` to
2026.7.22 as a transitive dependency; the later requirements work was a
dry-run only and changed nothing installed.

## Reviewed restart — 2026-09-14

The user-approved `launchctl kickstart -k` of the existing
`gui/501/ai.maapu.fusion-relay` job was executed against the reviewed
instance (old PID 92406, started 09:53:12). The service now runs the
working tree based on `f310fc7` — `build_identity` fingerprint
`8bc9e2dd221fce200572517c90e2adc5c234931663ccf635d14bfb6c4eff602a`.

- New PID 75991, started 14:31:55, same interpreter/command/port.
- `/healthz`: ok=true, accounting degraded=false, coverage=partial
  (pre-existing receipts with missing token fields), export_degraded=
  false, reconciliation_required=false.
- Verified launcher path (`bin/fusion-relay status`) and
  `ensure_service` both confirmed the new instance by HMAC identity —
  no respawn, no fallback. `/capabilities`: continuation
  `legacy_memory_only_unqualified`, `native_ack_contract` unavailable,
  native transport streaming, computer dispatch still disabled.
- Restart cleared the in-memory continuity caches only; persistent
  state was preserved and no reconciliation was required or run.
- No inference, provider, Keychain, or billing calls were made. This is
  a service restart verification, not a live Fusion/inference
  qualification — no parity claim.
- Evidence: `.scratch/reviewed_restart.log`,
  `.scratch/restart_preflight.log`, `.scratch/restart_verify.log`.
