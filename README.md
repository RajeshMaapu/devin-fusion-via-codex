# Devin Fusion via Codex

Bring your Codex subscription to Devin Fusion.

## Install dependencies

```bash
python3 -m pip install --require-hashes -r ~/projects/fusion-codex-relay/requirements.txt
```

All dependencies are hash-pinned; `--require-hashes` is required.

## Set your default model

With Devin CLI installed and Codex signed in, set Fusion Astra as your default in `~/.config/devin/config.json`. Merge this into your existing config, preserving other settings:

```json
{
  "agent": {
    "model": "fusion-gpt-6-astra-high-sidekick-swe-2-medium"
  }
}
```

This selects GPT-6 Astra High Thinking as the lead and SWE-2 Medium as the sidekick. If your default is already a supported Fusion Astra model, skip this step.

This user-wide default also applies to plain `devin`, but only the relay launcher below routes Astra through your Codex subscription.

## Start a session

```bash
cd "<your project>"
~/projects/fusion-codex-relay/bin/devin-fusion
```

The launcher uses your configured model; it does not choose Fusion automatically. No in-session model selection is needed when Fusion Astra is already your default.

To select Fusion Astra explicitly for a launch without editing your saved default:

```bash
cd "<your project>"
~/projects/fusion-codex-relay/bin/devin-fusion --model fusion-gpt-6-astra-high-sidekick-swe-2-medium
```

The TUI model picker may show only `Fusion`, without the full lead model or `Codex sub` label. Use the explicit model ID above rather than relying on picker labels.

> Note: Codex's computer-use skill has integration issues and is currently disabled in the relay.

## Why these changes were necessary

![A billing change needed a compatibility bridge: native Devin Fusion keeps orchestration and the native SWE-2 sidekick; lead requests go through the Fusion–Codex relay to Astra via the Codex subscription. Compatibility work covers request translation, response fidelity, context continuity, payload control, transport safeguards, and recovery + accounting. Still open and not guaranteed: native lane identity + acceptance ACK, compaction continuity + provider cancellation, native benchmark parity. Codex computer use is a separate optional integration.](docs/images/compatibility-bridge.png)

Switching Fusion’s Astra lead to a Codex subscription changes more than billing: Fusion and Codex use different request, response, and session contracts. The relay needs translation for tool calls, images, streaming, errors, and reasoning continuity while preserving Fusion’s orchestration and native SWE-2 sidekick. Recovery safeguards and validation help detect lost context, duplicate actions, and silent failures; they do not establish native benchmark parity.

The changes are extensive because the Fusion harness itself was kept intact: nothing in the CLI, the Fusion lead/sidekick orchestration, or the native SWE-2 route is modified or replaced. Every gap between the two contracts is absorbed on the relay side instead — payload budgets, response reassembly, an encrypted continuation ledger with explicit recovery states, host bindings, and the two optional native hooks (`bin/fusion-prompt-marker`, `bin/fusion-post-compaction`) that use only documented CLI extension points. See `NATIVE_HOST_CONTRACT.md` for what the native client does and does not expose, `QUALIFICATION_RESULTS.md` for the measured evidence, and `DURABLE_RECOVERY_RUNBOOK.md` for operations.

### Scope

This is the only scope available right now. The right scope now is to keep Fusion owning its harness and native SWE-2, minimize the lead adapter, and separately qualify computer use and enhanced recovery.

- **Fusion owns the harness and native SWE-2.** The relay never reimplements orchestration; the sidekick stays on its native route.
- **Minimal lead adapter.** The default path is the Astra → Codex translation only (legacy, in-memory continuity). This is what `bin/devin-fusion` runs.
- **Computer use: separately qualified.** Codex computer dispatch remains disabled fail-closed (`computer_policy_denied`) until it has its own trusted dispatcher, consent UI, and qualification.
- **Enhanced recovery: separately qualified.** Durable continuation, host bindings, epoch carry-over, and the experimental host are opt-in and labelled unqualified until the native lane-identity and acknowledgement contracts exist (`NATIVE_HOST_CONTRACT.md` §4).

## Notes

- If a relay from before this update is still running, it runs the old code. The launcher verifies the service's code fingerprint, so a stale pre-handshake relay cannot be silently reused — any restart should be reviewed first. See `validation.md` and `QUALIFICATION_RESULTS.md` for the current qualification status.
- Durable session continuation is not enabled automatically: it requires a host application to attach a trusted binding, and the native acknowledgment contract is not yet qualified. The default path remains in-memory and unqualified. `bin/fusion-experimental-host` provides an explicitly unqualified, launcher-provenance durable mode for evaluation.
