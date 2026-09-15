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

## Notes

- If a relay from before this update is still running, it runs the old code. The launcher verifies the service's code fingerprint, so a stale pre-handshake relay cannot be silently reused — any restart should be reviewed first. See `validation.md` for the current qualification status.
- Durable session continuation is not enabled automatically: it requires a host application to attach a trusted binding, and the native acknowledgment contract is not yet qualified. The default path remains in-memory and unqualified.
