# Devin Fusion via Codex

Bring your Codex subscription to Devin Fusion.

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
