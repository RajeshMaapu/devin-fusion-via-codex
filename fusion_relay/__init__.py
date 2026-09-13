"""fusion-relay: route Devin Fusion's Astra inference to a local Codex login.

Splits GetChatMessage calls by routed-model id: Astra lead traffic is
translated to the ChatGPT Codex Responses API (ChatGPT subscription auth),
SWE-2 sidekick traffic is forwarded to Cognition natively. See README.md.
"""

__version__ = "0.1.0"
