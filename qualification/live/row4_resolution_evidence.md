# Resolution evidence — experimental store (2026-09-15)
1. op 42f9c321... (scope c7ba338f...): status executing. requests.jsonl 03:08:44Z shows the request failed admission
   ('durable accounting unavailable') with no codex_http_status and codex_usage_calls=0 -> never dispatched. Abandon.
2. op 510980d0... (scope 372dca58...): status cancel_unconfirmed. requests.jsonl 03:15:04Z: client killed (SIGKILL) at ~9s,
   relay observed disconnect at 18s, codex_status cancelled, usage recorded from partial stream; provider-side cancellation
   is unconfirmed (no signal exists). Billing for the partial inference is possible. Abandon; result never delivered.
