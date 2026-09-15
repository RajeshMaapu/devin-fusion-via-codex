# Agent notes

## Test / dependency commands

- Full suite: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v`
- Targeted: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_<name>.py' -v`
- Install deps (hash-pinned, required): `python3 -m pip install --require-hashes -r requirements.txt`
- Smoke/tests use temp dirs and loopback fixtures only — no live auth,
  no real providers, no real Keychain.
- Gate interpreter is `/usr/bin/python3` (3.9.6). Run the final gate with
  `-X faulthandler -W error::ResourceWarning`; expect exactly 1 skip
  (`tests/test_keychain_live.py`, opt-in via `FUSION_RELAY_KEYCHAIN_LIVE=1`,
  mutates the login Keychain under a test-only service — needs approval).
- Tests must never call `ledger._db.execute(...)` directly while a server
  thread may be running: hold `ledger._lock` (use the `_ldb1/_ldba` test
  helpers) or open a separate `sqlite3.connect`. Sharing one connection
  across threads segfaults Python 3.9's sqlite3.
- Deliverable docs: `NATIVE_HOST_CONTRACT.md`, `DURABLE_RECOVERY_RUNBOOK.md`,
  `QUALIFICATION_RESULTS.md`; evidence under `qualification/`.

## Operational constraints

- A running production relay must not be restarted or stopped without
  explicit approval.
- Durable continuation (`FUSION_RELAY_CONTINUATION=durable`) requires a
  host-attached `ContinuationCoordinator`; without one it fails closed.
  The native ACK contract is unqualified (`native_ack_contract:
  unavailable`) — no production continuation enablement is claimed.
