# Accounting reconciliation evidence (experimental host store, 2026-09-15)

Store: ~/.local/share/fusion-codex-relay-live-c/accounting.sqlite3 (isolated test store, NOT production).
Observation: accounting_runs rows with closed=0 (runs c328e963..., fbd3da8a..., and the run ended by this stop),
pending operations = 0, reconciliations = 0. Cause established: ExperimentalHost.stop() posted /shutdown but did not
join the relay thread, so relay.serve()'s finally (accounting close(clean=True)) never ran before process exit.
No billable operation is unaccounted: every admitted operation in this store reached a terminal accounting event
(pending count 0). Resolution: acknowledge unknown with this file's sha256 as the evidence reference.
