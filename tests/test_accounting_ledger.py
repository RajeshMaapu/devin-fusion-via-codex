"""Accounting ledger durability tests: temp dirs, dummy refs, no live data."""
from __future__ import annotations

import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from fusion_relay.accounting import (
    AccountingLedger, AccountingUnavailable, MAX_COUNT, reference)


def _ldb1(led, sql, params=()):
    """Locked single-row access — /usr/bin/python3 3.9 sqlite3 crashes
    on concurrent statements on one connection; hold the ledger lock."""
    with led._lock:
        return led._db.execute(sql, params).fetchone()


def _ldba(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params).fetchall()


def _ldbw(led, sql, params=()):
    with led._lock:
        return led._db.execute(sql, params)


REPO = pathlib.Path(__file__).resolve().parent.parent


def ev(event_id, provider="codex", account=None, status="completed",
       counts=(7, 3, 2, 1)):
    return {"event_id": event_id, "provider": provider,
            "account_ref": account or reference("acct-a"),
            "status": status,
            "input_tokens": counts[0], "output_tokens": counts[1],
            "cached_tokens": counts[2], "reasoning_tokens": counts[3]}


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = pathlib.Path(self._tmp.name).resolve()
        self.db = self.dir / "ledger.db"
        self.led = AccountingLedger(self.db)
        self.addCleanup(self._close)

    def _close(self):
        try:
            self.led.close()
        except Exception:
            pass

    def admit(self, op="op-a", provider="codex", account=None):
        self.led.admit(reference(op), provider, account or reference("acct-a"))

    def complete(self, events, op="op-a"):
        self.led.complete(reference(op), events)

    def test_known_counts_and_missing_none_not_zero(self):
        acct = reference("acct-a")
        self.admit()
        self.complete([ev(reference("e-codex"), "codex", acct,
                          counts=(7, 3, 2, 1))])
        self.led.admit(reference("op-n"), "native", acct)
        self.led.complete(reference("op-n"),
                          [ev(reference("e-native"), "native", acct,
                              counts=(5, 4, None, None))])
        snap = self.led.snapshot()
        codex = snap["providers"]["codex"]
        native = snap["providers"]["native"]
        self.assertEqual(codex["responses"], 1)
        self.assertEqual(codex["known"], {"input_tokens": 7,
                                         "output_tokens": 3,
                                         "cached_tokens": 2,
                                         "reasoning_tokens": 1})
        self.assertEqual(native["responses"], 1)
        self.assertEqual(native["known"]["input_tokens"], 5)
        self.assertEqual(native["known"]["output_tokens"], 4)
        # unknown counts stay None, never silently zero
        self.assertIsNone(native["known"]["cached_tokens"])
        self.assertIsNone(native["known"]["reasoning_tokens"])
        self.assertEqual(native["missing"]["cached_tokens"], 1)
        self.assertEqual(native["missing"]["reasoning_tokens"], 1)
        self.assertTrue(snap["partial"])

    def test_identical_completion_event_idempotent(self):
        e = ev(reference("e1"))
        self.admit()
        self.complete([e])
        self.complete([e])          # same digest + same operation
        snap = self.led.snapshot()
        self.assertEqual(snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(snap["providers"]["codex"]["known"]
                         ["input_tokens"], 7)

    def test_conflicting_duplicate_rejected_counts_original(self):
        acct = reference("acct-a")
        self.admit()
        self.complete([ev(reference("e1"), "codex", acct,
                          counts=(7, 3, 2, 1))])
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e1"), "codex", acct,
                              counts=(8, 3, 2, 1))])
        snap = self.led.snapshot()
        self.assertEqual(snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(snap["providers"]["codex"]["known"]
                         ["input_tokens"], 7)

    def test_event_identity_mismatch_rejects_atomically(self):
        self.admit("op-a", "codex", reference("acct-a"))
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e1"), "native",
                              reference("acct-a"))])
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e2"), "codex",
                              reference("acct-b"))])
        snap = self.led.snapshot()
        self.assertEqual(snap["providers"]["codex"]["responses"], 0)
        self.assertEqual(snap["providers"]["native"]["responses"], 0)
        self.assertEqual(snap["providers"]["codex"]["pending"], 1)

    def test_readmission_rejected(self):
        # a completed/admitted operation id is never reusable — no
        # inference retry may silently double-count under a new event
        self.admit()
        self.complete([ev(reference("e1"))])
        with self.assertRaises(AccountingUnavailable):
            self.admit()
        with self.assertRaises(AccountingUnavailable):
            self.admit("op-a", "native", reference("acct-b"))

    def test_malformed_counts_rejected_before_write(self):
        self.admit()
        for bad in (True, -1, MAX_COUNT + 1, "7", 7.5):
            with self.assertRaises(ValueError):
                self.complete([ev(reference("e-%s" % bad),
                                  counts=(bad, 3, 2, 1))])
        self.assertEqual(
            self.led.snapshot()["providers"]["codex"]["responses"], 0)

    def test_unapproved_fields_rejected_no_secret_persisted(self):
        self.admit()
        e = ev(reference("e1"))
        e["prompt"] = "TOP-SECRET-PROMPT-BYTES"
        with self.assertRaises(ValueError):
            self.complete([e])
        e = ev(reference("e1"))
        e["tool_arguments"] = "TOP-SECRET-PROMPT-BYTES"
        with self.assertRaises(ValueError):
            self.complete([e])
        self.assertNotIn(b"TOP-SECRET-PROMPT-BYTES",
                         self.db.read_bytes())
        self.assertEqual(
            self.led.snapshot()["providers"]["codex"]["responses"], 0)

    def test_second_conflicting_event_rolls_back_whole_batch(self):
        acct = reference("acct-a")
        self.admit("op-first", "codex", acct)
        self.complete([ev(reference("e-shared"), "codex", acct,
                          counts=(7, 3, 2, 1))], op="op-first")
        self.admit("op-b", "codex", acct)
        fresh = ev(reference("e-new"), "codex", acct)
        conflict = ev(reference("e-shared"), "codex", acct,
                      counts=(9, 3, 2, 1))
        with self.assertRaises(AccountingUnavailable):
            self.complete([fresh, conflict], op="op-b")
        snap = self.led.snapshot()
        # the valid sibling event must not be committed either
        self.assertEqual(snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(snap["providers"]["codex"]["pending"], 1)

    def test_injected_commit_failure_degrades_and_blocks(self):
        self.admit()
        real = self.led._db

        class FailCommit:
            in_transaction = True

            def execute(self, sql, *a):
                if sql.startswith("COMMIT"):
                    raise sqlite3.OperationalError("readonly database")
                return real.execute(sql, *a)

        self.led._db = FailCommit()
        with self.assertRaises(AccountingUnavailable):
            self.admit("op-b")
        self.led._db = real
        self.assertTrue(self.led.degraded)
        self.assertEqual(self.led.gaps, 1)
        with self.assertRaises(AccountingUnavailable):
            self.admit("op-c")
        self.assertEqual(
            self.led.snapshot()["providers"]["codex"]["pending"], 1)

    def test_external_write_lock_blocks_admit_gap_counted(self):
        other = sqlite3.connect(str(self.db), timeout=1)
        try:
            other.execute("BEGIN IMMEDIATE")
            other.execute(
                "INSERT INTO accounting_runs(run_id,started) VALUES('x',0)")
            with self.assertRaises(AccountingUnavailable):
                self.admit()
        finally:
            other.execute("ROLLBACK")
            other.close()
        self.assertTrue(self.led.degraded)
        self.assertEqual(self.led.gaps, 1)

    def test_unclean_close_requires_reconciliation_on_reopen(self):
        self.admit()
        self.complete([ev(reference("e1"))])
        self.led.close(clean=False)
        led2 = AccountingLedger(self.db)
        try:
            snap = led2.snapshot()
            self.assertTrue(snap["reconciliation_required"])
            self.assertGreaterEqual(snap["unclean_runs"], 1)
            self.assertTrue(snap["degraded"])
            self.assertEqual(
                snap["providers"]["codex"]["responses"], 1)
        finally:
            led2.close()

    def test_pending_operation_survives_clean_close(self):
        self.admit()
        self.led.close()
        led2 = AccountingLedger(self.db)
        try:
            snap = led2.snapshot()
            self.assertEqual(
                snap["providers"]["codex"]["pending"], 1)
            self.assertTrue(snap["reconciliation_required"])
        finally:
            led2.close()

    def test_failed_completion_leaves_operation_pending_on_reopen(self):
        self.admit()
        real = self.led._db

        class FailEventWrite:
            in_transaction = True

            def execute(self, sql, *a):
                if sql.startswith("INSERT INTO accounting_events"):
                    raise sqlite3.OperationalError("disk full")
                return real.execute(sql, *a)

        self.led._db = FailEventWrite()
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e1"))])
        self.led._db = real
        self.led.close()
        led2 = AccountingLedger(self.db)
        try:
            snap = led2.snapshot()
            self.assertEqual(
                snap["providers"]["codex"]["pending"], 1)
            self.assertEqual(
                snap["providers"]["codex"]["responses"], 0)
        finally:
            led2.close()

    def test_exported_receipts_not_reexported_after_reopen(self):
        self.admit()
        self.complete([ev(reference("e1"))])
        rows = self.led.unexported()
        self.assertEqual(len(rows), 1)
        self.led.mark_exported(rows[0][0])
        self.led.close()
        led2 = AccountingLedger(self.db)
        try:
            self.assertEqual(led2.unexported(), [])
            snap = led2.snapshot()
            self.assertEqual(
                snap["providers"]["codex"]["responses"], 1)
            self.assertEqual(snap["pending_exports"], 0)
        finally:
            led2.close()

    def test_corrupt_db_fails_without_overwriting(self):
        self._close()               # release the valid ledger first
        self.db.write_bytes(b"\x00garbage-not-sqlite\x00" * 16)
        before = self.db.read_bytes()
        with self.assertRaises(AccountingUnavailable):
            AccountingLedger(self.db)
        self.assertEqual(self.db.read_bytes(), before)

    def test_corrupt_stored_payload_degrades_snapshot(self):
        self.admit()
        self.complete([ev(reference("e1"))])
        _ldbw(self.led, 
            "UPDATE accounting_events SET payload='not-json' "
            "WHERE event_id=?", (reference("e1"),))
        snap = self.led.snapshot()
        self.assertTrue(snap["degraded"])
        self.assertEqual(snap["coverage"], "unavailable")
        self.assertTrue(snap["reconciliation_required"])

    def test_digest_mismatch_degrades_snapshot(self):
        self.admit()
        self.complete([ev(reference("e1"))])
        # well-formed JSON but the stored digest no longer matches
        _ldbw(self.led, 
            'UPDATE accounting_events SET payload=\'{"a":1}\' '
            "WHERE event_id=?", (reference("e1"),))
        snap = self.led.snapshot()
        self.assertTrue(snap["degraded"])
        self.assertEqual(snap["coverage"], "unavailable")

    def test_valid_json_list_matching_digest_reports_no_counts(self):
        import hashlib
        self.admit()
        self.complete([ev(reference("e1"))])
        payload = "[]"
        _ldbw(self.led, 
            "UPDATE accounting_events SET payload=?,digest=? "
            "WHERE event_id=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest(),
             reference("e1")))
        snap = self.led.snapshot()
        self.assertTrue(snap["degraded"])
        self.assertEqual(snap["coverage"], "unavailable")

    def test_changed_event_set_on_completed_op_rejected(self):
        acct = reference("acct-a")
        self.admit()
        self.complete([ev(reference("e1"), "codex", acct)])
        # same operation, different event set — never merges
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e2"), "codex", acct)])
        with self.assertRaises(AccountingUnavailable):
            self.complete([ev(reference("e1"), "codex", acct),
                           ev(reference("e2"), "codex", acct)])
        snap = self.led.snapshot()
        self.assertEqual(
            snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(
            snap["providers"]["codex"]["known"]["input_tokens"], 7)

    def test_admit_rejected_after_dirty_reopen(self):
        self.led.close(clean=False)
        led2 = AccountingLedger(self.db)
        try:
            self.assertTrue(led2.degraded)
            with self.assertRaises(AccountingUnavailable):
                led2.admit(reference("op-x"), "codex",
                           reference("acct-a"))
            self.assertTrue(led2.snapshot()["reconciliation_required"])
        finally:
            led2.close()

    def test_cross_process_recovery(self):
        acct = reference("acct-a")
        self.admit()
        self.complete([ev(reference("e1"), "codex", acct)])
        self.led.admit(reference("op-n"), "native", acct)
        self.led.complete(reference("op-n"),
                          [ev(reference("e2"), "native", acct,
                              counts=(5, 4, None, None))])
        self.led.close()
        script = (
            "import json,pathlib,sys\n"
            "sys.path.insert(0, %r)\n"
            "from fusion_relay.accounting import AccountingLedger\n"
            "led = AccountingLedger(pathlib.Path(sys.argv[1]))\n"
            "print(json.dumps(led.snapshot()))\n"
            "led.close()\n" % str(REPO))
        out = subprocess.run(
            [sys.executable, "-c", script, str(self.db)],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        snap = json.loads(out.stdout.strip().splitlines()[-1])
        self.assertEqual(
            snap["providers"]["codex"]["known"]["input_tokens"], 7)
        self.assertEqual(
            snap["providers"]["native"]["known"]["input_tokens"], 5)
        self.assertEqual(snap["providers"]["native"]["missing"]
                         ["cached_tokens"], 1)


class ReconcileTest(LedgerTest):
    def _dirty_pending(self, op="op-p"):
        """Admit a never-completed op, close dirty, reopen degraded."""
        self.led.admit(reference(op), "codex", reference("acct-a"))
        self.led.close(clean=False)
        self.led = AccountingLedger(self.db)
        self.assertTrue(self.led.degraded)
        return self.led

    def test_acknowledge_unknown_pending_creates_unknown_receipt(self):
        led = self._dirty_pending()
        led.acknowledge_unknown("e" * 64, confirmed=True)
        snap = led.snapshot()
        codex = snap["providers"]["codex"]
        self.assertEqual(codex["responses"], 1)
        self.assertIsNone(codex["known"]["input_tokens"])
        self.assertEqual(codex["missing"]["input_tokens"], 1)
        self.assertEqual(codex["missing"]["output_tokens"], 1)
        self.assertEqual(codex["pending"], 0)
        self.assertFalse(snap["degraded"])
        self.assertTrue(snap["partial"])
        self.assertFalse(snap["reconciliation_required"])
        # admission unblocked after explicit acknowledgment
        led.admit(reference("op-new"), "codex", reference("acct-a"))

    def test_acknowledge_preserves_known_totals(self):
        acct = reference("acct-a")
        self.admit()
        self.complete([ev(reference("e1"), "codex", acct)])
        led = self._dirty_pending()
        led.acknowledge_unknown("e" * 64, confirmed=True)
        codex = led.snapshot()["providers"]["codex"]
        self.assertEqual(codex["responses"], 2)
        self.assertEqual(codex["known"]["input_tokens"], 7)
        self.assertEqual(codex["missing"]["input_tokens"], 1)

    def test_acknowledge_audit_ref_and_reopen_retains(self):
        led = self._dirty_pending()
        led.acknowledge_unknown("f" * 64, confirmed=True)
        row = _ldb1(led, 
            "SELECT evidence_ref,unknown_operations "
            "FROM accounting_reconciliations")
        self.assertEqual(row, ("f" * 64, 1))
        led.close(clean=True)
        self.led = AccountingLedger(self.db)
        snap = self.led.snapshot()
        self.assertFalse(snap["degraded"])
        self.assertEqual(
            snap["providers"]["codex"]["responses"], 1)
        self.assertEqual(
            snap["providers"]["codex"]["missing"]["input_tokens"], 1)
        self.led.admit(reference("op-x"), "codex", reference("acct-a"))

    def test_acknowledge_rejects_bad_ref_and_unconfirmed(self):
        led = self._dirty_pending()
        for args, kw in ((("short",), {"confirmed": True}),
                         (("e" * 64,), {"confirmed": False}),
                         (("e" * 64,), {})):
            with self.assertRaises(ValueError):
                led.acknowledge_unknown(*args, **kw)
        snap = led.snapshot()
        self.assertEqual(snap["providers"]["codex"]["pending"], 1)
        self.assertTrue(snap["degraded"])
        self.assertEqual(_ldbw(led, 
            "SELECT COUNT(*) FROM accounting_reconciliations")
            .fetchone()[0], 0)

    def test_acknowledge_failure_rolls_back(self):
        led = self._dirty_pending()
        # fail inside the transaction: the reconciliation id is minted
        # by uuid4, after the unknown receipts are written
        with mock.patch('uuid.uuid4',
                        side_effect=RuntimeError("injected")):
            with self.assertRaises(AccountingUnavailable):
                led.acknowledge_unknown("e" * 64, confirmed=True)
        snap = led.snapshot()
        self.assertTrue(snap["degraded"])
        self.assertEqual(snap["providers"]["codex"]["pending"], 1)
        self.assertEqual(_ldbw(led, 
            "SELECT COUNT(*) FROM accounting_reconciliations")
            .fetchone()[0], 0)
        self.assertEqual(_ldb1(led, 
            "SELECT state FROM accounting_operations WHERE operation_id=?",
            (reference("op-p"),))[0], "pending")

    def test_acknowledge_never_mutates_completed(self):
        self.admit()
        self.complete([ev(reference("e1"), "codex",
                          reference("acct-a"))])
        self.led.acknowledge_unknown("e" * 64, confirmed=True)
        codex = self.led.snapshot()["providers"]["codex"]
        self.assertEqual(codex["responses"], 1)
        self.assertEqual(codex["known"]["input_tokens"], 7)
        self.assertEqual(_ldb1(self.led, 
            "SELECT state FROM accounting_operations WHERE operation_id=?",
            (reference("op-a"),))[0], "complete")

    def test_acknowledge_corrupt_store_rejected(self):
        self.admit()
        self.complete([ev(reference("e1"), "codex",
                          reference("acct-a"))])
        _ldbw(self.led, 
            "UPDATE accounting_events SET digest=? WHERE event_id=?",
            ("0" * 64, reference("e1")))
        with self.assertRaises(AccountingUnavailable):
            self.led.acknowledge_unknown("e" * 64, confirmed=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
