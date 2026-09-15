"""Evidence-bound qualification receipt: recorded for one build identity,
verified by MAC, reported through diagnostics only while the tree matches."""
from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import diagnostics, launcher, qualification
from fusion_relay.identity import PrivateDirectory

SECRET = b"q" * 32


class QualificationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = pathlib.Path(self._tmp.name).resolve() / "data"
        self.dir.mkdir(mode=0o700)
        self.private = PrivateDirectory(self.dir)
        self.addCleanup(self.private.close)
        self.evidence = pathlib.Path(self._tmp.name) / "ev.log"
        self.evidence.write_text("Ran 811 tests OK\n")
        self.ref = qualification.evidence_ref(self.evidence)
        self.addCleanup(diagnostics.set_qualification, {})

    def test_absent_then_recorded_then_stale(self):
        self.assertEqual(qualification.status(self.private, SECRET),
                         {"level": "local_tests", "receipt": "absent",
                          "evidence_count": 0})
        receipt = qualification.record(
            self.private, SECRET, ["live_verified", "local_tests"],
            [self.ref], note="matrix rows 1-10")
        self.assertEqual(receipt["body"]["levels"],
                         ["local_tests", "live_verified"])
        st = qualification.status(self.private, SECRET)
        self.assertEqual(st, {"level": "live_verified",
                              "receipt": "verified", "evidence_count": 1})
        # wrong key -> invalid, never a level claim
        self.assertEqual(qualification.status(self.private, b"z" * 32),
                         {"level": "local_tests", "receipt": "invalid",
                          "evidence_count": 0})
        # a changed code tree -> stale_build -> local_tests
        with patch.object(qualification, "build_identity",
                          return_value="f" * 64):
            self.assertEqual(
                qualification.status(self.private, SECRET)["receipt"],
                "stale_build")
            self.assertEqual(
                qualification.status(self.private, SECRET)["level"],
                "local_tests")
        # re-record replaces the receipt with the higher level
        qualification.record(self.private, SECRET, ["benchmark_evaluated"],
                             [self.ref, "a" * 64])
        st = qualification.status(self.private, SECRET)
        self.assertEqual((st["level"], st["evidence_count"]),
                         ("benchmark_evaluated", 2))

    def test_record_guards(self):
        with self.assertRaises(ValueError):
            qualification.record(self.private, SECRET, ["bogus"], [self.ref])
        with self.assertRaises(ValueError):
            qualification.record(self.private, SECRET, ["live_verified"], [])
        with self.assertRaises(ValueError):
            qualification.record(self.private, SECRET, ["live_verified"],
                                 ["not-hex"])
        with self.assertRaises(ValueError):
            qualification.record(self.private, b"short", ["live_verified"],
                                 [self.ref])

    def test_tampered_receipt_invalid(self):
        qualification.record(self.private, SECRET, ["live_verified"],
                             [self.ref])
        path = self.dir / qualification.RECEIPT_NAME
        data = json.loads(path.read_text())
        data["body"]["levels"] = ["benchmark_evaluated"]
        path.write_text(json.dumps(data))
        self.assertEqual(qualification.status(self.private, SECRET),
                         {"level": "local_tests", "receipt": "invalid",
                          "evidence_count": 0})

    def test_diagnostics_reports_level(self):
        diagnostics.set_qualification(
            qualification.status(self.private, SECRET))
        self.assertEqual(diagnostics.qualification()["level"],
                         "local_tests")
        qualification.record(self.private, SECRET, ["live_verified"],
                             [self.ref])
        diagnostics.set_qualification(
            qualification.status(self.private, SECRET))
        self.assertEqual(diagnostics.qualification(),
                         {"level": "live_verified", "receipt": "verified",
                          "evidence_count": 1})
        diagnostics.record("gpt-6-astra-high", "a" * 64, "codex", {})
        snap = diagnostics.snapshot("a" * 64)
        self.assertEqual(snap["qualification"], "live_verified")
        self.assertEqual(snap["qualification_receipt"], "verified")
        diagnostics.set_qualification({"level": "nonsense"})
        self.assertEqual(diagnostics.qualification()["level"],
                         "local_tests")

    def test_launcher_qualify_command(self):
        (self.dir / "identity.key").write_bytes(SECRET)
        (self.dir / "identity.key").chmod(0o600)
        out = io.StringIO()
        with patch.object(launcher, "DATA_DIR", self.dir), \
                contextlib.redirect_stdout(out):
            code = launcher.main(["qualify", "--level", "live_verified",
                                  "--evidence", str(self.evidence),
                                  "--note", "test"])
        self.assertEqual(code, 0)
        printed = json.loads(out.getvalue())
        self.assertEqual(printed["levels"], ["live_verified"])
        self.assertEqual(qualification.status(self.private, SECRET)["level"],
                         "live_verified")
        err = io.StringIO()
        with patch.object(launcher, "DATA_DIR", self.dir), \
                contextlib.redirect_stderr(err):
            self.assertEqual(launcher.main(["qualify", "--level",
                                            "live_verified"]), 2)


if __name__ == "__main__":
    unittest.main()
