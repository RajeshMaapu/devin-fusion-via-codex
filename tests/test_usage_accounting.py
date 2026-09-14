import unittest

from fusion_relay import usage


class UsageAccountingTest(unittest.TestCase):
    def test_multiple_responses_and_duplicates(self):
        rec = {}
        first = {"id": "one", "usage": {"input_tokens": 10, "output_tokens": 2,
                 "input_tokens_details": {"cached_tokens": 4},
                 "output_tokens_details": {"reasoning_tokens": 1}}}
        second = {"id": "two", "usage": {"input_tokens": 20, "output_tokens": 3,
                  "input_tokens_details": {"cached_tokens": 6},
                  "output_tokens_details": {"reasoning_tokens": 2}}}
        for response in (first, second, first, second):
            usage.record_usage(response, rec, role="lead")
        total = rec["codex_usage"]
        self.assertEqual((total["input_tokens"], total["output_tokens"]), (30, 5))
        self.assertEqual(total["input_tokens_details"]["cached_tokens"], 10)
        self.assertEqual(total["output_tokens_details"]["reasoning_tokens"], 3)
        self.assertEqual(total["response_count"], 2)
        self.assertFalse(total["partial"])
        counts = {}
        usage.add_to_totals(counts, total)
        self.assertEqual((counts["input"], counts["output"], counts["cached"], counts["reasoning"]), (30, 5, 10, 3))

    def test_unknown_then_known_is_partial(self):
        rec = {}
        usage.record_usage({"id": "unknown", "status": "cancelled"}, rec)
        usage.record_usage({"id": "known", "usage": {"input_tokens": 20, "output_tokens": 3}}, rec)
        total = rec["codex_usage"]
        self.assertEqual((total["input_tokens"], total["output_tokens"]), (20, 3))
        self.assertEqual(total["unknown_calls"], 1)
        self.assertEqual(total["missing_fields"]["input_tokens"], 1)
        self.assertIsNone(total["input_tokens_details"]["cached_tokens"])
        self.assertTrue(total["partial"])

    def test_missing_does_not_encode_zero(self):
        rec = {}
        usage.record_usage({"id": "unknown"}, rec)
        self.assertIsNone(rec["codex_usage"]["input_tokens"])
        counts = {}
        usage.add_to_totals(counts, rec["codex_usage"])
        self.assertNotIn("input", counts)
        self.assertEqual(counts["unknown_calls"], 1)

    def test_terminal_statuses_and_roles_are_separate(self):
        rec = {}
        for role in ("lead", "sidekick"):
            for status in ("completed", "incomplete", "failed", "cancelled"):
                usage.record_usage({"id": status, "status": status,
                                   "usage": {"input_tokens": 1, "output_tokens": 2}}, rec, role=role)
        self.assertEqual(rec["codex_usage"]["response_count"], 8)
        self.assertEqual(rec["codex_usage_by_role"]["lead"]["input_tokens"], 4)
        self.assertEqual(rec["codex_usage_by_role"]["sidekick"]["output_tokens"], 8)
        self.assertEqual({entry["status"] for entry in rec["codex_usage_calls"]},
                         {"completed", "incomplete", "failed", "cancelled"})

    def test_unknown_event_upgrades_without_duplicate_count(self):
        rec = {}
        usage.record_usage({"id": "one"}, rec)
        usage.record_usage({"id": "one", "usage": {"input_tokens": 7, "output_tokens": 0}}, rec)
        self.assertEqual(rec["codex_usage"]["response_count"], 1)
        self.assertEqual(rec["codex_usage"]["unknown_calls"], 0)
        self.assertEqual(rec["codex_usage"]["output_tokens"], 0)

    def test_bad_counts_and_provider_metadata_are_not_trusted(self):
        rec = {}
        usage.record_usage({"id": "PRIVATE_MARKER", "model": "PRIVATE_MARKER",
                           "status": "PRIVATE_MARKER", "usage": {
                               "input_tokens": -1, "output_tokens": True,
                               "attribution": "PRIVATE_MARKER"}}, rec)
        self.assertIsNone(rec["codex_usage"]["input_tokens"])
        self.assertIsNone(rec["codex_usage"]["output_tokens"])
        self.assertNotIn("PRIVATE_MARKER", str(rec))

    def test_conflicting_duplicate_does_not_double_count(self):
        rec = {}
        usage.record_usage({"id": "one", "usage": {"input_tokens": 7}}, rec)
        usage.record_usage({"id": "one", "usage": {"input_tokens": 9}}, rec)
        self.assertEqual(rec["codex_usage"]["input_tokens"], 7)
        self.assertTrue(rec["codex_usage_calls"][0]["conflict"])
