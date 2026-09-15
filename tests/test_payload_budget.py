"""Payload budget policy: measurement, local rejection, upstream
classification, serialized-bytes authority, and durable preflight."""
from __future__ import annotations

import base64
import dataclasses
import io
import json
import pathlib
import sys
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from fusion_relay import payload_budget, translate, wire
from fusion_relay.continuation import (ContinuationBinding,
                                       ContinuationError,
                                       ContinuationLedger)
from fusion_relay.payload_budget import (BUDGET_POLICY_VERSION,
                                         BudgetExceeded, BudgetProfile,
                                         CODEX_PROFILE, PayloadReport,
                                         classify_upstream_error,
                                         coarse_incoming_check,
                                         measure_body, serialize_and_check)


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


FIXTURE_PNG = (pathlib.Path(__file__).resolve().parent
               / "fixtures" / "tool-image.png")
PNG = FIXTURE_PNG.read_bytes()
PNG_B64 = base64.b64encode(PNG).decode()

KEY = b'k' * 32
BINDING = dict(account='a', session='s', lane='lead',
               profile='astra:high', epoch='e1')


def img_part(url=None):
    return {'type': 'input_image',
            'image_url': url if url is not None
            else 'data:image/png;base64,' + PNG_B64}


def user_img(url=None):
    return {'role': 'user', 'content': [img_part(url)]}


def body_with(items):
    return {'model': 'gpt-6-astra', 'input': list(items)}


def report():
    return PayloadReport(route='codex', profile=CODEX_PROFILE.profile,
                         budget_policy_version=BUDGET_POLICY_VERSION)


class MeasureBodyTest(unittest.TestCase):
    def test_aggregate_image_count_overflow(self):
        # 40 historical image turns, then one current-turn image: the
        # aggregate 41st occurrence is rejected even though each single
        # message is small.
        items = [user_img() for _ in range(40)]
        items.append({'role': 'assistant', 'content': 'ok'})
        items.append(user_img())
        r = report()
        with self.assertRaises(BudgetExceeded) as ctx:
            measure_body(body_with(items), CODEX_PROFILE, r)
        e = ctx.exception
        self.assertEqual(e.kind, 'image_count')
        self.assertEqual(e.report.image_occurrences, 41)
        self.assertEqual(e.report.historical_image_count, 40)
        self.assertEqual(e.report.current_turn_image_count, 1)
        self.assertEqual(e.report.rejection_origin, 'local_image_count')
        self.assertIn('41 > 40', e.user_message())
        self.assertIn('compact', e.user_message())

    def test_at_limit_and_below_pass(self):
        for n in (40, 39):
            items = [user_img() for _ in range(n - 1)]
            items.append({'role': 'assistant', 'content': 'ok'})
            items.append(user_img())
            r = measure_body(body_with(items), CODEX_PROFILE, report())
            self.assertEqual(r.image_occurrences, n)

    def test_duplicate_occurrences_count_each_time(self):
        items = [user_img() for _ in range(41)]  # identical PNG 41x
        r = report()
        with self.assertRaises(BudgetExceeded) as ctx:
            measure_body(body_with(items), CODEX_PROFILE, r)
        self.assertEqual(ctx.exception.report.unique_image_count, 1)
        self.assertEqual(ctx.exception.report.image_occurrences, 41)

    def test_serialized_overflow_after_coarse_passes(self):
        tight = dataclasses.replace(CODEX_PROFILE, max_serialized_bytes=300)
        body = body_with([{'role': 'user', 'content': 'x' * 400}])
        serialized = json.dumps(body).encode()
        self.assertGreater(len(serialized), 300)
        # coarse check on the smaller inbound wire bytes passes; the
        # authoritative serialized check still rejects
        r = report()
        coarse_incoming_check(200, tight, r)  # 200*4//3 = 266 <= 300
        with self.assertRaises(BudgetExceeded) as ctx:
            serialize_and_check(body, tight, r)
        self.assertEqual(ctx.exception.kind, 'translated_bytes')
        self.assertEqual(r.final_serialized_bytes, len(serialized))

    def test_image_bytes_total_overflow(self):
        tight = dataclasses.replace(CODEX_PROFILE,
                                    max_image_bytes_total=len(PNG))
        r = report()
        with self.assertRaises(BudgetExceeded) as ctx:
            measure_body(body_with([user_img(), user_img()]), tight, r)
        self.assertEqual(ctx.exception.kind, 'image_bytes')

    def test_mixed_user_tool_text_and_privacy(self):
        items = [
            user_img(),
            {'role': 'assistant', 'content': 'looked'},
            {'type': 'function_call', 'call_id': 'c1', 'name': 't',
             'arguments': '{}'},
            {'type': 'function_call_output', 'call_id': 'c1',
             'output': [{'type': 'input_text', 'text': 'see'},
                        img_part(), img_part('https://x/img')]},
            {'role': 'user', 'content': 'thanks'},
        ]
        r = measure_body(body_with(items), CODEX_PROFILE, report())
        self.assertEqual(r.image_occurrences, 3)
        self.assertEqual(r.unique_image_count, 1)
        self.assertEqual(r.historical_image_count, 1)
        self.assertEqual(r.current_turn_image_count, 2)
        self.assertEqual(r.image_bytes_total, len(PNG) * 2)
        self.assertTrue(r.image_bytes_partial)  # non-data-URL part
        dumped = json.dumps(r.safe_dict())
        self.assertNotIn('data:', dumped)
        self.assertNotIn(PNG_B64[:32], dumped)
        self.assertNotIn('thanks', dumped)


class ClassifyUpstreamTest(unittest.TestCase):
    def test_image_count_keyword_inferred(self):
        body = json.dumps({"error": {
            "message": "Too many images in the conversation",
            "code": "invalid_request_error"}}).encode()
        r = classify_upstream_error(400, body)
        self.assertEqual(r.classification, 'image_count')
        self.assertEqual(r.certainty, 'inferred')
        self.assertEqual(r.code, 'invalid_request_error')

    def test_unrelated_400_is_unknown(self):
        body = json.dumps({"error": {"message": "something else"}}).encode()
        r = classify_upstream_error(400, body)
        self.assertEqual(r.classification, 'unknown_upstream_rejection')
        self.assertEqual(r.certainty, 'unknown')

    def test_413_empty_body(self):
        r = classify_upstream_error(413, b'')
        self.assertEqual(r.classification, 'payload_too_large')
        self.assertEqual(r.certainty, 'unknown')

    def test_malformed_json_unknown(self):
        r = classify_upstream_error(400, b'{not json')
        self.assertEqual(r.classification, 'unknown_upstream_rejection')

    def test_oversized_body_unknown_and_no_message_field(self):
        r = classify_upstream_error(400, b'x' * (
            payload_budget.MAX_ERROR_BODY_BYTES + 1))
        self.assertEqual(r.classification, 'unknown_upstream_rejection')
        self.assertNotIn('message',
                         {f.name for f in dataclasses.fields(r)})

    def test_context_and_format_keywords(self):
        for msg, cls in (
                ("maximum context length exceeded", 'context_limit'),
                ("image dimensions unsupported", 'image_dimensions_or_format'),
                ("request entity too large", 'payload_too_large')):
            r = classify_upstream_error(
                400, json.dumps({"error": {"message": msg}}).encode())
            self.assertEqual(r.classification, cls, msg)
            self.assertEqual(r.certainty, 'inferred', msg)

    def test_connect_code_mapping(self):
        for cls in ('image_count', 'payload_too_large', 'context_limit',
                    'image_dimensions_or_format'):
            self.assertEqual(payload_budget.connect_code_for(cls),
                             'resource_exhausted')
        self.assertEqual(payload_budget.connect_code_for(
            'unknown_upstream_rejection'), 'unavailable')


class SerializedBytesTest(unittest.TestCase):
    def setUp(self):
        translate.reset()
        self.addCleanup(translate.reset)

    def test_call_codex_sends_exact_serialized_bytes(self):
        from test_continuation_handler import FakeSSE, completed
        body = {'input': [{'role': 'user', 'content': 'hi'}]}
        serialized = serialize_and_check(body, CODEX_PROFILE, report())
        fake = FakeSSE([completed({"input_tokens": 1,
                                  "output_tokens": 1})])
        with patch.object(translate.auth, 'get_token',
                          return_value=('t', 'a')), \
                patch.object(translate, 'open_request',
                             return_value=fake) as send:
            translate.call_codex(body, {}, serialized=serialized)
        self.assertIs(send.call_args[0][0].data, serialized)

    def test_tool_loop_growth_remeasured(self):
        profile = dataclasses.replace(CODEX_PROFILE,
                                      max_serialized_bytes=0)
        body = {'prompt_cache_key': 'k', 'input': [
            {'role': 'user', 'content': 'go'}]}
        first_size = len(json.dumps(body).encode())
        profile = dataclasses.replace(
            profile, max_serialized_bytes=first_size)
        calls = []

        cua = {'type': 'function_call', 'call_id': 'c1',
               'name': translate.CUA_TOOL_NAME, 'arguments': '{}'}

        def fake(b, rec, on_delta=None, timeout=0, _items_out=None,
                 serialized=None, **kw):
            calls.append(json.loads(serialized or b'{}'))
            if _items_out is not None:
                _items_out.extend([cua])
            return wire.frame(wire.field(5, 10)) + wire.end_stream()

        rec: dict = {}
        with patch.object(translate, 'call_codex', fake):
            with self.assertRaises(BudgetExceeded) as ctx:
                translate.call_codex_with_tools(
                    body, rec, executor=None, budget_profile=profile)
        self.assertEqual(ctx.exception.kind, 'translated_bytes')
        # iteration 1 sent exactly one provider call; iteration 2's
        # grown body was rejected before dispatch
        self.assertEqual(len(calls), 1)
        grown = len(json.dumps(body).encode())
        self.assertGreater(grown, first_size)
        self.assertEqual(rec['payload']['final_serialized_bytes'], grown)


class LedgerPreflightTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.led = ContinuationLedger(
            pathlib.Path(self.tmp.name).resolve() / 'cont.sqlite3', KEY)
        self.addCleanup(self.led.close)
        self.b = ContinuationBinding(**BINDING)

    def _body(self, text='one'):
        return {'input': [{'role': 'user', 'content': text}],
                'model': 'astra', 'reasoning': {'effort': 'high'}}

    def test_abandon_marks_preflight_rejected_and_retry_rereserves(self):
        r = self.led.reserve(self.b, 'q1', self._body())
        self.led.abandon(r, 'preflight_rejected')
        row = _ldb1(self.led, 
            "SELECT status, result FROM operations "
            "WHERE operation_id='q1'")
        self.assertEqual(row, ('preflight_rejected', None))
        # identical retry re-reserves; a different op id is not blocked
        # by the abandoned row ('already reserved' only sees 'executing')
        r2 = self.led.reserve(self.b, 'q1', self._body())
        self.assertIsNone(r2['replay'])
        self.led.abandon(r2, 'preflight_rejected')
        r3 = self.led.reserve(self.b, 'q2', self._body('two'))
        self.assertIsNone(r3['replay'])

    def test_abandon_rejects_bad_reason_and_mismatch(self):
        r = self.led.reserve(self.b, 'q1', self._body())
        with self.assertRaises(ContinuationError):
            self.led.abandon(r, 'cancelled')
        bad = dict(r, native_body=self._body('tampered'))
        with self.assertRaises(ContinuationError):
            self.led.abandon(bad, 'preflight_rejected')
        row = _ldb1(self.led, 
            "SELECT status FROM operations WHERE operation_id='q1'"
        )
        self.assertEqual(row[0], 'executing')

    def test_preflight_rejected_conflicting_fingerprint_rejected(self):
        r = self.led.reserve(self.b, 'q1', self._body())
        self.led.abandon(r, 'preflight_rejected')
        with self.assertRaises(ContinuationError):
            self.led.reserve(self.b, 'q1', self._body('changed'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
