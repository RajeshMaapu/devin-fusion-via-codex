"""Durable route-state and assignment-outcome tests.

All persistence uses temporary directories; all responses are fakes.
"""

import json
import pathlib
import tempfile
import threading
import unittest
from unittest.mock import patch

from fusion_relay import catalog
from fusion_relay.catalog import RouteStateError
from fusion_relay.relay import ForwardResponse
from fusion_relay.wire import end_stream, field, frame


def _resp(status=200, ctype="application/proto", body=b"", headers=None):
    return ForwardResponse(status, body, ctype, headers or {})


def _frame_stream(data: bytes, trailer: bytes) -> bytes:
    return frame(data) + frame(trailer, 0x02)


class RouteStateTest(unittest.TestCase):
    def setUp(self):
        catalog.reset()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.addCleanup(catalog.reset)
        self.path = pathlib.Path(self._tmpdir.name) / "routes.json"
        catalog.attach_store(self.path)

    def test_begin_finish_commits_route(self):
        rev = catalog.begin_selection("s1", "native")
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})
        catalog.finish_selection("s1", rev, True)
        self.assertEqual(catalog.session_route({16: [b"s1"]}), "native")

    def test_finish_false_preserves_old_route(self):
        catalog.pin_route("s1", "native")
        rev = catalog.begin_selection("s1", "codex")
        catalog.finish_selection("s1", rev, False)
        self.assertEqual(catalog.session_route({16: [b"s1"]}), "native")

    def test_unknown_outcome_stays_pending_and_blocks(self):
        rev = catalog.begin_selection("s1", "native")
        catalog.finish_selection("s1", rev, None)
        self.assertEqual(catalog.selection_status("s1")["pending"]
                         ["status"], "selection_unconfirmed")
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})

    def test_second_pending_begin_rejected(self):
        catalog.begin_selection("s1", "native")
        self.assertRaises(RouteStateError, catalog.begin_selection,
                          "s1", "codex")
        self.assertRaises(RouteStateError, catalog.begin_selection,
                          "s1", "native")

    def test_concurrent_begin_single_winner(self):
        wins, errs = [], []

        def go(route):
            try:
                wins.append(catalog.begin_selection("s1", route))
            except RouteStateError:
                errs.append(route)

        threads = [threading.Thread(target=go, args=(r,))
                   for r in ("native", "codex")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(wins), 1)
        self.assertEqual(len(errs), 1)

    def test_pending_durable_across_restart(self):
        catalog.begin_selection("s1", "native")
        catalog.reset()
        catalog.attach_store(self.path)
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})
        self.assertRaises(RouteStateError, catalog.begin_selection,
                          "s1", "codex")

    def test_revisions_persist(self):
        r1 = catalog.begin_selection("s1", "native")
        catalog.finish_selection("s1", r1, True)
        catalog.reset()
        catalog.attach_store(self.path)
        r2 = catalog.begin_selection("s1", "codex")
        self.assertEqual(r2, r1 + 1)

    def test_invalid_inputs_rejected(self):
        for bad in (("", "native"), ("s1", "bogus"), ("s1", "")):
            self.assertRaises(RouteStateError, catalog.begin_selection, *bad)

    def test_failed_persistence_no_success(self):
        with patch.object(catalog, "atomic_write",
                          side_effect=OSError("disk gone")):
            self.assertRaises(RouteStateError, catalog.begin_selection,
                              "s1", "native")
        self.assertTrue(catalog._store_error)
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})

    def test_pin_route_failed_save_not_published(self):
        with patch.object(catalog, "atomic_write",
                          side_effect=OSError("disk gone")):
            self.assertRaises(RouteStateError, catalog.pin_route,
                              "s1", "native")
        self.assertTrue(catalog._store_error)
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})


class RouteStoreLoadTest(unittest.TestCase):
    def setUp(self):
        catalog.reset()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.addCleanup(catalog.reset)
        self.path = pathlib.Path(self._tmpdir.name) / "routes.json"

    def test_corrupt_state_never_defaults(self):
        self.path.write_text("{not json")
        self.assertRaises(RouteStateError, catalog.attach_store, self.path)
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})
        self.assertRaises(RouteStateError, catalog.begin_selection,
                          "s1", "native")

    def test_legacy_flat_state_loads(self):
        self.path.write_text(json.dumps({"s1": "native"}))
        catalog.attach_store(self.path)
        self.assertEqual(catalog.session_route({16: [b"s1"]}), "native")

    def test_v2_pending_blocks_on_load(self):
        self.path.write_text(json.dumps({
            "version": 2, "routes": {}, "revisions": {"s1": 1},
            "pending": {"s1": {"revision": 1, "route": "native",
                               "status": "pending"}}}))
        catalog.attach_store(self.path)
        self.assertRaises(RouteStateError, catalog.session_route, {16: [b"s1"]})
        # Resolution via finish_selection uses the persisted revision.
        catalog.finish_selection("s1", 1, False)
        self.assertIsNone(catalog.session_route({16: [b"s1"]}))

    def test_pending_revision_must_match_revision_map(self):
        self.path.write_text(json.dumps({
            "version": 2, "routes": {}, "revisions": {"s1": 7},
            "pending": {"s1": {"revision": 1, "route": "native",
                               "status": "pending"}}}))
        self.assertRaises(RouteStateError, catalog.attach_store, self.path)

    def test_missing_store_is_empty_not_error(self):
        catalog.attach_store(self.path.parent / "absent.json")
        self.assertIsNone(catalog.session_route({16: [b"s1"]}))


class AssignmentOutcomeTest(unittest.TestCase):
    def test_non_2xx_is_failure(self):
        self.assertIs(catalog.assignment_outcome(
            _resp(status=500, body=b"whatever")), False)

    def test_unary_proto_success(self):
        self.assertIs(catalog.assignment_outcome(
            _resp(body=field(1, "x"))), True)

    def test_unary_empty_body_success(self):
        self.assertIs(catalog.assignment_outcome(_resp(body=b"")), True)

    def test_unary_malformed_body_unknown(self):
        self.assertIs(catalog.assignment_outcome(
            _resp(body=b"\xff\xff\xff\xff")), None)

    def test_connect_trailer_empty_success(self):
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=_frame_stream(field(1, "x"), b"{}"))), True)

    def test_connect_trailer_metadata_success(self):
        trailer = json.dumps({"metadata": {"k": ["v"]}}).encode()
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=_frame_stream(field(1, "x"), trailer))), True)

    def test_connect_trailer_error_failure(self):
        trailer = json.dumps({"error": {"code": "internal"}}).encode()
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=_frame_stream(field(1, "x"), trailer))), False)

    def test_connect_missing_trailer_unknown(self):
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=frame(field(1, "x")))), None)

    def test_connect_malformed_frames_unknown(self):
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=b"\x00\x00\x00\x00\x09abc")), None)

    def test_connect_json_data_frame_unknown(self):
        # A JSON payload in a data frame is not a valid protobuf success.
        err = json.dumps({"code": "internal", "message": "boom"}).encode()
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=frame(err) + end_stream())), None)

    def test_begin_selection_requires_store(self):
        catalog.reset()
        self.addCleanup(catalog.reset)
        self.assertRaises(RouteStateError, catalog.begin_selection,
                          "s1", "native")

    def test_trailer_unknown_keys_unknown(self):
        trailer = json.dumps({"unexpected": {}}).encode()
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=_frame_stream(field(1, "x"), trailer))), None)

    def test_trailer_malformed_error_unknown(self):
        trailer = json.dumps({"error": "just a string"}).encode()
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/connect+proto",
            body=_frame_stream(field(1, "x"), trailer))), None)

    def test_grpc_status_nonzero_failure(self):
        self.assertIs(catalog.assignment_outcome(_resp(
            headers={"grpc-status": "7"})), False)

    def test_unknown_content_type_unknown(self):
        self.assertIs(catalog.assignment_outcome(
            _resp(ctype="text/html", body=b"<html>")), None)

    def test_json_error_failure(self):
        self.assertIs(catalog.assignment_outcome(_resp(
            ctype="application/json",
            body=json.dumps({"code": "x"}).encode())), False)


if __name__ == "__main__":
    unittest.main()
