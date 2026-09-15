"""Regression tests for the bounded refusal/terminal-content and runtime
handle-cleanup repairs. Synthetic only: no real model, credential, or
desktop calls."""
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fusion_relay import translate, wire, runtime, relay


class Response(io.BytesIO):
    status = 200
    headers = {}


def events_for(final_text, delta=None, refusal=True):
    kind = 'refusal' if refusal else 'output_text'
    part = {'type': kind, 'refusal' if refusal else 'text': final_text}
    item = {'id': 'message-1', 'type': 'message', 'role': 'assistant',
            'status': 'completed', 'content': [part]}
    events = []
    if delta is not None:
        events.append({'type': 'response.'+kind+'.delta', 'delta': delta,
                       'item_id': 'message-1', 'output_index': 0, 'content_index': 0})
    events.extend([{'type': 'response.output_item.done', 'output_index': 0, 'item': item},
                   {'type': 'response.completed', 'response': {
                       'id': 'response-1', 'status': 'completed', 'output': [item],
                       'usage': {'input_tokens': 7, 'output_tokens': 3}}}])
    return events


def terminal_events(output):
    return [{'type': 'response.completed', 'response': {
        'id': 'response-1', 'status': 'completed', 'output': output,
        'usage': {'input_tokens': 7, 'output_tokens': 3}}}]


class ResponseTests(unittest.TestCase):
    def run_response(self, events, streamed):
        body = b''.join(b'data: '+json.dumps(e).encode()+b'\n\n' for e in events)
        frames, record = [], {}
        def emit(chunk):
            frames.append(chunk)
            return True
        with patch.object(translate, 'open_request', return_value=Response(body)):
            tail = translate.call_codex({'prompt_cache_key': 'smoke'}, record,
                on_delta=emit if streamed else None, credentials=('dummy', 'dummy'))
        frames.append(tail)
        visible = ''.join(wire.text(wire.decode(p), 3)
                         for f, p in wire.iter_frames(b''.join(frames)) if not f & 2)
        return visible, record

    def test_refusal_is_visible_in_buffer_and_delta_modes(self):
        for streamed in (False, True):
            with self.subTest(streamed=streamed):
                text, rec = self.run_response(events_for('Cannot proceed.', 'Cannot proceed.'), streamed)
                self.assertEqual(text, 'Cannot proceed.')
                self.assertTrue(rec['response_refused'])

    def test_terminal_only_refusal_is_not_lost(self):
        for mode in (False, True):
            text, _ = self.run_response(events_for('Cannot proceed.'), mode)
            self.assertEqual(text, 'Cannot proceed.')

    def test_partial_refusal_completed_once(self):
        text, _ = self.run_response(events_for('Cannot proceed.', 'Cannot '), True)
        self.assertEqual(text, 'Cannot proceed.')

    def test_ordinary_text_unchanged(self):
        for mode in (False, True):
            text, _ = self.run_response(events_for('hello', 'hello', refusal=False), mode)
            self.assertEqual(text, 'hello')

    def test_conflicting_terminal_is_not_success(self):
        with self.assertRaises(translate.IncompleteResponse):
            self.run_response(events_for('different', 'already sent', refusal=False), True)


class MalformedContentTests(unittest.TestCase):
    def run_response(self, events):
        body = b''.join(b'data: '+json.dumps(e).encode()+b'\n\n' for e in events)
        with patch.object(translate, 'open_request', return_value=Response(body)):
            return translate.call_codex({'prompt_cache_key': 'smoke'}, {},
                                        credentials=('dummy', 'dummy'))

    def test_non_dict_output_item_rejected(self):
        with self.assertRaises(translate.IncompleteResponse):
            self.run_response(terminal_events([None]))

    def test_non_list_message_content_rejected(self):
        item = {'type': 'message', 'content': None}
        with self.assertRaises(translate.IncompleteResponse):
            self.run_response(terminal_events([item]))

    def test_non_dict_content_part_rejected(self):
        item = {'type': 'message', 'content': [None]}
        with self.assertRaises(translate.IncompleteResponse):
            self.run_response(terminal_events([item]))

    def test_unknown_content_part_type_rejected(self):
        item = {'type': 'message',
                'content': [{'type': 'output_image', 'image': 'x'}]}
        with self.assertRaises(translate.IncompleteResponse):
            self.run_response(terminal_events([item]))


class ToolCallPlusRefusalTests(unittest.TestCase):
    def test_tool_call_and_refusal_finish_tool_calls(self):
        call = {'type': 'function_call', 'call_id': 'call-1',
                'name': 'get_weather', 'arguments': '{"city":"x"}'}
        message = {'type': 'message', 'role': 'assistant',
                   'status': 'completed',
                   'content': [{'type': 'refusal', 'refusal': 'Cannot.'}]}
        events = terminal_events([call, message])
        body = b''.join(b'data: '+json.dumps(e).encode()+b'\n\n'
                        for e in events)
        rec = {}
        with patch.object(translate, 'open_request',
                          return_value=Response(body)):
            tail = translate.call_codex({'prompt_cache_key': 'smoke'}, rec,
                                        credentials=('dummy', 'dummy'))
        msgs = [wire.decode(p) for f, p in wire.iter_frames(tail)
                if not f & 2]
        msg = next(m for m in msgs if 5 in m)
        self.assertEqual(msg[5][0], translate.FINISH_TOOL_CALLS)
        nested = wire.decode(msg[6][0])
        self.assertEqual(wire.text(nested, 1), 'call-1')
        self.assertEqual(wire.text(nested, 2), 'get_weather')
        self.assertEqual(wire.text(nested, 3), '{"city":"x"}')
        self.assertTrue(rec['response_refused'])


class SafeRecordTests(unittest.TestCase):
    def test_response_refused_bool_retained(self):
        for value in (True, False):
            out = relay.safe_record({'route': 'codex',
                                     'response_refused': value})
            self.assertEqual(out['response_refused'], value)

    def test_response_refused_non_bool_omitted(self):
        out = relay.safe_record({'route': 'codex',
                                 'response_refused': 'yes'})
        self.assertNotIn('response_refused', out)

    def test_refusal_prose_never_logged(self):
        out = relay.safe_record({'route': 'codex',
                                 'response_refused': True,
                                 'refusal_text': 'Cannot proceed.',
                                 'refusal': 'Cannot proceed.'})
        self.assertNotIn('refusal_text', out)
        self.assertNotIn('refusal', out)
        self.assertNotIn('Cannot proceed.', repr(out))


class CleanupTests(unittest.TestCase):
    def adapter(self, fail=False):
        a = runtime.CuaRuntimeAdapter.__new__(runtime.CuaRuntimeAdapter)
        a._dead, a._sel = False, Mock()
        proc = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(), poll=lambda: 0)
        a._proc = proc
        a._kill_group = Mock(side_effect=RuntimeError('termination unknown') if fail else None)
        return a, proc

    def test_abort_closes_handles(self):
        a, p = self.adapter()
        a._abort()
        self.assertTrue(a._sel is None)
        self.assertTrue(p.stdin.closed and p.stdout.closed)
        self.assertIs(a._proc, p)
        self.assertTrue(a._dead)

    def test_uncertain_termination_closes_handles_but_stays_error(self):
        a, p = self.adapter(True)
        with self.assertRaises(RuntimeError):
            a.close()
        self.assertIsNone(a._sel)
        self.assertTrue(p.stdin.closed and p.stdout.closed)
        self.assertIs(a._proc, p)
        self.assertTrue(a._dead)

    def test_successful_close_releases_proc(self):
        a, p = self.adapter()
        a.close()
        self.assertIsNone(a._sel)
        self.assertTrue(p.stdin.closed and p.stdout.closed)
        self.assertIsNone(a._proc)
        self.assertTrue(a._dead)

    def test_abort_with_uncertain_termination_closes_handles(self):
        a, p = self.adapter(True)
        with self.assertRaises(RuntimeError):
            a._abort()
        self.assertIsNone(a._sel)
        self.assertTrue(p.stdin.closed and p.stdout.closed)
        self.assertIs(a._proc, p)
        self.assertTrue(a._dead)


if __name__ == '__main__':
    unittest.main(verbosity=2)
