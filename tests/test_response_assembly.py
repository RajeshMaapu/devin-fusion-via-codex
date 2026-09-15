"""Indexed response-assembly tests: synthetic events, loopback TCP only."""
from __future__ import annotations

import io
import json
import socket
import threading
import time
import unittest
from unittest.mock import patch

from fusion_relay import translate, wire
from fusion_relay.response_assembly import ResponseAssembly


class Response(io.BytesIO):
    status = 200
    headers = {}


def msg_item(mid, parts):
    return {'id': mid, 'type': 'message', 'role': 'assistant',
            'status': 'completed', 'content': parts}


def text_part(s):
    return {'type': 'output_text', 'text': s}


def refusal_part(s):
    return {'type': 'refusal', 'refusal': s}


def iev(etype, oi=None, ci=None, item_id=None, **kw):
    e = {'type': etype}
    if oi is not None:
        e['output_index'] = oi
    if ci is not None:
        e['content_index'] = ci
    if item_id is not None:
        e['item_id'] = item_id
    e.update(kw)
    return e


def done_item(oi, item, item_id=None):
    return iev('response.output_item.done', oi=oi, item_id=item_id,
               item=item)


def completed(output, rid='response-1', usage=None):
    return {'type': 'response.completed', 'response': {
        'id': rid, 'status': 'completed', 'output': output,
        'usage': usage or {'input_tokens': 7, 'output_tokens': 3}}}


def run(events, streamed=True):
    body = b''.join(b'data: ' + json.dumps(e).encode() + b'\n\n'
                    for e in events)
    frames, record, items = [], {}, []
    def emit(chunk):
        frames.append(chunk)
        return True
    with patch.object(translate, 'open_request',
                      return_value=Response(body)):
        tail = translate.call_codex({'prompt_cache_key': 'smoke'}, record,
            on_delta=emit if streamed else None,
            credentials=('dummy', 'dummy'), _items_out=items)
    frames.append(tail)
    visible = ''.join(wire.text(wire.decode(p), 3)
                      for f, p in wire.iter_frames(b''.join(frames))
                      if not f & 2)
    return visible, record, items, tail


def feed(events):
    """Accept the valid prefix; return the assembly for the bad event."""
    a = ResponseAssembly(translate.IncompleteResponse)
    for e in events:
        a.accept(e)
    return a


class InterleavedTest(unittest.TestCase):
    def test_two_messages_interleaved_emit_in_order(self):
        m1 = msg_item('m1', [text_part('first')])
        m2 = msg_item('m2', [text_part('second')])
        events = [
            iev('response.output_text.delta', oi=1, ci=0, item_id='m2',
                delta='sec'),
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='fir'),
            iev('response.output_text.delta', oi=1, ci=0, item_id='m2',
                delta='ond'),
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='st'),
            done_item(0, m1), done_item(1, m2),
            completed([m1, m2]),
        ]
        visible, _, _, _ = run(events)
        self.assertEqual(visible, 'firstsecond')

    def test_content_index_out_of_order(self):
        m2 = msg_item('m1', [text_part('a'), text_part('b')])
        events = [
            iev('response.output_text.delta', oi=0, ci=1, item_id='m1',
                delta='b'),
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='a'),
            done_item(0, m2),
            completed([m2]),
        ]
        visible, _, _, _ = run(events)
        self.assertEqual(visible, 'ab')

    def test_per_part_suffix_independent(self):
        m = msg_item('m1', [text_part('AB'), text_part('CD')])
        events = [
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='A'),
            iev('response.output_text.done', oi=0, ci=0, item_id='m1',
                text='AB'),
            iev('response.output_text.delta', oi=0, ci=1, item_id='m1',
                delta='C'),
            iev('response.output_text.done', oi=0, ci=1, item_id='m1',
                text='CD'),
            done_item(0, m),
            completed([m]),
        ]
        visible, _, _, _ = run(events)
        self.assertEqual(visible, 'ABCD')

    def test_refusal_after_partial_text_separate_part(self):
        m = msg_item('m1', [text_part('ok '), refusal_part('no')])
        events = [
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='ok'),
            iev('response.refusal.delta', oi=0, ci=1, item_id='m1',
                delta='n'),
            iev('response.output_text.done', oi=0, ci=0, item_id='m1',
                text='ok '),
            iev('response.refusal.done', oi=0, ci=1, item_id='m1',
                refusal='no'),
            done_item(0, m),
            completed([m]),
        ]
        visible, rec, _, _ = run(events)
        self.assertEqual(visible, 'ok no')
        self.assertTrue(rec['response_refused'])


class ToolAndReasoningTest(unittest.TestCase):
    def test_function_call_plus_refusal(self):
        call = {'type': 'function_call', 'id': 'f1', 'call_id': 'call-1',
                'name': 'get_weather', 'arguments': '{"city":"x"}'}
        m = msg_item('m1', [refusal_part('Cannot.')])
        events = [
            iev('response.output_item.added', oi=0, item_id='f1',
                item={'type': 'function_call', 'id': 'f1',
                      'call_id': 'call-1', 'name': 'get_weather',
                      'arguments': ''}),
            iev('response.function_call_arguments.delta', oi=0,
                delta='{"city"'),
            iev('response.function_call_arguments.delta', oi=0,
                delta=':"x"}'),
            done_item(0, call),
            iev('response.refusal.delta', oi=1, ci=0, item_id='m1',
                delta='Cannot.'),
            done_item(1, m),
            completed([call, m]),
        ]
        visible, rec, items, tail = run(events)
        self.assertEqual(visible, 'Cannot.')
        self.assertTrue(rec['response_refused'])
        msgs = [wire.decode(p) for f, p in wire.iter_frames(tail)
                if not f & 2]
        term = next(x for x in msgs if 5 in x)
        self.assertEqual(term[5][0], translate.FINISH_TOOL_CALLS)
        nested = wire.decode(term[6][0])
        self.assertEqual(wire.text(nested, 1), 'call-1')
        self.assertEqual(wire.text(nested, 2), 'get_weather')
        self.assertEqual(wire.text(nested, 3), '{"city":"x"}')
        self.assertEqual(items[0]['call_id'], 'call-1')

    def test_reasoning_skipped_but_preserved_opaque(self):
        reasoning = {'type': 'reasoning', 'id': 'r1',
                     'encrypted_content': 'opaque-blob'}
        m = msg_item('m1', [text_part('answer')])
        events = [
            iev('response.output_item.added', oi=0, item_id='r1',
                item=reasoning),
            iev('response.reasoning_summary_text.delta', oi=0,
                item_id='r1', delta='never emitted'),
            iev('response.reasoning_text.delta', oi=0, item_id='r1',
                delta='hidden'),
            done_item(0, reasoning),
            iev('response.output_text.delta', oi=1, ci=0, item_id='m1',
                delta='answer'),
            done_item(1, m),
            completed([reasoning, m]),
        ]
        visible, _, items, _ = run(events)
        self.assertEqual(visible, 'answer')
        self.assertEqual(items[0], reasoning)
        self.assertNotIn('never emitted', visible)
        self.assertNotIn('hidden', visible)


class ContradictionTest(unittest.TestCase):
    """Each case feeds a valid prefix then proves the bad event raises."""

    def test_conflicting_response_ids(self):
        a = feed([{'type': 'response.created', 'response': {'id': 'a'}}])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept({'type': 'response.created', 'response': {'id': 'b'}})

    def test_response_id_field_mismatch(self):
        a = feed([{'type': 'response.created', 'response': {'id': 'a'}}])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.delta', oi=0, ci=0,
                         item_id='m1', delta='x', response_id='b'))

    def test_item_id_bound_to_two_indexes(self):
        a = feed([iev('response.output_text.delta', oi=0, ci=0,
                      item_id='m1', delta='x')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.delta', oi=1, ci=0,
                         item_id='m1', delta='y'))

    def test_part_kind_conflict(self):
        a = feed([iev('response.output_text.delta', oi=0, ci=0,
                      item_id='m1', delta='x')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.refusal.delta', oi=0, ci=0,
                         item_id='m1', delta='y'))

    def test_item_type_conflict(self):
        a = feed([iev('response.output_item.added', oi=0,
                      item={'type': 'message', 'id': 'm1',
                            'content': []})])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_item.added', oi=0,
                         item={'type': 'function_call', 'id': 'f1',
                               'call_id': 'c', 'name': 'n',
                               'arguments': ''}))

    def test_done_text_contradicts_delta(self):
        a = feed([iev('response.output_text.delta', oi=0, ci=0,
                      item_id='m1', delta='abc')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.done', oi=0, ci=0,
                         item_id='m1', text='xyz'))

    def test_delta_after_done_rejected(self):
        a = feed([iev('response.output_text.done', oi=0, ci=0,
                      item_id='m1', text='done')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.delta', oi=0, ci=0,
                         item_id='m1', delta='more'))

    def test_done_cannot_change_after_done(self):
        a = feed([iev('response.output_text.done', oi=0, ci=0,
                      item_id='m1', text='final')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.done', oi=0, ci=0,
                         item_id='m1', text='final longer'))

    def test_duplicate_part_done_identical_ok(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        a.accept(iev('response.output_text.done', oi=0, ci=0,
                     item_id='m1', text='same'))
        self.assertEqual(a.accept(
            iev('response.output_text.done', oi=0, ci=0, item_id='m1',
                text='same')), '')
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.done', oi=0, ci=0,
                         item_id='m1', text='same plus'))

    def test_content_part_done_requires_text(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.content_part.done', oi=0, ci=0,
                         item_id='m1',
                         part={'type': 'output_text'}))

    def test_nonempty_content_part_added_rejected(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.content_part.added', oi=0, ci=0,
                         item_id='m1',
                         part={'type': 'output_text', 'text': 'pre'}))

    def test_nonempty_item_added_content_rejected(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_item.added', oi=0,
                         item={'type': 'message', 'id': 'm1',
                               'content': [text_part('pre')]}))

    def test_content_after_item_done_rejected(self):
        m = msg_item('m1', [text_part('x')])
        a = feed([done_item(0, m)])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.delta', oi=0, ci=1,
                         item_id='m1', delta='new'))

    def test_repeated_item_done_must_match(self):
        m = msg_item('m1', [text_part('x')])
        a = feed([done_item(0, m)])
        self.assertEqual(a.accept(done_item(0, m)), '')
        changed = msg_item('m1', [text_part('x'), text_part('y')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(done_item(0, changed))

    def test_fargs_contradiction(self):
        a = feed([iev('response.function_call_arguments.delta', oi=0,
                      delta='{"a"')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.function_call_arguments.done', oi=0,
                         arguments='{"b"}'))

    def test_fargs_wrong_item_id(self):
        a = feed([iev('response.output_item.added', oi=0, item_id='f1',
                      item={'type': 'function_call', 'id': 'f1',
                            'call_id': 'c', 'name': 'n',
                            'arguments': ''})])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.function_call_arguments.delta', oi=0,
                         item_id='other', delta='{}'))

    def test_fargs_delta_after_done(self):
        a = feed([iev('response.function_call_arguments.done', oi=0,
                      arguments='{}')])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.function_call_arguments.delta', oi=0,
                         delta='x'))
        self.assertEqual(a.accept(
            iev('response.function_call_arguments.done', oi=0,
                arguments='{}')), '')
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.function_call_arguments.done', oi=0,
                         arguments='{}extra'))

    def test_changed_call_identity_rejected(self):
        added = {'type': 'function_call', 'id': 'f1', 'call_id': 'c1',
                 'name': 'n1', 'arguments': ''}
        a = feed([iev('response.output_item.added', oi=0, item_id='f1',
                      item=added)])
        for key, val in (('call_id', 'c2'), ('name', 'n2')):
            bad = dict(added, **{key: val, 'arguments': '{}'})
            with self.assertRaises(translate.IncompleteResponse):
                a.accept(done_item(0, bad))

    def test_empty_call_id_name_rejected(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(done_item(0, {'type': 'function_call', 'id': 'f1',
                                   'call_id': '', 'name': 'n',
                                   'arguments': '{}'}))
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(done_item(0, {'type': 'function_call', 'id': 'f1',
                                   'call_id': 'c', 'name': '',
                                   'arguments': '{}'}))

    def test_reasoning_stream_requires_identity(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        with self.assertRaises(translate.IncompleteResponse):
            a.accept({'type': 'response.reasoning_text.delta',
                      'output_index': 0, 'delta': 'x'})
        with self.assertRaises(translate.IncompleteResponse):
            a.accept({'type': 'response.reasoning_text.delta',
                      'item_id': 'r1', 'delta': 'x'})

    def test_reasoning_item_id_conflict(self):
        reasoning = {'type': 'reasoning', 'id': 'r1',
                     'encrypted_content': 'e'}
        m = msg_item('m1', [text_part('x')])
        a = feed([
            iev('response.reasoning_text.delta', oi=0, item_id='r1',
                delta='hidden'),
        ])
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_item.added', oi=0,
                         item_id='r1', item=m))

    def test_streamed_reasoning_omitted_by_terminal(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.reasoning_text.delta', oi=0, item_id='r1',
                    delta='hidden'),
                completed([msg_item('m1', [text_part('x')])]),
            ])

    def test_mixed_mode_both_directions(self):
        a = ResponseAssembly(translate.IncompleteResponse)
        a.accept({'type': 'response.output_text.delta', 'delta': 'a'})
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(iev('response.output_text.delta', oi=0, ci=0,
                         item_id='m1', delta='b'))
        b = ResponseAssembly(translate.IncompleteResponse)
        b.accept(iev('response.output_text.delta', oi=0, ci=0,
                     item_id='m1', delta='b'))
        with self.assertRaises(translate.IncompleteResponse):
            b.accept({'type': 'response.output_text.delta',
                      'delta': 'a'})


class TerminalTest(unittest.TestCase):
    def test_duplicate_identical_completion_usage_once(self):
        m = msg_item('m1', [text_part('hi')])
        events = [
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='hi'),
            done_item(0, m),
            completed([m]), completed([m]),
        ]
        body = b''.join(b'data: ' + json.dumps(e).encode() + b'\n\n'
                        for e in events)
        with patch.object(translate, 'open_request',
                          return_value=Response(body)), \
                patch.object(translate, '_record_usage',
                             wraps=translate._record_usage) as rec_usage:
            tail = translate.call_codex({'prompt_cache_key': 'k'}, {},
                                        credentials=('d', 'd'),
                                        _items_out=[])
        rec_usage.assert_called_once()
        finishes = [p for f, p in wire.iter_frames(tail)
                    if not f & 2 and 5 in wire.decode(p)]
        self.assertEqual(len(finishes), 1)

    def test_duplicate_contradictory_completion(self):
        m = msg_item('m1', [text_part('hi')])
        m2 = msg_item('m1', [text_part('bye')])
        with self.assertRaises(translate.IncompleteResponse):
            run([done_item(0, m), completed([m]), completed([m2])])

    def test_event_after_terminal_rejected(self):
        m = msg_item('m1', [text_part('hi')])
        with self.assertRaises(translate.IncompleteResponse):
            run([completed([m]),
                 iev('response.output_text.delta', oi=0, ci=0,
                     item_id='m1', delta='x')])

    def test_missing_terminal(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([iev('response.output_text.delta', oi=0, ci=0,
                     item_id='m1', delta='x')])

    def test_terminal_missing_id(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([{'type': 'response.completed',
                  'response': {'status': 'completed', 'output': []}}])
        with self.assertRaises(translate.IncompleteResponse):
            run([completed([], rid='')])

    def test_terminal_null_output(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([{'type': 'response.completed',
                  'response': {'id': 'r', 'status': 'completed',
                               'output': None}}])

    def test_streamed_part_omitted_by_terminal(self):
        m = msg_item('m1', [text_part('only')])
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.output_text.delta', oi=0, ci=1,
                    item_id='m1', delta='extra'),
                completed([m]),
            ])

    def test_streamed_item_omitted_by_terminal(self):
        m = msg_item('m1', [text_part('only')])
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.output_text.delta', oi=1, ci=0,
                    item_id='m2', delta='later'),
                completed([m]),
            ])

    def test_terminal_only_item_text_emitted(self):
        m0 = msg_item('m1', [text_part('a')])
        m1 = msg_item('m2', [text_part('b')])
        visible, _, items, _ = run([
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta='a'),
            completed([m0, m1]),
        ])
        self.assertEqual(visible, 'ab')
        self.assertEqual(len(items), 2)

    def test_terminal_type_conflict_masked_by_same_id(self):
        # reasoning stream bound r1@oi0; terminal claims a message there
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.reasoning_text.delta', oi=0, item_id='r1',
                    delta='hidden'),
                completed([msg_item('r1', [text_part('x')])]),
            ])
        # fargs stream bound f1@oi0; terminal claims a message there
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.function_call_arguments.delta', oi=0,
                    item_id='f1', delta='{}'),
                completed([msg_item('f1', [text_part('x')])]),
            ])

    def test_terminal_message_bad_content_shapes(self):
        for bad in (None, {'text': 'x'}, [text_part('x')] * 2049):
            item = {'id': 'm1', 'type': 'message', 'content': bad}
            with self.assertRaises(translate.IncompleteResponse):
                run([
                    iev('response.output_text.delta', oi=0, ci=0,
                        item_id='m1', delta='x'),
                    completed([item]),
                ])

    def test_terminal_status_not_completed(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([{'type': 'response.completed', 'response': {
                'id': 'r', 'status': 'in_progress', 'output': []}}])

    def test_legacy_duplicate_item_ids(self):
        item = {'id': 'm1', 'type': 'message',
                'content': [text_part('x')]}
        with self.assertRaises(translate.IncompleteResponse):
            run([
                {'type': 'response.output_item.done', 'item': item},
                {'type': 'response.output_item.done', 'item': dict(item)},
                {'type': 'response.completed', 'response': {
                    'id': 'r', 'status': 'completed'}},
            ])

    def test_legacy_added_validation(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([
                {'type': 'response.output_item.added',
                 'item': {'type': 'mystery'}},
                completed([]),
            ])
        with self.assertRaises(translate.IncompleteResponse):
            run([
                {'type': 'response.content_part.added',
                 'part': {'type': 'output_text', 'text': 'pre'}},
                completed([]),
            ])
        with self.assertRaises(translate.IncompleteResponse):
            run([
                {'type': 'response.content_part.done',
                 'part': text_part('x')},
                completed([]),
            ])

    def test_terminal_item_missing_known_id(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.output_text.delta', oi=0, ci=0,
                    item_id='m1', delta='x'),
                completed([{'type': 'message',
                            'content': [text_part('x')]}]),
            ])

    def test_duplicate_terminal_item_id(self):
        m = msg_item('m1', [text_part('x')])
        m2 = msg_item('m2', [text_part('y')])
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.output_text.delta', oi=0, ci=0,
                    item_id='m1', delta='x'),
                completed([m, dict(m2, id='m1')]),
            ])

    def test_empty_terminal_output_conflicts_emitted(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([
                iev('response.output_text.delta', oi=0, ci=0,
                    item_id='m1', delta='x'),
                completed([]),
            ])

    def test_unknown_terminal_item_type(self):
        with self.assertRaises(translate.IncompleteResponse):
            run([completed([{'type': 'mystery', 'id': 'x'}])])

    def test_terminal_item_changes_completed(self):
        m = msg_item('m1', [text_part('x')])
        terminal = msg_item('m1', [text_part('xy')])
        with self.assertRaises(translate.IncompleteResponse):
            run([done_item(0, m), completed([terminal])])


CALL_ITEM = {'id': 'fc1', 'type': 'function_call', 'call_id': 'c1',
             'name': 'read',
             'arguments': '{"file_path":"./imgs/img_00.png"}'}


def call_stream(oi=0, item_id='fc1'):
    """Indexed function_call stream: added, args deltas, args done,
    item done — the shape the live backend sends."""
    args = CALL_ITEM['arguments']
    return [
        iev('response.output_item.added', oi=oi, item_id=item_id,
            item={'id': item_id, 'type': 'function_call',
                  'call_id': 'c1', 'name': 'read', 'arguments': ''}),
        iev('response.function_call_arguments.delta', oi=oi,
            item_id=item_id, delta=args[:20]),
        iev('response.function_call_arguments.delta', oi=oi,
            item_id=item_id, delta=args[20:]),
        iev('response.function_call_arguments.done', oi=oi,
            item_id=item_id, arguments=args),
        done_item(oi, CALL_ITEM, item_id=item_id),
    ]


class EmptyTerminalOutputTest(unittest.TestCase):
    """Codex may complete with output [] / absent after fully streaming
    every item; the streamed items reconstruct the terminal output."""

    def test_empty_output_recovers_done_items(self):
        a = feed(call_stream())
        a.accept(completed([]))
        complete, output = a.finish()
        self.assertEqual(output, [CALL_ITEM])
        self.assertEqual(complete['output'], [CALL_ITEM])
        rec: dict = {}
        decoded = wire.decode(
            translate._final_message(complete, output, rec))
        self.assertEqual(decoded[5][0], translate.FINISH_TOOL_CALLS)
        call = wire.decode(decoded[6][0])
        self.assertEqual(wire.text(call, 2), 'read')
        self.assertEqual(wire.text(call, 1), 'c1')
        self.assertEqual(rec['tool_call_names'], ['read'])

    def test_absent_output_recovers_done_items(self):
        a = feed(call_stream())
        a.accept({'type': 'response.completed', 'response': {
            'id': 'r', 'status': 'completed',
            'usage': {'input_tokens': 1}}})
        complete, output = a.finish()
        self.assertEqual(output, [CALL_ITEM])
        self.assertEqual(complete['output'], [CALL_ITEM])

    def test_undone_item_with_empty_output_fails(self):
        # added + deltas but no output_item.done — partially streamed
        a = feed(call_stream()[:-1])
        with self.assertRaises(translate.IncompleteResponse) as cm:
            a.accept(completed([]))
        self.assertIn('omits streamed content', str(cm.exception))

    def test_shorter_terminal_output_still_fails(self):
        m0 = msg_item('m1', [text_part('x')])
        events = call_stream(oi=0, item_id='fc1') + [
            done_item(1, m0, item_id='m1')]
        a = feed(events)
        with self.assertRaises(translate.IncompleteResponse) as cm:
            a.accept(completed([CALL_ITEM]))
        self.assertIn('omitted by terminal output', str(cm.exception))

    def test_noncontiguous_indices_with_empty_output_fail(self):
        m2 = msg_item('m2', [text_part('y')])
        events = call_stream(oi=0, item_id='fc1') + [
            done_item(2, m2, item_id='m2')]
        a = feed(events)
        with self.assertRaises(translate.IncompleteResponse) as cm:
            a.accept(completed([]))
        self.assertIn('omits streamed content', str(cm.exception))


class MalformedTest(unittest.TestCase):
    def assert_bad(self, event, preceding=()):
        a = feed(list(preceding))
        with self.assertRaises(translate.IncompleteResponse):
            a.accept(event)

    def test_nonstring_delta(self):
        self.assert_bad(iev('response.output_text.delta', oi=0, ci=0,
                            item_id='m1', delta=123))

    def test_negative_index(self):
        self.assert_bad(iev('response.output_text.delta', oi=-1, ci=0,
                            item_id='m1', delta='x'))

    def test_bool_index(self):
        self.assert_bad(iev('response.output_text.delta', oi=True, ci=0,
                            item_id='m1', delta='x'))

    def test_huge_index(self):
        self.assert_bad(iev('response.output_text.delta', oi=2048, ci=0,
                            item_id='m1', delta='x'))

    def test_output_not_list(self):
        self.assert_bad(completed('notalist'))

    def test_unknown_content_type(self):
        self.assert_bad(completed([msg_item('m1',
                                            [{'type': 'output_image'}])]))

    def test_unknown_indexed_event(self):
        self.assert_bad(iev('response.future_thing.delta', oi=0, ci=0,
                            item_id='m1', delta='x'))

    def test_partial_indices_rejected(self):
        self.assert_bad({'type': 'response.output_text.delta',
                         'output_index': 0, 'delta': 'x'})

    def test_unterminated_json_event(self):
        body = b'data: {"type": "response.compl'
        with patch.object(translate, 'open_request',
                          return_value=Response(body)):
            with self.assertRaises(translate.IncompleteResponse):
                translate.call_codex({'prompt_cache_key': 'k'}, {},
                                     credentials=('d', 'd'))

    def test_complete_json_without_newline_rejected(self):
        event = completed([msg_item('m1', [text_part('x')])])
        body = b'data: ' + json.dumps(event).encode()  # no trailing \n
        with patch.object(translate, 'open_request',
                          return_value=Response(body)):
            with self.assertRaises(translate.IncompleteResponse):
                translate.call_codex({'prompt_cache_key': 'k'}, {},
                                     credentials=('d', 'd'))

    def test_non_dict_event(self):
        body = b'data: [1,2]\n\n'
        with patch.object(translate, 'open_request',
                          return_value=Response(body)):
            with self.assertRaises(translate.IncompleteResponse):
                translate.call_codex({'prompt_cache_key': 'k'}, {},
                                     credentials=('d', 'd'))


class ClientGoneTest(unittest.TestCase):
    def test_downstream_false_closes_response_no_finish(self):
        holder = {}

        def fake_open(*a, **k):
            holder['r'] = Response(b''.join(
                b'data: ' + json.dumps(e).encode() + b'\n\n'
                for e in [iev('response.output_text.delta', oi=0, ci=0,
                              item_id='m1', delta='x')] * 3))
            return holder['r']

        with patch.object(translate, 'open_request', fake_open):
            with self.assertRaises(translate.ClientGone):
                translate.call_codex({}, {}, on_delta=lambda f: False,
                                     credentials=('d', 'd'))
        self.assertTrue(holder['r'].closed)


class SplitUTF8Test(unittest.TestCase):
    """Real loopback TCP: SSE bytes split mid-multibyte still decode."""

    def test_split_utf8_delta(self):
        text = 'héllo—wörld'
        m = msg_item('m1', [text_part(text)])
        events = [
            iev('response.output_text.delta', oi=0, ci=0, item_id='m1',
                delta=text),
            done_item(0, m),
            completed([m]),
        ]
        body = b''.join(b'data: ' + json.dumps(e, ensure_ascii=False)
                        .encode('utf-8') + b'\n\n' for e in events)
        idx = body.find('é'.encode('utf-8')) + 1
        self.assertGreater(idx, 0)
        self.assertTrue(0x80 <= body[idx] < 0xC0)  # split mid-sequence
        chunks = [body[:idx], body[idx:]]

        server = socket.socket()
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        port = server.getsockname()[1]
        done = threading.Event()

        def serve():
            conn, _ = server.accept()
            try:
                conn.settimeout(5)
                data = b''
                while b'\r\n\r\n' not in data:
                    data += conn.recv(4096)
                head, _, rest = data.partition(b'\r\n\r\n')
                length = 0
                for line in head.split(b'\r\n'):
                    if line.lower().startswith(b'content-length:'):
                        length = int(line.split(b':', 1)[1])
                while len(rest) < length:
                    rest += conn.recv(4096)
                conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: '
                             b'text/event-stream\r\n\r\n')
                for c in chunks:
                    conn.sendall(c)
                    time.sleep(0.02)
                conn.shutdown(socket.SHUT_WR)
                while conn.recv(4096):
                    pass
            finally:
                conn.close()
                server.close()
                done.set()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        rec = {}
        with patch.object(translate, 'CODEX_RESPONSES_URL',
                          'http://127.0.0.1:%d/' % port):
            out = translate.call_codex(
                {'prompt_cache_key': 'tcp'}, rec,
                credentials=('dummy', 'dummy'), timeout=10,
                _items_out=[])
        self.assertTrue(done.wait(timeout=5))
        thread.join(timeout=5)
        frames = wire.iter_frames(out)
        visible = ''.join(wire.text(wire.decode(p), 3)
                          for f, p in frames if not f & 2)
        self.assertEqual(visible, text)


if __name__ == '__main__':
    unittest.main(verbosity=2)
