from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from .identity import PrivateDirectory

FIELDS = ('input_tokens', 'output_tokens', 'cached_tokens', 'reasoning_tokens')
MAX_COUNT = 2**63 - 1
REF = re.compile(r'[0-9a-f]{64}')
PROVIDERS = ('codex', 'native')
STATUSES = ('completed', 'incomplete', 'failed', 'cancelled', 'unknown')


class AccountingUnavailable(RuntimeError):
    pass


def reference(*parts: str) -> str:
    return hashlib.sha256(json.dumps(parts, separators=(',', ':'),
                                     ensure_ascii=True).encode()).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False)


def _ref(value):
    return isinstance(value, str) and REF.fullmatch(value) is not None


class AccountingLedger:
    def __init__(self, path: Path):
        self._lock = threading.RLock()
        self.degraded = False
        self.gaps = 0
        self.run_id = uuid.uuid4().hex
        self._private = PrivateDirectory(Path(path).parent, create=True)
        self._db = None
        try:
            fd = self._private._file(Path(path).name, os.O_RDWR | os.O_CREAT)
            os.close(fd)
            self._db = sqlite3.connect(str(path), isolation_level=None,
                                       check_same_thread=False, timeout=1)
            self._db.execute('PRAGMA journal_mode=DELETE')
            self._db.execute('PRAGMA synchronous=FULL')
            if self._db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise AccountingUnavailable('corrupt accounting store')
            self._db.executescript('''
                CREATE TABLE IF NOT EXISTS accounting_runs(
                    run_id TEXT PRIMARY KEY, started REAL NOT NULL,
                    closed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS accounting_operations(
                    operation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    provider TEXT NOT NULL, account_ref TEXT NOT NULL,
                    state TEXT NOT NULL, admitted REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS accounting_events(
                    event_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL,
                    provider TEXT NOT NULL, account_ref TEXT NOT NULL,
                    digest TEXT NOT NULL, payload TEXT NOT NULL,
                    committed REAL NOT NULL, exported INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS accounting_reconciliations(
                    reconciliation_id TEXT PRIMARY KEY, evidence_ref TEXT NOT NULL,
                    acknowledged REAL NOT NULL, unknown_operations INTEGER NOT NULL);
            ''')
            dirty = self._db.execute(
                'SELECT COUNT(*) FROM accounting_runs WHERE closed=0').fetchone()[0]
            pending = self._db.execute(
                "SELECT COUNT(*) FROM accounting_operations WHERE state='pending'").fetchone()[0]
            self.degraded = bool(dirty or pending)
            self._db.execute('INSERT INTO accounting_runs(run_id, started) VALUES(?,?)',
                             (self.run_id, time.time()))
        except BaseException:
            if self._db is not None:
                self._db.close()
            self._private.close()
            raise AccountingUnavailable('accounting store unavailable') from None

    def _transaction(self, execute):
        with self._lock:
            try:
                self._db.execute('BEGIN IMMEDIATE')
                result = execute()
                self._db.execute('COMMIT')
                return result
            except BaseException:
                if self._db.in_transaction:
                    try:
                        self._db.execute('ROLLBACK')
                    except sqlite3.Error:
                        pass
                self.degraded = True
                self.gaps += 1
                raise AccountingUnavailable('durable accounting unavailable') from None

    def admit(self, operation_id: str, provider: str, account_ref: str):
        if not _ref(operation_id) or not _ref(account_ref) or provider not in PROVIDERS:
            raise ValueError('invalid accounting identity')
        if self.degraded:
            raise AccountingUnavailable('accounting degraded')

        def execute():
            row = self._db.execute(
                'SELECT provider,account_ref,state FROM accounting_operations '
                'WHERE operation_id=?', (operation_id,)).fetchone()
            if row is not None:
                raise AccountingUnavailable('operation already admitted')
            self._db.execute(
                'INSERT INTO accounting_operations VALUES(?,?,?,?,?,?)',
                (operation_id, self.run_id, provider, account_ref, 'pending', time.time()))
        self._transaction(execute)

    def complete(self, operation_id: str, events: list[dict]):
        if not _ref(operation_id) or not isinstance(events, list) or not 1 <= len(events) <= 32:
            raise ValueError('invalid accounting completion')
        approved = {'event_id', 'provider', 'account_ref', 'status', *FIELDS}
        validated = []
        for event in events:
            if not isinstance(event, dict) or set(event) != approved:
                raise ValueError('unapproved accounting fields')
            if not _ref(event['event_id']) or not _ref(event['account_ref']) \
                    or event['provider'] not in PROVIDERS or event['status'] not in STATUSES:
                raise ValueError('invalid accounting event')
            if any(event[k] is not None and (type(event[k]) is not int
                   or not 0 <= event[k] <= MAX_COUNT) for k in FIELDS):
                raise ValueError('invalid accounting counts')
            payload = _canonical(event)
            validated.append((event, payload, hashlib.sha256(payload.encode()).hexdigest()))

        def execute():
            operation = self._db.execute(
                'SELECT provider,account_ref,state FROM accounting_operations WHERE operation_id=?',
                (operation_id,)).fetchone()
            if operation is None:
                raise AccountingUnavailable('unadmitted operation')
            if operation[2] == 'complete':
                recorded = dict(self._db.execute(
                    'SELECT event_id,digest FROM accounting_events WHERE operation_id=?',
                    (operation_id,)).fetchall())
                proposed = {event['event_id']: digest for event, _, digest in validated}
                if recorded != proposed:
                    raise AccountingUnavailable('conflicting completed accounting operation')
            for event, payload, digest in validated:
                if (event['provider'], event['account_ref']) != operation[:2]:
                    raise AccountingUnavailable('accounting identity conflict')
                prior = self._db.execute(
                    'SELECT digest,operation_id FROM accounting_events WHERE event_id=?',
                    (event['event_id'],)).fetchone()
                if prior is not None:
                    if prior != (digest, operation_id):
                        raise AccountingUnavailable('conflicting accounting receipt')
                    continue
                self._db.execute(
                    'INSERT INTO accounting_events(event_id,operation_id,provider,account_ref,'
                    'digest,payload,committed) VALUES(?,?,?,?,?,?,?)',
                    (event['event_id'], operation_id, event['provider'], event['account_ref'],
                     digest, payload, time.time()))
            self._db.execute("UPDATE accounting_operations SET state='complete' WHERE operation_id=?",
                             (operation_id,))
        self._transaction(execute)

    def record_gap(self):
        with self._lock:
            self.degraded = True
            self.gaps += 1

    def snapshot(self):
        with self._lock:
            try:
                stored = self._db.execute('SELECT payload,committed,exported,digest FROM accounting_events').fetchall()
                rows = []
                for payload, committed, exported, digest in stored:
                    if hashlib.sha256(payload.encode()).hexdigest() != digest:
                        raise ValueError('corrupt receipt')
                    event = json.loads(payload)
                    if not isinstance(event, dict) or set(event) != {
                            'event_id', 'provider', 'account_ref', 'status', *FIELDS} \
                            or event['provider'] not in PROVIDERS \
                            or event['status'] not in STATUSES \
                            or not _ref(event['event_id']) or not _ref(event['account_ref']) or any(
                            event[k] is not None and (type(event[k]) is not int
                            or not 0 <= event[k] <= MAX_COUNT) for k in FIELDS):
                        raise ValueError('corrupt receipt')
                    rows.append((payload, committed, exported))
                pending = self._db.execute(
                    "SELECT provider,COUNT(*) FROM accounting_operations WHERE state='pending' GROUP BY provider").fetchall()
                dirty = self._db.execute(
                    'SELECT COUNT(*) FROM accounting_runs WHERE closed=0 AND run_id<>?',
                    (self.run_id,)).fetchone()[0]
            except (sqlite3.Error, ValueError):
                self.degraded = True
                return {'degraded': True, 'partial': True, 'coverage': 'unavailable',
                        'gap_count': self.gaps, 'reconciliation_required': True}
            providers = {p: {'responses': 0, 'known': {k: None for k in FIELDS},
                             'missing': {k: 0 for k in FIELDS}, 'pending': 0}
                         for p in PROVIDERS}
            for payload, _, _ in rows:
                event = json.loads(payload)
                bucket = providers[event['provider']]
                bucket['responses'] += 1
                for key in FIELDS:
                    if event[key] is None:
                        bucket['missing'][key] += 1
                    else:
                        bucket['known'][key] = (bucket['known'][key] or 0) + event[key]
            for provider, count in pending:
                providers[provider]['pending'] = count
            partial = self.degraded or bool(dirty) or bool(pending) or any(
                any(p['missing'].values()) for p in providers.values())
            return {'providers': providers, 'partial': partial,
                    'degraded': self.degraded or bool(dirty), 'coverage': 'partial' if partial else 'complete',
                    'gap_count': self.gaps, 'unclean_runs': dirty,
                    'reconciliation_required': bool(dirty or pending or self.gaps),
                    'pending_exports': sum(not row[2] for row in rows),
                    'last_durable_write': max((row[1] for row in rows), default=None)}

    def unexported(self, limit=100):
        with self._lock:
            return self._db.execute(
                'SELECT event_id,payload FROM accounting_events WHERE exported=0 ORDER BY committed,event_id LIMIT ?',
                (limit,)).fetchall()

    def mark_exported(self, event_id):
        self._transaction(lambda: self._db.execute(
            'UPDATE accounting_events SET exported=1 WHERE event_id=?', (event_id,)))

    def acknowledge_unknown(self, evidence_ref: str, *, confirmed: bool = False):
        if confirmed is not True or not _ref(evidence_ref):
            raise ValueError('explicit reconciliation acknowledgment required')
        if self.snapshot()['coverage'] == 'unavailable':
            raise AccountingUnavailable('corrupt accounting store cannot be acknowledged')

        def execute():
            pending = self._db.execute(
                "SELECT operation_id,provider,account_ref FROM accounting_operations WHERE state='pending'").fetchall()
            for operation_id, provider, account_ref in pending:
                event = dict(event_id=reference('reconciled-unknown', operation_id),
                             provider=provider, account_ref=account_ref, status='unknown',
                             **{key: None for key in FIELDS})
                payload = _canonical(event)
                self._db.execute(
                    'INSERT INTO accounting_events(event_id,operation_id,provider,account_ref,digest,payload,committed) '
                    'VALUES(?,?,?,?,?,?,?)', (event['event_id'], operation_id, provider, account_ref,
                    hashlib.sha256(payload.encode()).hexdigest(), payload, time.time()))
                self._db.execute(
                    "UPDATE accounting_operations SET state='reconciled_unknown' WHERE operation_id=?",
                    (operation_id,))
            self._db.execute('INSERT INTO accounting_reconciliations VALUES(?,?,?,?)',
                             (uuid.uuid4().hex, evidence_ref, time.time(), len(pending)))
            self._db.execute('UPDATE accounting_runs SET closed=1 WHERE run_id<>?', (self.run_id,))
        self._transaction(execute)
        self.degraded = False
        self.gaps = 0

    def close(self, clean=True):
        with self._lock:
            try:
                if clean and not self.degraded:
                    self._db.execute('UPDATE accounting_runs SET closed=1 WHERE run_id=?',
                                     (self.run_id,))
            finally:
                self._db.close()
                self._private.close()
