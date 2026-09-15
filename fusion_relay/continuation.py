from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat as _stat
import time
from dataclasses import dataclass, replace

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .identity import PrivateDirectory
from .operations import OperationJournal

MAX_TURNS = 256
MAX_BYTES = 4 << 20
MAX_SCOPES = 4096

SCHEMA_VERSION = 2
# Migration-in-progress marker: a schema_version row of -2 is written
# before a v1->v2 migration and replaced by 2 after; finding it means a
# crashed migration and the store is refused.
_MIGRATION_MARKER = -2

# Operation statuses beyond the journal's own: a reservation that was
# rejected before dispatch ('preflight_rejected'), a provider outcome we
# cannot prove ('provider_outcome_unknown'), and a client cancel we only
# observed as a closed socket ('cancel_unconfirmed').
UNRESOLVED_STATUSES = ('executing', 'provider_outcome_unknown',
                       'cancel_unconfirmed')

_EPOCH_REASONS = {'model_switch', 'history_compaction', 'fork',
                  'account_change', 'legacy_history', 'history_divergence',
                  'operator_recovery'}
_EVIDENCE_REF = re.compile(r'[0-9a-f]{64}\Z')


class ContinuationError(RuntimeError):
    pass


class EpochRequired(ContinuationError):
    """The client's history no longer extends the stored anchors — an
    explicit epoch transition (e.g. a verified compaction note) is
    required before this request may be admitted."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


@dataclass(frozen=True)
class ContinuationBinding:
    account: str
    session: str
    lane: str
    profile: str
    epoch: str

    def scope(self):
        values = (self.account, self.session, self.lane, self.profile, self.epoch)
        if any(not isinstance(v, str) or not v or len(v) > 512 for v in values):
            raise ContinuationError('trusted continuation binding required')
        if self.lane not in ('lead', 'sidekick'):
            raise ContinuationError('trusted continuation lane required')
        return digest(values)


def visible_items(output):
    if not isinstance(output, list) or len(output) > 2048 or any(
            not isinstance(item, dict) for item in output):
        raise ContinuationError('invalid continuation output')
    result = []
    text = []
    calls = []
    for item in output:
        kind = item.get('type')
        if kind == 'message':
            content = item.get('content', [])
            if not isinstance(content, list) or any(not isinstance(p, dict) for p in content):
                raise ContinuationError('invalid continuation content')
            for part in content:
                key = 'text' if part.get('type') == 'output_text' else 'refusal'
                if part.get('type') not in ('output_text', 'refusal') or not isinstance(part.get(key), str):
                    raise ContinuationError('unsupported continuation content')
                text.append(part[key])
        elif kind == 'function_call':
            if any(not isinstance(item.get(key), str) for key in ('call_id', 'name', 'arguments')) \
                    or not item['call_id'] or not item['name']:
                raise ContinuationError('invalid continuation call')
            call = {key: item[key] for key in ('type', 'call_id', 'name', 'arguments')}
            calls.append(call)
        elif kind != 'reasoning':
            raise ContinuationError('unsupported continuation item')
    if text and ''.join(text):
        result.append({'role': 'assistant', 'content': ''.join(text)})
    result.extend(calls)
    return result


def enumerate_turns(rows):
    """Yield stored turn rows, refusing a non-contiguous revision chain."""
    for expected, row in enumerate(rows, 1):
        if row[0] != expected:
            raise ContinuationError('continuation revision gap')
        yield row


class ContinuationLedger(OperationJournal):
    def __init__(self, path, key: bytes):
        if not isinstance(key, bytes) or len(key) != 32:
            raise ContinuationError('continuation key unavailable')
        self._private = PrivateDirectory(path.parent, create=True)
        self._owner_fd = None
        try:
            # Single-owner flock: exactly one process may hold the store.
            fd = os.open(str(path) + '.owner',
                         os.O_RDWR | os.O_CREAT
                         | getattr(os, 'O_NOFOLLOW', 0)
                         | getattr(os, 'O_NONBLOCK', 0), 0o600)
            try:
                if not _stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ContinuationError(
                        'continuation owner lock is not a file')
                os.fchmod(fd, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except ContinuationError:
                os.close(fd)
                raise
            except OSError:
                os.close(fd)
                raise ContinuationError(
                    'continuation store owned by another process'
                ) from None
            self._owner_fd = fd
            super().__init__(path)
            self._aead = AESGCM(key)
            self._migrate_schema()
            row = self._db.execute(
                "SELECT result FROM operations WHERE scope='key-check' AND operation_id='key-check'").fetchone()
            if row is None:
                if self._db.execute('SELECT 1 FROM continuation_turns LIMIT 1').fetchone() is not None:
                    raise ContinuationError('continuation key reference missing')
                sealed = self._seal('key-check', 'key-check', b'continuation-v1')
                self._db.execute("INSERT INTO operations VALUES('key-check','key-check','v1','succeeded',?)",
                                 (sealed,))
            elif self._open('key-check', 'key-check', row[0]) != b'continuation-v1':
                raise ContinuationError('continuation key mismatch')
        except ContinuationError:
            if getattr(self, '_db', None) is not None:
                self._db.close()
            if self._owner_fd is not None:
                os.close(self._owner_fd)
                self._owner_fd = None
            self._private.close()
            raise
        except BaseException:
            if getattr(self, '_db', None) is not None:
                self._db.close()
            if self._owner_fd is not None:
                os.close(self._owner_fd)
                self._owner_fd = None
            self._private.close()
            raise ContinuationError('continuation store unavailable') from None

    def _migrate_schema(self):
        """Bring the store to schema v2 (or refuse).

        v1 -> v2 adds response_digest/state on continuation_turns plus
        the acknowledgements and epoch_transitions tables. The marker row
        version=-2 is committed before the migration body so a crash
        leaves a detectable 'migration incomplete' store.
        """
        has_version = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='schema_version'").fetchone() is not None
        has_turns = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='continuation_turns'").fetchone() is not None
        if has_version:
            row = self._db.execute(
                'SELECT version FROM schema_version').fetchone()
            version = row[0] if row else None
            if version == _MIGRATION_MARKER:
                raise ContinuationError(
                    'continuation store migration incomplete')
            if version is None or version > SCHEMA_VERSION:
                raise ContinuationError(
                    'continuation store schema newer than supported')
            if version == SCHEMA_VERSION:
                return
            if version != 1:
                raise ContinuationError(
                    'continuation store schema newer than supported')
        elif not has_turns:
            self._db.executescript('''
                CREATE TABLE schema_version(version INTEGER NOT NULL);
                INSERT INTO schema_version VALUES(2);
                CREATE TABLE continuation_heads(
                    scope TEXT PRIMARY KEY, revision INTEGER NOT NULL);
                CREATE TABLE continuation_turns(
                    scope TEXT NOT NULL, revision INTEGER NOT NULL,
                    operation_id TEXT NOT NULL, input_digest TEXT NOT NULL,
                    output_digest TEXT NOT NULL, sealed BLOB NOT NULL,
                    response_digest TEXT, state TEXT NOT NULL
                        DEFAULT 'result_committed',
                    PRIMARY KEY(scope, revision),
                    UNIQUE(scope, operation_id));
                CREATE TABLE acknowledgements(
                    scope TEXT NOT NULL, operation_id TEXT NOT NULL,
                    revision INTEGER, response_digest TEXT,
                    consumer_commit_reference TEXT,
                    PRIMARY KEY(scope, operation_id));
                CREATE TABLE epoch_transitions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_scope TEXT NOT NULL, to_scope TEXT NOT NULL,
                    to_epoch TEXT NOT NULL,
                    reason TEXT NOT NULL, prior_head INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    carried_from_scope TEXT);
                CREATE TABLE scope_bindings(
                    scope TEXT PRIMARY KEY, account TEXT NOT NULL,
                    session TEXT NOT NULL, lane TEXT NOT NULL,
                    profile TEXT NOT NULL, epoch TEXT NOT NULL);
            ''')
            return
        # v1 store: marker committed first so an interrupted migration is
        # detectable, then the migration body in its own transaction.
        self._db.execute('BEGIN IMMEDIATE')
        try:
            self._db.execute(
                'CREATE TABLE IF NOT EXISTS schema_version('
                'version INTEGER NOT NULL)')
            self._db.execute('DELETE FROM schema_version')
            self._db.execute('INSERT INTO schema_version VALUES(-2)')
            self._db.execute('COMMIT')
        except BaseException:
            if self._db.in_transaction:
                self._db.execute('ROLLBACK')
            raise
        self._db.execute('BEGIN IMMEDIATE')
        try:
            self._db.execute(
                'ALTER TABLE continuation_turns '
                'ADD COLUMN response_digest TEXT')
            self._db.execute(
                "ALTER TABLE continuation_turns ADD COLUMN state "
                "TEXT NOT NULL DEFAULT 'result_committed'")
            self._db.execute('''
                CREATE TABLE IF NOT EXISTS acknowledgements(
                    scope TEXT NOT NULL, operation_id TEXT NOT NULL,
                    revision INTEGER, response_digest TEXT,
                    consumer_commit_reference TEXT,
                    PRIMARY KEY(scope, operation_id))''')
            self._db.execute('''
                CREATE TABLE IF NOT EXISTS epoch_transitions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_scope TEXT NOT NULL, to_scope TEXT NOT NULL,
                    to_epoch TEXT NOT NULL,
                    reason TEXT NOT NULL, prior_head INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    carried_from_scope TEXT)''')
            self._db.execute('''
                CREATE TABLE IF NOT EXISTS scope_bindings(
                    scope TEXT PRIMARY KEY, account TEXT NOT NULL,
                    session TEXT NOT NULL, lane TEXT NOT NULL,
                    profile TEXT NOT NULL, epoch TEXT NOT NULL)''')
            self._db.execute('UPDATE schema_version SET version=2')
            self._db.execute('COMMIT')
        except BaseException:
            if self._db.in_transaction:
                self._db.execute('ROLLBACK')
            raise

    def _seal(self, scope, operation_id, payload):
        if len(payload) > MAX_BYTES:
            raise ContinuationError('continuation retention limit reached')
        nonce = secrets.token_bytes(12)
        return base64.b64encode(nonce + self._aead.encrypt(
            nonce, payload, canonical([scope, operation_id]))).decode()

    def _open(self, scope, operation_id, sealed):
        try:
            blob = base64.b64decode(sealed, validate=True)
            return self._aead.decrypt(blob[:12], blob[12:], canonical([scope, operation_id]))
        except Exception:
            raise ContinuationError('continuation authentication failed') from None

    def _turns(self, scope):
        rows = self._db.execute(
            'SELECT revision,operation_id,input_digest,output_digest,sealed '
            'FROM continuation_turns WHERE scope=? ORDER BY revision', (scope,)).fetchall()
        turns = []
        for expected, (revision, operation_id, incoming, outgoing, sealed) in enumerate(rows, 1):
            if revision != expected:
                raise ContinuationError('continuation revision gap')
            turn = json.loads(self._open(scope, operation_id, sealed))
            if digest(turn['input']) != incoming or digest(turn['output']) != outgoing:
                raise ContinuationError('continuation digest mismatch')
            turns.append((operation_id, turn))
        return turns

    def reserve(self, binding: ContinuationBinding, operation_id: str, body: dict):
        scope = binding.scope()
        if not isinstance(operation_id, str) or not operation_id or len(operation_id) > 512:
            raise ContinuationError('stable operation identity required')
        if not isinstance(body, dict) or len(canonical(body)) > MAX_BYTES:
            raise ContinuationError('invalid continuation body')
        body = json.loads(canonical(body))
        history = body.get('input')
        if not isinstance(history, list) or any(not isinstance(x, dict) for x in history):
            raise ContinuationError('invalid native history')
        incoming = digest(body)
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                prior = self._fetch(scope, operation_id)
                if prior is not None:
                    if prior['fingerprint'] != incoming:
                        raise ContinuationError('conflicting continuation operation')
                    if prior['status'] in ('preflight_rejected',
                                           'admission_rejected'):
                        # Rejected before dispatch: nothing was sent, so
                        # the identical retry re-reserves cleanly instead
                        # of stranding the lane as outcome-unknown.
                        self._db.execute(
                            'DELETE FROM operations WHERE scope=? '
                            'AND operation_id=?', (scope, operation_id))
                        prior = None
                if prior is not None:
                    if prior['status'] in ('provider_outcome_unknown',
                                           'cancel_unconfirmed'):
                        raise ContinuationError(
                            'continuation outcome unknown; '
                            'explicit reconciliation required')
                    if prior['status'] != 'succeeded':
                        raise ContinuationError('continuation outcome unknown')
                    result = json.loads(self._open(scope, operation_id, prior['result']))
                    self._db.execute('COMMIT')
                    return {'replay': base64.b64decode(result['wire']), 'scope': scope,
                            'operation_id': operation_id, 'revision': result['revision']}
                pending = self._db.execute(
                    "SELECT 1 FROM operations WHERE scope=? AND status='executing' LIMIT 1", (scope,)).fetchone()
                if pending:
                    raise ContinuationError('continuation turn already reserved')
                # A new inference on top of an unproven provider outcome
                # could double-bill; refuse until reconciled.
                unresolved = self._db.execute(
                    "SELECT 1 FROM operations WHERE scope=? AND status IN "
                    "('provider_outcome_unknown','cancel_unconfirmed') "
                    "LIMIT 1", (scope,)).fetchone()
                if unresolved:
                    raise ContinuationError(
                        'continuation lane has an unresolved operation')
                turns = self._turns(scope)
                head = self._db.execute(
                    'SELECT revision FROM continuation_heads WHERE scope=?', (scope,)).fetchone()
                if (head[0] if head else 0) != len(turns):
                    raise ContinuationError('continuation history missing')
                if head is None and self._db.execute(
                        'SELECT COUNT(*) FROM continuation_heads'
                        ).fetchone()[0] >= MAX_SCOPES:
                    raise ContinuationError(
                        'continuation retention limit reached')
                if len(turns) >= MAX_TURNS:
                    raise ContinuationError('continuation retention limit reached')
                replacements = []
                last_end = 0
                for previous_id, turn in turns:
                    anchor = turn['input']['input']
                    projection = visible_items(turn['output'])
                    start = len(anchor)
                    end = start + len(projection)
                    if start < last_end or history[:start] != anchor or history[start:end] != projection:
                        raise EpochRequired('native history transition requires explicit epoch')
                    replacements.append((start, end, turn['output']))
                    last_end = end
                if not turns and head is None and any(
                        x.get('role') == 'assistant' or x.get('type') in
                        ('function_call', 'function_call_output', 'reasoning') for x in history):
                    raise ContinuationError('legacy continuation unavailable; explicit new epoch required')
                merged = list(history)
                for start, end, output in reversed(replacements):
                    merged[start:end] = output
                # Every prior turn's visible projection was just matched
                # inside the client's own history: that is evidence the
                # consumer retained the result. It is recorded as its own
                # level — never as a consumer acknowledgement.
                evidenced = 0
                if turns:
                    evidenced = self._db.execute(
                        "UPDATE continuation_turns SET "
                        "state='history_evidenced' WHERE scope=? AND "
                        "state IN ('result_committed','offered')",
                        (scope,)).rowcount
                self._db.execute(
                    "INSERT INTO operations(scope,operation_id,fingerprint,status) VALUES(?,?,?,'executing')",
                    (scope, operation_id, incoming))
                self._record_scope(binding)
                self._db.execute('COMMIT')
                return {'body': dict(body, input=merged), 'native_body': body,
                        'scope': scope, 'operation_id': operation_id,
                        'revision': len(turns), 'replay': None,
                        'evidenced': evidenced}
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def commit(self, reservation, output: list[dict], wire: bytes):
        scope, operation_id = reservation['scope'], reservation['operation_id']
        revision = reservation['revision'] + 1
        native_body = reservation['native_body']
        visible_items(output)
        turn = {'input': native_body, 'output': output}
        sealed = self._seal(scope, operation_id, canonical(turn))
        result = self._seal(scope, operation_id, canonical({
            'wire': base64.b64encode(wire).decode(), 'revision': revision}))
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                row = self._fetch(scope, operation_id)
                if row is None or row['status'] != 'executing' or row['fingerprint'] != digest(native_body):
                    raise ContinuationError('continuation reservation mismatch')
                head = self._db.execute(
                    'SELECT revision FROM continuation_heads WHERE scope=?', (scope,)).fetchone()
                if (head[0] if head else 0) != revision - 1:
                    raise ContinuationError('stale continuation revision')
                self._db.execute(
                    'INSERT INTO continuation_turns VALUES(?,?,?,?,?,?,?,?)',
                    (scope, revision, operation_id, digest(native_body),
                     digest(output), sealed,
                     hashlib.sha256(wire).hexdigest(),
                     'result_committed'))
                self._db.execute('INSERT OR REPLACE INTO continuation_heads VALUES(?,?)',
                                 (scope, revision))
                self._db.execute("UPDATE operations SET status='succeeded',result=? WHERE scope=? AND operation_id=?",
                                 (result, scope, operation_id))
                self._db.execute('COMMIT')
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def abandon(self, reservation, reason):
        """Settle a reservation that was rejected before dispatch.

        'preflight_rejected' and 'admission_rejected' are supported:
        the provider was never called, so the operation's outcome is
        known (nothing happened) and an identical retry may re-reserve.
        """
        if reason not in ('preflight_rejected', 'admission_rejected'):
            raise ContinuationError('unsupported abandon reason')
        self._settle_executing(reservation, reason)

    def _settle_executing(self, reservation, status):
        """Shared guard: settle a still-executing reservation to *status*."""
        scope, operation_id = reservation['scope'], reservation['operation_id']
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                row = self._fetch(scope, operation_id)
                if row is None or row['status'] != 'executing' \
                        or row['fingerprint'] != digest(
                            reservation['native_body']):
                    raise ContinuationError(
                        'continuation reservation mismatch')
                self._db.execute(
                    'UPDATE operations SET status=?, result=NULL '
                    'WHERE scope=? AND operation_id=?',
                    (status, scope, operation_id))
                self._db.execute('COMMIT')
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def mark_outcome_unknown(self, reservation):
        """The provider's response did not reach a commit; the outcome
        is unproven and the lane blocks until reconciled."""
        self._settle_executing(reservation, 'provider_outcome_unknown')

    def mark_cancel(self, reservation):
        """The client went away mid-request; we only closed a socket —
        the provider never confirmed cancellation."""
        self._settle_executing(reservation, 'cancel_unconfirmed')

    def resolve_unknown(self, scope, operation_id, evidence_ref,
                        outcome):
        """Operator reconciliation for an unproven operation.

        Applies to 'provider_outcome_unknown' / 'cancel_unconfirmed'
        rows and to 'executing' rows stranded by a crash or admission
        failure — from the operator's viewpoint all are unresolved.

        Only 'abandoned' is supported: a claimed success cannot be
        reconstructed, so the row settles to 'failed' with sealed
        evidence, never a fabricated result.
        """
        if outcome != 'abandoned':
            raise ContinuationError('unsupported resolution outcome')
        if not isinstance(evidence_ref, str) \
                or not _EVIDENCE_REF.fullmatch(evidence_ref):
            raise ContinuationError('evidence reference required')
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                row = self._fetch(scope, operation_id)
                if row is None or row['status'] not in (
                        'provider_outcome_unknown', 'cancel_unconfirmed',
                        'executing'):
                    raise ContinuationError(
                        'operation is not unresolved')
                sealed = self._seal(scope, operation_id, canonical(
                    {'evidence_ref': evidence_ref,
                     'resolution': 'abandoned'}))
                self._db.execute(
                    "UPDATE operations SET status='failed', result=? "
                    'WHERE scope=? AND operation_id=?',
                    (sealed, scope, operation_id))
                self._db.execute('COMMIT')
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    @staticmethod
    def _consumer_for(binding):
        consumer = 'native-client:' + binding.session
        if len(consumer) > 512:
            raise ContinuationError('invalid continuation consumer')
        return consumer

    def offer_result(self, reservation, binding):
        """Offer the committed wire result to the native consumer.

        Separate from commit(): a failure here must not undo the durable
        commit. Returns the stable delivery_id.
        """
        scope, operation_id = reservation['scope'], reservation['operation_id']
        consumer = self._consumer_for(binding)
        offered = self.offer(scope, operation_id, consumer)
        with self._lock:
            self._db.execute(
                "UPDATE continuation_turns SET state='offered' "
                "WHERE scope=? AND operation_id=? "
                "AND state='result_committed'",
                (scope, operation_id))
        return offered['delivery_id']

    def acknowledge_result(self, binding, operation_id, revision,
                           response_digest, consumer_commit_reference):
        """Record consumer acknowledgement of a committed turn.

        Returns False for the first ack, True for an identical retry;
        a conflicting ack for the same operation rejects.
        """
        scope = binding.scope()
        consumer = self._consumer_for(binding)
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                turn = self._db.execute(
                    'SELECT revision, response_digest, state FROM '
                    'continuation_turns WHERE scope=? AND operation_id=?',
                    (scope, operation_id)).fetchone()
                if turn is None or turn[2] not in (
                        'offered', 'history_evidenced',
                        'consumer_acknowledged'):
                    raise ContinuationError(
                        'acknowledgement does not match committed result')
                if turn[0] != revision or turn[1] != response_digest:
                    raise ContinuationError(
                        'acknowledgement does not match committed result')
                prior = self._db.execute(
                    'SELECT revision, response_digest, '
                    'consumer_commit_reference FROM acknowledgements '
                    'WHERE scope=? AND operation_id=?',
                    (scope, operation_id)).fetchone()
                if prior is not None:
                    if prior != (revision, response_digest,
                                 consumer_commit_reference):
                        raise ContinuationError(
                            'conflicting acknowledgement')
                    self._db.execute('COMMIT')
                    return True
                self._db.execute(
                    'INSERT INTO acknowledgements VALUES(?,?,?,?,?)',
                    (scope, operation_id, revision, response_digest,
                     consumer_commit_reference))
                self._db.execute(
                    "UPDATE continuation_turns SET "
                    "state='consumer_acknowledged' WHERE scope=? "
                    'AND operation_id=?', (scope, operation_id))
                delivery = self._db.execute(
                    'SELECT delivery_id FROM delivery WHERE scope=? '
                    'AND operation_id=? AND consumer=?',
                    (scope, operation_id, consumer)).fetchone()
                if delivery is not None:
                    # acknowledge() holds no txn of its own — its UPDATE
                    # joins this transaction.
                    self.acknowledge(scope, consumer, delivery[0])
                self._db.execute('COMMIT')
                return False
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def acceptance_state(self, binding, operation_id):
        with self._lock:
            turn = self._db.execute(
                'SELECT state FROM continuation_turns WHERE scope=? '
                'AND operation_id=?',
                (binding.scope(), operation_id)).fetchone()
        if turn is None:
            return 'uncertain'
        if turn[0] == 'consumer_acknowledged':
            return 'acknowledged'
        if turn[0] == 'history_evidenced':
            # The consumer later sent this turn's visible projection back
            # inside its own history — evidence of retention, qualified:
            # not a consumer acknowledgement.
            return 'history_evidenced'
        if turn[0] in ('offered', 'result_committed'):
            return 'pending'
        return 'uncertain'

    def _record_scope(self, binding):
        """Remember which binding a scope belongs to (same txn as caller)
        so carry-over candidates can be found by account/lane/profile."""
        self._db.execute(
            'INSERT OR IGNORE INTO scope_bindings VALUES(?,?,?,?,?,?)',
            (binding.scope(), binding.account, binding.session,
             binding.lane, binding.profile, binding.epoch))

    def _history_matches(self, turns, history):
        """True when *history* extends every stored turn's anchor and
        visible projection, exactly as reserve() requires."""
        last_end = 0
        for _, turn in turns:
            anchor = turn['input']['input']
            projection = visible_items(turn['output'])
            start = len(anchor)
            end = start + len(projection)
            if start < last_end or history[:start] != anchor \
                    or history[start:end] != projection:
                return False
            last_end = end
        return True

    def find_carry_candidate(self, binding, history, limit=64):
        """Find a prior scope of the same account/lane/profile whose
        committed turns are all extended by *history* — the case of a
        conversation resumed under a new native session seed. Returns
        that scope's binding or None. Bounded to the newest *limit*
        scopes; sealed turns are opened only under the lock.
        """
        if not isinstance(history, list):
            return None
        with self._lock:
            rows = self._db.execute(
                'SELECT scope, account, session, lane, profile, epoch '
                'FROM scope_bindings WHERE account=? AND lane=? '
                'AND profile=? AND scope != ? ORDER BY rowid DESC LIMIT ?',
                (binding.account, binding.lane, binding.profile,
                 binding.scope(), int(limit))).fetchall()
            for scope, account, session, lane, profile, epoch in rows:
                candidate = ContinuationBinding(
                    account=account, session=session, lane=lane,
                    profile=profile, epoch=epoch)
                if candidate.scope() != scope:
                    continue
                unresolved = self._db.execute(
                    "SELECT 1 FROM operations WHERE scope=? AND status IN "
                    "('executing','provider_outcome_unknown',"
                    "'cancel_unconfirmed') LIMIT 1", (scope,)).fetchone()
                if unresolved:
                    continue
                try:
                    turns = self._turns(scope)
                except ContinuationError:
                    continue
                if turns and self._history_matches(turns, history):
                    return candidate
        return None

    def transition_epoch(self, binding, new_epoch, reason, *,
                         carry_from=None):
        """Start a new continuation scope when the native history can no
        longer extend the old one (compaction, fork, model switch).

        Reasons: 'model_switch', 'fork', 'account_change',
        'operator_recovery'; 'history_compaction' when a verified
        compaction note authorized the transition; 'legacy_history' when
        a resumed/rewritten history is adopted as new genesis;
        'history_divergence' when the client history no longer extends
        the committed anchors and the cause is NOT established (a
        recent compaction note is evidence only, never proof).

        The old scope's turns are preserved untouched. Without
        ``carry_from`` the new scope starts at revision 0. With
        ``carry_from`` (a binding found by ``find_carry_candidate``) that
        scope's committed turns are copied — re-sealed under the new
        scope — so the resumed conversation keeps its stored reasoning
        items; operations rows are never copied (an old operation id
        must not replay under the new scope).
        """
        if reason not in _EPOCH_REASONS:
            raise ContinuationError('unsupported epoch transition reason')
        if not isinstance(new_epoch, str) or not new_epoch \
                or len(new_epoch) > 512 or new_epoch == binding.epoch:
            raise ContinuationError('invalid continuation epoch')
        if carry_from is not None and (
                not isinstance(carry_from, ContinuationBinding)
                or (carry_from.account, carry_from.lane, carry_from.profile)
                != (binding.account, binding.lane, binding.profile)):
            raise ContinuationError('carry-over source binding mismatch')
        from_scope = binding.scope()
        new_binding = replace(binding, epoch=new_epoch)
        to_scope = new_binding.scope()
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                unresolved = self._db.execute(
                    "SELECT 1 FROM operations WHERE scope=? AND status IN "
                    "('executing','provider_outcome_unknown',"
                    "'cancel_unconfirmed') LIMIT 1",
                    (from_scope,)).fetchone()
                if unresolved:
                    raise ContinuationError(
                        'epoch transition blocked by unresolved operation')
                if self._db.execute(
                        'SELECT 1 FROM continuation_heads WHERE scope=?',
                        (to_scope,)).fetchone():
                    raise ContinuationError(
                        'continuation epoch already exists')
                head = self._db.execute(
                    'SELECT revision FROM continuation_heads '
                    'WHERE scope=?', (from_scope,)).fetchone()
                carried_scope = None
                carried_head = 0
                if carry_from is not None:
                    carried_scope = carry_from.scope()
                    if self._db.execute(
                            "SELECT 1 FROM operations WHERE scope=? AND "
                            "status IN ('executing',"
                            "'provider_outcome_unknown',"
                            "'cancel_unconfirmed') LIMIT 1",
                            (carried_scope,)).fetchone():
                        raise ContinuationError(
                            'carry-over blocked by unresolved operation')
                    rows = self._db.execute(
                        'SELECT revision, operation_id, input_digest, '
                        'output_digest, sealed, response_digest, state '
                        'FROM continuation_turns WHERE scope=? '
                        'ORDER BY revision', (carried_scope,)).fetchall()
                    for (revision, operation_id, in_d, out_d, sealed,
                         resp_d, state) in enumerate_turns(rows):
                        payload = self._open(carried_scope, operation_id,
                                             sealed)
                        self._db.execute(
                            'INSERT INTO continuation_turns'
                            '(scope,revision,operation_id,input_digest,'
                            'output_digest,sealed,response_digest,state) '
                            'VALUES(?,?,?,?,?,?,?,?)',
                            (to_scope, revision, operation_id, in_d, out_d,
                             self._seal(to_scope, operation_id, payload),
                             resp_d, state))
                        carried_head = revision
                self._db.execute(
                    'INSERT INTO epoch_transitions'
                    '(from_scope,to_scope,to_epoch,reason,prior_head,'
                    'created_at,carried_from_scope) VALUES(?,?,?,?,?,?,?)',
                    (from_scope, to_scope, new_epoch, reason,
                     head[0] if head else 0, time.time(), carried_scope))
                # Seeding the new scope's head marks it as an authorized
                # genesis (revision 0) or as the carried continuation.
                self._db.execute(
                    'INSERT INTO continuation_heads VALUES(?,?)',
                    (to_scope, carried_head))
                self._record_scope(new_binding)
                self._db.execute('COMMIT')
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise
        return new_binding

    def epoch_chain(self, binding):
        """The chain of bindings from *binding* through recorded epoch
        transitions: the start binding first, the resolved binding last.
        A scope with more than one outgoing transition follows the newest
        (highest id). Cycles and overlong chains are refused.
        """
        chain = [binding]
        seen = {binding.scope()}
        current = binding
        for _ in range(1024):
            with self._lock:
                row = self._db.execute(
                    'SELECT to_scope, to_epoch FROM epoch_transitions '
                    'WHERE from_scope=? ORDER BY id DESC LIMIT 1',
                    (current.scope(),)).fetchone()
            if row is None:
                return chain
            current = replace(current, epoch=row[1])
            if current.scope() in seen or current.scope() != row[0]:
                raise ContinuationError('continuation epoch chain invalid')
            seen.add(current.scope())
            chain.append(current)
        raise ContinuationError('continuation epoch chain invalid')

    def resolve_epoch(self, binding):
        """The effective binding: *binding* walked to the newest epoch
        in its transition chain."""
        return self.epoch_chain(binding)[-1]

    def retire_scope(self, scope):
        """Delete a fully-acknowledged scope. Never called automatically;
        refused while anything is pending or uncertain."""
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                pending = self._db.execute(
                    "SELECT 1 FROM continuation_turns WHERE scope=? "
                    "AND state != 'consumer_acknowledged' LIMIT 1",
                    (scope,)).fetchone()
                unresolved = self._db.execute(
                    'SELECT 1 FROM operations WHERE scope=? AND status IN '
                    "('executing','provider_outcome_unknown',"
                    "'cancel_unconfirmed') LIMIT 1", (scope,)).fetchone()
                if pending or unresolved:
                    raise ContinuationError(
                        'scope has pending or uncertain operations')
                for table in ('continuation_turns', 'continuation_heads',
                              'operations', 'delivery',
                              'acknowledgements'):
                    self._db.execute(
                        'DELETE FROM %s WHERE scope=?' % table, (scope,))
                self._db.execute('COMMIT')
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def delivery(self, binding, operation_id, consumer):
        offered = super().offer(binding.scope(), operation_id, consumer)
        payload = json.loads(self._open(binding.scope(), operation_id, offered['result']))
        return dict(offered, result=base64.b64decode(payload['wire']))

    def close(self):
        try:
            super().close()
        finally:
            if self._owner_fd is not None:
                os.close(self._owner_fd)
                self._owner_fd = None
            self._private.close()
