"""Durable SQLite journal for relay-owned tool operations.

Each relay-dispatched tool call is journaled under ``(scope,
operation_id)`` where *scope* is an opaque trusted host binding
(account/session/profile/role) supplied by the caller — never a model
argument or cache key. The execution intent is persisted before
dispatch and the outcome afterward; a crash between those writes leaves
``executing``, which observers see as ``outcome_unknown`` — never an
automatic retry. Conflicting reuse of an operation id under a different
fingerprint is refused, including after restart.

This is an operation journal, not a desktop lease: dedup happens in the
local DB while the lock is released across the actuator call, so nothing
here serializes or fences the actual desktop surface.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sqlite3
import stat as _stat
import threading
import uuid
from typing import Callable, Optional

from . import storage

MAX_RESULT_BYTES = 4 << 20

_SCHEMA = """
CREATE TABLE IF NOT EXISTS operations(
    scope TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    PRIMARY KEY(scope, operation_id));
CREATE TABLE IF NOT EXISTS delivery(
    scope TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    consumer TEXT NOT NULL,
    delivery_id TEXT NOT NULL UNIQUE,
    acknowledged INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(scope, operation_id, consumer))
"""

_TERMINAL = ("succeeded", "failed", "cancelled")


class OperationJournal:
    """SQLite-backed operation journal for one data directory."""

    def __init__(self, path: pathlib.Path) -> None:
        path = pathlib.Path(path)
        storage.ensure_private_dir(path.parent)
        fd = os.open(path, os.O_RDWR | os.O_CREAT
                     | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_NONBLOCK", 0), 0o600)
        try:
            if not _stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError("journal path is not a regular file")
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        self._db = sqlite3.connect(str(path), isolation_level=None,
                                   check_same_thread=False)
        self._db.execute("PRAGMA busy_timeout=1000")
        self._db.execute("PRAGMA journal_mode=DELETE")
        self._db.execute("PRAGMA synchronous=FULL")
        for stmt in _SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._db.execute(stmt)
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _fetch(self, scope: str, operation_id: str) -> Optional[dict]:
        row = self._db.execute(
            "SELECT fingerprint, status, result FROM operations "
            "WHERE scope=? AND operation_id=?",
            (scope, operation_id)).fetchone()
        if row is None:
            return None
        return {"fingerprint": row[0], "status": row[1], "result": row[2]}

    def lookup(self, scope: str, operation_id: str) -> Optional[dict]:
        """Return the recorded operation, or None. ``executing`` rows are
        reported as ``outcome_unknown`` to observers."""
        with self._lock:
            row = self._fetch(scope, operation_id)
        if row is None:
            return None
        status = row["status"]
        return {"scope": scope, "operation_id": operation_id,
                "fingerprint": row["fingerprint"],
                "status": "outcome_unknown" if status == "executing" else status,
                "result": row["result"]}

    def _finish(self, scope: str, operation_id: str, status: str,
                result: Optional[str]) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE operations SET status=?, result=? "
                "WHERE scope=? AND operation_id=?",
                (status, result, scope, operation_id))

    def run(self, scope: str, call: dict, execute: Callable[[], str],
            check: Callable[[], None]) -> str:
        """Execute *call* at most once per (scope, call_id).

        *check* is a cancellation callback invoked while waiting for the
        journal lock, inside the pre-dispatch transaction, and again
        before dispatch. The database transaction is never held across
        the side-effecting call itself.
        """
        from . import translate  # local import: avoids a module cycle
        if not isinstance(scope, str) or not scope:
            raise translate.UnsupportedRequest("operation scope required")
        cid = call.get("call_id")
        name = call.get("name")
        arguments = call.get("arguments")
        if (not isinstance(cid, str) or not cid
                or not isinstance(name, str) or not name
                or not isinstance(arguments, str)):
            raise translate.UnsupportedRequest("invalid tool call shape")
        try:
            parsed = json.loads(arguments)
        except ValueError:
            raise translate.UnsupportedRequest(
                "tool call arguments not valid JSON")
        if not isinstance(parsed, dict):
            raise translate.UnsupportedRequest(
                "tool call arguments not a JSON object")
        fingerprint = hashlib.sha256(
            json.dumps([name, arguments]).encode()).hexdigest()

        while not self._lock.acquire(timeout=0.05):
            check()
        try:
            check()
            committed = False
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._fetch(scope, cid)
                if row is not None:
                    self._db.execute("COMMIT")
                    committed = True
                    if row["fingerprint"] != fingerprint:
                        raise translate.UnsupportedRequest(
                            "conflicting reuse of operation id")
                    if row["status"] in _TERMINAL:
                        return row["result"]
                    raise translate.IncompleteResponse(
                        "outcome_unknown: explicit reconciliation required")
                self._db.execute(
                    "INSERT INTO operations"
                    "(scope, operation_id, fingerprint, status) "
                    "VALUES(?,?,?,'executing')", (scope, cid, fingerprint))
                self._db.execute("COMMIT")
                committed = True
            except BaseException:
                if not committed:
                    try:
                        self._db.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
        finally:
            self._lock.release()

        try:
            check()
        except BaseException:
            self._finish(scope, cid, "cancelled", "cancelled before dispatch")
            raise
        try:
            result = execute()
        except BaseException:
            self._finish(scope, cid, "outcome_unknown", None)
            raise
        if not isinstance(result, str) \
                or len(result.encode("utf-8")) > MAX_RESULT_BYTES:
            self._finish(scope, cid, "outcome_unknown", None)
            raise translate.IncompleteResponse(
                "outcome_unknown: explicit reconciliation required")
        self._finish(scope, cid, "succeeded", result)
        return result

    # -- durable result delivery --------------------------------------
    #
    # The delivery table records per-consumer handoff of terminal
    # results. ``consumer`` is an arbitrary caller-supplied label — it is
    # not a trusted identity. Offering is not acknowledgement: a result
    # stays pending until ``acknowledge`` is called.

    @staticmethod
    def _check_delivery_args(scope: str, consumer: str) -> None:
        from . import translate
        for v, name in ((scope, "scope"), (consumer, "consumer")):
            if not isinstance(v, str) or not v or len(v) > 512:
                raise translate.UnsupportedRequest(
                    f"delivery {name} required")

    def offer(self, scope: str, operation_id: str,
              consumer: str) -> dict:
        """Offer a terminal result to a consumer with a stable id.

        Only persisted ``succeeded``/``failed``/``cancelled`` results can
        be offered; ``executing``/unknown outcomes refuse. The random
        delivery id is created once per (scope, operation, consumer) and
        reused across retries.
        """
        from . import translate
        self._check_delivery_args(scope, consumer)
        if not isinstance(operation_id, str) or not operation_id \
                or len(operation_id) > 512:
            raise translate.UnsupportedRequest("operation_id required")
        with self._lock:
            committed = False
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._fetch(scope, operation_id)
                if row is None:
                    raise translate.UnsupportedRequest(
                        "unknown operation")
                if row["status"] not in _TERMINAL:
                    raise translate.IncompleteResponse(
                        "outcome_unknown: explicit reconciliation "
                        "required")
                existing = self._db.execute(
                    "SELECT delivery_id FROM delivery WHERE scope=? "
                    "AND operation_id=? AND consumer=?",
                    (scope, operation_id, consumer)).fetchone()
                delivery_id = existing[0] if existing \
                    else uuid.uuid4().hex
                if existing is None:
                    self._db.execute(
                        "INSERT INTO delivery"
                        "(scope, operation_id, consumer, delivery_id) "
                        "VALUES(?,?,?,?)",
                        (scope, operation_id, consumer, delivery_id))
                self._db.execute("COMMIT")
                committed = True
            except BaseException:
                if not committed:
                    try:
                        self._db.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
        return {"delivery_id": delivery_id,
                "operation_id": operation_id,
                "status": row["status"], "result": row["result"]}

    def acknowledge(self, scope: str, consumer: str,
                    delivery_id: str) -> bool:
        """Mark a delivery acknowledged; idempotent True after ack."""
        self._check_delivery_args(scope, consumer)
        with self._lock:
            row = self._db.execute(
                "SELECT acknowledged FROM delivery WHERE scope=? "
                "AND consumer=? AND delivery_id=?",
                (scope, consumer, delivery_id)).fetchone()
            if row is None:
                return False
            if not row[0]:
                self._db.execute(
                    "UPDATE delivery SET acknowledged=1 WHERE scope=? "
                    "AND consumer=? AND delivery_id=?",
                    (scope, consumer, delivery_id))
            return True

    def pending_deliveries(self, scope: str, consumer: str) -> list:
        """Unacknowledged deliveries for one scoped consumer."""
        self._check_delivery_args(scope, consumer)
        with self._lock:
            rows = self._db.execute(
                "SELECT d.operation_id, d.delivery_id, o.status, "
                "o.result FROM delivery d JOIN operations o "
                "ON o.scope=d.scope AND o.operation_id=d.operation_id "
                "WHERE d.scope=? AND d.consumer=? AND d.acknowledged=0",
                (scope, consumer)).fetchall()
        return [{"operation_id": r[0], "delivery_id": r[1],
                 "status": r[2], "result": r[3]} for r in rows]
