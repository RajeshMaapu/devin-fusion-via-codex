"""Scoped, validated PNG artifact store plus typed tool results.

Only the PNG subset described below is supported — anything else fails
explicitly. Validation happens fully in memory before any byte reaches
disk: signature, chunk CRC32, exactly-one first IHDR (13 bytes,
8-bit RGB/RGBA, no interlace), ordered IDAT, terminal empty IEND, no
trailing data, bounded size/count, and a strict zlib-bounded IDAT
decompression against the exact expected scanline length with filter
bytes 0–4. This is a validation boundary, not a general image codec.

Store capacity is a fixed aggregate quota (default 32 MiB) enforced in
the same SQLite transaction as metadata insert; on exhaustion the write
fails ``resource_exhausted`` — existing artifacts are never auto-deleted
to make room. Expired artifacts fail ``get`` even while their bytes
remain on disk; cleanup is a separate, later decision (no
``prune_expired`` in this slice).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import pathlib
import re
import sqlite3
import stat as _stat
import struct
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from typing import Optional

from . import storage

MAX_ARTIFACT_BYTES = 8 << 20   # encoded PNG
MAX_CHUNKS = 4096
MAX_DIMENSION = 8192
MAX_PIXELS = 16_000_000
_MAX_TEXT = 64 << 10
_MAX_IMAGES = 4
_REF_RE = re.compile(r"[0-9a-f]{32}")
_NAME_MAX = 512
_PNG_SIG = b"\x89PNG\r\n\x1a\n"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifacts(
    ref TEXT NOT NULL PRIMARY KEY,
    scope TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    captured_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    sha256 TEXT NOT NULL,
    nbytes INTEGER NOT NULL)
"""


class ResourceExhausted(RuntimeError):
    """Artifact store aggregate quota exceeded."""


class ArtifactError(ValueError):
    """Malformed, unsupported, or unknown artifact."""


class ArtifactExpired(PermissionError):
    """Artifact exists but its retention window has closed."""


def _check_name(v: str, name: str) -> None:
    if not isinstance(v, str) or not v or len(v) > _NAME_MAX:
        raise ArtifactError(f"{name} must be a bounded nonempty string")


def validate_png(data: bytes) -> tuple[int, int]:
    """Strict PNG-subset validation; returns (width, height)."""
    if not isinstance(data, (bytes, bytearray)) \
            or not data or len(data) > MAX_ARTIFACT_BYTES:
        raise ArtifactError("artifact bytes missing or too large")
    data = bytes(data)
    if not data.startswith(_PNG_SIG):
        raise ArtifactError("not a PNG")
    pos = len(_PNG_SIG)
    seen_ihdr = False
    seen_idat = False
    idat_closed = False
    ended = False
    width = height = 0
    channels = 0
    idat = bytearray()
    nchunks = 0
    while pos < len(data):
        nchunks += 1
        if nchunks > MAX_CHUNKS:
            raise ArtifactError("too many PNG chunks")
        if pos + 8 > len(data):
            raise ArtifactError("truncated chunk header")
        (length,) = struct.unpack(">I", data[pos:pos + 4])
        ctype = data[pos + 4:pos + 8]
        if not all(65 <= c <= 90 or 97 <= c <= 122 for c in ctype) \
                or ctype[2] & 0x20:
            raise ArtifactError("invalid PNG chunk name")
        if pos + 8 + length + 4 > len(data):
            raise ArtifactError("truncated chunk body")
        body = data[pos + 8:pos + 8 + length]
        (crc,) = struct.unpack(">I", data[pos + 8 + length:
                                          pos + 12 + length])
        if binascii.crc32(ctype + body) & 0xFFFFFFFF != crc:
            raise ArtifactError("chunk CRC mismatch")
        pos += 12 + length
        if ended:
            raise ArtifactError("trailing data after IEND")
        if ctype == b"IHDR":
            if nchunks != 1:
                raise ArtifactError("IHDR must be first and unique")
            if length != 13:
                raise ArtifactError("IHDR length must be 13")
            seen_ihdr = True
            width, height, bitdepth, color, comp, filt, interlace = \
                struct.unpack(">IIBBBBB", body)
            if not (0 < width <= MAX_DIMENSION
                    and 0 < height <= MAX_DIMENSION):
                raise ArtifactError("PNG dimensions out of bounds")
            if width * height > MAX_PIXELS:
                raise ArtifactError("PNG pixel count out of bounds")
            if bitdepth != 8 or color not in (2, 6):
                raise ArtifactError("only 8-bit RGB/RGBA supported")
            if comp != 0 or filt != 0 or interlace != 0:
                raise ArtifactError("unsupported PNG encoding fields")
            channels = 3 if color == 2 else 4
        elif ctype == b"IDAT":
            if not seen_ihdr or ended or idat_closed:
                raise ArtifactError("IDAT out of order")
            seen_idat = True
            idat += body
        elif ctype == b"IEND":
            if length != 0:
                raise ArtifactError("IEND must be empty")
            if not seen_ihdr or not seen_idat:
                raise ArtifactError("IEND before required chunks")
            ended = True
        elif not (ctype[0] & 0x20):
            raise ArtifactError(f"unknown critical chunk {ctype!r}")
        else:
            # ancillary chunk: IDAT sequence, if any, is now closed
            if seen_idat:
                idat_closed = True
    if not ended:
        raise ArtifactError("missing IEND")
    expected = (width * channels + 1) * height
    d = zlib.decompressobj()
    try:
        out = d.decompress(bytes(idat), expected + 1)
    except zlib.error:
        raise ArtifactError("invalid IDAT stream")
    if len(out) != expected or d.unconsumed_tail:
        raise ArtifactError("IDAT output size mismatch")
    if not d.eof or d.unused_data:
        raise ArtifactError("truncated or concatenated IDAT stream")
    rowlen = width * channels + 1
    for i in range(height):
        if out[i * rowlen] > 4:
            raise ArtifactError("invalid PNG row filter byte")
    return width, height


@dataclass(frozen=True)
class ImageArtifact:
    ref: str
    scope: str
    operation_id: str
    mime_type: str
    width: int
    height: int
    captured_at: float
    expires_at: float
    sha256: str


@dataclass(frozen=True)
class ToolResult:
    operation_id: str
    status: str
    text_blocks: tuple = ()
    image_artifact_refs: tuple = ()


class ArtifactStore:
    """PNG artifact store rooted at a private directory."""

    def __init__(self, root: pathlib.Path, clock=time.time,
                 max_bytes: int = 32 << 20):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ArtifactError("max_bytes must be a positive int")
        self._root = pathlib.Path(root)
        self._clock = clock
        self._max_bytes = max_bytes
        storage.ensure_private_dir(self._root)
        db_path = self._root / "artifacts.db"
        fd = os.open(db_path, os.O_RDWR | os.O_CREAT
                     | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_NONBLOCK", 0), 0o600)
        try:
            if not _stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError("artifact db path is not a regular file")
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        self._db = sqlite3.connect(str(db_path), isolation_level=None,
                                   check_same_thread=False)
        self._db.execute("PRAGMA busy_timeout=1000")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute(_SCHEMA)
        self._lock = threading.RLock()
        self._failed = False

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def put_png(self, scope: str, operation_id: str, data: bytes,
                ttl: float = 300) -> ImageArtifact:
        _check_name(scope, "scope")
        _check_name(operation_id, "operation_id")
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) \
                or not (ttl == ttl) or ttl in (float("inf"),
                                               -float("inf")):
            raise ArtifactError("invalid ttl")
        if not isinstance(data, (bytes, bytearray)):
            raise ArtifactError("artifact bytes required")
        data = bytes(data)  # immutable snapshot before validate/hash
        ttl = min(300.0, max(1.0, float(ttl)))
        if self._failed:
            raise ArtifactError(
                "artifact store unavailable after ambiguous commit")
        width, height = validate_png(data)  # fully validated pre-write
        digest = hashlib.sha256(data).hexdigest()
        for _ in range(3):
            ref = uuid.uuid4().hex
            path = self._root / f"{ref}.png"
            now = self._clock()
            art = ImageArtifact(
                ref=ref, scope=scope, operation_id=operation_id,
                mime_type="image/png", width=width, height=height,
                captured_at=now, expires_at=now + ttl, sha256=digest)
            commit_attempted = False
            created = False
            with self._lock:
                if self._failed:
                    raise ArtifactError(
                        "artifact store unavailable after ambiguous "
                        "commit")
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    (used,) = self._db.execute(
                        "SELECT COALESCE(SUM(nbytes),0) FROM artifacts"
                    ).fetchone()
                    if used + len(data) > self._max_bytes:
                        raise ResourceExhausted(
                            "resource_exhausted: artifact store capacity")
                    self._db.execute(
                        "INSERT INTO artifacts(ref, scope, operation_id,"
                        " mime_type, width, height, captured_at,"
                        " expires_at, sha256, nbytes)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (ref, scope, operation_id, "image/png", width,
                         height, art.captured_at, art.expires_at,
                         digest, len(data)))
                    fd = os.open(
                        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                        | getattr(os, "O_NOFOLLOW", 0), 0o600)
                    created = True
                    try:
                        view = memoryview(data)
                        while view:
                            n = os.write(fd, view)
                            if n == 0:
                                raise OSError("zero-length write")
                            view = view[n:]
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    # durable directory entry before the metadata commit
                    dfd = os.open(self._root, os.O_RDONLY)
                    try:
                        os.fsync(dfd)
                    finally:
                        os.close(dfd)
                    commit_attempted = True
                    try:
                        self._db.execute("COMMIT")
                    except BaseException:
                        # ambiguous outcome: never remove the file, and
                        # fail the store so repeated ambiguous writes
                        # cannot accumulate unaccounted orphans
                        self._failed = True
                        if self._db.in_transaction:
                            try:
                                self._db.execute("ROLLBACK")
                            except sqlite3.Error:
                                pass
                        raise
                    return art
                except (sqlite3.IntegrityError, FileExistsError):
                    # ref collision (row) or pre-existing orphan file:
                    # roll back the uncommitted row and retry a new ref
                    try:
                        self._db.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    continue
                except BaseException:
                    if not commit_attempted:
                        try:
                            self._db.execute("ROLLBACK")
                        except sqlite3.Error:
                            pass
                        if created:
                            try:
                                path.unlink()
                            except OSError:
                                pass
                    # once COMMIT is attempted the file is never
                    # removed: an ambiguous-success commit must not
                    # orphan recorded evidence, and a rolled-back row
                    # leaves a known harmless orphan file.
                    raise
        raise ArtifactError("artifact ref allocation failed")

    def _row(self, scope: str, ref: str):
        if not _REF_RE.fullmatch(ref or ""):
            raise ArtifactError("invalid artifact ref")
        _check_name(scope, "scope")
        return self._db.execute(
            "SELECT operation_id, width, height, captured_at,"
            " expires_at, sha256, nbytes FROM artifacts"
            " WHERE scope=? AND ref=?", (scope, ref)).fetchone()

    def get(self, scope: str, ref: str) -> tuple:
        """Validated artifact + bytes; expired artifacts fail."""
        with self._lock:
            row = self._row(scope, ref)
        if row is None:
            raise ArtifactError("unknown artifact ref")
        (op_id, width, height, captured_at, expires_at,
         sha256, nbytes) = row
        if self._clock() >= expires_at:
            raise ArtifactExpired("artifact retention expired")
        data = storage.read_private(self._root / f"{ref}.png",
                                    MAX_ARTIFACT_BYTES + 1)
        if len(data) != nbytes \
                or hashlib.sha256(data).hexdigest() != sha256:
            raise ArtifactError("artifact bytes failed integrity check")
        if validate_png(data) != (width, height):
            raise ArtifactError("artifact dimensions changed")
        art = ImageArtifact(
            ref=ref, scope=scope, operation_id=op_id,
            mime_type="image/png", width=width, height=height,
            captured_at=captured_at, expires_at=expires_at,
            sha256=sha256)
        return art, data

    def image_part(self, scope: str, ref: str) -> dict:
        _, data = self.get(scope, ref)
        return {"type": "input_image",
                "image_url": "data:image/png;base64,"
                             + base64.b64encode(data).decode(),
                "detail": "original"}


def encode_result(result: ToolResult, store: ArtifactStore,
                  scope: str) -> list:
    """Serialize a ToolResult into Responses-API input items.

    Only whitelisted statuses pass; image refs must belong to this scope
    and this operation. Pixel data is embedded unscaled (original
    coordinates).
    """
    if not isinstance(result, ToolResult):
        raise ArtifactError("ToolResult required")
    _check_name(scope, "scope")
    if result.status not in ("succeeded", "failed", "cancelled",
                             "outcome_unknown"):
        raise ArtifactError("unsupported result status")
    _check_name(result.operation_id, "result.operation_id")
    if not isinstance(result.text_blocks, (tuple, list)) \
            or len(result.text_blocks) > 64:
        raise ArtifactError("text_blocks must be a bounded tuple/list")
    total = 0
    blocks = []
    for block in result.text_blocks:
        if not isinstance(block, str) or not block:
            raise ArtifactError("text block missing")
        total += len(block.encode("utf-8"))
        if total > _MAX_TEXT:
            raise ArtifactError("text blocks exceed 64 KiB total")
        blocks.append(block)
    if not isinstance(result.image_artifact_refs, (tuple, list)) \
            or len(result.image_artifact_refs) > _MAX_IMAGES:
        raise ArtifactError("too many image artifacts")
    parts = [{"type": "input_text",
              "text": json.dumps({"operation_id": result.operation_id,
                                  "status": result.status},
                                 sort_keys=True)}]
    parts.extend({"type": "input_text", "text": b} for b in blocks)
    for ref in result.image_artifact_refs:
        if not isinstance(ref, str) or not _REF_RE.fullmatch(ref):
            raise ArtifactError("invalid image artifact ref")
        art, data = store.get(scope, ref)
        if art.operation_id != result.operation_id:
            raise ArtifactError("image artifact belongs to another "
                                "operation")
        parts.append({"type": "input_image",
                      "image_url": "data:image/png;base64,"
                                   + base64.b64encode(data).decode(),
                      "detail": "original"})
    return parts
