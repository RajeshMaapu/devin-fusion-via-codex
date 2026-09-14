"""Minimal protobuf + Connect-RPC framing codec for the Devin api-server wire.

Connect protocol on the wire: every message is framed as
``[1 byte flags][4 byte big-endian length][payload]``. A trailer frame has
flag bit ``0x02`` set and carries a JSON object (errors / end metadata); bit
``0x01`` marks compressed payloads, which this relay never emits.

Only the subset of protobuf wire types the api-server actually uses is
implemented: varint (0), length-delimited (2), 64-bit (1), 32-bit (5).
Unknown fields are preserved by :func:`decode` and ignored by callers.
"""

from __future__ import annotations

import struct
from typing import Any, Union

FieldValue = Union[int, bytes]
Message = dict[int, list[FieldValue]]

FLAG_COMPRESSED = 0x01
FLAG_TRAILER = 0x02
MAX_FRAME_PAYLOAD = 64 << 20
MAX_FRAMES = 1024


def varint(n: int) -> bytes:
    """Encode a non-negative integer as a protobuf varint."""
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def field(number: int, value: Union[int, str, bytes]) -> bytes:
    """Encode one protobuf field (varint for int, length-delimited otherwise)."""
    if isinstance(value, bool):
        return varint(number << 3) + varint(int(value))
    if isinstance(value, int):
        return varint(number << 3) + varint(value)
    if isinstance(value, str):
        value = value.encode()
    return varint(number << 3 | 2) + varint(len(value)) + value


def decode(buf: bytes) -> Message:
    """Decode a protobuf message into ``{field_number: [values...]}``.

    Length-delimited values stay ``bytes``; callers decode nested messages
    with :func:`decode` or strings with ``.decode()`` as needed.
    """
    out: Message = {}
    pos = 0

    def read_varint() -> int:
        nonlocal pos
        n = 0
        shift = 0
        while pos < len(buf):
            byte = buf[pos]
            pos += 1
            n |= (byte & 127) << shift
            if byte < 128:
                return n
            shift += 7
            if shift > 70:
                raise ValueError("varint too long")
        raise ValueError("truncated varint")

    while pos < len(buf):
        tag = read_varint()
        number, wire_type = tag >> 3, tag & 7
        if not number:
            raise ValueError("field number zero")
        if wire_type == 0:
            value: FieldValue = read_varint()
        elif wire_type == 2:
            length = read_varint()
            value = buf[pos : pos + length]
            pos += length
        elif wire_type in (1, 5):
            length = 8 if wire_type == 1 else 4
            value = buf[pos : pos + length]
            pos += length
        else:
            raise ValueError(f"unsupported wire type {wire_type}")
        if pos > len(buf):
            raise ValueError("truncated field")
        out.setdefault(number, []).append(value)
    return out


def frame(payload: bytes, flags: int = 0) -> bytes:
    """Wrap a payload in a Connect envelope frame."""
    return bytes([flags]) + struct.pack(">I", len(payload)) + payload


def unframe(buf: bytes) -> tuple[int, bytes]:
    """Read one Connect frame from the start of *buf*; return (flags, payload)."""
    if len(buf) < 5:
        raise ValueError("truncated frame header")
    flags = buf[0]
    if flags not in (0, FLAG_COMPRESSED, FLAG_TRAILER):
        raise ValueError(f"invalid frame flags {flags}")
    length = struct.unpack(">I", buf[1:5])[0]
    if length > MAX_FRAME_PAYLOAD:
        raise ValueError("frame payload too large")
    if len(buf) < 5 + length:
        raise ValueError("truncated frame payload")
    return flags, buf[5 : 5 + length]


def iter_frames(buf) -> list[tuple[int, bytes]]:
    """Split a buffer into all its Connect frames.

    Strict: rejects truncated headers/payloads, invalid flags, more than
    MAX_FRAMES frames, and any bytes after the end-of-stream trailer
    (including another frame). Reads through a memoryview; returned
    payloads are plain bytes.
    """
    view = memoryview(buf)
    frames = []
    pos = 0
    while pos < len(view):
        if len(frames) >= MAX_FRAMES:
            raise ValueError("too many frames")
        flags, payload = unframe(view[pos:])
        if frames and frames[-1][0] & FLAG_TRAILER:
            raise ValueError("bytes after end-of-stream trailer")
        frames.append((flags, bytes(payload)))
        pos += 5 + len(payload)
    return frames


def bounded_decompress(payload: bytes, limit: int = MAX_FRAME_PAYLOAD) -> bytes:
    """Gunzip *payload* with a hard output cap.

    Rejects streams exceeding *limit*, truncated streams, concatenated
    gzip members, and trailing garbage.
    """
    import zlib

    if type(limit) is not int or limit < 0:
        raise ValueError("invalid decompress limit")
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        out = d.decompress(payload, limit + 1)
    except zlib.error:
        raise ValueError("invalid compressed frame")
    if len(out) > limit or d.unconsumed_tail:
        raise ValueError("decompressed frame too large")
    if not d.eof or d.unused_data:
        raise ValueError("invalid compressed frame")
    return out


TypedMessage = dict[int, list[tuple[FieldValue, int]]]


def decode_typed(buf: bytes) -> TypedMessage:
    """Decode like :func:`decode` but keep each field's wire type.

    Values: wire type 0 -> int, types 1/2/5 -> bytes. Re-encode with
    :func:`encode_typed` to round-trip losslessly (fixed32/64 stay fixed).
    """
    out: TypedMessage = {}
    pos = 0

    def read_varint() -> int:
        nonlocal pos
        n = 0
        shift = 0
        while pos < len(buf):
            byte = buf[pos]
            pos += 1
            n |= (byte & 127) << shift
            if byte < 128:
                return n
            shift += 7
            if shift > 70:
                raise ValueError("varint too long")
        raise ValueError("truncated varint")

    while pos < len(buf):
        tag = read_varint()
        number, wire_type = tag >> 3, tag & 7
        if not number:
            raise ValueError("field number zero")
        if wire_type == 0:
            value: FieldValue = read_varint()
        elif wire_type == 2:
            length = read_varint()
            value = buf[pos : pos + length]
            pos += length
        elif wire_type in (1, 5):
            length = 8 if wire_type == 1 else 4
            value = buf[pos : pos + length]
            pos += length
        else:
            raise ValueError(f"unsupported wire type {wire_type}")
        if pos > len(buf):
            raise ValueError("truncated field")
        out.setdefault(number, []).append((value, wire_type))
    return out


def encode_typed(msg: TypedMessage) -> bytes:
    """Re-encode a :func:`decode_typed` message preserving wire types."""
    out = bytearray()
    for number, fields_ in msg.items():  # dict preserves wire order
        for value, wire_type in fields_:
            if wire_type == 0:
                out += varint(number << 3) + varint(int(value))
            elif wire_type == 2:
                raw = value if isinstance(value, bytes) else str(value).encode()
                out += varint(number << 3 | 2) + varint(len(raw)) + raw
            else:
                raw = bytes(value)
                out += varint(number << 3 | wire_type) + raw
    return bytes(out)


def set_string(msg: TypedMessage, number: int, value: str) -> None:
    """Replace-or-set a string field on a typed message."""
    msg[number] = [(value.encode(), 2)]


def get_string(msg: TypedMessage, number: int, default: str = "") -> str:
    """Read a string field from a typed message."""
    vals = msg.get(number)
    if not vals:
        return default
    v = vals[0][0]
    return v.decode() if isinstance(v, bytes) else default


def text(msg: Message, number: int, default: str = "") -> str:
    """Read a string field from a decoded message."""
    value = msg.get(number, [default.encode()])[0]
    return value.decode() if isinstance(value, bytes) else str(value)


def error_frame(code: str, message: str) -> bytes:
    """Build a Connect end-of-stream frame carrying an error object."""
    import json

    return frame(json.dumps({"error": {"code": code, "message": message}}).encode(), FLAG_TRAILER)


def end_stream() -> bytes:
    """Build the empty end-of-stream trailer the CLI expects after a stream."""
    return frame(b"{}", FLAG_TRAILER)


def first(msg: Message, number: int, default: Any = None) -> Any:
    """Return the first value of a field, or *default*."""
    values = msg.get(number)
    return values[0] if values else default
