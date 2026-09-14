"""Image decode/validation for wire payloads (PNG native, JPEG via Pillow)."""

from __future__ import annotations

import io
import warnings

from .artifacts import (MAX_ARTIFACT_BYTES, MAX_DIMENSION, MAX_PIXELS,
                        ArtifactError, validate_png)


def checked_image(data: bytes,
                  declared_mime: str) -> tuple[str, tuple[int, int]]:
    if not isinstance(data, bytes) or not data \
            or len(data) > MAX_ARTIFACT_BYTES:
        raise ArtifactError('image bytes missing or too large')
    if declared_mime not in ('image/png', 'image/jpeg'):
        raise ArtifactError('unsupported image MIME')
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        if declared_mime != 'image/png':
            raise ArtifactError('image MIME mismatch')
        return 'image/png', validate_png(data)
    if not data.startswith(b'\xff\xd8\xff') \
            or not data.endswith(b'\xff\xd9'):
        raise ArtifactError('unsupported image encoding')
    try:
        from PIL import Image
    except ImportError:
        raise ArtifactError(
            'JPEG decoder unavailable; install requirements-image.txt'
        ) from None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data),
                            formats=['JPEG']) as image:
                width, height = image.size
                if not (0 < width <= MAX_DIMENSION
                        and 0 < height <= MAX_DIMENSION) \
                        or width * height > MAX_PIXELS:
                    raise ArtifactError('image dimensions exceed limits')
                if image.format != 'JPEG' \
                        or image.mode not in ('RGB', 'L'):
                    raise ArtifactError('unsupported JPEG mode')
                image.verify()
            with Image.open(io.BytesIO(data),
                            formats=['JPEG']) as image:
                image.load()
        return 'image/jpeg', (width, height)
    except (OSError, ValueError, Image.DecompressionBombWarning,
            Image.DecompressionBombError):
        raise ArtifactError('invalid JPEG') from None


def image_as_png(data: bytes, declared_mime: str) -> tuple[bytes, str]:
    mime, size = checked_image(data, declared_mime)
    if mime == 'image/png':
        return data, mime
    from PIL import Image
    with Image.open(io.BytesIO(data), formats=['JPEG']) as image:
        image.load()
        output = io.BytesIO()
        image.convert('RGB').save(output, format='PNG')
    png = output.getvalue()
    if validate_png(png) != size:
        raise ArtifactError('image dimensions changed')
    return png, mime
