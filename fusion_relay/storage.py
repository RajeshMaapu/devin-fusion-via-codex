"""Private filesystem helpers for relay-owned state.

Every path the relay writes is created with restrictive permissions
from the first write, never through a symlink, and replaced atomically.
These helpers never delete pre-existing or historical data — a failure
cleans up only the temp file the failing call itself created.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import pathlib
import stat as _stat
import tempfile

_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


def _is_regular(mode: int) -> bool:
    return _stat.S_ISREG(mode)


def ensure_private_dir(path: pathlib.Path) -> pathlib.Path:
    """Create *path* (mode 0700) or verify it is a real directory.

    A symlinked or non-directory leaf is refused before anything is
    created; intermediate parents are created normally.
    """
    path = pathlib.Path(path)
    try:
        if _stat.S_ISLNK(os.lstat(path).st_mode):
            raise OSError("private dir path is a symlink")
    except FileNotFoundError:
        pass
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    st = os.lstat(path)
    if _stat.S_ISLNK(st.st_mode) or not _stat.S_ISDIR(st.st_mode):
        raise OSError("private dir path is not a real directory")
    os.chmod(path, 0o700)
    return path


def atomic_write(path: pathlib.Path, data: bytes) -> None:
    """Write *data* to *path* atomically with 0600 permissions.

    The temp file is created fresh in the target's directory (mkstemp —
    no predictable name, 0600 from first write), fsynced, then
    ``os.replace``d; the parent directory is fsynced afterwards. An
    existing non-regular or symlinked target is refused.
    """
    path = pathlib.Path(path)
    ensure_private_dir(path.parent)
    try:
        if not _is_regular(os.lstat(path).st_mode):
            raise OSError("refusing to replace a non-regular file")
    except FileNotFoundError:
        pass
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".relay-")
    tmp_path = pathlib.Path(tmp)
    try:
        try:
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            raise
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        dirfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except BaseException:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def read_private(path: pathlib.Path, max_bytes: int) -> bytes:
    """Read at most *max_bytes* from a regular file, never via symlink.

    Oversized content raises — returning a truncated prefix could let a
    valid-looking prefix mask trailing corruption.
    """
    path = pathlib.Path(path)
    fd = os.open(path, os.O_RDONLY | _NOFOLLOW | _NONBLOCK)
    try:
        if not _is_regular(os.fstat(fd).st_mode):
            raise OSError("not a regular file")
        data = b""
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            data += chunk
            if len(data) > max_bytes:
                raise OSError("private state file too large")
        return data
    finally:
        os.close(fd)


def append_private(path: pathlib.Path, data: bytes) -> None:
    """Append all of *data* under an flock so writers don't interleave."""
    path = pathlib.Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT
                 | _NOFOLLOW | _NONBLOCK, 0o600)
    try:
        if not _is_regular(os.fstat(fd).st_mode):
            raise OSError("not a regular file")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            written = 0
            while written < len(data):
                n = os.write(fd, data[written:])
                if n <= 0:
                    raise OSError("append made no progress")
                written += n
            os.fsync(fd)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextlib.contextmanager
def store_owner(dir_path: pathlib.Path):
    """Hold a nonblocking exclusive flock on the data directory.

    Exactly one relay process may own a data directory at a time —
    route state is single-writer. Acquisition failure is explicit.
    """
    ensure_private_dir(dir_path)
    fd = os.open(pathlib.Path(dir_path) / ".owner.lock",
                 os.O_RDWR | os.O_CREAT | _NOFOLLOW | _NONBLOCK, 0o600)
    try:
        if not _is_regular(os.fstat(fd).st_mode):
            raise OSError("owner lock is not a regular file")
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise OSError("another relay process owns this data directory")
        yield
    finally:
        os.close(fd)
