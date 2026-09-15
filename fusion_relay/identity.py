"""Service identity proof and dirfd-pinned private filesystem access."""
from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path

PROTOCOL = 1
_NONCE_RE = re.compile(r"[0-9a-f]{64}")
_ENDPOINT_RE = re.compile(r"^http://127\.0\.0\.1:([0-9]{1,5})$")
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode()


def _hex64(value) -> bool:
    return isinstance(value, str) and _NONCE_RE.fullmatch(value) is not None


def _endpoint_ok(value) -> bool:
    if not isinstance(value, str):
        return False
    m = _ENDPOINT_RE.match(value)
    return m is not None and 1 <= int(m.group(1)) <= 65535


def service_proof(secret, nonce, endpoint, instance, release):
    body = {'nonce': nonce, 'endpoint': endpoint, 'instance': instance,
            'release': release, 'protocol': PROTOCOL}
    return {'body': body, 'mac': hmac.new(
        secret, canonical(body), hashlib.sha256).hexdigest()}


def verify_service(proof, secret, nonce, endpoint, allowed_release):
    try:
        if not isinstance(secret, bytes) or len(secret) != 32:
            return False
        if not isinstance(proof, dict) or set(proof) != {'body', 'mac'}:
            return False
        body = proof['body']
        if not isinstance(body, dict) or set(body) != {
                'nonce', 'endpoint', 'instance', 'release', 'protocol'}:
            return False
        if (body['nonce'] != nonce or body['endpoint'] != endpoint
                or body['release'] != allowed_release
                or type(body['protocol']) is not int
                or body['protocol'] != PROTOCOL):
            return False
        if not (_hex64(nonce) and _hex64(body['instance'])
                and _hex64(allowed_release) and _endpoint_ok(endpoint)):
            return False
        mac = proof['mac']
        if not _hex64(mac):
            return False
        expected = hmac.new(secret, canonical(body),
                            hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, mac)
    except (KeyError, TypeError, ValueError):
        return False


def build_identity(root=None) -> str:
    root = Path(root) if root is not None \
        else Path(__file__).resolve().parent.parent
    pkg = root / 'fusion_relay'
    files = sorted(p for p in pkg.iterdir() if p.suffix == '.py')
    files += [root / 'bin' / 'devin-fusion', root / 'bin' / 'fusion-relay',
              root / 'bin' / 'fusion-continuation-admin',
              root / 'bin' / 'fusion-experimental-host',
              root / 'bin' / 'fusion-post-compaction',
              root / 'bin' / 'fusion-prompt-marker']
    digest = hashlib.sha256()
    for path in files:
        st = os.lstat(path)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise RuntimeError('build identity: non-regular code file')
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b'\0')
        fd = os.open(path, os.O_RDONLY | _NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError('build identity: non-regular code file')
            while True:
                chunk = os.read(fd, 1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
        finally:
            os.close(fd)
    return digest.hexdigest()


def _check_name(name) -> None:
    if not isinstance(name, str) or not name or '/' in name \
            or name in ('.', '..'):
        raise ValueError('invalid private file name')


def _check_dir(st, final: bool) -> None:
    uid = os.getuid()
    if final:
        if st.st_uid != uid or st.st_mode & 0o077:
            raise OSError('private directory has unsafe ownership or mode')
        return
    if st.st_uid not in (0, uid):
        raise OSError('private path ancestor has unsafe ownership')
    if st.st_mode & 0o022 and not (
            st.st_uid == 0 and st.st_mode & stat.S_ISVTX):
        raise OSError('private path ancestor is writable by others')


class PrivateDirectory:
    """A directory held open by fd; all access is relative to it."""

    def __init__(self, path, create: bool = False):
        raw = os.fspath(path)
        if isinstance(raw, bytes):
            raw = os.fsdecode(raw)
        if '\x00' in raw:
            raise ValueError('unsafe path')
        for comp in raw.split('/'):
            if comp in ('.', '..'):
                raise ValueError('unsafe path component')
        path = Path(raw)
        if not path.is_absolute():
            raise ValueError('private path must be absolute')
        parts = path.parts
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW)
        try:
            _check_dir(os.fstat(fd), False)
            for i, comp in enumerate(parts[1:], 1):
                final = i == len(parts) - 1
                fd = self._child(fd, comp, create)
                _check_dir(os.fstat(fd), final)
            _check_dir(os.fstat(fd), True)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    @staticmethod
    def _child(parent_fd, name, create):
        # Bounded retry: this platform's openat spuriously reports
        # ENOENT under concurrent opens of the same name.
        fd = None
        for _ in range(5):
            try:
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY
                             | _NOFOLLOW, dir_fd=parent_fd)
                break
            except FileNotFoundError:
                if not create:
                    continue
                try:
                    os.mkdir(name, 0o700, dir_fd=parent_fd)
                except FileExistsError:
                    pass
        if fd is None:
            raise FileNotFoundError(name)
        try:
            entry = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cur = os.fstat(fd)
            if stat.S_ISLNK(entry.st_mode) or \
                    (entry.st_dev, entry.st_ino) != (cur.st_dev, cur.st_ino):
                raise OSError('private path component identity mismatch')
        except BaseException:
            os.close(fd)
            raise
        os.close(parent_fd)
        return fd

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)

    def _file(self, name, flags, mode=0o600):
        if self._fd is None:
            raise OSError('private directory closed')
        _check_name(name)
        fd = None
        for _ in range(5):
            try:
                fd = os.open(name, flags | _NOFOLLOW | _NONBLOCK, mode,
                             dir_fd=self._fd)
                break
            except FileNotFoundError:
                if not flags & os.O_CREAT:
                    raise
        if fd is None:
            raise FileNotFoundError(name)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() \
                    or st.st_nlink != 1 or st.st_mode & 0o077:
                raise OSError('unsafe private file')
        except BaseException:
            os.close(fd)
            raise
        return fd

    def read(self, name, limit: int) -> bytes:
        fd = self._file(name, os.O_RDONLY)
        try:
            data = b''
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    return data
                data += chunk
                if len(data) > limit:
                    raise OSError('private file too large')
        finally:
            os.close(fd)

    def write_new(self, name, data: bytes) -> None:
        fd = self._file(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        try:
            written = 0
            while written < len(data):
                n = os.write(fd, data[written:])
                if n <= 0:
                    raise OSError('write made no progress')
                written += n
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(self._fd)

    def append_fd(self, name):
        return self._file(name, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

    def lock(self, name):
        fd = self._file(name, os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        return fd


@dataclass
class ServiceIdentity:
    secret: bytes
    endpoint: str
    instance: str
    release: str

    @classmethod
    def create(cls, directory: PrivateDirectory, port: int):
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('invalid port')
        try:
            raw = directory.read('identity.key', 64)
        except FileNotFoundError:
            raw = None
        if raw is None:
            secret = secrets.token_bytes(32)
            directory.write_new('identity.key', secret)
        else:
            if len(raw) != 32:
                raise RuntimeError('invalid identity key')
            secret = raw
        return cls(secret=secret,
                   endpoint='http://127.0.0.1:%d' % port,
                   instance=secrets.token_hex(32),
                   release=build_identity())

    def proof(self, nonce) -> dict:
        if not _hex64(nonce):
            raise ValueError('invalid nonce')
        return service_proof(self.secret, nonce, self.endpoint,
                             self.instance, self.release)
