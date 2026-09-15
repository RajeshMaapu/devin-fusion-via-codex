"""macOS Keychain access for the continuation sealing key.

Only the OS key store is consulted; there is no disk/env fallback and no
subprocess `security` invocation (which would expose the secret on argv).
Signatures follow the Security.framework SDK headers
(SecKeychain.h): Find takes eight parameters with a leading
CFTypeRef keychainOrArray; the password payload is a const void*.
"""
from __future__ import annotations

import ctypes
import re
import secrets
import sys

from .continuation import ContinuationError

_ACCOUNT = re.compile(r'[0-9a-f]{64}\Z')
_ERR_ITEM_NOT_FOUND = -25300
_ERR_DUPLICATE_ITEM = -25299
_ERR_AUTH_FAILED = -25293          # errSecAuthFailed
_ERR_INTERACTION_NOT_ALLOWED = -25308
_ERR_INVALID_KEYCHAIN = -25295
_ERR_NOT_AVAILABLE = -25291
_FRAMEWORK = '/System/Library/Frameworks/Security.framework/Security'
_COREFOUNDATION = ('/System/Library/Frameworks/CoreFoundation.framework'
                   '/CoreFoundation')


def _configure(lib):
    lib.SecKeychainFindGenericPassword.restype = ctypes.c_int32
    lib.SecKeychainFindGenericPassword.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_char_p,
        ctypes.c_uint32, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p)]
    lib.SecKeychainItemFreeContent.restype = ctypes.c_int32
    lib.SecKeychainItemFreeContent.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p]
    lib.SecKeychainAddGenericPassword.restype = ctypes.c_int32
    lib.SecKeychainAddGenericPassword.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_char_p,
        ctypes.c_uint32, ctypes.c_char_p, ctypes.c_uint32,
        ctypes.c_void_p, ctypes.c_void_p]
    lib.SecKeychainItemDelete.restype = ctypes.c_int32
    lib.SecKeychainItemDelete.argtypes = [ctypes.c_void_p]
    return lib


class MacOSKeychain:
    def __init__(self, service='ai.fusion-codex-relay.continuation',
                 _library=None):
        if sys.platform != 'darwin':
            raise ContinuationError('OS key store unavailable')
        self._service = service.encode()
        if _library is not None:
            self._lib = _library
        else:
            try:
                self._lib = _configure(ctypes.CDLL(_FRAMEWORK))
            except OSError:
                raise ContinuationError(
                    'OS key store unavailable') from None

    def load(self, account_ref: str, create: bool = False) -> bytes:
        if sys.platform != 'darwin':
            raise ContinuationError('OS key store unavailable')
        if not isinstance(account_ref, str) \
                or not _ACCOUNT.fullmatch(account_ref):
            raise ContinuationError('invalid key account reference')
        account = account_ref.encode()
        length = ctypes.c_uint32()
        pointer = ctypes.c_void_p()
        status = self._lib.SecKeychainFindGenericPassword(
            None, len(self._service), self._service, len(account), account,
            ctypes.byref(length), ctypes.byref(pointer), None)
        if status == 0:
            try:
                if not pointer.value or length.value != 32:
                    raise ContinuationError('OS key store record invalid')
                return ctypes.string_at(pointer, 32)
            finally:
                self._lib.SecKeychainItemFreeContent(None, pointer)
        if status == _ERR_ITEM_NOT_FOUND:
            if not create:
                raise ContinuationError('continuation key missing')
            secret = secrets.token_bytes(32)
            payload = ctypes.create_string_buffer(secret, len(secret))
            added = self._lib.SecKeychainAddGenericPassword(
                None, len(self._service), self._service, len(account),
                account, len(secret), payload, None)
            if added == _ERR_DUPLICATE_ITEM:
                return self.load(account_ref, create=False)
            if added != 0:
                raise ContinuationError('OS key store error')
            return secret
        raise ContinuationError(self._map_error(status))

    @staticmethod
    def _map_error(status) -> str:
        if status in (_ERR_AUTH_FAILED, _ERR_INTERACTION_NOT_ALLOWED):
            return 'OS key store access denied'
        if status in (_ERR_INVALID_KEYCHAIN, _ERR_NOT_AVAILABLE):
            return 'OS key store unavailable'
        return 'OS key store error'

    def delete(self, account_ref: str) -> None:
        """Remove a key item; missing items are an explicit error."""
        if sys.platform != 'darwin':
            raise ContinuationError('OS key store unavailable')
        if not isinstance(account_ref, str) \
                or not _ACCOUNT.fullmatch(account_ref):
            raise ContinuationError('invalid key account reference')
        account = account_ref.encode()
        length = ctypes.c_uint32()
        pointer = ctypes.c_void_p()
        item = ctypes.c_void_p()
        status = self._lib.SecKeychainFindGenericPassword(
            None, len(self._service), self._service, len(account), account,
            ctypes.byref(length), ctypes.byref(pointer),
            ctypes.byref(item))
        if status == _ERR_ITEM_NOT_FOUND:
            raise ContinuationError('continuation key missing')
        if status != 0:
            raise ContinuationError(self._map_error(status))
        try:
            if pointer.value:
                self._lib.SecKeychainItemFreeContent(None, pointer)
            deleted = self._lib.SecKeychainItemDelete(item)
            if deleted != 0:
                raise ContinuationError('OS key store error')
        finally:
            self._cfrelease(item.value)

    def _cfrelease(self, ref) -> None:
        if not ref:
            return
        try:
            cf = self._cf
        except AttributeError:
            cf = self._cf = ctypes.CDLL(_COREFOUNDATION)
        cf.CFRelease(ctypes.c_void_p(ref))
