import ctypes
import pathlib
import sys
import unittest
from unittest import mock

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fusion_relay.continuation import ContinuationError
from fusion_relay.keychain import MacOSKeychain, _configure

ACCOUNT = 'a' * 64

# SecKeychain.h: Find takes eight parameters — leading CFTypeRef
# keychainOrArray, then service/account lengths+strings, out length,
# out data pointer, item ref.
FIND_T = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p,
                          ctypes.c_uint32, ctypes.c_char_p,
                          ctypes.c_uint32, ctypes.c_char_p,
                          ctypes.POINTER(ctypes.c_uint32),
                          ctypes.POINTER(ctypes.c_void_p),
                          ctypes.c_void_p)


class FakeSecurity:
    """Injectable stand-in for Security.framework — records calls and
    fills ctypes output parameters with synthetic bytes."""

    def __init__(self):
        self.status = 0
        self.add_status = 0
        self.payload = b'p' * 32
        self.freed = []
        self.added = []
        self.finds = 0
        self.keychain_args = []
        self._bufs = []

    def SecKeychainFindGenericPassword(self, keychain, slen, service,
                                       alen, account, out_len, out_ptr,
                                       item):
        self.finds += 1
        self.keychain_args.append(keychain)
        self.last_service = service
        self.last_account = account
        if self.status == 0:
            buf = ctypes.create_string_buffer(self.payload)
            self._bufs.append(buf)
            out_len._obj.value = len(self.payload)
            out_ptr._obj.value = ctypes.addressof(buf)
        return self.status

    def SecKeychainItemFreeContent(self, attrs, pointer):
        self.freed.append(pointer)
        return 0

    def SecKeychainAddGenericPassword(self, chain, slen, service, alen,
                                      account, plen, secret, item):
        self.added.append(ctypes.string_at(secret, plen))
        return self.add_status


class TestKeychain(unittest.TestCase):
    def setUp(self):
        # tests must simulate darwin so no real framework load is needed
        p = mock.patch('sys.platform', 'darwin')
        self.addCleanup(p.stop)
        p.start()

    def store(self, lib=None):
        return MacOSKeychain(_library=lib or FakeSecurity())

    def test_find_called_with_eight_args_none_keychain(self):
        seen = []

        @FIND_T
        def find(keychain, slen, service, alen, account, out_len,
                 out_ptr, item):
            seen.append((keychain, service, account))
            buf = ctypes.create_string_buffer(b'k' * 32)
            find._buf = buf
            out_len.contents.value = 32
            out_ptr.contents.value = ctypes.addressof(buf)
            return 0

        lib = FakeSecurity()
        lib.SecKeychainFindGenericPassword = find
        secret = self.store(lib).load(ACCOUNT)
        self.assertEqual(secret, b'k' * 32)
        self.assertEqual(seen, [(None, b'ai.fusion-codex-relay.continuation',
                               ACCOUNT.encode())])
        self.assertEqual(len(lib.freed), 1)

    def test_argtypes_declared(self):
        lib = _configure(mock.Mock())
        self.assertEqual(
            len(lib.SecKeychainFindGenericPassword.argtypes), 8)
        self.assertEqual(
            lib.SecKeychainFindGenericPassword.argtypes[0],
            ctypes.c_void_p)
        self.assertEqual(
            lib.SecKeychainAddGenericPassword.argtypes[6],
            ctypes.c_void_p)
        self.assertEqual(
            lib.SecKeychainFindGenericPassword.restype, ctypes.c_int32)

    def test_load_returns_secret_and_frees(self):
        lib = FakeSecurity()
        secret = self.store(lib).load(ACCOUNT)
        self.assertEqual(secret, b'p' * 32)
        self.assertEqual(len(lib.freed), 1)
        self.assertEqual(lib.keychain_args, [None])

    def test_non_darwin_rejected_before_load(self):
        with mock.patch('sys.platform', 'linux'):
            with self.assertRaisesRegex(ContinuationError,
                                        'OS key store unavailable'):
                MacOSKeychain()
            lib = FakeSecurity()
            with self.assertRaisesRegex(ContinuationError,
                                        'OS key store unavailable'):
                MacOSKeychain(_library=lib).load(ACCOUNT)
            self.assertEqual(lib.finds, 0)

    def test_framework_load_failure(self):
        with mock.patch('ctypes.CDLL', side_effect=OSError('no fwk')):
            with self.assertRaisesRegex(ContinuationError,
                                        'OS key store unavailable'):
                MacOSKeychain()

    def test_invalid_account_ref_rejected(self):
        lib = FakeSecurity()
        for ref in ('short', 'g' * 64, 'A' * 64, '', None):
            with self.assertRaises(ContinuationError):
                self.store(lib).load(ref)
        self.assertEqual(lib.finds, 0)

    def test_wrong_length_rejected(self):
        lib = FakeSecurity()
        lib.payload = b'too-short'
        with self.assertRaisesRegex(ContinuationError,
                                    'record invalid'):
            self.store(lib).load(ACCOUNT)
        self.assertEqual(len(lib.freed), 1)

    def test_missing_without_create_raises(self):
        lib = FakeSecurity()
        lib.status = -25300
        with self.assertRaisesRegex(ContinuationError, 'missing'):
            self.store(lib).load(ACCOUNT)
        self.assertEqual(lib.added, [])

    def test_create_generates_and_adds(self):
        lib = FakeSecurity()
        lib.status = -25300
        secret = self.store(lib).load(ACCOUNT, create=True)
        self.assertEqual(len(secret), 32)
        self.assertEqual(len(lib.added), 1)
        self.assertEqual(lib.added[0], secret)

    def test_duplicate_race_reloads(self):
        lib = FakeSecurity()
        lib.status = -25300
        lib.add_status = -25299
        stored = b's' * 32

        def find(keychain, slen, service, alen, account, out_len,
                 out_ptr, item):
            lib.finds += 1
            if lib.added:
                buf = ctypes.create_string_buffer(stored)
                lib._bufs.append(buf)
                out_len._obj.value = 32
                out_ptr._obj.value = ctypes.addressof(buf)
                return 0
            return -25300

        lib.SecKeychainFindGenericPassword = find
        secret = self.store(lib).load(ACCOUNT, create=True)
        self.assertEqual(secret, stored)  # raced value wins, no overwrite
        self.assertEqual(len(lib.added), 1)

    def test_denied_and_other_errors(self):
        lib = FakeSecurity()
        for status, message in (
                (-128, 'OS key store error'), (1, 'OS key store error'),
                (-25293, 'OS key store access denied'),
                (-25308, 'OS key store access denied'),
                (-25295, 'OS key store unavailable'),
                (-25291, 'OS key store unavailable')):
            lib.status = status
            with self.assertRaisesRegex(ContinuationError, message):
                self.store(lib).load(ACCOUNT)

    def test_delete(self):
        calls = []
        released = []

        class Lib(FakeSecurity):
            def SecKeychainFindGenericPassword(self, keychain, slen,
                                               service, alen, account,
                                               out_len, out_ptr, item):
                self.finds += 1
                buf = ctypes.create_string_buffer(self.payload)
                self._bufs.append(buf)
                out_len._obj.value = len(self.payload)
                out_ptr._obj.value = ctypes.addressof(buf)
                item._obj.value = 0xDEADBEEF
                return 0

            def SecKeychainItemDelete(self, item):
                calls.append(item)
                return 0

        lib = Lib()
        store = self.store(lib)
        with mock.patch.object(store, '_cfrelease',
                               side_effect=released.append) as release:
            store.delete(ACCOUNT)
        self.assertEqual(len(calls), 1)
        release.assert_called_once_with(0xDEADBEEF)
        # the found data buffer is freed too
        self.assertEqual(len(lib.freed), 1)

    def test_delete_failure(self):
        class Lib(FakeSecurity):
            def SecKeychainFindGenericPassword(self, *a):
                a[7]._obj.value = 1
                return 0

            def SecKeychainItemDelete(self, item):
                return -25300

        lib = Lib()
        store = self.store(lib)
        with mock.patch.object(store, '_cfrelease'):
            with self.assertRaisesRegex(ContinuationError,
                                        'OS key store error'):
                store.delete(ACCOUNT)

    def test_delete_missing(self):
        lib = FakeSecurity()
        lib.status = -25300
        with self.assertRaisesRegex(ContinuationError, 'missing'):
            self.store(lib).delete(ACCOUNT)

    def test_delete_error_mapping(self):
        lib = FakeSecurity()
        lib.status = -25293
        with self.assertRaisesRegex(ContinuationError,
                                    'access denied'):
            self.store(lib).delete(ACCOUNT)

    def test_delete_invalid_ref_and_non_darwin(self):
        lib = FakeSecurity()
        for ref in ('short', '', None):
            with self.assertRaises(ContinuationError):
                self.store(lib).delete(ref)
        self.assertEqual(lib.finds, 0)
        with mock.patch('sys.platform', 'linux'):
            with self.assertRaisesRegex(ContinuationError,
                                        'unavailable'):
                self.store(lib).delete(ACCOUNT)

    def test_add_failure_raises(self):
        lib = FakeSecurity()
        lib.status = -25300
        lib.add_status = -25291
        with self.assertRaisesRegex(ContinuationError,
                                    'OS key store error'):
            self.store(lib).load(ACCOUNT, create=True)

    def test_secret_with_nul_bytes_stored_fully(self):
        lib = FakeSecurity()
        lib.status = -25300
        nul_secret = b'\x00head' + b'\x00' * 20 + b'tail\x00\x00\x00'
        with mock.patch('secrets.token_bytes', return_value=nul_secret):
            secret = self.store(lib).load(ACCOUNT, create=True)
        self.assertEqual(secret, nul_secret)
        self.assertEqual(lib.added[0], nul_secret)

    def test_no_subprocess_used(self):
        lib = FakeSecurity()
        with mock.patch('subprocess.run',
                        side_effect=AssertionError('no security CLI')):
            self.store(lib).load(ACCOUNT)


if __name__ == '__main__':
    unittest.main()
