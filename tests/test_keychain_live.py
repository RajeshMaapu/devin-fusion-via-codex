"""Opt-in live Keychain test — mutates the login Keychain under a
test-only service namespace. Skipped unless
FUSION_RELAY_KEYCHAIN_LIVE=1 on darwin. Never run by default.

If FUSION_RELAY_KEYCHAIN_LIVE_REPORT is set, platform/interpreter
details are written there as JSON.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import platform
import secrets
import subprocess
import sys
import tempfile
import threading
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fusion_relay.continuation import (ContinuationError,
                                       ContinuationLedger)

_LIVE = os.environ.get('FUSION_RELAY_KEYCHAIN_LIVE') == '1' \
    and sys.platform == 'darwin'
_SKIP = ('opt-in: set FUSION_RELAY_KEYCHAIN_LIVE=1 (mutates the login '
         'Keychain under a test-only service namespace)')


@unittest.skipUnless(_LIVE, _SKIP)
class TestLiveKeychain(unittest.TestCase):
    def setUp(self):
        from fusion_relay.keychain import MacOSKeychain
        self.service = 'ai.fusion-codex-relay.test.' + secrets.token_hex(8)
        self.account = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
        self.kc = MacOSKeychain(service=self.service)
        self.addCleanup(self._cleanup)
        report = os.environ.get('FUSION_RELAY_KEYCHAIN_LIVE_REPORT')
        if report:
            def _write():
                with open(report, 'a') as f:
                    f.write(json.dumps({
                        'service': self.service,
                        'mac': platform.mac_ver(),
                        'python': sys.executable}) + '\n')
            self.addCleanup(_write)

    def _cleanup(self):
        try:
            self.kc.delete(self.account)
        except Exception:
            pass

    def test_create_load_subprocess_decrypt_and_delete(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        # /var -> /private/var on macOS; PrivateDirectory refuses symlinks
        path = pathlib.Path(tmp.name).resolve() / 'c.sqlite3'
        key = self.kc.load(self.account, create=True)
        self.assertEqual(len(key), 32)
        led = ContinuationLedger(path, key)
        led.close()
        # a separate process loads the same key and opens the store
        script = (
            "import sys,pathlib\n"
            "sys.path.insert(0,%r)\n"
            "import os\n"
            "from fusion_relay.keychain import MacOSKeychain\n"
            "from fusion_relay.continuation import ContinuationLedger\n"
            "kc=MacOSKeychain(service=os.environ['T_SVC'])\n"
            "key=kc.load(os.environ['T_ACCT'])\n"
            "led=ContinuationLedger(pathlib.Path(sys.argv[1]),key)\n"
            "led.close()\n"
            "print('ok')\n" % str(REPO))
        proc = subprocess.run(
            [sys.executable, '-c', script, str(path)],
            env={'T_SVC': self.service, 'T_ACCT': self.account,
                 'PATH': os.environ.get('PATH', '')},
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('ok', proc.stdout)
        # missing item -> explicit error
        missing = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
        with self.assertRaisesRegex(ContinuationError, 'missing'):
            self.kc.load(missing)
        # create=False on a fresh account fails
        fresh = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
        with self.assertRaises(ContinuationError):
            self.kc.load(fresh, create=False)
        # duplicate concurrent creation returns the same key
        results = []
        other = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
        threads = [threading.Thread(
            target=lambda: results.append(
                self.kc.load(other, create=True))) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        try:
            self.kc.delete(other)
        except Exception:
            pass
        # delete removes the key
        self.kc.delete(self.account)
        with self.assertRaisesRegex(ContinuationError, 'missing'):
            self.kc.load(self.account)


if __name__ == '__main__':
    unittest.main()
