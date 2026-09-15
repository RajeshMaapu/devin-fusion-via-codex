"""serve() must turn SIGTERM into a clean shutdown so the accounting run
is closed (launchd stops the service with SIGTERM)."""
from __future__ import annotations

import http.client
import os
import pathlib
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent

_DRIVER = """
import sys
sys.path.insert(0, %r)
from fusion_relay import relay
relay.auth.get_token = lambda: ('tok', 'acct')
relay.serve(int(sys.argv[1]))
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class GracefulShutdownTest(unittest.TestCase):
    def test_sigterm_closes_accounting_run_clean(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        data_dir = pathlib.Path(tmp.name).resolve() / "data"
        driver = pathlib.Path(tmp.name) / "driver.py"
        driver.write_text(_DRIVER % str(REPO))
        port = _free_port()
        env = dict(os.environ, FUSION_RELAY_DATA_DIR=str(data_dir),
                   PYTHONDONTWRITEBYTECODE="1")
        proc = subprocess.Popen(
            [sys.executable, str(driver), str(port)], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 15
            up = False
            while time.monotonic() < deadline:
                try:
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", port, timeout=1)
                    conn.request("GET", "/healthz")
                    conn.getresponse().read()
                    conn.close()
                    up = True
                    break
                except OSError:
                    time.sleep(0.1)
            self.assertTrue(up, "relay did not come up")
            self.assertEqual(sqlite3.connect(
                str(data_dir / "accounting.sqlite3")).execute(
                "SELECT COUNT(*) FROM accounting_runs WHERE closed=0"
            ).fetchone()[0], 1)
            proc.send_signal(signal.SIGTERM)
            out, err = proc.communicate(timeout=15)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        self.assertEqual(proc.returncode, 0, err.decode(errors="replace"))
        self.assertIn(b"shutting down (signal 15)", err)
        self.assertEqual(sqlite3.connect(
            str(data_dir / "accounting.sqlite3")).execute(
            "SELECT COUNT(*) FROM accounting_runs WHERE closed=0"
        ).fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
