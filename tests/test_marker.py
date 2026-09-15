"""UserPromptSubmit marker: render/verify, extraction from packets, and
the hook script end-to-end (offline)."""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fusion_relay import marker, wire
from fusion_relay.accounting import reference

REPO = pathlib.Path(__file__).resolve().parent.parent
SECRET = b"m" * 32


def _msg(source, body):
    return wire.field(3, wire.field(2, source) + wire.field(3, body))


def _packet(*msgs):
    return wire.decode(b"".join(msgs) + wire.field(16, "seed")
                       + wire.field(21, "gpt-6-astra-high"))


class MarkerTest(unittest.TestCase):
    def test_render_and_extract_last_verified(self):
        m1 = marker.render(SECRET, "frost-dust", "11111111-aaaa")
        m2 = marker.render(SECRET, "frost-dust", "22222222-bbbb")
        pk = _packet(_msg(1, "hello"), _msg(1, "ctx " + m1),
                     _msg(2, "answer"), _msg(1, m2 + " trailing"))
        res = marker.extract(pk, SECRET)
        self.assertEqual(res.status, "verified")
        self.assertEqual(res.session_name_ref,
                         reference("session", "frost-dust"))
        self.assertEqual(res.prompt_id, "22222222-bbbb")

    def test_absent_and_invalid(self):
        self.assertEqual(marker.extract(_packet(_msg(1, "hi")),
                                        SECRET).status, "absent")
        m = marker.render(SECRET, "s", "12345678")
        # wrong key -> invalid
        self.assertEqual(marker.extract(_packet(_msg(1, m)),
                                        b"x" * 32).status, "invalid")
        # tampered prompt -> invalid
        parts = m.split(" ")
        parts[3] = "deadbeef"
        self.assertEqual(marker.extract(
            _packet(_msg(1, " ".join(parts))), SECRET).status, "invalid")
        # marker only in an assistant message is ignored (user-source only)
        self.assertEqual(marker.extract(_packet(_msg(2, m)),
                                        SECRET).status, "absent")
        # no secret available -> invalid, never verified
        self.assertEqual(marker.extract(_packet(_msg(1, m)),
                                        None).status, "invalid")

    def test_render_rejects_unusable_values(self):
        with self.assertRaises(ValueError):
            marker.render(b"short", "s", "12345678")
        with self.assertRaises(ValueError):
            marker.render(SECRET, "bad name!", "12345678")
        with self.assertRaises(ValueError):
            marker.render(SECRET, "s", "zz")

    def test_hook_script_emits_verifiable_marker_only(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        data_dir = pathlib.Path(tmp.name).resolve() / "data"
        data_dir.mkdir(mode=0o700)
        key_path = data_dir / "identity.key"
        key_path.write_bytes(SECRET)
        key_path.chmod(0o600)
        payload = {"hook_event_name": "UserPromptSubmit",
                   "session_id": "frost-dust",
                   "prompt_id": "67bba5e1-fea2-4d98-a5b9-bb4cbd87c239",
                   "prompt": "SECRET USER TEXT MUST NOT ECHO"}
        env = dict(os.environ, FUSION_RELAY_DATA_DIR=str(data_dir),
                   PYTHONDONTWRITEBYTECODE="1")
        proc = subprocess.run(
            [sys.executable, str(REPO / "bin" / "fusion-prompt-marker")],
            input=json.dumps(payload).encode(), env=env,
            capture_output=True, timeout=20)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        text = out["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(out["hookSpecificOutput"]["hookEventName"],
                         "UserPromptSubmit")
        self.assertNotIn("SECRET USER TEXT", proc.stdout.decode())
        res = marker.extract(_packet(_msg(1, text)), SECRET)
        self.assertEqual(res.status, "verified")
        self.assertEqual(res.prompt_id, payload["prompt_id"])
        # missing key -> silent, exit 0, no output (never blocks the CLI)
        key_path.unlink()
        proc = subprocess.run(
            [sys.executable, str(REPO / "bin" / "fusion-prompt-marker")],
            input=json.dumps(payload).encode(), env=env,
            capture_output=True, timeout=20)
        self.assertEqual((proc.returncode, proc.stdout), (0, b""))


if __name__ == "__main__":
    unittest.main()
