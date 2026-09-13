"""ChatGPT subscription auth for the Codex backend.

Reads the canonical ``~/.codex/auth.json`` written by ``codex login``. When the
access token is expired (or nearly), refreshes it via OpenAI's OAuth token
endpoint and writes the result back to the same file — the canonical token
store, not a copy — under an flock so concurrent refreshes serialize.

The refresh request uses Codex CLI's public OAuth client id, matching what the
CLI itself sends.
"""

from __future__ import annotations

import base64
import fcntl
import json
import os
import pathlib
import time
import urllib.error
import urllib.request

CODEX_AUTH = pathlib.Path(os.environ.get("CODEX_HOME", pathlib.Path.home() / ".codex")) / "auth.json"
LOCK_PATH = CODEX_AUTH.with_suffix(".json.relay-lock")
TOKEN_URL = "https://auth.openai.com/oauth/token"
CLIENT_ID = "app_69a1d78e929881919bba0dbda1f6436d"
REFRESH_LEEWAY = 120  # refresh if the token expires within this many seconds


class AuthError(RuntimeError):
    """Raised when no usable ChatGPT credentials are available."""


def _jwt_expiry(token: str) -> float:
    """Return the exp claim of a JWT without verifying the signature."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return 0.0


def _load() -> dict:
    try:
        data = json.loads(CODEX_AUTH.read_text())
    except FileNotFoundError:
        raise AuthError(f"{CODEX_AUTH} not found — run `codex login` first")
    if data.get("auth_mode") != "chatgpt" or "tokens" not in data:
        raise AuthError("auth.json is not a ChatGPT login (auth_mode != chatgpt)")
    return data


def _refresh(tokens: dict) -> dict:
    """Exchange the refresh token for a new token set."""
    body = json.dumps({
        "client_id": CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
    }).encode()
    req = urllib.request.Request(
        TOKEN_URL, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            out = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise AuthError(f"token refresh failed: HTTP {e.code}")
    merged = dict(tokens)
    for key in ("access_token", "refresh_token", "id_token"):
        if out.get(key):
            merged[key] = out[key]
    return merged


def get_token() -> tuple[str, str]:
    """Return ``(access_token, account_id)``, refreshing if needed.

    The lock is held across check-refresh-write so two relay workers can't
    race a rotation; if the file's refresh token changed while we waited
    (Codex CLI refreshed first), we just use the fresh file contents.
    """
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK_PATH, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        data = _load()
        tokens = data["tokens"]
        exp = _jwt_expiry(tokens.get("access_token", ""))
        if exp and exp - REFRESH_LEEWAY > time.time():
            return tokens["access_token"], tokens["account_id"]
        tokens = _refresh(tokens)
        data["tokens"] = tokens
        data["last_refresh"] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        tmp = CODEX_AUTH.with_suffix(".json.relay-tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.chmod(tmp, 0o600)
        os.replace(tmp, CODEX_AUTH)
        return tokens["access_token"], tokens["account_id"]
