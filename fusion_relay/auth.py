"""ChatGPT subscription auth for the Codex backend — read-only.

Reads the canonical ``~/.codex/auth.json`` written by ``codex login``.
Credential refresh and file ownership belong to Codex, not this relay:
an expired or invalid login raises :class:`AuthError` telling the
operator to renew it in Codex. The relay never writes, locks, or
replaces the canonical file, so a local bug here cannot corrupt the
user's credentials.
"""

from __future__ import annotations

import base64
import json
import math
import os
import pathlib
import time

CODEX_AUTH = pathlib.Path(os.environ.get("CODEX_HOME", pathlib.Path.home() / ".codex")) / "auth.json"
REFRESH_LEEWAY = 120  # treat the token as expired this many seconds early


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
        raise AuthError("no Codex login found — run `codex login` first")
    except (OSError, ValueError):
        raise AuthError("Codex login is unreadable; renew the login in Codex")
    if not isinstance(data, dict) or data.get("auth_mode") != "chatgpt":
        raise AuthError("Codex login is not a ChatGPT login")
    return data


def _refresh(tokens: dict) -> dict:
    """Refresh is owned by Codex; the relay never writes credentials."""
    raise AuthError("credential refresh is owned by Codex; renew the "
                    "login in Codex")


def get_token() -> tuple[str, str]:
    """Return ``(access_token, account_id)`` from the canonical file.

    Read-only: no refresh, no lock, no write. An expired or malformed
    login fails closed with :class:`AuthError`.
    """
    data = _load()
    tokens = data.get("tokens")
    if not isinstance(tokens, dict):
        raise AuthError("Codex login has no token set")
    access = tokens.get("access_token")
    account = tokens.get("account_id")
    if not isinstance(access, str) or not access \
            or not isinstance(account, str) or not account:
        raise AuthError("Codex login is missing token fields")
    expiry = _jwt_expiry(access)
    if not math.isfinite(expiry) or expiry <= time.time() + REFRESH_LEEWAY:
        raise AuthError("Codex login expired or invalid; renew the "
                        "login in Codex")
    return access, account
