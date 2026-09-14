"""Request lifecycle cancellation for the relay.

This is transport/request lifecycle only — not an agent-role or
authorization signal. A :class:`RequestContext` carries a monotonic
deadline plus an optional disconnect probe; ``check()`` is invoked at
every boundary (before inference, per stream event, before and after
tool dispatch). The upstream HTTP read itself stays timeout-bounded:
cancellation is observed at read boundaries, not mid-socket, for this
transport.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional


class RequestCancelled(RuntimeError):
    """The request was cancelled, timed out, or its client went away."""


class RequestContext:
    """Cancellation token + monotonic deadline for one request."""

    def __init__(self, timeout: float = 300,
                 disconnected: Optional[Callable[[], bool]] = None) -> None:
        self._event = threading.Event()
        self._deadline = time.monotonic() + timeout
        self._disconnected = disconnected

    def cancel(self) -> None:
        self._event.set()

    def check(self) -> None:
        if (self._event.is_set() or time.monotonic() >= self._deadline
                or (self._disconnected is not None
                    and self._disconnected())):
            raise RequestCancelled("request_cancelled")
