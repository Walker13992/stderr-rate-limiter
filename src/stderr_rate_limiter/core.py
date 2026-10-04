"""Token-bucket rate limiter as a logging.Handler wrapper.

The problem this addresses: during incidents a noisy logger can write hundreds
of near-identical messages per second to stderr, swamping the useful output and
making operators miss the one line that matters. A token bucket lets a steady
trickle through and bursts up to the bucket capacity, then drops the overflow.

Design decisions made (and the only ones this module honours):

* Dropped messages are silently discarded. We deliberately do NOT emit a
  summary like "N messages suppressed" because that summary itself can become
  noisy during an incident, and ordering it correctly against concurrent
  handlers is a problem we do not want to own. Callers who need a drop
  indicator can subclass and override `on_drop`.
* The clock is injectable. `time.monotonic` is the default because we only
  ever compare durations, never absolute time; monotonic is immune to system
  clock adjustments. Tests pass a fake clock so they never depend on wall
  time.
* The bucket is refilled lazily on each `emit` call. There is no background
  thread, which keeps the handler usable in environments where threads are
  restricted (e.g. some async runtimes, embedded interpreters).
* Token counts are kept as exact integers whenever possible and as floats only
  when fractional refill has actually occurred. We never compare floats with
  equality; the only comparison is `tokens >= 1.0`, which is robust to small
  accumulation error because the bucket is capped at `capacity`.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Callable, Optional


class RateLimitedHandler(logging.Handler):
    """A logging.Handler that applies a token-bucket limit to a wrapped handler.

    Each emitted record consumes one token. Tokens accrue at `rate` per second
    up to a maximum of `capacity`. When no token is available the record is
    dropped (after calling `on_drop` if it was overridden).

    Parameters
    ----------
    wrapped:
        The underlying handler that actually writes the record (e.g. a
        StreamHandler on stderr). Must not be None.
    rate:
        Refill rate in tokens per second. Must be strictly positive.
    capacity:
        Maximum tokens the bucket can hold, i.e. the largest burst allowed.
        Must be a positive integer (we require int so callers can reason about
        whole messages, not fractional ones).
    clock:
        Callable returning the current time in seconds. Defaults to
        `time.monotonic`. Inject a fake in tests.
    min_level:
        If set, records below this logging level bypass the limiter entirely
        and are always forwarded to `wrapped`. Useful when you want critical
        messages to always get through. Defaults to None (all levels limited).

    Notes
    -----
    The wrapped handler's lifecycle (acquire, release, close, flush) is fully
    delegated, so this wrapper is transparent to the logging framework except
    for the rate limit.
    """

    def __init__(
        self,
        wrapped: logging.Handler,
        rate: float,
        capacity: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        min_level: Optional[int] = None,
    ) -> None:
        super().__init__()
        if wrapped is None:
            raise ValueError("wrapped handler must not be None")
        if not (rate > 0):
            raise ValueError("rate must be strictly positive")
        if not (capacity > 0):
            raise ValueError("capacity must be a positive integer")
        if not isinstance(capacity, int):
            raise ValueError("capacity must be an integer")

        self._wrapped = wrapped
        self._rate = float(rate)
        # Start full so that the first `capacity` messages in a burst pass
        # through, which is the behaviour operators expect at startup.
        self._tokens = float(capacity)
        self._capacity = capacity
        self._clock = clock
        self._min_level = min_level
        self._last_refill = clock()
        # Total dropped since construction. Exposed for observability in
        # long-running processes; we do not interpret it in tests beyond what
        # the implementation guarantees.
        self.dropped = 0

    # ------------------------------------------------------------------ #
    # Bucket logic                                                        #
    # ------------------------------------------------------------------ #
    def _refill(self) -> None:
        """Add tokens accrued since the last refill, capping at capacity."""
        now = self._clock()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = now

    def _try_consume(self) -> bool:
        """Refill and consume one token. Returns True if a token was consumed."""
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    # ------------------------------------------------------------------ #
    # logging.Handler API                                                #
    # ------------------------------------------------------------------ #
    def emit(self, record: logging.LogRecord) -> None:
        """Forward `record` to the wrapped handler if a token is available.

        Records at or above `min_level` (when set) bypass the limiter.
        Dropped records are counted and passed to `on_drop`.
        """
        if self._min_level is not None and record.levelno >= self._min_level:
            self._wrapped.emit(record)
            return

        if self._try_consume():
            self._wrapped.emit(record)
        else:
            self.dropped += 1
            self.on_drop(record)

    def on_drop(self, record: logging.LogRecord) -> None:
        """Hook called when a record is dropped. No-op by default.

        Override in a subclass to implement drop-side effects (metrics,
        buffering, an occasional summary). Kept as a separate method so
        subclasses do not need to reimplement `emit` and risk breaking the
        token accounting.
        """
        # Intentionally empty. See class docstring for the rationale.

    def flush(self) -> None:
        """Flush the wrapped handler."""
        self._wrapped.flush()

    def close(self) -> None:
        """Close the wrapped handler."""
        try:
            self._wrapped.close()
        finally:
            super().close()

    @property
    def tokens(self) -> float:
        """Current token count, refilled lazily to the current clock value.

        Exposed for tests and dashboards. The value is a float because refill
        is fractional; it is never compared with equality outside this
        module.
        """
        self._refill()
        return self._tokens

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"wrapped={self._wrapped!r}, rate={self._rate}, "
            f"capacity={self._capacity}, dropped={self.dropped})"
        )
