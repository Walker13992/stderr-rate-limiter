"""Tests for RateLimitedHandler.

Every test injects a fake clock so behaviour is deterministic and independent
of wall time. No test sleeps and no test compares floats with equality except
via the explicit token-count assertions where the value is an integer stored
as a float.
"""

import io
import logging
import unittest

from stderr_rate_limiter import RateLimitedHandler


class _FakeClock:
    """A controllable monotonic clock for tests.

    `now` advances on each call by `step`; `advance` jumps it explicitly.
    """

    def __init__(self, start: float = 0.0, step: float = 0.0) -> None:
        self._t = float(start)
        self._step = float(step)

    def __call__(self) -> float:
        self._t += self._step
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += float(seconds)


def _make_record(msg: str = "msg", level: int = logging.ERROR) -> logging.LogRecord:
    """Build a minimal LogRecord without going through the logging framework."""
    return logging.LogRecord(
        name="test", level=level, pathname=__file__, lineno=1,
        msg=msg, args=None, exc_info=None,
    )


def _stream_handler() -> logging.StreamHandler:
    """A StreamHandler on a fresh StringIO for capture."""
    return logging.StreamHandler(io.StringIO())


class TestConstruction(unittest.TestCase):
    def test_rejects_none_wrapped(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(None, rate=1.0, capacity=1)

    def test_rejects_zero_rate(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_stream_handler(), rate=0.0, capacity=1)

    def test_rejects_negative_rate(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_stream_handler(), rate=-1.0, capacity=1)

    def test_rejects_zero_capacity(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_stream_handler(), rate=1.0, capacity=0)

    def test_rejects_non_integer_capacity(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_stream_handler(), rate=1.0, capacity=1.5)

    def test_starts_full(self):
        clock = _FakeClock()
        h = RateLimitedHandler(_stream_handler(), rate=1.0, capacity=5, clock=clock)
        # Starts at capacity. tokens is refilled lazily but no time has passed.
        self.assertEqual(h.tokens, 5.0)


class TestBurstAndRefill(unittest.TestCase):
    def test_initial_burst_passes_then_drops(self):
        # Capacity 3 means the first 3 messages pass; the 4th is dropped.
        clock = _FakeClock()
        emitted = []
        wrapped = logging.Handler()
        wrapped.emit = lambda r: emitted.append(r)  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=3, clock=clock)

        results = [h._try_consume() for _ in range(4)]
        self.assertEqual(results, [True, True, True, False])
        self.assertEqual(len(emitted), 0)  # _try_consume does not emit
        self.assertEqual(h.dropped, 0)
        self.assertEqual(h.tokens, 0.0)

    def test_refill_after_time(self):
        # rate=2 tokens/sec; consume all, advance 1s, expect 2 available.
        clock = _FakeClock()
        h = RateLimitedHandler(_stream_handler(), rate=2.0, capacity=2, clock=clock)
        self.assertTrue(h._try_consume())
        self.assertTrue(h._try_consume())
        self.assertFalse(h._try_consume())
        clock.advance(1.0)
        self.assertTrue(h._try_consume())
        self.assertTrue(h._try_consume())
        self.assertFalse(h._try_consume())

    def test_refill_caps_at_capacity(self):
        # Even after a long idle period the bucket never exceeds capacity.
        clock = _FakeClock()
        h = RateLimitedHandler(_stream_handler(), rate=10.0, capacity=3, clock=clock)
        clock.advance(1000.0)
        self.assertEqual(h.tokens, 3.0)

    def test_fractional_refill_accumulates(self):
        # rate=1, advance 0.5s twice => 1 token available.
        clock = _FakeClock()
        h = RateLimitedHandler(_stream_handler(), rate=1.0, capacity=1, clock=clock)
        self.assertTrue(h._try_consume())
        self.assertFalse(h._try_consume())
        clock.advance(0.5)
        self.assertFalse(h._try_consume())
        clock.advance(0.5)
        self.assertTrue(h._try_consume())


class TestEmit(unittest.TestCase):
    def test_emit_forwards_when_token_available(self):
        clock = _FakeClock()
        emitted = []
        wrapped = logging.Handler()
        wrapped.emit = lambda r: emitted.append(r)  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=1, clock=clock)
        rec = _make_record("hello")
        h.emit(rec)
        self.assertIs(emitted[0], rec)
        self.assertEqual(h.dropped, 0)

    def test_emit_drops_when_no_token(self):
        clock = _FakeClock()
        emitted = []
        wrapped = logging.Handler()
        wrapped.emit = lambda r: emitted.append(r)  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=1, clock=clock)
        h.emit(_make_record("first"))   # passes
        h.emit(_make_record("second"))  # dropped
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].getMessage(), "first")
        self.assertEqual(h.dropped, 1)

    def test_min_level_bypasses_limiter(self):
        clock = _FakeClock()
        emitted = []
        wrapped = logging.Handler()
        wrapped.emit = lambda r: emitted.append(r)  # type: ignore[assignment]
        # Capacity 0 is not allowed, so use capacity 1 and exhaust it first.
        h = RateLimitedHandler(
            wrapped, rate=1.0, capacity=1, clock=clock, min_level=logging.CRITICAL
        )
        # Exhaust the bucket with an ERROR-level record.
        h.emit(_make_record("normal", level=logging.ERROR))
        # A CRITICAL record bypasses the limiter even though bucket is empty.
        crit = _make_record("critical", level=logging.CRITICAL)
        h.emit(crit)
        self.assertIn(crit, emitted)
        self.assertEqual(h.dropped, 0)

    def test_on_drop_called_with_record(self):
        clock = _FakeClock()
        wrapped = logging.Handler()
        wrapped.emit = lambda r: None  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=1, clock=clock)
        dropped_records = []
        h.on_drop = lambda r: dropped_records.append(r)  # type: ignore[assignment]
        h.emit(_make_record("first"))   # consumes token
        second = _make_record("second")
        h.emit(second)  # dropped
        self.assertEqual(dropped_records, [second])


class TestLifecycle(unittest.TestCase):
    def test_flush_delegates(self):
        clock = _FakeClock()
        flushed = []
        wrapped = _stream_handler()
        wrapped.flush = lambda: flushed.append(True)  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=1, clock=clock)
        h.flush()
        self.assertEqual(flushed, [True])

    def test_close_delegates(self):
        clock = _FakeClock()
        closed = []
        wrapped = _stream_handler()
        wrapped.close = lambda: closed.append(True)  # type: ignore[assignment]
        h = RateLimitedHandler(wrapped, rate=1.0, capacity=1, clock=clock)
        h.close()
        self.assertEqual(closed, [True])

    def test_repr_mentions_dropped(self):
        clock = _FakeClock()
        h = RateLimitedHandler(_stream_handler(), rate=1.0, capacity=1, clock=clock)
        h.emit(_make_record("first"))
        h.emit(_make_record("second"))  # dropped
        rep = repr(h)
        self.assertIn("dropped=1", rep)


if __name__ == "__main__":
    unittest.main()
