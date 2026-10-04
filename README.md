# stderr-rate-limiter

A `logging.Handler` wrapper that enforces a token-bucket rate limit so noisy error streams do not flood stderr during incidents.

## Usage

```python
import logging
from stderr_rate_limiter import RateLimitedHandler

stderr_handler = logging.StreamHandler()
h = RateLimitedHandler(
    stderr_handler,
    rate=5.0,        # 5 messages per second sustained
    capacity=20,     # allow an initial burst of 20
    min_level=logging.CRITICAL,  # CRITICAL and above bypass the limiter
)
logger = logging.getLogger("app")
logger.addHandler(h)
logger.error("something failed")
```

## Why this exists

During an incident a single hot loop can emit hundreds of near-identical error lines per second, pushing the one useful line off the screen. A token bucket allows a steady trickle plus a bounded burst, then drops the overflow. This library wraps an existing handler rather than replacing it, so the underlying formatting, filters, and stream remain under your control.

The trade-off: dropped messages are silently discarded. We deliberately do not emit a "N messages suppressed" summary — that summary can itself become noisy during an incident, and ordering it correctly against concurrent handlers is a problem this library does not try to solve. If you need a drop indicator, subclass `RateLimitedHandler` and override `on_drop(record)`.

## Edge cases worth knowing

- The bucket starts full, so the first `capacity` messages in a burst pass through. This matches operator expectations at process start but means a restart immediately after an incident can re-flood stderr for up to `capacity` messages.
- The clock is injectable (`clock=time.monotonic` by default). `time.monotonic` is used because only durations are compared, never absolute time, so system clock jumps cannot break the limiter.
- `min_level` lets high-severity records bypass the limiter entirely. Without it, a CRITICAL message can be dropped during a flood — usually the opposite of what you want.

## Exported names

- `RateLimitedHandler(wrapped, rate, capacity, *, clock=time.monotonic, min_level=None)` — the only public class.

Methods you will call: `emit`, `flush`, `close`, plus the `on_drop(record)` override hook. The `tokens` property and `dropped` counter are exposed for observability.

## Running the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```
