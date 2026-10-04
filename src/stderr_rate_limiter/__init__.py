"""Public package interface for stderr_rate_limiter.

Only `RateLimitedHandler` is part of the supported API. Keeping the surface
small means callers cannot accidentally depend on internals that may change
between releases.
"""

from .core import RateLimitedHandler

__all__ = ["RateLimitedHandler"]
