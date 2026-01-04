"""
Async rate limiter for SEC API requests.

SEC enforces a 10 requests/second limit, but in practice 5 req/sec is safer.
This module provides a proactive token-bucket style rate limiter that prevents
hitting rate limits rather than reacting to them.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class RateStats:
  """Statistics from rate monitoring."""

  requests_per_second: float
  mb_per_second: float
  total_requests: int
  total_bytes: int


class AsyncRateLimiter:
  """
  Token-bucket style async rate limiter.

  Usage:
      limiter = AsyncRateLimiter(rate=5.0)

      async with limiter:
          await fetch_something()

  Or:
      await limiter.acquire()
      await fetch_something()
  """

  def __init__(self, rate: float = 5.0, interval: float = 1.0):
    """
    Initialize rate limiter.

    Args:
        rate: Maximum requests per interval (default: 5.0)
        interval: Time window in seconds (default: 1.0)
    """
    self.rate = rate
    self.interval = interval
    self.last_request = 0.0
    self._lock = asyncio.Lock()

  async def acquire(self) -> None:
    """Acquire a rate limit token, waiting if necessary."""
    async with self._lock:
      token_time = self.interval / self.rate
      now = time.monotonic()
      wait_time = max(0.0, self.last_request + token_time - now)
      if wait_time > 0:
        await asyncio.sleep(wait_time)
      self.last_request = time.monotonic()

  async def __aenter__(self):
    await self.acquire()
    return self

  async def __aexit__(self, *args):
    pass


@dataclass
class RateMonitor:
  """
  Monitor request rates and bandwidth.

  Tracks requests over a sliding window to calculate current rates.
  """

  window_size: float = 10.0
  _requests: deque = field(default_factory=deque)
  _total_requests: int = 0
  _total_bytes: int = 0
  _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

  async def record(self, bytes_transferred: int = 0) -> None:
    """Record a request with optional bytes transferred."""
    async with self._lock:
      now = time.monotonic()
      self._requests.append((now, bytes_transferred))
      self._total_requests += 1
      self._total_bytes += bytes_transferred

      # Prune old entries outside the window
      while self._requests and now - self._requests[0][0] > self.window_size:
        self._requests.popleft()

  def get_stats(self) -> RateStats:
    """Get current rate statistics."""
    if not self._requests:
      return RateStats(
        requests_per_second=0.0,
        mb_per_second=0.0,
        total_requests=self._total_requests,
        total_bytes=self._total_bytes,
      )

    now = time.monotonic()
    # Filter to requests within window
    window_requests = [(t, b) for t, b in self._requests if now - t <= self.window_size]

    if len(window_requests) < 2:
      return RateStats(
        requests_per_second=0.0,
        mb_per_second=0.0,
        total_requests=self._total_requests,
        total_bytes=self._total_bytes,
      )

    elapsed = now - window_requests[0][0]
    if elapsed <= 0:
      return RateStats(
        requests_per_second=0.0,
        mb_per_second=0.0,
        total_requests=self._total_requests,
        total_bytes=self._total_bytes,
      )

    req_per_sec = len(window_requests) / elapsed
    bytes_per_sec = sum(b for _, b in window_requests) / elapsed
    mb_per_sec = bytes_per_sec / (1024 * 1024)

    return RateStats(
      requests_per_second=round(req_per_sec, 2),
      mb_per_second=round(mb_per_sec, 3),
      total_requests=self._total_requests,
      total_bytes=self._total_bytes,
    )
