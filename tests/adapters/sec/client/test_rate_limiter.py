"""Tests for SEC async rate limiter."""

import asyncio
import time

import pytest

from robosystems.adapters.sec.client.rate_limiter import (
  AsyncRateLimiter,
  RateMonitor,
  RateStats,
)


class TestAsyncRateLimiter:
  """Tests for AsyncRateLimiter token-bucket implementation."""

  @pytest.mark.asyncio
  async def test_acquire_no_wait_on_first_request(self):
    """First request should not wait."""
    limiter = AsyncRateLimiter(rate=5.0)

    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start

    # Should be nearly instant (< 50ms)
    assert elapsed < 0.05

  @pytest.mark.asyncio
  async def test_acquire_rate_limits_subsequent_requests(self):
    """Subsequent requests should be rate limited."""
    limiter = AsyncRateLimiter(rate=10.0)  # 10 req/sec = 100ms between requests

    # First request
    await limiter.acquire()

    # Second request should wait ~100ms
    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start

    # Should wait approximately 100ms (give margin for timing variance)
    assert 0.08 < elapsed < 0.15

  @pytest.mark.asyncio
  async def test_context_manager_acquires(self):
    """Context manager should call acquire()."""
    limiter = AsyncRateLimiter(rate=5.0)

    # First use
    async with limiter:
      pass

    # Record time after first use
    start = time.monotonic()

    # Second use should wait
    async with limiter:
      pass

    elapsed = time.monotonic() - start

    # 5 req/sec = 200ms between requests
    assert 0.15 < elapsed < 0.25

  @pytest.mark.asyncio
  async def test_custom_interval(self):
    """Custom interval should work correctly."""
    # 2 requests per 0.5 second = 250ms between requests
    limiter = AsyncRateLimiter(rate=2.0, interval=0.5)

    await limiter.acquire()

    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start

    assert 0.20 < elapsed < 0.30

  @pytest.mark.asyncio
  async def test_concurrent_requests_serialize(self):
    """Concurrent requests should serialize properly."""
    limiter = AsyncRateLimiter(rate=10.0)  # 100ms between requests
    results = []

    async def make_request(idx: int):
      async with limiter:
        results.append((idx, time.monotonic()))

    start = time.monotonic()
    # Launch 3 concurrent requests
    await asyncio.gather(
      make_request(0),
      make_request(1),
      make_request(2),
    )
    total_elapsed = time.monotonic() - start

    # 3 requests at 10 req/sec should take ~200ms total
    assert len(results) == 3
    assert 0.15 < total_elapsed < 0.30


class TestRateMonitor:
  """Tests for RateMonitor sliding window statistics."""

  @pytest.mark.asyncio
  async def test_empty_stats(self):
    """Empty monitor should return zero stats."""
    monitor = RateMonitor()
    stats = monitor.get_stats()

    assert stats.requests_per_second == 0.0
    assert stats.mb_per_second == 0.0
    assert stats.total_requests == 0
    assert stats.total_bytes == 0

  @pytest.mark.asyncio
  async def test_single_request_stats(self):
    """Single request should return zero rate (need 2+ for rate calculation)."""
    monitor = RateMonitor()
    await monitor.record(bytes_transferred=1000)

    stats = monitor.get_stats()
    assert stats.total_requests == 1
    assert stats.total_bytes == 1000
    # Rate requires at least 2 data points
    assert stats.requests_per_second == 0.0

  @pytest.mark.asyncio
  async def test_multiple_requests_rate_calculation(self):
    """Multiple requests should calculate rate correctly."""
    monitor = RateMonitor(window_size=5.0)

    # Record 5 requests over ~0.5 seconds
    for i in range(5):
      await monitor.record(bytes_transferred=1024)
      await asyncio.sleep(0.1)

    stats = monitor.get_stats()

    # Should be approximately 10 req/sec
    assert 5.0 < stats.requests_per_second < 15.0
    assert stats.total_requests == 5
    assert stats.total_bytes == 5 * 1024

  @pytest.mark.asyncio
  async def test_bytes_tracking(self):
    """Bytes transferred should accumulate correctly."""
    monitor = RateMonitor()

    await monitor.record(bytes_transferred=1024)
    await asyncio.sleep(0.05)
    await monitor.record(bytes_transferred=2048)
    await asyncio.sleep(0.05)
    await monitor.record(bytes_transferred=4096)

    stats = monitor.get_stats()
    assert stats.total_bytes == 1024 + 2048 + 4096
    assert stats.total_requests == 3

  @pytest.mark.asyncio
  async def test_window_pruning(self):
    """Old requests outside window should be pruned."""
    monitor = RateMonitor(window_size=0.2)  # 200ms window

    # Record request
    await monitor.record(bytes_transferred=1000)
    assert monitor._total_requests == 1

    # Wait for it to fall outside window
    await asyncio.sleep(0.3)

    # Record another - should prune the first from the window (but not totals)
    await monitor.record(bytes_transferred=500)

    stats = monitor.get_stats()
    # Total should still include both
    assert stats.total_requests == 2
    assert stats.total_bytes == 1500
    # But rate calculation uses only requests in window


class TestRateStats:
  """Tests for RateStats dataclass."""

  def test_dataclass_fields(self):
    """RateStats should have expected fields."""
    stats = RateStats(
      requests_per_second=5.5,
      mb_per_second=1.25,
      total_requests=100,
      total_bytes=131072,
    )

    assert stats.requests_per_second == 5.5
    assert stats.mb_per_second == 1.25
    assert stats.total_requests == 100
    assert stats.total_bytes == 131072
