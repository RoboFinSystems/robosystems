"""Tests for download rate limiting module."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.middleware.rate_limits.download_limits import DownloadRateLimiter


class FakeRedis:
  """Atomic INCR/DECR over a dict, yielding to the loop between calls the way
  a real round trip does."""

  def __init__(self, values: dict[str, int] | None = None):
    self.values = dict(values or {})
    self.expired: list[str] = []
    self.closed = 0

  async def get(self, key):
    await asyncio.sleep(0)
    value = self.values.get(key)
    return None if value is None else str(value).encode()

  async def incr(self, key):
    await asyncio.sleep(0)
    self.values[key] = self.values.get(key, 0) + 1
    return self.values[key]

  async def decr(self, key):
    await asyncio.sleep(0)
    self.values[key] = self.values.get(key, 0) - 1
    return self.values[key]

  async def set(self, key, value, keepttl=False):
    self.values[key] = int(value)

  async def expire(self, key, ttl):
    self.expired.append(key)

  async def aclose(self):
    self.closed += 1


class TestDownloadRateLimiter:
  """Test suite for DownloadRateLimiter class."""

  def test_get_key_format(self):
    """Test Redis key format uses year-month."""
    key = DownloadRateLimiter._get_key("user123", "sec")
    month = datetime.now(UTC).strftime("%Y%m")
    assert key == f"download_limit:sec:user123:{month}"

  def test_get_key_different_users(self):
    """Test keys are different for different users."""
    key1 = DownloadRateLimiter._get_key("user1", "sec")
    key2 = DownloadRateLimiter._get_key("user2", "sec")
    assert key1 != key2

  def test_get_key_different_repositories(self):
    """Test keys are different for different repositories."""
    key1 = DownloadRateLimiter._get_key("user1", "sec")
    key2 = DownloadRateLimiter._get_key("user1", "economic")
    assert key1 != key2

  def test_get_monthly_limit_starter_plan(self):
    """Test monthly limit for STARTER plan."""
    limit = DownloadRateLimiter.get_shared_repo_monthly_limit("sec", "starter")
    assert limit == 1  # Starter gets 1 download/month (R2 zero-egress)

  def test_get_monthly_limit_advanced_plan(self):
    """Test monthly limit for ADVANCED plan."""
    limit = DownloadRateLimiter.get_shared_repo_monthly_limit("sec", "advanced")
    assert limit == 4  # Advanced gets 4 downloads/month (R2 zero-egress)

  def test_get_monthly_limit_unknown_repository(self):
    """Test monthly limit falls back to default for unknown repository."""
    limit = DownloadRateLimiter.get_shared_repo_monthly_limit("unknown_repo", "starter")
    assert limit == DownloadRateLimiter.DEFAULT_DOWNLOADS_PER_MONTH

  def test_get_reset_time_is_first_of_next_month_utc(self):
    """Test reset time is set to first of next month, midnight UTC."""
    reset_time = DownloadRateLimiter._get_reset_time()
    now = datetime.now(UTC)

    if now.month == 12:
      expected_year = now.year + 1
      expected_month = 1
    else:
      expected_year = now.year
      expected_month = now.month + 1

    assert reset_time.year == expected_year
    assert reset_time.month == expected_month
    assert reset_time.day == 1
    assert reset_time.hour == 0
    assert reset_time.minute == 0
    assert reset_time.second == 0
    assert reset_time.tzinfo == UTC

  @pytest.mark.asyncio
  async def test_reserve_allows_under_limit(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      allowed, remaining, reset_at = await DownloadRateLimiter.reserve_download(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 4
      )

    assert allowed is True
    assert remaining == 3
    assert reset_at > datetime.now(UTC)

  @pytest.mark.asyncio
  async def test_reserve_refuses_at_limit_and_hands_the_slot_back(self):
    key = DownloadRateLimiter._get_key("user123", "sec")
    redis = FakeRedis({key: 4})
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      allowed, remaining, _ = await DownloadRateLimiter.reserve_download(
        user_id="user123", repository="sec", plan="advanced"
      )

    assert allowed is False
    assert remaining == 0
    assert redis.values[key] == 4

  @pytest.mark.asyncio
  async def test_first_reservation_sets_ttl(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      await DownloadRateLimiter.reserve_download("user123", "sec", "advanced")
      await DownloadRateLimiter.reserve_download("user123", "sec", "advanced")

    assert len(redis.expired) == 1

  @pytest.mark.asyncio
  async def test_parallel_requests_cannot_exceed_the_limit(self):
    """Reading the count and incrementing later let N concurrent requests all
    pass a read of 0 against a 1/month limit."""
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      with patch.object(
        DownloadRateLimiter, "get_graph_tier_monthly_limit", return_value=1
      ):
        results = await asyncio.gather(
          *(
            DownloadRateLimiter.reserve_graph_download(
              "user123", "kg1", "ladybug-standard"
            )
            for _ in range(5)
          )
        )

    assert sum(1 for allowed, _, _ in results if allowed) == 1
    assert redis.values[DownloadRateLimiter._get_key("user123", "kg1")] == 1

  @pytest.mark.asyncio
  async def test_release_returns_a_reservation(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      await DownloadRateLimiter.reserve_download("user123", "sec", "advanced")
      await DownloadRateLimiter.release_download("user123", "sec")

    assert redis.values[DownloadRateLimiter._get_key("user123", "sec")] == 0

  @pytest.mark.asyncio
  async def test_release_never_goes_negative(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      await DownloadRateLimiter.release_download("user123", "sec")

    assert redis.values[DownloadRateLimiter._get_key("user123", "sec")] == 0

  @pytest.mark.asyncio
  async def test_get_download_quota_returns_complete_info(self):
    """Test get_download_quota returns all quota information."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # No downloads used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      quota = await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 4
      )

    assert quota["limit_per_month"] == 4
    assert quota["used_this_month"] == 0
    assert quota["remaining"] == 4
    assert "resets_at" in quota

  @pytest.mark.asyncio
  async def test_get_download_quota_at_limit(self):
    """Test get_download_quota when limit is reached."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"4"  # 4 downloads used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      quota = await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 4
      )

    assert quota["limit_per_month"] == 4
    assert quota["used_this_month"] == 4
    assert quota["remaining"] == 0

  @pytest.mark.asyncio
  async def test_redis_client_closed_after_reserve(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      await DownloadRateLimiter.reserve_download("user123", "sec", "advanced")

    assert redis.closed == 1

  @pytest.mark.asyncio
  async def test_redis_client_closed_after_release(self):
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      await DownloadRateLimiter.release_download("user123", "sec")

    assert redis.closed == 1

  @pytest.mark.asyncio
  async def test_redis_client_closed_after_get_quota(self):
    """Test Redis client is properly closed after get_download_quota."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan="advanced",
      )

    mock_redis.aclose.assert_called_once()


class TestDownloadRateLimiterIntegration:
  """Integration-style tests for download rate limiting behavior."""

  @pytest.mark.asyncio
  async def test_full_download_cycle(self):
    """0 to the limit (4/month for advanced), then refused."""
    redis = FakeRedis()
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      for expected_remaining in (3, 2, 1, 0):
        allowed, remaining, _ = await DownloadRateLimiter.reserve_download(
          "user1", "sec", "advanced"
        )
        assert allowed is True
        assert remaining == expected_remaining

      allowed, remaining, _ = await DownloadRateLimiter.reserve_download(
        "user1", "sec", "advanced"
      )
      assert allowed is False
      assert remaining == 0

  @pytest.mark.asyncio
  async def test_different_users_have_separate_limits(self):
    redis = FakeRedis({DownloadRateLimiter._get_key("user1", "sec"): 4})
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      allowed1, _, _ = await DownloadRateLimiter.reserve_download(
        "user1", "sec", "advanced"
      )
      allowed2, remaining2, _ = await DownloadRateLimiter.reserve_download(
        "user2", "sec", "advanced"
      )

    assert allowed1 is False
    assert allowed2 is True
    assert remaining2 == 3

  @pytest.mark.asyncio
  async def test_different_repositories_have_separate_limits(self):
    redis = FakeRedis({DownloadRateLimiter._get_key("user1", "sec"): 4})
    with patch.object(DownloadRateLimiter, "_get_redis_client", return_value=redis):
      allowed1, _, _ = await DownloadRateLimiter.reserve_download(
        "user1", "sec", "advanced"
      )
      # Economic falls back to the default of 1.
      allowed2, remaining2, _ = await DownloadRateLimiter.reserve_download(
        "user1", "economic", "advanced"
      )

    assert allowed1 is False
    assert allowed2 is True
    assert remaining2 == 0
