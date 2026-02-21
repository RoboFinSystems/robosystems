"""Tests for download rate limiting module."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.middleware.rate_limits.download_limits import DownloadRateLimiter


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
    """Test monthly limit for STARTER plan (downloads disabled)."""
    limit = DownloadRateLimiter._get_monthly_limit("sec", "starter")
    assert limit == 0  # Starter plan has no download access

  def test_get_monthly_limit_advanced_plan(self):
    """Test monthly limit for ADVANCED plan."""
    limit = DownloadRateLimiter._get_monthly_limit("sec", "advanced")
    assert limit == 1  # Advanced gets 1 download/month

  def test_get_monthly_limit_unknown_repository(self):
    """Test monthly limit falls back to default for unknown repository."""
    limit = DownloadRateLimiter._get_monthly_limit("unknown_repo", "starter")
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
  async def test_check_download_limit_allows_when_under_limit(self):
    """Test download is allowed when under monthly limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # No downloads used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 1
      )

    assert allowed is True
    assert remaining == 1
    assert reset_at > datetime.now(UTC)

  @pytest.mark.asyncio
  async def test_check_download_limit_blocks_when_at_limit(self):
    """Test download is blocked when at monthly limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"1"  # 1 download used (at limit for ADVANCED)

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 1
      )

    assert allowed is False
    assert remaining == 0

  @pytest.mark.asyncio
  async def test_check_download_limit_blocks_when_over_limit(self):
    """Test download is blocked when over monthly limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"3"  # 3 downloads used (over limit for ADVANCED)

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 1
      )

    assert allowed is False
    assert remaining == 0  # Never negative

  @pytest.mark.asyncio
  async def test_check_download_limit_starter_always_blocked(self):
    """Test starter plan is always blocked (limit is 0)."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # No downloads yet

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan="starter",  # Limit is 0
      )

    assert allowed is False
    assert remaining == 0

  @pytest.mark.asyncio
  async def test_increment_download_count_first_increment(self):
    """Test first increment sets TTL."""
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1  # First increment

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      count = await DownloadRateLimiter.increment_download_count(
        user_id="user123",
        repository="sec",
      )

    assert count == 1
    mock_redis.incr.assert_called_once()
    mock_redis.expire.assert_called_once()  # TTL should be set on first increment

  @pytest.mark.asyncio
  async def test_increment_download_count_subsequent_increment(self):
    """Test subsequent increments don't reset TTL."""
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 2  # Second increment

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      count = await DownloadRateLimiter.increment_download_count(
        user_id="user123",
        repository="sec",
      )

    assert count == 2
    mock_redis.incr.assert_called_once()
    mock_redis.expire.assert_not_called()  # TTL not set for subsequent increments

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
        plan="advanced",  # Limit is 1
      )

    assert quota["limit_per_month"] == 1
    assert quota["used_this_month"] == 0
    assert quota["remaining"] == 1
    assert "resets_at" in quota

  @pytest.mark.asyncio
  async def test_get_download_quota_at_limit(self):
    """Test get_download_quota when limit is reached."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"1"  # 1 download used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      quota = await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan="advanced",  # Limit is 1
      )

    assert quota["limit_per_month"] == 1
    assert quota["used_this_month"] == 1
    assert quota["remaining"] == 0

  @pytest.mark.asyncio
  async def test_redis_client_closed_after_check(self):
    """Test Redis client is properly closed after check_download_limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan="advanced",
      )

    mock_redis.aclose.assert_called_once()

  @pytest.mark.asyncio
  async def test_redis_client_closed_after_increment(self):
    """Test Redis client is properly closed after increment_download_count."""
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      await DownloadRateLimiter.increment_download_count(
        user_id="user123",
        repository="sec",
      )

    mock_redis.aclose.assert_called_once()

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
    """Test a complete download cycle from 0 to limit (1/month for advanced)."""
    mock_redis = AsyncMock()
    download_count = {"value": 0}

    def mock_get(key):
      if download_count["value"] == 0:
        return None
      return str(download_count["value"]).encode()

    def mock_incr(key):
      download_count["value"] += 1
      return download_count["value"]

    mock_redis.get.side_effect = mock_get
    mock_redis.incr.side_effect = mock_incr

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      # First download - should be allowed (advanced plan, limit 1)
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", "advanced"
      )
      assert allowed is True
      assert remaining == 1

      await DownloadRateLimiter.increment_download_count("user1", "sec")

      # Second download - should be blocked (1 used, limit 1)
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", "advanced"
      )
      assert allowed is False
      assert remaining == 0

  @pytest.mark.asyncio
  async def test_different_users_have_separate_limits(self):
    """Test that different users have independent download limits."""
    mock_redis = AsyncMock()
    user_counts = {"user1": 1, "user2": 0}

    def mock_get(key):
      for user, count in user_counts.items():
        if user in key:
          if count == 0:
            return None
          return str(count).encode()
      return None

    mock_redis.get.side_effect = mock_get

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      # User1 has used their 1 download (advanced plan, limit 1)
      allowed1, remaining1, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", "advanced"
      )
      assert allowed1 is False
      assert remaining1 == 0

      # User2 has fresh limit
      allowed2, remaining2, _ = await DownloadRateLimiter.check_download_limit(
        "user2", "sec", "advanced"
      )
      assert allowed2 is True
      assert remaining2 == 1

  @pytest.mark.asyncio
  async def test_different_repositories_have_separate_limits(self):
    """Test that different repositories have independent download limits."""
    mock_redis = AsyncMock()
    repo_counts = {"sec": 1, "economic": 0}

    def mock_get(key):
      for repo, count in repo_counts.items():
        if repo in key:
          if count == 0:
            return None
          return str(count).encode()
      return None

    mock_redis.get.side_effect = mock_get

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      # SEC has 1 download used (advanced plan, limit 1)
      allowed1, remaining1, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", "advanced"
      )
      assert allowed1 is False
      assert remaining1 == 0

      # Economic has fresh limit (falls back to default of 1)
      allowed2, remaining2, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "economic", "advanced"
      )
      assert allowed2 is True
      assert remaining2 == 1
