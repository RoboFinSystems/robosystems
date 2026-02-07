"""Tests for download rate limiting module."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.config.shared_repositories import RepositoryPlan
from robosystems.middleware.rate_limits.download_limits import DownloadRateLimiter


class TestDownloadRateLimiter:
  """Test suite for DownloadRateLimiter class."""

  def test_get_key_format(self):
    """Test Redis key format is correct."""
    key = DownloadRateLimiter._get_key("user123", "sec")
    today = datetime.now(UTC).strftime("%Y%m%d")
    assert key == f"download_limit:sec:user123:{today}"

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

  def test_get_daily_limit_starter_plan(self):
    """Test daily limit for STARTER plan."""
    limit = DownloadRateLimiter._get_daily_limit("sec", RepositoryPlan.STARTER)
    assert limit == 3  # Starter gets 3 downloads/day

  def test_get_daily_limit_advanced_plan(self):
    """Test daily limit for ADVANCED plan."""
    limit = DownloadRateLimiter._get_daily_limit("sec", RepositoryPlan.ADVANCED)
    assert limit == 5  # Advanced gets 5 downloads/day

  def test_get_daily_limit_unknown_repository(self):
    """Test daily limit falls back to default for unknown repository."""
    limit = DownloadRateLimiter._get_daily_limit("unknown_repo", RepositoryPlan.STARTER)
    assert limit == DownloadRateLimiter.DEFAULT_DOWNLOADS_PER_DAY

  def test_get_reset_time_is_tomorrow_midnight_utc(self):
    """Test reset time is set to midnight UTC tomorrow."""
    reset_time = DownloadRateLimiter._get_reset_time()
    now = datetime.now(UTC)
    tomorrow = now.date() + timedelta(days=1)

    assert reset_time.date() == tomorrow
    assert reset_time.hour == 0
    assert reset_time.minute == 0
    assert reset_time.second == 0
    assert reset_time.tzinfo == UTC

  @pytest.mark.asyncio
  async def test_check_download_limit_allows_when_under_limit(self):
    """Test download is allowed when under daily limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"1"  # 1 download used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.STARTER,  # Limit is 3
      )

    assert allowed is True
    assert remaining == 2  # 3 - 1 = 2
    assert reset_at > datetime.now(UTC)

  @pytest.mark.asyncio
  async def test_check_download_limit_blocks_when_at_limit(self):
    """Test download is blocked when at daily limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"3"  # 3 downloads used (at limit for STARTER)

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.STARTER,  # Limit is 3
      )

    assert allowed is False
    assert remaining == 0

  @pytest.mark.asyncio
  async def test_check_download_limit_blocks_when_over_limit(self):
    """Test download is blocked when over daily limit."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"5"  # 5 downloads used (over limit for STARTER)

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.STARTER,  # Limit is 3
      )

    assert allowed is False
    assert remaining == 0  # Never negative

  @pytest.mark.asyncio
  async def test_check_download_limit_allows_all_for_no_usage(self):
    """Test full limit available when no downloads used."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # No downloads yet

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      allowed, remaining, reset_at = await DownloadRateLimiter.check_download_limit(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.STARTER,  # Limit is 3
      )

    assert allowed is True
    assert remaining == 3

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
    mock_redis.get.return_value = b"2"  # 2 downloads used

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      quota = await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.STARTER,  # Limit is 3
      )

    assert quota["limit_per_day"] == 3
    assert quota["used_today"] == 2
    assert quota["remaining"] == 1
    assert "resets_at" in quota

  @pytest.mark.asyncio
  async def test_get_download_quota_no_usage(self):
    """Test get_download_quota when no downloads used today."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # No usage

    with patch.object(
      DownloadRateLimiter, "_get_redis_client", return_value=mock_redis
    ):
      quota = await DownloadRateLimiter.get_download_quota(
        user_id="user123",
        repository="sec",
        plan=RepositoryPlan.ADVANCED,  # Limit is 5
      )

    assert quota["limit_per_day"] == 5
    assert quota["used_today"] == 0
    assert quota["remaining"] == 5

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
        plan=RepositoryPlan.STARTER,
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
        plan=RepositoryPlan.STARTER,
      )

    mock_redis.aclose.assert_called_once()


class TestDownloadRateLimiterIntegration:
  """Integration-style tests for download rate limiting behavior."""

  @pytest.mark.asyncio
  async def test_full_download_cycle(self):
    """Test a complete download cycle from 0 to limit."""
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
      # First download - should be allowed
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert allowed is True
      assert remaining == 3

      await DownloadRateLimiter.increment_download_count("user1", "sec")

      # Second download
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert allowed is True
      assert remaining == 2

      await DownloadRateLimiter.increment_download_count("user1", "sec")

      # Third download (last allowed)
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert allowed is True
      assert remaining == 1

      await DownloadRateLimiter.increment_download_count("user1", "sec")

      # Fourth download - should be blocked
      allowed, remaining, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert allowed is False
      assert remaining == 0

  @pytest.mark.asyncio
  async def test_different_users_have_separate_limits(self):
    """Test that different users have independent download limits."""
    mock_redis = AsyncMock()
    user_counts = {"user1": 2, "user2": 0}

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
      # User1 has used 2 downloads
      allowed1, remaining1, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert allowed1 is True
      assert remaining1 == 1

      # User2 has fresh limit
      allowed2, remaining2, _ = await DownloadRateLimiter.check_download_limit(
        "user2", "sec", RepositoryPlan.STARTER
      )
      assert allowed2 is True
      assert remaining2 == 3

  @pytest.mark.asyncio
  async def test_different_repositories_have_separate_limits(self):
    """Test that different repositories have independent download limits."""
    mock_redis = AsyncMock()
    repo_counts = {"sec": 2, "economic": 0}

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
      # SEC has 2 downloads used
      allowed1, remaining1, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "sec", RepositoryPlan.STARTER
      )
      assert remaining1 == 1

      # Economic has fresh limit
      allowed2, remaining2, _ = await DownloadRateLimiter.check_download_limit(
        "user1", "economic", RepositoryPlan.STARTER
      )
      assert remaining2 == 3
