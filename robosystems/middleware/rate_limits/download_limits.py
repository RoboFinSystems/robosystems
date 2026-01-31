"""
Download rate limiting for shared repository backup downloads.

This module implements daily download limits for shared repository backups.
Uses Valkey DB 7 (RATE_LIMITING) with daily TTL expiration.
"""

from datetime import UTC, datetime, timedelta

from robosystems.config.billing.repositories import (
  RepositoryBillingConfig,
  RepositoryPlan,
  SharedRepository,
)
from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
from robosystems.logger import logger


class DownloadRateLimiter:
  """Rate limiter for shared repository backup downloads."""

  # Default limit if not configured
  DEFAULT_DOWNLOADS_PER_DAY = 3

  @classmethod
  def _get_redis_client(cls):
    """Get async Redis client for rate limiting database."""
    return create_async_redis_client(ValkeyDatabase.RATE_LIMITING)

  @classmethod
  def _get_key(cls, user_id: str, repository: str) -> str:
    """Build the Redis key for download tracking."""
    today = datetime.now(UTC).strftime("%Y%m%d")
    return f"download_limit:{repository}:{user_id}:{today}"

  @classmethod
  def _get_daily_limit(cls, repository: str, plan: RepositoryPlan) -> int:
    """Get the daily download limit for a repository and plan."""
    try:
      repo_enum = SharedRepository(repository)
      limits = RepositoryBillingConfig.get_rate_limits(repo_enum, plan)
      if limits:
        return limits.get("downloads_per_day", cls.DEFAULT_DOWNLOADS_PER_DAY)
    except (ValueError, KeyError):
      pass
    return cls.DEFAULT_DOWNLOADS_PER_DAY

  @classmethod
  def _get_reset_time(cls) -> datetime:
    """Get the time when the daily limit resets (midnight UTC)."""
    now = datetime.now(UTC)
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, datetime.min.time(), tzinfo=UTC)

  @classmethod
  async def check_download_limit(
    cls,
    user_id: str,
    repository: str,
    plan: RepositoryPlan,
  ) -> tuple[bool, int, datetime]:
    """
    Check if user has remaining downloads for the day.

    Args:
        user_id: User ID
        repository: Repository ID (e.g., "sec")
        plan: User's repository subscription plan

    Returns:
        Tuple of (allowed, remaining, resets_at)
        - allowed: True if download is permitted
        - remaining: Number of downloads remaining today
        - resets_at: When the limit resets (midnight UTC)
    """
    daily_limit = cls._get_daily_limit(repository, plan)
    reset_at = cls._get_reset_time()

    redis_client = cls._get_redis_client()
    try:
      key = cls._get_key(user_id, repository)
      current = await redis_client.get(key)
      used = int(current) if current else 0
      remaining = max(0, daily_limit - used)
      allowed = used < daily_limit

      logger.debug(
        f"Download limit check for user {user_id} on {repository}: "
        f"used={used}, limit={daily_limit}, remaining={remaining}"
      )

      return allowed, remaining, reset_at
    finally:
      await redis_client.aclose()

  @classmethod
  async def increment_download_count(
    cls,
    user_id: str,
    repository: str,
  ) -> int:
    """
    Increment the download counter for a user.

    Args:
        user_id: User ID
        repository: Repository ID (e.g., "sec")

    Returns:
        New download count for today
    """
    redis_client = cls._get_redis_client()
    try:
      key = cls._get_key(user_id, repository)

      # Increment and set TTL to expire at midnight UTC
      count = await redis_client.incr(key)

      # Set expiry to end of day if this is the first increment
      if count == 1:
        reset_at = cls._get_reset_time()
        ttl_seconds = int((reset_at - datetime.now(UTC)).total_seconds())
        await redis_client.expire(key, ttl_seconds)

      logger.info(
        f"Download count incremented for user {user_id} on {repository}: count={count}"
      )

      return count
    finally:
      await redis_client.aclose()

  @classmethod
  async def get_download_quota(
    cls,
    user_id: str,
    repository: str,
    plan: RepositoryPlan,
  ) -> dict:
    """
    Get the full download quota information for a user.

    Args:
        user_id: User ID
        repository: Repository ID (e.g., "sec")
        plan: User's repository subscription plan

    Returns:
        Dict with quota information:
        - limit_per_day: Daily download limit
        - used_today: Downloads used today
        - remaining: Downloads remaining today
        - resets_at: ISO timestamp when limit resets
    """
    daily_limit = cls._get_daily_limit(repository, plan)
    reset_at = cls._get_reset_time()

    redis_client = cls._get_redis_client()
    try:
      key = cls._get_key(user_id, repository)
      current = await redis_client.get(key)
      used = int(current) if current else 0
      remaining = max(0, daily_limit - used)

      return {
        "limit_per_day": daily_limit,
        "used_today": used,
        "remaining": remaining,
        "resets_at": reset_at.isoformat(),
      }
    finally:
      await redis_client.aclose()
