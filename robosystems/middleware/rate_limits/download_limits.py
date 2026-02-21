"""
Download rate limiting for shared repository backup downloads.

This module implements monthly download limits for shared repository backups.
Uses Valkey DB 1 (RATE_LIMITS) with monthly TTL expiration.
"""

from datetime import UTC, datetime
from typing import Any

from robosystems.config.shared_repositories import (
  get_rate_limits as _get_rate_limits,
)
from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
from robosystems.logger import logger


class DownloadRateLimiter:
  """Rate limiter for shared repository backup downloads."""

  # Default limit if not configured
  DEFAULT_DOWNLOADS_PER_MONTH = 1

  @classmethod
  def _get_redis_client(cls) -> Any:
    """Get async Redis client for rate limiting database."""
    return create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

  @classmethod
  def _get_key(cls, user_id: str, repository: str) -> str:
    """Build the Redis key for download tracking."""
    month = datetime.now(UTC).strftime("%Y%m")
    return f"download_limit:{repository}:{user_id}:{month}"

  @classmethod
  def _get_monthly_limit(cls, repository: str, plan: str) -> int:
    """Get the monthly download limit for a repository and plan."""
    try:
      limits = _get_rate_limits(repository, plan)
      if limits:
        return limits.get("downloads_per_month", cls.DEFAULT_DOWNLOADS_PER_MONTH)
    except (ValueError, KeyError) as e:
      logger.warning(
        f"Failed to get download limit for repository={repository}, plan={plan}: {e}"
      )
    return cls.DEFAULT_DOWNLOADS_PER_MONTH

  @classmethod
  def _get_reset_time(cls) -> datetime:
    """Get the time when the monthly limit resets (first of next month, midnight UTC)."""
    now = datetime.now(UTC)
    if now.month == 12:
      next_month = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
      next_month = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    return next_month

  @classmethod
  async def check_download_limit(
    cls,
    user_id: str,
    repository: str,
    plan: str,
  ) -> tuple[bool, int, datetime]:
    """
    Check if user has remaining downloads for the month.

    Args:
        user_id: User ID
        repository: Repository ID (e.g., "sec")
        plan: User's repository subscription plan

    Returns:
        Tuple of (allowed, remaining, resets_at)
        - allowed: True if download is permitted
        - remaining: Number of downloads remaining this month
        - resets_at: When the limit resets (first of next month, UTC)
    """
    monthly_limit = cls._get_monthly_limit(repository, plan)
    reset_at = cls._get_reset_time()

    redis_client = None
    try:
      redis_client = cls._get_redis_client()
      key = cls._get_key(user_id, repository)
      current = await redis_client.get(key)
      used = int(current) if current else 0
      remaining = max(0, monthly_limit - used)
      allowed = used < monthly_limit

      logger.debug(
        f"Download limit check for user {user_id} on {repository}: "
        f"used={used}, limit={monthly_limit}, remaining={remaining}"
      )

      return allowed, remaining, reset_at
    finally:
      if redis_client is not None:
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
        New download count for this month
    """
    redis_client = None
    try:
      redis_client = cls._get_redis_client()
      key = cls._get_key(user_id, repository)

      # Increment and set TTL to expire at start of next month
      count = await redis_client.incr(key)

      # Set expiry to end of month if this is the first increment
      if count == 1:
        reset_at = cls._get_reset_time()
        ttl_seconds = int((reset_at - datetime.now(UTC)).total_seconds())
        await redis_client.expire(key, ttl_seconds)

      logger.info(
        f"Download count incremented for user {user_id} on {repository}: count={count}"
      )

      return count
    finally:
      if redis_client is not None:
        await redis_client.aclose()

  @classmethod
  async def get_download_quota(
    cls,
    user_id: str,
    repository: str,
    plan: str,
  ) -> dict:
    """
    Get the full download quota information for a user.

    Args:
        user_id: User ID
        repository: Repository ID (e.g., "sec")
        plan: User's repository subscription plan

    Returns:
        Dict with quota information:
        - limit_per_month: Monthly download limit
        - used_this_month: Downloads used this month
        - remaining: Downloads remaining this month
        - resets_at: ISO timestamp when limit resets
    """
    monthly_limit = cls._get_monthly_limit(repository, plan)
    reset_at = cls._get_reset_time()

    redis_client = None
    try:
      redis_client = cls._get_redis_client()
      key = cls._get_key(user_id, repository)
      current = await redis_client.get(key)
      used = int(current) if current else 0
      remaining = max(0, monthly_limit - used)

      return {
        "limit_per_month": monthly_limit,
        "used_this_month": used,
        "remaining": remaining,
        "resets_at": reset_at.isoformat(),
      }
    finally:
      if redis_client is not None:
        await redis_client.aclose()
