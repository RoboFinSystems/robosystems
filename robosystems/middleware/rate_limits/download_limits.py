"""Monthly backup-download limits, counted per user and resource per
calendar month (UTC). Shared-repository limits come from the adapter
manifests, dedicated-graph limits from ``config/billing/core.py``."""

from datetime import UTC, datetime
from typing import Any

from robosystems.config.billing.core import get_tier_backup_downloads_per_month
from robosystems.config.shared_repositories import (
  get_rate_limits as _get_rate_limits,
)
from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
from robosystems.logger import logger


class DownloadRateLimiter:
  DEFAULT_DOWNLOADS_PER_MONTH = 1

  @classmethod
  def _get_redis_client(cls) -> Any:
    return create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

  @classmethod
  def _get_key(cls, user_id: str, resource_id: str) -> str:
    month = datetime.now(UTC).strftime("%Y%m")
    return f"download_limit:{resource_id}:{user_id}:{month}"

  @classmethod
  def get_shared_repo_monthly_limit(cls, repository: str, plan: str) -> int:
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
  def get_graph_tier_monthly_limit(cls, graph_tier: str) -> int:
    """The tier's monthly limit, or DEFAULT_DOWNLOADS_PER_MONTH if unknown."""
    limit = get_tier_backup_downloads_per_month(graph_tier)
    if limit is None:
      logger.warning(f"Unknown graph tier '{graph_tier}', using default download limit")
      return cls.DEFAULT_DOWNLOADS_PER_MONTH
    return limit

  @classmethod
  def _get_reset_time(cls) -> datetime:
    """First of next month, midnight UTC."""
    now = datetime.now(UTC)
    if now.month == 12:
      next_month = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
      next_month = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    return next_month

  @classmethod
  async def _reserve(
    cls,
    user_id: str,
    resource_id: str,
    monthly_limit: int,
  ) -> tuple[bool, int, datetime]:
    """Take one download from the month's allowance: (allowed, remaining,
    reset_at). Increment first and compare, handing the slot back on refusal,
    so parallel requests cannot all pass a read of the same count."""
    reset_at = cls._get_reset_time()

    redis_client = None
    try:
      redis_client = cls._get_redis_client()
      key = cls._get_key(user_id, resource_id)

      used = await redis_client.incr(key)
      if used == 1:
        ttl_seconds = int((reset_at - datetime.now(UTC)).total_seconds())
        await redis_client.expire(key, ttl_seconds)

      if used > monthly_limit:
        await redis_client.decr(key)
        logger.debug(
          f"Download refused on {resource_id}: limit={monthly_limit} reached"
        )
        return False, 0, reset_at

      remaining = monthly_limit - used
      logger.info(
        f"Download reserved on {resource_id}: used={used}, limit={monthly_limit}"
      )
      return True, remaining, reset_at
    finally:
      if redis_client is not None:
        await redis_client.aclose()

  @classmethod
  async def reserve_download(
    cls,
    user_id: str,
    repository: str,
    plan: str,
  ) -> tuple[bool, int, datetime]:
    monthly_limit = cls.get_shared_repo_monthly_limit(repository, plan)
    return await cls._reserve(user_id, repository, monthly_limit)

  @classmethod
  async def reserve_graph_download(
    cls,
    user_id: str,
    graph_id: str,
    graph_tier: str,
  ) -> tuple[bool, int, datetime]:
    monthly_limit = cls.get_graph_tier_monthly_limit(graph_tier)
    return await cls._reserve(user_id, graph_id, monthly_limit)

  @classmethod
  async def release_download(cls, user_id: str, resource_id: str) -> None:
    """Return a reserved download that produced no URL."""
    redis_client = None
    try:
      redis_client = cls._get_redis_client()
      key = cls._get_key(user_id, resource_id)
      if await redis_client.decr(key) < 0:
        await redis_client.set(key, 0, keepttl=True)
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
    monthly_limit = cls.get_shared_repo_monthly_limit(repository, plan)
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
