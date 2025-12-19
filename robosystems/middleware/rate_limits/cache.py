"""Rate limiting cache using Valkey/Redis."""

import time
from typing import Any, cast

import redis

from ...config import env
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...logger import logger


class RateLimitCache:
  """Manages rate limiting using Valkey/Redis DB 7."""

  # Rate limiting configuration
  RATE_LIMIT_PREFIX = "rate_limit:"

  def __init__(self):
    """Initialize Redis connection for rate limiting."""
    self._redis = None
    # Rate limiting configuration
    self.enabled = env.RATE_LIMIT_ENABLED

  @property
  def redis(self) -> redis.Redis:
    """Get Redis connection, creating if needed."""
    if self._redis is None:
      try:
        # Use the new connection factory with proper ElastiCache support
        self._redis = create_redis_client(ValkeyDatabase.RATE_LIMITING)
        # Test connection
        self._redis.ping()
        logger.info("Connected to Valkey/Redis for rate limiting")
      except Exception as e:
        logger.error(f"Failed to connect to Valkey/Redis for rate limiting: {e}")
        raise
    return self._redis

  def _get_rate_limit_key(self, identifier: str) -> str:
    """Get cache key for rate limiting."""
    return f"{self.RATE_LIMIT_PREFIX}{identifier}"

  def check_rate_limit(
    self, identifier: str, limit: int, window: int
  ) -> tuple[bool, int]:
    """
    Check if request is within rate limit using sliding window.

    Args:
        identifier: Unique identifier (e.g., user:123, ip:1.2.3.4)
        limit: Maximum requests allowed
        window: Time window in seconds

    Returns:
        tuple[bool, int]: (allowed, remaining_requests)
    """
    if not self.enabled:
      return True, limit

    try:
      key = self._get_rate_limit_key(identifier)
      now = time.time()

      # Use Lua script for atomic sliding window with debugging
      lua_script = """
      local key = KEYS[1]
      local window = tonumber(ARGV[1])
      local limit = tonumber(ARGV[2])
      local now = tonumber(ARGV[3])

      -- Remove expired entries
      local removed = redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

      -- Count current requests
      local current = redis.call('ZCARD', key)

      if current < limit then
          -- Add current request with unique member to avoid duplicates
          -- Use microsecond precision timestamp as member for uniqueness
          local unique_member = tostring(now) .. ':' .. tostring(math.random(1000000))
          redis.call('ZADD', key, now, unique_member)
          redis.call('EXPIRE', key, window + 1)  -- Add 1 second buffer
          return {1, limit - current - 1}
      else
          return {0, 0}
      end
      """

      result = cast(
        list[int],
        self.redis.eval(lua_script, 1, key, str(window), str(limit), str(now)),
      )
      allowed = bool(result[0])
      remaining = int(result[1])

      if not allowed:
        # Log with actual count for debugging
        try:
          actual_count = self.redis.zcard(key)
        except Exception:
          actual_count = "unknown"
        logger.warning(
          f"Rate limit exceeded for {identifier}: limit={limit}/{window}s, actual_count={actual_count}"
        )
      else:
        logger.debug(f"Rate limit check passed for {identifier}: {remaining} remaining")

      return allowed, remaining

    except Exception as e:
      logger.error(f"Rate limiting check failed for {identifier}: {e}")
      # Fail open - allow request if rate limiting is broken
      return True, limit

  def get_rate_limit_stats(self) -> dict[str, Any]:
    """Get rate limiting statistics."""
    if not self.enabled:
      return {"enabled": False}

    try:
      # Get all rate limit keys
      keys = cast(list[str], self.redis.keys(f"{self.RATE_LIMIT_PREFIX}*")) or []

      stats = {
        "enabled": True,
        "active_limits": len(keys),
        "user_limits": len([k for k in keys if "user:" in k]),
        "ip_limits": len([k for k in keys if "ip:" in k]),
        "apikey_limits": len([k for k in keys if "apikey:" in k]),
        "jwt_limits": len([k for k in keys if "jwt:" in k]),
      }

      # Memory usage tracking removed - redis.memory_usage_pattern not available

      return stats

    except Exception as e:
      logger.error(f"Failed to get rate limit stats: {e}")
      return {"enabled": True, "error": str(e)}

  def clear_rate_limit(self, identifier: str) -> bool:
    """Clear rate limit for specific identifier."""
    try:
      key = self._get_rate_limit_key(identifier)
      deleted = self.redis.delete(key)
      logger.info(f"Cleared rate limit for {identifier}")
      return bool(deleted)
    except Exception as e:
      logger.error(f"Failed to clear rate limit for {identifier}: {e}")
      return False

  def get(self, key: str) -> Any:
    """Get value from cache."""
    if not self.enabled:
      return None
    try:
      return self.redis.get(key)
    except Exception:
      return None

  def set(self, key: str, value: Any, expire: int | None = None) -> bool:
    """Set value in cache with optional expiration."""
    if not self.enabled:
      return False
    try:
      return bool(self.redis.set(key, value, ex=expire))
    except Exception:
      return False


# Global rate limit cache instance
try:
  rate_limit_cache = RateLimitCache()
  logger.debug("Rate limiting cache initialized successfully")
except Exception as e:
  logger.error(f"Failed to initialize rate limiting cache: {e}")
  rate_limit_cache = None
