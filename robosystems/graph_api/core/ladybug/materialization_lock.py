"""Per-graph Valkey distributed lock for materialization.

Prevents concurrent materializations of the same graph database.
Uses Valkey (Redis-compatible) with Lua-based compare-and-delete for safe release.

Key design:
- Per-graph: graph A doesn't block graph B
- WIP/prev resolve to base: kg123-wip and kg123 compete for the same lock
- 1-hour TTL: safety net for crashed processes
- 5s acquire timeout: fail fast if another materialization is running
- Compare-and-delete via Lua: prevents releasing a lock re-acquired by another process
- Lock passthrough: callers pass token via X-Materialization-Lock-Token header
"""

import re
import uuid

import redis.asyncio as redis_async

from robosystems.logger import logger

# Lock key prefix
_LOCK_PREFIX = "materialize_lock:"

# Default TTL: 1 hour (safety net for crashed processes)
DEFAULT_LOCK_TTL_SECONDS = 3600

# Acquire timeout: fail fast
DEFAULT_ACQUIRE_TIMEOUT_SECONDS = 5

# Lua script for atomic compare-and-delete
# Only deletes the key if the value matches (prevents releasing someone else's lock)
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def _resolve_base_graph_id(graph_id: str) -> str:
  """Resolve WIP/prev suffixes to the base graph ID.

  kg123-wip -> kg123
  kg123-prev -> kg123
  kg123 -> kg123
  kg123_dev-wip -> kg123_dev
  """
  return re.sub(r"-(wip|prev)$", "", graph_id)


class MaterializationLock:
  """Distributed lock for graph materialization operations."""

  def __init__(
    self,
    redis_client: redis_async.Redis,
    graph_id: str,
    ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
  ):
    base_id = _resolve_base_graph_id(graph_id)
    self.redis = redis_client
    self.lock_key = f"{_LOCK_PREFIX}{base_id}"
    self.ttl_seconds = ttl_seconds
    self.token = str(uuid.uuid4())
    self._acquired = False

  @property
  def acquired(self) -> bool:
    return self._acquired

  async def acquire(
    self, timeout_seconds: float = DEFAULT_ACQUIRE_TIMEOUT_SECONDS
  ) -> bool:
    """Try to acquire the lock.

    Args:
        timeout_seconds: Maximum time to wait. Default 5s (fail fast).

    Returns:
        True if lock was acquired, False if timed out.
    """
    import asyncio
    import time

    deadline = time.monotonic() + timeout_seconds
    attempt = 0

    while time.monotonic() < deadline:
      try:
        result = await self.redis.set(
          self.lock_key,
          self.token,
          nx=True,
          ex=self.ttl_seconds,
        )
        if result:
          self._acquired = True
          logger.info(f"Materialization lock acquired: {self.lock_key}")
          return True
      except Exception as e:
        logger.warning(f"Lock acquire attempt failed: {e}")

      attempt += 1
      wait = min(0.5, (deadline - time.monotonic()) / 2)
      if wait > 0:
        await asyncio.sleep(wait)

    logger.info(f"Materialization lock acquire timed out: {self.lock_key}")
    return False

  async def release(self) -> bool:
    """Release the lock using compare-and-delete.

    Returns:
        True if this process held and released the lock,
        False if the lock was already released or held by another process.
    """
    if not self._acquired:
      return False

    try:
      result = await self.redis.eval(
        _RELEASE_SCRIPT,
        1,
        self.lock_key,
        self.token,
      )
      released = result == 1
      if released:
        logger.info(f"Materialization lock released: {self.lock_key}")
      else:
        logger.warning(
          f"Materialization lock release failed (token mismatch): {self.lock_key}"
        )
      self._acquired = False
      return released
    except Exception as e:
      logger.error(f"Failed to release materialization lock: {e}")
      self._acquired = False
      return False

  async def is_locked(self) -> bool:
    """Check if the lock is currently held (by anyone)."""
    try:
      return await self.redis.exists(self.lock_key) > 0
    except Exception:
      return False

  @staticmethod
  def from_token(
    redis_client: redis_async.Redis,
    graph_id: str,
    token: str,
    ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
  ) -> "MaterializationLock":
    """Create a lock instance with an existing token (for passthrough).

    Used when the caller already holds the lock and passes the token
    via X-Materialization-Lock-Token header.
    """
    lock = MaterializationLock(redis_client, graph_id, ttl_seconds)
    lock.token = token
    lock._acquired = True
    return lock
