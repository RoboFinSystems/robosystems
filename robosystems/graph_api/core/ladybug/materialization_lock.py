"""Per-graph Valkey distributed lock for materialization.

Prevents concurrent materializations of the same graph database.
Uses Valkey (Redis-compatible) with Lua-based compare-and-delete for safe release.

Key design:
- Per-graph: graph A doesn't block graph B
- WIP/prev resolve to base: kg123-wip and kg123 compete for the same lock
- 1-hour TTL: safety net for crashed processes
- 5s acquire timeout: fail fast if another materialization is running
- Compare-and-delete via Lua: prevents releasing a lock re-acquired by another process
- Compare-and-extend via Lua: long runs refresh the TTL at checkpoints, and learn
  when the lock has lapsed under them instead of continuing to the swap
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

# Lua script for atomic compare-and-extend
# Only refreshes the TTL if the value matches (a lapsed lock re-acquired by
# another process must not be extended by its previous holder)
_EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
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
    # The last backend error seen by ``acquire`` (None when the final attempt
    # reached Valkey). Lets a caller tell "held by another run" apart from
    # "lock service unavailable" when acquire returns False.
    self.last_backend_error: str | None = None

  @property
  def acquired(self) -> bool:
    return self._acquired

  async def acquire(
    self, timeout_seconds: float = DEFAULT_ACQUIRE_TIMEOUT_SECONDS
  ) -> bool:
    """Try to acquire the lock, returning False if ``timeout_seconds`` elapses.

    Polls with a halving backoff. Redis errors are retried within the window
    rather than raised — a blip should not fail a materialization outright.
    """
    import asyncio
    import time

    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    self.last_backend_error = None

    while time.monotonic() < deadline:
      try:
        result = await self.redis.set(
          self.lock_key,
          self.token,
          nx=True,
          ex=self.ttl_seconds,
        )
        self.last_backend_error = None
        if result:
          self._acquired = True
          logger.info(f"Materialization lock acquired: {self.lock_key}")
          return True
      except Exception as e:
        self.last_backend_error = str(e)
        logger.warning(f"Lock acquire attempt failed: {e}")

      attempt += 1
      wait = min(0.5, (deadline - time.monotonic()) / 2)
      if wait > 0:
        await asyncio.sleep(wait)

    if self.last_backend_error:
      logger.warning(
        f"Materialization lock acquire gave up on backend errors: "
        f"{self.lock_key} ({self.last_backend_error})"
      )
    else:
      logger.info(f"Materialization lock acquire timed out: {self.lock_key}")
    return False

  async def extend(self, ttl_seconds: int | None = None) -> bool:
    """Reset the TTL to a full window, returning False if the lock is no
    longer ours.

    A materialization can outlive the TTL that was meant as a crash safety
    net; refreshing at checkpoints keeps the lock alive for as long as the run
    is demonstrably still making progress. A False return means the key has
    expired or been re-acquired by another process — the caller must abort
    rather than continue to the swap. Backend errors are raised, not swallowed:
    the caller decides whether a blip is tolerable given the TTL still on the
    key.
    """
    if not self._acquired:
      return False

    new_ttl = ttl_seconds if ttl_seconds is not None else self.ttl_seconds
    result = await self.redis.eval(
      _EXTEND_SCRIPT,
      1,
      self.lock_key,
      self.token,
      str(new_ttl),
    )
    if result == 1:
      logger.debug(f"Materialization lock extended: {self.lock_key} ({new_ttl}s)")
      return True

    logger.warning(
      f"Materialization lock extend failed (token mismatch or expired): {self.lock_key}"
    )
    self._acquired = False
    return False

  async def release(self) -> bool:
    """Release the lock, returning False if this process no longer holds it.

    The compare-and-delete is what makes that safe: after a TTL expiry the key
    may belong to another process, and deleting it unconditionally would strip
    a lock someone else is relying on.
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

  async def __aenter__(self) -> "MaterializationLock":
    """Async context manager: acquire the lock."""
    if not await self.acquire():
      raise RuntimeError(f"Could not acquire materialization lock: {self.lock_key}")
    return self

  async def __aexit__(self, *args: object) -> None:
    """Async context manager: release the lock."""
    await self.release()

  async def is_locked(self) -> bool:
    """Check if the lock is currently held (by anyone)."""
    try:
      return await self.redis.exists(self.lock_key) > 0
    except Exception:
      return False

  @staticmethod
  def from_trusted_token(
    redis_client: redis_async.Redis,
    graph_id: str,
    token: str,
    ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
  ) -> "MaterializationLock":
    """Create a lock instance that trusts the caller already holds the lock.

    WARNING: Does NOT verify the token against Valkey. The caller is trusted
    to have acquired the lock themselves and is passing the token through
    (e.g., via X-Materialization-Lock-Token header) so that downstream
    endpoints don't re-acquire it. Only use for internal service calls.
    """
    lock = MaterializationLock(redis_client, graph_id, ttl_seconds)
    lock.token = token
    lock._acquired = True
    return lock
