"""Per-graph Valkey lock that serializes materializations of one database.

``-wip``/``-prev`` resolve to the base id, so a build and its target share a
lock. Release and extend are compare-and-set Lua scripts, so a holder whose
lock lapsed cannot touch the next holder's. One lock per operation: the
caller that acquires it passes the token on (task params, the
``X-Materialization-Lock-Token`` header), and every downstream step adopts it
rather than taking the lock again.
"""

import re
import uuid

import redis
import redis.asyncio as redis_async

from robosystems.logger import logger

_LOCK_PREFIX = "materialize_lock:"

# Crash safety net; long runs refresh it via extend().
DEFAULT_LOCK_TTL_SECONDS = 3600

DEFAULT_ACQUIRE_TIMEOUT_SECONDS = 5

_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
else
    return 0
end
"""


def _resolve_base_graph_id(graph_id: str) -> str:
  """kg123-wip -> kg123, kg123_dev-prev -> kg123_dev."""
  return re.sub(r"-(wip|prev)$", "", graph_id)


def lock_key_for(graph_id: str) -> str:
  """The Valkey key holding ``graph_id``'s materialization lock."""
  return f"{_LOCK_PREFIX}{_resolve_base_graph_id(graph_id)}"


class MaterializationLock:
  """Distributed lock for graph materialization operations."""

  def __init__(
    self,
    redis_client: redis_async.Redis,
    graph_id: str,
    ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
  ):
    self.redis = redis_client
    self.lock_key = lock_key_for(graph_id)
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

    Always makes one attempt, so ``timeout_seconds=0`` is a non-blocking try.
    Polls with a halving backoff. Redis errors are retried within the window
    rather than raised — a blip should not fail a materialization outright.
    """
    import asyncio
    import time

    deadline = time.monotonic() + timeout_seconds
    self.last_backend_error = None

    while True:
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

      remaining = deadline - time.monotonic()
      if remaining <= 0:
        break
      await asyncio.sleep(min(0.5, remaining / 2))

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

    Call at checkpoints of a long run. On False the key expired or was
    re-acquired elsewhere, and the caller must abort rather than swap. Backend
    errors are raised so the caller can judge a blip against the remaining TTL.
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

    Never raises.
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
    if not await self.acquire():
      raise RuntimeError(f"Could not acquire materialization lock: {self.lock_key}")
    return self

  async def __aexit__(self, *args: object) -> None:
    await self.release()

  async def is_locked(self) -> bool:
    """Check if the lock is currently held (by anyone)."""
    try:
      return await self.redis.exists(self.lock_key) > 0
    except Exception:
      return False

  @classmethod
  async def adopt(
    cls,
    redis_client: redis_async.Redis,
    graph_id: str,
    token: str,
    ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
  ) -> "MaterializationLock | None":
    """Take over a lock another process acquired, given its token.

    The token must be the one stored: the check and a TTL refresh are one
    compare-and-set, so the lock is held for a full window from here. Returns
    None when it is not (the lock lapsed, or the token is not the holder's).
    Backend errors are raised; the caller must not proceed unlocked.
    """
    lock = cls(redis_client, graph_id, ttl_seconds)
    lock.token = token
    result = await redis_client.eval(
      _EXTEND_SCRIPT, 1, lock.lock_key, token, str(ttl_seconds)
    )
    if result != 1:
      logger.warning(f"Materialization lock token not the holder's: {lock.lock_key}")
      return None
    lock._acquired = True
    return lock


def release_token(redis_client: redis.Redis, graph_id: str, token: str) -> bool:
  """Release ``graph_id``'s lock from a process other than the acquirer.

  Compare-and-delete, so a lapsed holder cannot strip a successor's lock.
  Never raises; a failed release leaves the lock to its TTL.
  """
  key = lock_key_for(graph_id)
  try:
    released = redis_client.eval(_RELEASE_SCRIPT, 1, key, token) == 1
  except Exception as e:
    logger.warning(f"Materialization lock release failed for {key}: {e}")
    return False
  if released:
    logger.info(f"Materialization lock released: {key}")
  return released


def extend_token(
  redis_client: redis.Redis,
  graph_id: str,
  token: str,
  ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
) -> bool:
  """Refresh ``graph_id``'s lock TTL for a holder in another process.

  False when the token is no longer the holder's or the backend failed.
  """
  key = lock_key_for(graph_id)
  try:
    return redis_client.eval(_EXTEND_SCRIPT, 1, key, token, str(ttl_seconds)) == 1
  except Exception as e:
    logger.warning(f"Materialization lock extend failed for {key}: {e}")
    return False
