"""Redis-based distributed locking for SSO token and auth operations.

Serializes token operations across API instances. Locks always carry a TTL,
so a process that dies holding one cannot deadlock the others, and release is
an atomic compare-and-delete against the holder's lock_id so a lock that
already expired and was re-acquired is never released by its previous owner.
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import cast

import redis
from redis.exceptions import RedisError

from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType


@dataclass
class LockAcquisitionResult:
  acquired: bool
  lock_id: str | None
  holder_id: str | None
  ttl_remaining: int | None
  error_message: str | None = None
  # Redis never answered, so "not acquired" says nothing about the holder.
  backend_error: bool = False


class DistributedLock:
  """A single distributed lock: SET NX EX to acquire, compare-and-delete to
  release, exponential backoff between blocking retries.

  Instance-bound — the acquiring object must be the releasing object. Use
  `release_lock_by_id` when the lock spans processes.
  """

  def __init__(self, redis_client: redis.Redis, lock_key: str, ttl_seconds: int = 30):
    self.redis = redis_client
    self.lock_key = f"lock:{lock_key}"
    self.ttl_seconds = ttl_seconds
    self.lock_id = str(uuid.uuid4())
    self.acquired = False
    self.acquisition_time: float | None = None

  def acquire(
    self, blocking: bool = True, timeout: float | None = None
  ) -> LockAcquisitionResult:
    """Acquire the lock, retrying with backoff while `blocking`.

    Non-blocking failures report the current holder and remaining TTL.
    """
    start_time = time.time()
    max_retries = 50 if blocking else 1
    retry_count = 0

    while retry_count < max_retries:
      try:
        # Atomic NX + EX: a crash can never leave an immortal lock.
        result = self.redis.set(
          self.lock_key,
          self.lock_id,
          nx=True,
          ex=self.ttl_seconds,
        )

        if result:
          self.acquired = True
          self.acquisition_time = time.time()

          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.AUTH_SUCCESS,
            details={
              "action": "distributed_lock_acquired",
              "lock_key": self.lock_key,
              "lock_id": self.lock_id,
              "ttl_seconds": self.ttl_seconds,
              "retry_count": retry_count,
            },
            risk_level="low",
          )

          return LockAcquisitionResult(
            acquired=True,
            lock_id=self.lock_id,
            holder_id=self.lock_id,
            ttl_remaining=self.ttl_seconds,
          )

        if not blocking:
          # bytes or str, depending on `decode_responses`.
          raw_holder = self.redis.get(self.lock_key)
          if isinstance(raw_holder, bytes):
            holder = raw_holder.decode("utf-8")
          elif isinstance(raw_holder, str):
            holder = raw_holder
          else:
            holder = None
          ttl = cast(int | None, self.redis.ttl(self.lock_key))

          return LockAcquisitionResult(
            acquired=False,
            lock_id=None,
            holder_id=holder,
            ttl_remaining=ttl if ttl and ttl > 0 else None,
            error_message="Lock is currently held by another process",
          )

        if timeout and (time.time() - start_time) >= timeout:
          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
            details={
              "action": "distributed_lock_timeout",
              "lock_key": self.lock_key,
              "timeout_seconds": timeout,
              "retry_count": retry_count,
            },
            risk_level="medium",
          )
          return LockAcquisitionResult(
            acquired=False,
            lock_id=None,
            holder_id=None,
            ttl_remaining=None,
            error_message=f"Lock acquisition timed out after {timeout} seconds",
          )

        retry_count += 1
        wait_time = min(0.01 * (2**retry_count), 0.5)
        time.sleep(wait_time)

      except RedisError as e:
        logger.error(f"Redis error during lock acquisition: {e}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "distributed_lock_redis_error",
            "lock_key": self.lock_key,
            "error": str(e),
          },
          risk_level="high",
        )
        return LockAcquisitionResult(
          acquired=False,
          lock_id=None,
          holder_id=None,
          ttl_remaining=None,
          error_message=f"Redis error: {e!s}",
          backend_error=True,
        )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      details={
        "action": "distributed_lock_max_retries",
        "lock_key": self.lock_key,
        "max_retries": max_retries,
        "total_wait_time": time.time() - start_time,
      },
      risk_level="medium",
    )

    return LockAcquisitionResult(
      acquired=False,
      lock_id=None,
      holder_id=None,
      ttl_remaining=None,
      error_message=f"Failed to acquire lock after {max_retries} retries",
    )

  def release(self) -> bool:
    """Release the lock only if this instance still holds it (atomic
    compare-and-delete)."""
    if not self.acquired:
      return False

    try:
      lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """

      result = self.redis.eval(lua_script, 1, self.lock_key, self.lock_id)

      if result:
        self.acquired = False
        lock_duration = (
          time.time() - self.acquisition_time if self.acquisition_time else 0
        )

        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_SUCCESS,
          details={
            "action": "distributed_lock_released",
            "lock_key": self.lock_key,
            "lock_id": self.lock_id,
            "lock_duration_seconds": lock_duration,
          },
          risk_level="low",
        )

        return True
      else:
        logger.warning(
          f"Failed to release lock {self.lock_key} - not the current holder"
        )
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "distributed_lock_release_failed",
            "lock_key": self.lock_key,
            "lock_id": self.lock_id,
            "reason": "not_current_holder",
          },
          risk_level="medium",
        )
        return False

    except RedisError as e:
      logger.error(f"Redis error during lock release: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "distributed_lock_release_error",
          "lock_key": self.lock_key,
          "error": str(e),
        },
        risk_level="high",
      )
      return False

  def extend(self, additional_seconds: int) -> bool:
    """Extend the lock's TTL, only while this instance still holds it."""
    if not self.acquired:
      return False

    try:
      lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("expire", KEYS[1], ARGV[2])
            else
                return 0
            end
            """

      new_ttl = self.ttl_seconds + additional_seconds
      result = cast(
        bool, self.redis.eval(lua_script, 1, self.lock_key, self.lock_id, str(new_ttl))
      )

      if result:
        self.ttl_seconds = new_ttl
        logger.debug(f"Extended lock {self.lock_key} by {additional_seconds} seconds")
        return True
      else:
        logger.warning(
          f"Failed to extend lock {self.lock_key} - not the current holder"
        )
        return False

    except RedisError as e:
      logger.error(f"Redis error during lock extension: {e}")
      return False

  def __enter__(self):
    result = self.acquire()
    if not result.acquired:
      raise RuntimeError(f"Failed to acquire lock: {result.error_message}")
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.release()


def extend_lock_by_id(
  redis_client: redis.Redis,
  lock_key: str,
  lock_id: str,
  ttl_seconds: int,
) -> bool:
  """Reset a lock's TTL to ``ttl_seconds`` while ``lock_id`` still holds it.

  The cross-process counterpart of `DistributedLock.extend`, for a holder that
  outlives the TTL the acquirer set. `lock_key` is unprefixed.
  """
  full_key = f"lock:{lock_key}"
  lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
  try:
    return bool(redis_client.eval(lua_script, 1, full_key, lock_id, str(ttl_seconds)))
  except RedisError as e:
    logger.warning(f"extend_lock_by_id failed for {full_key}: {e}")
    return False


def release_lock_by_id(
  redis_client: redis.Redis,
  lock_key: str,
  lock_id: str,
) -> bool:
  """Release a lock acquired in a different process.

  For an acquirer in another process (an API endpoint and the Dagster job it
  launched): pass the acquirer's `lock_id`. Same compare-and-delete as
  `DistributedLock.release()`. `lock_key` is unprefixed.
  """
  full_key = f"lock:{lock_key}"
  lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
  try:
    result = redis_client.eval(lua_script, 1, full_key, lock_id)
    if result:
      logger.debug(f"Released distributed lock {full_key} via release_lock_by_id")
      return True
    return False
  except RedisError as e:
    logger.warning(
      f"release_lock_by_id failed for {full_key}: {e}; lock will expire via TTL"
    )
    return False


class SSOTokenLockManager:
  """Lock helpers for SSO token and session operations, each with a TTL and
  acquisition timeout tuned to how long that operation should take.
  """

  def __init__(self, redis_client: redis.Redis):
    self.redis = redis_client

    self.lock_configs = {
      "token_verification": {"ttl": 10, "timeout": 5},
      "token_exchange": {"ttl": 30, "timeout": 10},
      "session_creation": {"ttl": 15, "timeout": 8},
      "cleanup": {"ttl": 60, "timeout": 30},
    }

  @asynccontextmanager
  async def lock_sso_token(self, token_id: str, operation: str = "token_verification"):
    """Hold a lock on an SSO token for the duration of the block.

    `operation` selects the TTL/timeout pair from `lock_configs`. Raises
    `RuntimeError` if the lock can't be acquired within that timeout.
    """
    config = self.lock_configs.get(operation, self.lock_configs["token_verification"])
    lock_key = f"sso_token:{token_id}:{operation}"

    lock = DistributedLock(
      redis_client=self.redis, lock_key=lock_key, ttl_seconds=config["ttl"]
    )

    try:
      # acquire() backs off with time.sleep; keep that off the event loop.
      result = await asyncio.to_thread(
        lock.acquire, blocking=True, timeout=config["timeout"]
      )

      if not result.acquired:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "sso_token_lock_failed",
            "token_id": token_id[:8] + "...",
            "operation": operation,
            "error": result.error_message,
          },
          risk_level="high",
        )
        raise RuntimeError(f"Failed to acquire SSO token lock: {result.error_message}")

      logger.debug(f"Acquired SSO token lock for {operation}: {token_id[:8]}...")
      yield lock

    finally:
      if lock.acquired:
        lock.release()
        logger.debug(f"Released SSO token lock for {operation}: {token_id[:8]}...")

  @asynccontextmanager
  async def lock_sso_session(
    self, session_id: str, operation: str = "session_creation"
  ):
    """Hold a lock on an SSO session for the duration of the block."""
    config = self.lock_configs.get(operation, self.lock_configs["session_creation"])
    lock_key = f"sso_session:{session_id}:{operation}"

    lock = DistributedLock(
      redis_client=self.redis, lock_key=lock_key, ttl_seconds=config["ttl"]
    )

    try:
      # acquire() backs off with time.sleep; keep that off the event loop.
      result = await asyncio.to_thread(
        lock.acquire, blocking=True, timeout=config["timeout"]
      )

      if not result.acquired:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "sso_session_lock_failed",
            "session_id": session_id[:8] + "...",
            "operation": operation,
            "error": result.error_message,
          },
          risk_level="high",
        )
        raise RuntimeError(
          f"Failed to acquire SSO session lock: {result.error_message}"
        )

      logger.debug(f"Acquired SSO session lock for {operation}: {session_id[:8]}...")
      yield lock

    finally:
      if lock.acquired:
        lock.release()
        logger.debug(f"Released SSO session lock for {operation}: {session_id[:8]}...")


def get_sso_lock_manager() -> SSOTokenLockManager | None:
  """Build an `SSOTokenLockManager`, or None when Redis is unreachable."""
  try:
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

    redis_client = create_redis_client(ValkeyDatabase.LOCKS, decode_responses=True)

    redis_client.ping()

    return SSOTokenLockManager(redis_client)

  except Exception as e:
    logger.error(f"Failed to initialize SSO lock manager: {e}")
    return None
