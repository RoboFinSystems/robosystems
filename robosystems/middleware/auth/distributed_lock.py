"""Redis-based distributed locking for SSO token and auth operations.

Serializes token operations across API instances. Locks always carry a TTL,
so a process that dies holding one cannot deadlock the others, and release is
an atomic compare-and-delete against the holder's lock_id so a lock that
already expired and was re-acquired is never released by its previous owner.
"""

import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, cast

import redis
from redis.exceptions import RedisError

from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType


@dataclass
class LockAcquisitionResult:
  """Result of lock acquisition attempt."""

  acquired: bool
  lock_id: str | None
  holder_id: str | None
  ttl_remaining: int | None
  error_message: str | None = None
  # True when the attempt never got an answer from Redis, so "not acquired"
  # says nothing about whether the lock is held. Callers that fail closed use
  # this to report "lock service unavailable" rather than "already in progress".
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
        # NX + EX in one call: acquisition and expiry are set atomically, so a
        # crash between the two can never leave an immortal lock.
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
          # `redis.get()` returns bytes or str depending on the client's
          # `decode_responses` setting; `.decode()` on a str would raise.
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
        wait_time = min(0.01 * (2**retry_count), 0.5)  # Max 500ms
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
    """Release the lock, but only if this instance is still the holder.

    The Lua compare-and-delete is what makes that safe: without it, a lock
    that expired and was re-acquired by someone else would be deleted here.
    """
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


def release_lock_by_id(
  redis_client: redis.Redis,
  lock_key: str,
  lock_id: str,
) -> bool:
  """Release a lock acquired in a different process.

  `DistributedLock.release()` needs the acquiring object — its `acquired`
  flag doesn't cross process boundaries. When one process acquires and
  another releases (an API endpoint and the Dagster job it launched), pass
  the `lock_id` from the acquirer's `LockAcquisitionResult` here.

  Returns True if released, False if the lock was already gone or is held
  by a different lock_id — the same compare-and-delete guarantee as
  `DistributedLock.release()`.

  `lock_key` is the unprefixed key; the `lock:` prefix is applied here to
  match `DistributedLock.__init__`.
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
      "token_verification": {"ttl": 10, "timeout": 5},  # Quick verification
      "token_exchange": {"ttl": 30, "timeout": 10},  # Exchange operations
      "session_creation": {"ttl": 15, "timeout": 8},  # Session management
      "cleanup": {"ttl": 60, "timeout": 30},  # Cleanup operations
    }

  @asynccontextmanager
  async def lock_sso_token(self, token_id: str, operation: str = "verification"):
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
      result = lock.acquire(blocking=True, timeout=config["timeout"])

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
      result = lock.acquire(blocking=True, timeout=config["timeout"])

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

  def cleanup_expired_locks(self) -> dict[str, Any]:
    """Delete SSO locks that somehow have no TTL, and report the counts.

    Locks with a live TTL are left alone — Redis expires those itself.
    """
    try:
      stats = {
        "sso_token_locks_cleaned": 0,
        "sso_session_locks_cleaned": 0,
        "total_locks_cleaned": 0,
      }

      sso_token_pattern = "lock:sso_token:*"
      sso_session_pattern = "lock:sso_session:*"

      for pattern, stat_key in [
        (sso_token_pattern, "sso_token_locks_cleaned"),
        (sso_session_pattern, "sso_session_locks_cleaned"),
      ]:
        lock_keys = cast(list, self.redis.keys(pattern))

        for lock_key in lock_keys:
          try:
            ttl = cast(int, self.redis.ttl(lock_key))
            if ttl == -1:  # Exists with no expiry
              self.redis.delete(lock_key)
              stats[stat_key] += 1
            elif ttl == -2:  # Already gone
              continue
          except RedisError:
            continue

      stats["total_locks_cleaned"] = (
        stats["sso_token_locks_cleaned"] + stats["sso_session_locks_cleaned"]
      )

      if stats["total_locks_cleaned"] > 0:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_SUCCESS,
          details={
            "action": "sso_lock_cleanup",
            "stats": stats,
          },
          risk_level="low",
        )

      return stats

    except RedisError as e:
      logger.error(f"Error during SSO lock cleanup: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "sso_lock_cleanup_failed",
          "error": str(e),
        },
        risk_level="medium",
      )
      return {
        "error": str(e),
        "sso_token_locks_cleaned": 0,
        "sso_session_locks_cleaned": 0,
        "total_locks_cleaned": 0,
      }


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
