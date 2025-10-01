"""
Distributed locking service for SSO tokens and authentication operations.

This module provides Redis-based distributed locking to prevent race conditions
in SSO token operations, ensuring atomic operations across multiple instances.
"""

import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Dict, cast, Any
from dataclasses import dataclass

import redis
from redis.exceptions import RedisError

from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType


@dataclass
class LockAcquisitionResult:
  """Result of lock acquisition attempt."""

  acquired: bool
  lock_id: Optional[str]
  holder_id: Optional[str]
  ttl_remaining: Optional[int]
  error_message: Optional[str] = None


class DistributedLock:
  """
  Redis-based distributed lock with automatic expiry and deadlock prevention.

  Features:
  - Atomic lock acquisition and release
  - Automatic expiry to prevent deadlocks
  - Lock ownership verification
  - Retry mechanisms with exponential backoff
  - Comprehensive security logging
  """

  def __init__(self, redis_client: redis.Redis, lock_key: str, ttl_seconds: int = 30):
    """
    Initialize distributed lock.

    Args:
        redis_client: Redis client instance
        lock_key: Unique key for the lock
        ttl_seconds: Lock expiry time in seconds
    """
    self.redis = redis_client
    self.lock_key = f"lock:{lock_key}"
    self.ttl_seconds = ttl_seconds
    self.lock_id = str(uuid.uuid4())
    self.acquired = False
    self.acquisition_time: Optional[float] = None

  def acquire(
    self, blocking: bool = True, timeout: Optional[float] = None
  ) -> LockAcquisitionResult:
    """
    Acquire the distributed lock.

    Args:
        blocking: Whether to block until lock is available
        timeout: Maximum time to wait for lock (seconds)

    Returns:
        LockAcquisitionResult with acquisition status
    """
    start_time = time.time()
    max_retries = 50 if blocking else 1
    retry_count = 0

    while retry_count < max_retries:
      try:
        # Use SET with NX (only if not exists) and EX (expiry) for atomic operation
        result = self.redis.set(
          self.lock_key,
          self.lock_id,
          nx=True,  # Only set if key doesn't exist
          ex=self.ttl_seconds,  # Set expiry
        )

        if result:
          # Lock acquired successfully
          self.acquired = True
          self.acquisition_time = time.time()

          # Log successful lock acquisition
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

        # Lock is held by another process
        if not blocking:
          current_holder = cast(Optional[bytes], self.redis.get(self.lock_key))
          ttl = cast(Optional[int], self.redis.ttl(self.lock_key))

          return LockAcquisitionResult(
            acquired=False,
            lock_id=None,
            holder_id=current_holder.decode("utf-8") if current_holder else None,
            ttl_remaining=ttl if ttl and ttl > 0 else None,
            error_message="Lock is currently held by another process",
          )

        # Check timeout
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

        # Wait before retry with exponential backoff
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
          error_message=f"Redis error: {str(e)}",
        )

    # Max retries exceeded
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
    """
    Release the distributed lock safely.

    Only releases the lock if we are the current holder.

    Returns:
        True if lock was released, False otherwise
    """
    if not self.acquired:
      return False

    try:
      # Use Lua script for atomic compare-and-delete operation
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

        # Log successful lock release
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
        # We don't own the lock (expired or taken by another process)
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
    """
    Extend the lock expiry time.

    Args:
        additional_seconds: Additional time to extend the lock

    Returns:
        True if lock was extended, False otherwise
    """
    if not self.acquired:
      return False

    try:
      # Use Lua script for atomic extend operation
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
    """Context manager entry."""
    result = self.acquire()
    if not result.acquired:
      raise RuntimeError(f"Failed to acquire lock: {result.error_message}")
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit."""
    self.release()


class SSOTokenLockManager:
  """
  Specialized lock manager for SSO token operations.

  Provides high-level locking primitives for common SSO token operations
  with appropriate timeouts and security logging.
  """

  def __init__(self, redis_client: redis.Redis):
    """
    Initialize SSO token lock manager.

    Args:
        redis_client: Redis client instance
    """
    self.redis = redis_client

    # Lock configuration for different operations
    self.lock_configs = {
      "token_verification": {"ttl": 10, "timeout": 5},  # Quick verification
      "token_exchange": {"ttl": 30, "timeout": 10},  # Exchange operations
      "session_creation": {"ttl": 15, "timeout": 8},  # Session management
      "cleanup": {"ttl": 60, "timeout": 30},  # Cleanup operations
    }

  @asynccontextmanager
  async def lock_sso_token(self, token_id: str, operation: str = "verification"):
    """
    Async context manager for SSO token locking.

    Args:
        token_id: SSO token ID to lock
        operation: Type of operation (verification, exchange, session_creation, cleanup)

    Yields:
        DistributedLock instance

    Raises:
        RuntimeError: If lock cannot be acquired
    """
    config = self.lock_configs.get(operation, self.lock_configs["token_verification"])
    lock_key = f"sso_token:{token_id}:{operation}"

    lock = DistributedLock(
      redis_client=self.redis, lock_key=lock_key, ttl_seconds=config["ttl"]
    )

    try:
      # Acquire lock with timeout
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
      # Always attempt to release the lock
      if lock.acquired:
        lock.release()
        logger.debug(f"Released SSO token lock for {operation}: {token_id[:8]}...")

  @asynccontextmanager
  async def lock_sso_session(
    self, session_id: str, operation: str = "session_creation"
  ):
    """
    Async context manager for SSO session locking.

    Args:
        session_id: SSO session ID to lock
        operation: Type of operation

    Yields:
        DistributedLock instance
    """
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

  def cleanup_expired_locks(self) -> Dict[str, Any]:
    """
    Clean up expired SSO locks (maintenance operation).

    Returns:
        Dictionary with cleanup statistics
    """
    try:
      stats = {
        "sso_token_locks_cleaned": 0,
        "sso_session_locks_cleaned": 0,
        "total_locks_cleaned": 0,
      }

      # Find all SSO-related locks
      sso_token_pattern = "lock:sso_token:*"
      sso_session_pattern = "lock:sso_session:*"

      for pattern, stat_key in [
        (sso_token_pattern, "sso_token_locks_cleaned"),
        (sso_session_pattern, "sso_session_locks_cleaned"),
      ]:
        lock_keys = cast(list, self.redis.keys(pattern))

        for lock_key in lock_keys:
          try:
            # Check if lock is expired
            ttl = cast(int, self.redis.ttl(lock_key))
            if ttl == -1:  # Key exists but has no expiry
              self.redis.delete(lock_key)
              stats[stat_key] += 1
            elif ttl == -2:  # Key doesn't exist
              continue
            # If ttl > 0, lock is still valid
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


def get_sso_lock_manager() -> Optional[SSOTokenLockManager]:
  """
  Get the global SSO lock manager instance.

  Returns:
      SSOTokenLockManager instance or None if Redis is unavailable
  """
  try:
    # Use DB 2 for auth cache (same as SSO tokens)
    # Use distributed locks database from registry
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

    # Use factory method to handle SSL params correctly
    redis_client = create_redis_client(
      ValkeyDatabase.DISTRIBUTED_LOCKS, decode_responses=True
    )

    # Test connection
    redis_client.ping()

    return SSOTokenLockManager(redis_client)

  except Exception as e:
    logger.error(f"Failed to initialize SSO lock manager: {e}")
    return None
