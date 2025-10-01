"""Tests for distributed lock module."""

from unittest.mock import Mock, patch
import pytest
import redis
from redis.exceptions import RedisError

from robosystems.middleware.auth.distributed_lock import (
  DistributedLock,
  LockAcquisitionResult,
  SSOTokenLockManager,
  get_sso_lock_manager,
)

# Mark entire test module as slow due to distributed lock operations
pytestmark = pytest.mark.slow


@pytest.fixture
def mock_redis():
  """Create a mock Redis client."""
  client = Mock(spec=redis.Redis)
  # Set up default behaviors
  client.set.return_value = True
  client.get.return_value = None
  client.ttl.return_value = 30
  client.eval.return_value = 1
  client.keys.return_value = []
  client.ping.return_value = True
  return client


@pytest.fixture
def distributed_lock(mock_redis):
  """Create a DistributedLock instance with mock Redis."""
  return DistributedLock(mock_redis, "test_lock", ttl_seconds=30)


@pytest.fixture
def sso_lock_manager(mock_redis):
  """Create an SSOTokenLockManager with mock Redis."""
  return SSOTokenLockManager(mock_redis)


class TestLockAcquisitionResult:
  """Tests for LockAcquisitionResult dataclass."""

  def test_successful_acquisition(self):
    """Test successful lock acquisition result."""
    result = LockAcquisitionResult(
      acquired=True,
      lock_id="lock-123",
      holder_id="lock-123",
      ttl_remaining=30,
    )
    assert result.acquired is True
    assert result.lock_id == "lock-123"
    assert result.holder_id == "lock-123"
    assert result.ttl_remaining == 30
    assert result.error_message is None

  def test_failed_acquisition(self):
    """Test failed lock acquisition result."""
    result = LockAcquisitionResult(
      acquired=False,
      lock_id=None,
      holder_id="other-lock",
      ttl_remaining=15,
      error_message="Lock is held by another process",
    )
    assert result.acquired is False
    assert result.lock_id is None
    assert result.holder_id == "other-lock"
    assert result.ttl_remaining == 15
    assert result.error_message == "Lock is held by another process"


class TestDistributedLock:
  """Tests for DistributedLock class."""

  def test_initialization(self, mock_redis):
    """Test lock initialization."""
    lock = DistributedLock(mock_redis, "my_lock", ttl_seconds=60)

    assert lock.redis == mock_redis
    assert lock.lock_key == "lock:my_lock"
    assert lock.ttl_seconds == 60
    assert lock.acquired is False
    assert lock.acquisition_time is None
    # Lock ID should be a UUID
    assert len(lock.lock_id) == 36

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_non_blocking_success(
    self, mock_audit_logger, distributed_lock, mock_redis
  ):
    """Test successful non-blocking lock acquisition."""
    mock_redis.set.return_value = True

    result = distributed_lock.acquire(blocking=False)

    assert result.acquired is True
    assert result.lock_id == distributed_lock.lock_id
    assert result.holder_id == distributed_lock.lock_id
    assert result.ttl_remaining == 30
    assert distributed_lock.acquired is True
    assert distributed_lock.acquisition_time is not None

    # Verify Redis call
    mock_redis.set.assert_called_once_with(
      "lock:test_lock",
      distributed_lock.lock_id,
      nx=True,
      ex=30,
    )

    # Verify security logging
    mock_audit_logger.log_security_event.assert_called_once()

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_non_blocking_failure(
    self, mock_audit_logger, distributed_lock, mock_redis
  ):
    """Test failed non-blocking lock acquisition."""
    mock_redis.set.return_value = False
    mock_redis.get.return_value = b"other-lock-id"
    mock_redis.ttl.return_value = 15

    result = distributed_lock.acquire(blocking=False)

    assert result.acquired is False
    assert result.lock_id is None
    assert result.holder_id == "other-lock-id"
    assert result.ttl_remaining == 15
    assert "Lock is currently held" in result.error_message
    assert distributed_lock.acquired is False

  @patch("robosystems.middleware.auth.distributed_lock.time.sleep")
  @patch("robosystems.middleware.auth.distributed_lock.time.time")
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_blocking_with_retry(
    self, mock_audit_logger, mock_time, mock_sleep, distributed_lock, mock_redis
  ):
    """Test blocking acquisition with retry."""
    # First two attempts fail, third succeeds
    mock_redis.set.side_effect = [False, False, True]
    mock_time.return_value = 1000.0

    result = distributed_lock.acquire(blocking=True, timeout=10)

    assert result.acquired is True
    assert distributed_lock.acquired is True
    assert mock_redis.set.call_count == 3
    assert mock_sleep.call_count == 2

    # Verify exponential backoff
    sleep_calls = mock_sleep.call_args_list
    assert sleep_calls[0][0][0] < sleep_calls[1][0][0]  # Increasing wait times

  @patch("robosystems.middleware.auth.distributed_lock.time.sleep")
  @patch("robosystems.middleware.auth.distributed_lock.time.time")
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_blocking_timeout(
    self, mock_audit_logger, mock_time, mock_sleep, distributed_lock, mock_redis
  ):
    """Test blocking acquisition timeout."""
    mock_redis.set.return_value = False
    # Simulate time progression past timeout
    mock_time.side_effect = [0, 0, 5, 11]  # Start time, then increasing times

    result = distributed_lock.acquire(blocking=True, timeout=10)

    assert result.acquired is False
    assert "timed out after 10 seconds" in result.error_message
    assert distributed_lock.acquired is False

    # Verify security event logged
    mock_audit_logger.log_security_event.assert_called()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["details"]["action"] == "distributed_lock_timeout"

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_redis_error(self, mock_audit_logger, distributed_lock, mock_redis):
    """Test Redis error during acquisition."""
    mock_redis.set.side_effect = RedisError("Connection lost")

    result = distributed_lock.acquire(blocking=False)

    assert result.acquired is False
    assert "Redis error: Connection lost" in result.error_message
    assert distributed_lock.acquired is False

    # Verify error logging
    mock_audit_logger.log_security_event.assert_called()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["risk_level"] == "high"

  @patch("robosystems.middleware.auth.distributed_lock.time.sleep")
  @patch("robosystems.middleware.auth.distributed_lock.time.time")
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_acquire_max_retries_exceeded(
    self, mock_audit_logger, mock_time, mock_sleep, distributed_lock, mock_redis
  ):
    """Test max retries exceeded in blocking mode."""
    mock_redis.set.return_value = False
    mock_time.side_effect = [0] + [i * 0.1 for i in range(100)]

    result = distributed_lock.acquire(blocking=True, timeout=None)

    assert result.acquired is False
    assert "Failed to acquire lock after 50 retries" in result.error_message
    assert mock_redis.set.call_count == 50

  @patch("robosystems.middleware.auth.distributed_lock.time.time")
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_release_success(
    self, mock_audit_logger, mock_time, distributed_lock, mock_redis
  ):
    """Test successful lock release."""
    # Acquire lock first
    distributed_lock.acquired = True
    distributed_lock.acquisition_time = 1000.0
    mock_time.return_value = 1010.0  # 10 seconds later
    mock_redis.eval.return_value = 1

    result = distributed_lock.release()

    assert result is True
    assert distributed_lock.acquired is False

    # Verify Lua script execution
    mock_redis.eval.assert_called_once()
    call_args = mock_redis.eval.call_args
    assert 'redis.call("get", KEYS[1]) == ARGV[1]' in call_args[0][0]
    assert "lock:test_lock" in call_args[0]

    # Verify security logging
    mock_audit_logger.log_security_event.assert_called_once()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["details"]["action"] == "distributed_lock_released"
    assert call_args[1]["details"]["lock_duration_seconds"] == 10.0

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_release_not_owner(self, mock_audit_logger, distributed_lock, mock_redis):
    """Test release when not lock owner."""
    distributed_lock.acquired = True
    mock_redis.eval.return_value = 0  # Lua script returns 0 when not owner

    result = distributed_lock.release()

    assert result is False

    # Verify security event
    mock_audit_logger.log_security_event.assert_called_once()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["details"]["reason"] == "not_current_holder"

  def test_release_not_acquired(self, distributed_lock):
    """Test release when lock was never acquired."""
    result = distributed_lock.release()
    assert result is False

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_release_redis_error(self, mock_audit_logger, distributed_lock, mock_redis):
    """Test Redis error during release."""
    distributed_lock.acquired = True
    mock_redis.eval.side_effect = RedisError("Connection lost")

    result = distributed_lock.release()

    assert result is False

    # Verify error logging
    mock_audit_logger.log_security_event.assert_called_once()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["risk_level"] == "high"

  def test_extend_success(self, distributed_lock, mock_redis):
    """Test successful lock extension."""
    distributed_lock.acquired = True
    distributed_lock.ttl_seconds = 30
    mock_redis.eval.return_value = True

    result = distributed_lock.extend(15)

    assert result is True
    assert distributed_lock.ttl_seconds == 45

    # Verify Lua script execution
    mock_redis.eval.assert_called_once()
    call_args = mock_redis.eval.call_args
    assert 'redis.call("expire", KEYS[1], ARGV[2])' in call_args[0][0]
    assert "45" in call_args[0]

  def test_extend_not_owner(self, distributed_lock, mock_redis):
    """Test extend when not lock owner."""
    distributed_lock.acquired = True
    mock_redis.eval.return_value = False

    result = distributed_lock.extend(15)

    assert result is False
    assert distributed_lock.ttl_seconds == 30  # Unchanged

  def test_extend_not_acquired(self, distributed_lock):
    """Test extend when lock was never acquired."""
    result = distributed_lock.extend(15)
    assert result is False

  def test_extend_redis_error(self, distributed_lock, mock_redis):
    """Test Redis error during extension."""
    distributed_lock.acquired = True
    mock_redis.eval.side_effect = RedisError("Connection lost")

    result = distributed_lock.extend(15)

    assert result is False

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_context_manager_success(
    self, mock_audit_logger, distributed_lock, mock_redis
  ):
    """Test using lock as context manager."""
    mock_redis.set.return_value = True

    with distributed_lock as lock:
      assert lock.acquired is True
      assert lock == distributed_lock

    # Lock should be released after context
    mock_redis.eval.assert_called_once()

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_context_manager_acquisition_failure(
    self, mock_audit_logger, distributed_lock, mock_redis
  ):
    """Test context manager when acquisition fails."""
    mock_redis.set.return_value = False

    with pytest.raises(RuntimeError, match="Failed to acquire lock"):
      with distributed_lock:
        pass


class TestSSOTokenLockManager:
  """Tests for SSOTokenLockManager."""

  def test_initialization(self, mock_redis):
    """Test SSO lock manager initialization."""
    manager = SSOTokenLockManager(mock_redis)

    assert manager.redis == mock_redis
    assert "token_verification" in manager.lock_configs
    assert "token_exchange" in manager.lock_configs
    assert "session_creation" in manager.lock_configs
    assert "cleanup" in manager.lock_configs

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  async def test_lock_sso_token_success(
    self, mock_audit_logger, sso_lock_manager, mock_redis
  ):
    """Test successful SSO token locking."""
    mock_redis.set.return_value = True
    mock_redis.eval.return_value = 1

    async with sso_lock_manager.lock_sso_token("token-123", "verification") as lock:
      assert lock.acquired is True
      assert "sso_token:token-123:verification" in lock.lock_key

    # Lock should be released
    mock_redis.eval.assert_called()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  async def test_lock_sso_token_failure(
    self, mock_audit_logger, sso_lock_manager, mock_redis
  ):
    """Test failed SSO token locking."""
    mock_redis.set.return_value = False

    with pytest.raises(RuntimeError, match="Failed to acquire SSO token lock"):
      async with sso_lock_manager.lock_sso_token("token-123", "verification"):
        pass

    # Verify security event logged
    mock_audit_logger.log_security_event.assert_called()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["details"]["action"] == "sso_token_lock_failed"

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  async def test_lock_sso_session_success(
    self, mock_audit_logger, sso_lock_manager, mock_redis
  ):
    """Test successful SSO session locking."""
    mock_redis.set.return_value = True
    mock_redis.eval.return_value = 1

    async with sso_lock_manager.lock_sso_session(
      "session-456", "session_creation"
    ) as lock:
      assert lock.acquired is True
      assert "sso_session:session-456:session_creation" in lock.lock_key

    # Lock should be released
    mock_redis.eval.assert_called()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  async def test_lock_sso_session_failure(
    self, mock_audit_logger, sso_lock_manager, mock_redis
  ):
    """Test failed SSO session locking."""
    mock_redis.set.return_value = False

    with pytest.raises(RuntimeError, match="Failed to acquire SSO session lock"):
      async with sso_lock_manager.lock_sso_session("session-456"):
        pass

    # Verify security event logged
    mock_audit_logger.log_security_event.assert_called()

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_cleanup_expired_locks(self, mock_audit_logger, sso_lock_manager, mock_redis):
    """Test cleanup of expired locks."""
    # Setup mock data
    mock_redis.keys.side_effect = [
      [b"lock:sso_token:abc", b"lock:sso_token:def"],  # Token locks
      [b"lock:sso_session:123", b"lock:sso_session:456"],  # Session locks
    ]
    # TTL returns: -1 means no expiry, -2 means doesn't exist, >0 means valid
    mock_redis.ttl.side_effect = [-1, 30, -1, -2]  # First and third have no expiry
    mock_redis.delete.return_value = 1

    stats = sso_lock_manager.cleanup_expired_locks()

    assert stats["sso_token_locks_cleaned"] == 1
    assert stats["sso_session_locks_cleaned"] == 1
    assert stats["total_locks_cleaned"] == 2

    # Verify delete was called for expired locks
    assert mock_redis.delete.call_count == 2

    # Verify security logging
    mock_audit_logger.log_security_event.assert_called_once()

  @patch("robosystems.middleware.auth.distributed_lock.SecurityAuditLogger")
  def test_cleanup_redis_error(self, mock_audit_logger, sso_lock_manager, mock_redis):
    """Test cleanup with Redis error."""
    mock_redis.keys.side_effect = RedisError("Connection lost")

    stats = sso_lock_manager.cleanup_expired_locks()

    assert "error" in stats
    assert stats["total_locks_cleaned"] == 0

    # Verify error logging
    mock_audit_logger.log_security_event.assert_called_once()
    call_args = mock_audit_logger.log_security_event.call_args
    assert call_args[1]["details"]["action"] == "sso_lock_cleanup_failed"

  def test_cleanup_no_expired_locks(self, sso_lock_manager, mock_redis):
    """Test cleanup when no locks are expired."""
    mock_redis.keys.side_effect = [[], []]  # No locks found

    stats = sso_lock_manager.cleanup_expired_locks()

    assert stats["total_locks_cleaned"] == 0
    assert stats["sso_token_locks_cleaned"] == 0
    assert stats["sso_session_locks_cleaned"] == 0


class TestGetSSOLockManager:
  """Tests for get_sso_lock_manager function."""

  @patch("robosystems.config.valkey_registry.create_redis_client")
  def test_get_sso_lock_manager_success(self, mock_create_redis_client):
    """Test successful SSO lock manager creation."""
    mock_redis = Mock()
    mock_redis.ping.return_value = True
    mock_create_redis_client.return_value = mock_redis

    manager = get_sso_lock_manager()

    assert isinstance(manager, SSOTokenLockManager)
    assert manager.redis == mock_redis

    # Verify correct database was requested
    from robosystems.config.valkey_registry import ValkeyDatabase

    mock_create_redis_client.assert_called_once_with(
      ValkeyDatabase.DISTRIBUTED_LOCKS, decode_responses=True
    )

  @patch("robosystems.middleware.auth.distributed_lock.logger")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  def test_get_sso_lock_manager_connection_error(
    self, mock_create_redis_client, mock_logger
  ):
    """Test SSO lock manager creation with connection error."""
    mock_redis = Mock()
    mock_redis.ping.side_effect = RedisError("Connection refused")
    mock_create_redis_client.return_value = mock_redis

    manager = get_sso_lock_manager()

    assert manager is None
    mock_logger.error.assert_called_once()

  @patch("robosystems.middleware.auth.distributed_lock.logger")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  def test_get_sso_lock_manager_import_error(
    self, mock_create_redis_client, mock_logger
  ):
    """Test SSO lock manager creation with import error."""
    mock_create_redis_client.side_effect = ImportError("Module not found")

    manager = get_sso_lock_manager()

    assert manager is None
    mock_logger.error.assert_called_once()
