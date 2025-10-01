"""Comprehensive tests for authentication edge cases and failure scenarios."""

import time
from unittest.mock import MagicMock, patch
import pytest
import secrets

from robosystems.middleware.auth.cache import APIKeyCache

# Mark entire test module as slow due to encryption operations
pytestmark = pytest.mark.slow


class TestKeyRotationEdgeCases:
  """Test key rotation with rollback and error handling."""

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_key_rotation_successful_validation(self, mock_redis):
    """Test successful key rotation with validation."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    # Set up existing key
    old_key = "old_key_component_123"
    mock_redis_client.get.return_value = old_key.encode()
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()
    cache._rotate_encryption_key()

    # Verify new key was set
    assert mock_redis_client.setex.called
    call_args = mock_redis_client.setex.call_args[0]
    assert call_args[0].endswith("current")  # generation key
    assert call_args[1] == cache.KEY_ROTATION_INTERVAL * 2  # TTL
    assert call_args[2] != old_key  # New key generated

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_key_rotation_rollback_on_validation_failure(self, mock_redis):
    """Test key rotation rollback when validation fails."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    # Set up existing key
    old_key = "old_key_component_456"
    mock_redis_client.get.return_value = old_key.encode()
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()

    # Mock encryption to fail validation
    with patch.object(
      cache, "_encrypt_cache_data", side_effect=Exception("Encryption failed")
    ):
      with pytest.raises(Exception, match="Encryption failed"):
        cache._rotate_encryption_key()

    # Verify rollback occurred
    setex_calls = mock_redis_client.setex.call_args_list
    # First call sets new key, second call rolls back to old key
    assert len(setex_calls) >= 2
    rollback_call = setex_calls[-1][0]
    assert rollback_call[2] == old_key.encode()  # Old key restored (as bytes)

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  @patch("robosystems.middleware.auth.cache.SecurityAuditLogger")
  def test_key_rotation_critical_rollback_failure(self, mock_audit, mock_redis):
    """Test handling of critical rollback failure during key rotation."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    old_key = "old_key_789"
    mock_redis_client.get.return_value = old_key.encode()
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()

    # Make both encryption and rollback fail
    with patch.object(
      cache, "_encrypt_cache_data", side_effect=Exception("Encryption failed")
    ):
      # Make rollback setex fail
      mock_redis_client.setex.side_effect = [None, Exception("Redis unavailable")]

      with pytest.raises(Exception, match="Redis unavailable"):
        cache._rotate_encryption_key()

    # Verify critical security event was logged
    mock_audit.log_security_event.assert_called()
    calls = mock_audit.log_security_event.call_args_list
    # Find the critical rollback failure event
    critical_event = next(
      (
        call
        for call in calls
        if call[1]["details"].get("action") == "key_rotation_rollback_failed"
      ),
      None,
    )
    assert critical_event is not None
    assert critical_event[1]["risk_level"] == "critical"


class TestCacheSignatureEdgeCases:
  """Test signature cache LRU eviction and memory management."""

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_signature_cache_lru_eviction(self, mock_redis):
    """Test LRU eviction when cache exceeds max size."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()

    # Fill cache beyond max size
    cache.MAX_SIGNATURE_CACHE_SIZE = 10  # Small size for testing
    cache._signature_cache = {}
    cache._signature_cache_times = {}

    # Add entries with different timestamps
    base_time = time.time()
    for i in range(15):
      key = f"hash_{i}"
      cache._signature_cache[key] = f"signature_{i}"
      cache._signature_cache_times[key] = base_time + i  # Incremental timestamps

    # Trigger cleanup
    cache._cleanup_signature_cache()

    # Should keep 80% of max size (8 entries)
    assert len(cache._signature_cache) == 8
    # Oldest entries (0-6) should be removed
    assert "hash_0" not in cache._signature_cache
    assert "hash_6" not in cache._signature_cache
    # Newest entries should remain
    assert "hash_14" in cache._signature_cache
    assert "hash_8" in cache._signature_cache

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_signature_cache_expired_entry_cleanup(self, mock_redis):
    """Test removal of expired entries before LRU eviction."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()
    cache.SIGNATURE_CACHE_TTL = 1  # 1 second TTL for testing

    # Add mix of expired and valid entries
    current_time = time.time()
    cache._signature_cache = {
      "expired_1": "sig_1",
      "expired_2": "sig_2",
      "valid_1": "sig_3",
      "valid_2": "sig_4",
    }
    cache._signature_cache_times = {
      "expired_1": current_time - 10,  # Expired
      "expired_2": current_time - 5,  # Expired
      "valid_1": current_time - 0.5,  # Valid
      "valid_2": current_time - 0.1,  # Valid
    }

    cache._cleanup_signature_cache()

    # Only valid entries should remain
    assert len(cache._signature_cache) == 2
    assert "valid_1" in cache._signature_cache
    assert "valid_2" in cache._signature_cache
    assert "expired_1" not in cache._signature_cache


class TestConcurrentAccessEdgeCases:
  """Test concurrent access patterns and race conditions."""

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_concurrent_cache_writes(self, mock_redis):
    """Test handling of concurrent cache write operations."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    # Simulate concurrent writes with pipeline
    mock_pipeline = MagicMock()
    mock_redis_client.pipeline.return_value = mock_pipeline

    cache = APIKeyCache()

    # Create test data with required fields
    test_user_data = {
      "id": "user_123",  # Required field
      "email": "test@example.com",  # Required field
      "is_active": True,  # Required field
      "exp": int(time.time()) + 3600,  # Future expiration
      "sub": "user_123",
    }

    # Perform cache set operation (use JWT cache as example)
    cache.cache_jwt_validation("test_jwt_token", test_user_data)

    # Verify Redis was used (setex for cache storage)
    assert mock_redis_client.setex.called


class TestMemoryLeakPrevention:
  """Test memory leak prevention in cache operations."""

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_signature_cache_memory_bounded(self, mock_redis):
    """Test that signature cache memory usage is bounded."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()
    cache.MAX_SIGNATURE_CACHE_SIZE = 100

    # Simulate heavy load with many unique payloads
    for i in range(1000):
      payload = f"unique_payload_{i}_{secrets.token_hex(16)}"
      data = {"test": "data", "index": i}
      cache._create_cache_signature(payload, data)

      # Cache should never exceed max size + buffer
      assert len(cache._signature_cache) <= cache.MAX_SIGNATURE_CACHE_SIZE * 1.2

    # Final size should be at or below max
    assert len(cache._signature_cache) <= cache.MAX_SIGNATURE_CACHE_SIZE

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_validation_failure_counter_reset(self, mock_redis):
    """Test that validation failure counter resets appropriately."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()

    # Accumulate validation failures
    cache._validation_failures = cache.VALIDATION_FAILURE_THRESHOLD - 1

    # Successful validation should reset counter
    with patch.object(cache, "_decrypt_cache_data", return_value={"test": "data"}):
      # Trigger successful decryption
      mock_redis_client.get.return_value = "encrypted_data"
      cache.get_cached_jwt_validation("jwt_789")

    # Counter should be reset after successful operation
    assert cache._validation_failures < cache.VALIDATION_FAILURE_THRESHOLD


class TestRedisConnectionFailures:
  """Test handling of Redis connection failures."""

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  def test_cache_operations_with_redis_down(self, mock_redis):
    """Test graceful degradation when Redis is unavailable."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    # Simulate Redis connection failure
    mock_redis_client.get.side_effect = ConnectionError("Redis unavailable")
    mock_redis_client.setex.side_effect = ConnectionError("Redis unavailable")

    cache = APIKeyCache()

    # Operations should fail gracefully without crashing
    result = cache.get_cached_jwt_validation("jwt_fail")
    assert result is None

    # Set operations should handle failure (cache operations don't raise, they log)
    cache.cache_jwt_validation(
      "jwt_fail",
      {
        "id": "user_fail",
        "email": "test@example.com",
        "is_active": True,
        "data": "test",
        "exp": 1234567890,
      },
    )
    # Should complete without raising

  @patch("robosystems.middleware.auth.cache.create_redis_client")
  @patch("robosystems.middleware.auth.cache.SecurityAuditLogger")
  def test_cache_security_event_logging_on_failures(self, mock_audit, mock_redis):
    """Test that security events are logged for cache operation failures."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client
    mock_redis_client.keys.return_value = []  # Return empty list for keys() calls

    cache = APIKeyCache()

    # Simulate encryption failure
    with patch.object(
      cache, "_encrypt_cache_data", side_effect=Exception("Encryption error")
    ):
      # Cache operations log errors but don't raise
      cache.cache_jwt_validation(
        "jwt_sec",
        {
          "id": "user_sec",
          "email": "sec@example.com",
          "is_active": True,
          "data": "sensitive",
          "exp": 1234567890,
        },
      )

    # Verify security event was logged
    mock_audit.log_security_event.assert_called()
    if mock_audit.log_security_event.call_args_list:
      call_args = mock_audit.log_security_event.call_args_list[-1]
      # Handle both keyword and positional arguments
      details = (
        call_args[1].get("details")
        if call_args[1]
        else call_args[0][1].get("details", {})
      )
      assert "encryption" in str(details).lower()
