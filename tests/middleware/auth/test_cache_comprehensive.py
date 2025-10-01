"""
Comprehensive tests for authentication cache functionality.

Tests all aspects of the APIKeyCache class including encryption,
signature verification, rate limiting, and security features.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta
import json
import base64
import secrets
from cryptography.fernet import InvalidToken

from robosystems.middleware.auth.cache import APIKeyCache
from robosystems.security import SecurityEventType

# Mark entire test module as slow due to encryption operations
pytestmark = pytest.mark.slow


class TestAPIKeyCacheInitialization:
  """Test APIKeyCache initialization and configuration."""

  def test_cache_initialization_success(self):
    """Test successful cache initialization."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()

      assert cache._redis is None  # Lazy initialization
      assert cache.ttl is not None
      assert cache.jwt_ttl is not None
      assert cache._encryption_key is None  # Lazy initialization
      assert cache._cipher is None  # Lazy initialization
      assert cache._validation_failures == 0

  def test_redis_connection_lazy_loading(self):
    """Test Redis connection lazy loading."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()
      assert cache._redis is None

      # Access redis property triggers connection
      redis_conn = cache.redis
      assert redis_conn is mock_redis
      MockRedis.from_url.assert_called_once()
      mock_redis.ping.assert_called_once()

  def test_redis_connection_failure(self):
    """Test Redis connection failure handling."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      MockRedis.from_url.side_effect = Exception("Connection failed")

      cache = APIKeyCache()
      with pytest.raises(Exception, match="Connection failed"):
        _ = cache.redis

  def test_encryption_key_derivation(self):
    """Test encryption key derivation."""
    cache = APIKeyCache()

    # Verify key is properly derived (accessing property triggers derivation)
    assert len(cache.encryption_key) == 44  # Base64 encoded 32-byte key
    assert cache.cipher is not None

    # Verify key is deterministic (same inputs produce same key)
    cache2 = APIKeyCache()
    assert cache.encryption_key == cache2.encryption_key

  def test_cache_key_methods(self):
    """Test cache key generation methods."""
    cache = APIKeyCache()

    api_key_hash = "test_hash_123"
    graph_id = "kg123abc"
    user_id = "user_456"
    jwt_hash = "jwt_hash_789"

    assert cache._get_api_key_cache_key(api_key_hash) == f"apikey:{api_key_hash}"
    assert (
      cache._get_graph_cache_key(api_key_hash, graph_id)
      == f"apikey_graph:{api_key_hash}:{graph_id}"
    )
    assert cache._get_user_cache_key(user_id) == f"user:{user_id}"
    assert cache._get_jwt_cache_key(jwt_hash) == f"jwt:{jwt_hash}"
    assert (
      cache._get_jwt_graph_cache_key(user_id, graph_id)
      == f"jwt_graph:{user_id}:{graph_id}"
    )
    assert cache._get_jwt_blacklist_key(jwt_hash) == f"jwt_blacklist:{jwt_hash}"


class TestEncryptionDecryption:
  """Test cache encryption and decryption functionality."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis"):
      cache = APIKeyCache()
      # Ensure validation failures counter is properly initialized
      if not hasattr(cache, "_validation_failures"):
        cache._validation_failures = 0
      return cache

  def test_encrypt_decrypt_success(self, cache):
    """Test successful encryption and decryption."""
    test_data = {
      "user_id": "user_123",
      "email": "test@example.com",
      "is_active": True,
      "permissions": ["read", "write"],
    }

    encrypted = cache._encrypt_cache_data(test_data)
    assert isinstance(encrypted, str)
    assert len(encrypted) > 0

    decrypted = cache._decrypt_cache_data(encrypted)
    assert decrypted == test_data

  def test_encrypt_with_metadata(self, cache):
    """Test encryption includes proper metadata."""
    test_data = {"test": "value"}
    encrypted = cache._encrypt_cache_data(test_data)

    # Manually decrypt to verify metadata
    encrypted_bytes = base64.urlsafe_b64decode(encrypted.encode())
    decrypted_bytes = cache.cipher.decrypt(encrypted_bytes)
    protected_data = json.loads(decrypted_bytes.decode())

    assert "data" in protected_data
    assert "version" in protected_data
    assert "encrypted_at" in protected_data
    assert "nonce" in protected_data
    assert protected_data["data"] == test_data
    assert protected_data["version"] == cache.CACHE_VERSION

  def test_decrypt_invalid_token(self, cache):
    """Test decryption with invalid token."""
    # Ensure validation failures counter is initialized
    initial_failures = getattr(cache, "_validation_failures", 0)

    # Create valid base64 data so it reaches the cipher.decrypt step
    valid_b64_data = base64.urlsafe_b64encode(b"fake encrypted data").decode()

    # Initialize cipher first to ensure it exists for patching
    _ = cache.cipher

    with patch.object(cache._cipher, "decrypt", side_effect=InvalidToken()):
      result = cache._decrypt_cache_data(valid_b64_data)
      assert result is None
      assert cache._validation_failures == initial_failures + 1

  def test_decrypt_validation_failure_threshold(self, cache):
    """Test validation failure threshold handling."""
    # Initialize cipher first to ensure it exists for patching
    _ = cache.cipher

    with patch.object(cache._cipher, "decrypt", side_effect=InvalidToken()):
      with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
        # Trigger failures up to threshold
        for _ in range(cache.VALIDATION_FAILURE_THRESHOLD):
          cache._decrypt_cache_data("invalid_data")

        # Verify security event logged
        mock_audit.log_security_event.assert_called()
        call_args = mock_audit.log_security_event.call_args
        assert call_args[1]["event_type"] == SecurityEventType.SUSPICIOUS_ACTIVITY
        assert call_args[1]["details"]["action"] == "cache_validation_failure_threshold"

  def test_decrypt_version_mismatch(self, cache):
    """Test decryption with version mismatch."""
    # Create data with wrong version
    wrong_version_data = {
      "data": {"test": "value"},
      "version": "v1.0",  # Wrong version
      "encrypted_at": datetime.now(timezone.utc).isoformat(),
      "nonce": secrets.token_hex(16),
    }

    json_data = json.dumps(wrong_version_data)
    encrypted = cache.cipher.encrypt(json_data.encode())
    encrypted_b64 = base64.urlsafe_b64encode(encrypted).decode()

    result = cache._decrypt_cache_data(encrypted_b64)
    assert result is None

  def test_decrypt_expired_data(self, cache):
    """Test decryption with expired data."""
    # Create data that's too old
    old_time = datetime.now(timezone.utc) - timedelta(
      seconds=cache.MAX_CACHE_AGE_SECONDS + 100
    )
    old_data = {
      "data": {"test": "value"},
      "version": cache.CACHE_VERSION,
      "encrypted_at": old_time.isoformat(),
      "nonce": secrets.token_hex(16),
    }

    json_data = json.dumps(old_data)
    encrypted = cache.cipher.encrypt(json_data.encode())
    encrypted_b64 = base64.urlsafe_b64encode(encrypted).decode()

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      result = cache._decrypt_cache_data(encrypted_b64)
      assert result is None

      # Verify security event logged for age violation
      mock_audit.log_security_event.assert_called()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["details"]["action"] == "cache_age_violation"

  def test_encryption_failure(self, cache):
    """Test encryption failure handling."""
    with patch.object(
      cache.cipher, "encrypt", side_effect=Exception("Encryption failed")
    ):
      with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
        with pytest.raises(Exception, match="Encryption failed"):
          cache._encrypt_cache_data({"test": "data"})

        # Verify security event logged
        mock_audit.log_security_event.assert_called()
        call_args = mock_audit.log_security_event.call_args
        assert call_args[1]["details"]["action"] == "cache_encryption_unexpected_error"


class TestUserDataValidation:
  """Test user data integrity validation."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis"):
      return APIKeyCache()

  def test_valid_user_data(self, cache):
    """Test validation of valid user data."""
    valid_data = {
      "id": "user_123",
      "email": "test@example.com",
      "is_active": True,
      "name": "Test User",
    }

    result = cache._validate_user_data_integrity(valid_data)
    assert result is True

  def test_missing_required_fields(self, cache):
    """Test validation with missing required fields."""
    # Missing 'id'
    invalid_data1 = {
      "email": "test@example.com",
      "is_active": True,
    }
    assert cache._validate_user_data_integrity(invalid_data1) is False

    # Missing 'email'
    invalid_data2 = {
      "id": "user_123",
      "is_active": True,
    }
    assert cache._validate_user_data_integrity(invalid_data2) is False

    # Missing 'is_active'
    invalid_data3 = {
      "id": "user_123",
      "email": "test@example.com",
    }
    assert cache._validate_user_data_integrity(invalid_data3) is False

  def test_invalid_data_types(self, cache):
    """Test validation with invalid data types."""
    # Invalid ID type
    invalid_data1 = {
      "id": 123,  # Should be string
      "email": "test@example.com",
      "is_active": True,
    }
    assert cache._validate_user_data_integrity(invalid_data1) is False

    # Empty ID
    invalid_data2 = {
      "id": "",
      "email": "test@example.com",
      "is_active": True,
    }
    assert cache._validate_user_data_integrity(invalid_data2) is False

    # Invalid email format
    invalid_data3 = {
      "id": "user_123",
      "email": "invalid_email",
      "is_active": True,
    }
    assert cache._validate_user_data_integrity(invalid_data3) is False

    # Invalid is_active type
    invalid_data4 = {
      "id": "user_123",
      "email": "test@example.com",
      "is_active": "true",  # Should be boolean
    }
    assert cache._validate_user_data_integrity(invalid_data4) is False

  def test_inactive_user_security_check(self, cache):
    """Test security check for inactive users."""
    inactive_data = {
      "id": "user_123",
      "email": "test@example.com",
      "is_active": False,
    }

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      result = cache._validate_user_data_integrity(inactive_data)
      assert result is False

      # Verify security event logged
      mock_audit.log_security_event.assert_called()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.AUTHORIZATION_DENIED
      assert call_args[1]["details"]["action"] == "inactive_user_cache_access"

  def test_validation_exception_handling(self, cache):
    """Test validation exception handling."""
    # Pass invalid data that causes an exception
    result = cache._validate_user_data_integrity(None)
    assert result is False


class TestSignatureVerification:
  """Test cache signature creation and verification."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis"):
      return APIKeyCache()

  def test_create_signature_success(self, cache):
    """Test successful signature creation."""
    cache_key = "test:key"
    data = {"user_id": "123", "email": "test@example.com"}

    signature = cache._create_cache_signature(cache_key, data)
    assert isinstance(signature, str)
    assert len(signature) == 64  # SHA-256 hex digest length

  def test_verify_signature_success(self, cache):
    """Test successful signature verification."""
    cache_key = "test:key"
    data = {"user_id": "123", "email": "test@example.com"}

    signature = cache._create_cache_signature(cache_key, data)
    result = cache._verify_cache_signature(cache_key, data, signature)
    assert result is True

  def test_verify_signature_mismatch(self, cache):
    """Test signature verification with mismatch."""
    cache_key = "test:key"
    data = {"user_id": "123", "email": "test@example.com"}

    signature = cache._create_cache_signature(cache_key, data)
    # Modify data after signature creation
    modified_data = {"user_id": "456", "email": "test@example.com"}

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      result = cache._verify_cache_signature(cache_key, modified_data, signature)
      assert result is False

      # Verify security event logged
      mock_audit.log_security_event.assert_called()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["details"]["action"] == "cache_signature_mismatch"

  def test_verify_signature_wrong_signature(self, cache):
    """Test signature verification with wrong signature."""
    cache_key = "test:key"
    data = {"user_id": "123", "email": "test@example.com"}
    wrong_signature = "wrong_signature"

    result = cache._verify_cache_signature(cache_key, data, wrong_signature)
    assert result is False

  def test_signature_deterministic(self, cache):
    """Test that signatures are deterministic."""
    cache_key = "test:key"
    data = {"user_id": "123", "email": "test@example.com"}

    sig1 = cache._create_cache_signature(cache_key, data)
    sig2 = cache._create_cache_signature(cache_key, data)
    assert sig1 == sig2

  def test_signature_different_for_different_data(self, cache):
    """Test that different data produces different signatures."""
    cache_key = "test:key"
    data1 = {"user_id": "123", "email": "test1@example.com"}
    data2 = {"user_id": "123", "email": "test2@example.com"}

    sig1 = cache._create_cache_signature(cache_key, data1)
    sig2 = cache._create_cache_signature(cache_key, data2)
    assert sig1 != sig2

  def test_signature_creation_exception(self, cache):
    """Test signature creation exception handling."""
    cache_key = "test:key"
    # Pass non-serializable data
    data = {"function": lambda x: x}  # Cannot be JSON serialized

    with pytest.raises(Exception):
      cache._create_cache_signature(cache_key, data)

  def test_signature_verification_exception(self, cache):
    """Test signature verification exception handling."""
    with patch.object(
      cache, "_create_cache_signature", side_effect=Exception("Signature error")
    ):
      result = cache._verify_cache_signature("key", {"data": "test"}, "signature")
      assert result is False


class TestAPIKeyCaching:
  """Test API key validation caching functionality."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.pipeline.return_value = Mock()
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()
      cache._redis = mock_redis  # Set directly to avoid lazy loading
      return cache

  def test_cache_api_key_validation_success(self, cache):
    """Test successful API key validation caching."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}

    mock_pipe = Mock()
    cache.redis.pipeline.return_value = mock_pipe

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify pipeline operations
      mock_pipe.setex.assert_called()  # Should be called twice (data + signature)
      mock_pipe.execute.assert_called_once()

      # Verify security audit log
      mock_audit.log_security_event.assert_called()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.AUTH_SUCCESS
      assert call_args[1]["details"]["action"] == "secure_cache_write"

  def test_cache_api_key_validation_invalid_user_data(self, cache):
    """Test caching with invalid user data."""
    api_key_hash = "test_hash_123"
    invalid_user_data = {
      "id": "",  # Invalid empty ID
      "email": "test@example.com",
      "is_active": True,
    }

    mock_pipe = Mock()
    cache.redis.pipeline.return_value = mock_pipe

    cache.cache_api_key_validation(api_key_hash, invalid_user_data, True)

    # Should not call pipeline operations due to invalid data
    mock_pipe.setex.assert_not_called()
    mock_pipe.execute.assert_not_called()

  def test_cache_api_key_validation_exception(self, cache):
    """Test caching exception handling."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}

    cache.redis.pipeline.side_effect = Exception("Redis error")

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify error audit log
      mock_audit.log_security_event.assert_called()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["details"]["action"] == "cache_write_failed"

  def test_get_cached_api_key_validation_success(self, cache):
    """Test successful retrieval of cached API key validation."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}
    cache_data = {
      "user_data": user_data,
      "is_active": True,
      "cached_at": datetime.now(timezone.utc).isoformat(),
      "cache_version": cache.CACHE_VERSION,
    }

    # Mock encrypted data and signature
    encrypted_data = cache._encrypt_cache_data(cache_data)
    signature = cache._create_cache_signature(f"apikey:{api_key_hash}", cache_data)

    mock_pipe = Mock()
    mock_pipe.execute.return_value = [encrypted_data, signature]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)

    assert result is not None
    assert result == cache_data

  def test_get_cached_api_key_validation_cache_miss(self, cache):
    """Test cache miss scenarios."""
    api_key_hash = "test_hash_123"

    mock_pipe = Mock()
    # Test missing data
    mock_pipe.execute.return_value = [None, "signature"]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

    # Test missing signature
    mock_pipe.execute.return_value = ["encrypted_data", None]
    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

  def test_get_cached_api_key_validation_decryption_failure(self, cache):
    """Test cached retrieval with decryption failure."""
    api_key_hash = "test_hash_123"

    mock_pipe = Mock()
    mock_pipe.execute.return_value = ["corrupted_data", "signature"]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

    # Verify cleanup was called
    cache.redis.delete.assert_called()

  def test_get_cached_api_key_validation_signature_failure(self, cache):
    """Test cached retrieval with signature verification failure."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}
    cache_data = {
      "user_data": user_data,
      "is_active": True,
      "cached_at": datetime.now(timezone.utc).isoformat(),
      "cache_version": cache.CACHE_VERSION,
    }

    # Create valid encrypted data but wrong signature
    encrypted_data = cache._encrypt_cache_data(cache_data)
    wrong_signature = "wrong_signature"

    mock_pipe = Mock()
    mock_pipe.execute.return_value = [encrypted_data, wrong_signature]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

    # Verify cleanup was called
    cache.redis.delete.assert_called()

  def test_get_cached_api_key_validation_user_data_invalid(self, cache):
    """Test cached retrieval with invalid user data."""
    api_key_hash = "test_hash_123"
    invalid_user_data = {
      "id": "",  # Invalid
      "email": "test@example.com",
      "is_active": True,
    }
    cache_data = {
      "user_data": invalid_user_data,
      "is_active": True,
      "cached_at": datetime.now(timezone.utc).isoformat(),
      "cache_version": cache.CACHE_VERSION,
    }

    encrypted_data = cache._encrypt_cache_data(cache_data)
    signature = cache._create_cache_signature(f"apikey:{api_key_hash}", cache_data)

    mock_pipe = Mock()
    mock_pipe.execute.return_value = [encrypted_data, signature]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

    # Verify cleanup was called
    cache.redis.delete.assert_called()

  def test_get_cached_api_key_validation_expired(self, cache):
    """Test cached retrieval with expired data."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}
    # Create expired cache data
    old_time = datetime.now(timezone.utc) - timedelta(
      seconds=cache.MAX_CACHE_AGE_SECONDS + 100
    )
    cache_data = {
      "user_data": user_data,
      "is_active": True,
      "cached_at": old_time.isoformat(),
      "cache_version": cache.CACHE_VERSION,
    }

    encrypted_data = cache._encrypt_cache_data(cache_data)
    signature = cache._create_cache_signature(f"apikey:{api_key_hash}", cache_data)

    mock_pipe = Mock()
    mock_pipe.execute.return_value = [encrypted_data, signature]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None

    # Verify cleanup was called
    cache.redis.delete.assert_called()


class TestJWTCaching:
  """Test JWT token caching functionality."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()
      cache._redis = mock_redis
      return cache

  def test_hash_jwt_token(self, cache):
    """Test JWT token hashing."""
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.signature"

    hash1 = cache._hash_jwt_token(token)
    hash2 = cache._hash_jwt_token(token)

    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA-256 hex length
    assert hash1 == hash2  # Deterministic

    # Different tokens should produce different hashes
    different_token = "different.jwt.token"
    hash3 = cache._hash_jwt_token(different_token)
    assert hash1 != hash3


class TestCacheIntegration:
  """Test cache integration scenarios and edge cases."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.pipeline.return_value = Mock()
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()
      cache._redis = mock_redis
      return cache

  def test_full_cache_cycle(self, cache):
    """Test complete cache cycle: store -> retrieve -> validate."""
    api_key_hash = "test_hash_123"
    user_data = {
      "id": "user_123",
      "email": "test@example.com",
      "is_active": True,
      "name": "Test User",
    }

    # Mock pipeline for caching
    mock_pipe = Mock()
    cache.redis.pipeline.return_value = mock_pipe

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger"):
      # Cache the data
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify caching operations
      assert mock_pipe.setex.call_count == 2  # Data + signature
      mock_pipe.execute.assert_called_once()

  def test_concurrent_cache_operations(self, cache):
    """Test handling of concurrent cache operations."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}

    # Mock concurrent pipeline operations
    mock_pipe1 = Mock()
    mock_pipe2 = Mock()
    cache.redis.pipeline.side_effect = [mock_pipe1, mock_pipe2]

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger"):
      # Simulate concurrent cache operations
      cache.cache_api_key_validation(api_key_hash, user_data, True)
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify both operations completed
      mock_pipe1.execute.assert_called_once()
      mock_pipe2.execute.assert_called_once()

  def test_cache_security_audit_integration(self, cache):
    """Test integration with security audit logging."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}

    mock_pipe = Mock()
    cache.redis.pipeline.return_value = mock_pipe

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger") as mock_audit:
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify security audit was called
      mock_audit.log_security_event.assert_called()

      # Verify audit event details
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.AUTH_SUCCESS
      assert call_args[1]["details"]["action"] == "secure_cache_write"
      assert call_args[1]["details"]["cache_type"] == "api_key_validation"
      assert call_args[1]["details"]["encrypted"] is True

  def test_cache_cleanup_on_failures(self, cache):
    """Test cache cleanup on various failure scenarios."""
    api_key_hash = "test_hash_123"

    # Test cleanup on decryption failure
    mock_pipe = Mock()
    mock_pipe.execute.return_value = ["corrupted_data", "signature"]
    cache.redis.pipeline.return_value = mock_pipe

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None
    cache.redis.delete.assert_called()

    # Reset mock
    cache.redis.delete.reset_mock()

    # Test cleanup on signature failure
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}
    cache_data = {
      "user_data": user_data,
      "is_active": True,
      "cached_at": datetime.now(timezone.utc).isoformat(),
      "cache_version": cache.CACHE_VERSION,
    }
    encrypted_data = cache._encrypt_cache_data(cache_data)
    wrong_signature = "wrong_signature"

    mock_pipe.execute.return_value = [encrypted_data, wrong_signature]

    result = cache.get_cached_api_key_validation(api_key_hash)
    assert result is None
    cache.redis.delete.assert_called()


class TestCachePerformance:
  """Test cache performance and optimization features."""

  @pytest.fixture
  def cache(self):
    """Create APIKeyCache instance with mocked Redis."""
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      cache = APIKeyCache()
      cache._redis = mock_redis
      return cache

  def test_pipeline_usage_optimization(self, cache):
    """Test Redis pipeline usage for performance optimization."""
    api_key_hash = "test_hash_123"
    user_data = {"id": "user_123", "email": "test@example.com", "is_active": True}

    mock_pipe = Mock()
    cache.redis.pipeline.return_value = mock_pipe

    with patch("robosystems.middleware.auth.cache.SecurityAuditLogger"):
      cache.cache_api_key_validation(api_key_hash, user_data, True)

      # Verify pipeline is used for batch operations
      cache.redis.pipeline.assert_called_once()
      assert mock_pipe.setex.call_count == 2  # Data + signature in batch
      mock_pipe.execute.assert_called_once()

  def test_lazy_connection_performance(self, cache):
    """Test lazy connection initialization for performance."""
    # Create a fresh cache instance to test lazy loading
    with patch("robosystems.middleware.auth.cache.redis.Redis") as MockRedis:
      mock_redis = Mock()
      mock_redis.ping.return_value = True
      mock_redis.keys.return_value = []  # Return empty list for keys() calls
      MockRedis.from_url.return_value = mock_redis

      fresh_cache = APIKeyCache()

      # Cache should not connect to Redis until needed
      assert fresh_cache._redis is None

      # Accessing redis property should trigger connection
      redis_conn = fresh_cache.redis
      assert redis_conn is mock_redis
      MockRedis.from_url.assert_called_once()

  def test_encryption_performance(self, cache):
    """Test encryption performance with various data sizes."""
    # Test with small data
    small_data = {"id": "123", "email": "test@example.com", "is_active": True}
    encrypted_small = cache._encrypt_cache_data(small_data)
    decrypted_small = cache._decrypt_cache_data(encrypted_small)
    assert decrypted_small == small_data

    # Test with larger data
    large_data = {
      "id": "user_123",
      "email": "test@example.com",
      "is_active": True,
      "permissions": [f"permission_{i}" for i in range(100)],
      "metadata": {f"key_{i}": f"value_{i}" for i in range(50)},
    }
    encrypted_large = cache._encrypt_cache_data(large_data)
    decrypted_large = cache._decrypt_cache_data(encrypted_large)
    assert decrypted_large == large_data
