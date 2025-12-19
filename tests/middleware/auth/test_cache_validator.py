"""
Comprehensive tests for authentication cache validator.

Tests the CacheValidator class which provides validation, monitoring,
and cleanup services for the encrypted authentication cache system.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.middleware.auth.cache_validator import (
  CacheValidationResult,
  CacheValidator,
  get_cache_validator,
)
from robosystems.security import SecurityEventType


class TestCacheValidationResult:
  """Test CacheValidationResult dataclass."""

  def test_valid_result_creation(self):
    """Test creation of valid cache validation result."""
    result = CacheValidationResult(
      is_valid=True,
      issues_found=[],
      corrective_actions_taken=[],
      security_events_logged=0,
      validation_timestamp=datetime.now(UTC),
    )

    assert result.is_valid is True
    assert result.issues_found == []
    assert result.corrective_actions_taken == []
    assert result.security_events_logged == 0
    assert not result.has_security_issues

  def test_invalid_result_with_issues(self):
    """Test creation of invalid result with security issues."""
    issues = ["Cache entry missing signature", "Orphaned signature found"]
    actions = ["Removed unsigned cache entry", "Removed orphaned signature"]

    result = CacheValidationResult(
      is_valid=False,
      issues_found=issues,
      corrective_actions_taken=actions,
      security_events_logged=2,
      validation_timestamp=datetime.now(UTC),
    )

    assert result.is_valid is False
    assert len(result.issues_found) == 2
    assert len(result.corrective_actions_taken) == 2
    assert result.security_events_logged == 2
    assert result.has_security_issues is True

  def test_has_security_issues_property(self):
    """Test has_security_issues property logic."""
    # No issues
    result_clean = CacheValidationResult(
      is_valid=True,
      issues_found=[],
      corrective_actions_taken=[],
      security_events_logged=0,
      validation_timestamp=datetime.now(UTC),
    )
    assert result_clean.has_security_issues is False

    # With issues
    result_issues = CacheValidationResult(
      is_valid=False,
      issues_found=["Test issue"],
      corrective_actions_taken=["Test action"],
      security_events_logged=1,
      validation_timestamp=datetime.now(UTC),
    )
    assert result_issues.has_security_issues is True


class TestCacheValidator:
  """Test CacheValidator class functionality."""

  @pytest.fixture
  def mock_api_key_cache(self):
    """Create mock API key cache."""
    cache = Mock()
    cache.CACHE_KEY_PREFIX = "apikey:"
    cache.CACHE_SIGNATURE_PREFIX = "cache_sig:"
    cache.JWT_CACHE_KEY_PREFIX = "jwt:"
    cache.JWT_GRAPH_CACHE_KEY_PREFIX = "jwtgraph:"
    cache.GRAPH_CACHE_KEY_PREFIX = "apigraph:"
    cache.CACHE_VALIDATION_PREFIX = "validation:"
    cache.redis = Mock()
    cache._decrypt_cache_data = Mock()
    cache._verify_cache_signature = Mock()
    cache._validate_user_data_integrity = Mock()
    return cache

  @pytest.fixture
  def validator(self, mock_api_key_cache):
    """Create CacheValidator instance with mocked dependencies."""
    return CacheValidator(mock_api_key_cache)

  def _setup_async_redis_mock(
    self, validator, keys_return=None, get_side_effect=None, delete_return=None
  ):
    """Helper to set up async Redis mock for tests."""
    mock_async_redis = AsyncMock()

    if keys_return is not None:
      mock_async_redis.keys = AsyncMock(return_value=keys_return)

    if get_side_effect is not None:
      mock_async_redis.get = AsyncMock(side_effect=get_side_effect)

    if delete_return is not None:
      mock_async_redis.delete = AsyncMock(return_value=delete_return)
    else:
      mock_async_redis.delete = AsyncMock()

    mock_async_redis.ping = AsyncMock()

    # Mock the async Redis getter
    validator._get_async_redis = AsyncMock(return_value=mock_async_redis)

    return mock_async_redis

  def test_validator_initialization(self, validator, mock_api_key_cache):
    """Test validator initialization."""
    assert validator.api_key_cache == mock_api_key_cache
    assert validator.max_validation_failures == 10
    assert validator.max_cache_age_hours == 24
    assert validator.validation_interval_minutes == 30

  @pytest.mark.asyncio
  async def test_validate_cache_integrity_success(self, validator, mock_api_key_cache):
    """Test successful cache integrity validation."""
    # Mock all validation methods to return clean results
    validator._validate_api_key_cache = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._validate_jwt_cache = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._check_cache_consistency = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._validate_cache_freshness = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._detect_suspicious_patterns = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.validate_cache_integrity()

      assert result.is_valid is True
      assert len(result.issues_found) == 0
      assert len(result.corrective_actions_taken) == 0
      assert result.security_events_logged == 0
      assert not result.has_security_issues

      # Verify success audit log
      mock_audit.log_security_event.assert_called_once()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.AUTH_SUCCESS
      assert call_args[1]["details"]["action"] == "cache_validation_passed"
      assert call_args[1]["risk_level"] == "low"

  @pytest.mark.asyncio
  async def test_validate_cache_integrity_with_issues(self, validator):
    """Test cache integrity validation with security issues found."""
    # Mock validation methods to return issues
    validator._validate_api_key_cache = AsyncMock(
      return_value={
        "issues": ["Cache entry missing signature"],
        "actions": ["Removed unsigned cache entry"],
        "events": 1,
      }
    )
    validator._validate_jwt_cache = AsyncMock(
      return_value={
        "issues": ["JWT signature verification failed"],
        "actions": ["Removed tampered JWT cache"],
        "events": 1,
      }
    )
    validator._check_cache_consistency = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._validate_cache_freshness = AsyncMock(
      return_value={
        "issues": ["Stale cache entry"],
        "actions": ["Removed stale cache"],
        "events": 0,
      }
    )
    validator._detect_suspicious_patterns = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.validate_cache_integrity()

      assert result.is_valid is False
      assert len(result.issues_found) == 3
      assert len(result.corrective_actions_taken) == 3
      assert result.security_events_logged == 2
      assert result.has_security_issues is True

      # Verify suspicious activity audit log
      mock_audit.log_security_event.assert_called_once()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.SUSPICIOUS_ACTIVITY
      assert call_args[1]["details"]["action"] == "cache_validation_issues_found"
      assert call_args[1]["risk_level"] == "medium"  # Less than 5 issues

  @pytest.mark.asyncio
  async def test_validate_cache_integrity_high_risk(self, validator):
    """Test cache integrity validation with high-risk issues (>5 issues)."""
    # Create more than 5 issues for high risk classification
    issues = [f"Issue {i}" for i in range(6)]
    actions = [f"Action {i}" for i in range(6)]

    validator._validate_api_key_cache = AsyncMock(
      return_value={"issues": issues[:3], "actions": actions[:3], "events": 3}
    )
    validator._validate_jwt_cache = AsyncMock(
      return_value={"issues": issues[3:], "actions": actions[3:], "events": 3}
    )
    validator._check_cache_consistency = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._validate_cache_freshness = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._detect_suspicious_patterns = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.validate_cache_integrity()

      assert result.is_valid is False
      assert len(result.issues_found) == 6
      assert result.security_events_logged == 6

      # Verify high risk classification
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["risk_level"] == "high"

  @pytest.mark.asyncio
  async def test_validate_cache_integrity_exception_handling(self, validator):
    """Test exception handling during cache validation."""
    # Make one of the validation methods raise an exception
    validator._validate_api_key_cache = AsyncMock(side_effect=Exception("Test error"))
    validator._validate_jwt_cache = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._check_cache_consistency = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._validate_cache_freshness = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )
    validator._detect_suspicious_patterns = AsyncMock(
      return_value={"issues": [], "actions": [], "events": 0}
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.validate_cache_integrity()

      assert result.is_valid is False
      assert len(result.issues_found) == 1
      assert "Validation failed: Test error" in result.issues_found[0]
      assert result.security_events_logged == 1

      # Verify failure audit log
      mock_audit.log_security_event.assert_called_once()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["event_type"] == SecurityEventType.SUSPICIOUS_ACTIVITY
      assert call_args[1]["details"]["action"] == "cache_validation_failed"
      assert call_args[1]["risk_level"] == "high"

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_success(self, validator, mock_api_key_cache):
    """Test successful API key cache validation."""
    # Create mock async Redis
    mock_async_redis = AsyncMock()
    mock_async_redis.keys = AsyncMock(return_value=["apikey:hash123", "apikey:hash456"])
    mock_async_redis.get = AsyncMock(
      side_effect=lambda key: {
        "apikey:hash123": b"encrypted_data_123",
        "cache_sig:hash123": b"signature_123",
        "apikey:hash456": b"encrypted_data_456",
        "cache_sig:hash456": b"signature_456",
      }.get(key)
    )
    mock_async_redis.ping = AsyncMock()

    # Mock the async Redis getter
    validator._get_async_redis = AsyncMock(return_value=mock_async_redis)

    # Mock successful decryption and validation
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"user_id": "test_user"},
      "cached_at": datetime.now(UTC).isoformat(),
    }
    mock_api_key_cache._verify_cache_signature.return_value = True
    mock_api_key_cache._validate_user_data_integrity.return_value = True

    result = await validator._validate_api_key_cache()

    assert result["issues"] == []
    assert result["actions"] == []
    assert result["events"] == 0

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_missing_signature(
    self, validator, mock_api_key_cache
  ):
    """Test API key cache validation with missing signatures."""
    # Create mock async Redis
    mock_async_redis = AsyncMock()
    mock_async_redis.keys = AsyncMock(return_value=["apikey:hash123"])
    # Return encrypted data but no signature
    mock_async_redis.get = AsyncMock(
      side_effect=lambda key: {
        "apikey:hash123": b"encrypted_data_123",
        "cache_sig:hash123": None,  # Missing signature
      }.get(key)
    )
    mock_async_redis.delete = AsyncMock()
    mock_async_redis.ping = AsyncMock()

    # Mock the async Redis getter
    validator._get_async_redis = AsyncMock(return_value=mock_async_redis)

    result = await validator._validate_api_key_cache()

    assert len(result["issues"]) == 1
    assert "missing signature" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed unsigned cache entry" in result["actions"][0].lower()
    assert result["events"] == 1

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("apikey:hash123")

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_orphaned_signature(
    self, validator, mock_api_key_cache
  ):
    """Test API key cache validation with orphaned signatures."""
    # Create mock async Redis
    mock_async_redis = AsyncMock()
    mock_async_redis.keys = AsyncMock(return_value=["apikey:hash123"])
    # Return signature but no encrypted data
    mock_async_redis.get = AsyncMock(
      side_effect=lambda key: {
        "apikey:hash123": None,  # Missing data
        "cache_sig:hash123": b"signature_123",
      }.get(key)
    )
    mock_async_redis.delete = AsyncMock()
    mock_async_redis.ping = AsyncMock()

    # Mock the async Redis getter
    validator._get_async_redis = AsyncMock(return_value=mock_async_redis)

    result = await validator._validate_api_key_cache()

    assert len(result["issues"]) == 1
    assert "orphaned signature" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed orphaned signature" in result["actions"][0].lower()

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("cache_sig:hash123")

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_decryption_failure(
    self, validator, mock_api_key_cache
  ):
    """Test API key cache validation with decryption failure."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=["apikey:hash123"],
      get_side_effect=lambda key: {
        "apikey:hash123": b"encrypted_data_123",
        "cache_sig:hash123": b"signature_123",
      }.get(key),
    )

    # Mock decryption failure
    mock_api_key_cache._decrypt_cache_data.return_value = None

    result = await validator._validate_api_key_cache()

    assert len(result["issues"]) == 1
    assert "failed to decrypt" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed corrupted cache entry" in result["actions"][0].lower()
    assert result["events"] == 1

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("apikey:hash123", "cache_sig:hash123")

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_signature_verification_failure(
    self, validator, mock_api_key_cache
  ):
    """Test API key cache validation with signature verification failure."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=["apikey:hash123"],
      get_side_effect=lambda key: {
        "apikey:hash123": b"encrypted_data_123",
        "cache_sig:hash123": b"signature_123",
      }.get(key),
    )

    # Mock successful decryption but failed signature verification
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"user_id": "test_user"},
      "cached_at": datetime.now(UTC).isoformat(),
    }
    mock_api_key_cache._verify_cache_signature.return_value = False

    result = await validator._validate_api_key_cache()

    assert len(result["issues"]) == 1
    assert "signature verification failed" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed tampered cache entry" in result["actions"][0].lower()
    assert result["events"] == 1

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("apikey:hash123", "cache_sig:hash123")

  @pytest.mark.asyncio
  async def test_validate_api_key_cache_invalid_user_data(
    self, validator, mock_api_key_cache
  ):
    """Test API key cache validation with invalid user data."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=["apikey:hash123"],
      get_side_effect=lambda key: {
        "apikey:hash123": b"encrypted_data_123",
        "cache_sig:hash123": b"signature_123",
      }.get(key),
    )

    # Mock successful decryption and signature verification but invalid user data
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"invalid": "data"},
      "cached_at": datetime.now(UTC).isoformat(),
    }
    mock_api_key_cache._verify_cache_signature.return_value = True
    mock_api_key_cache._validate_user_data_integrity.return_value = False

    result = await validator._validate_api_key_cache()

    assert len(result["issues"]) == 1
    assert "invalid user data" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed invalid user data cache" in result["actions"][0].lower()
    assert result["events"] == 1

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("apikey:hash123", "cache_sig:hash123")

  @pytest.mark.asyncio
  async def test_validate_jwt_cache_success(self, validator, mock_api_key_cache):
    """Test successful JWT cache validation."""
    # Use helper to set up async Redis mock
    self._setup_async_redis_mock(  # Sets up validator's async Redis
      validator,
      keys_return=["jwt:hash123", "jwt:hash456"],
      get_side_effect=lambda key: {
        "jwt:hash123": b"encrypted_jwt_123",
        "cache_sig:jwt_hash123": b"jwt_signature_123",
        "jwt:hash456": b"encrypted_jwt_456",
        "cache_sig:jwt_hash456": b"jwt_signature_456",
      }.get(key),
    )

    # Mock successful decryption and validation
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"user_id": "test_user"},
      "cached_at": datetime.now(UTC).isoformat(),
    }
    mock_api_key_cache._verify_cache_signature.return_value = True
    mock_api_key_cache._validate_user_data_integrity.return_value = True

    result = await validator._validate_jwt_cache()

    assert result["issues"] == []
    assert result["actions"] == []
    assert result["events"] == 0

  @pytest.mark.asyncio
  async def test_check_cache_consistency_success(self, validator, mock_api_key_cache):
    """Test successful cache consistency check."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=None,  # Will be overridden by side_effect
    )
    # Return matching cache and signature keys
    mock_async_redis.keys = AsyncMock(
      side_effect=lambda pattern: {
        "apikey:*": ["apikey:hash123", "apikey:hash456"],
        "cache_sig:*": ["cache_sig:hash123", "cache_sig:hash456"],
      }.get(pattern, [])
    )

    result = await validator._check_cache_consistency()

    assert result["issues"] == []
    assert result["actions"] == []
    assert result["events"] == 0

  @pytest.mark.asyncio
  async def test_check_cache_consistency_orphaned_entries(
    self, validator, mock_api_key_cache
  ):
    """Test cache consistency check with orphaned entries."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=None,  # Will be overridden by side_effect
    )
    # Return mismatched cache and signature keys
    mock_async_redis.keys = AsyncMock(
      side_effect=lambda pattern: {
        "apikey:*": ["apikey:hash123", "apikey:hash456"],  # hash456 has no signature
        "cache_sig:*": [
          "cache_sig:hash123",
          "cache_sig:hash789",
        ],  # hash789 has no cache entry
      }.get(pattern, [])
    )

    result = await validator._check_cache_consistency()

    assert len(result["issues"]) == 2
    assert any(
      "cache entry without signature" in issue.lower() for issue in result["issues"]
    )
    assert any(
      "signature without cache entry" in issue.lower() for issue in result["issues"]
    )
    assert len(result["actions"]) == 2
    assert result["events"] == 1  # Only orphaned cache entries increment events

    # Verify orphaned entries were deleted
    mock_async_redis.delete.assert_any_call("apikey:hash456")
    # Orphaned signatures delete both possible formats
    mock_async_redis.delete.assert_any_call(
      "cache_sig:hash789", "cache_sig:jwt_hash789"
    )

  @pytest.mark.asyncio
  async def test_validate_cache_freshness_success(self, validator, mock_api_key_cache):
    """Test successful cache freshness validation."""
    mock_redis = mock_api_key_cache.redis
    mock_redis.keys = AsyncMock(return_value=["apikey:hash123"])
    mock_redis.get = AsyncMock(return_value=b"encrypted_data")

    # Mock fresh cache data
    fresh_time = datetime.now(UTC) - timedelta(hours=1)  # 1 hour old
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"user_id": "test_user"},
      "cached_at": fresh_time.isoformat(),
    }

    result = await validator._validate_cache_freshness()

    assert result["issues"] == []
    assert result["actions"] == []
    assert result["events"] == 0

  @pytest.mark.asyncio
  async def test_validate_cache_freshness_stale_entries(
    self, validator, mock_api_key_cache
  ):
    """Test cache freshness validation with stale entries."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=["apikey:hash123"],
      get_side_effect=lambda key: b"encrypted_data"
      if key == "apikey:hash123"
      else b"signature_data"
      if key == "cache_sig:hash123"
      else None,
    )

    # Mock stale cache data (25 hours old, max age is 24 hours)
    stale_time = datetime.now(UTC) - timedelta(hours=25)
    mock_api_key_cache._decrypt_cache_data.return_value = {
      "user_data": {"user_id": "test_user"},
      "cached_at": stale_time.isoformat(),
    }

    result = await validator._validate_cache_freshness()

    assert len(result["issues"]) == 1
    assert "stale cache entry" in result["issues"][0].lower()
    assert len(result["actions"]) == 1
    assert "removed stale cache" in result["actions"][0].lower()

    # Verify cleanup was called
    mock_async_redis.delete.assert_called_with("apikey:hash123", "cache_sig:hash123")

  @pytest.mark.asyncio
  async def test_detect_suspicious_patterns_normal(self, validator, mock_api_key_cache):
    """Test suspicious pattern detection with normal cache levels."""
    mock_redis = mock_api_key_cache.redis
    # Return normal numbers of cache entries
    mock_redis.keys = AsyncMock(
      side_effect=lambda pattern: {
        "apikey:*": [f"apikey:hash{i}" for i in range(100)],  # 100 API key entries
        "jwt:*": [f"jwt:hash{i}" for i in range(1000)],  # 1000 JWT entries
      }.get(pattern, [])
    )

    result = await validator._detect_suspicious_patterns()

    assert result["issues"] == []
    assert result["actions"] == []
    assert result["events"] == 0

  @pytest.mark.asyncio
  async def test_detect_suspicious_patterns_excessive_entries(
    self, validator, mock_api_key_cache
  ):
    """Test suspicious pattern detection with excessive cache entries."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=None,  # Will be overridden by side_effect
    )
    # Return excessive numbers of cache entries
    mock_async_redis.keys = AsyncMock(
      side_effect=lambda pattern: {
        "apikey:*": [f"apikey:hash{i}" for i in range(15000)],  # Over 10k limit
        "jwt:*": [f"jwt:hash{i}" for i in range(60000)],  # Over 50k limit
      }.get(pattern, [])
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator._detect_suspicious_patterns()

      assert len(result["issues"]) == 2
      assert any(
        "excessive api key cache entries" in issue.lower() for issue in result["issues"]
      )
      assert any(
        "excessive jwt cache entries" in issue.lower() for issue in result["issues"]
      )
      assert result["events"] == 2

      # Verify security events were logged
      assert mock_audit.log_security_event.call_count == 2

  @pytest.mark.asyncio
  async def test_emergency_cache_purge_success(self, validator, mock_api_key_cache):
    """Test successful emergency cache purge."""
    # Use helper to set up async Redis mock
    mock_async_redis = self._setup_async_redis_mock(
      validator,
      keys_return=None,  # Will be overridden by side_effect
    )
    # Mock keys for different patterns
    mock_async_redis.keys = AsyncMock(
      side_effect=lambda pattern: {
        "apikey:*": ["apikey:hash1", "apikey:hash2"],
        "apigraph:*": ["apigraph:graph1"],
        "jwt:*": ["jwt:hash1", "jwt:hash2"],
        "jwtgraph:*": ["jwtgraph:graph1"],
        "cache_sig:*": ["cache_sig:hash1", "cache_sig:hash2"],
        "validation:*": ["validation:check1"],
      }.get(pattern, [])
    )

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.emergency_cache_purge("Security breach detected")

      assert result is True
      # Verify all pattern deletions were called
      assert mock_async_redis.delete.call_count == 6  # One for each pattern with keys

      # Verify security audit log
      mock_audit.log_security_event.assert_called_once()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["details"]["action"] == "emergency_cache_purge"
      assert call_args[1]["details"]["reason"] == "Security breach detected"
      assert call_args[1]["details"]["keys_deleted"] == 9  # Total keys deleted
      assert call_args[1]["risk_level"] == "critical"

  @pytest.mark.asyncio
  async def test_emergency_cache_purge_failure(self, validator, mock_api_key_cache):
    """Test emergency cache purge failure handling."""
    # Set up async Redis mock that raises an exception
    mock_async_redis = AsyncMock()
    mock_async_redis.keys = AsyncMock(side_effect=Exception("Redis connection failed"))
    validator._get_async_redis = AsyncMock(return_value=mock_async_redis)

    with patch(
      "robosystems.middleware.auth.cache_validator.SecurityAuditLogger"
    ) as mock_audit:
      result = await validator.emergency_cache_purge("Test purge failure")

      assert result is False

      # Verify failure audit log
      mock_audit.log_security_event.assert_called_once()
      call_args = mock_audit.log_security_event.call_args
      assert call_args[1]["details"]["action"] == "emergency_cache_purge_failed"
      assert call_args[1]["details"]["reason"] == "Test purge failure"
      assert "Redis connection failed" in call_args[1]["details"]["error"]
      assert call_args[1]["risk_level"] == "critical"


class TestGlobalCacheValidator:
  """Test global cache validator functions."""

  def test_get_cache_validator_initialization(self):
    """Test global cache validator initialization."""
    with patch("robosystems.middleware.auth.cache.api_key_cache") as mock_api_cache:
      with patch("robosystems.middleware.auth.cache_validator.cache_validator", None):
        # Reset global variable
        import robosystems.middleware.auth.cache_validator as validator_module

        validator_module.cache_validator = None

        validator = get_cache_validator()

        assert validator is not None
        assert isinstance(validator, CacheValidator)
        assert validator.api_key_cache == mock_api_cache

  def test_get_cache_validator_missing_dependencies(self):
    """Test global cache validator with missing dependencies."""
    with patch("robosystems.middleware.auth.cache.api_key_cache", None):
      with patch("robosystems.middleware.auth.cache_validator.cache_validator", None):
        # Reset global variable
        import robosystems.middleware.auth.cache_validator as validator_module

        validator_module.cache_validator = None

        validator = get_cache_validator()

        assert validator is None

  def test_get_cache_validator_initialization_error(self):
    """Test global cache validator initialization error handling."""
    with patch("robosystems.middleware.auth.cache_validator.cache_validator", None):
      with patch("robosystems.middleware.auth.cache_validator.logger") as mock_logger:
        with patch("robosystems.middleware.auth.cache.api_key_cache"):
          # Make CacheValidator constructor raise an exception
          with patch(
            "robosystems.middleware.auth.cache_validator.CacheValidator",
            side_effect=Exception("Constructor failed"),
          ):
            # Reset global variable
            import robosystems.middleware.auth.cache_validator as validator_module

            validator_module.cache_validator = None

            validator = get_cache_validator()

            assert validator is None
            mock_logger.error.assert_called_once()
            assert (
              "Failed to initialize cache validator"
              in mock_logger.error.call_args[0][0]
            )

  def test_get_cache_validator_singleton(self):
    """Test that cache validator is a singleton."""
    mock_validator = Mock(spec=CacheValidator)

    with patch(
      "robosystems.middleware.auth.cache_validator.cache_validator", mock_validator
    ):
      validator1 = get_cache_validator()
      validator2 = get_cache_validator()

      assert validator1 is mock_validator
      assert validator2 is mock_validator
      assert validator1 is validator2
