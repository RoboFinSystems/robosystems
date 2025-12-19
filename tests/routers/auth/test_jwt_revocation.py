"""Test JWT token revocation functionality."""

from datetime import UTC
from unittest.mock import MagicMock, patch

from robosystems.middleware.auth.jwt import (
  create_jwt_token,
  is_jwt_token_revoked,
  revoke_jwt_token,
  verify_jwt_token,
)


class TestJWTRevocation:
  """Test JWT token revocation system."""

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_jwt_token_has_jti_claim(self, mock_redis):
    """Test that new JWT tokens include JTI claim for revocation tracking."""
    # Mock Redis client
    mock_redis_instance = MagicMock()
    mock_redis.return_value = mock_redis_instance

    # Create a JWT token (no device fingerprint needed for this test)
    token = create_jwt_token("test-user-123")

    # Decode and verify it contains JTI
    import jwt

    from robosystems.config import env

    payload = jwt.decode(
      token,
      env.JWT_SECRET_KEY,
      algorithms=["HS256"],
      options={"verify_exp": False, "verify_aud": False},
    )

    assert "jti" in payload
    assert isinstance(payload["jti"], str)
    assert len(payload["jti"]) > 0

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_token_verification_before_revocation(self, mock_redis):
    """Test that token verification works before revocation."""
    # Mock Redis client to return no revocation
    mock_redis_instance = MagicMock()
    mock_redis_instance.exists.return_value = False
    mock_redis.return_value = mock_redis_instance

    # Create and verify token (no device fingerprint needed for this test)
    token = create_jwt_token("test-user-123")
    user_id = verify_jwt_token(token)

    assert user_id == "test-user-123"

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_token_revocation_flow(self, mock_redis):
    """Test complete token revocation flow."""
    # Mock Redis client
    mock_redis_instance = MagicMock()
    mock_redis_instance.exists.return_value = False  # Initially not revoked
    mock_pipeline = MagicMock()
    mock_redis_instance.pipeline.return_value = mock_pipeline
    mock_redis.return_value = mock_redis_instance

    # Create token (no device fingerprint needed for this test)
    token = create_jwt_token("test-user-123")

    # Initially should not be revoked
    assert not is_jwt_token_revoked(token)

    # Revoke the token
    success = revoke_jwt_token(token, reason="test_revocation")
    assert success

    # Verify Redis operations were called
    mock_redis_instance.pipeline.assert_called_once()
    mock_pipeline.hset.assert_called_once()
    mock_pipeline.expire.assert_called_once()
    mock_pipeline.execute.assert_called_once()

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_token_verification_after_revocation(self, mock_redis):
    """Test that token verification fails after revocation."""
    # Mock Redis client to return revocation exists
    mock_redis_instance = MagicMock()
    mock_redis_instance.exists.return_value = True  # Token is revoked
    mock_redis.return_value = mock_redis_instance

    # Create token (no device fingerprint needed for this test)
    token = create_jwt_token("test-user-123")

    # Verification should fail due to revocation (no device fingerprint needed)
    user_id = verify_jwt_token(token)
    assert user_id is None

  def test_token_revocation_without_jti(self):
    """Test that tokens without JTI are considered valid (backward compatibility)."""
    # Create a token manually without JTI
    from datetime import datetime, timedelta

    import jwt

    from robosystems.config import env

    payload = {
      "user_id": "test-user-123",
      "exp": datetime.now(UTC) + timedelta(hours=24),
      "iat": datetime.now(UTC),
      "iss": "api.robosystems.ai",
      "aud": ["robosystems.ai", "roboledger.ai", "roboinvestor.ai"],
      # No JTI claim
    }
    token = jwt.encode(payload, env.JWT_SECRET_KEY, algorithm="HS256")

    # Should not be considered revoked (backward compatibility)
    assert not is_jwt_token_revoked(token)

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_revocation_redis_error_handling(self, mock_redis):
    """Test that Redis errors are handled with fail-closed behavior."""
    # Mock Redis client to raise exception
    mock_redis_instance = MagicMock()
    mock_redis_instance.exists.side_effect = Exception("Redis connection failed")
    mock_redis.return_value = mock_redis_instance

    # Create token (no device fingerprint needed for this test)
    token = create_jwt_token("test-user-123")

    # On Redis error, token is considered revoked (fail closed for security)
    # This ensures that when revocation system is unavailable, tokens are rejected
    assert is_jwt_token_revoked(token)

    # Token verification should fail when Redis is down (fail closed, no device fingerprint needed)
    user_id = verify_jwt_token(token)
    assert user_id is None

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_revocation_redis_connection_error(self, mock_redis):
    """Test specific handling of Redis ConnectionError."""
    import redis

    # Mock Redis client to raise ConnectionError
    mock_redis_instance = MagicMock()
    mock_redis_instance.exists.side_effect = redis.ConnectionError("Connection refused")
    mock_redis.return_value = mock_redis_instance

    # Create token
    token = create_jwt_token("test-user-456")

    # On Redis ConnectionError, token is considered revoked (fail closed)
    assert is_jwt_token_revoked(token)

    # Token verification should fail when Redis connection is down (no device fingerprint needed)
    user_id = verify_jwt_token(token)
    assert user_id is None

  @patch("robosystems.middleware.auth.jwt.get_redis_client")
  def test_revoke_expired_token(self, mock_redis):
    """Test that revoking an already expired token is handled gracefully."""
    # Mock Redis client
    mock_redis_instance = MagicMock()
    mock_redis.return_value = mock_redis_instance

    # Create an expired token manually
    from datetime import datetime, timedelta

    import jwt

    from robosystems.config import env

    payload = {
      "user_id": "test-user-123",
      "jti": "test-jti-123",
      "exp": datetime.now(UTC) - timedelta(hours=1),  # Expired
      "iat": datetime.now(UTC) - timedelta(hours=2),
      "iss": "api.robosystems.ai",
      "aud": ["robosystems.ai", "roboledger.ai", "roboinvestor.ai"],
    }
    expired_token = jwt.encode(payload, env.JWT_SECRET_KEY, algorithm="HS256")

    # Should return True (no need to revoke expired token)
    success = revoke_jwt_token(expired_token, reason="test")
    assert success

    # Redis operations should not be called for expired tokens
    mock_redis_instance.pipeline.assert_not_called()
