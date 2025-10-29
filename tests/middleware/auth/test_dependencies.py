"""
Comprehensive tests for authentication dependencies.

These tests cover all authentication dependency functions used by FastAPI routes,
including JWT token verification, user authentication, graph access validation,
and repository access controls.
"""

import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException, status, Request
import jwt

from robosystems.middleware.auth.dependencies import (
  _validate_cached_user_data,
  _create_user_from_cache,
  verify_jwt_token,
  get_optional_user,
  get_current_user,
  get_current_user_sse,
  get_current_user_with_graph,
  get_current_user_with_repository_access,
  get_repository_user_dependency,
  API_KEY_HEADER,
)
from robosystems.models.iam import User


class TestCachedUserDataValidation:
  """Test cached user data validation functions."""

  def test_validate_cached_user_data_valid(self):
    """Test validation with valid cached user data."""
    valid_data = {
      "id": 123,
      "email": "test@example.com",
      "name": "Test User",
      "is_active": True,
    }

    assert _validate_cached_user_data(valid_data) is True

  def test_validate_cached_user_data_minimal_valid(self):
    """Test validation with minimal valid data (only required fields)."""
    minimal_data = {
      "id": "user123",
      "email": "user@test.com",
    }

    assert _validate_cached_user_data(minimal_data) is True

  def test_validate_cached_user_data_invalid_type(self):
    """Test validation fails with non-dict input."""
    assert _validate_cached_user_data("not a dict") is False
    assert _validate_cached_user_data(None) is False
    assert _validate_cached_user_data(123) is False

  def test_validate_cached_user_data_missing_id(self):
    """Test validation fails without user ID."""
    data = {"email": "test@example.com"}
    assert _validate_cached_user_data(data) is False

  def test_validate_cached_user_data_invalid_id_type(self):
    """Test validation fails with invalid ID type."""
    data = {"id": None, "email": "test@example.com"}
    assert _validate_cached_user_data(data) is False

    data = {"id": [], "email": "test@example.com"}
    assert _validate_cached_user_data(data) is False

  def test_validate_cached_user_data_missing_email(self):
    """Test validation fails without email."""
    data = {"id": 123}
    assert _validate_cached_user_data(data) is False

  def test_validate_cached_user_data_invalid_email(self):
    """Test validation fails with invalid email format."""
    data = {"id": 123, "email": "invalid-email"}
    assert _validate_cached_user_data(data) is False

    data = {"id": 123, "email": None}
    assert _validate_cached_user_data(data) is False

    data = {"id": 123, "email": 123}
    assert _validate_cached_user_data(data) is False

  def test_validate_cached_user_data_invalid_optional_fields(self):
    """Test validation fails with invalid optional field types."""
    # Invalid name type
    data = {"id": 123, "email": "test@example.com", "name": 123}
    assert _validate_cached_user_data(data) is False

    # Invalid is_active type
    data = {"id": 123, "email": "test@example.com", "is_active": "true"}
    assert _validate_cached_user_data(data) is False

  def test_create_user_from_cache_valid(self):
    """Test creating user from valid cached data."""
    user_data = {
      "id": 123,
      "email": "test@example.com",
      "name": "Test User",
      "is_active": True,
    }

    user = _create_user_from_cache(user_data)

    assert user is not None
    assert user.id == 123
    assert user.email == "test@example.com"
    assert user.name == "Test User"
    assert user.is_active is True

  def test_create_user_from_cache_minimal_data(self):
    """Test creating user with minimal valid cached data."""
    user_data = {
      "id": "user456",
      "email": "minimal@example.com",
    }

    user = _create_user_from_cache(user_data)

    assert user is not None
    assert user.id == "user456"
    assert user.email == "minimal@example.com"
    assert user.name is None
    assert user.is_active is True  # Default value

  def test_create_user_from_cache_invalid_data(self):
    """Test creating user fails with invalid cached data."""
    invalid_data = {"id": None, "email": "test@example.com"}

    with patch("robosystems.middleware.auth.dependencies.logger") as mock_logger:
      user = _create_user_from_cache(invalid_data)

      assert user is None
      mock_logger.warning.assert_called_once_with(
        "Invalid cached user data detected, falling back to database"
      )

  def test_create_user_from_cache_construction_error(self):
    """Test handling of User construction errors."""
    # Valid data structure but causes User constructor error
    user_data = {
      "id": 123,
      "email": "test@example.com",
      "name": "Test User",
      "is_active": True,
    }

    with patch("robosystems.middleware.auth.dependencies.User") as mock_user_class:
      mock_user_class.side_effect = TypeError("Construction error")

      with patch("robosystems.middleware.auth.dependencies.logger") as mock_logger:
        user = _create_user_from_cache(user_data)

        assert user is None
        mock_logger.error.assert_called_once()
        assert (
          "Invalid data type in cached user data" in mock_logger.error.call_args[0][0]
        )

  def test_create_user_from_cache_unexpected_error(self):
    """Test handling of unexpected errors during user creation."""
    user_data = {
      "id": 123,
      "email": "test@example.com",
    }

    with patch("robosystems.middleware.auth.dependencies.User") as mock_user_class:
      mock_user_class.side_effect = RuntimeError("Unexpected error")

      with patch("robosystems.middleware.auth.dependencies.logger") as mock_logger:
        user = _create_user_from_cache(user_data)

        assert user is None
        mock_logger.error.assert_called_once()
        assert (
          "Unexpected error creating User from cached data"
          in mock_logger.error.call_args[0][0]
        )


class TestJWTTokenVerification:
  """Test JWT token verification functionality."""

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  def test_verify_jwt_token_blacklisted(self, mock_is_revoked):
    """Test JWT verification fails for revoked/blacklisted token."""
    token = "blacklisted.jwt.token"
    mock_is_revoked.return_value = True

    result = verify_jwt_token(token)

    assert result is None
    mock_is_revoked.assert_called_once_with(token)

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  def test_verify_jwt_token_cached_valid(self, mock_cache, mock_is_revoked):
    """Test JWT verification uses cached validation."""
    token = "valid.jwt.token"
    cached_data = {"user_data": {"id": "user123"}}

    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = cached_data

    result = verify_jwt_token(token)

    assert result == "user123"
    mock_cache.get_cached_jwt_validation.assert_called_once_with(token)

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  @patch("robosystems.middleware.auth.dependencies.User")
  def test_verify_jwt_token_valid_decode_and_cache(
    self, mock_user_class, mock_jwt_decode, mock_cache, mock_is_revoked
  ):
    """Test JWT verification with valid token decode and caching."""
    token = "valid.jwt.token"
    user_id = "user456"

    # Setup mocks
    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None  # Cache miss
    mock_jwt_decode.return_value = {"user_id": user_id, "jti": "test-jti"}

    # Mock user
    mock_user = Mock()
    mock_user.id = user_id
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    result = verify_jwt_token(token)

    assert result == user_id
    # JWT decode is now called from utils with the secret from config
    mock_jwt_decode.assert_called_once()
    mock_cache.cache_jwt_validation.assert_called_once()

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  def test_verify_jwt_token_invalid_issuer_rejected(
    self, mock_jwt_decode, mock_cache, mock_is_revoked
  ):
    """Test JWT verification rejects tokens without proper issuer/audience claims."""
    token = "legacy.jwt.token"

    # Setup mocks
    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None  # Cache miss

    # Decode raises InvalidIssuerError for missing issuer
    mock_jwt_decode.side_effect = jwt.InvalidIssuerError("No issuer")

    result = verify_jwt_token(token)

    # Should return None for invalid issuer
    assert result is None
    # Should only be called once - no fallback
    assert mock_jwt_decode.call_count == 1

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  def test_verify_jwt_token_invalid_audience_rejected(
    self, mock_jwt_decode, mock_cache, mock_is_revoked
  ):
    """Test JWT verification rejects tokens with invalid audience."""
    token = "bad-audience.jwt.token"

    # Setup mocks
    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None  # Cache miss

    # Decode raises InvalidAudienceError for wrong audience
    mock_jwt_decode.side_effect = jwt.InvalidAudienceError("Wrong audience")

    result = verify_jwt_token(token)

    # Should return None for invalid audience
    assert result is None
    # Should only be called once - no fallback
    assert mock_jwt_decode.call_count == 1

  # Test for no secret key removed - Config class already handles missing JWT secrets properly

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  def test_verify_jwt_token_expired(self, mock_jwt_decode, mock_cache, mock_is_revoked):
    """Test JWT verification handles expired tokens."""
    token = "expired.jwt.token"

    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None
    mock_jwt_decode.side_effect = jwt.ExpiredSignatureError("Token expired")

    result = verify_jwt_token(token)

    assert result is None

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  def test_verify_jwt_token_invalid(self, mock_jwt_decode, mock_cache, mock_is_revoked):
    """Test JWT verification handles invalid tokens."""
    token = "invalid.jwt.token"

    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None
    mock_jwt_decode.side_effect = jwt.InvalidTokenError("Invalid token")

    result = verify_jwt_token(token)

    assert result is None

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  @patch("robosystems.middleware.auth.dependencies.User")
  def test_verify_jwt_token_inactive_user(
    self, mock_user_class, mock_jwt_decode, mock_cache, mock_is_revoked
  ):
    """Test JWT verification fails for inactive users."""
    token = "valid.jwt.token"
    user_id = "user789"

    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None
    mock_jwt_decode.return_value = {"user_id": user_id, "jti": "test-jti"}

    # Mock inactive user
    mock_user = Mock()
    mock_user.is_active = False
    mock_user_class.get_by_id.return_value = mock_user

    result = verify_jwt_token(token)

    assert result is None

  @patch("robosystems.middleware.auth.jwt.is_jwt_token_revoked")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.jwt.jwt.decode")
  @patch("robosystems.middleware.auth.dependencies.User")
  def test_verify_jwt_token_no_user_found(
    self, mock_user_class, mock_jwt_decode, mock_cache, mock_is_revoked
  ):
    """Test JWT verification handles missing user."""
    token = "valid.jwt.token"
    user_id = "nonexistent_user"

    mock_is_revoked.return_value = False
    mock_cache.get_cached_jwt_validation.return_value = None
    mock_jwt_decode.return_value = {"user_id": user_id, "jti": "test-jti"}
    mock_user_class.get_by_id.return_value = None

    result = verify_jwt_token(token)

    assert result is None


class TestGetOptionalUser:
  """Test optional user authentication dependency."""

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies._create_user_from_cache")
  async def test_get_optional_user_jwt_token_cached(
    self, mock_create_user, mock_cache, mock_verify_jwt
  ):
    """Test optional user authentication with valid JWT token from cache."""
    auth_token = "valid.jwt.token"
    user_id = "user123"
    cached_data = {"user_data": {"id": user_id, "email": "test@example.com"}}

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = cached_data

    mock_user = Mock(spec=User)
    mock_create_user.return_value = mock_user

    # Create mock request with authorization header
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    result = await get_optional_user(request=mock_request, api_key=None)

    assert result == mock_user
    mock_verify_jwt.assert_called_once_with(auth_token)
    mock_create_user.assert_called_once_with(cached_data["user_data"])

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  async def test_get_optional_user_jwt_token_database_fallback(
    self, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test optional user authentication with JWT token database fallback."""
    auth_token = "valid.jwt.token"
    user_id = "user123"

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = None  # No cache

    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    # Create mock request with authorization header
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    result = await get_optional_user(request=mock_request, api_key=None)

    assert result == mock_user
    mock_user_class.get_by_id.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.validate_api_key")
  async def test_get_optional_user_api_key_fallback(
    self, mock_validate_api_key, mock_verify_jwt
  ):
    """Test optional user authentication falls back to API key."""
    auth_token = "invalid.jwt.token"
    api_key = "valid-api-key"

    # Create mock request
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    mock_verify_jwt.return_value = None  # JWT validation fails

    mock_user = Mock(spec=User)
    mock_validate_api_key.return_value = mock_user

    result = await get_optional_user(request=mock_request, api_key=api_key)

    assert result == mock_user
    mock_validate_api_key.assert_called_once_with(api_key)

  @pytest.mark.asyncio
  async def test_get_optional_user_no_authentication(self):
    """Test optional user authentication returns None when no auth provided."""
    # Create mock request with no auth
    mock_request = Mock()
    mock_request.headers = {}

    result = await get_optional_user(request=mock_request, api_key=None)

    assert result is None

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.validate_api_key")
  async def test_get_optional_user_both_invalid(
    self, mock_validate_api_key, mock_verify_jwt
  ):
    """Test optional user authentication returns None when both auth methods fail."""
    auth_token = "invalid.jwt.token"
    api_key = "invalid-api-key"

    # Create mock request
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    mock_verify_jwt.return_value = None
    mock_validate_api_key.return_value = None

    result = await get_optional_user(request=mock_request, api_key=api_key)

    assert result is None


class TestGetCurrentUser:
  """Test required user authentication dependency."""

  def setup_method(self):
    """Setup test fixtures."""
    self.mock_request = Mock(spec=Request)
    self.mock_request.client.host = "192.168.1.100"
    self.mock_request.headers = {"user-agent": "TestClient/1.0"}
    self.mock_request.url.path = "/test/endpoint"

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies._create_user_from_cache")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_jwt_cookie_success(
    self, mock_audit_logger, mock_create_user, mock_cache, mock_verify_jwt
  ):
    """Test current user authentication with JWT from cookie."""
    auth_token = "valid.jwt.token"
    user_id = "user123"
    cached_data = {"user_data": {"id": user_id, "email": "test@example.com"}}

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = cached_data

    mock_user = Mock(spec=User)
    mock_create_user.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    result = await get_current_user(self.mock_request, api_key=None)

    assert result == mock_user
    mock_audit_logger.log_auth_success.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_jwt_authorization_header(
    self, mock_audit_logger, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test current user authentication with JWT from Authorization header."""
    authorization = "Bearer valid.jwt.token"
    user_id = "user123"

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = None

    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": authorization}

    result = await get_current_user(self.mock_request, api_key=None)

    assert result == mock_user
    mock_verify_jwt.assert_called_once_with("valid.jwt.token")
    mock_audit_logger.log_auth_success.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_invalid_jwt_token(
    self, mock_audit_logger, mock_verify_jwt
  ):
    """Test current user authentication fails with invalid JWT token."""
    auth_token = "invalid.jwt.token"
    mock_verify_jwt.return_value = None

    # Set authorization header on request
    self.mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user(self.mock_request, api_key=None)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Invalid or expired token" in str(exc_info.value.detail)
    mock_audit_logger.log_security_event.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.validate_api_key")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_api_key_success(
    self, mock_audit_logger, mock_validate_api_key
  ):
    """Test current user authentication with valid API key."""
    api_key = "valid-api-key"

    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_validate_api_key.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": None}

    result = await get_current_user(self.mock_request, api_key=api_key)

    assert result == mock_user
    mock_audit_logger.log_auth_success.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.validate_api_key")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_invalid_api_key(
    self, mock_audit_logger, mock_validate_api_key
  ):
    """Test current user authentication fails with invalid API key."""
    api_key = "invalid-api-key"
    mock_validate_api_key.return_value = None

    # Set empty authorization header on request
    self.mock_request.headers = {}

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user(self.mock_request, api_key=api_key)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Invalid API key" in str(exc_info.value.detail)
    mock_audit_logger.log_security_event.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_no_authentication(self, mock_audit_logger):
    """Test current user authentication fails with no authentication provided."""
    # Set empty authorization header on request
    self.mock_request.headers = {}

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user(self.mock_request, api_key=None)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Authentication required" in str(exc_info.value.detail)
    mock_audit_logger.log_auth_failure.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_jwt_query_param_success(
    self, mock_audit_logger, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test current user authentication with JWT from query parameter (for SSE)."""
    token_value = "valid.jwt.token"
    user_id = "user123"

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = None

    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    result = await get_current_user_sse(
      self.mock_request, api_key=None, authorization=None, token=token_value
    )

    assert result == mock_user
    mock_verify_jwt.assert_called_once_with(token_value)
    mock_audit_logger.log_auth_success.assert_called_once()


class TestGetCurrentUserWithGraph:
  """Test user authentication with graph access validation."""

  def setup_method(self):
    """Setup test fixtures."""
    self.mock_request = Mock(spec=Request)
    self.mock_request.client.host = "192.168.1.100"
    self.mock_request.headers = {"user-agent": "TestClient/1.0"}
    self.mock_request.url.path = "/test/endpoint"
    self.graph_id = "graph123"

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_with_graph_jwt_success(
    self, mock_audit_logger, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test user authentication with graph access via JWT token."""
    auth_token = "valid.jwt.token"
    user_id = "user123"
    cached_data = {
      "user_data": {"id": user_id, "email": "test@example.com", "is_active": True}
    }

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = cached_data
    mock_cache.get_cached_jwt_graph_access.return_value = True  # Has graph access

    # Create user mock
    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    result = await get_current_user_with_graph(
      self.mock_request,
      self.graph_id,
      api_key=None,
    )

    assert result == mock_user
    mock_audit_logger.log_auth_success.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_with_graph_database_fallback(
    self, mock_audit_logger, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test graph access validation with database fallback."""
    auth_token = "valid.jwt.token"
    user_id = "user123"

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = None  # No JWT cache
    mock_cache.get_cached_jwt_graph_access.return_value = None  # No graph cache

    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    # Mock UserGraph access check
    with patch("robosystems.models.iam.UserGraph") as mock_user_graph:
      mock_user_graph.user_has_access.return_value = True

      result = await get_current_user_with_graph(
        self.mock_request,
        self.graph_id,
        api_key=None,
      )

      assert result == mock_user
      mock_user_graph.user_has_access.assert_called_once()
      mock_cache.cache_jwt_graph_access.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  @patch("robosystems.middleware.auth.dependencies.User")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_with_graph_access_denied(
    self, mock_audit_logger, mock_user_class, mock_cache, mock_verify_jwt
  ):
    """Test graph access validation fails when access is denied."""
    auth_token = "valid.jwt.token"
    user_id = "user123"

    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = None
    mock_cache.get_cached_jwt_graph_access.return_value = False  # Access denied

    mock_user = Mock(spec=User)
    mock_user.is_active = True
    mock_user_class.get_by_id.return_value = mock_user

    # Set authorization header on request
    self.mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user_with_graph(
        self.mock_request,
        self.graph_id,
        api_key=None,
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert "Access denied to graph" in str(exc_info.value.detail)
    mock_audit_logger.log_authorization_denied.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.validate_api_key_with_graph")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_with_graph_api_key_success(
    self, mock_audit_logger, mock_validate_api_key_with_graph
  ):
    """Test graph access validation with API key."""
    api_key = "valid-api-key"

    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_validate_api_key_with_graph.return_value = mock_user

    # Set empty authorization header on request
    self.mock_request.headers = {}

    result = await get_current_user_with_graph(
      self.mock_request, self.graph_id, api_key=api_key
    )

    assert result == mock_user
    mock_validate_api_key_with_graph.assert_called_once_with(api_key, self.graph_id)
    mock_audit_logger.log_auth_success.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.validate_api_key_with_graph")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_get_current_user_with_graph_api_key_invalid(
    self, mock_audit_logger, mock_validate_api_key_with_graph
  ):
    """Test graph access validation fails with invalid API key."""
    api_key = "invalid-api-key"
    mock_validate_api_key_with_graph.return_value = None

    # Set empty authorization header on request
    self.mock_request.headers = {}

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user_with_graph(
        self.mock_request,
        self.graph_id,
        api_key=api_key,
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert "Invalid API key or access denied to graph" in str(exc_info.value.detail)
    mock_audit_logger.log_security_event.assert_called_once()


class TestRepositoryAccess:
  """Test repository access validation functionality."""

  def setup_method(self):
    """Setup test fixtures."""
    self.mock_request = Mock(spec=Request)
    self.mock_request.client.host = "192.168.1.100"
    self.mock_request.headers = {"user-agent": "TestClient/1.0"}
    self.mock_request.url.path = "/test/endpoint"

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.get_current_user")
  @patch("robosystems.middleware.auth.dependencies.validate_repository_access")
  async def test_get_current_user_with_repository_access_success(
    self, mock_validate_repo_access, mock_get_current_user
  ):
    """Test repository access validation succeeds."""
    repository_id = "sec"
    operation_type = "read"

    mock_user = Mock(spec=User)
    mock_get_current_user.return_value = mock_user
    mock_validate_repo_access.return_value = True

    result = await get_current_user_with_repository_access(
      self.mock_request,
      repository_id,
      operation_type,
      api_key="test-key",
    )

    assert result == mock_user
    mock_validate_repo_access.assert_called_once_with(
      mock_user, repository_id, operation_type
    )

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.get_current_user")
  @patch("robosystems.middleware.auth.dependencies.validate_repository_access")
  async def test_get_current_user_with_repository_access_denied(
    self, mock_validate_repo_access, mock_get_current_user
  ):
    """Test repository access validation fails."""
    repository_id = "premium_data"
    operation_type = "write"

    mock_user = Mock(spec=User)
    mock_get_current_user.return_value = mock_user
    mock_validate_repo_access.return_value = False

    with pytest.raises(HTTPException) as exc_info:
      await get_current_user_with_repository_access(
        self.mock_request,
        repository_id,
        operation_type,
        api_key="test-key",
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert f"{repository_id.upper()} repository {operation_type} access denied" in str(
      exc_info.value.detail
    )

  def test_get_repository_user_dependency_factory(self):
    """Test repository user dependency factory function."""
    repository_id = "test_repo"
    operation_type = "admin"

    dependency_func = get_repository_user_dependency(repository_id, operation_type)

    assert callable(dependency_func)
    assert dependency_func.__name__ == "_get_repository_user"


class TestAPIKeyHeader:
  """Test API key header configuration."""

  def test_api_key_header_configuration(self):
    """Test API key header is properly configured."""
    assert API_KEY_HEADER.model.name == "X-API-Key"
    assert API_KEY_HEADER.auto_error is False


class TestSecurityScenarios:
  """Test security-focused edge cases and attack scenarios."""

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  async def test_jwt_injection_attempt(self, mock_verify_jwt):
    """Test handling of JWT injection attempts."""
    # Malicious JWT token with injection attempt
    malicious_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYWRtaW47RFJPUCBUQUJMRSB1c2VyczsifQ.malicious"

    # Create mock request
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {malicious_token}"}

    mock_verify_jwt.return_value = None  # Token validation should fail

    result = await get_optional_user(request=mock_request, api_key=None)

    assert result is None
    # verify_jwt_token should be called with just the token (Bearer prefix is stripped)

  def test_cached_user_data_xss_protection(self):
    """Test that cached user data is protected against XSS."""
    # Create mock request
    mock_request = Mock()
    mock_request.headers = {}

    # Attempt XSS through name field
    xss_data = {
      "id": 123,
      "email": "test@example.com",
      "name": "<script>alert('xss')</script>",
      "is_active": True,
    }

    # Validation should pass (XSS filtering is done at presentation layer)
    assert _validate_cached_user_data(xss_data) is True

    # User creation should succeed
    user = _create_user_from_cache(xss_data)
    assert user is not None
    assert user.name == "<script>alert('xss')</script>"

  def test_cached_user_data_sql_injection_protection(self):
    """Test that cached user data protects against SQL injection."""
    sql_injection_data = {
      "id": "1'; DROP TABLE users; --",
      "email": "test@example.com",
      "is_active": True,
    }

    # Data should validate (SQL injection protection is at database layer)
    assert _validate_cached_user_data(sql_injection_data) is True

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.SecurityAuditLogger")
  async def test_brute_force_token_attack_logging(
    self, mock_audit_logger, mock_verify_jwt
  ):
    """Test that brute force token attacks are properly logged."""
    mock_request = Mock(spec=Request)
    mock_request.client.host = "192.168.1.100"
    mock_request.headers = {"user-agent": "AttackBot/1.0"}
    mock_request.url.path = "/secure/endpoint"

    # Simulate multiple failed attempts
    failed_tokens = ["token1", "token2", "token3"]
    mock_verify_jwt.return_value = None

    for token in failed_tokens:
      # Set authorization header for each attempt
      mock_request.headers = {
        "authorization": f"Bearer {token}",
        "user-agent": "AttackBot/1.0",
      }
      with pytest.raises(HTTPException):
        await get_current_user(mock_request, api_key=None)

    # Should log security events for each failed attempt
    assert mock_audit_logger.log_security_event.call_count == len(failed_tokens)

  def test_memory_exhaustion_protection(self):
    """Test protection against memory exhaustion attacks."""
    # Create extremely large user data
    large_name = "A" * 1000000  # 1MB string
    large_data = {
      "id": 123,
      "email": "test@example.com",
      "name": large_name,
      "is_active": True,
    }

    # Should still validate (size limits handled elsewhere)
    assert _validate_cached_user_data(large_data) is True

  @pytest.mark.asyncio
  async def test_concurrent_authentication_requests(self):
    """Test handling of concurrent authentication requests."""
    import asyncio

    # Create mock request with no auth
    mock_request = Mock()
    mock_request.headers = {}

    # Simulate concurrent requests
    tasks = []
    for i in range(10):
      task = get_optional_user(request=mock_request, api_key=None)
      tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should return None (no authentication)
    assert all(result is None for result in results)

  def test_edge_case_email_validation(self):
    """Test edge cases in email validation.

    Note: The current implementation only checks for @ presence,
    not full RFC-compliant email validation.
    """
    edge_case_emails = [
      ("test@", True),  # Has @ so passes basic check
      ("@example.com", True),  # Has @ so passes basic check
      ("test@@example.com", True),  # Has @ so passes basic check
      ("test@example@com", True),  # Has @ so passes basic check
      ("", False),  # Empty - fails
      ("test", False),  # No @ - fails
      ("a@b", True),  # Minimal valid
    ]

    for email, expected_valid in edge_case_emails:
      data = {"id": 123, "email": email}
      is_valid = _validate_cached_user_data(data)
      assert is_valid == expected_valid, f"Email '{email}' validation mismatch"


class TestPerformanceAndCaching:
  """Test performance-related functionality and caching behavior."""

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  @patch("robosystems.middleware.auth.dependencies.api_key_cache")
  async def test_cache_hit_performance(self, mock_cache, mock_verify_jwt):
    """Test that cache hits avoid expensive operations."""
    auth_token = "cached.jwt.token"
    user_id = "user123"

    # Create mock request
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    # Setup cache hit
    cached_data = {"user_data": {"id": user_id, "email": "test@example.com"}}
    mock_verify_jwt.return_value = user_id
    mock_cache.get_cached_jwt_validation.return_value = cached_data

    with patch(
      "robosystems.middleware.auth.dependencies._create_user_from_cache"
    ) as mock_create_user:
      mock_user = Mock(spec=User)
      mock_create_user.return_value = mock_user

      result = await get_optional_user(request=mock_request, api_key=None)

      assert result == mock_user
      # Should use cache, not database
      mock_create_user.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.auth.dependencies.verify_jwt_token")
  async def test_multiple_auth_method_precedence(self, mock_verify_jwt):
    """Test that JWT token takes precedence over API key."""
    auth_token = "valid.jwt.token"
    api_key = "valid-api-key"
    user_id = "user123"

    # Create mock request
    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {auth_token}"}

    mock_verify_jwt.return_value = user_id

    with patch("robosystems.middleware.auth.dependencies.api_key_cache") as mock_cache:
      mock_cache.get_cached_jwt_validation.return_value = None

      with patch("robosystems.middleware.auth.dependencies.User") as mock_user_class:
        mock_user = Mock(spec=User)
        mock_user.is_active = True
        mock_user_class.get_by_id.return_value = mock_user

        with patch(
          "robosystems.middleware.auth.dependencies.validate_api_key"
        ) as mock_validate_api_key:
          result = await get_optional_user(request=mock_request, api_key=api_key)

          assert result == mock_user
          # API key validation should not be called
          mock_validate_api_key.assert_not_called()
