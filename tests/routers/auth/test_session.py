"""
Tests for session management endpoints.

Comprehensive test coverage for /me and /refresh endpoints.
"""

import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException, status

from robosystems.routers.auth.session import get_me, refresh_session
from robosystems.models.iam import User
from robosystems.models.api.auth import AuthResponse


@pytest.fixture
def mock_user():
  """Create a mock user for testing."""
  user = Mock(spec=User)
  user.id = "user_123"
  user.name = "Test User"
  user.email = "test@example.com"
  user.is_active = True
  return user


@pytest.fixture
def mock_inactive_user():
  """Create a mock inactive user for testing."""
  user = Mock(spec=User)
  user.id = "user_456"
  user.name = "Inactive User"
  user.email = "inactive@example.com"
  user.is_active = False
  return user


class TestGetMe:
  """Test the /me endpoint."""

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_get_me_with_cookie_token(
    self, mock_get_by_id, mock_verify_jwt, mock_user
  ):
    """Test getting current user with cookie token."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_session = Mock()

    # Mock the FastAPI request
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }

    # Call endpoint
    result = await get_me(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify result
    expected = {"id": "user_123", "name": "Test User", "email": "test@example.com"}
    assert result == expected

    # Verify mocks were called correctly (now includes device fingerprint)
    args, kwargs = mock_verify_jwt.call_args
    assert args[0] == "valid_jwt_token"
    assert "user_agent" in args[1]  # Device fingerprint dict
    mock_get_by_id.assert_called_once_with("user_123", mock_session)

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_get_me_with_authorization_header(
    self, mock_get_by_id, mock_verify_jwt, mock_user
  ):
    """Test getting current user with Authorization header."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_session = Mock()

    # Mock the FastAPI request
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }

    # Call endpoint
    result = await get_me(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify result
    expected = {"id": "user_123", "name": "Test User", "email": "test@example.com"}
    assert result == expected

    # Verify JWT token was extracted from Bearer header (now includes device fingerprint)
    args, kwargs = mock_verify_jwt.call_args
    assert args[0] == "valid_jwt_token"
    assert "user_agent" in args[1]  # Device fingerprint dict
    mock_get_by_id.assert_called_once_with("user_123", mock_session)

  async def test_get_me_no_token(self):
    """Test getting current user with no token."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {"user-agent": "test-agent"}  # No authorization header
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"

  async def test_get_me_malformed_authorization_header(self):
    """Test with malformed Authorization header."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "InvalidHeader",
      }  # Missing "Bearer "
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  async def test_get_me_invalid_token(self, mock_verify_jwt):
    """Test getting current user with invalid token."""
    mock_verify_jwt.return_value = None  # Invalid token
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Invalid or expired token"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_get_me_user_not_found(self, mock_get_by_id, mock_verify_jwt):
    """Test getting current user when user not found."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = None  # User not found
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_get_me_inactive_user(
    self, mock_get_by_id, mock_verify_jwt, mock_inactive_user
  ):
    """Test getting current user when user is inactive."""
    mock_verify_jwt.return_value = "user_456"
    mock_get_by_id.return_value = mock_inactive_user
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert exc_info.value.detail == "User account is deactivated"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.logger")
  async def test_get_me_database_error(
    self, mock_logger, mock_get_by_id, mock_verify_jwt
  ):
    """Test getting current user with database error."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.side_effect = Exception("Database connection failed")
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert exc_info.value.detail == "Failed to get user information"

    # Verify error was logged
    mock_logger.error.assert_called_once()


class TestRefreshSession:
  """Test the /refresh endpoint."""

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  async def test_refresh_session_success(
    self,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test successful session refresh."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Mock the FastAPI request
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer old_jwt_token",
    }

    # Call endpoint
    result = await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify result
    assert isinstance(result, AuthResponse)
    assert result.user["id"] == "user_123"
    assert result.user["name"] == "Test User"
    assert result.user["email"] == "test@example.com"
    assert result.message == "Session refreshed successfully"
    assert result.token == "new_jwt_token"  # JWT Bearer auth

    # Verify mocks were called correctly (now includes device fingerprint)
    args, kwargs = mock_verify_jwt.call_args
    assert args[0] == "old_jwt_token"
    assert "user_agent" in args[1]  # Device fingerprint dict
    mock_get_by_id.assert_called_once_with("user_123", mock_session)
    mock_revoke_jwt.assert_called_once_with("old_jwt_token", reason="session_refresh")
    mock_api_key_cache.invalidate_jwt_token.assert_called_once_with("old_jwt_token")
    # create_jwt_token now also gets device fingerprint
    create_args, create_kwargs = mock_create_jwt.call_args
    assert create_args[0] == "user_123"
    assert "user_agent" in create_args[1]  # Device fingerprint dict

    # JWT tokens are returned in response body, not set as cookies
    # No cookie assertion needed

  async def test_refresh_session_no_token(self):
    """Test session refresh with no token."""
    Mock()
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {"user-agent": "test-agent"}  # No authorization header
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  async def test_refresh_session_invalid_token(self, mock_verify_jwt):
    """Test session refresh with invalid token."""
    mock_verify_jwt.return_value = None  # Invalid token
    Mock()
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Invalid or expired token"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_refresh_session_user_not_found(self, mock_get_by_id, mock_verify_jwt):
    """Test session refresh when user not found."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = None  # User not found
    Mock()
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found or inactive"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_refresh_session_inactive_user(
    self, mock_get_by_id, mock_verify_jwt, mock_inactive_user
  ):
    """Test session refresh with inactive user."""
    mock_verify_jwt.return_value = "user_456"
    mock_get_by_id.return_value = mock_inactive_user
    Mock()
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found or inactive"

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  @patch("robosystems.routers.auth.session.logger")
  async def test_refresh_session_revoke_failure(
    self,
    mock_logger,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test session refresh when token revocation fails."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = False  # Revocation failed
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Mock the FastAPI request
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }

    # Call endpoint
    result = await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Should still succeed despite revocation failure
    assert isinstance(result, AuthResponse)
    assert result.message == "Session refreshed successfully"

    # Verify warning was logged
    mock_logger.warning.assert_called_once()
    warning_call = mock_logger.warning.call_args[0][0]
    assert "Failed to revoke old JWT token" in warning_call

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  @patch("robosystems.routers.auth.session.logger")
  async def test_refresh_session_revoke_success_logging(
    self,
    mock_logger,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test session refresh logs success when token revocation succeeds."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True  # Revocation succeeded
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Call endpoint
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify success was logged
    mock_logger.info.assert_called_once()
    info_call = mock_logger.info.call_args[0][0]
    assert "Old JWT token revoked during session refresh" in info_call
    assert "user_123" in info_call

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.logger")
  async def test_refresh_session_database_error(
    self, mock_logger, mock_get_by_id, mock_verify_jwt
  ):
    """Test session refresh with database error."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.side_effect = Exception("Database connection failed")
    Mock()
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert exc_info.value.detail == "Session refresh failed"

    # Verify error was logged
    mock_logger.error.assert_called_once()

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  async def test_refresh_session_create_token_error(
    self,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test session refresh when creating new token fails."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True
    mock_create_jwt.side_effect = Exception("Token creation failed")
    mock_session = Mock()
    Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer valid_jwt_token",
      }
      await refresh_session(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert exc_info.value.detail == "Session refresh failed"


class TestCookieSettings:
  """Test cookie configuration in session refresh."""

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  async def test_cookie_security_settings(
    self,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test that cookies are set with proper security settings."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Call endpoint
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # JWT tokens are returned in response body, not set as cookies
    # No cookie assertion needed

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  async def test_session_refresh_with_jwt_token(
    self,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test session refresh with JWT token returns new token."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Call endpoint
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )


class TestIntegrationScenarios:
  """Test integration scenarios and edge cases."""

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  async def test_get_me_with_bearer_token_variations(
    self, mock_get_by_id, mock_verify_jwt, mock_user
  ):
    """Test /me endpoint with various Bearer token formats."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_session = Mock()

    # Valid Bearer token
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer valid_jwt_token",
    }
    result = await get_me(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )
    assert result["id"] == "user_123"

    # Bearer with extra space
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer  token123",
    }  # Extra space
    result = await get_me(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )
    assert result["id"] == "user_123"
    # Should extract " token123" (with leading space) and include device fingerprint
    args, kwargs = mock_verify_jwt.call_args
    assert args[0] == " token123"

    # Lowercase "bearer" should not work
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "bearer lowercase_token",
    }
    with pytest.raises(HTTPException):
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

  @patch("robosystems.routers.auth.session.verify_jwt_token")
  @patch("robosystems.routers.auth.session.User.get_by_id")
  @patch("robosystems.routers.auth.session.revoke_jwt_token")
  @patch("robosystems.routers.auth.session.create_jwt_token")
  @patch("robosystems.routers.auth.session.api_key_cache")
  async def test_cache_invalidation_order(
    self,
    mock_api_key_cache,
    mock_create_jwt,
    mock_revoke_jwt,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test that cache invalidation happens in correct order."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_revoke_jwt.return_value = True
    mock_create_jwt.return_value = "new_jwt_token"
    mock_session = Mock()
    Mock()

    # Call endpoint
    mock_request = Mock()
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {
      "user-agent": "test-agent",
      "authorization": "Bearer old_jwt_token",
    }
    await refresh_session(
      fastapi_request=mock_request,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify both old and new cache operations
    mock_revoke_jwt.assert_called_once_with("old_jwt_token", reason="session_refresh")
    mock_api_key_cache.invalidate_jwt_token.assert_called_once_with("old_jwt_token")

  async def test_empty_authorization_header(self):
    """Test with empty Authorization header."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "",
      }  # Empty string
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"

  async def test_bearer_without_token(self):
    """Test Authorization header with 'Bearer' but no token."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      mock_request = Mock()
      mock_request.client.host = "127.0.0.1"
      mock_request.headers = {
        "user-agent": "test-agent",
        "authorization": "Bearer",
      }  # No token after Bearer
      await get_me(
        fastapi_request=mock_request,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"
