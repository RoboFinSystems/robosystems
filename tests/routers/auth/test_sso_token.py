"""
Tests for SSO token generation endpoint.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
from fastapi import HTTPException, status

from robosystems.routers.auth.sso import generate_sso_token
from robosystems.models.iam import User


@pytest.fixture
def mock_user():
  """Create a mock user for testing."""
  user = Mock(spec=User)
  user.id = "user_123"
  user.name = "Test User"
  user.email = "test@example.com"
  user.is_active = True
  return user


class TestGenerateSSOToken:
  """Test the /sso-token endpoint."""

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.create_sso_token")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_generate_sso_token_with_bearer(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_create_sso,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test SSO token generation with Bearer token authentication."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_create_sso.return_value = ("sso_token_value", "token_id_123")

    # Mock Redis client
    mock_redis_instance = AsyncMock()
    mock_redis_instance.get.return_value = None
    mock_redis_instance.setex.return_value = True
    mock_redis_client.return_value = mock_redis_instance

    # Mock lock manager
    mock_lock_manager.return_value = None  # Test without locking

    mock_session = Mock()

    # Call endpoint with Bearer token
    result = await generate_sso_token(
      authorization="Bearer valid_jwt_token",
      auth_token=None,
      session=mock_session,
      _rate_limit=None,
    )

    # Verify result
    assert result.token == "sso_token_value"
    assert isinstance(result.expires_at, datetime)
    assert result.apps == ["roboledger", "roboinvestor", "robosystems"]

    # Verify mocks were called correctly
    mock_verify_jwt.assert_called_once_with("valid_jwt_token")
    mock_get_by_id.assert_called_once_with("user_123", mock_session)
    mock_create_sso.assert_called_once_with("user_123")

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.create_sso_token")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_generate_sso_token_with_cookie_fallback(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_create_sso,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test SSO token generation with cookie fallback (backward compatibility)."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_create_sso.return_value = ("sso_token_value", "token_id_123")

    # Mock Redis client
    mock_redis_instance = AsyncMock()
    mock_redis_instance.get.return_value = None
    mock_redis_instance.setex.return_value = True
    mock_redis_client.return_value = mock_redis_instance

    # Mock lock manager
    mock_lock_manager.return_value = None

    mock_session = Mock()

    # Call endpoint with cookie token (no Bearer)
    result = await generate_sso_token(
      authorization=None,
      auth_token="cookie_jwt_token",
      session=mock_session,
      _rate_limit=None,
    )

    # Verify result
    assert result.token == "sso_token_value"

    # Verify cookie token was used
    mock_verify_jwt.assert_called_once_with("cookie_jwt_token")

  async def test_generate_sso_token_no_auth(self):
    """Test SSO token generation with no authentication."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      await generate_sso_token(
        authorization=None,
        auth_token=None,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"
    assert exc_info.value.headers == {"WWW-Authenticate": "Bearer"}

  async def test_generate_sso_token_malformed_bearer(self):
    """Test SSO token generation with malformed Bearer token."""
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      await generate_sso_token(
        authorization="InvalidHeader",  # Missing "Bearer "
        auth_token=None,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Not authenticated"

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  async def test_generate_sso_token_invalid_jwt(self, mock_verify_jwt):
    """Test SSO token generation with invalid JWT."""
    mock_verify_jwt.return_value = None  # Invalid token
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      await generate_sso_token(
        authorization="Bearer invalid_token",
        auth_token=None,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Invalid or expired token"

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  async def test_generate_sso_token_user_not_found(
    self, mock_get_by_id, mock_verify_jwt
  ):
    """Test SSO token generation when user not found."""
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = None  # User not found
    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      await generate_sso_token(
        authorization="Bearer valid_token",
        auth_token=None,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found or inactive"

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  async def test_generate_sso_token_inactive_user(
    self, mock_get_by_id, mock_verify_jwt
  ):
    """Test SSO token generation with inactive user."""
    mock_verify_jwt.return_value = "user_123"

    # Create inactive user
    inactive_user = Mock(spec=User)
    inactive_user.id = "user_123"
    inactive_user.is_active = False
    mock_get_by_id.return_value = inactive_user

    mock_session = Mock()

    with pytest.raises(HTTPException) as exc_info:
      await generate_sso_token(
        authorization="Bearer valid_token",
        auth_token=None,
        session=mock_session,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found or inactive"

  @patch("robosystems.routers.auth.sso.verify_jwt_token")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.create_sso_token")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_bearer_takes_precedence_over_cookie(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_create_sso,
    mock_get_by_id,
    mock_verify_jwt,
    mock_user,
  ):
    """Test that Bearer token takes precedence over cookie when both provided."""
    # Setup mocks
    mock_verify_jwt.return_value = "user_123"
    mock_get_by_id.return_value = mock_user
    mock_create_sso.return_value = ("sso_token_value", "token_id_123")

    # Mock Redis client
    mock_redis_instance = AsyncMock()
    mock_redis_instance.get.return_value = None
    mock_redis_instance.setex.return_value = True
    mock_redis_client.return_value = mock_redis_instance

    # Mock lock manager
    mock_lock_manager.return_value = None

    mock_session = Mock()

    # Call endpoint with both Bearer and cookie
    result = await generate_sso_token(
      authorization="Bearer bearer_jwt_token",
      auth_token="cookie_jwt_token",
      session=mock_session,
      _rate_limit=None,
    )

    # Verify Bearer token was used, not cookie
    mock_verify_jwt.assert_called_once_with("bearer_jwt_token")
    assert result.token == "sso_token_value"
