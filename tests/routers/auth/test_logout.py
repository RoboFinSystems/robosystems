"""
Tests for user logout endpoint.

These tests cover the logout functionality including JWT token revocation
and cache invalidation.
"""

from unittest.mock import Mock, patch

import pytest

from robosystems.routers.auth.logout import logout, router


class TestLogoutEndpoint:
  """Test the logout endpoint."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logout_with_valid_jwt_token(self, mock_cache, mock_revoke):
    """Test logout with a valid JWT token."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = True

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    assert result == {"message": "Logout successful"}
    mock_revoke.assert_called_once_with("valid.jwt.token", reason="user_logout")
    mock_cache.invalidate_jwt_token.assert_called_once_with("valid.jwt.token")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logout_with_revocation_failure(self, mock_cache, mock_revoke):
    """Test logout when token revocation fails."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = False

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should still succeed even if revocation fails
    assert result == {"message": "Logout successful"}
    mock_revoke.assert_called_once()
    # Cache should still be invalidated
    mock_cache.invalidate_jwt_token.assert_called_once_with("valid.jwt.token")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logout_with_revocation_exception(self, mock_cache, mock_revoke):
    """Test logout when token revocation throws an exception."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.side_effect = Exception("Revocation failed")

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should still succeed even if revocation throws
    assert result == {"message": "Logout successful"}

  @pytest.mark.asyncio
  async def test_logout_without_token(self):
    """Test logout without a JWT token."""
    mock_request = Mock()
    mock_request.headers = {}
    mock_response = Mock()

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should succeed even without a token
    assert result == {"message": "Logout successful"}

  @pytest.mark.asyncio
  async def test_logout_with_empty_authorization_header(self):
    """Test logout with an empty authorization header."""
    mock_request = Mock()
    mock_request.headers = {"authorization": ""}
    mock_response = Mock()

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    assert result == {"message": "Logout successful"}

  @pytest.mark.asyncio
  async def test_logout_with_non_bearer_authorization(self):
    """Test logout with non-Bearer authorization."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Basic dXNlcjpwYXNz"}
    mock_response = Mock()

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should succeed but not try to revoke since it's not a Bearer token
    assert result == {"message": "Logout successful"}

  @pytest.mark.asyncio
  async def test_logout_handles_general_exception(self):
    """Test logout handles general exceptions gracefully."""
    mock_request = Mock()
    # Make headers.get raise an exception
    mock_request.headers = Mock()
    mock_request.headers.get = Mock(side_effect=Exception("Unexpected error"))
    mock_response = Mock()

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should return completed message even on error
    assert result == {"message": "Logout completed"}

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logout_with_bearer_only(self, mock_cache, mock_revoke):
    """Test logout with 'Bearer ' prefix but no actual token."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer "}
    mock_response = Mock()

    result = await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Empty token should still be processed (though it won't do much)
    assert result == {"message": "Logout successful"}


class TestLogoutTokenExtraction:
  """Test JWT token extraction from authorization header."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_extracts_token_correctly(self, mock_cache, mock_revoke):
    """Test that token is correctly extracted from Bearer header."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer my.jwt.token.here"}
    mock_response = Mock()

    mock_revoke.return_value = True

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Verify the token extracted correctly (7 chars = "Bearer ")
    mock_revoke.assert_called_once_with("my.jwt.token.here", reason="user_logout")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_token_with_special_characters(self, mock_cache, mock_revoke):
    """Test token extraction with special characters."""
    # JWT tokens can contain base64url characters
    special_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"

    mock_request = Mock()
    mock_request.headers = {"authorization": f"Bearer {special_token}"}
    mock_response = Mock()

    mock_revoke.return_value = True

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    mock_revoke.assert_called_once_with(special_token, reason="user_logout")


class TestLogoutLogging:
  """Test logging behavior during logout."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.logger")
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logs_successful_revocation(self, mock_cache, mock_revoke, mock_logger):
    """Test that successful revocation is logged."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = True

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    mock_logger.info.assert_called_once_with("JWT token successfully revoked on logout")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.logger")
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logs_failed_revocation(self, mock_cache, mock_revoke, mock_logger):
    """Test that failed revocation is logged."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = False

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    mock_logger.warning.assert_any_call("Failed to revoke JWT token on logout")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.logger")
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_logs_revocation_exception(self, mock_cache, mock_revoke, mock_logger):
    """Test that revocation exception is logged."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.side_effect = Exception("Revocation error")

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Should log warning about the failure
    mock_logger.warning.assert_called()
    log_call = str(mock_logger.warning.call_args)
    assert "Failed to revoke JWT token during logout" in log_call


class TestLogoutCacheInvalidation:
  """Test cache invalidation during logout."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_cache_invalidated_after_revocation(self, mock_cache, mock_revoke):
    """Test that cache is invalidated after token revocation."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = True

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Verify both revocation and cache invalidation happened
    mock_revoke.assert_called_once()
    mock_cache.invalidate_jwt_token.assert_called_once_with("valid.jwt.token")

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.logout.revoke_jwt_token")
  @patch("robosystems.routers.auth.logout.api_key_cache")
  async def test_cache_invalidated_even_on_revocation_failure(
    self, mock_cache, mock_revoke
  ):
    """Test that cache is invalidated even when revocation fails."""
    mock_request = Mock()
    mock_request.headers = {"authorization": "Bearer valid.jwt.token"}
    mock_response = Mock()

    mock_revoke.return_value = False

    await logout(
      request=mock_request,
      response=mock_response,
      _rate_limit=None,
    )

    # Cache should still be invalidated
    mock_cache.invalidate_jwt_token.assert_called_once_with("valid.jwt.token")


class TestLogoutRouter:
  """Test the logout router configuration."""

  def test_router_has_logout_endpoint(self):
    """Test that router has the logout endpoint."""
    routes = [route.path for route in router.routes]
    assert "/logout" in routes

  def test_logout_endpoint_is_post(self):
    """Test that logout endpoint uses POST method."""
    for route in router.routes:
      if route.path == "/logout":
        assert "POST" in route.methods
        break
    else:
      pytest.fail("Logout route not found")

  def test_logout_endpoint_configuration(self):
    """Test logout endpoint configuration."""
    for route in router.routes:
      if route.path == "/logout":
        assert route.operation_id == "logoutUser"
        break
    else:
      pytest.fail("Logout route not found")
