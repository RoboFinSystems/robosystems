"""Tests for graph_api auth middleware."""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.datastructures import Headers

from robosystems.graph_api.middleware.auth import (
  KuzuAuthMiddleware,
  get_api_key_from_secrets_manager,
  clear_api_key_cache,
  create_api_key,
)


class TestKuzuAuthMiddleware:
  """Test cases for Kuzu authentication middleware."""

  @pytest.fixture
  def mock_request(self):
    """Create a mock request object."""
    request = MagicMock(spec=Request)
    request.url.path = "/databases"
    request.client.host = "192.168.1.1"
    request.headers = Headers({})
    return request

  @pytest.fixture
  def mock_app(self):
    """Create a mock application."""
    return MagicMock()

  @pytest.mark.asyncio
  async def test_middleware_exempt_paths(self, mock_app, mock_request):
    """Test that exempt paths bypass authentication."""
    middleware = KuzuAuthMiddleware(mock_app, api_key="test-key")
    call_next = AsyncMock(return_value=JSONResponse({"status": "ok"}))

    # Test various exempt paths
    for path in KuzuAuthMiddleware.EXEMPT_PATHS:
      mock_request.url.path = path
      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == 200
      call_next.assert_called_with(mock_request)

  @pytest.mark.asyncio
  async def test_middleware_development_bypass(self, mock_app, mock_request):
    """Test that authentication is bypassed in development."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app)
      call_next = AsyncMock(return_value=JSONResponse({"status": "ok"}))

      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == 200
      call_next.assert_called_once_with(mock_request)

  @pytest.mark.asyncio
  async def test_middleware_valid_api_key_header(self, mock_app, mock_request):
    """Test successful authentication with valid API key in header."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="valid-key-123")
      mock_request.headers = Headers({"X-Kuzu-API-Key": "valid-key-123"})
      call_next = AsyncMock(return_value=JSONResponse({"status": "ok"}))

      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == 200
      call_next.assert_called_once_with(mock_request)

  @pytest.mark.asyncio
  async def test_middleware_valid_bearer_token(self, mock_app, mock_request):
    """Test successful authentication with Bearer token."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="bearer-key-456")
      mock_request.headers = Headers({"Authorization": "Bearer bearer-key-456"})
      call_next = AsyncMock(return_value=JSONResponse({"status": "ok"}))

      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == 200
      call_next.assert_called_once_with(mock_request)

  @pytest.mark.asyncio
  async def test_middleware_missing_api_key(self, mock_app, mock_request):
    """Test authentication failure with missing API key."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="secret-key")
      call_next = AsyncMock()

      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == status.HTTP_401_UNAUTHORIZED
      response_body = json.loads(response.body)
      assert "Missing API key" in response_body["detail"]
      call_next.assert_not_called()

  @pytest.mark.asyncio
  async def test_middleware_invalid_api_key(self, mock_app, mock_request):
    """Test authentication failure with invalid API key."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="correct-key")
      mock_request.headers = Headers({"X-Kuzu-API-Key": "wrong-key"})
      call_next = AsyncMock()

      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == status.HTTP_401_UNAUTHORIZED
      response_body = json.loads(response.body)
      assert "Invalid API key" in response_body["detail"]
      call_next.assert_not_called()

  @pytest.mark.asyncio
  async def test_middleware_rate_limiting(self, mock_app, mock_request):
    """Test rate limiting after multiple failed attempts."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="correct-key")
      middleware.max_failed_attempts = 3  # Lower for testing
      mock_request.headers = Headers({"X-Kuzu-API-Key": "wrong-key"})
      call_next = AsyncMock()

      # Make multiple failed attempts
      for _ in range(3):
        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

      # Next attempt should be rate limited
      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
      response_body = json.loads(response.body)
      assert "Too many failed authentication attempts" in response_body["detail"]

  @pytest.mark.asyncio
  async def test_middleware_rate_limit_expiry(self, mock_app, mock_request):
    """Test that rate limiting expires after lockout duration."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="correct-key")
      middleware.max_failed_attempts = 2
      middleware.lockout_duration = 0.1  # 100ms for testing
      mock_request.headers = Headers({"X-Kuzu-API-Key": "wrong-key"})
      call_next = AsyncMock()

      # Make failed attempts to trigger rate limit
      for _ in range(2):
        await middleware.dispatch(mock_request, call_next)

      # Should be rate limited now
      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

      # Wait for lockout to expire
      time.sleep(0.2)

      # Should be able to try again (though still with wrong key)
      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == status.HTTP_401_UNAUTHORIZED  # Not 429

  @pytest.mark.asyncio
  async def test_middleware_resets_failed_attempts_on_success(
    self, mock_app, mock_request
  ):
    """Test that successful auth resets failed attempt counter."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      middleware = KuzuAuthMiddleware(mock_app, api_key="correct-key")
      call_next = AsyncMock(return_value=JSONResponse({"status": "ok"}))

      # Make a failed attempt
      mock_request.headers = Headers({"X-Kuzu-API-Key": "wrong-key"})
      await middleware.dispatch(mock_request, call_next)
      assert "192.168.1.1" in middleware.failed_attempts

      # Successful attempt should clear the counter
      mock_request.headers = Headers({"X-Kuzu-API-Key": "correct-key"})
      response = await middleware.dispatch(mock_request, call_next)
      assert response.status_code == 200
      assert "192.168.1.1" not in middleware.failed_attempts

  def test_middleware_initialization_with_env_key(self, mock_app):
    """Test middleware initialization with key from environment."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = "env-api-key"

      middleware = KuzuAuthMiddleware(mock_app)
      assert middleware.api_key == "env-api-key"
      assert middleware.auth_enabled is True

  def test_middleware_initialization_requires_key_in_prod(self, mock_app):
    """Test that middleware requires API key in production."""
    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      mock_env.KUZU_API_KEY = None

      with patch(
        "robosystems.graph_api.middleware.auth.get_api_key_from_secrets_manager"
      ) as mock_get_key:
        mock_get_key.return_value = None

        with pytest.raises(ValueError, match="KUZU_API_KEY must be set"):
          KuzuAuthMiddleware(mock_app)

  def test_constant_time_compare(self, mock_app):
    """Test constant-time string comparison."""
    middleware = KuzuAuthMiddleware(mock_app, api_key="test")

    # Test equal strings
    assert middleware._constant_time_compare("test123", "test123") is True

    # Test different strings
    assert middleware._constant_time_compare("test123", "test456") is False

    # Test different lengths
    assert middleware._constant_time_compare("short", "longer_string") is False

  def test_cleanup_failed_attempts(self, mock_app):
    """Test cleanup of expired failed attempts."""
    middleware = KuzuAuthMiddleware(mock_app, api_key="test")
    middleware.lockout_duration = 0.1  # 100ms for testing

    # Add some failed attempts
    current_time = time.time()
    middleware.failed_attempts = {
      "192.168.1.1": (5, current_time - 0.2),  # Expired
      "192.168.1.2": (3, current_time - 0.05),  # Not expired
      "192.168.1.3": (10, current_time - 0.3),  # Expired
    }

    middleware._cleanup_failed_attempts()

    # Only non-expired entry should remain
    assert len(middleware.failed_attempts) == 1
    assert "192.168.1.2" in middleware.failed_attempts


class TestSecretsManagerIntegration:
  """Test cases for Secrets Manager integration."""

  @patch("robosystems.graph_api.middleware.auth.boto3.client")
  def test_get_api_key_from_secrets_manager_success(self, mock_boto_client):
    """Test successful API key retrieval from Secrets Manager."""
    # Clear cache first
    clear_api_key_cache()

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.get_secret_value.return_value = {
      "SecretString": json.dumps({"KUZU_API_KEY": "secret-key-123"})
    }

    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      api_key = get_api_key_from_secrets_manager(key_type="writer")
      assert api_key == "secret-key-123"
      mock_client.get_secret_value.assert_called_once_with(
        SecretId="robosystems/prod/kuzu"
      )

  @patch("robosystems.graph_api.middleware.auth.boto3.client")
  def test_get_api_key_from_secrets_manager_not_found(self, mock_boto_client):
    """Test handling of missing secret in Secrets Manager."""
    # Clear cache first
    clear_api_key_cache()

    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.get_secret_value.side_effect = ClientError(
      {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
    )

    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"

      api_key = get_api_key_from_secrets_manager()
      assert api_key is None

  @patch("robosystems.graph_api.middleware.auth.boto3.client")
  def test_get_api_key_from_secrets_manager_no_key_in_secret(self, mock_boto_client):
    """Test handling when secret exists but doesn't contain API key."""
    # Clear cache first
    clear_api_key_cache()

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.get_secret_value.return_value = {
      "SecretString": json.dumps({"OTHER_KEY": "value"})
    }

    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      api_key = get_api_key_from_secrets_manager()
      assert api_key is None

  @patch("robosystems.graph_api.middleware.auth.boto3.client")
  def test_get_api_key_caching(self, mock_boto_client):
    """Test that API key is cached after first retrieval."""
    # Clear cache first
    clear_api_key_cache()

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.get_secret_value.return_value = {
      "SecretString": json.dumps({"KUZU_API_KEY": "cached-key"})
    }

    with patch("robosystems.graph_api.middleware.auth.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      # First call
      api_key1 = get_api_key_from_secrets_manager(key_type="writer")
      assert api_key1 == "cached-key"

      # Second call should use cache
      api_key2 = get_api_key_from_secrets_manager(key_type="writer")
      assert api_key2 == "cached-key"

      # Should only have called Secrets Manager once
      assert mock_client.get_secret_value.call_count == 1


class TestAPIKeyGeneration:
  """Test cases for API key generation."""

  def test_create_api_key(self):
    """Test secure API key generation."""
    with patch(
      "robosystems.graph_api.middleware.auth.SecurityAuditLogger"
    ) as mock_logger:
      api_key, key_hash = create_api_key(prefix="test")

      # Check API key format
      assert api_key.startswith("test_")
      assert len(api_key) > 50  # Should be long enough

      # Check that hash is different from key
      assert key_hash != api_key
      assert len(key_hash) > 50

      # Verify security logging
      mock_logger.log_security_event.assert_called_once()

  def test_create_api_key_unique(self):
    """Test that generated API keys are unique."""
    keys = set()
    for _ in range(10):
      api_key, _ = create_api_key()
      keys.add(api_key)

    # All keys should be unique
    assert len(keys) == 10

  @patch("robosystems.graph_api.middleware.auth.bcrypt")
  def test_create_api_key_bcrypt_hashing(self, mock_bcrypt):
    """Test that bcrypt is used for hashing."""
    mock_salt = b"$2b$12$test_salt"
    mock_hash = b"$2b$12$hashed_value"
    mock_bcrypt.gensalt.return_value = mock_salt
    mock_bcrypt.hashpw.return_value = mock_hash

    api_key, key_hash = create_api_key(prefix="kuzu")

    # Verify bcrypt was called correctly
    mock_bcrypt.gensalt.assert_called_once_with(rounds=12)
    mock_bcrypt.hashpw.assert_called_once()
    assert key_hash == mock_hash.decode("utf-8")
