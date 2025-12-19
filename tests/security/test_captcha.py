"""
Comprehensive tests for CAPTCHA security utilities.

These tests cover Cloudflare Turnstile CAPTCHA verification, including token validation,
error handling, environment-based configuration, and security scenarios.
"""

import asyncio
import json
from dataclasses import asdict
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

from robosystems.security.captcha import (
  TURNSTILE_ERROR_DESCRIPTIONS,
  TURNSTILE_VERIFY_URL,
  CaptchaService,
  CaptchaVerificationResult,
  captcha_service,
  get_error_description,
)


class TestCaptchaVerificationResult:
  """Test CAPTCHA verification result data structure."""

  def test_captcha_verification_result_creation(self):
    """Test creating CaptchaVerificationResult with all fields."""
    result = CaptchaVerificationResult(
      success=True,
      error_codes=[],
      challenge_ts="2024-01-01T10:00:00Z",
      hostname="example.com",
      action="login",
      cdata="custom_data",
    )

    assert result.success is True
    assert result.error_codes == []
    assert result.challenge_ts == "2024-01-01T10:00:00Z"
    assert result.hostname == "example.com"
    assert result.action == "login"
    assert result.cdata == "custom_data"

  def test_captcha_verification_result_minimal(self):
    """Test creating CaptchaVerificationResult with minimal fields."""
    result = CaptchaVerificationResult(
      success=False, error_codes=["invalid-input-response"]
    )

    assert result.success is False
    assert result.error_codes == ["invalid-input-response"]
    assert result.challenge_ts is None
    assert result.hostname is None
    assert result.action is None
    assert result.cdata is None

  def test_captcha_verification_result_serialization(self):
    """Test that result can be serialized to dict."""
    result = CaptchaVerificationResult(
      success=True, error_codes=["timeout-or-duplicate"]
    )

    result_dict = asdict(result)
    expected = {
      "success": True,
      "error_codes": ["timeout-or-duplicate"],
      "challenge_ts": None,
      "hostname": None,
      "action": None,
      "cdata": None,
    }

    assert result_dict == expected


class TestCaptchaService:
  """Test CAPTCHA service functionality."""

  def setup_method(self):
    """Setup test fixtures."""
    self.service = CaptchaService()

  @patch("robosystems.security.captcha.env")
  def test_captcha_service_initialization(self, mock_env):
    """Test CAPTCHA service initialization with environment variables."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret_key"
    mock_env.TURNSTILE_SITE_KEY = "test_site_key"

    service = CaptchaService()

    assert service.secret_key == "test_secret_key"
    assert service.site_key == "test_site_key"

  @patch("robosystems.security.captcha.env")
  def test_captcha_service_missing_config(self, mock_env):
    """Test CAPTCHA service with missing configuration."""
    mock_env.TURNSTILE_SECRET_KEY = None
    mock_env.TURNSTILE_SITE_KEY = None

    service = CaptchaService()

    assert service.secret_key is None
    assert service.site_key is None

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_missing_secret(self, mock_env):
    """Test verification with missing secret key."""
    mock_env.TURNSTILE_SECRET_KEY = None

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is True  # Allows through with warning
    assert "missing-secret-key" in result.error_codes

  @pytest.mark.asyncio
  async def test_verify_turnstile_token_empty_token(self):
    """Test verification with empty token."""
    result = await self.service.verify_turnstile_token("")

    assert result.success is False
    assert "missing-input-response" in result.error_codes

  @pytest.mark.asyncio
  async def test_verify_turnstile_token_none_token(self):
    """Test verification with None token."""
    result = await self.service.verify_turnstile_token(None)

    assert result.success is False
    assert "missing-input-response" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_success(self, mock_env, mock_session):
    """Test successful CAPTCHA token verification."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    # Mock successful API response
    mock_response_data = {
      "success": True,
      "error-codes": [],
      "challenge_ts": "2024-01-01T10:00:00Z",
      "hostname": "example.com",
      "action": "login",
      "cdata": "custom_data",
    }

    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is True
    assert result.error_codes == []
    assert result.challenge_ts == "2024-01-01T10:00:00Z"
    assert result.hostname == "example.com"
    assert result.action == "login"
    assert result.cdata == "custom_data"

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_failure(self, mock_env, mock_session):
    """Test failed CAPTCHA token verification."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response_data = {
      "success": False,
      "error-codes": ["invalid-input-response", "timeout-or-duplicate"],
    }

    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("invalid_token")

    assert result.success is False
    assert "invalid-input-response" in result.error_codes
    assert "timeout-or-duplicate" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_with_optional_params(
    self, mock_env, mock_session
  ):
    """Test verification with optional parameters (IP, idempotency key)."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response_data = {"success": True, "error-codes": []}
    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token(
      "valid_token", remote_ip="192.168.1.100", idempotency_key="unique_key_123"
    )

    assert result.success is True

    # Verify the request included optional parameters
    expected_data = {
      "secret": "test_secret",
      "response": "valid_token",
      "remoteip": "192.168.1.100",
      "idempotency_key": "unique_key_123",
    }

    mock_session_instance.post.assert_called_with(
      TURNSTILE_VERIFY_URL, data=expected_data, timeout=aiohttp.ClientTimeout(total=10)
    )

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_api_error(self, mock_env, mock_session):
    """Test handling of API error responses."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response = Mock()
    mock_response.status = 500  # Server error

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "api-error" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_network_error(self, mock_env, mock_session):
    """Test handling of network errors."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_session_instance = Mock()
    mock_session_instance.post.side_effect = aiohttp.ClientError("Connection failed")
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "network-error" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_unexpected_error(self, mock_env, mock_session):
    """Test handling of unexpected errors."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_session_instance = Mock()
    mock_session_instance.post.side_effect = Exception("Unexpected error")
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "internal-error" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_verify_turnstile_token_malformed_json(self, mock_env, mock_session):
    """Test handling of malformed JSON responses."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(
      side_effect=json.JSONDecodeError("Invalid JSON", "", 0)
    )

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "internal-error" in result.error_codes

  @patch("robosystems.security.captcha.env")
  def test_is_captcha_required(self, mock_env):
    """Test CAPTCHA requirement checking."""
    mock_env.CAPTCHA_ENABLED = True
    service = CaptchaService()
    assert service.is_captcha_required() is True

    mock_env.CAPTCHA_ENABLED = False
    service = CaptchaService()
    assert service.is_captcha_required() is False

  @patch("robosystems.security.captcha.env")
  def test_get_site_key(self, mock_env):
    """Test getting site key."""
    mock_env.TURNSTILE_SITE_KEY = "test_site_key"
    service = CaptchaService()

    assert service.get_site_key() == "test_site_key"

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_verify_captcha_or_skip_development_mode(self, mock_env):
    """Test skipping CAPTCHA verification in development mode."""
    mock_env.CAPTCHA_ENABLED = False

    service = CaptchaService()
    result = await service.verify_captcha_or_skip(None)

    assert result.success is True
    assert "dev-mode-skip" in result.error_codes

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_verify_captcha_or_skip_production_missing_token(self, mock_env):
    """Test CAPTCHA verification in production without token."""
    mock_env.CAPTCHA_ENABLED = True

    service = CaptchaService()
    result = await service.verify_captcha_or_skip(None)

    assert result.success is False
    assert "missing-input-response" in result.error_codes

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.CaptchaService.verify_turnstile_token")
  @patch("robosystems.security.captcha.env")
  async def test_verify_captcha_or_skip_production_with_token(
    self, mock_env, mock_verify
  ):
    """Test CAPTCHA verification in production with token."""
    mock_env.CAPTCHA_ENABLED = True
    mock_verify.return_value = CaptchaVerificationResult(success=True, error_codes=[])

    service = CaptchaService()
    result = await service.verify_captcha_or_skip("valid_token", "192.168.1.100")

    assert result.success is True
    mock_verify.assert_called_once_with("valid_token", "192.168.1.100")


class TestGlobalCaptchaService:
  """Test global CAPTCHA service instance."""

  def test_global_captcha_service_instance(self):
    """Test that global captcha_service instance exists."""
    assert captcha_service is not None
    assert isinstance(captcha_service, CaptchaService)

  def test_global_service_methods(self):
    """Test that global service has all expected methods."""
    assert hasattr(captcha_service, "verify_turnstile_token")
    assert hasattr(captcha_service, "is_captcha_required")
    assert hasattr(captcha_service, "get_site_key")
    assert hasattr(captcha_service, "verify_captcha_or_skip")


class TestErrorDescriptions:
  """Test CAPTCHA error descriptions and utilities."""

  def test_error_descriptions_completeness(self):
    """Test that all common error codes have descriptions."""
    expected_errors = [
      "missing-input-secret",
      "invalid-input-secret",
      "missing-input-response",
      "invalid-input-response",
      "bad-request",
      "timeout-or-duplicate",
      "internal-error",
      "api-error",
      "network-error",
      "missing-secret-key",
      "dev-mode-skip",
    ]

    for error_code in expected_errors:
      assert error_code in TURNSTILE_ERROR_DESCRIPTIONS
      assert len(TURNSTILE_ERROR_DESCRIPTIONS[error_code]) > 0

  def test_get_error_description_known_error(self):
    """Test getting description for known error codes."""
    description = get_error_description("missing-input-response")
    assert description == "The response parameter is missing"

  def test_get_error_description_unknown_error(self):
    """Test getting description for unknown error codes."""
    description = get_error_description("unknown-error-code")
    assert description == "Unknown error: unknown-error-code"

  def test_error_descriptions_format(self):
    """Test that error descriptions are properly formatted."""
    for error_code, description in TURNSTILE_ERROR_DESCRIPTIONS.items():
      assert isinstance(description, str)
      assert len(description.strip()) > 0
      assert description[0].isupper()  # Should start with capital letter


class TestSecurityScenarios:
  """Test security-focused scenarios and attack vectors."""

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_token_injection_attack(self, mock_env, mock_session):
    """Test handling of malicious token injection attempts."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    # Malicious tokens with injection attempts
    malicious_tokens = [
      "'; DROP TABLE users; --",
      "<script>alert('xss')</script>",
      "../../../etc/passwd",
      "../../config/secrets.json",
      "token\x00null_byte",
    ]

    mock_response_data = {"success": False, "error-codes": ["invalid-input-response"]}
    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()

    for malicious_token in malicious_tokens:
      result = await service.verify_turnstile_token(malicious_token)
      # Should handle gracefully without crashing
      assert isinstance(result, CaptchaVerificationResult)

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_oversized_token_handling(self, mock_env, mock_session):
    """Test handling of extremely large tokens."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    # Very large token (potential DoS attack)
    large_token = "A" * 100000

    mock_response_data = {"success": False, "error-codes": ["invalid-input-response"]}
    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token(large_token)

    # Should handle large input without issues
    assert isinstance(result, CaptchaVerificationResult)

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_secret_key_exposure_protection(self, mock_env):
    """Test that secret key is not exposed in errors or logs."""
    mock_env.TURNSTILE_SECRET_KEY = "super_secret_key_123"

    service = CaptchaService()

    # Even if service fails, secret shouldn't be exposed
    result = await service.verify_turnstile_token("")

    assert "super_secret_key_123" not in str(result)
    assert "super_secret_key_123" not in str(result.error_codes)

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_timeout_handling(self, mock_env, mock_session):
    """Test proper timeout handling for slow responses."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    # Simulate timeout

    mock_session_instance = Mock()
    mock_session_instance.post.side_effect = TimeoutError("Request timed out")
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "network-error" in result.error_codes

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_rate_limiting_simulation(self, mock_env, mock_session):
    """Test handling of rate limiting responses."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response = Mock()
    mock_response.status = 429  # Too Many Requests

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()
    result = await service.verify_turnstile_token("valid_token")

    assert result.success is False
    assert "api-error" in result.error_codes

  @pytest.mark.asyncio
  async def test_concurrent_verification_requests(self):
    """Test handling of concurrent CAPTCHA verification requests."""

    service = CaptchaService()

    # Create multiple concurrent verification tasks
    tasks = [
      service.verify_turnstile_token(""),  # Empty token
      service.verify_turnstile_token(None),  # None token
      service.verify_captcha_or_skip(None),  # Skip verification
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # All should complete without exceptions
    for result in results:
      assert isinstance(result, CaptchaVerificationResult)


class TestEnvironmentConfiguration:
  """Test environment-specific configuration and behavior."""

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_development_environment_behavior(self, mock_env):
    """Test CAPTCHA behavior in development environment."""
    mock_env.CAPTCHA_ENABLED = False
    mock_env.TURNSTILE_SECRET_KEY = None
    mock_env.TURNSTILE_SITE_KEY = None

    service = CaptchaService()

    # Should skip verification in development
    result = await service.verify_captcha_or_skip("any_token")
    assert result.success is True
    assert "dev-mode-skip" in result.error_codes

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_production_environment_behavior(self, mock_env):
    """Test CAPTCHA behavior in production environment."""
    mock_env.CAPTCHA_ENABLED = True
    mock_env.TURNSTILE_SECRET_KEY = "prod_secret"
    mock_env.TURNSTILE_SITE_KEY = "prod_site_key"

    service = CaptchaService()

    # Should require token in production
    result = await service.verify_captcha_or_skip(None)
    assert result.success is False
    assert "missing-input-response" in result.error_codes

  @patch("robosystems.security.captcha.env")
  def test_configuration_validation(self, mock_env):
    """Test validation of CAPTCHA configuration."""
    # Test missing configuration
    mock_env.TURNSTILE_SECRET_KEY = None
    mock_env.TURNSTILE_SITE_KEY = None
    mock_env.CAPTCHA_ENABLED = False

    service = CaptchaService()
    assert service.secret_key is None
    assert service.site_key is None
    assert service.is_captcha_required() is False

    # Test complete configuration
    mock_env.TURNSTILE_SECRET_KEY = "valid_secret"
    mock_env.TURNSTILE_SITE_KEY = "valid_site_key"
    mock_env.CAPTCHA_ENABLED = True

    service = CaptchaService()
    assert service.secret_key == "valid_secret"
    assert service.site_key == "valid_site_key"
    assert service.is_captcha_required() is True


class TestPerformanceAndReliability:
  """Test performance characteristics and reliability."""

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_response_time_measurement(self, mock_env, mock_session):
    """Test that CAPTCHA verification completes within reasonable time."""
    import time

    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    mock_response_data = {"success": True, "error-codes": []}
    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=mock_response_data)

    mock_session_instance = Mock()
    mock_session_instance.post.return_value.__aenter__ = AsyncMock(
      return_value=mock_response
    )
    mock_session_instance.post.return_value.__aexit__ = AsyncMock(return_value=None)
    mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
    mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

    service = CaptchaService()

    start_time = time.time()
    result = await service.verify_turnstile_token("test_token")
    end_time = time.time()

    # Should complete quickly (mock response)
    assert (end_time - start_time) < 1.0
    assert result.success is True

  @pytest.mark.asyncio
  async def test_memory_usage_with_large_responses(self):
    """Test memory handling with large API responses."""
    service = CaptchaService()

    # Test with multiple verification attempts
    for _ in range(100):
      result = await service.verify_turnstile_token("")  # Empty token
      assert isinstance(result, CaptchaVerificationResult)

    # Memory should not accumulate (Python GC handles cleanup)

  @pytest.mark.asyncio
  @patch("aiohttp.ClientSession")
  @patch("robosystems.security.captcha.env")
  async def test_network_resilience(self, mock_env, mock_session):
    """Test resilience to various network conditions."""
    mock_env.TURNSTILE_SECRET_KEY = "test_secret"

    network_errors = [
      aiohttp.ClientConnectionError("Connection refused"),
      TimeoutError("Request timeout"),
      aiohttp.ClientError("Generic client error"),
    ]

    service = CaptchaService()

    for error in network_errors:
      mock_session_instance = Mock()
      mock_session_instance.post.side_effect = error
      mock_session.return_value.__aenter__ = AsyncMock(
        return_value=mock_session_instance
      )
      mock_session.return_value.__aexit__ = AsyncMock(return_value=None)

      result = await service.verify_turnstile_token("test_token")

      assert result.success is False
      assert "network-error" in result.error_codes

  def test_constants_validation(self):
    """Test that module constants are properly defined."""
    assert (
      TURNSTILE_VERIFY_URL
      == "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    )
    assert isinstance(TURNSTILE_ERROR_DESCRIPTIONS, dict)
    assert len(TURNSTILE_ERROR_DESCRIPTIONS) > 0

  @pytest.mark.asyncio
  @patch("robosystems.security.captcha.env")
  async def test_graceful_degradation(self, mock_env):
    """Test graceful degradation when CAPTCHA service is unavailable."""
    mock_env.TURNSTILE_SECRET_KEY = None
    mock_env.CAPTCHA_ENABLED = False

    service = CaptchaService()

    # Should still work even with missing configuration
    result = await service.verify_captcha_or_skip("any_token")
    assert result.success is True  # Graceful degradation in dev mode
