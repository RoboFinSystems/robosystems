"""
Tests for the improved secrets manager with TTL caching and better error handling.
"""

import json
import time
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError

from robosystems.config.secrets_manager import (
  SecretsManager,
  get_secret_value,
  SECRET_MAPPINGS,
)


class TestSecretsManagerTTL:
  """Test TTL-based caching functionality."""

  def test_cache_ttl_respects_expiry(self):
    """Test that cached secrets expire after TTL."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      # Create manager with 1 second TTL
      manager = SecretsManager(environment="staging", cache_ttl_seconds=1)

      # Setup mock response
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"TEST_KEY": "test_value"})
      }

      # First call should hit AWS
      result1 = manager.get_secret()
      assert result1["TEST_KEY"] == "test_value"
      assert mock_client.get_secret_value.call_count == 1

      # Immediate second call should use cache
      result2 = manager.get_secret()
      assert result2["TEST_KEY"] == "test_value"
      assert mock_client.get_secret_value.call_count == 1

      # Wait for TTL to expire
      time.sleep(1.1)

      # Next call should hit AWS again
      result3 = manager.get_secret()
      assert result3["TEST_KEY"] == "test_value"
      assert mock_client.get_secret_value.call_count == 2

  def test_cache_refresh_clears_cache(self):
    """Test that refresh() properly clears cached entries."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="staging")

      # Setup mock response
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"TEST_KEY": "test_value"})
      }

      # First call populates cache
      manager.get_secret()
      assert mock_client.get_secret_value.call_count == 1

      # Refresh cache
      manager.refresh()

      # Next call should hit AWS again
      manager.get_secret()
      assert mock_client.get_secret_value.call_count == 2


class TestSecretsManagerErrorHandling:
  """Test improved error handling."""

  def test_resource_not_found_returns_empty(self):
    """Test that missing secrets return empty dict."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="staging")

      # Setup mock to raise ResourceNotFoundException
      mock_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
      )

      # Should return empty dict, not raise
      result = manager.get_secret()
      assert result == {}

  def test_access_denied_raises_in_production(self):
    """Test that access denied raises in production."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="prod")

      # Setup mock to raise AccessDeniedException
      mock_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException"}}, "GetSecretValue"
      )

      # Should raise in production
      with pytest.raises(ClientError):
        manager.get_secret()

  def test_access_denied_returns_empty_in_dev(self):
    """Test that access denied returns empty in dev."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="dev")

      # Dev environment should return empty immediately
      result = manager.get_secret()
      assert result == {}

      # Should not have called AWS
      mock_client.get_secret_value.assert_not_called()


class TestSecretValueFunction:
  """Test the get_secret_value convenience function."""

  def test_get_secret_value_with_env_var(self):
    """Test that environment variables take precedence."""
    with patch("os.getenv") as mock_getenv:
      mock_getenv.side_effect = lambda key, default=None: {
        "JWT_SECRET_KEY": "env_jwt_secret",
        "ENVIRONMENT": "staging",
      }.get(key, default)

      # Should return env var even in staging
      result = get_secret_value("JWT_SECRET_KEY", "default")
      assert result == "env_jwt_secret"

  def test_get_secret_value_from_mappings(self):
    """Test that mapped secrets are retrieved correctly."""
    with patch("os.getenv") as mock_getenv, patch("boto3.client") as mock_boto:
      mock_getenv.side_effect = lambda key, default=None: {
        "ENVIRONMENT": "staging"
      }.get(key, default)

      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      # Setup mock for S3 secrets
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"SEC_RAW_BUCKET": "robosystems-sec-raw-staging"})
      }

      result = get_secret_value("SEC_RAW_BUCKET", "default")
      assert result == "robosystems-sec-raw-staging"

  def test_get_secret_value_handles_exceptions(self):
    """Test that exceptions are caught and defaults returned."""
    with patch("os.getenv") as mock_getenv, patch("boto3.client") as mock_boto:
      mock_getenv.side_effect = lambda key, default=None: {
        "ENVIRONMENT": "staging"
      }.get(key, default)

      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      # Setup mock to raise exception
      mock_client.get_secret_value.side_effect = Exception("Network error")

      # Should return default, not raise
      result = get_secret_value("JWT_SECRET_KEY", "fallback_value")
      assert result == "fallback_value"


class TestSecretMappingsConfiguration:
  """Test the externalized secret mappings."""

  def test_all_critical_secrets_mapped(self):
    """Verify all critical secrets are in the mapping."""
    critical_secrets = [
      "DATABASE_URL",
      "JWT_SECRET_KEY",
      "AWS_S3_ACCESS_KEY_ID",
      "AWS_S3_SECRET_ACCESS_KEY",
      "SEC_RAW_BUCKET",
      "SEC_PROCESSED_BUCKET",
      "KUZU_API_KEY",
      "ANTHROPIC_API_KEY",
    ]

    for secret in critical_secrets:
      assert secret in SECRET_MAPPINGS, f"Critical secret {secret} not mapped"

  def test_mappings_have_valid_structure(self):
    """Verify all mappings have correct structure."""
    for key, value in SECRET_MAPPINGS.items():
      assert isinstance(value, tuple), f"Mapping for {key} is not a tuple"
      assert len(value) == 2, f"Mapping for {key} should have exactly 2 elements"

      secret_type, secret_key = value
      assert secret_type is None or isinstance(secret_type, str), (
        f"Secret type for {key} should be None or string"
      )
      assert isinstance(secret_key, str), f"Secret key for {key} should be string"
