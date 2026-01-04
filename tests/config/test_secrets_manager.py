"""
Tests for the improved secrets manager with TTL caching and better error handling.
"""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.config.secrets_manager import (
  SECRET_MAPPINGS,
  SecretsManager,
  get_secret_value,
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

  def test_refresh_specific_secret_clears_only_target(self):
    """Refreshing a specific secret invalidates only that cache entry."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="staging")

      def secret_response(secret_id):
        payload = {"SecretString": json.dumps({secret_id: secret_id})}
        response = MagicMock()
        response.__getitem__.side_effect = payload.__getitem__
        return payload

      mock_client.get_secret_value.side_effect = [
        {"SecretString": json.dumps({"base": "value1"})},
        {"SecretString": json.dumps({"s3": "value2"})},
        {"SecretString": json.dumps({"s3": "value3"})},
      ]

      manager.get_secret()  # base
      manager.get_secret("s3")
      assert mock_client.get_secret_value.call_count == 2

      manager.refresh("s3")
      manager.get_secret("s3")
      assert mock_client.get_secret_value.call_count == 3
      assert manager._cache[f"{manager.environment}/s3"][0]["s3"] == "value3"
      # base should still be cached
      manager.get_secret()
      assert mock_client.get_secret_value.call_count == 3


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

  def test_unexpected_error_raises_in_prod(self):
    """Unexpected exceptions are surfaced in production."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="prod")
      mock_client.get_secret_value.side_effect = RuntimeError("boom")

      with pytest.raises(RuntimeError):
        manager.get_secret("s3")

  def test_unexpected_error_returns_empty_in_dev(self):
    """Unexpected exceptions do not blow up in dev."""
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client
      mock_client.get_secret_value.side_effect = RuntimeError("boom")

      manager = SecretsManager(environment="dev")
      assert manager.get_secret("s3") == {}


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
    with (
      patch("os.getenv") as mock_getenv,
      patch("robosystems.config.secrets_manager.boto3.client") as mock_boto,
      patch("robosystems.config.secrets_manager._secrets_manager", None),
    ):
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

  def test_get_secret_value_default_for_non_prod(self):
    """Non prod/staging environments should return default when not set."""
    with patch("os.getenv") as mock_getenv:
      mock_getenv.side_effect = lambda key, default=None: {"ENVIRONMENT": "dev"}.get(
        key, default
      )

      assert get_secret_value("JWT_SECRET_KEY", "local_default") == "local_default"

  def test_get_secret_value_base_secret_lookup(self, monkeypatch):
    """Keys without explicit mapping use base secret payload."""
    with patch("os.getenv") as mock_getenv, patch("boto3.client") as mock_boto:
      mock_getenv.side_effect = lambda key, default=None: {
        "ENVIRONMENT": "staging"
      }.get(key, default)

      mock_client = MagicMock()
      mock_boto.return_value = mock_client
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"CUSTOM_KEY": "from_base"})
      }

      from robosystems.config import secrets_manager as module

      monkeypatch.setattr(module, "_secrets_manager", None)

      assert get_secret_value("CUSTOM_KEY", "default") == "from_base"


class TestSecretMappingsConfiguration:
  """Test the externalized secret mappings."""

  def test_all_critical_secrets_mapped(self):
    """Verify all critical secrets are in the mapping."""
    # Note: Bucket names (SEC_RAW_BUCKET, SEC_PROCESSED_BUCKET, etc.) are
    # computed from environment in env.py, not fetched from secrets.
    critical_secrets = [
      "DATABASE_URL",
      "JWT_SECRET_KEY",
      "AWS_S3_ACCESS_KEY_ID",
      "AWS_S3_SECRET_ACCESS_KEY",
      "GRAPH_API_KEY",
    ]

    for secret in critical_secrets:
      assert secret in SECRET_MAPPINGS, f"Critical secret {secret} not mapped"


class TestSecretsManagerHelpers:
  """Tests covering helper methods and global accessors."""

  def test_get_s3_buckets_dev_defaults(self):
    """Test bucket names computed for dev environment (no suffix)."""
    manager = SecretsManager(environment="dev")
    buckets = manager.get_s3_buckets()
    # New bucket names
    assert buckets["shared_raw"] == "robosystems-shared-raw"
    assert buckets["shared_processed"] == "robosystems-shared-processed"
    assert buckets["user_data"] == "robosystems-user"
    assert buckets["public_data"] == "robosystems-public-data"
    # Deprecated aliases point to new names
    assert buckets["aws_s3"] == "robosystems-user"
    assert buckets["sec_raw"] == "robosystems-shared-raw"
    assert buckets["sec_processed"] == "robosystems-shared-processed"

  def test_get_s3_buckets_staging_computed(self):
    """Test bucket names computed for staging environment (with suffix)."""
    # Bucket names are now computed from environment, not fetched from secrets
    manager = SecretsManager(environment="staging")
    buckets = manager.get_s3_buckets()

    # New bucket names with environment suffix
    assert buckets["shared_raw"] == "robosystems-shared-raw-staging"
    assert buckets["shared_processed"] == "robosystems-shared-processed-staging"
    assert buckets["user_data"] == "robosystems-user-staging"
    assert buckets["public_data"] == "robosystems-public-data-staging"
    # Deprecated aliases point to new names
    assert buckets["aws_s3"] == "robosystems-user-staging"
    assert buckets["sec_raw"] == "robosystems-shared-raw-staging"

  def test_get_database_url_non_prod(self):
    manager = SecretsManager(environment="dev")
    assert manager.get_database_url() == ""

  def test_get_database_url_from_secret(self):
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"DATABASE_URL": "postgres://example"})
      }
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="prod")
      assert manager.get_database_url() == "postgres://example"

  def test_get_s3_credentials_default(self):
    manager = SecretsManager(environment="dev")
    creds = manager.get_s3_credentials()
    assert creds == {"access_key_id": "", "secret_access_key": ""}

  def test_get_s3_credentials_from_secret(self):
    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
          {
            "AWS_S3_ACCESS_KEY_ID": "key",
            "AWS_S3_SECRET_ACCESS_KEY": "secret",
          }
        )
      }
      mock_boto.return_value = mock_client

      manager = SecretsManager(environment="prod")
      creds = manager.get_s3_credentials()
      assert creds == {"access_key_id": "key", "secret_access_key": "secret"}

  def test_get_secrets_manager_singleton(self, monkeypatch):
    from robosystems.config import secrets_manager as module

    with patch("boto3.client") as mock_boto:
      mock_client = MagicMock()
      mock_boto.return_value = mock_client

      monkeypatch.setattr(module, "_secrets_manager", None)
      instance1 = module.get_secrets_manager()
      instance2 = module.get_secrets_manager()

      assert instance1 is instance2

  def test_get_s3_bucket_name_warns_when_missing(self, monkeypatch):
    from robosystems.config import secrets_manager as module

    manager = MagicMock()
    manager.get_s3_buckets.return_value = {"sec_raw": "bucket"}
    monkeypatch.setattr(module, "get_secrets_manager", lambda: manager)

    with patch.object(module.logger, "warning") as mock_warning:
      assert module.get_s3_bucket_name("unknown") == ""
      mock_warning.assert_called_once()

  def test_get_s3_bucket_name_maps_purpose(self, monkeypatch):
    from robosystems.config import secrets_manager as module

    manager = MagicMock()
    manager.get_s3_buckets.return_value = {"sec_processed": "bucket-name"}
    monkeypatch.setattr(module, "get_secrets_manager", lambda: manager)

    assert module.get_s3_bucket_name("sec_processed") == "bucket-name"

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
