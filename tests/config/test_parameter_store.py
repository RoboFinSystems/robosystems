"""
Tests for the SSM Parameter Store integration for feature flags.

These are pure unit tests that mock AWS SSM, requiring no database or external services.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.config.parameter_store import (
  ParameterStoreManager,
  get_parameter_manager,
  get_parameter_value,
  preload_feature_flags,
)

# Mark all tests in this module as unit tests (no database required)
pytestmark = pytest.mark.unit


class TestParameterStoreManagerCaching:
  """Test TTL-based caching functionality."""

  def test_cache_ttl_respects_expiry(self, monkeypatch):
    """Test that cached parameters expire after TTL."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      # Create manager with 1 second TTL
      manager = ParameterStoreManager(environment="staging", cache_ttl_seconds=1)

      # Setup mock response
      mock_client.get_parameter.return_value = {"Parameter": {"Value": "true"}}

      # First call should hit AWS
      result1 = manager.get_parameter("RATE_LIMIT_ENABLED")
      assert result1 == "true"
      assert mock_client.get_parameter.call_count == 1

      # Immediate second call should use cache
      result2 = manager.get_parameter("RATE_LIMIT_ENABLED")
      assert result2 == "true"
      assert mock_client.get_parameter.call_count == 1

      # Advance the manager's clock past the TTL instead of sleeping.
      import time as _t
      from types import SimpleNamespace

      future = _t.time() + manager.cache_ttl_seconds + 1
      monkeypatch.setattr(
        "robosystems.config.parameter_store.time",
        SimpleNamespace(time=lambda: future),
      )

      # Next call should hit AWS again
      result3 = manager.get_parameter("RATE_LIMIT_ENABLED")
      assert result3 == "true"
      assert mock_client.get_parameter.call_count == 2

  def test_cache_refresh_clears_cache(self):
    """Test that refresh() properly clears cached entries."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      manager = ParameterStoreManager(environment="staging")

      mock_client.get_parameter.return_value = {"Parameter": {"Value": "true"}}

      # First call populates cache
      manager.get_parameter("BILLING_ENABLED")
      assert mock_client.get_parameter.call_count == 1

      # Refresh cache
      manager.refresh()

      # Next call should hit AWS again
      manager.get_parameter("BILLING_ENABLED")
      assert mock_client.get_parameter.call_count == 2

  def test_refresh_specific_parameter_clears_only_target(self):
    """Refreshing a specific parameter invalidates only that cache entry."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      manager = ParameterStoreManager(environment="staging")

      mock_client.get_parameter.side_effect = [
        {"Parameter": {"Value": "true"}},
        {"Parameter": {"Value": "false"}},
        {"Parameter": {"Value": "updated"}},
      ]

      manager.get_parameter("RATE_LIMIT_ENABLED")
      manager.get_parameter("BILLING_ENABLED")
      assert mock_client.get_parameter.call_count == 2

      # Refresh only BILLING_ENABLED
      manager.refresh("BILLING_ENABLED")
      manager.get_parameter("BILLING_ENABLED")
      assert mock_client.get_parameter.call_count == 3

      # RATE_LIMIT_ENABLED should still be cached
      manager.get_parameter("RATE_LIMIT_ENABLED")
      assert mock_client.get_parameter.call_count == 3


class TestParameterStoreManagerEnvironment:
  """Test environment-based behavior."""

  def test_dev_environment_returns_default(self):
    """Test that dev environment returns default without calling AWS."""
    manager = ParameterStoreManager(environment="dev")

    result = manager.get_parameter("RATE_LIMIT_ENABLED", default="default_value")
    assert result == "default_value"

  def test_staging_environment_calls_aws(self):
    """Test that staging environment calls AWS."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      mock_client.get_parameter.return_value = {"Parameter": {"Value": "true"}}

      manager = ParameterStoreManager(environment="staging")
      result = manager.get_parameter("RATE_LIMIT_ENABLED")

      assert result == "true"
      mock_client.get_parameter.assert_called_once()

  def test_prod_environment_calls_aws(self):
    """Test that prod environment calls AWS."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      mock_client.get_parameter.return_value = {"Parameter": {"Value": "false"}}

      manager = ParameterStoreManager(environment="prod")
      result = manager.get_parameter("USER_REGISTRATION_ENABLED")

      assert result == "false"
      mock_client.get_parameter.assert_called_once()


class TestParameterStoreManagerErrorHandling:
  """Test error handling."""

  def test_parameter_not_found_returns_default(self):
    """Test that missing parameters return default value."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      # Create a proper exception class mock
      mock_client.exceptions = MagicMock()
      mock_client.exceptions.ParameterNotFound = type(
        "ParameterNotFound", (Exception,), {}
      )
      mock_client.get_parameter.side_effect = mock_client.exceptions.ParameterNotFound(
        "Not found"
      )

      manager = ParameterStoreManager(environment="staging")
      result = manager.get_parameter("NONEXISTENT", default="fallback")

      assert result == "fallback"

  def test_generic_exception_returns_default(self):
    """Test that generic exceptions return default value."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client
      mock_client.exceptions = MagicMock()
      mock_client.exceptions.ParameterNotFound = type(
        "ParameterNotFound", (Exception,), {}
      )
      mock_client.get_parameter.side_effect = Exception("Network error")

      manager = ParameterStoreManager(environment="staging")
      result = manager.get_parameter("RATE_LIMIT_ENABLED", default="true")

      assert result == "true"

  def test_missing_client_returns_default(self):
    """Test that missing boto3 client returns default."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_get_client.return_value = None

      manager = ParameterStoreManager(environment="staging")
      result = manager.get_parameter("RATE_LIMIT_ENABLED", default="fallback")

      assert result == "fallback"


class TestBatchFetch:
  """Test batch fetching of feature flags."""

  def test_batch_fetch_returns_all_parameters(self):
    """Test that batch fetch returns all parameters."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      # Setup paginator mock
      mock_paginator = MagicMock()
      mock_client.get_paginator.return_value = mock_paginator
      mock_paginator.paginate.return_value = [
        {
          "Parameters": [
            {
              "Name": "/robosystems/staging/features/RATE_LIMIT_ENABLED",
              "Value": "true",
            },
            {"Name": "/robosystems/staging/features/BILLING_ENABLED", "Value": "false"},
          ]
        }
      ]

      manager = ParameterStoreManager(environment="staging")
      result = manager.get_all_feature_flags()

      assert result == {
        "RATE_LIMIT_ENABLED": "true",
        "BILLING_ENABLED": "false",
      }

  def test_batch_fetch_caches_results(self):
    """Test that batch fetch results are cached."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      mock_paginator = MagicMock()
      mock_client.get_paginator.return_value = mock_paginator
      mock_paginator.paginate.return_value = [
        {
          "Parameters": [
            {"Name": "/robosystems/staging/features/TEST_FLAG", "Value": "value"},
          ]
        }
      ]

      manager = ParameterStoreManager(environment="staging")

      # First call
      manager.get_all_feature_flags()
      assert mock_client.get_paginator.call_count == 1

      # Second call should use cache
      manager.get_all_feature_flags()
      assert mock_client.get_paginator.call_count == 1

  def test_batch_fetch_dev_returns_empty(self):
    """Test that batch fetch in dev returns empty dict."""
    manager = ParameterStoreManager(environment="dev")
    result = manager.get_all_feature_flags()
    assert result == {}


class TestGetParameterValue:
  """Test the get_parameter_value convenience function."""

  def test_env_var_takes_precedence(self):
    """Test that environment variables take precedence over SSM."""
    with patch("os.getenv") as mock_getenv:
      mock_getenv.side_effect = lambda key, default=None: {
        "RATE_LIMIT_ENABLED": "env_value",
        "ENVIRONMENT": "staging",
      }.get(key, default)

      result = get_parameter_value("RATE_LIMIT_ENABLED", "default")
      assert result == "env_value"

  def test_ssm_value_used_when_no_env_var(self):
    """Test that SSM value is used when no env var is set."""
    with (
      patch("os.getenv") as mock_getenv,
      patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client,
      patch("robosystems.config.parameter_store._parameter_manager", None),
    ):
      mock_getenv.side_effect = lambda key, default=None: {
        "ENVIRONMENT": "staging",
        "AWS_REGION": "us-east-1",
      }.get(key, default)

      mock_client = MagicMock()
      mock_get_client.return_value = mock_client
      mock_client.get_parameter.return_value = {"Parameter": {"Value": "ssm_value"}}

      result = get_parameter_value("BILLING_ENABLED", "default")
      assert result == "ssm_value"

  def test_default_used_in_dev(self):
    """Test that default is used in dev environment."""
    with patch("os.getenv") as mock_getenv:
      mock_getenv.side_effect = lambda key, default=None: {"ENVIRONMENT": "dev"}.get(
        key, default
      )

      result = get_parameter_value("RATE_LIMIT_ENABLED", "default_value")
      assert result == "default_value"

  def test_default_used_when_ssm_fails(self):
    """Test that default is used when SSM fails."""
    with (
      patch("os.getenv") as mock_getenv,
      patch(
        "robosystems.config.parameter_store.get_parameter_manager"
      ) as mock_get_manager,
    ):
      mock_getenv.side_effect = lambda key, default=None: {
        "ENVIRONMENT": "staging"
      }.get(key, default)

      mock_manager = MagicMock()
      mock_get_manager.return_value = mock_manager
      mock_manager.get_parameter.side_effect = Exception("Network error")

      result = get_parameter_value("RATE_LIMIT_ENABLED", "fallback")
      assert result == "fallback"


class TestPreloadFeatureFlags:
  """Test the preload_feature_flags function."""

  def test_preload_in_dev_returns_empty(self):
    """Test that preload in dev returns empty dict."""
    with patch("os.getenv") as mock_getenv:
      mock_getenv.return_value = "dev"

      result = preload_feature_flags()
      assert result == {}

  def test_preload_calls_batch_fetch(self):
    """Test that preload calls batch fetch in prod/staging."""
    with (
      patch("os.getenv") as mock_getenv,
      patch(
        "robosystems.config.parameter_store.get_parameter_manager"
      ) as mock_get_manager,
    ):
      mock_getenv.return_value = "staging"

      mock_manager = MagicMock()
      mock_get_manager.return_value = mock_manager
      mock_manager.get_all_feature_flags.return_value = {
        "FLAG1": "value1",
        "FLAG2": "value2",
      }

      result = preload_feature_flags()

      assert result == {"FLAG1": "value1", "FLAG2": "value2"}
      mock_manager.get_all_feature_flags.assert_called_once()


class TestParameterManagerSingleton:
  """Test the singleton pattern for parameter manager."""

  def test_get_parameter_manager_returns_same_instance(self, monkeypatch):
    """Test that get_parameter_manager returns the same instance."""
    from robosystems.config import parameter_store as module

    monkeypatch.setattr(module, "_parameter_manager", None)

    instance1 = get_parameter_manager()
    instance2 = get_parameter_manager()

    assert instance1 is instance2


class TestParameterNaming:
  """Test parameter naming conventions."""

  def test_parameter_path_format(self):
    """Test that parameter paths are correctly formatted."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client
      mock_client.get_parameter.return_value = {"Parameter": {"Value": "test"}}

      manager = ParameterStoreManager(environment="prod")
      manager.get_parameter("RATE_LIMIT_ENABLED")

      mock_client.get_parameter.assert_called_with(
        Name="/robosystems/prod/features/RATE_LIMIT_ENABLED"
      )

  def test_batch_fetch_path_format(self):
    """Test that batch fetch uses correct path prefix."""
    with patch("robosystems.config.parameter_store._get_ssm_client") as mock_get_client:
      mock_client = MagicMock()
      mock_get_client.return_value = mock_client

      mock_paginator = MagicMock()
      mock_client.get_paginator.return_value = mock_paginator
      mock_paginator.paginate.return_value = [{"Parameters": []}]

      manager = ParameterStoreManager(environment="staging")
      manager.get_all_feature_flags()

      mock_paginator.paginate.assert_called_with(
        Path="/robosystems/staging/features",
        Recursive=True,
      )
