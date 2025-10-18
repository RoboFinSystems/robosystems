"""Tests for Kuzu API client configuration."""

import os
from unittest.mock import patch
import pytest

from robosystems.graph_api.client.config import GraphClientConfig


class TestGraphClientConfig:
  """Test cases for GraphClientConfig."""

  def test_default_configuration(self):
    """Test default configuration values."""
    config = GraphClientConfig()

    assert config.base_url == ""
    assert config.timeout == 30
    assert config.max_retries == 3
    assert config.retry_delay == 1.0
    assert config.retry_backoff == 2.0
    assert config.max_connections == 100
    assert config.max_keepalive_connections == 20
    assert config.keepalive_expiry == 5.0
    assert config.circuit_breaker_threshold == 5
    assert config.circuit_breaker_timeout == 60
    assert config.headers == {}
    assert config.verify_ssl is True

  def test_custom_configuration(self):
    """Test creating config with custom values."""
    config = GraphClientConfig(
      base_url="http://localhost:8001",
      timeout=60,
      max_retries=5,
      headers={"X-Custom": "value"},
      verify_ssl=False,
    )

    assert config.base_url == "http://localhost:8001"
    assert config.timeout == 60
    assert config.max_retries == 5
    assert config.headers == {"X-Custom": "value"}
    assert config.verify_ssl is False

  def test_from_env_with_defaults(self):
    """Test creating config from environment with no env vars set."""
    with patch.dict(os.environ, {}, clear=True):
      config = GraphClientConfig.from_env()

      # Should use defaults when no env vars are set
      assert config.base_url == ""
      assert config.timeout == 30
      assert config.max_retries == 3

  def test_from_env_with_all_values(self):
    """Test creating config from environment with all values set."""
    env_vars = {
      "KUZU_CLIENT_BASE_URL": "http://api.example.com",
      "KUZU_CLIENT_TIMEOUT": "120",
      "KUZU_CLIENT_MAX_RETRIES": "10",
      "KUZU_CLIENT_RETRY_DELAY": "2.5",
      "KUZU_CLIENT_RETRY_BACKOFF": "3.0",
      "KUZU_CLIENT_MAX_CONNECTIONS": "200",
      "KUZU_CLIENT_MAX_KEEPALIVE_CONNECTIONS": "50",
      "KUZU_CLIENT_KEEPALIVE_EXPIRY": "10.0",
      "KUZU_CLIENT_CIRCUIT_BREAKER_THRESHOLD": "10",
      "KUZU_CLIENT_CIRCUIT_BREAKER_TIMEOUT": "120",
      "KUZU_CLIENT_VERIFY_SSL": "false",
    }

    with patch.dict(os.environ, env_vars):
      config = GraphClientConfig.from_env()

      assert config.base_url == "http://api.example.com"
      assert config.timeout == 120
      assert config.max_retries == 10
      assert config.retry_delay == 2.5
      assert config.retry_backoff == 3.0
      assert config.max_connections == 200
      assert config.max_keepalive_connections == 50
      assert config.keepalive_expiry == 10.0
      assert config.circuit_breaker_threshold == 10
      assert config.circuit_breaker_timeout == 120
      assert config.verify_ssl is False

  def test_from_env_with_custom_prefix(self):
    """Test creating config from environment with custom prefix."""
    env_vars = {
      "MY_KUZU_BASE_URL": "http://custom.example.com",
      "MY_KUZU_TIMEOUT": "90",
      "MY_KUZU_MAX_RETRIES": "7",
    }

    with patch.dict(os.environ, env_vars):
      config = GraphClientConfig.from_env(prefix="MY_KUZU_")

      assert config.base_url == "http://custom.example.com"
      assert config.timeout == 90
      assert config.max_retries == 7

  def test_from_env_boolean_parsing(self):
    """Test boolean parsing from environment variables."""
    test_cases = [
      ("true", True),
      ("True", True),
      ("TRUE", True),
      ("1", True),
      ("yes", True),
      ("YES", True),
      ("false", False),
      ("False", False),
      ("FALSE", False),
      ("0", False),
      ("no", False),
      ("NO", False),
      ("invalid", False),  # Any other value defaults to False
    ]

    for value, expected in test_cases:
      with patch.dict(os.environ, {"KUZU_CLIENT_VERIFY_SSL": value}):
        config = GraphClientConfig.from_env()
        assert config.verify_ssl is expected, f"Failed for value: {value}"

  def test_from_env_partial_values(self):
    """Test creating config from environment with partial values."""
    env_vars = {
      "KUZU_CLIENT_BASE_URL": "http://partial.example.com",
      "KUZU_CLIENT_MAX_RETRIES": "8",
      # Other values should use defaults
    }

    with patch.dict(os.environ, env_vars):
      config = GraphClientConfig.from_env()

      assert config.base_url == "http://partial.example.com"
      assert config.max_retries == 8
      # Check that other values are defaults
      assert config.timeout == 30
      assert config.retry_delay == 1.0
      assert config.verify_ssl is True

  def test_with_overrides(self):
    """Test creating new config with overridden values."""
    original = GraphClientConfig(
      base_url="http://original.com",
      timeout=30,
      max_retries=3,
      headers={"X-Original": "value"},
    )

    # Override some values
    modified = original.with_overrides(
      base_url="http://modified.com", timeout=60, headers={"X-Modified": "new"}
    )

    # Check modified values
    assert modified.base_url == "http://modified.com"
    assert modified.timeout == 60
    assert modified.headers == {"X-Modified": "new"}

    # Check unchanged values
    assert modified.max_retries == 3

    # Check original is unchanged
    assert original.base_url == "http://original.com"
    assert original.timeout == 30
    assert original.headers == {"X-Original": "value"}

  def test_with_overrides_preserves_defaults(self):
    """Test that with_overrides preserves unspecified values."""
    original = GraphClientConfig()

    modified = original.with_overrides(base_url="http://new.com")

    assert modified.base_url == "http://new.com"
    # All other values should be preserved
    assert modified.timeout == original.timeout
    assert modified.max_retries == original.max_retries
    assert modified.retry_delay == original.retry_delay
    assert modified.verify_ssl == original.verify_ssl

  def test_headers_are_copied(self):
    """Test that headers are properly copied in with_overrides."""
    original_headers = {"X-Original": "value"}
    original = GraphClientConfig(headers=original_headers)

    modified = original.with_overrides()

    # Modify the new headers
    modified.headers["X-New"] = "new_value"

    # Original should be unchanged
    assert "X-New" not in original.headers
    assert original.headers == {"X-Original": "value"}

  def test_invalid_env_values(self):
    """Test handling of invalid environment variable values."""
    env_vars = {
      "KUZU_CLIENT_TIMEOUT": "not_a_number",
    }

    with patch.dict(os.environ, env_vars):
      with pytest.raises(ValueError):
        GraphClientConfig.from_env()

  def test_float_env_parsing(self):
    """Test float parsing from environment variables."""
    env_vars = {
      "KUZU_CLIENT_RETRY_DELAY": "1.5",
      "KUZU_CLIENT_RETRY_BACKOFF": "2.75",
      "KUZU_CLIENT_KEEPALIVE_EXPIRY": "7.25",
    }

    with patch.dict(os.environ, env_vars):
      config = GraphClientConfig.from_env()

      assert config.retry_delay == 1.5
      assert config.retry_backoff == 2.75
      assert config.keepalive_expiry == 7.25
