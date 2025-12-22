#!/usr/bin/env python3
"""
Test suite for Valkey security features.

This module tests the enhanced Valkey authentication and URL building capabilities
introduced as part of the security hardening initiative.
"""

import os
from unittest.mock import patch

import pytest

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  ValkeyURLBuilder,
)


class TestValkeyURLBuilder:
  """Test cases for ValkeyURLBuilder with authentication support."""

  def test_build_url_basic(self):
    """Test basic URL building without authentication."""
    url = ValkeyURLBuilder.build_url(
      base_url="redis://localhost:6379", database=ValkeyDatabase.AUTH_CACHE
    )
    assert url == "redis://localhost:6379/2"

  def test_build_url_with_auth_token(self):
    """Test URL building with authentication token in dev/test environment."""
    # In dev/test environment, auth token doesn't automatically enable TLS
    with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
      url = ValkeyURLBuilder.build_url(
        base_url="redis://localhost:6379",
        database=ValkeyDatabase.AUTH_CACHE,
        auth_token="test_token_123",
      )
      assert url == "redis://default:test_token_123@localhost:6379/2"

  def test_build_url_with_auth_token_no_tls(self):
    """Test URL building with auth token but explicit no TLS."""
    url = ValkeyURLBuilder.build_url(
      base_url="redis://localhost:6379",
      database=ValkeyDatabase.AUTH_CACHE,
      auth_token="test_token_123",
      use_tls=False,
    )
    assert url == "redis://default:test_token_123@localhost:6379/2"

  def test_build_url_with_auth_token_prod(self):
    """Test URL building with authentication token in production environment."""
    # In production environment, auth token automatically enables TLS with SSL params
    with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
      url = ValkeyURLBuilder.build_url(
        base_url="redis://localhost:6379",
        database=ValkeyDatabase.AUTH_CACHE,
        auth_token="test_token_123",
      )
      assert (
        url
        == "rediss://default:test_token_123@localhost:6379/2?ssl_cert_reqs=CERT_NONE"
      )

  def test_build_url_with_auth_token_staging(self):
    """Test URL building with authentication token in staging environment."""
    # In staging environment, auth token automatically enables TLS with SSL params
    with patch.dict(os.environ, {"ENVIRONMENT": "staging"}):
      url = ValkeyURLBuilder.build_url(
        base_url="redis://localhost:6379",
        database=ValkeyDatabase.AUTH_CACHE,
        auth_token="test_token_123",
      )
      assert (
        url
        == "rediss://default:test_token_123@localhost:6379/2?ssl_cert_reqs=CERT_NONE"
      )

  def test_build_url_with_existing_auth_in_url(self):
    """Test URL building when base URL already contains auth."""
    # In dev/test environment, auth token doesn't automatically enable TLS
    with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
      url = ValkeyURLBuilder.build_url(
        base_url="redis://old_user:old_pass@localhost:6379",
        database=ValkeyDatabase.AUTH_CACHE,
        auth_token="new_token",
      )
      assert url == "redis://default:new_token@localhost:6379/2"

  def test_build_url_with_database_in_base_url(self):
    """Test URL building when base URL already contains database number."""
    url = ValkeyURLBuilder.build_url(
      base_url="redis://localhost:6379/5", database=ValkeyDatabase.AUTH_CACHE
    )
    assert url == "redis://localhost:6379/2"

  def test_build_url_valkey_prefix(self):
    """Test URL building with valkey:// prefix."""
    url = ValkeyURLBuilder.build_url(
      base_url="redis://localhost:6379",
      database=ValkeyDatabase.AUTH_CACHE,
      use_valkey_prefix=True,
    )
    assert url == "valkey://localhost:6379/2"

  def test_build_url_no_protocol(self):
    """Test URL building when base URL has no protocol."""
    # In dev/test environment, auth token doesn't automatically enable TLS
    with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
      url = ValkeyURLBuilder.build_url(
        base_url="localhost:6379",
        database=ValkeyDatabase.AUTH_CACHE,
        auth_token="test_token",
      )
      assert url == "redis://default:test_token@localhost:6379/2"

  def test_build_authenticated_url_with_token(self):
    """Test auto-authenticated URL building when token is available."""
    # In dev/test environment, auth token doesn't automatically enable TLS
    with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
      with patch.object(ValkeyURLBuilder, "get_auth_token", return_value="auto_token"):
        url = ValkeyURLBuilder.build_authenticated_url(
          database=ValkeyDatabase.AUTH_CACHE, base_url="redis://localhost:6379"
        )
        assert url == "redis://default:auto_token@localhost:6379/2"

  def test_build_authenticated_url_no_token(self):
    """Test auto-authenticated URL building when no token is available."""
    with patch.object(ValkeyURLBuilder, "get_auth_token", return_value=None):
      url = ValkeyURLBuilder.build_authenticated_url(
        database=ValkeyDatabase.AUTH_CACHE, base_url="redis://localhost:6379"
      )
      assert url == "redis://localhost:6379/2"

  def test_parse_url_with_auth(self):
    """Test URL parsing with authentication."""
    base_url, db_num = ValkeyURLBuilder.parse_url("rediss://default:token@host:6379/5")
    assert base_url == "rediss://default:token@host:6379"
    assert db_num == 5

  def test_parse_url_without_auth(self):
    """Test URL parsing without authentication."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://host:6379/3")
    assert base_url == "redis://host:6379"
    assert db_num == 3

  def test_parse_url_no_database(self):
    """Test URL parsing without database number."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://host:6379")
    assert base_url == "redis://host:6379"
    assert db_num is None


class TestValkeyAuthToken:
  """Test cases for Valkey auth token retrieval."""

  def setUp(self):
    """Clear cached values before each test."""
    ValkeyURLBuilder._cached_auth_token = None
    ValkeyURLBuilder._auth_token_environment = None

  def test_get_auth_token_from_env_var(self):
    """Test getting auth token from environment variable."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "dev", "VALKEY_AUTH_TOKEN": "env_token_123"}
    ):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache
      token = ValkeyURLBuilder.get_auth_token()
      assert token == "env_token_123"

  def test_get_auth_token_no_token(self):
    """Test getting auth token when none is configured."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "dev", "VALKEY_AUTH_TOKEN": ""}, clear=True
    ):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache
      token = ValkeyURLBuilder.get_auth_token()
      assert token is None

  @patch("robosystems.config.secrets_manager.get_secret_value")
  def test_get_auth_token_from_secrets_manager_dict(self, mock_get_secret):
    """Test getting auth token from Secrets Manager (dict format)."""
    mock_get_secret.return_value = "secrets_token_456"

    with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache
      token = ValkeyURLBuilder.get_auth_token()
      assert token == "secrets_token_456"
      mock_get_secret.assert_called_once_with("VALKEY_AUTH_TOKEN", "")

  @patch("robosystems.config.secrets_manager.get_secret_value")
  def test_get_auth_token_from_secrets_manager_string(self, mock_get_secret):
    """Test getting auth token from Secrets Manager (string format)."""
    mock_get_secret.return_value = "secrets_token_789"

    with patch.dict(
      os.environ,
      {"ENVIRONMENT": "staging"},
    ):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache
      token = ValkeyURLBuilder.get_auth_token()
      assert token == "secrets_token_789"

  def test_get_auth_token_caching(self):
    """Test that auth token is cached properly."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "dev", "VALKEY_AUTH_TOKEN": "cached_token"}
    ):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache

      # First call should fetch from env
      token1 = ValkeyURLBuilder.get_auth_token()
      assert token1 == "cached_token"

      # Second call should use cache (change env to verify)
      with patch.dict(os.environ, {"VALKEY_AUTH_TOKEN": "different_token"}):
        token2 = ValkeyURLBuilder.get_auth_token()
        assert token2 == "cached_token"  # Should still be cached value

  def test_get_auth_token_cache_invalidation(self):
    """Test that cache is invalidated when environment changes."""
    # Set up cache for dev environment
    with patch.dict(
      os.environ, {"ENVIRONMENT": "dev", "VALKEY_AUTH_TOKEN": "dev_token"}
    ):
      ValkeyURLBuilder._cached_auth_token = None  # Clear cache
      token1 = ValkeyURLBuilder.get_auth_token()
      assert token1 == "dev_token"

    # Change to prod environment - should invalidate cache
    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "prod_token"}
    ):
      token2 = ValkeyURLBuilder.get_auth_token()
      assert token2 == "prod_token"


class TestValkeyDatabaseRegistry:
  """Test cases for Valkey database registry."""

  def test_all_databases_have_unique_numbers(self):
    """Test that all database numbers are unique."""
    used_numbers = [db.value for db in ValkeyDatabase]
    assert len(used_numbers) == len(set(used_numbers)), (
      "Duplicate database numbers found"
    )

  def test_get_next_available(self):
    """Test getting next available database number."""
    next_available = ValkeyDatabase.get_next_available()
    used_numbers = {db.value for db in ValkeyDatabase}
    assert next_available not in used_numbers
    assert 0 <= next_available <= 15

  def test_database_purposes(self):
    """Test that all databases have purpose descriptions."""
    from robosystems.config.valkey_registry import get_database_purpose

    for db in ValkeyDatabase:
      purpose = get_database_purpose(db)
      assert purpose is not None
      assert len(purpose) > 10  # Should have meaningful description

  def test_get_url_convenience_method(self):
    """Test the convenience get_url method."""
    url = ValkeyDatabase.get_url(ValkeyDatabase.AUTH_CACHE, "redis://test:6379")
    assert url == "redis://test:6379/2"


if __name__ == "__main__":
  # Run tests if executed directly
  pytest.main([__file__, "-v"])
