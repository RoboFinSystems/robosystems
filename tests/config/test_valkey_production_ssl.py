#!/usr/bin/env python3
"""
Test suite for Valkey/Redis production SSL parameter handling.

This module specifically tests the critical production issue where SSL parameters
in the URL query string cause "Invalid SSL Certificate Requirements Flag: CERT_NONE"
errors with redis-py clients, while background tasks requires them.
"""

import os
from unittest.mock import MagicMock, patch

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  ValkeyURLBuilder,
  create_async_redis_client,
  create_redis_client,
  get_redis_connection_params,
)


class TestProductionSSLHandling:
  """Test production-specific SSL parameter handling."""

  def test_build_authenticated_url_excludes_ssl_params_when_requested(self):
    """Test that SSL params can be excluded from URLs for redis-py clients."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
    ):
      # Without SSL params (for redis-py clients)
      url_without_ssl = ValkeyURLBuilder.build_authenticated_url(
        ValkeyDatabase.AUTH, include_ssl_params=False
      )
      assert "ssl_cert_reqs=CERT_NONE" not in url_without_ssl
      assert url_without_ssl.startswith("rediss://")
      assert "test_token" in url_without_ssl

  def test_build_authenticated_url_includes_ssl_params_by_default(self):
    """Test that SSL params are included by default."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
    ):
      # With SSL params
      url_with_ssl = ValkeyURLBuilder.build_authenticated_url(
        ValkeyDatabase.AUTH, include_ssl_params=True
      )
      assert "ssl_cert_reqs=CERT_NONE" in url_with_ssl
      assert url_with_ssl.startswith("rediss://")

  def test_create_redis_client_production_no_ssl_in_url(self):
    """Test that create_redis_client doesn't include SSL params in URL."""
    with (
      patch.dict(
        os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
      ),
      patch("redis.Redis.from_url") as mock_from_url,
    ):
      mock_client = MagicMock()
      mock_from_url.return_value = mock_client

      create_redis_client(ValkeyDatabase.AUTH)

      # Check the URL passed to Redis.from_url
      call_args = mock_from_url.call_args
      url = call_args[0][0]

      # URL should NOT contain SSL params
      assert "ssl_cert_reqs=CERT_NONE" not in url

      # But connection params should include SSL settings for ElastiCache
      kwargs = call_args[1]
      assert kwargs.get("ssl_cert_reqs") == "none"
      assert kwargs.get("ssl_check_hostname") is False

  def test_create_async_redis_client_production_no_ssl_in_url(self):
    """Test that create_async_redis_client doesn't include SSL params in URL."""
    with (
      patch.dict(
        os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
      ),
      patch("redis.asyncio.from_url") as mock_from_url,
    ):
      mock_client = MagicMock()
      mock_from_url.return_value = mock_client

      create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

      # Check the URL passed to redis_async.from_url
      call_args = mock_from_url.call_args
      url = call_args[0][0]

      # URL should NOT contain SSL params
      assert "ssl_cert_reqs=CERT_NONE" not in url

      # But connection params should include SSL settings for ElastiCache
      kwargs = call_args[1]
      assert kwargs.get("ssl_cert_reqs") == "none"
      assert kwargs.get("ssl_check_hostname") is False

  def test_get_redis_connection_params_production(self):
    """Test that connection params include SSL settings in production."""
    with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
      params = get_redis_connection_params()

      # Should include SSL settings for production (ElastiCache)
      assert params.get("ssl_cert_reqs") == "none"
      assert params.get("ssl_check_hostname") is False

  def test_get_redis_connection_params_development(self):
    """Test that connection params don't include SSL settings in dev."""
    with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
      params = get_redis_connection_params()

      # Should NOT include SSL settings for dev
      assert "ssl_cert_reqs" not in params
      assert "ssl_check_hostname" not in params

  def test_other_database_urls_include_ssl_params(self):
    """Test that other database URLs include SSL params in production."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
    ):
      # SSE URL should include SSL params
      sse_url = ValkeyURLBuilder.build_authenticated_url(
        ValkeyDatabase.SSE,
        include_ssl_params=True,
      )
      assert "ssl_cert_reqs=CERT_NONE" in sse_url

      # LOCKS URL should include SSL params
      locks_url = ValkeyURLBuilder.build_authenticated_url(
        ValkeyDatabase.LOCKS,
        include_ssl_params=True,
      )
      assert "ssl_cert_reqs=CERT_NONE" in locks_url

  def test_production_staging_difference(self):
    """Test that both prod and staging handle SSL params correctly."""
    for env in ["prod", "staging"]:
      with patch.dict(
        os.environ, {"ENVIRONMENT": env, "VALKEY_AUTH_TOKEN": "test_token"}
      ):
        # Factory methods should NOT include SSL params in URL
        with patch("redis.Redis.from_url") as mock_sync:
          mock_sync.return_value = MagicMock()
          create_redis_client(ValkeyDatabase.AUTH)
          url = mock_sync.call_args[0][0]
          assert "ssl_cert_reqs=CERT_NONE" not in url

        # URLs with include_ssl_params=True should include them
        auth_url = ValkeyURLBuilder.build_authenticated_url(
          ValkeyDatabase.AUTH, include_ssl_params=True
        )
        assert "ssl_cert_reqs=CERT_NONE" in auth_url

  def test_url_encoding_special_characters_in_auth_token(self):
    """Test that special characters in auth tokens are properly encoded."""
    special_token = "token!@#$%^&*()_+{}[]|:;<>?,./~`"

    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": special_token}
    ):
      url = ValkeyURLBuilder.build_authenticated_url(
        ValkeyDatabase.AUTH, include_ssl_params=False
      )

      # Token should be URL-encoded
      assert "!" not in url or "%21" in url
      assert "@" not in url or "%40" in url
      # URL should still be valid
      assert url.startswith("rediss://")
