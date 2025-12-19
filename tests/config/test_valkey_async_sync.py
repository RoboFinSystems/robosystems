#!/usr/bin/env python3
"""
Test suite for async/sync Redis client usage.

This module tests that async operations use async Redis clients and
synchronous operations use synchronous Redis clients to prevent runtime errors.
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
  create_redis_client,
)


class TestAsyncSyncClientUsage:
  """Test that async and sync Redis clients are used appropriately."""

  def test_create_redis_client_returns_sync_client(self):
    """Test that create_redis_client returns a synchronous Redis client."""
    with patch("redis.Redis.from_url") as mock_from_url:
      mock_client = MagicMock()
      mock_client.ping.return_value = True  # Sync return
      mock_from_url.return_value = mock_client

      client = create_redis_client(ValkeyDatabase.AUTH_CACHE)

      # Should be able to call sync methods without await
      result = client.ping()
      assert result is True

      # Should NOT be a coroutine
      assert not asyncio.iscoroutine(result)

  def test_create_async_redis_client_returns_async_client(self):
    """Test that create_async_redis_client returns an async Redis client."""
    with patch("redis.asyncio.from_url") as mock_from_url:
      mock_client = AsyncMock()
      mock_client.ping.return_value = True  # Will be wrapped in coroutine by AsyncMock
      mock_from_url.return_value = mock_client

      client = create_async_redis_client(ValkeyDatabase.RATE_LIMITING)

      # The client itself is not a coroutine
      assert not asyncio.iscoroutine(client)

      # But its methods should return coroutines
      ping_result = client.ping()
      assert asyncio.iscoroutine(ping_result)

  @pytest.mark.asyncio
  async def test_async_client_operations_are_awaitable(self):
    """Test that async Redis client operations can be awaited."""
    with patch("redis.asyncio.from_url") as mock_from_url:
      mock_client = AsyncMock()
      mock_client.get.return_value = "test_value"
      mock_client.set.return_value = True
      mock_client.keys.return_value = ["key1", "key2"]
      mock_from_url.return_value = mock_client

      client = create_async_redis_client(ValkeyDatabase.LBUG_CACHE)

      # All these operations should be awaitable
      get_result = await client.get("test_key")
      assert get_result == "test_value"

      set_result = await client.set("test_key", "test_value")
      assert set_result is True

      keys_result = await client.keys("test_*")
      assert keys_result == ["key1", "key2"]

  def test_sync_client_operations_are_not_awaitable(self):
    """Test that sync Redis client operations are not coroutines."""
    with patch("redis.Redis.from_url") as mock_from_url:
      mock_client = MagicMock()
      mock_client.get.return_value = "test_value"
      mock_client.set.return_value = True
      mock_from_url.return_value = mock_client

      client = create_redis_client(ValkeyDatabase.PIPELINE_TRACKING)

      # These should NOT be coroutines
      get_result = client.get("test_key")
      assert not asyncio.iscoroutine(get_result)
      assert get_result == "test_value"

      set_result = client.set("test_key", "test_value")
      assert not asyncio.iscoroutine(set_result)
      assert set_result is True

  def test_factory_methods_production_environment(self):
    """Test that factory methods work correctly in production environment."""
    with patch.dict(
      os.environ, {"ENVIRONMENT": "prod", "VALKEY_AUTH_TOKEN": "test_token"}
    ):
      # Sync client
      with patch("redis.Redis.from_url") as mock_sync:
        mock_sync.return_value = MagicMock()
        create_redis_client(ValkeyDatabase.AUTH_CACHE)

        # Should be called with correct parameters
        assert mock_sync.called
        call_kwargs = mock_sync.call_args[1]
        assert "ssl_cert_reqs" in call_kwargs  # SSL params in kwargs

        # URL should not have SSL params
        url = mock_sync.call_args[0][0]
        assert "ssl_cert_reqs=CERT_NONE" not in url

      # Async client
      with patch("redis.asyncio.from_url") as mock_async:
        mock_async.return_value = AsyncMock()
        create_async_redis_client(ValkeyDatabase.RATE_LIMITING)

        # Should be called with correct parameters
        assert mock_async.called
        call_kwargs = mock_async.call_args[1]
        assert "ssl_cert_reqs" in call_kwargs  # SSL params in kwargs

        # URL should not have SSL params
        url = mock_async.call_args[0][0]
        assert "ssl_cert_reqs=CERT_NONE" not in url
