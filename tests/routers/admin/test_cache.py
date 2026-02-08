"""Tests for admin cache endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request

from robosystems.config.valkey_registry import ValkeyDatabase


def mock_require_admin(permissions=None):
  """Mock decorator that bypasses admin authentication."""

  def decorator(func):
    return func

  return decorator


@pytest.fixture(autouse=True)
def patch_admin_auth(monkeypatch):
  """Automatically patch admin authentication for all tests in this module."""

  async def mock_admin_auth(self, request, credentials=None):
    request.state.admin = {
      "key_id": "test_key",
      "permissions": ["cache:read", "cache:write"],
    }
    request.state.admin_key_id = "test_key"

  monkeypatch.setattr(
    "robosystems.middleware.auth.admin.AdminAuthMiddleware.__call__",
    mock_admin_auth,
  )


class TestCacheInfo:
  """Tests for GET /admin/v1/cache/info."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_info_returns_overview(self, mock_request):
    """Test that cache info returns overview with all databases."""
    from robosystems.routers.admin.cache import cache_info

    mock_client = MagicMock()
    mock_client.dbsize.return_value = 5
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_info(mock_request)

    assert len(result.databases) == len(list(ValkeyDatabase))
    assert result.total_keys == 5 * len(list(ValkeyDatabase))
    for db_info in result.databases:
      assert db_info.key_count == 5


class TestCacheDatabaseInfo:
  """Tests for GET /admin/v1/cache/info/{database}."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_database_info_valid(self, mock_request):
    """Test getting info for a valid database name."""
    from robosystems.routers.admin.cache import cache_database_info

    mock_client = MagicMock()
    mock_client.dbsize.return_value = 10
    mock_client.scan_iter.return_value = iter(["key1", "key2", "key3"])
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_database_info(mock_request, "auth")

    assert result.name == "auth"
    assert result.db_number == 0
    assert result.key_count == 10
    assert len(result.sample_keys) == 3

  @pytest.mark.asyncio
  async def test_cache_database_info_invalid_name(self, mock_request):
    """Test getting info for an invalid database name returns 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.cache import cache_database_info

    with pytest.raises(HTTPException) as exc_info:
      await cache_database_info(mock_request, "invalid-name")

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_cache_database_info_kebab_case(self, mock_request):
    """Test that kebab-case names are resolved correctly."""
    from robosystems.routers.admin.cache import cache_database_info

    mock_client = MagicMock()
    mock_client.dbsize.return_value = 3
    mock_client.scan_iter.return_value = iter([])
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_database_info(mock_request, "rate-limits")

    assert result.name == "rate-limits"
    assert result.db_number == 1


class TestCacheFlush:
  """Tests for POST /admin/v1/cache/flush/{database}."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_flush_single(self, mock_request):
    """Test flushing a single database."""
    from robosystems.routers.admin.cache import cache_flush

    mock_client = MagicMock()
    mock_client.dbsize.return_value = 15
    mock_client.flushdb.return_value = True
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_flush(mock_request, "auth")

    assert result.name == "auth"
    assert result.keys_before == 15
    assert result.flushed is True
    mock_client.flushdb.assert_called_once()


class TestCacheFlushAll:
  """Tests for POST /admin/v1/cache/flush-all."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_flush_all(self, mock_request):
    """Test flushing all databases."""
    from robosystems.routers.admin.cache import cache_flush_all

    mock_client = MagicMock()
    mock_client.dbsize.return_value = 5
    mock_client.flushdb.return_value = True
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_flush_all(mock_request)

    db_count = len(list(ValkeyDatabase))
    assert len(result.databases) == db_count
    assert result.total_keys_flushed == 5 * db_count


class TestCacheKeys:
  """Tests for GET /admin/v1/cache/keys/{database}."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_keys_with_pattern(self, mock_request):
    """Test listing keys with a pattern."""
    from robosystems.routers.admin.cache import cache_keys

    mock_client = MagicMock()
    mock_client.scan_iter.return_value = iter(["apikey:abc", "apikey:def"])
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_keys(mock_request, "auth", pattern="apikey:*", count=100)

    assert result.name == "auth"
    assert result.pattern == "apikey:*"
    assert len(result.keys) == 2
    assert result.count == 2


class TestCacheDeleteKeys:
  """Tests for DELETE /admin/v1/cache/keys/{database}."""

  @pytest.fixture
  def mock_request(self):
    request = MagicMock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.mark.asyncio
  async def test_cache_delete_keys(self, mock_request):
    """Test deleting keys matching a pattern."""
    from robosystems.routers.admin.cache import cache_delete_keys

    mock_client = MagicMock()
    mock_client.scan_iter.return_value = iter(["apikey:abc", "apikey:def"])
    mock_client.delete.return_value = 2
    mock_client.close.return_value = None

    with patch(
      "robosystems.routers.admin.cache.create_redis_client",
      return_value=mock_client,
    ):
      result = await cache_delete_keys(mock_request, "auth", pattern="apikey:*")

    assert result.name == "auth"
    assert result.pattern == "apikey:*"
    assert result.keys_deleted == 2

  @pytest.mark.asyncio
  async def test_cache_delete_keys_wildcard_rejected(self, mock_request):
    """Test that wildcard '*' pattern is rejected."""
    from fastapi import HTTPException

    from robosystems.routers.admin.cache import cache_delete_keys

    with pytest.raises(HTTPException) as exc_info:
      await cache_delete_keys(mock_request, "auth", pattern="*")

    assert exc_info.value.status_code == 400


class TestResolveDatabase:
  """Tests for the _resolve_database helper."""

  def test_resolve_all_databases(self):
    """Test resolving all valid database names."""
    from robosystems.routers.admin.cache import _resolve_database

    expected = {
      "auth": ValkeyDatabase.AUTH,
      "rate-limits": ValkeyDatabase.RATE_LIMITS,
      "credits": ValkeyDatabase.CREDITS,
      "billing": ValkeyDatabase.BILLING,
      "sse": ValkeyDatabase.SSE,
      "locks": ValkeyDatabase.LOCKS,
      "graph-routing": ValkeyDatabase.GRAPH_ROUTING,
      "task-state": ValkeyDatabase.TASK_STATE,
    }

    for name, expected_db in expected.items():
      result = _resolve_database(name)
      assert result == expected_db, f"Failed for {name}"

  def test_resolve_invalid_name(self):
    """Test that invalid names raise HTTPException."""
    from fastapi import HTTPException

    from robosystems.routers.admin.cache import _resolve_database

    with pytest.raises(HTTPException) as exc_info:
      _resolve_database("nonexistent")

    assert exc_info.value.status_code == 404
