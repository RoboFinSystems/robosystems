"""Comprehensive unit tests for graph repository wrapper module."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.middleware.graph.repository import (
  UniversalRepository,
  get_repository_type,
  is_api_repository,
  is_direct_repository,
)


def _make_sync_repository(**props):
  """Create a mock sync (direct file) Repository."""
  mock = MagicMock()
  mock.__class__.__name__ = "Repository"
  for key, value in props.items():
    setattr(mock, key, value)
  return mock


def _make_async_repository(**props):
  """Create a mock async (API) GraphClient."""
  mock = AsyncMock()
  mock.__class__.__name__ = "GraphClient"
  for key, value in props.items():
    setattr(mock, key, value)
  return mock


@pytest.mark.unit
class TestUniversalRepositoryInit:
  """Tests for UniversalRepository initialization."""

  def test_init_with_sync_repository(self):
    """Test initialization with a sync Repository."""
    mock_repo = _make_sync_repository()
    ur = UniversalRepository(mock_repo)
    assert ur._is_async is False
    assert ur.repository_type == "direct"

  def test_init_with_async_client(self):
    """Test initialization with an async GraphClient."""
    from robosystems.graph_api.client import GraphClient

    mock_client = MagicMock(spec=GraphClient)

    ur = UniversalRepository(mock_client)
    assert ur._is_async is True
    assert ur.repository_type == "api"

  def test_is_async_property(self):
    """Test is_async property delegation."""
    mock_repo = _make_sync_repository()
    ur = UniversalRepository(mock_repo)
    assert ur.is_async is False


@pytest.mark.unit
class TestUniversalRepositoryMethodDetection:
  """Tests for sync/async method detection and caching."""

  def test_is_method_async_for_sync_method(self):
    """Test _is_method_async returns False for sync methods."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock()
    ur = UniversalRepository(mock_repo)
    assert ur._is_method_async("execute_query") is False

  def test_is_method_async_for_async_method(self):
    """Test _is_method_async returns True for async methods."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = AsyncMock()
    ur = UniversalRepository(mock_repo)
    assert ur._is_method_async("execute_query") is True

  def test_is_method_async_missing_method(self):
    """Test _is_method_async returns False for missing methods."""
    mock_repo = MagicMock(spec=[])
    ur = UniversalRepository(mock_repo)
    assert ur._is_method_async("nonexistent_method") is False

  def test_method_detection_caching(self):
    """Test that method detection results are cached."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock()
    ur = UniversalRepository(mock_repo)

    # Call twice - should use cache the second time
    result1 = ur._is_method_async("execute_query")
    result2 = ur._is_method_async("execute_query")
    assert result1 == result2
    assert "execute_query" in ur._method_cache


@pytest.mark.unit
class TestUniversalRepositoryCallMethod:
  """Tests for _call_method async dispatch."""

  @pytest.mark.asyncio
  async def test_call_method_async(self):
    """Test _call_method correctly awaits async methods."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = AsyncMock(return_value=[{"name": "test"}])
    ur = UniversalRepository(mock_repo)

    result = await ur._call_method("execute_query", "MATCH (n) RETURN n")
    assert result == [{"name": "test"}]
    mock_repo.execute_query.assert_called_once_with("MATCH (n) RETURN n")

  @pytest.mark.asyncio
  async def test_call_method_sync(self):
    """Test _call_method correctly calls sync methods."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock(return_value=[{"name": "test"}])
    ur = UniversalRepository(mock_repo)

    result = await ur._call_method("execute_query", "MATCH (n) RETURN n")
    assert result == [{"name": "test"}]

  @pytest.mark.asyncio
  async def test_call_method_raises_for_missing(self):
    """Test _call_method raises AttributeError for missing methods."""
    mock_repo = MagicMock(spec=[])
    ur = UniversalRepository(mock_repo)

    with pytest.raises(AttributeError, match="has no method"):
      await ur._call_method("nonexistent_method")


@pytest.mark.unit
class TestUniversalRepositoryCoreOperations:
  """Tests for core database operations."""

  @pytest.fixture
  def sync_ur(self):
    """Create a UniversalRepository with sync backend."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock(return_value=[{"id": 1}])
    mock_repo.execute_single = MagicMock(return_value={"id": 1})
    mock_repo.execute_transaction = MagicMock(return_value=[[{"id": 1}]])
    mock_repo.count_nodes = MagicMock(return_value=42)
    mock_repo.node_exists = MagicMock(return_value=True)
    mock_repo.health_check = MagicMock(return_value={"status": "ok"})
    mock_repo.close = MagicMock()
    return UniversalRepository(mock_repo)

  @pytest.fixture
  def async_ur(self):
    """Create a UniversalRepository with async backend."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = AsyncMock(return_value=[{"id": 1}])
    mock_repo.execute_single = AsyncMock(return_value={"id": 1})
    mock_repo.execute_transaction = AsyncMock(return_value=[[{"id": 1}]])
    mock_repo.count_nodes = AsyncMock(return_value=42)
    mock_repo.node_exists = AsyncMock(return_value=True)
    mock_repo.health_check = AsyncMock(return_value={"status": "ok"})
    mock_repo.close = AsyncMock()
    return UniversalRepository(mock_repo)

  @pytest.mark.asyncio
  async def test_execute_query(self, sync_ur):
    result = await sync_ur.execute_query("MATCH (n) RETURN n")
    assert result == [{"id": 1}]

  @pytest.mark.asyncio
  async def test_execute_single(self, sync_ur):
    result = await sync_ur.execute_single("MATCH (n) RETURN n LIMIT 1")
    assert result == {"id": 1}

  @pytest.mark.asyncio
  async def test_execute_transaction(self, sync_ur):
    result = await sync_ur.execute_transaction([])
    assert result == [[{"id": 1}]]

  @pytest.mark.asyncio
  async def test_count_nodes(self, sync_ur):
    result = await sync_ur.count_nodes("Entity")
    assert result == 42

  @pytest.mark.asyncio
  async def test_node_exists(self, sync_ur):
    result = await sync_ur.node_exists("Entity", {"name": "test"})
    assert result is True

  @pytest.mark.asyncio
  async def test_node_exists_raises_on_error(self):
    """Test node_exists re-raises exceptions."""
    mock_repo = _make_sync_repository()
    mock_repo.node_exists = MagicMock(side_effect=RuntimeError("DB error"))
    ur = UniversalRepository(mock_repo)

    with pytest.raises(RuntimeError, match="DB error"):
      await ur.node_exists("Entity", {"name": "test"})

  @pytest.mark.asyncio
  async def test_health_check(self, sync_ur):
    result = await sync_ur.health_check()
    assert result == {"status": "ok"}

  @pytest.mark.asyncio
  async def test_close(self, sync_ur):
    await sync_ur.close()

  @pytest.mark.asyncio
  async def test_execute_query_async_backend(self, async_ur):
    result = await async_ur.execute_query("MATCH (n) RETURN n")
    assert result == [{"id": 1}]

  @pytest.mark.asyncio
  async def test_count_nodes_async_backend(self, async_ur):
    result = await async_ur.count_nodes("Entity")
    assert result == 42


@pytest.mark.unit
class TestUniversalRepositoryStreaming:
  """Tests for execute_query_streaming fallback chunking."""

  @pytest.mark.asyncio
  async def test_streaming_with_native_support(self):
    """Test streaming uses native method when available."""
    mock_repo = _make_sync_repository()
    chunks = [
      {"chunk_index": 0, "data": [{"id": 1}], "is_last_chunk": True},
    ]

    async def mock_streaming(*args, **kwargs):
      for chunk in chunks:
        yield chunk

    mock_repo.execute_query_streaming = mock_streaming
    ur = UniversalRepository(mock_repo)

    collected = []
    async for chunk in ur.execute_query_streaming("MATCH (n) RETURN n"):
      collected.append(chunk)

    assert len(collected) == 1
    assert collected[0]["is_last_chunk"] is True

  @pytest.mark.asyncio
  async def test_streaming_fallback_chunking(self):
    """Test streaming falls back to chunking when native not available."""
    mock_repo = _make_sync_repository()
    # No execute_query_streaming method
    if hasattr(mock_repo, "execute_query_streaming"):
      delattr(mock_repo, "execute_query_streaming")
    mock_repo.execute_query = MagicMock(return_value=[{"id": i} for i in range(5)])
    # Remove execute_query_streaming from the mock
    mock_repo_no_streaming = MagicMock(spec=["execute_query"])
    mock_repo_no_streaming.execute_query = MagicMock(
      return_value=[{"id": i} for i in range(5)]
    )

    ur = UniversalRepository(mock_repo_no_streaming)

    collected = []
    async for chunk in ur.execute_query_streaming("MATCH (n) RETURN n", chunk_size=2):
      collected.append(chunk)

    # 5 results with chunk_size=2 -> 3 chunks (2, 2, 1)
    assert len(collected) == 3
    assert collected[0]["chunk_index"] == 0
    assert collected[0]["row_count"] == 2
    assert collected[0]["is_last_chunk"] is False
    assert collected[2]["is_last_chunk"] is True
    assert collected[2]["row_count"] == 1


@pytest.mark.unit
class TestUniversalRepositorySyncMethods:
  """Tests for synchronous backward-compatibility methods."""

  def test_execute_query_sync(self):
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock(return_value=[{"id": 1}])
    ur = UniversalRepository(mock_repo)

    result = ur.execute_query_sync("MATCH (n) RETURN n")
    assert result == [{"id": 1}]

  def test_execute_single_sync(self):
    mock_repo = _make_sync_repository()
    mock_repo.execute_single = MagicMock(return_value={"id": 1})
    ur = UniversalRepository(mock_repo)

    result = ur.execute_single_sync("MATCH (n) RETURN n LIMIT 1")
    assert result == {"id": 1}

  def test_execute_alias(self):
    """Test that execute() is an alias for execute_query_sync()."""
    mock_repo = _make_sync_repository()
    mock_repo.execute_query = MagicMock(return_value=[{"id": 1}])
    ur = UniversalRepository(mock_repo)

    result = ur.execute("MATCH (n) RETURN n")
    assert result == [{"id": 1}]


@pytest.mark.unit
class TestUniversalRepositoryContextManagers:
  """Tests for context manager implementations."""

  @pytest.mark.asyncio
  async def test_async_context_manager_with_aenter(self):
    """Test async context manager delegates to repository __aenter__."""
    mock_repo = _make_sync_repository()
    mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
    mock_repo.__aexit__ = AsyncMock()

    ur = UniversalRepository(mock_repo)
    async with ur as ctx:
      assert ctx is ur

    mock_repo.__aenter__.assert_called_once()
    mock_repo.__aexit__.assert_called_once()

  @pytest.mark.asyncio
  async def test_async_context_manager_fallback_to_close(self):
    """Test async exit falls back to close() if no __aexit__."""
    mock_repo = MagicMock(spec=["close"])
    mock_repo.close = AsyncMock()

    ur = UniversalRepository(mock_repo)
    async with ur:
      pass

    mock_repo.close.assert_called_once()

  def test_sync_context_manager_with_enter(self):
    """Test sync context manager delegates to repository __enter__."""
    mock_repo = _make_sync_repository()
    mock_repo.__enter__ = MagicMock(return_value=mock_repo)
    mock_repo.__exit__ = MagicMock()

    ur = UniversalRepository(mock_repo)
    with ur as ctx:
      assert ctx is ur

    mock_repo.__enter__.assert_called_once()
    mock_repo.__exit__.assert_called_once()

  def test_sync_context_manager_fallback_to_close(self):
    """Test sync exit falls back to close_sync() if no __exit__."""
    mock_repo = MagicMock(spec=["close"])
    mock_repo.close = MagicMock()

    ur = UniversalRepository(mock_repo)
    with ur:
      pass

    mock_repo.close.assert_called_once()


@pytest.mark.unit
class TestUniversalRepositoryProperties:
  """Tests for property delegation."""

  def test_database_name_from_database_name(self):
    """Test database_name property from database_name attr."""
    mock_repo = _make_sync_repository(database_name="test_db")
    ur = UniversalRepository(mock_repo)
    assert ur.database_name == "test_db"

  def test_database_name_from_database_path(self):
    """Test database_name fallback to database_path."""
    mock_repo = MagicMock(spec=["database_path"])
    mock_repo.database_path = "/path/to/db"
    ur = UniversalRepository(mock_repo)
    assert ur.database_name == "/path/to/db"

  def test_database_name_unknown(self):
    """Test database_name returns 'unknown' when no attr exists."""
    mock_repo = MagicMock(spec=[])
    ur = UniversalRepository(mock_repo)
    assert ur.database_name == "unknown"

  def test_read_only_true(self):
    """Test read_only property when set."""
    mock_repo = _make_sync_repository(read_only=True)
    ur = UniversalRepository(mock_repo)
    assert ur.read_only is True

  def test_read_only_default_false(self):
    """Test read_only defaults to False."""
    mock_repo = MagicMock(spec=[])
    ur = UniversalRepository(mock_repo)
    assert ur.read_only is False

  def test_get_underlying_repository(self):
    """Test get_underlying_repository returns the wrapped repo."""
    mock_repo = _make_sync_repository()
    ur = UniversalRepository(mock_repo)
    assert ur.get_underlying_repository() is mock_repo

  def test_repr(self):
    """Test __repr__ format."""
    mock_repo = _make_sync_repository()
    mock_repo.database_name = "test_db"
    ur = UniversalRepository(mock_repo)
    repr_str = repr(ur)
    assert "Repository" in repr_str
    assert "test_db" in repr_str


@pytest.mark.unit
class TestUniversalRepositorySchema:
  """Tests for API-only schema method."""

  @pytest.mark.asyncio
  async def test_get_schema_with_api_repo(self):
    """Test get_schema when repository supports it."""
    mock_repo = _make_sync_repository()
    mock_repo.get_schema = AsyncMock(return_value=[{"label": "Entity", "type": "node"}])
    ur = UniversalRepository(mock_repo)

    result = await ur.get_schema()
    assert result == [{"label": "Entity", "type": "node"}]

  @pytest.mark.asyncio
  async def test_get_schema_raises_for_direct_repo(self):
    """Test get_schema raises NotImplementedError for direct repos."""
    mock_repo = MagicMock(spec=[])
    ur = UniversalRepository(mock_repo)

    with pytest.raises(NotImplementedError, match="only available for API"):
      await ur.get_schema()


@pytest.mark.unit
class TestRepositoryTypeDetection:
  """Tests for utility functions is_api_repository, is_direct_repository, get_repository_type."""

  def test_is_api_repository_with_graph_client(self):
    """Test is_api_repository for GraphClient."""
    from robosystems.graph_api.client import GraphClient

    mock_client = MagicMock(spec=GraphClient)
    assert is_api_repository(mock_client) is True

  def test_is_api_repository_with_universal_async(self):
    """Test is_api_repository with async UniversalRepository."""
    from robosystems.graph_api.client import GraphClient

    mock_client = MagicMock(spec=GraphClient)
    ur = UniversalRepository(mock_client)
    assert is_api_repository(ur) is True

  def test_is_api_repository_with_sync_repo(self):
    """Test is_api_repository returns False for sync repos."""
    mock_repo = _make_sync_repository()
    assert is_api_repository(mock_repo) is False

  def test_is_direct_repository_with_repository(self):
    """Test is_direct_repository for Repository instances."""
    from robosystems.graph_api.core.ladybug import Repository

    mock_repo = MagicMock(spec=Repository)
    assert is_direct_repository(mock_repo) is True

  def test_is_direct_repository_with_universal_sync(self):
    """Test is_direct_repository with sync UniversalRepository."""
    mock_repo = _make_sync_repository()
    ur = UniversalRepository(mock_repo)
    assert is_direct_repository(ur) is True

  def test_get_repository_type_api(self):
    """Test get_repository_type for API repositories."""
    from robosystems.graph_api.client import GraphClient

    mock_client = MagicMock(spec=GraphClient)
    assert get_repository_type(mock_client) == "api"

  def test_get_repository_type_direct(self):
    """Test get_repository_type for direct repositories."""
    from robosystems.graph_api.core.ladybug import Repository

    mock_repo = MagicMock(spec=Repository)
    assert get_repository_type(mock_repo) == "direct"

  def test_get_repository_type_universal(self):
    """Test get_repository_type for UniversalRepository."""
    mock_repo = _make_sync_repository()
    ur = UniversalRepository(mock_repo)
    assert get_repository_type(ur) == "direct"

  def test_get_repository_type_unknown(self):
    """Test get_repository_type for unknown types."""
    assert get_repository_type("not_a_repo") == "unknown"
