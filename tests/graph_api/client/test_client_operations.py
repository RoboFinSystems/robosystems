"""Tests for the Graph API client's operation wrappers.

`GraphClient` is the boundary every service crosses to reach customer data,
so these assert the contract each wrapper puts on the wire: method, path,
and payload. Retry/circuit-breaker behaviour lives in `test_client.py`.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.graph_api.client.client import GraphClient


@pytest.fixture(autouse=True)
def mock_env():
  with patch("robosystems.config.env") as mock:
    mock.GRAPH_API_KEY = "test-api-key"
    mock.ENVIRONMENT = "test"
    yield mock


@pytest.fixture
def client():
  return GraphClient(base_url="http://localhost:8001")


@pytest.fixture
def request_mock(client):
  """Replace `_request` and hand back the recorded calls."""
  response = MagicMock()
  response.json.return_value = {"status": "ok"}
  mock = AsyncMock(return_value=response)
  client._request = mock
  return mock


def _call(mock):
  """Normalise a recorded `_request` call to (method, path, kwargs)."""
  args, kwargs = mock.call_args
  return args[0], args[1], kwargs


@pytest.mark.unit
class TestClassifyOperation:
  @pytest.mark.parametrize(
    "path,expected",
    [
      ("/databases/kg1/query", "query"),
      ("/databases/kg1/tables", "table_ops"),
      ("/databases/kg1/tables/Fact/materialize", "materialize"),
      ("/databases/kg1/schema", "schema"),
      ("/databases/kg1/backup", "backup"),
      ("/databases/kg1/restore", "restore"),
      ("/databases/kg1/metrics", "metrics"),
      ("/health", "health"),
      ("/databases/kg1/info", "info"),
      ("/databases", "database_ops"),
      ("/databases/kg1/memory", "memory"),
      ("/databases/kg1/fork", "fork"),
      ("/databases/kg1/tasks", "task"),
      ("/databases/kg1/something-else", "other"),
    ],
  )
  def test_classifies_paths_for_metrics(self, path, expected):
    assert GraphClient._classify_operation(path) == expected

  def test_ingest_paths_are_classified_by_substring(self):
    assert GraphClient._classify_operation("/databases/kg1/ingest/s3") == "ingest"

  @pytest.mark.parametrize(
    "path", ["/databases/kg1/memory/boost", "/databases/kg1/memory/status"]
  )
  def test_memory_subpaths_currently_fall_through_to_other(self, path):
    """Characterisation, not endorsement: the memory check only inspects the
    final path segment, so `/memory/boost` lands in `other` and memory work
    is under-reported in the operation metrics. Harmless to correctness --
    this label only feeds dashboards -- but the grouping is misleading.
    """
    assert GraphClient._classify_operation(path) == "other"


@pytest.mark.unit
class TestMemoryManagement:
  @pytest.mark.asyncio
  async def test_boost_memory_defaults_to_both_targets(self, client, request_mock):
    await client.boost_memory("kg1")

    method, path, kwargs = _call(request_mock)
    assert (method, path) == ("POST", "/databases/kg1/memory/boost")
    assert kwargs["json_data"] == {"target": "both"}

  @pytest.mark.asyncio
  async def test_boost_memory_forwards_explicit_target(self, client, request_mock):
    await client.boost_memory("kg1", target="duckdb")

    assert _call(request_mock)[2]["json_data"] == {"target": "duckdb"}

  @pytest.mark.asyncio
  async def test_restore_memory(self, client, request_mock):
    await client.restore_memory("kg1")

    method, path, _ = _call(request_mock)
    assert method == "POST"
    assert "kg1" in path and "memory" in path

  @pytest.mark.asyncio
  async def test_memory_status(self, client, request_mock):
    await client.memory_status("kg1")

    method, path, _ = _call(request_mock)
    assert method == "GET"
    assert "memory" in path


@pytest.mark.unit
class TestDatabaseLifecycle:
  @pytest.mark.asyncio
  async def test_database_exists_true_when_get_succeeds(self, client):
    client.get_database = AsyncMock(return_value={"graph_id": "kg1"})

    assert await client.database_exists("kg1") is True

  @pytest.mark.asyncio
  async def test_database_exists_false_on_404(self, client):
    error = RuntimeError("not found")
    error.status_code = 404
    client.get_database = AsyncMock(side_effect=error)

    assert await client.database_exists("kg1") is False

  @pytest.mark.asyncio
  async def test_database_exists_propagates_non_404(self, client):
    """A 500 must not be reported as 'does not exist' -- callers create the
    database on False, which would be destructive against a live graph."""
    error = RuntimeError("server exploded")
    error.status_code = 500
    client.get_database = AsyncMock(side_effect=error)

    with pytest.raises(RuntimeError):
      await client.database_exists("kg1")

  @pytest.mark.asyncio
  async def test_ensure_database_exists_creates_when_missing(self, client):
    client.database_exists = AsyncMock(return_value=False)
    client.create_database = AsyncMock()

    await client.ensure_database_exists("kg1", schema_type="entity")

    client.create_database.assert_awaited_once_with("kg1", "entity")

  @pytest.mark.asyncio
  async def test_ensure_database_exists_is_a_noop_when_present(self, client):
    client.database_exists = AsyncMock(return_value=True)
    client.create_database = AsyncMock()

    await client.ensure_database_exists("kg1")

    client.create_database.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_swap_database_without_lock_token(self, client, request_mock):
    await client.swap_database("kg1")

    method, path, kwargs = _call(request_mock)
    assert (method, path) == ("POST", "/databases/kg1/swap")
    assert kwargs["headers"] is None

  @pytest.mark.asyncio
  async def test_swap_database_forwards_held_lock_token(self, client, request_mock):
    """Passing the token avoids re-acquiring a lock the caller already holds,
    which would deadlock the swap."""
    await client.swap_database("kg1", lock_token="tok-123")

    assert _call(request_mock)[2]["headers"] == {
      "X-Materialization-Lock-Token": "tok-123"
    }


@pytest.mark.unit
class TestDDLAndExistence:
  @pytest.mark.asyncio
  async def test_execute_ddl_targets_explicit_graph(self, client):
    client.query = AsyncMock(return_value={"rows": []})

    await client.execute_ddl("CREATE NODE TABLE Foo(id STRING, PRIMARY KEY(id))", "kg1")

    client.query.assert_awaited_once()
    assert client.query.await_args.args[1] == "kg1"

  @pytest.mark.asyncio
  async def test_execute_ddl_falls_back_to_client_graph(self, client):
    client.graph_id = "kg_default"
    client.query = AsyncMock(return_value={})

    await client.execute_ddl("CREATE NODE TABLE Foo(id STRING, PRIMARY KEY(id))")

    assert client.query.await_args.args[1] == "kg_default"


@pytest.mark.unit
class TestTableOperations:
  @pytest.mark.asyncio
  async def test_list_tables(self, client, request_mock):
    request_mock.return_value.json.return_value = {"tables": [{"name": "Fact"}]}

    await client.list_tables("kg1")

    method, path, _ = _call(request_mock)
    assert method == "GET"
    assert path == "/databases/kg1/tables"

  @pytest.mark.asyncio
  async def test_delete_table(self, client, request_mock):
    await client.delete_table("kg1", "Fact")

    method, path, _ = _call(request_mock)
    assert method == "DELETE"
    assert path.endswith("/tables/Fact")

  @pytest.mark.asyncio
  async def test_materialize_table_posts_to_table_path(self, client, request_mock):
    await client.materialize_table(graph_id="kg1", table_name="Fact")

    method, path, _ = _call(request_mock)
    assert method == "POST"
    assert "Fact" in path and path.endswith("materialize")

  @pytest.mark.asyncio
  async def test_query_table_sends_sql(self, client, request_mock):
    await client.query_table("kg1", "SELECT 1")

    method, path, kwargs = _call(request_mock)
    assert method == "POST"
    assert "tables" in path
    assert "SELECT 1" in str(kwargs.get("json_data"))


@pytest.mark.unit
class TestMigrationOperations:
  """These drive the engine-upgrade path; a wrong verb or path silently
  skips the migration rather than failing loudly."""

  @pytest.mark.asyncio
  async def test_migration_status_is_a_read(self, client, request_mock):
    await client.migration_status()

    method, path, _ = _call(request_mock)
    assert method == "GET"
    assert "migration" in path

  @pytest.mark.asyncio
  async def test_migration_import_is_a_write(self, client, request_mock):
    await client.migration_import()

    method, path, _ = _call(request_mock)
    assert method == "POST"
    assert "migration" in path

  @pytest.mark.asyncio
  async def test_migration_cleanup_is_a_write(self, client, request_mock):
    await client.migration_cleanup()

    method, path, _ = _call(request_mock)
    assert method == "POST"
    assert "migration" in path


@pytest.mark.unit
class TestVectorOperations:
  @pytest.mark.asyncio
  async def test_vector_search_posts_embedding(self, client, request_mock):
    request_mock.return_value.json.return_value = {"results": [], "total": 0}

    await client.vector_search("kg1", "Fact", [0.1, 0.2], limit=5)

    method, path, kwargs = _call(request_mock)
    assert method == "POST"
    assert "vector" in path
    assert kwargs["json_data"]["embedding"] == [0.1, 0.2]

  @pytest.mark.asyncio
  async def test_build_vector_index_targets_the_table(self, client, request_mock):
    await client.build_vector_index("kg1", "Fact")

    method, path, _ = _call(request_mock)
    assert method == "POST"
    assert "Fact" in path
