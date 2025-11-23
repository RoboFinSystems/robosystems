import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from robosystems.graph_api.backends.lbug import LadybugBackend
from robosystems.graph_api.backends.base import S3IngestionError


@pytest.fixture(autouse=True)
def mock_path_mkdir():
  with patch("robosystems.graph_api.backends.lbug.Path.mkdir"):
    yield


class TestLadybugBackendInitialization:
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  def test_backend_initialization(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend(data_path="/test/path")

    assert backend.data_path == Path("/test/path")
    assert backend.connection_pool == mock_pool
    mock_get_pool.assert_called_once()

  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  def test_backend_initialization_default_path(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()

    assert backend.data_path == Path("/data/lbug-dbs")
    assert backend.connection_pool == mock_pool


class TestLadybugBackendQueryExecution:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_execute_query_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()

    mock_result.get_column_names.return_value = ["id", "name"]
    mock_result.has_next.side_effect = [True, True, False]
    mock_result.get_next.side_effect = [[1, "Alice"], [2, "Bob"]]

    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    result = await backend.execute_query("test_graph", "MATCH (n) RETURN n")

    assert len(result) == 2
    assert result[0] == {"id": 1, "name": "Alice"}
    assert result[1] == {"id": 2, "name": "Bob"}
    mock_conn.execute.assert_called_once_with("MATCH (n) RETURN n")

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_execute_query_with_parameters(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()

    mock_result.get_column_names.return_value = ["id"]
    mock_result.has_next.side_effect = [True, False]
    mock_result.get_next.return_value = [1]

    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    params = {"id": 1}
    result = await backend.execute_query(
      "test_graph", "MATCH (n {id: $id}) RETURN n", params
    )

    assert len(result) == 1
    assert result[0] == {"id": 1}
    mock_conn.execute.assert_called_once_with("MATCH (n {id: $id}) RETURN n", params)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_execute_query_error(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_conn.execute.side_effect = RuntimeError("Query failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()

    with pytest.raises(RuntimeError) as exc_info:
      await backend.execute_query("test_graph", "INVALID QUERY")

    assert "Query failed" in str(exc_info.value)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_execute_write_delegates_to_query(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()

    mock_result.get_column_names.return_value = []
    mock_result.has_next.return_value = False

    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    result = await backend.execute_write("test_graph", "CREATE (n:Node)")

    assert result == []
    mock_conn.execute.assert_called_once_with("CREATE (n:Node)")


class TestLadybugBackendDatabaseManagement:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_create_database(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()

    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    result = await backend.create_database("test_db")

    assert result is True
    mock_pool.get_connection.assert_called_once_with("test_db", read_only=False)
    mock_conn.execute.assert_called_once_with("RETURN 1 as test")
    mock_result.close.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.shutil")
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_delete_database_directory(self, mock_get_pool, mock_shutil):
    mock_pool = MagicMock()
    mock_pool.force_database_cleanup.return_value = None
    mock_get_pool.return_value = mock_pool

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.is_file.return_value = False

    with (
      patch.object(Path, "exists", return_value=True),
      patch.object(Path, "is_file", return_value=False),
      patch.object(Path, "__truediv__", return_value=mock_path),
    ):
      backend = LadybugBackend()
      result = await backend.delete_database("test_db")

      assert result is True
      mock_pool.force_database_cleanup.assert_called_once_with(
        "test_db", aggressive=True
      )

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_delete_database_cleanup_failure(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_pool.force_database_cleanup.side_effect = RuntimeError("Cleanup failed")
    mock_get_pool.return_value = mock_pool

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    with patch.object(Path, "__truediv__", return_value=mock_path):
      backend = LadybugBackend()
      result = await backend.delete_database("test_db")

      assert result is True
      mock_pool.force_database_cleanup.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_list_databases(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    mock_file1 = MagicMock()
    mock_file1.stem = "db1"
    mock_file1.suffix = ".lbug"

    mock_file2 = MagicMock()
    mock_file2.stem = "db2"
    mock_file2.suffix = ".lbug"

    mock_file3 = MagicMock()
    mock_file3.stem = "other"
    mock_file3.suffix = ".txt"

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.iterdir.return_value = [mock_file1, mock_file2, mock_file3]

    with (
      patch.object(Path, "exists", return_value=True),
      patch.object(Path, "iterdir", return_value=[mock_file1, mock_file2, mock_file3]),
    ):
      backend = LadybugBackend()
      result = await backend.list_databases()

      assert result == ["db1", "db2"]

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_list_databases_empty_directory(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    with patch.object(Path, "exists", return_value=False):
      backend = LadybugBackend()
      result = await backend.list_databases()

      assert result == []


class TestLadybugBackendDatabaseInfo:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_get_database_info_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_node_result = MagicMock()
    mock_node_result.has_next.side_effect = [True, False]
    mock_node_result.get_next.return_value = [100]

    mock_rel_result = MagicMock()
    mock_rel_result.has_next.side_effect = [True, False]
    mock_rel_result.get_next.return_value = [50]

    mock_conn.execute.side_effect = [mock_node_result, mock_rel_result]
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.is_file.return_value = True
    mock_stat = MagicMock()
    mock_stat.st_size = 1024
    mock_path.stat.return_value = mock_stat

    with patch.object(Path, "__truediv__", return_value=mock_path):
      backend = LadybugBackend()
      result = await backend.get_database_info("test_db")

      assert result.name == "test_db"
      assert result.node_count == 100
      assert result.relationship_count == 50
      assert result.size_bytes == 1024

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_get_database_info_query_error(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_conn.execute.side_effect = RuntimeError("Query error")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    with patch.object(Path, "__truediv__", return_value=mock_path):
      backend = LadybugBackend()
      result = await backend.get_database_info("test_db")

      assert result.name == "test_db"
      assert result.node_count == 0
      assert result.relationship_count == 0
      assert result.size_bytes == 0


class TestLadybugBackendS3Ingestion:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_ingest_from_s3_extension_load_error(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_extensions_result = MagicMock()
    mock_extensions_result.has_next.side_effect = [True, False]
    mock_extensions_result.get_next.return_value = ["other"]

    mock_conn.execute.side_effect = [
      mock_extensions_result,
      RuntimeError("Failed to load extension"),
    ]

    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()

    with pytest.raises(S3IngestionError) as exc_info:
      await backend.ingest_from_s3(
        graph_id="test_graph",
        table_name="TestTable",
        s3_pattern="s3://bucket/data/*.parquet",
      )

    assert "httpfs extension required" in str(exc_info.value)


class TestLadybugBackendClusterTopology:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_get_cluster_topology(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    result = await backend.get_cluster_topology()

    assert result.mode == "embedded"
    assert result.leader == {"backend": "ladybug"}

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_health_check(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    result = await backend.health_check()

    assert result is True


class TestLadybugBackendClose:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.lbug.get_connection_pool")
  async def test_close(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    backend = LadybugBackend()
    await backend.close()

    mock_pool.close_all.assert_called_once()
