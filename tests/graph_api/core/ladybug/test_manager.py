"""Comprehensive unit tests for LadybugDatabaseManager - targeting missed coverage lines."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.ladybug.manager import (
  LadybugDatabaseManager,
  validate_database_path,
)
from robosystems.graph_api.models.database import DatabaseCreateRequest


def _make_manager(temp_dir, max_databases=50, read_only=False):
  """Helper to create a LadybugDatabaseManager with mocked pool."""
  with patch(
    "robosystems.graph_api.core.ladybug.manager.initialize_connection_pool"
  ) as mock_init_pool:
    mock_pool = MagicMock()
    mock_init_pool.return_value = mock_pool
    manager = LadybugDatabaseManager(
      base_path=str(temp_dir),
      max_databases=max_databases,
      max_connections_per_db=3,
      read_only=read_only,
    )
  return manager


@pytest.mark.unit
class TestValidateDatabasePath:
  """Tests for validate_database_path security function."""

  def test_valid_names(self):
    """Should accept valid database names."""
    base = Path("/tmp/test")
    for name in ["abc", "db_01", "my-db", "A1b2C3"]:
      result = validate_database_path(base, name)
      assert result == base / f"{name}.lbug"

  def test_empty_name_raises(self):
    """Should reject empty database name."""
    with pytest.raises(HTTPException) as exc_info:
      validate_database_path(Path("/tmp"), "")
    assert exc_info.value.status_code == 400

  def test_special_chars_raise(self):
    """Should reject names with special characters."""
    for name in ["db/../etc", "db;DROP", "db name", "db/path", "db.lbug"]:
      with pytest.raises(HTTPException):
        validate_database_path(Path("/tmp"), name)

  def test_none_name_raises(self):
    """Should reject None as database name."""
    with pytest.raises(HTTPException):
      validate_database_path(Path("/tmp"), None)


@pytest.mark.unit
class TestLadybugDatabaseManagerCreateDatabase:
  """Tests for create_database lifecycle."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_create_database_empty_graph_id_raises(self):
    """Should raise ValidationError for empty graph_id (Pydantic pattern check)."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      DatabaseCreateRequest(graph_id="", schema_type="entity")

  def test_create_database_capacity_exceeded(self):
    """Should raise 507 when max_databases reached."""
    manager = _make_manager(self.temp_dir, max_databases=1)

    # Create one database to fill capacity
    (Path(self.temp_dir) / "existing.lbug").touch()

    # No "_": a top-level graph id, which is what the cap counts.
    request = DatabaseCreateRequest(graph_id="newdb", schema_type="entity")

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)
    assert exc_info.value.status_code == 507

  def test_capacity_exemption_ignores_the_request_flag(self):
    """The exemption derives from the graph id, matching how the cap counts."""
    manager = _make_manager(self.temp_dir, max_databases=1)
    (Path(self.temp_dir) / "existing.lbug").touch()

    request = DatabaseCreateRequest(
      graph_id="newdb", schema_type="entity", is_subgraph=True
    )

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)
    assert exc_info.value.status_code == 507

  def test_blue_green_wip_is_exempt_on_a_full_node(self):
    """The swap builds a WIP beside a live primary on a one-slot instance.

    ``max_databases`` is 1 on every tier, so the live graph already fills the
    node. If the WIP were charged a slot the swap could never start, and
    blue-green materialization would fail for every graph.
    """
    manager = _make_manager(self.temp_dir, max_databases=1)
    (Path(self.temp_dir) / "kg1234567890abcdef.lbug").touch()

    request = DatabaseCreateRequest(
      graph_id="kg1234567890abcdef-wip",
      schema_type="entity",
      is_subgraph=True,
    )

    with (
      patch("robosystems.graph_api.core.ladybug.manager.lbug.Database"),
      patch(
        "robosystems.graph_api.core.ladybug.manager.lbug.Connection"
      ) as mock_conn_cls,
      patch.object(manager, "_apply_schema", return_value=True),
      patch("robosystems.graph_api.core.ladybug.manager.env") as mock_env,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
        return_value=256,
      ),
    ):
      mock_conn_cls.return_value = MagicMock()
      mock_env.is_development.return_value = False

      result = manager.create_database(request)
      assert result.status == "success"

  def test_create_database_subgraph_bypasses_capacity(self):
    """Subgraph should bypass max_databases check."""
    manager = _make_manager(self.temp_dir, max_databases=1)

    # Fill capacity
    (Path(self.temp_dir) / "existing.lbug").touch()

    request = DatabaseCreateRequest(
      graph_id="parent_sub1",
      schema_type="entity",
      is_subgraph=True,
    )

    with (
      patch("robosystems.graph_api.core.ladybug.manager.lbug.Database"),
      patch(
        "robosystems.graph_api.core.ladybug.manager.lbug.Connection"
      ) as mock_conn_cls,
      patch.object(manager, "_apply_schema", return_value=True),
      patch("robosystems.graph_api.core.ladybug.manager.env") as mock_env,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
        return_value=256,
      ),
    ):
      mock_conn = MagicMock()
      mock_conn_cls.return_value = mock_conn
      mock_env.is_development.return_value = False

      result = manager.create_database(request)
      assert result.status == "success"

  def test_create_database_conflict_if_exists(self):
    """Should raise 409 if database already exists."""
    manager = _make_manager(self.temp_dir)

    (Path(self.temp_dir) / "existing.lbug").touch()

    request = DatabaseCreateRequest(graph_id="existing", schema_type="entity")

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)
    assert exc_info.value.status_code == 409

  def test_create_database_cleans_orphaned_wal(self):
    """Should remove orphaned WAL file before creation."""
    manager = _make_manager(self.temp_dir)

    wal_path = Path(self.temp_dir) / "newdb.lbug.wal"
    wal_path.touch()
    assert wal_path.exists()

    request = DatabaseCreateRequest(graph_id="newdb", schema_type="entity")

    with (
      patch("ladybug.Database"),
      patch("ladybug.Connection") as mock_conn_cls,
    ):
      mock_conn = MagicMock()
      mock_conn_cls.return_value = mock_conn

      with (
        patch.object(manager, "_apply_schema", return_value=True),
        patch("robosystems.graph_api.core.ladybug.manager.env") as mock_env,
        patch(
          "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
          return_value=256,
        ),
      ):
        mock_env.is_development.return_value = False

        manager.create_database(request)

    assert not wal_path.exists()

  def test_create_database_sec_checkpoint_threshold(self):
    """SEC database should use 128MB checkpoint threshold."""
    manager = _make_manager(self.temp_dir)
    request = DatabaseCreateRequest(graph_id="sec", schema_type="shared")

    with (
      patch("ladybug.Database") as mock_db_cls,
      patch("ladybug.Connection") as mock_conn_cls,
    ):
      mock_conn = MagicMock()
      mock_conn_cls.return_value = mock_conn

      with (
        patch.object(manager, "_apply_schema", return_value=True),
        patch("robosystems.graph_api.core.ladybug.manager.env") as mock_env,
        patch(
          "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
          return_value=256,
        ),
      ):
        mock_env.is_development.return_value = False

        manager.create_database(request)

      # Verify checkpoint_threshold was 128MB for SEC
      call_kwargs = mock_db_cls.call_args
      assert call_kwargs[1]["checkpoint_threshold"] == 134217728

  def test_create_database_failure_cleans_up_file(self):
    """Should clean up .lbug file on creation failure."""
    manager = _make_manager(self.temp_dir)
    request = DatabaseCreateRequest(graph_id="faildb", schema_type="entity")

    with (
      patch("ladybug.Database", side_effect=RuntimeError("DB create failed")),
      patch(
        "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
        return_value=256,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        manager.create_database(request)
      assert exc_info.value.status_code == 500

  def test_create_database_subgraph_skips_schema(self):
    """Subgraph should skip schema application."""
    manager = _make_manager(self.temp_dir)
    request = DatabaseCreateRequest(
      graph_id="parent_child",
      schema_type="entity",
      is_subgraph=True,
    )

    with (
      patch("ladybug.Database"),
      patch("ladybug.Connection") as mock_conn_cls,
    ):
      mock_conn = MagicMock()
      mock_conn_cls.return_value = mock_conn

      with (
        patch.object(manager, "_apply_schema") as mock_apply,
        patch("robosystems.graph_api.core.ladybug.manager.env") as mock_env,
        patch(
          "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
          return_value=256,
        ),
      ):
        mock_env.is_development.return_value = False

        result = manager.create_database(request)

        # _apply_schema should NOT be called for subgraphs
        mock_apply.assert_not_called()
        assert result.schema_applied is True


@pytest.mark.unit
class TestLadybugDatabaseManagerDeleteDatabase:
  """Tests for delete_database lifecycle."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_delete_database_not_found(self):
    """Should raise 404 if database doesn't exist."""
    manager = _make_manager(self.temp_dir)

    with pytest.raises(HTTPException) as exc_info:
      manager.delete_database("nonexistent")
    assert exc_info.value.status_code == 404

  def test_delete_database_success(self):
    """Should delete database file and WAL."""
    manager = _make_manager(self.temp_dir)

    db_path = Path(self.temp_dir) / "testdb.lbug"
    wal_path = Path(self.temp_dir) / "testdb.lbug.wal"
    db_path.touch()
    wal_path.touch()

    with patch("robosystems.graph_api.core.duckdb.get_duckdb_pool") as mock_get_duck:
      mock_duck_pool = MagicMock()
      mock_get_duck.return_value = mock_duck_pool

      result = manager.delete_database("testdb")

    assert result["status"] == "success"
    assert not db_path.exists()
    assert not wal_path.exists()

  def test_delete_database_preserve_duckdb(self):
    """Should skip DuckDB cleanup when preserve_duckdb is True."""
    manager = _make_manager(self.temp_dir)

    db_path = Path(self.temp_dir) / "testdb.lbug"
    db_path.touch()

    with patch("robosystems.graph_api.core.duckdb.get_duckdb_pool") as mock_get_duck:
      result = manager.delete_database("testdb", preserve_duckdb=True)

      # DuckDB pool should NOT be called
      mock_get_duck.assert_not_called()

    assert result["status"] == "success"

  def test_delete_database_directory_legacy(self):
    """Should delete legacy directory-based database."""
    manager = _make_manager(self.temp_dir)

    db_path = Path(self.temp_dir) / "testdb.lbug"
    db_path.mkdir()
    (db_path / "data.bin").touch()

    with patch("robosystems.graph_api.core.duckdb.get_duckdb_pool") as mock_get_duck:
      mock_duck_pool = MagicMock()
      mock_get_duck.return_value = mock_duck_pool

      result = manager.delete_database("testdb")

    assert result["status"] == "success"
    assert not db_path.exists()


@pytest.mark.unit
class TestLadybugDatabaseManagerListDatabases:
  """Tests for list_databases scanning."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_list_databases_empty(self):
    """Should return empty list for empty directory."""
    manager = _make_manager(self.temp_dir)
    assert manager.list_databases() == []

  def test_list_databases_filters_non_lbug(self):
    """Should only return .lbug files."""
    manager = _make_manager(self.temp_dir)

    (Path(self.temp_dir) / "db1.lbug").touch()
    (Path(self.temp_dir) / "db2.lbug").touch()
    (Path(self.temp_dir) / "db1.lbug.wal").touch()
    (Path(self.temp_dir) / "other.txt").touch()

    result = manager.list_databases()
    assert sorted(result) == ["db1", "db2"]

  def test_list_databases_sorted(self):
    """Should return sorted database names."""
    manager = _make_manager(self.temp_dir)

    (Path(self.temp_dir) / "zebra.lbug").touch()
    (Path(self.temp_dir) / "alpha.lbug").touch()
    (Path(self.temp_dir) / "middle.lbug").touch()

    result = manager.list_databases()
    assert result == ["alpha", "middle", "zebra"]


@pytest.mark.unit
class TestLadybugDatabaseManagerApplySchema:
  """Tests for _apply_schema dispatch."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_apply_schema_unknown_type(self):
    """Should return False for unknown schema type."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    result = manager._apply_schema(mock_conn, "nonexistent_type")
    assert result is False

  def test_apply_custom_schema_no_ddl(self):
    """Should return False when custom DDL is None."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    result = manager._apply_custom_schema(mock_conn, None)
    assert result is False

  def test_apply_custom_schema_success(self):
    """Should execute DDL statements."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    ddl = "CREATE NODE TABLE A(id STRING, PRIMARY KEY(id)); CREATE NODE TABLE B(id STRING, PRIMARY KEY(id))"
    result = manager._apply_custom_schema(mock_conn, ddl)

    assert result is True
    assert mock_conn.execute.call_count == 2

  def test_apply_custom_schema_execution_error(self):
    """Should return False if DDL execution fails."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Syntax error")

    result = manager._apply_custom_schema(mock_conn, "INVALID DDL")
    assert result is False


@pytest.mark.unit
class TestLadybugDatabaseManagerMapSchemaType:
  """Tests for _map_schema_type_to_lbug type mapping."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_known_types(self):
    """Should map known types correctly."""
    manager = _make_manager(self.temp_dir)

    assert manager._map_schema_type_to_lbug("STRING") == "STRING"
    assert manager._map_schema_type_to_lbug("INT64") == "INT64"
    assert manager._map_schema_type_to_lbug("DOUBLE") == "DOUBLE"
    assert manager._map_schema_type_to_lbug("BOOLEAN") == "BOOLEAN"

  def test_case_insensitive(self):
    """Should handle lowercase input."""
    manager = _make_manager(self.temp_dir)

    assert manager._map_schema_type_to_lbug("string") == "STRING"
    assert manager._map_schema_type_to_lbug("int64") == "INT64"

  def test_parameterized_types_pass_through(self):
    """Should pass through parameterized types like FLOAT[384]."""
    manager = _make_manager(self.temp_dir)

    assert manager._map_schema_type_to_lbug("FLOAT[384]") == "FLOAT[384]"
    assert manager._map_schema_type_to_lbug("INT64[10]") == "INT64[10]"

  def test_unknown_type_defaults_to_string(self):
    """Should default to STRING for unknown types."""
    manager = _make_manager(self.temp_dir)

    assert manager._map_schema_type_to_lbug("UNKNOWN") == "STRING"
    assert manager._map_schema_type_to_lbug("CUSTOM_TYPE") == "STRING"


@pytest.mark.unit
class TestLadybugDatabaseManagerCreateVectorIndex:
  """Tests for create_vector_index."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_create_vector_index_success(self):
    """Should create vector index successfully."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    result = manager.create_vector_index(mock_conn, "Element", "embedding")

    assert result is True
    mock_conn.execute.assert_any_call(
      "CALL CREATE_VECTOR_INDEX('Element', 'element_vec_index', 'embedding')"
    )

  def test_create_vector_index_already_exists(self):
    """Should return True when index already exists."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    # LOAD succeeds, but CREATE_VECTOR_INDEX raises "already exists"
    call_count = 0

    def side_effect(query):
      nonlocal call_count
      call_count += 1
      if "CREATE_VECTOR_INDEX" in query:
        raise RuntimeError("Index already exists")
      return None

    mock_conn.execute.side_effect = side_effect

    result = manager.create_vector_index(mock_conn, "Element")
    assert result is True

  def test_create_vector_index_no_column(self):
    """Should return False when table has no embedding column."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    def side_effect(query):
      if "CREATE_VECTOR_INDEX" in query:
        raise RuntimeError("Column not found")
      return None

    mock_conn.execute.side_effect = side_effect

    result = manager.create_vector_index(mock_conn, "NoEmbedding")
    assert result is False

  def test_create_vector_index_load_extension_fallback(self):
    """Should try INSTALL if LOAD fails."""
    manager = _make_manager(self.temp_dir)
    mock_conn = MagicMock()

    call_count = 0

    def side_effect(query):
      nonlocal call_count
      call_count += 1
      if call_count == 1 and "LOAD" in query:
        raise RuntimeError("Not loaded")
      return None

    mock_conn.execute.side_effect = side_effect

    result = manager.create_vector_index(mock_conn, "Table1")
    assert result is True


@pytest.mark.unit
class TestLadybugDatabaseManagerHealthCheck:
  """Tests for _check_database_health."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_health_check_file_not_found(self):
    """Should return False if .lbug file doesn't exist."""
    manager = _make_manager(self.temp_dir)
    assert manager._check_database_health("nonexistent") is False

  def test_health_check_connection_failure(self):
    """Should return False on connection error."""
    manager = _make_manager(self.temp_dir)

    (Path(self.temp_dir) / "testdb.lbug").touch()
    manager.connection_pool.get_connection.return_value.__enter__.side_effect = (
      RuntimeError("Connection failed")
    )

    assert manager._check_database_health("testdb") is False

  def test_health_check_success(self):
    """Should return True on successful health check."""
    manager = _make_manager(self.temp_dir)

    (Path(self.temp_dir) / "testdb.lbug").touch()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock()
    manager.connection_pool.get_connection.return_value.__enter__.return_value = (
      mock_conn
    )

    assert manager._check_database_health("testdb") is True


@pytest.mark.unit
class TestLadybugDatabaseManagerGetDatabaseInfo:
  """Tests for get_database_info."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_get_database_info_not_found(self):
    """Should raise 404 if database doesn't exist."""
    manager = _make_manager(self.temp_dir)

    with pytest.raises(HTTPException) as exc_info:
      manager.get_database_info("nonexistent")
    assert exc_info.value.status_code == 404

  def test_get_database_info_success(self):
    """Should return database info with size and health status."""
    manager = _make_manager(self.temp_dir)

    db_path = Path(self.temp_dir) / "testdb.lbug"
    db_path.write_bytes(b"x" * 1024)

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock()
    manager.connection_pool.get_connection.return_value.__enter__.return_value = (
      mock_conn
    )
    manager.connection_pool.has_active_connections.return_value = True

    result = manager.get_database_info("testdb")

    assert result.graph_id == "testdb"
    assert result.size_bytes == 1024
    assert result.is_healthy is True
    assert result.last_accessed is not None


@pytest.mark.unit
class TestLadybugDatabaseManagerCloseAll:
  """Tests for close_all_connections."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_close_all_connections(self):
    """Should delegate to connection pool."""
    manager = _make_manager(self.temp_dir)
    manager.close_all_connections()
    manager.connection_pool.close_all_connections.assert_called_once()

  def test_close_all_connections_error_handled(self):
    """Should handle errors in pool cleanup."""
    manager = _make_manager(self.temp_dir)
    manager.connection_pool.close_all_connections.side_effect = RuntimeError(
      "Cleanup failed"
    )

    # Should not raise
    manager.close_all_connections()
