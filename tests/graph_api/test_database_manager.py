"""Comprehensive tests for LadybugDatabaseManager."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

from robosystems.graph_api.core.ladybug.manager import (
  LadybugDatabaseManager,
  validate_database_path,
)
from robosystems.graph_api.models.database import (
  DatabaseCreateRequest,
  DatabaseInfo,
)


class TestDatabasePathValidation:
  """Test database path validation security function."""

  def test_validate_database_path_valid(self):
    """Test valid database path validation."""
    base_path = Path("/tmp/lbug-dbs-dbs")

    valid_names = [
      "kg1a2b3c",
      "test_db",
      "my-database",
      "DB_01",
      "a1b2c3",
    ]

    for name in valid_names:
      result = validate_database_path(base_path, name)
      expected = base_path / f"{name}.lbug"
      assert result == expected

  def test_validate_database_path_invalid(self):
    """Test invalid database path validation."""
    from fastapi import HTTPException

    base_path = Path("/tmp/lbug-dbs-dbs")

    invalid_names = [
      "",
      "db/../other",
      "db\\windows",
      "db..parent",
      "db/subdir",
      "db with spaces",
      "db@special",
    ]

    for name in invalid_names:
      with pytest.raises(HTTPException) as exc_info:
        validate_database_path(base_path, name)
      assert exc_info.value.status_code == 400, (
        f"Name '{name}' should raise HTTPException"
      )

    # Test None separately (causes TypeError)
    with pytest.raises((HTTPException, TypeError)):
      validate_database_path(base_path, None)

  def test_validate_database_path_traversal_attempt(self):
    """Test path traversal attack prevention."""
    from fastapi import HTTPException

    base_path = Path("/tmp/lbug-dbs-dbs")

    # These should all be blocked
    malicious_names = [
      "../../../etc/passwd",
      "..\\..\\windows\\system32",
      "./../sensitive",
      "normal/../../etc",
    ]

    for name in malicious_names:
      with pytest.raises(HTTPException) as exc_info:
        validate_database_path(base_path, name)
      assert exc_info.value.status_code == 400


class TestLadybugDatabaseManager:
  """Test LadybugDatabaseManager class."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.base_path = Path(self.temp_dir)
    self.max_databases = 50

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_initialization(self, mock_init_pool):
    """Test manager initialization."""
    mock_pool = MagicMock()
    mock_init_pool.return_value = mock_pool

    manager = LadybugDatabaseManager(
      base_path=str(self.base_path),
      max_databases=self.max_databases,
      max_connections_per_db=5,
    )

    assert manager.base_path == self.base_path
    assert manager.max_databases == self.max_databases
    assert manager.connection_pool == mock_pool
    assert self.base_path.exists()

    # Verify connection pool initialization
    mock_init_pool.assert_called_once_with(
      base_path=str(self.base_path),
      max_connections_per_db=5,
      connection_ttl_minutes=30,
    )

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_create_database_success_entity_schema(
    self, mock_conn_class, mock_db_class, mock_init_pool
  ):
    """Test successful database creation with entity schema."""
    # Setup mocks
    mock_init_pool.return_value = MagicMock()
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Mock list_databases to return empty (capacity check)
    manager.list_databases = MagicMock(return_value=[])

    request = DatabaseCreateRequest(
      graph_id="test_entity",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
      read_only=False,
    )

    response = manager.create_database(request)

    # Verify response
    assert response.status == "success"
    assert response.graph_id == "test_entity"
    assert response.schema_applied is True
    assert response.execution_time_ms > 0
    assert "test_entity.lbug" in response.database_path

    # Verify LadybugDB calls
    expected_db_path = str(self.base_path / "test_entity.lbug")
    # Check that database was called with expected parameters (buffer_pool_size is now included)
    mock_db_class.assert_called_once()
    call_args = mock_db_class.call_args
    assert call_args[0][0] == expected_db_path
    assert call_args[1]["read_only"] is False
    assert "buffer_pool_size" in call_args[1]
    mock_conn_class.assert_called_once_with(mock_db)

    # Verify schema application - should execute multiple CREATE statements
    assert (
      mock_conn.execute.call_count >= 5
    )  # Entity schema has multiple tables + relationships

    # Verify database was created successfully (connection pool manages connections)
    # New implementation uses connection pool, no direct database/connection storage

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_create_database_read_only(
    self, mock_conn_class, mock_db_class, mock_init_pool
  ):
    """Test database creation in read-only mode."""
    # Setup mocks
    mock_init_pool.return_value = MagicMock()
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)
    manager.list_databases = MagicMock(return_value=[])

    request = DatabaseCreateRequest(
      graph_id="test_readonly",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
      read_only=True,
    )

    response = manager.create_database(request)

    assert response.status == "success"

    # Verify connections are closed for read-only databases
    mock_conn.close.assert_called_once()
    mock_db.close.assert_called_once()

    # Verify read-only database creation completed successfully
    # New implementation uses connection pool, no direct connection storage

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_create_database_capacity_exceeded(self, mock_init_pool):
    """Test database creation when capacity is exceeded."""
    from fastapi import HTTPException

    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), max_databases=2)

    # Mock list_databases to return max capacity
    manager.list_databases = MagicMock(return_value=["db1", "db2"])

    request = DatabaseCreateRequest(
      graph_id="test_db",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
    )

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)

    assert exc_info.value.status_code == 507  # Insufficient Storage
    assert "Maximum database capacity reached" in str(exc_info.value.detail)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_create_database_already_exists(self, mock_init_pool):
    """Test database creation when database already exists."""
    from fastapi import HTTPException

    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)
    manager.list_databases = MagicMock(return_value=[])

    # Create a database file to simulate existing database (LadybugDB 0.11.0)
    existing_db_path = self.base_path / "existing_db.lbug"
    existing_db_path.touch()

    request = DatabaseCreateRequest(
      graph_id="existing_db",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
    )

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)

    assert exc_info.value.status_code == 409  # Conflict
    assert "already exists" in str(exc_info.value.detail)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  @patch("real_ladybug.Database")
  def test_create_database_lbug_error_cleanup(self, mock_db_class, mock_init_pool):
    """Test database creation error handling and cleanup."""
    from fastapi import HTTPException

    mock_init_pool.return_value = MagicMock()
    # Make LadybugDB raise an exception
    mock_db_class.side_effect = Exception("LadybugDB connection failed")

    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)
    manager.list_databases = MagicMock(return_value=[])

    request = DatabaseCreateRequest(
      graph_id="test_fail",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
    )

    with pytest.raises(HTTPException) as exc_info:
      manager.create_database(request)

    assert exc_info.value.status_code == 500
    assert "Database creation failed" in str(exc_info.value.detail)

    # Verify cleanup - database directory should not exist
    db_path = self.base_path / "test_fail.lbug"
    assert not db_path.exists()

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_delete_database_success(self, mock_init_pool):
    """Test successful database deletion."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Create a test database file (LadybugDB 0.11.0)
    db_path = self.base_path / "test_db.lbug"
    db_path.touch()

    # Mock connection pool cleanup method
    manager.connection_pool.close_database_connections = MagicMock()

    result = manager.delete_database("test_db")

    assert result["status"] == "success"
    assert result["graph_id"] == "test_db"
    assert "successfully" in result["message"]

    # Verify connection pool was called to close database connections
    manager.connection_pool.close_database_connections.assert_called_once_with(
      "test_db"
    )

    # Verify directory was deleted
    assert not db_path.exists()

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_delete_database_not_found(self, mock_init_pool):
    """Test database deletion when database doesn't exist."""
    from fastapi import HTTPException

    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    with pytest.raises(HTTPException) as exc_info:
      manager.delete_database("nonexistent_db")

    assert exc_info.value.status_code == 404
    assert "not found" in str(exc_info.value.detail)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_list_databases(self, mock_init_pool):
    """Test database listing."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Create test database directories
    (self.base_path / "db1.lbug").touch()
    (self.base_path / "db2.lbug").touch()
    (self.base_path / "not_a_db").mkdir()  # Should be ignored
    (self.base_path / "db3.lbug").touch()

    databases = manager.list_databases()

    # Should return only .lbug files, sorted
    assert databases == ["db1", "db2", "db3"]

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_list_databases_empty(self, mock_init_pool):
    """Test database listing when no databases exist."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    databases = manager.list_databases()

    assert databases == []

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_get_database_info_success(self, mock_init_pool):
    """Test successful database info retrieval."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Create test database file (LadybugDB 0.11.0 uses single files)
    db_path = self.base_path / "test_db.lbug"
    db_path.write_bytes(b"x" * 1024)  # 1KB file

    # Mock health check
    manager._check_database_health = MagicMock(return_value=True)

    # Mock connection pool active connections check
    manager.connection_pool.has_active_connections = MagicMock(return_value=True)

    info = manager.get_database_info("test_db")

    assert info.graph_id == "test_db"
    assert str(db_path) in info.database_path
    assert info.size_bytes >= 1024
    assert info.read_only == manager.read_only  # Uses manager's read_only setting
    assert info.is_healthy is True
    assert info.last_accessed is not None
    assert isinstance(datetime.fromisoformat(info.created_at), datetime)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_get_database_info_not_found(self, mock_init_pool):
    """Test database info retrieval for non-existent database."""
    from fastapi import HTTPException

    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    with pytest.raises(HTTPException) as exc_info:
      manager.get_database_info("nonexistent_db")

    assert exc_info.value.status_code == 404
    assert "not found" in str(exc_info.value.detail)

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_get_all_databases_info(self, mock_init_pool):
    """Test retrieval of all databases info."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), max_databases=100)

    # Mock list_databases and get_database_info
    manager.list_databases = MagicMock(return_value=["db1", "db2"])

    mock_db_info_1 = DatabaseInfo(
      graph_id="db1",
      database_path="/tmp/db1.lbug",
      created_at="2023-01-01T00:00:00",
      size_bytes=1024,
      read_only=False,
      is_healthy=True,
      last_accessed=None,
    )
    mock_db_info_2 = DatabaseInfo(
      graph_id="db2",
      database_path="/tmp/db2.lbug",
      created_at="2023-01-01T00:00:00",
      size_bytes=2048,
      read_only=True,
      is_healthy=True,
      last_accessed=None,
    )

    manager.get_database_info = MagicMock(side_effect=[mock_db_info_1, mock_db_info_2])

    response = manager.get_all_databases_info()

    assert response.total_databases == 2
    assert response.total_size_bytes == 3072
    assert len(response.databases) == 2
    assert response.node_capacity["max_databases"] == 100
    assert response.node_capacity["current_databases"] == 2
    assert response.node_capacity["capacity_remaining"] == 98
    assert response.node_capacity["utilization_percent"] == 2.0

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_health_check_all(self, mock_init_pool):
    """Test health check for all databases."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Mock databases with different health status
    manager.list_databases = MagicMock(return_value=["healthy_db", "unhealthy_db"])

    healthy_info = DatabaseInfo(
      graph_id="healthy_db",
      database_path="/tmp/healthy_db.lbug",
      created_at="2023-01-01T00:00:00",
      size_bytes=1024,
      read_only=False,
      is_healthy=True,
      last_accessed=None,
    )
    unhealthy_info = DatabaseInfo(
      graph_id="unhealthy_db",
      database_path="/tmp/unhealthy_db.lbug",
      created_at="2023-01-01T00:00:00",
      size_bytes=1024,
      read_only=False,
      is_healthy=False,
      last_accessed=None,
    )

    manager.get_database_info = MagicMock(side_effect=[healthy_info, unhealthy_info])

    health_response = manager.health_check_all()

    assert health_response.healthy_databases == 1
    assert health_response.unhealthy_databases == 1
    assert len(health_response.databases) == 2

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_check_database_health_success(self, mock_init_pool):
    """Test successful database health check (file existence only)."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Create test database directory
    db_path = self.base_path / "test_db.lbug"
    db_path.touch()

    is_healthy = manager._check_database_health("test_db")

    assert is_healthy is True

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_check_database_health_failure(self, mock_init_pool):
    """Test database health check failure (file does not exist)."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Do not create test database directory to test file existence failure
    is_healthy = manager._check_database_health("test_db")

    assert is_healthy is False

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_check_database_health_not_found(self, mock_init_pool):
    """Test database health check for non-existent database."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    is_healthy = manager._check_database_health("nonexistent_db")

    assert is_healthy is False

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_entity_schema(self, mock_init_pool):
    """Test entity schema application using dynamic schema loader."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()

    result = manager._apply_entity_schema(mock_conn)

    assert result is True

    # Verify that schema statements were executed (should be many due to dynamic schema)
    assert mock_conn.execute.called
    # Should have multiple calls for creating tables (adjusted after schema reorganization)
    assert mock_conn.execute.call_count >= 8

    # Verify that CREATE NODE TABLE and CREATE REL TABLE statements were made
    call_args = [call[0][0] for call in mock_conn.execute.call_args_list]
    node_table_calls = [arg for arg in call_args if "CREATE NODE TABLE" in arg]
    rel_table_calls = [arg for arg in call_args if "CREATE REL TABLE" in arg]

    # Should have created multiple node and relationship tables
    assert len(node_table_calls) >= 3  # Adjusted expectations after schema cleanup
    assert len(rel_table_calls) >= 3

    # Verify some key tables are present
    all_calls_str = " ".join(call_args)
    assert "CREATE NODE TABLE IF NOT EXISTS Entity" in all_calls_str
    assert "CREATE NODE TABLE IF NOT EXISTS Element" in all_calls_str

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_shared_schema(self, mock_init_pool):
    """Test shared schema application using dynamic schema loader."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()

    result = manager._apply_shared_schema(mock_conn, "sec")

    assert result is True

    # Verify that schema statements were executed
    assert mock_conn.execute.called
    assert mock_conn.execute.call_count > 0

    # Verify that CREATE NODE TABLE and CREATE REL TABLE statements were made
    call_args = [call[0][0] for call in mock_conn.execute.call_args_list]
    all_calls_str = " ".join(call_args)
    assert "CREATE NODE TABLE IF NOT EXISTS" in all_calls_str

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_schema_unknown_type(self, mock_init_pool):
    """Test schema application with unknown schema type."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()

    result = manager._apply_schema(mock_conn, "unknown_schema")

    assert result is False

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_fallback_entity_schema(self, mock_init_pool):
    """Test fallback entity schema application."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()

    result = manager._apply_fallback_entity_schema(mock_conn)

    assert result is True

    # Verify minimal schema was applied
    assert mock_conn.execute.called
    assert mock_conn.execute.call_count == 3  # Entity, User, HAS_USER relationship

    # Verify the minimal schema calls
    call_args = [call[0][0] for call in mock_conn.execute.call_args_list]
    all_calls_str = " ".join(call_args)
    assert "CREATE NODE TABLE IF NOT EXISTS Entity" in all_calls_str
    assert "CREATE NODE TABLE IF NOT EXISTS User" in all_calls_str
    assert "CREATE REL TABLE IF NOT EXISTS HAS_USER" in all_calls_str

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_fallback_entity_schema_with_error(self, mock_init_pool):
    """Test fallback entity schema application with database error."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Database error")

    result = manager._apply_fallback_entity_schema(mock_conn)

    assert result is False

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_map_schema_type_to_lbug(self, mock_init_pool):
    """Test schema type mapping to LadybugDB types."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    # Test known type mappings
    assert manager._map_schema_type_to_lbug("STRING") == "STRING"
    assert manager._map_schema_type_to_lbug("INT64") == "INT64"
    assert manager._map_schema_type_to_lbug("DOUBLE") == "DOUBLE"
    assert manager._map_schema_type_to_lbug("BOOLEAN") == "BOOLEAN"
    assert manager._map_schema_type_to_lbug("TIMESTAMP") == "TIMESTAMP"
    assert manager._map_schema_type_to_lbug("DATE") == "DATE"

    # Test case insensitivity
    assert manager._map_schema_type_to_lbug("string") == "STRING"
    assert manager._map_schema_type_to_lbug("int64") == "INT64"

    # Test unknown type defaults to STRING
    assert manager._map_schema_type_to_lbug("UNKNOWN_TYPE") == "STRING"
    assert manager._map_schema_type_to_lbug("") == "STRING"

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  @patch("robosystems.schemas.loader.get_schema_loader")
  def test_apply_entity_schema_with_schema_loader_failure(
    self, mock_get_schema_loader, mock_init_pool
  ):
    """Test entity schema application when schema loader fails."""
    mock_init_pool.return_value = MagicMock()
    mock_get_schema_loader.side_effect = ImportError("Schema loader not available")

    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)
    mock_conn = MagicMock()

    result = manager._apply_entity_schema(mock_conn)

    # Should fallback to minimal schema and still succeed
    assert result is True

    # Verify fallback was used (should have exactly 3 calls for minimal schema)
    assert mock_conn.execute.call_count == 3

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_apply_shared_schema_with_different_repositories(self, mock_init_pool):
    """Test shared schema application with different repository types."""
    mock_init_pool.return_value = MagicMock()
    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    mock_conn = MagicMock()

    # Test SEC repository
    result_sec = manager._apply_shared_schema(mock_conn, "sec")
    assert result_sec is True

    # Reset mock
    mock_conn.reset_mock()

    # Test other repository
    result_other = manager._apply_shared_schema(mock_conn, "industry")
    assert result_other is True

  @patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool")
  def test_close_all_connections(self, mock_init_pool):
    """Test closing all connections."""
    mock_pool = MagicMock()
    mock_init_pool.return_value = mock_pool

    manager = LadybugDatabaseManager(str(self.base_path), self.max_databases)

    manager.close_all_connections()

    # Verify connection pool cleanup
    mock_pool.close_all_connections.assert_called_once()

  def test_database_create_request_validation(self):
    """Test DatabaseCreateRequest model validation."""
    from pydantic import ValidationError

    # Valid request
    valid_request = DatabaseCreateRequest(
      graph_id="test_entity",
      schema_type="entity",
      repository_name=None,
      custom_schema_ddl=None,
    )
    assert valid_request.graph_id == "test_entity"
    assert valid_request.schema_type == "entity"

    # Invalid database name
    with pytest.raises(ValidationError):
      DatabaseCreateRequest(
        graph_id="invalid@name",
        schema_type="entity",
        repository_name=None,
        custom_schema_ddl=None,
      )

    # Invalid schema type
    with pytest.raises(ValidationError):
      DatabaseCreateRequest(
        graph_id="test_db",
        schema_type="invalid_schema",
        repository_name=None,
        custom_schema_ddl=None,
      )

    # Database name too long
    with pytest.raises(ValidationError):
      DatabaseCreateRequest(
        graph_id="x" * 65,
        schema_type="entity",
        repository_name=None,
        custom_schema_ddl=None,
      )


@pytest.mark.integration
class TestLadybugDatabaseManagerIntegration:
  """Integration tests for LadybugDatabaseManager."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.base_path = Path(self.temp_dir)
    self.max_databases = 50

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_concurrent_database_operations(self):
    """Test concurrent database creation and deletion with mocked LadybugDB."""
    import time
    import concurrent.futures
    from unittest.mock import patch, MagicMock

    # Mock LadybugDB to avoid segmentation faults while testing concurrency logic
    with (
      patch("real_ladybug.Database") as mock_db_class,
      patch("real_ladybug.Connection") as mock_conn_class,
    ):
      # Setup mocks
      mock_db = MagicMock()
      mock_conn = MagicMock()
      mock_db_class.return_value = mock_db
      mock_conn_class.return_value = mock_conn

      # Mock schema loading to trigger fallback behavior
      with patch(
        "robosystems.schemas.loader.get_schema_loader",
        side_effect=ImportError("Mocked schema error"),
      ):
        manager = LadybugDatabaseManager(str(self.base_path), max_databases=20)

      # Track operations and errors
      results = {"created": [], "deleted": [], "errors": []}

      def create_database(db_name):
        """Create a database in a thread."""
        try:
          request = DatabaseCreateRequest(
            graph_id=db_name,
            schema_type="entity",
            repository_name=None,
            custom_schema_ddl=None,
            read_only=False,
          )
          manager.create_database(request)
          results["created"].append(db_name)
          return f"Created {db_name}"
        except Exception as e:
          results["errors"].append(f"Create {db_name}: {str(e)}")
          return f"Error creating {db_name}: {str(e)}"

      def delete_database(db_name):
        """Delete a database in a thread."""
        try:
          # Wait a bit to ensure database exists before trying to delete
          time.sleep(0.1)
          manager.delete_database(db_name)
          results["deleted"].append(db_name)
          return f"Deleted {db_name}"
        except Exception as e:
          results["errors"].append(f"Delete {db_name}: {str(e)}")
          return f"Error deleting {db_name}: {str(e)}"

      # Test 1: Concurrent creation
      db_names = [f"concurrent_db_{i}" for i in range(5)]

      with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all creation tasks
        create_futures = [executor.submit(create_database, name) for name in db_names]

        # Wait for all to complete
        concurrent.futures.wait(create_futures)

      # Since we're mocking LadybugDB, we can't rely on filesystem directories
      # Instead, check that the creation operations succeeded
      created_count = len(results["created"])

      # Should have created most or all databases (some might conflict)
      assert created_count >= 3, (
        f"Only {created_count} databases created, expected at least 3. Errors: {results['errors']}"
      )

      # Test 2: Concurrent deletion - create actual directories first for this test
      for db_name in results["created"]:
        db_path = self.base_path / f"{db_name}.lbug"
        db_path.touch(exist_ok=True)

      existing_dbs = results["created"][:3]  # Test with first 3 created databases

      with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit deletion tasks for existing databases
        delete_futures = [
          executor.submit(delete_database, name) for name in existing_dbs
        ]

        # Wait for all to complete
        concurrent.futures.wait(delete_futures)

      # Check deletion success by tracking operations, not filesystem
      deleted_count = len(results["deleted"])

      # Should have deleted most or all databases
      assert deleted_count >= len(existing_dbs) - 1, (
        f"Only {deleted_count} databases deleted from {len(existing_dbs)}"
      )

      # Test 3: Mixed concurrent operations
      results = {"created": [], "deleted": [], "errors": []}  # Reset results
      mixed_db_names = [f"mixed_db_{i}" for i in range(6)]

      with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = []

        # Create first 3 databases
        for i in range(3):
          futures.append(executor.submit(create_database, mixed_db_names[i]))

        # Small delay then create and immediately try to delete others
        time.sleep(0.05)
        for i in range(3, 6):
          # Create directories for deletion tests
          db_path = self.base_path / f"{mixed_db_names[i]}.lbug"
          db_path.touch(exist_ok=True)

          futures.append(executor.submit(create_database, mixed_db_names[i]))
          futures.append(executor.submit(delete_database, mixed_db_names[i]))

        # Wait for all operations
        concurrent.futures.wait(futures)

      # Verify operations completed without major errors
      total_operations = len(results["created"]) + len(results["deleted"])

      # Should have completed several operations successfully
      assert total_operations >= 4, f"Too few operations completed: {total_operations}"
      # Some conflicts are expected in mixed concurrent operations
      assert len(results["errors"]) <= 4, (
        f"Too many errors in mixed operations: {len(results['errors'])}"
      )

      # Test 4: Verify database manager integrity
      try:
        # Should still be able to perform operations
        test_request = DatabaseCreateRequest(
          graph_id="integrity_test",
          schema_type="entity",
          repository_name=None,
          custom_schema_ddl=None,
          read_only=False,
        )
        response = manager.create_database(test_request)
        assert response.status == "success"

        # Create directory for cleanup test
        integrity_path = self.base_path / "integrity_test.lbug"
        integrity_path.touch(exist_ok=True)

        # Clean up
        result = manager.delete_database("integrity_test")
        assert result["status"] == "success"

      except Exception as e:
        pytest.fail(
          f"Database manager integrity compromised after concurrent operations: {e}"
        )

      # Clean up any remaining test directories (since mocking doesn't create real LadybugDB DBs)
      import shutil

      for pattern in ["concurrent_db_", "mixed_db_"]:
        for path in self.base_path.glob(f"{pattern}*.lbug"):
          try:
            shutil.rmtree(path, ignore_errors=True)
          except Exception:
            pass  # Ignore cleanup errors
