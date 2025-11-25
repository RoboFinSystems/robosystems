"""Comprehensive tests for DuckDBConnectionPool."""

import pytest
import tempfile
import shutil
import threading
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

from robosystems.graph_api.core.duckdb.pool import (
  DuckDBConnectionPool,
  DuckDBConnectionInfo,
  initialize_duckdb_pool,
  get_duckdb_pool,
)


class TestDuckDBConnectionInfo:
  """Test DuckDBConnectionInfo dataclass."""

  def test_connection_info_creation(self):
    """Test DuckDBConnectionInfo creation and attributes."""
    mock_conn = MagicMock()
    mock_path = Path("/tmp/test.duckdb")
    now = datetime.now(timezone.utc)

    info = DuckDBConnectionInfo(
      connection=mock_conn,
      database_path=mock_path,
      created_at=now,
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    assert info.connection == mock_conn
    assert info.database_path == mock_path
    assert info.created_at == now
    assert info.last_used == now
    assert info.use_count == 1
    assert info.is_healthy is True


class TestDuckDBConnectionPool:
  """Test DuckDBConnectionPool class."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.base_path = str(self.temp_dir)

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_initialization(self):
    """Test connection pool initialization."""
    pool = DuckDBConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=5,
      connection_ttl_minutes=20,
      health_check_interval_minutes=3,
      cleanup_interval_minutes=8,
    )

    assert pool.base_path == Path(self.base_path)
    assert pool.max_connections_per_db == 5
    assert pool.connection_ttl == timedelta(minutes=20)
    assert pool.health_check_interval == timedelta(minutes=3)
    assert pool.cleanup_interval == timedelta(minutes=8)

    # Verify thread-safe structures
    assert isinstance(pool._pools, dict)
    assert isinstance(pool._locks, dict)
    assert isinstance(pool._global_lock, type(threading.RLock()))
    assert isinstance(pool._stats, dict)

    # Verify stats initialization
    expected_stats = {
      "connections_created": 0,
      "connections_reused": 0,
      "connections_closed": 0,
      "health_checks": 0,
      "health_failures": 0,
      "databases_cleaned": 0,
    }
    assert pool._stats == expected_stats

  def test_initialization_defaults(self):
    """Test connection pool initialization with default parameters."""
    pool = DuckDBConnectionPool(base_path=self.base_path)

    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=30)
    assert pool.health_check_interval == timedelta(minutes=5)
    assert pool.cleanup_interval == timedelta(minutes=10)

  @patch("duckdb.connect")
  def test_get_connection_context_manager(self, mock_connect):
    """Test connection retrieval using context manager."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(base_path=self.base_path)

    with pool.get_connection("test_graph") as conn:
      assert conn == mock_conn

    # Verify connection was created
    expected_db_path = str(Path(self.base_path) / "test_graph.duckdb")
    mock_connect.assert_called_once_with(expected_db_path)

    # Verify extensions were loaded and configured
    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert "INSTALL httpfs" in execute_calls
    assert "LOAD httpfs" in execute_calls
    assert "INSTALL parquet" in execute_calls
    assert "LOAD parquet" in execute_calls

  @patch("duckdb.connect")
  def test_connection_reuse(self, mock_connect):
    """Test connection reuse from pool."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(base_path=self.base_path)

    # First connection - should create new
    with pool.get_connection("test_graph") as conn1:
      assert conn1 == mock_conn

    # Reset mock to verify second call behavior
    mock_connect.reset_mock()

    # Second connection - should reuse existing
    with pool.get_connection("test_graph") as conn2:
      assert conn2 == mock_conn

    # Verify connection was not created again
    mock_connect.assert_not_called()

    # Verify stats
    assert pool._stats["connections_created"] == 1
    assert pool._stats["connections_reused"] == 1

  @patch("duckdb.connect")
  def test_max_connections_per_database(self, mock_connect):
    """Test connection limit enforcement per database."""
    mock_conns = [MagicMock() for _ in range(5)]
    mock_connect.side_effect = mock_conns

    # Mock successful health checks
    for mock_conn in mock_conns:
      mock_result = MagicMock()
      mock_result.__getitem__.return_value = 1
      mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=2,
    )

    # Create first connection
    _ = pool._create_new_connection("test_graph")
    assert len(pool._pools["test_graph"]) == 1

    # Create second connection
    _ = pool._create_new_connection("test_graph")
    assert len(pool._pools["test_graph"]) == 2

    # Create third connection (should trigger eviction)
    _ = pool._create_new_connection("test_graph")

    # Should still respect the limit
    assert len(pool._pools["test_graph"]) <= pool.max_connections_per_db

    # At least one connection should have been closed during eviction
    close_calls = sum(1 for mock_conn in mock_conns if mock_conn.close.called)
    assert close_calls >= 1

  @patch("duckdb.connect")
  def test_connection_ttl_expiry(self, mock_connect):
    """Test connection TTL expiry and cleanup."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(
      base_path=self.base_path,
      connection_ttl_minutes=1,  # Very short TTL for testing
    )

    # Create a connection
    with pool.get_connection("test_graph"):
      pass

    # Manually expire the connection by modifying its created_at time
    db_pool = pool._pools["test_graph"]
    for conn_info in db_pool.values():
      conn_info.created_at = datetime.now(timezone.utc) - timedelta(minutes=2)

    # Mock new connection for the replacement
    mock_conn_new = MagicMock()
    mock_connect.return_value = mock_conn_new
    mock_conn_new.execute.return_value.fetchone.return_value = mock_result

    # Force cleanup of expired connections
    pool._cleanup_expired_connections()

    # Get connection again - should create new due to TTL expiry
    with pool.get_connection("test_graph") as conn:
      assert conn == mock_conn_new

    # Verify old connection was closed during cleanup
    mock_conn.close.assert_called()

  @patch("duckdb.connect")
  def test_connection_health_check_failure(self, mock_connect):
    """Test connection health check failure and recovery."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # First health check succeeds, second fails
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.side_effect = [
      mock_result,
      Exception("Connection lost"),
    ]

    pool = DuckDBConnectionPool(base_path=self.base_path)

    # Create a connection
    with pool.get_connection("test_graph"):
      pass

    # Simulate health check failure by calling directly
    db_pool = pool._pools["test_graph"]
    conn_info = list(db_pool.values())[0]
    is_healthy = pool._test_connection_health(conn_info)

    assert is_healthy is False
    assert pool._stats["health_failures"] == 1

  def test_database_lock_creation(self):
    """Test database-specific lock creation."""
    pool = DuckDBConnectionPool(base_path=self.base_path)

    # Get locks for different databases
    lock1 = pool._get_database_lock("graph1")
    lock2 = pool._get_database_lock("graph2")
    lock1_again = pool._get_database_lock("graph1")

    # Verify locks are different for different databases
    assert lock1 != lock2

    # Verify same lock is returned for same database
    assert lock1 == lock1_again

    # Verify locks are stored
    assert "graph1" in pool._locks
    assert "graph2" in pool._locks

  @patch("duckdb.connect")
  def test_connection_validity_check(self, mock_connect):
    """Test connection validity checking."""
    pool = DuckDBConnectionPool(
      base_path=self.base_path,
      connection_ttl_minutes=30,
    )

    now = datetime.now(timezone.utc)
    mock_conn = MagicMock()
    mock_path = Path("/tmp/test.duckdb")

    # Valid connection
    valid_conn_info = DuckDBConnectionInfo(
      connection=mock_conn,
      database_path=mock_path,
      created_at=now - timedelta(minutes=10),
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    # Expired connection
    expired_conn_info = DuckDBConnectionInfo(
      connection=mock_conn,
      database_path=mock_path,
      created_at=now - timedelta(minutes=40),  # Exceeds TTL
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    # Unhealthy connection
    unhealthy_conn_info = DuckDBConnectionInfo(
      connection=mock_conn,
      database_path=mock_path,
      created_at=now,
      last_used=now,
      use_count=1,
      is_healthy=False,
    )

    # Test validity checks
    assert pool._is_connection_valid(valid_conn_info) is True
    assert pool._is_connection_valid(expired_conn_info) is False
    assert pool._is_connection_valid(unhealthy_conn_info) is False

  @patch("duckdb.connect")
  def test_get_stats(self, mock_connect):
    """Test connection pool statistics retrieval."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=5,
      connection_ttl_minutes=30,
    )

    # Create connections for multiple databases
    with pool.get_connection("graph1"):
      pass
    with pool.get_connection("graph2"):
      pass

    stats = pool.get_stats()

    assert stats["total_connections"] == 2
    assert "database_pools" in stats
    assert "graph1" in stats["database_pools"]
    assert "graph2" in stats["database_pools"]
    assert stats["database_pools"]["graph1"]["total_connections"] == 1
    assert stats["database_pools"]["graph1"]["healthy_connections"] == 1
    assert stats["database_pools"]["graph1"]["max_connections"] == 5
    assert "stats" in stats
    assert stats["configuration"]["max_connections_per_db"] == 5
    assert stats["configuration"]["connection_ttl_minutes"] == 30

  @patch("duckdb.connect")
  def test_path_traversal_protection(self, mock_connect):
    """Test that path traversal is prevented."""
    pool = DuckDBConnectionPool(base_path=self.base_path)

    # Try malicious graph_ids
    malicious_ids = [
      "../../../etc/passwd",
      "..\\..\\..\\windows\\system32",
      "graph/../../../evil",
    ]

    for malicious_id in malicious_ids:
      with pytest.raises(ValueError, match="Invalid graph_id"):
        pool._get_database_path(malicious_id)

  def test_automatic_cleanup_is_disabled(self):
    """Test that automatic database cleanup is intentionally disabled."""
    pool = DuckDBConnectionPool(base_path=self.base_path)

    # Create an old database file
    old_db_path = Path(self.base_path) / "old_graph.duckdb"
    old_db_path.touch()

    # Make it appear old
    old_time = time.time() - (60 * 24 * 60 * 60)  # 60 days ago
    import os

    os.utime(old_db_path, (old_time, old_time))

    # Run maintenance (which no longer calls database cleanup)
    pool._maybe_run_maintenance()

    # Verify database was NOT deleted (intentional behavior)
    assert old_db_path.exists()
    assert pool._stats["databases_cleaned"] == 0

  @patch("duckdb.connect")
  def test_force_database_cleanup(self, mock_connect):
    """Test forced cleanup of a specific database."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_result.__getitem__.return_value = 1
    mock_conn.execute.return_value.fetchone.return_value = mock_result

    pool = DuckDBConnectionPool(base_path=self.base_path)

    # Create a connection and database file
    with pool.get_connection("test_graph"):
      pass

    # Verify database file exists
    db_path = Path(self.base_path) / "test_graph.duckdb"
    db_path.touch()  # Ensure file exists
    assert db_path.exists()

    # Force cleanup
    pool.force_database_cleanup("test_graph")

    # Verify database file was deleted
    assert not db_path.exists()
    assert len(pool._pools.get("test_graph", {})) == 0


class TestConnectionPoolGlobals:
  """Test global connection pool functions."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    # Reset global state
    import robosystems.graph_api.core.duckdb.pool as pool_module

    pool_module._duckdb_pool = None

  def test_initialize_duckdb_pool(self):
    """Test global DuckDB pool initialization."""
    pool = initialize_duckdb_pool(
      base_path=self.temp_dir,
      max_connections_per_db=10,
      connection_ttl_minutes=45,
    )

    assert isinstance(pool, DuckDBConnectionPool)
    assert pool.base_path == Path(self.temp_dir)
    assert pool.max_connections_per_db == 10
    assert pool.connection_ttl == timedelta(minutes=45)

  def test_get_duckdb_pool_success(self):
    """Test getting initialized connection pool."""
    # First initialize
    pool = initialize_duckdb_pool(base_path=self.temp_dir)

    # Then get
    retrieved_pool = get_duckdb_pool()

    assert retrieved_pool == pool

  def test_get_duckdb_pool_not_initialized(self):
    """Test getting connection pool when not initialized."""
    with pytest.raises(RuntimeError, match="DuckDB connection pool not initialized"):
      get_duckdb_pool()

  def test_initialize_duckdb_pool_defaults(self):
    """Test connection pool initialization with default parameters."""
    pool = initialize_duckdb_pool(base_path=self.temp_dir)

    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=30)
