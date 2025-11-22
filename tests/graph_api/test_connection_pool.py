"""Comprehensive tests for LadybugConnectionPool."""

import pytest
import tempfile
import shutil
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

from robosystems.graph_api.core.connection_pool import (
  LadybugConnectionPool,
  ConnectionInfo,
  initialize_connection_pool,
  get_connection_pool,
)


class TestConnectionInfo:
  """Test ConnectionInfo dataclass."""

  def test_connection_info_creation(self):
    """Test ConnectionInfo creation and attributes."""
    mock_conn = MagicMock()
    mock_db = MagicMock()
    now = datetime.now(timezone.utc)

    info = ConnectionInfo(
      connection=mock_conn,
      database=mock_db,
      created_at=now,
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    assert info.connection == mock_conn
    assert info.database == mock_db
    assert info.created_at == now
    assert info.last_used == now
    assert info.use_count == 1
    assert info.is_healthy is True


class TestLadybugConnectionPool:
  """Test LadybugConnectionPool class."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.base_path = str(self.temp_dir)

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_initialization(self):
    """Test connection pool initialization."""
    pool = LadybugConnectionPool(
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
    }
    assert pool._stats == expected_stats

  def test_initialization_defaults(self):
    """Test connection pool initialization with default parameters."""
    pool = LadybugConnectionPool(base_path=self.base_path)

    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=30)
    assert pool.health_check_interval == timedelta(minutes=5)
    assert pool.cleanup_interval == timedelta(minutes=10)

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_get_connection_context_manager(self, mock_conn_class, mock_db_class):
    """Test connection retrieval using context manager."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(base_path=self.base_path)

    with pool.get_connection("test_db", read_only=True) as conn:
      assert conn == mock_conn

    # Verify LadybugDB objects were created correctly (LadybugDB 0.11.0 uses .lbug files)
    expected_db_path = str(Path(self.base_path) / "test_db.lbug")
    # Check that database was called with expected parameters (buffer_pool_size is now included)
    mock_db_class.assert_called_once()
    call_args = mock_db_class.call_args
    assert call_args[0][0] == expected_db_path
    # CRITICAL: Database objects are always created with read_only=False to prevent
    # the bug where first read-only request locks the database in read-only mode
    assert call_args[1]["read_only"] is False
    assert "buffer_pool_size" in call_args[1]
    mock_conn_class.assert_called_once_with(mock_db)

    # Verify configuration calls and health check were performed
    # The connection pool now applies configuration settings before health check
    execute_calls = mock_conn.execute.call_args_list

    # Should have multiple configuration calls plus health check
    assert len(execute_calls) >= 6, (
      f"Expected at least 6 execute calls, got {len(execute_calls)}"
    )

    # Verify health check was the first call (before config)
    health_check_call = execute_calls[0]
    assert health_check_call[0][0] == "RETURN 1 as test"

    # Verify configuration calls were made (order may vary, so check they exist)
    call_args = [call[0][0] for call in execute_calls]
    expected_config_calls = [
      "CALL progress_bar=false;",
      "CALL timeout=120000;",
      "CALL enable_semi_mask=true;",
      "CALL warning_limit=1024;",
      "CALL spill_to_disk=true;",
    ]

    for expected_call in expected_config_calls:
      assert expected_call in call_args, (
        f"Expected config call '{expected_call}' not found in {call_args}"
      )

    mock_result.close.assert_called_once()

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_connection_reuse(self, mock_conn_class, mock_db_class):
    """Test connection reuse from pool."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(base_path=self.base_path)

    # First connection - should create new
    with pool.get_connection("test_db") as conn1:
      assert conn1 == mock_conn

    # Reset mock to verify second call behavior
    mock_db_class.reset_mock()
    mock_conn_class.reset_mock()

    # Second connection - should reuse existing
    with pool.get_connection("test_db") as conn2:
      assert conn2 == mock_conn

    # Verify database and connection were not created again
    mock_db_class.assert_not_called()
    mock_conn_class.assert_not_called()

    # Verify stats
    assert pool._stats["connections_created"] == 1
    assert pool._stats["connections_reused"] == 1

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_max_connections_per_database(self, mock_conn_class, mock_db_class):
    """Test connection limit enforcement per database."""
    # Mock LadybugDB classes to return different instances
    mock_dbs = [MagicMock() for _ in range(5)]
    mock_conns = [MagicMock() for _ in range(5)]
    mock_db_class.side_effect = mock_dbs
    mock_conn_class.side_effect = mock_conns

    # Mock successful health checks
    for mock_conn in mock_conns:
      mock_result = MagicMock()
      mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=2,
    )

    # Force creation of multiple connections by making them appear unhealthy/unusable
    # so the pool will create new ones
    connections = []

    # Create first connection
    conn_info_1 = pool._create_new_connection("test_db", read_only=True)
    connections.append(conn_info_1)

    # Create second connection (should work)
    conn_info_2 = pool._create_new_connection("test_db", read_only=True)
    connections.append(conn_info_2)

    # Verify we have 2 connections
    assert len(pool._pools["test_db"]) == 2

    # Create third connection (should trigger eviction)
    conn_info_3 = pool._create_new_connection("test_db", read_only=True)
    connections.append(conn_info_3)

    # Should still respect the limit
    assert len(pool._pools["test_db"]) <= pool.max_connections_per_db

    # At least one connection should have been closed during eviction
    close_calls = sum(1 for mock_conn in mock_conns if mock_conn.close.called)
    assert close_calls >= 1

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_connection_ttl_expiry(self, mock_conn_class, mock_db_class):
    """Test connection TTL expiry and cleanup."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(
      base_path=self.base_path,
      connection_ttl_minutes=1,  # Very short TTL for testing
    )

    # Create a connection
    with pool.get_connection("test_db") as conn:
      pass

    # Manually expire the connection by modifying its created_at time
    db_pool = pool._pools["test_db"]
    for conn_info in db_pool.values():
      conn_info.created_at = datetime.now(timezone.utc) - timedelta(minutes=2)

    # Mock new LadybugDB objects for the replacement connection
    mock_db_new = MagicMock()
    mock_conn_new = MagicMock()
    mock_db_class.return_value = mock_db_new
    mock_conn_class.return_value = mock_conn_new
    mock_conn_new.execute.return_value = mock_result

    # Force cleanup of expired connections
    pool._cleanup_expired_connections()

    # Get connection again - should create new due to TTL expiry
    with pool.get_connection("test_db") as conn:
      assert conn == mock_conn_new

    # Verify old connection was closed during cleanup
    mock_conn.close.assert_called()
    # Database is not closed until all connections are closed or invalidated

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_connection_health_check_failure(self, mock_conn_class, mock_db_class):
    """Test connection health check failure and recovery."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # First health check succeeds
    mock_result = MagicMock()
    mock_conn.execute.side_effect = [mock_result, Exception("Connection lost")]

    pool = LadybugConnectionPool(base_path=self.base_path)

    # Create a connection
    with pool.get_connection("test_db"):
      pass

    # Simulate health check failure by calling directly
    db_pool = pool._pools["test_db"]
    conn_info = list(db_pool.values())[0]
    is_healthy = pool._test_connection_health(conn_info)

    assert is_healthy is False
    assert pool._stats["health_failures"] == 1

  def test_database_lock_creation(self):
    """Test database-specific lock creation."""
    pool = LadybugConnectionPool(base_path=self.base_path)

    # Get locks for different databases
    lock1 = pool._get_database_lock("db1")
    lock2 = pool._get_database_lock("db2")
    lock1_again = pool._get_database_lock("db1")

    # Verify locks are different for different databases
    assert lock1 != lock2

    # Verify same lock is returned for same database
    assert lock1 == lock1_again

    # Verify locks are stored
    assert "db1" in pool._locks
    assert "db2" in pool._locks

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_connection_validity_check(self, mock_conn_class, mock_db_class):
    """Test connection validity checking."""
    pool = LadybugConnectionPool(
      base_path=self.base_path,
      connection_ttl_minutes=30,
    )

    now = datetime.now(timezone.utc)

    # Create connection info with different states
    mock_conn = MagicMock()
    mock_db = MagicMock()

    # Valid connection
    valid_conn_info = ConnectionInfo(
      connection=mock_conn,
      database=mock_db,
      created_at=now - timedelta(minutes=10),
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    # Expired connection
    expired_conn_info = ConnectionInfo(
      connection=mock_conn,
      database=mock_db,
      created_at=now - timedelta(minutes=40),  # Exceeds TTL
      last_used=now,
      use_count=1,
      is_healthy=True,
    )

    # Unhealthy connection
    unhealthy_conn_info = ConnectionInfo(
      connection=mock_conn,
      database=mock_db,
      created_at=now,
      last_used=now,
      use_count=1,
      is_healthy=False,
    )

    # Test validity checks
    assert pool._is_connection_valid(valid_conn_info) is True
    assert pool._is_connection_valid(expired_conn_info) is False
    assert pool._is_connection_valid(unhealthy_conn_info) is False

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_cleanup_expired_connections(self, mock_conn_class, mock_db_class):
    """Test cleanup of expired connections."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(
      base_path=self.base_path,
      connection_ttl_minutes=30,
    )

    # Create a connection
    with pool.get_connection("test_db"):
      pass

    # Manually expire the connection
    db_pool = pool._pools["test_db"]
    for conn_info in db_pool.values():
      conn_info.created_at = datetime.now(timezone.utc) - timedelta(minutes=40)

    # Run cleanup
    pool._cleanup_expired_connections()

    # Verify expired connection was closed and removed
    mock_conn.close.assert_called()
    # Database is not closed until all connections are closed or invalidated
    assert len(pool._pools.get("test_db", {})) == 0

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_maintenance_task_scheduling(self, mock_conn_class, mock_db_class):
    """Test automatic maintenance task scheduling."""
    pool = LadybugConnectionPool(
      base_path=self.base_path,
      cleanup_interval_minutes=1,  # Very short for testing
      health_check_interval_minutes=1,
    )

    # Mock maintenance methods
    pool._cleanup_expired_connections = MagicMock()
    pool._check_connection_health = MagicMock()

    # Set last maintenance times to force running
    pool._last_cleanup = datetime.now(timezone.utc) - timedelta(minutes=2)
    pool._last_health_check = datetime.now(timezone.utc) - timedelta(minutes=2)

    # Trigger maintenance
    pool._maybe_run_maintenance()

    # Verify maintenance tasks were called
    pool._cleanup_expired_connections.assert_called_once()
    pool._check_connection_health.assert_called_once()

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_get_stats(self, mock_conn_class, mock_db_class):
    """Test connection pool statistics retrieval."""
    # Mock LadybugDB classes
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db_class.return_value = mock_db
    mock_conn_class.return_value = mock_conn

    # Mock successful health check
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=5,
      connection_ttl_minutes=30,
    )

    # Create connections for multiple databases
    with pool.get_connection("db1"):
      pass
    with pool.get_connection("db2"):
      pass

    stats = pool.get_stats()

    assert stats["total_connections"] == 2
    assert "database_pools" in stats
    assert "db1" in stats["database_pools"]
    assert "db2" in stats["database_pools"]
    assert stats["database_pools"]["db1"]["total_connections"] == 1
    assert stats["database_pools"]["db1"]["healthy_connections"] == 1
    assert stats["database_pools"]["db1"]["max_connections"] == 5
    assert "stats" in stats
    assert stats["configuration"]["max_connections_per_db"] == 5
    assert stats["configuration"]["connection_ttl_minutes"] == 30

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_close_database_connections(self, mock_conn_class, mock_db_class):
    """Test closing all connections for a specific database."""
    # Mock LadybugDB classes
    mock_dbs = [MagicMock() for _ in range(3)]
    mock_conns = [MagicMock() for _ in range(3)]
    mock_db_class.side_effect = mock_dbs
    mock_conn_class.side_effect = mock_conns

    # Mock successful health checks
    for mock_conn in mock_conns:
      mock_result = MagicMock()
      mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(base_path=self.base_path)

    # Create connections for different databases
    with pool.get_connection("db1"):
      pass
    with pool.get_connection("db2"):
      pass

    # Close connections for db1 only
    pool.close_database_connections("db1")

    # Verify only db1 connections were closed
    mock_conns[0].close.assert_called_once()
    mock_dbs[0].close.assert_called_once()

    # Verify db2 connections remain
    assert "db2" in pool._pools
    assert len(pool._pools.get("db1", {})) == 0

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_close_all_connections(self, mock_conn_class, mock_db_class):
    """Test closing all connections in the pool."""
    # Mock LadybugDB classes
    mock_dbs = [MagicMock() for _ in range(2)]
    mock_conns = [MagicMock() for _ in range(2)]
    mock_db_class.side_effect = mock_dbs
    mock_conn_class.side_effect = mock_conns

    # Mock successful health checks
    for mock_conn in mock_conns:
      mock_result = MagicMock()
      mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(base_path=self.base_path)

    # Create connections
    with pool.get_connection("db1"):
      pass
    with pool.get_connection("db2"):
      pass

    # Close all connections
    pool.close_all_connections()

    # Verify all connections were closed
    for mock_conn in mock_conns:
      mock_conn.close.assert_called_once()
    for mock_db in mock_dbs:
      mock_db.close.assert_called_once()

    # Verify pools and locks were cleared
    assert len(pool._pools) == 0
    assert len(pool._locks) == 0

  @patch("real_ladybug.Database")
  def test_connection_creation_error_handling(self, mock_db_class):
    """Test error handling during connection creation."""
    # Mock LadybugDB database creation failure
    mock_db_class.side_effect = Exception("Database connection failed")

    pool = LadybugConnectionPool(base_path=self.base_path)

    # Attempting to get connection should raise the original exception
    with pytest.raises(Exception, match="Database connection failed"):
      with pool.get_connection("test_db"):
        pass

  def test_thread_safety(self):
    """Test thread safety of connection pool operations."""
    pool = LadybugConnectionPool(base_path=self.base_path)

    # This is a basic test - more comprehensive thread safety testing
    # would require complex scenarios with actual threading

    def get_lock_multiple_times():
      for i in range(10):
        lock = pool._get_database_lock(f"db_{i % 3}")
        with lock:
          time.sleep(0.001)  # Simulate work

    # Run multiple threads accessing locks
    threads = []
    for _ in range(5):
      thread = threading.Thread(target=get_lock_multiple_times)
      threads.append(thread)
      thread.start()

    for thread in threads:
      thread.join()

    # Verify locks were created correctly
    assert len(pool._locks) == 3  # db_0, db_1, db_2

  @patch("real_ladybug.Database")
  @patch("real_ladybug.Connection")
  def test_lru_connection_selection(self, mock_conn_class, mock_db_class):
    """Test least recently used connection selection."""
    # Mock LadybugDB classes
    mock_dbs = [MagicMock() for _ in range(3)]
    mock_conns = [MagicMock() for _ in range(3)]
    mock_db_class.side_effect = mock_dbs
    mock_conn_class.side_effect = mock_conns

    # Mock successful health checks
    for mock_conn in mock_conns:
      mock_result = MagicMock()
      mock_conn.execute.return_value = mock_result

    pool = LadybugConnectionPool(
      base_path=self.base_path,
      max_connections_per_db=2,
    )

    # Directly create connections to test LRU eviction
    pool._create_new_connection("test_db", read_only=True)
    time.sleep(0.01)  # Small delay to ensure different timestamps

    pool._create_new_connection("test_db", read_only=True)
    time.sleep(0.01)

    # Verify we have 2 connections at the limit
    assert len(pool._pools["test_db"]) == 2

    # Creating a third connection should evict the oldest (first one)
    pool._create_new_connection("test_db", read_only=True)

    # Should still be at the limit
    assert len(pool._pools["test_db"]) <= pool.max_connections_per_db

    # The oldest connection should have been closed
    mock_conns[0].close.assert_called_once()
    # Database is not closed - it's shared across connections


class TestConnectionPoolGlobals:
  """Test global connection pool functions."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    # Reset global state
    import robosystems.graph_api.core.connection_pool as pool_module

    pool_module._connection_pool = None

  def test_initialize_connection_pool(self):
    """Test global connection pool initialization."""
    pool = initialize_connection_pool(
      base_path=self.temp_dir,
      max_connections_per_db=10,
      connection_ttl_minutes=45,
    )

    assert isinstance(pool, LadybugConnectionPool)
    assert pool.base_path == Path(self.temp_dir)
    assert pool.max_connections_per_db == 10
    assert pool.connection_ttl == timedelta(minutes=45)

  def test_get_connection_pool_success(self):
    """Test getting initialized connection pool."""
    # First initialize
    pool = initialize_connection_pool(base_path=self.temp_dir)

    # Then get
    retrieved_pool = get_connection_pool()

    assert retrieved_pool == pool

  def test_get_connection_pool_not_initialized(self):
    """Test getting connection pool when not initialized."""
    with pytest.raises(RuntimeError, match="Connection pool not initialized"):
      get_connection_pool()

  def test_initialize_connection_pool_defaults(self):
    """Test connection pool initialization with default parameters."""
    pool = initialize_connection_pool(base_path=self.temp_dir)

    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=30)


@pytest.mark.integration
class TestConnectionPoolIntegration:
  """Integration tests for connection pool."""

  def test_connection_pool_integration_with_mocked_lbug(self):
    """Test connection pool integration with mocked LadybugDB to avoid segfaults."""
    import tempfile
    import shutil
    from unittest.mock import patch, MagicMock

    # Create temporary directory
    temp_base = tempfile.mkdtemp()
    pool = None
    try:
      # Mock LadybugDB to avoid segmentation faults
      with (
        patch("real_ladybug.Database") as mock_db_class,
        patch("real_ladybug.Connection") as mock_conn_class,
      ):
        # Setup mocks
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()

        mock_db_class.return_value = mock_db
        mock_conn_class.return_value = mock_conn
        mock_conn.execute.return_value = mock_result

        # Mock result iteration
        mock_result.has_next.side_effect = [True, False]  # One row then done
        mock_result.get_next.return_value = ["test1", "Test Node 1"]

        pool = LadybugConnectionPool(
          base_path=temp_base,
          max_connections_per_db=2,
          connection_ttl_minutes=1,
        )

        # Test 1: Create database and connection
        with pool.get_connection("test_db", read_only=False) as conn:
          # Execute queries to verify connection works
          result = conn.execute("RETURN 1 as test_value")
          assert result is not None

          # Create table
          conn.execute(
            "CREATE NODE TABLE IF NOT EXISTS TestNode (id STRING, name STRING, PRIMARY KEY (id))"
          )

          # Insert data
          conn.execute("CREATE (n:TestNode {id: 'test1', name: 'Test Node 1'})")

          # Query data
          result = conn.execute("MATCH (n:TestNode) RETURN n.id, n.name")
          assert result is not None

        # Test 2: Connection reuse
        with pool.get_connection("test_db", read_only=True) as conn:
          result = conn.execute("MATCH (n:TestNode) RETURN count(n) as node_count")
          assert result is not None

        # Test 3: Multiple databases
        with pool.get_connection("test_db2", read_only=False) as conn:
          conn.execute("RETURN 2 as test_value")

        # Verify both databases exist in the pool
        assert "test_db" in pool._pools
        assert "test_db2" in pool._pools

        # Test 4: Connection statistics
        stats = pool.get_stats()
        assert len(stats["database_pools"]) == 2
        assert stats["stats"]["connections_created"] >= 2

        # Test 5: Connection health and cleanup
        pool._cleanup_expired_connections()
        stats_after_cleanup = pool.get_stats()
        assert stats_after_cleanup["total_connections"] >= 0

        # Verify mocks were called appropriately
        assert mock_db_class.call_count >= 2  # At least 2 databases created
        assert mock_conn_class.call_count >= 2  # At least 2 connections created
        assert mock_conn.execute.call_count >= 5  # Multiple queries executed

    finally:
      # Cleanup
      try:
        if pool:
          pool.close_all_connections()
        shutil.rmtree(temp_base, ignore_errors=True)
      except Exception:
        pass  # Ignore cleanup errors in tests

  def test_high_concurrency_stress_test(self):
    """Test connection pool under high concurrency with mocked LadybugDB."""
    import threading
    import time
    import concurrent.futures
    import random
    import tempfile
    import shutil
    from unittest.mock import patch, MagicMock

    # Create temporary directory for stress test
    temp_base = tempfile.mkdtemp()
    pool = None
    try:
      # Mock LadybugDB to avoid segmentation faults
      with (
        patch("real_ladybug.Database") as mock_db_class,
        patch("real_ladybug.Connection") as mock_conn_class,
      ):
        # Setup mocks
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()

        mock_db_class.return_value = mock_db
        mock_conn_class.return_value = mock_conn
        mock_conn.execute.return_value = mock_result

        pool = LadybugConnectionPool(
          base_path=temp_base,
          max_connections_per_db=3,
          connection_ttl_minutes=2,
        )

        # Stress test parameters
        num_databases = 5
        num_threads = 15
        operations_per_thread = 10
        database_names = [f"stress_db_{i}" for i in range(num_databases)]

        # Track results
        results = {"success": 0, "errors": 0, "exceptions": []}
        results_lock = threading.Lock()

        def stress_worker(worker_id):
          """Worker function that performs random database operations."""
          for op_num in range(operations_per_thread):
            try:
              # Randomly select database and operation type
              db_name = random.choice(database_names)
              operation = random.choice(["read", "write", "query"])
              read_only = operation == "read"

              # Perform operation
              with pool.get_connection(db_name, read_only=read_only) as conn:
                if operation == "write":
                  # Create table if not exists and insert data
                  try:
                    conn.execute(f"""
                      CREATE NODE TABLE IF NOT EXISTS StressTest_{worker_id} (
                        id STRING,
                        worker_id INT64,
                        operation_num INT64,
                        timestamp STRING,
                        PRIMARY KEY (id)
                      )
                    """)

                    conn.execute(f"""
                      CREATE (n:StressTest_{worker_id} {{
                        id: '{worker_id}_{op_num}_{random.randint(1000, 9999)}',
                        worker_id: {worker_id},
                        operation_num: {op_num},
                        timestamp: '{time.time()}'
                      }})
                    """)
                  except Exception:
                    # Table creation might fail due to concurrency, that's ok
                    pass

                elif operation == "query":
                  # Query existing data
                  result = conn.execute("RETURN 1 as test_value")
                  assert result is not None

                else:  # read operation
                  # Simple read operation
                  result = conn.execute(f"RETURN '{worker_id}_{op_num}' as worker_op")
                  assert result is not None

              # Record success
              with results_lock:
                results["success"] += 1

            except Exception as e:
              with results_lock:
                results["errors"] += 1
                results["exceptions"].append(
                  f"Worker {worker_id}, Op {op_num}: {str(e)}"
                )

              # Don't fail the test on individual operation errors
              # as some concurrency conflicts are expected

        # Run stress test
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
          # Submit all worker tasks
          futures = [executor.submit(stress_worker, i) for i in range(num_threads)]

          # Wait for all to complete
          concurrent.futures.wait(futures, timeout=60)  # 1 minute timeout

        end_time = time.time()
        duration = end_time - start_time

        # Analyze results
        total_operations = num_threads * operations_per_thread
        success_rate = results["success"] / total_operations

        print("\nStress Test Results:")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Total Operations: {total_operations}")
        print(f"  Successful: {results['success']}")
        print(f"  Errors: {results['errors']}")
        print(f"  Success Rate: {success_rate:.2%}")
        print(f"  Operations/second: {total_operations / duration:.1f}")

        # Verify pool statistics
        stats = pool.get_stats()
        print("  Pool Stats:")
        print(f"    Total Databases: {len(stats['database_pools'])}")
        print(f"    Connections Created: {stats['stats']['connections_created']}")
        print(f"    Connections Reused: {stats['stats']['connections_reused']}")
        print(f"    Current Active: {stats['total_connections']}")

        # Assertions for robustness
        assert success_rate >= 0.85, (
          f"Success rate too low: {success_rate:.2%}, errors: {results['errors']}"
        )
        assert len(stats["database_pools"]) <= num_databases + 2, (
          "Too many databases created"
        )
        assert stats["stats"]["connections_created"] <= num_databases * 5, (
          "Too many connections created"
        )

        # Test pool integrity after stress
        try:
          with pool.get_connection("integrity_check", read_only=False) as conn:
            result = conn.execute("RETURN 'stress_test_complete' as status")
            assert result is not None
        except Exception as e:
          pytest.fail(f"Pool integrity compromised after stress test: {e}")

        # Verify no connection leaks
        pool._cleanup_expired_connections()
        final_stats = pool.get_stats()
        assert final_stats["total_connections"] <= num_databases * 3, (
          "Connection leak detected"
        )

    finally:
      # Cleanup
      try:
        if pool:
          pool.close_all_connections()
        shutil.rmtree(temp_base, ignore_errors=True)
      except Exception:
        pass  # Ignore cleanup errors
