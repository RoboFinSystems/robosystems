"""Comprehensive unit tests for LadybugConnectionPool - targeting missed coverage lines."""

import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.graph_api.core.ladybug.pool import (
  ConnectionInfo,
  LadybugConnectionPool,
  get_connection_pool,
  initialize_connection_pool,
)


def _make_pool(temp_dir, **kwargs):
  """Helper to create a pool with mocked LadybugDB."""
  defaults = {
    "base_path": str(temp_dir),
    "max_connections_per_db": 3,
    "connection_ttl_minutes": 30,
  }
  defaults.update(kwargs)
  return LadybugConnectionPool(**defaults)


@pytest.mark.unit
class TestConnectionInfoDataclass:
  """Tests for ConnectionInfo dataclass."""

  def test_default_read_only(self):
    """Should default read_only to False."""
    info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=0,
      is_healthy=True,
    )
    assert info.read_only is False

  def test_read_only_set(self):
    """Should allow setting read_only."""
    info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=0,
      is_healthy=True,
      read_only=True,
    )
    assert info.read_only is True


@pytest.mark.unit
class TestLadybugConnectionPoolInit:
  """Tests for pool initialization."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_pool_init_sets_attributes(self):
    """Should set all pool attributes correctly."""
    pool = _make_pool(self.temp_dir, connection_ttl_minutes=60)
    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=60)
    assert pool._stats["connections_created"] == 0


@pytest.mark.unit
class TestLadybugConnectionPoolAcquireRelease:
  """Tests for connection acquire/release lifecycle."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.graph_api.core.ladybug.pool.lbug")
  @patch("robosystems.graph_api.core.ladybug.config.get_database_memory_config")
  def test_acquire_creates_new_connection(self, mock_mem_config, mock_lbug):
    """Should create new connection when pool is empty."""
    mock_mem_config.return_value = 256
    mock_db = MagicMock()
    mock_lbug.Database.return_value = mock_db
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.close.return_value = None
    mock_conn.execute.return_value = mock_result
    mock_lbug.Connection.return_value = mock_conn

    # Create .lbug file for the database
    (Path(self.temp_dir) / "testdb.lbug").touch()

    pool = _make_pool(self.temp_dir)

    conn_info = pool._acquire_connection("testdb", read_only=False)

    assert conn_info is not None
    assert conn_info.use_count == 1
    assert pool._stats["connections_created"] == 1

  @patch("robosystems.graph_api.core.ladybug.pool.lbug")
  @patch("robosystems.graph_api.core.ladybug.config.get_database_memory_config")
  def test_acquire_reuses_existing_connection(self, mock_mem_config, mock_lbug):
    """Should reuse connection when one exists and is valid."""
    mock_mem_config.return_value = 256
    mock_db = MagicMock()
    mock_lbug.Database.return_value = mock_db
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.close.return_value = None
    mock_conn.execute.return_value = mock_result
    mock_lbug.Connection.return_value = mock_conn

    (Path(self.temp_dir) / "testdb.lbug").touch()

    pool = _make_pool(self.temp_dir)

    # First acquire creates
    pool._acquire_connection("testdb", read_only=False)
    # Second acquire should reuse
    conn_info2 = pool._acquire_connection("testdb", read_only=False)

    assert conn_info2.use_count == 2
    assert pool._stats["connections_reused"] == 1

  def test_release_connection(self):
    """Should release connection without error."""
    pool = _make_pool(self.temp_dir)
    mock_info = MagicMock()

    # Should not raise
    pool._release_connection("testdb", mock_info)


@pytest.mark.unit
class TestLadybugConnectionPoolValidity:
  """Tests for connection validity checking."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_connection_expired_by_ttl(self):
    """Should mark connection as invalid after TTL."""
    pool = _make_pool(self.temp_dir, connection_ttl_minutes=1)

    info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=datetime.now(UTC) - timedelta(minutes=5),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=True,
    )

    assert pool._is_connection_valid(info) is False

  def test_connection_valid_within_ttl(self):
    """Should mark connection as valid within TTL."""
    pool = _make_pool(self.temp_dir, connection_ttl_minutes=30)

    info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=True,
    )

    assert pool._is_connection_valid(info) is True

  def test_unhealthy_connection_invalid(self):
    """Should mark unhealthy connection as invalid."""
    pool = _make_pool(self.temp_dir)

    info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=False,
    )

    assert pool._is_connection_valid(info) is False


@pytest.mark.unit
class TestLadybugConnectionPoolLRU:
  """Tests for LRU (least recently used) connection selection."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_get_existing_returns_lru(self):
    """Should return least recently used connection."""
    pool = _make_pool(self.temp_dir)

    now = datetime.now(UTC)
    old_info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=now,
      last_used=now - timedelta(minutes=10),
      use_count=1,
      is_healthy=True,
      read_only=False,
    )
    new_info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=now,
      last_used=now - timedelta(minutes=1),
      use_count=5,
      is_healthy=True,
      read_only=False,
    )

    pool._pools["testdb"] = {"conn_0": old_info, "conn_1": new_info}

    result = pool._get_existing_connection("testdb", read_only=False)
    assert result is old_info

  def test_get_existing_respects_read_only(self):
    """Should only return connections matching read_only flag."""
    pool = _make_pool(self.temp_dir)

    now = datetime.now(UTC)
    rw_info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=now,
      last_used=now - timedelta(minutes=5),
      use_count=1,
      is_healthy=True,
      read_only=False,
    )
    ro_info = ConnectionInfo(
      connection=MagicMock(),
      database=MagicMock(),
      created_at=now,
      last_used=now - timedelta(minutes=10),
      use_count=1,
      is_healthy=True,
      read_only=True,
    )

    pool._pools["testdb"] = {"conn_0": rw_info, "conn_1": ro_info}

    result = pool._get_existing_connection("testdb", read_only=True)
    assert result is ro_info

  def test_get_existing_returns_none_for_empty_pool(self):
    """Should return None when no connections exist."""
    pool = _make_pool(self.temp_dir)
    assert pool._get_existing_connection("testdb", read_only=False) is None


@pytest.mark.unit
class TestLadybugConnectionPoolInvalidate:
  """Tests for invalidate_connection."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_invalidate_closes_all_connections(self):
    """Should close all connections and database for a database."""
    pool = _make_pool(self.temp_dir)

    mock_conn1 = MagicMock()
    mock_conn2 = MagicMock()
    mock_db = MagicMock()

    now = datetime.now(UTC)
    pool._pools["testdb"] = {
      "c0": ConnectionInfo(
        connection=mock_conn1,
        database=mock_db,
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
      "c1": ConnectionInfo(
        connection=mock_conn2,
        database=mock_db,
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }
    pool._databases["testdb"] = mock_db

    pool.invalidate_connection("testdb")

    mock_conn1.close.assert_called_once()
    mock_conn2.close.assert_called_once()
    mock_db.close.assert_called_once()
    assert "testdb" not in pool._pools
    assert "testdb" not in pool._databases

  def test_invalidate_nonexistent_database(self):
    """Should handle invalidating a database with no connections."""
    pool = _make_pool(self.temp_dir)

    # Should not raise
    pool.invalidate_connection("nonexistent")


@pytest.mark.unit
class TestLadybugConnectionPoolForceDatabaseCleanup:
  """Tests for force_database_cleanup."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_force_cleanup_closes_and_removes_database(self):
    """Should close connections and database object."""
    pool = _make_pool(self.temp_dir)

    mock_db = MagicMock()
    pool._databases["testdb"] = mock_db
    pool._pools["testdb"] = {}

    with patch("gc.collect"):
      pool.force_database_cleanup("testdb", aggressive=False)

    mock_db.close.assert_called_once()
    assert "testdb" not in pool._databases

  def test_force_cleanup_sec_checkpoints_first(self):
    """SEC database should attempt checkpoint before cleanup."""
    pool = _make_pool(self.temp_dir)

    mock_db = MagicMock()
    mock_db.execute = MagicMock()  # has execute
    pool._databases["sec"] = mock_db
    pool._pools["sec"] = {}

    with (
      patch("ladybug.Connection") as mock_conn_cls,
      patch("gc.collect"),
    ):
      mock_temp_conn = MagicMock()
      mock_conn_cls.return_value = mock_temp_conn

      pool.force_database_cleanup("sec", aggressive=False)

      mock_temp_conn.execute.assert_called_once_with("CHECKPOINT;")
      mock_temp_conn.close.assert_called_once()

  def test_force_cleanup_aggressive_gc(self):
    """Aggressive mode should run multiple GC rounds."""
    pool = _make_pool(self.temp_dir)

    mock_db = MagicMock()
    pool._databases["testdb"] = mock_db
    pool._pools["testdb"] = {}

    with patch("gc.collect") as mock_gc:
      pool.force_database_cleanup("testdb", aggressive=True)

      # Should call gc.collect for generations 0, 1, 2
      assert mock_gc.call_count == 3

  def test_force_cleanup_nonexistent(self):
    """Should handle cleanup of nonexistent database."""
    pool = _make_pool(self.temp_dir)

    # Should not raise
    pool.force_database_cleanup("nonexistent")


@pytest.mark.unit
class TestLadybugConnectionPoolStats:
  """Tests for get_stats."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_stats_empty_pool(self):
    """Should return zero stats for empty pool."""
    pool = _make_pool(self.temp_dir)
    stats = pool.get_stats()

    assert stats["total_connections"] == 0
    assert stats["database_pools"] == {}
    assert stats["stats"]["connections_created"] == 0

  def test_stats_with_connections(self):
    """Should report per-database stats."""
    pool = _make_pool(self.temp_dir)

    now = datetime.now(UTC)
    pool._pools["db1"] = {
      "c0": ConnectionInfo(
        connection=MagicMock(),
        database=MagicMock(),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
      "c1": ConnectionInfo(
        connection=MagicMock(),
        database=MagicMock(),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=False,
      ),
    }

    stats = pool.get_stats()

    assert stats["total_connections"] == 2
    assert stats["database_pools"]["db1"]["total_connections"] == 2
    assert stats["database_pools"]["db1"]["healthy_connections"] == 1


@pytest.mark.unit
class TestLadybugConnectionPoolGlobals:
  """Tests for global pool management functions."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    # Reset global pool
    import robosystems.graph_api.core.ladybug.pool as pool_module

    pool_module._connection_pool = None

  def test_get_pool_uninitialized_raises(self):
    """Should raise RuntimeError when pool is not initialized."""
    import robosystems.graph_api.core.ladybug.pool as pool_module

    pool_module._connection_pool = None

    with pytest.raises(RuntimeError, match="not initialized"):
      get_connection_pool()

  def test_initialize_and_get_pool(self):
    """Should initialize and return pool."""
    pool = initialize_connection_pool(
      base_path=str(self.temp_dir),
      max_connections_per_db=2,
      connection_ttl_minutes=15,
    )

    assert pool is not None
    assert get_connection_pool() is pool


@pytest.mark.unit
class TestLadybugConnectionPoolHasActiveConnections:
  """Tests for has_active_connections."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_no_active_connections(self):
    """Should return False when no connections exist."""
    pool = _make_pool(self.temp_dir)
    assert pool.has_active_connections("testdb") is False

  def test_has_active_connections(self):
    """Should return True when connections exist."""
    pool = _make_pool(self.temp_dir)
    pool._pools["testdb"] = {"c0": MagicMock()}
    assert pool.has_active_connections("testdb") is True


@pytest.mark.unit
class TestLadybugConnectionPoolListDatabases:
  """Tests for list_databases."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_list_databases_returns_known_databases(self):
    """Should return list of databases known to pool."""
    pool = _make_pool(self.temp_dir)
    pool._databases["db1"] = MagicMock()
    pool._databases["db2"] = MagicMock()

    result = pool.list_databases()
    assert sorted(result) == ["db1", "db2"]

  def test_list_databases_empty(self):
    """Should return empty list when no databases."""
    pool = _make_pool(self.temp_dir)
    assert pool.list_databases() == []


@pytest.mark.unit
class TestLadybugConnectionPoolRecreateDatabases:
  """Tests for recreate_database."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_recreate_database_delegates_to_close(self):
    """Should close database connections to force recreation."""
    pool = _make_pool(self.temp_dir)

    with patch.object(pool, "close_database_connections") as mock_close:
      pool.recreate_database("testdb")
      mock_close.assert_called_once_with("testdb")
