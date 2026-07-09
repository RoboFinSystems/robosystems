"""Comprehensive unit tests for DuckDBConnectionPool - targeting missed coverage lines."""

import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.graph_api.core.duckdb.pool import (
  DuckDBConnectionInfo,
  DuckDBConnectionPool,
  get_duckdb_memory_override,
  get_duckdb_pool,
  initialize_duckdb_pool,
  set_duckdb_memory_override,
)


def _make_pool(temp_dir, **kwargs):
  """Helper to create a pool with defaults."""
  defaults = {
    "base_path": str(temp_dir),
    "max_connections_per_db": 3,
    "connection_ttl_minutes": 30,
  }
  defaults.update(kwargs)
  return DuckDBConnectionPool(**defaults)


@pytest.mark.unit
class TestDuckDBConnectionInfoDataclass:
  """Tests for DuckDBConnectionInfo."""

  def test_creation(self):
    """Should store all fields."""
    now = datetime.now(UTC)
    info = DuckDBConnectionInfo(
      connection=MagicMock(),
      database_path=Path("/tmp/test.duckdb"),
      created_at=now,
      last_used=now,
      use_count=1,
      is_healthy=True,
    )
    assert info.use_count == 1
    assert info.is_healthy is True


@pytest.mark.unit
class TestDuckDBConnectionPoolInit:
  """Tests for pool initialization."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_pool_init(self):
    """Should initialize with correct attributes."""
    pool = _make_pool(self.temp_dir)
    assert pool.max_connections_per_db == 3
    assert pool.connection_ttl == timedelta(minutes=30)
    assert pool._stats["connections_created"] == 0
    assert pool._stats["databases_cleaned"] == 0

  def test_pool_init_creates_directory(self):
    """Should create base directory on init."""
    new_dir = Path(self.temp_dir) / "subdir"
    _make_pool(new_dir)
    assert new_dir.exists()


@pytest.mark.unit
class TestDuckDBConnectionPoolMemoryConfig:
  """Tests for _get_duckdb_memory_limit and _get_duckdb_max_threads."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    # Clear any overrides
    set_duckdb_memory_override(None, graph_id="sec")
    set_duckdb_memory_override(None, graph_id="test")

  def test_memory_limit_uses_override(self):
    """Should use memory override when set."""
    pool = _make_pool(self.temp_dir)
    set_duckdb_memory_override("58GB", graph_id="sec")

    result = pool._get_duckdb_memory_limit("sec")
    assert result == "58GB"

  def test_memory_limit_tier_based(self):
    """Should use tier config when no override."""
    pool = _make_pool(self.temp_dir)

    with patch("robosystems.config.env") as mock_env:
      mock_env.CLUSTER_TIER = "ladybug-large"
      mock_env.DUCKDB_MEMORY_LIMIT = "2GB"

      with patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_duckdb_memory_limit",
        return_value="8GB",
      ):
        result = pool._get_duckdb_memory_limit("test")

    assert result == "8GB"

  def test_memory_limit_default_fallback(self):
    """Should fall back to env var when tier config fails."""
    pool = _make_pool(self.temp_dir)

    with patch("robosystems.config.env") as mock_env:
      mock_env.CLUSTER_TIER = None
      mock_env.DUCKDB_MEMORY_LIMIT = "4GB"

      result = pool._get_duckdb_memory_limit("test")

    assert result == "4GB"

  def test_max_threads_tier_based(self):
    """Should use tier config for thread count."""
    pool = _make_pool(self.temp_dir)

    with patch("robosystems.config.env") as mock_env:
      mock_env.CLUSTER_TIER = "ladybug-xlarge"
      mock_env.DUCKDB_MAX_THREADS = 4

      with patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_duckdb_max_threads",
        return_value=8,
      ):
        result = pool._get_duckdb_max_threads()

    assert result == 8

  def test_max_threads_default_fallback(self):
    """Should fall back to env var when tier config fails."""
    pool = _make_pool(self.temp_dir)

    with patch("robosystems.config.env") as mock_env:
      mock_env.CLUSTER_TIER = None
      mock_env.DUCKDB_MAX_THREADS = 2

      result = pool._get_duckdb_max_threads()

    assert result == 2


@pytest.mark.unit
class TestDuckDBConnectionPoolValidity:
  """Tests for connection validity checking."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_expired_connection(self):
    """Should mark expired connection as invalid."""
    pool = _make_pool(self.temp_dir, connection_ttl_minutes=1)

    info = DuckDBConnectionInfo(
      connection=MagicMock(),
      database_path=Path("/tmp/t.duckdb"),
      created_at=datetime.now(UTC) - timedelta(minutes=5),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=True,
    )

    assert pool._is_connection_valid(info) is False

  def test_healthy_connection_valid(self):
    """Should mark healthy connection as valid."""
    pool = _make_pool(self.temp_dir)

    info = DuckDBConnectionInfo(
      connection=MagicMock(),
      database_path=Path("/tmp/t.duckdb"),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=True,
    )

    assert pool._is_connection_valid(info) is True

  def test_unhealthy_connection_invalid(self):
    """Should mark unhealthy connection as invalid."""
    pool = _make_pool(self.temp_dir)

    info = DuckDBConnectionInfo(
      connection=MagicMock(),
      database_path=Path("/tmp/t.duckdb"),
      created_at=datetime.now(UTC),
      last_used=datetime.now(UTC),
      use_count=1,
      is_healthy=False,
    )

    assert pool._is_connection_valid(info) is False


@pytest.mark.unit
class TestDuckDBConnectionPoolInvalidate:
  """Tests for invalidate_connection."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_invalidate_closes_with_checkpoint(self):
    """Should checkpoint and close all connections."""
    pool = _make_pool(self.temp_dir)

    mock_conn = MagicMock()
    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=mock_conn,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    pool.invalidate_connection("g1")

    # Should have tried CHECKPOINT then close
    mock_conn.execute.assert_called_once_with("CHECKPOINT")
    mock_conn.close.assert_called_once()
    assert "g1" not in pool._pools

  def test_invalidate_nonexistent(self):
    """Should handle invalidating nonexistent database."""
    pool = _make_pool(self.temp_dir)
    pool.invalidate_connection("nonexistent")  # Should not raise


@pytest.mark.unit
class TestDuckDBConnectionPoolInterrupt:
  """Tests for interrupt_connections."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_interrupt_cancels_and_closes(self):
    """Should interrupt, close, and remove connections."""
    pool = _make_pool(self.temp_dir)

    mock_conn = MagicMock()
    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=mock_conn,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    count = pool.interrupt_connections("g1")

    assert count == 1
    mock_conn.interrupt.assert_called_once()
    mock_conn.close.assert_called_once()
    assert len(pool._pools["g1"]) == 0

  def test_interrupt_handles_errors(self):
    """Should continue even if interrupt/close fails."""
    pool = _make_pool(self.temp_dir)

    mock_conn = MagicMock()
    mock_conn.interrupt.side_effect = RuntimeError("Interrupt failed")
    mock_conn.close.side_effect = RuntimeError("Close failed")
    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=mock_conn,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    count = pool.interrupt_connections("g1")

    assert count == 1

  def test_interrupt_no_connections(self):
    """Should return 0 when no connections to interrupt."""
    pool = _make_pool(self.temp_dir)
    count = pool.interrupt_connections("nonexistent")
    assert count == 0


@pytest.mark.unit
class TestDuckDBConnectionPoolReconfigureMemory:
  """Tests for reconfigure_memory_limit."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_reconfigure_all_connections(self):
    """Should reconfigure memory on all connections."""
    pool = _make_pool(self.temp_dir)

    mock_conn1 = MagicMock()
    mock_conn2 = MagicMock()
    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=mock_conn1,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
      "c1": DuckDBConnectionInfo(
        connection=mock_conn2,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    count = pool.reconfigure_memory_limit("g1", "58GB")

    assert count == 2
    mock_conn1.execute.assert_called_once_with("SET memory_limit='58GB'")
    mock_conn2.execute.assert_called_once_with("SET memory_limit='58GB'")

  def test_reconfigure_handles_errors(self):
    """Should continue even if reconfiguration fails on some connections."""
    pool = _make_pool(self.temp_dir)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Cannot set memory")
    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=mock_conn,
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    count = pool.reconfigure_memory_limit("g1", "58GB")

    assert count == 0

  def test_reconfigure_no_connections(self):
    """Should return 0 when no connections exist."""
    pool = _make_pool(self.temp_dir)
    count = pool.reconfigure_memory_limit("nonexistent", "58GB")
    assert count == 0


@pytest.mark.unit
class TestDuckDBConnectionPoolForceCleanup:
  """Tests for force_database_cleanup."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_force_cleanup_deletes_database_file(self):
    """Should delete database file and WAL."""
    pool = _make_pool(self.temp_dir)

    # Create database and WAL files
    from robosystems.utils.path_validation import get_duckdb_staging_path

    db_path = get_duckdb_staging_path("testdb", base_path=str(self.temp_dir))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.touch()
    wal_path = db_path.with_suffix(".duckdb.wal")
    wal_path.touch()

    pool.force_database_cleanup("testdb")

    assert not db_path.exists()
    assert not wal_path.exists()

  def test_force_cleanup_nonexistent(self):
    """Should handle cleanup of nonexistent database."""
    pool = _make_pool(self.temp_dir)
    pool.force_database_cleanup("nonexistent")  # Should not raise


@pytest.mark.unit
class TestDuckDBConnectionPoolStats:
  """Tests for get_stats."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_stats_empty_pool(self):
    """Should return zero stats."""
    pool = _make_pool(self.temp_dir)
    stats = pool.get_stats()

    assert stats["total_connections"] == 0
    assert stats["total_databases_on_disk"] == 0
    assert stats["database_pools"] == {}

  def test_stats_with_connections(self):
    """Should report per-database stats."""
    pool = _make_pool(self.temp_dir)

    now = datetime.now(UTC)
    pool._pools["g1"] = {
      "c0": DuckDBConnectionInfo(
        connection=MagicMock(),
        database_path=Path("/tmp/t.duckdb"),
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
      ),
    }

    stats = pool.get_stats()

    assert stats["total_connections"] == 1
    assert stats["database_pools"]["g1"]["total_connections"] == 1
    assert stats["database_pools"]["g1"]["healthy_connections"] == 1


@pytest.mark.unit
class TestDuckDBConnectionPoolHasActive:
  """Tests for has_active_connections."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_no_active(self):
    """Should return False when no connections."""
    pool = _make_pool(self.temp_dir)
    assert pool.has_active_connections("g1") is False

  def test_has_active(self):
    """Should return True when connections exist."""
    pool = _make_pool(self.temp_dir)
    pool._pools["g1"] = {"c0": MagicMock()}
    assert pool.has_active_connections("g1") is True


@pytest.mark.unit
class TestDuckDBConnectionPoolGlobals:
  """Tests for global pool management functions."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)
    import robosystems.graph_api.core.duckdb.pool as pool_module

    pool_module._duckdb_pool = None

  def test_get_pool_uninitialized_raises(self):
    """Should raise RuntimeError when pool not initialized."""
    import robosystems.graph_api.core.duckdb.pool as pool_module

    pool_module._duckdb_pool = None

    with pytest.raises(RuntimeError, match="not initialized"):
      get_duckdb_pool()

  def test_initialize_and_get_pool(self):
    """Should initialize and return pool."""
    pool = initialize_duckdb_pool(
      base_path=str(self.temp_dir),
      max_connections_per_db=2,
    )

    assert pool is not None
    assert get_duckdb_pool() is pool


@pytest.mark.unit
class TestDuckDBMemoryOverrides:
  """Tests for memory override functions."""

  def teardown_method(self):
    set_duckdb_memory_override(None, graph_id="sec")
    set_duckdb_memory_override(None, graph_id="test")

  def test_set_and_get_override(self):
    """Should set and retrieve override."""
    set_duckdb_memory_override("58GB", graph_id="sec")
    assert get_duckdb_memory_override("sec") == "58GB"

  def test_clear_override(self):
    """Should clear override when None passed."""
    set_duckdb_memory_override("58GB", graph_id="sec")
    set_duckdb_memory_override(None, graph_id="sec")
    assert get_duckdb_memory_override("sec") is None

  def test_per_graph_isolation(self):
    """Should track overrides per graph_id independently."""
    set_duckdb_memory_override("58GB", graph_id="sec")
    set_duckdb_memory_override("10GB", graph_id="test")

    assert get_duckdb_memory_override("sec") == "58GB"
    assert get_duckdb_memory_override("test") == "10GB"
    assert get_duckdb_memory_override("other") is None

  def test_returns_previous_value(self):
    """Should return previous override value."""
    set_duckdb_memory_override("10GB", graph_id="sec")
    old = set_duckdb_memory_override("58GB", graph_id="sec")
    assert old == "10GB"

  def test_returns_none_for_first_set(self):
    """Should return None when no previous override exists."""
    old = set_duckdb_memory_override("58GB", graph_id="sec")
    assert old is None


@pytest.mark.unit
class TestReadOnlySandbox:
  """The hardened read-only tenant connection (P0 security fix)."""

  def _stage_table(self, base_dir, graph_id="kgtest001"):
    """Create a local materialized staging table at the pool's expected path."""
    import duckdb

    from robosystems.utils.path_validation import get_duckdb_staging_path

    path = get_duckdb_staging_path(graph_id, base_path=str(base_dir))
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE Element AS SELECT i AS id FROM range(2000) AS r(i)")
    con.execute("CHECKPOINT")
    con.close()
    return graph_id

  def test_local_reads_work(self, tmp_path):
    pool = _make_pool(tmp_path)
    gid = self._stage_table(tmp_path)
    with pool.get_readonly_connection(gid) as conn:
      assert conn.execute("SELECT count(*) FROM Element").fetchone()[0] == 2000

  def test_external_access_blocked(self, tmp_path):
    pool = _make_pool(tmp_path)
    gid = self._stage_table(tmp_path)
    with pool.get_readonly_connection(gid) as conn:
      for bad in [
        "SELECT read_text('/etc/hostname')",
        "SELECT * FROM read_csv('/etc/hostname')",
        "ATTACH '/tmp/x.db' AS p",
        "SET enable_external_access=true",
        "INSTALL postgres_scanner",
        "COPY Element TO '/tmp/leak.csv'",
        "CREATE TABLE evil (i INT)",
      ]:
        with pytest.raises(Exception):
          conn.execute(bad)

  def test_evicts_readwrite_connection_to_open(self, tmp_path):
    """A live read-write pool connection must not block the read-only open
    (DuckDB forbids mixed-config coexistence — the read path evicts first)."""
    pool = _make_pool(tmp_path)
    gid = self._stage_table(tmp_path)
    # Warm a read-write pool connection (external access on) for the graph.
    with pool.get_connection(gid) as rw:
      rw.execute("SELECT 1").fetchone()
    # Read-only open should succeed by evicting the cached read-write conn.
    with pool.get_readonly_connection(gid) as conn:
      assert conn.execute("SELECT count(*) FROM Element").fetchone()[0] == 2000

  def test_missing_staging_db_raises(self, tmp_path):
    pool = _make_pool(tmp_path)
    with pytest.raises(FileNotFoundError):
      with pool.get_readonly_connection("kgnostaging"):
        pass
