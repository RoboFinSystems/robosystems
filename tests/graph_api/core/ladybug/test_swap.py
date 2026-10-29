"""Tests for swap_database() and rollback_database() on LadybugDatabaseManager."""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.ladybug.manager import LadybugDatabaseManager


@pytest.fixture
def db_dir(tmp_path):
  """Temporary directory for LadybugDB files."""
  return tmp_path


@pytest.fixture
def manager(db_dir):
  """LadybugDatabaseManager with mocked connection pool."""
  with patch(
    "robosystems.graph_api.core.ladybug.manager.initialize_connection_pool"
  ) as mock_pool_init:
    mock_pool = MagicMock()
    mock_pool_init.return_value = mock_pool
    mgr = LadybugDatabaseManager(str(db_dir), max_databases=10)
  return mgr


class TestDatabaseExists:
  def test_exists_when_file_present(self, manager, db_dir):
    (db_dir / "kg123.lbdb").touch()
    assert manager.database_exists("kg123") is True

  def test_not_exists_when_no_file(self, manager):
    assert manager.database_exists("kg999") is False


class TestSwapDatabase:
  def test_swap_promotes_wip_to_active(self, manager, db_dir):
    """WIP becomes active, old active becomes -prev then is cleaned up."""
    # Create active and WIP files
    (db_dir / "kg123.lbdb").write_text("active")
    (db_dir / "kg123-wip.lbdb").write_text("wip")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbdb").read_text() == "wip"
    assert not (db_dir / "kg123-wip.lbdb").exists()
    assert not (db_dir / "kg123-prev.lbdb").exists()

  def test_swap_without_existing_active(self, manager, db_dir):
    """WIP becomes active even if no active file exists."""
    (db_dir / "kg123-wip.lbdb").write_text("wip")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbdb").read_text() == "wip"

  def test_swap_cleans_up_prior_prev(self, manager, db_dir):
    """Existing -prev from a previous swap is cleaned up."""
    (db_dir / "kg123.lbdb").write_text("active")
    (db_dir / "kg123-wip.lbdb").write_text("wip")
    (db_dir / "kg123-prev.lbdb").write_text("old-prev")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert not (db_dir / "kg123-prev.lbdb").exists()

  def test_swap_moves_wal_files(self, manager, db_dir):
    """WAL files are moved alongside database files."""
    (db_dir / "kg123.lbdb").write_text("active")
    (db_dir / "kg123.lbdb.wal").write_text("active-wal")
    (db_dir / "kg123-wip.lbdb").write_text("wip")
    (db_dir / "kg123-wip.lbdb.wal").write_text("wip-wal")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    # WIP WAL should have become the active WAL, then -prev cleaned up
    assert not (db_dir / "kg123-wip.lbdb.wal").exists()
    assert (db_dir / "kg123.lbdb").read_text() == "wip"
    assert not (db_dir / "kg123-prev.lbdb.wal").exists()

  def test_swap_404_when_no_wip(self, manager, db_dir):
    """Swap fails with 404 if no WIP database exists."""
    (db_dir / "kg123.lbdb").touch()

    with pytest.raises(HTTPException) as exc_info:
      manager.swap_database("kg123")
    assert exc_info.value.status_code == 404

  def test_swap_closes_connections(self, manager, db_dir):
    """Swap closes connections for both active and WIP databases."""
    (db_dir / "kg123.lbdb").touch()
    (db_dir / "kg123-wip.lbdb").touch()

    manager.swap_database("kg123")

    pool = manager.connection_pool
    calls = [str(c) for c in pool.close_database_connections.call_args_list]
    assert any("kg123" in c for c in calls)
    assert any("kg123-wip" in c for c in calls)


class TestListDatabasesFiltering:
  def test_excludes_wip_and_prev(self, manager, db_dir):
    """list_databases() should not include -wip or -prev databases."""
    (db_dir / "kg123.lbdb").touch()
    (db_dir / "kg123-wip.lbdb").touch()
    (db_dir / "kg123-prev.lbdb").touch()
    (db_dir / "kg456.lbdb").touch()

    result = manager.list_databases()

    assert "kg123" in result
    assert "kg456" in result
    assert "kg123-wip" not in result
    assert "kg123-prev" not in result
    assert len(result) == 2


class TestSwapHoldsTheDatabaseLock:
  def test_close_and_both_renames_run_under_the_database_lock(self, manager, db_dir):
    """No connection can open on the active file while it is being moved."""
    (db_dir / "kg123.lbdb").write_text("active")
    (db_dir / "kg123-wip.lbdb").write_text("wip")

    held: set[str] = set()
    events: list[tuple[str, str, bool]] = []

    @contextmanager
    def exclusive(name):
      held.add(name)
      try:
        yield
      finally:
        held.discard(name)

    manager.connection_pool.exclusive = exclusive
    manager.connection_pool.close_database_connections.side_effect = lambda name: (
      events.append(("close", name, "kg123" in held))
    )
    real_rename = Path.rename

    def rename(self, target):
      events.append(("rename", self.name, "kg123" in held))
      return real_rename(self, target)

    with patch.object(Path, "rename", rename):
      result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert ("close", "kg123", True) in events
    renames = [event for event in events if event[0] == "rename"]
    assert [name for _, name, _ in renames] == ["kg123.lbdb", "kg123-wip.lbdb"]
    assert all(under_lock for _, _, under_lock in renames)
