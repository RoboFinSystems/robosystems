"""Tests for swap_database() and rollback_database() on LadybugDatabaseManager."""

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
    (db_dir / "kg123.lbug").touch()
    assert manager.database_exists("kg123") is True

  def test_not_exists_when_no_file(self, manager):
    assert manager.database_exists("kg999") is False


class TestSwapDatabase:
  def test_swap_promotes_wip_to_active(self, manager, db_dir):
    """WIP becomes active, old active becomes -prev then is cleaned up."""
    # Create active and WIP files
    (db_dir / "kg123.lbug").write_text("active")
    (db_dir / "kg123-wip.lbug").write_text("wip")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbug").read_text() == "wip"
    assert not (db_dir / "kg123-wip.lbug").exists()
    assert not (db_dir / "kg123-prev.lbug").exists()

  def test_swap_without_existing_active(self, manager, db_dir):
    """WIP becomes active even if no active file exists."""
    (db_dir / "kg123-wip.lbug").write_text("wip")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbug").read_text() == "wip"

  def test_swap_cleans_up_prior_prev(self, manager, db_dir):
    """Existing -prev from a previous swap is cleaned up."""
    (db_dir / "kg123.lbug").write_text("active")
    (db_dir / "kg123-wip.lbug").write_text("wip")
    (db_dir / "kg123-prev.lbug").write_text("old-prev")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    assert not (db_dir / "kg123-prev.lbug").exists()

  def test_swap_moves_wal_files(self, manager, db_dir):
    """WAL files are moved alongside database files."""
    (db_dir / "kg123.lbug").write_text("active")
    (db_dir / "kg123.lbug.wal").write_text("active-wal")
    (db_dir / "kg123-wip.lbug").write_text("wip")
    (db_dir / "kg123-wip.lbug.wal").write_text("wip-wal")

    result = manager.swap_database("kg123")

    assert result["status"] == "success"
    # WIP WAL should have become the active WAL
    assert not (db_dir / "kg123-wip.lbug.wal").exists()

  def test_swap_404_when_no_wip(self, manager, db_dir):
    """Swap fails with 404 if no WIP database exists."""
    (db_dir / "kg123.lbug").touch()

    with pytest.raises(HTTPException) as exc_info:
      manager.swap_database("kg123")
    assert exc_info.value.status_code == 404

  def test_swap_closes_connections(self, manager, db_dir):
    """Swap closes connections for both active and WIP databases."""
    (db_dir / "kg123.lbug").touch()
    (db_dir / "kg123-wip.lbug").touch()

    manager.swap_database("kg123")

    pool = manager.connection_pool
    calls = [str(c) for c in pool.close_database_connections.call_args_list]
    assert any("kg123" in c for c in calls)
    assert any("kg123-wip" in c for c in calls)


class TestRollbackDatabase:
  def test_rollback_restores_prev_to_active(self, manager, db_dir):
    """Rollback restores -prev to active."""
    (db_dir / "kg123.lbug").write_text("bad-active")
    (db_dir / "kg123-prev.lbug").write_text("good-prev")

    result = manager.rollback_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbug").read_text() == "good-prev"
    assert not (db_dir / "kg123-prev.lbug").exists()

  def test_rollback_404_when_no_prev(self, manager, db_dir):
    """Rollback fails with 404 if no -prev database exists."""
    (db_dir / "kg123.lbug").touch()

    with pytest.raises(HTTPException) as exc_info:
      manager.rollback_database("kg123")
    assert exc_info.value.status_code == 404

  def test_rollback_works_without_active(self, manager, db_dir):
    """-prev can be restored even if active was deleted."""
    (db_dir / "kg123-prev.lbug").write_text("good-prev")

    result = manager.rollback_database("kg123")

    assert result["status"] == "success"
    assert (db_dir / "kg123.lbug").read_text() == "good-prev"
