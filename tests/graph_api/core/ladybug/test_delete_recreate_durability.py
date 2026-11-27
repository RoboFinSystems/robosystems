"""A database deleted and recreated under the same name keeps what is written to it.

Run against the real engine. It keeps a database open for as long as any
connection to it is, and when the last one goes it deletes the WAL at the
database's path. If the file was deleted and recreated in between, that is the
new database's WAL: its writes stay readable and vanish at the next restart.
"""

import gc
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import ladybug as lbug
import pytest

from robosystems.graph_api.core.ladybug.manager import LadybugDatabaseManager
from robosystems.graph_api.core.ladybug.pool import LadybugConnectionPool

GRAPH = "kg1_scratch"


@pytest.fixture
def base_path():
  path = Path(tempfile.mkdtemp())
  yield path
  shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def pool(base_path):
  with patch(
    "robosystems.graph_api.core.ladybug.config.get_database_memory_config",
    return_value=64,
  ):
    yield LadybugConnectionPool(base_path=str(base_path))


def _create_empty(base_path: Path) -> None:
  db = lbug.Database(str(base_path / f"{GRAPH}.lbdb"))
  lbug.Connection(db).close()
  db.close()


def _delete_files(base_path: Path) -> None:
  for path in base_path.glob(f"{GRAPH}.lbdb*"):
    path.unlink()


def _count(base_path: Path, table: str) -> int:
  db = lbug.Database(str(base_path / f"{GRAPH}.lbdb"))
  conn = lbug.Connection(db)
  try:
    return conn.execute(f"MATCH (n:{table}) RETURN count(n)").get_next()[0]
  finally:
    conn.close()
    db.close()


@pytest.mark.unit
class TestRecreatedDatabaseKeepsItsWrites:
  def test_a_connection_left_on_the_old_database_cannot_take_the_new_wal(
    self, base_path, pool
  ):
    _create_empty(base_path)
    with pool.get_connection(GRAPH) as conn:
      conn.execute("CREATE NODE TABLE Old(id STRING, PRIMARY KEY(id))")
      conn.execute("CREATE (:Old {id: 'a'})")
    # A connection the pool does not track, still open when the database goes.
    stray = lbug.Connection(pool._databases[GRAPH])
    stray.execute("RETURN 1")

    pool.close_database_connections(GRAPH)
    _delete_files(base_path)

    _create_empty(base_path)
    with pool.get_connection(GRAPH) as conn:
      conn.execute("CREATE NODE TABLE New(id STRING, PRIMARY KEY(id))")
      conn.execute("UNWIND range(1, 500) AS i CREATE (:New {id: 'n' + string(i)})")

    # The old database's last reference goes away after the new one is in use.
    del stray
    gc.collect()

    assert (base_path / f"{GRAPH}.lbdb.wal").exists()
    pool.close_database_connections(GRAPH)
    assert _count(base_path, "New") == 500

  def test_a_database_object_without_pooled_connections_is_still_closed(
    self, base_path, pool
  ):
    _create_empty(base_path)
    with pool.get_connection(GRAPH):
      pass
    database = pool._databases[GRAPH]
    for conn_id in list(pool._pools[GRAPH]):
      pool._close_connection(GRAPH, conn_id)
    del pool._pools[GRAPH]

    pool.close_database_connections(GRAPH)

    assert GRAPH not in pool._databases
    assert database.is_closed


@pytest.mark.unit
class TestLostWalIsReported:
  def _write(self, pool, statement: str) -> None:
    with pool.get_connection(GRAPH) as conn:
      conn.execute(statement)

  def test_a_wal_removed_without_a_checkpoint_is_an_error(self, base_path, pool):
    _create_empty(base_path)
    self._write(pool, "CREATE NODE TABLE T(id STRING, PRIMARY KEY(id))")
    (base_path / f"{GRAPH}.lbdb.wal").unlink()

    with patch("robosystems.graph_api.core.ladybug.pool.logger") as log:
      with pool.get_connection(GRAPH) as conn:
        conn.execute("RETURN 1")

    assert "removed without a checkpoint" in log.error.call_args.args[0]

  def test_a_checkpoint_removing_the_wal_is_not(self, base_path, pool):
    _create_empty(base_path)
    self._write(pool, "CREATE NODE TABLE T(id STRING, PRIMARY KEY(id))")

    with patch("robosystems.graph_api.core.ladybug.pool.logger") as log:
      self._write(pool, "CHECKPOINT;")
      with pool.get_connection(GRAPH) as conn:
        conn.execute("RETURN 1")

    assert not (base_path / f"{GRAPH}.lbdb.wal").exists()
    log.error.assert_not_called()


@pytest.mark.unit
class TestDeleteRemovesEverySideFile:
  """An interrupted checkpoint leaves more than a WAL beside the file, and the
  engine refuses, or absorbs, any of it found beside a new database."""

  SIDE_FILES = (".wal", ".shadow", ".wal.checkpoint", ".checkpoint.intent.lock")

  def _manager(self, base_path: Path) -> LadybugDatabaseManager:
    with patch("robosystems.graph_api.core.ladybug.manager.initialize_connection_pool"):
      return LadybugDatabaseManager(str(base_path), max_databases=5)

  def _touch(self, base_path: Path, name: str) -> list[Path]:
    paths = [base_path / f"{name}.lbdb{suffix}" for suffix in ("", *self.SIDE_FILES)]
    for path in paths:
      path.touch()
    return paths

  def test_nothing_of_the_database_is_left(self, base_path):
    manager = self._manager(base_path)
    paths = self._touch(base_path, GRAPH)

    manager._remove_graph_files(GRAPH)

    assert [p for p in paths if p.exists()] == []

  def test_its_neighbours_and_its_upgrade_copy_are_kept(self, base_path):
    manager = self._manager(base_path)
    self._touch(base_path, "kg1")
    subgraph = self._touch(base_path, GRAPH)
    upgrade_copy = base_path / "kg1.lbdb.pre-migration"
    upgrade_copy.touch()

    manager._remove_graph_files("kg1")

    assert all(p.exists() for p in subgraph)
    assert upgrade_copy.exists()
    assert not (base_path / "kg1.lbdb.shadow").exists()
