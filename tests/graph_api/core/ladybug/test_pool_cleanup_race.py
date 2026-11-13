"""A cleanup and a connection being opened do not cross.

Real engine and the real pool: a cleanup that landed inside an open left the
new connection in the pool on a closed database, and every statement on that
graph failed until the health check removed it.
"""

import threading
from unittest.mock import patch

import ladybug as lb
import pytest

from robosystems.config.storage.graph import graph_db_filename
from robosystems.graph_api.core.ladybug.pool import LadybugConnectionPool

pytestmark = pytest.mark.unit

GRAPH = "cleanup"


@pytest.fixture
def pool(tmp_path):
  database = lb.Database(str(tmp_path / graph_db_filename(GRAPH)))
  connection = lb.Connection(database)
  connection.execute("CREATE NODE TABLE T(id INT64, x INT64, PRIMARY KEY(id))")
  connection.execute("CREATE (:T {id: 1, x: 0})")
  connection.close()
  database.close()
  pool = LadybugConnectionPool(base_path=str(tmp_path))
  yield pool
  pool.close_all_connections()


def count(pool) -> int:
  with pool.get_connection(GRAPH) as conn:
    result = conn.execute("MATCH (t:T) RETURN count(t)")
    found = result.get_next()[0]
    result.close()
    return found


def test_a_cleanup_waits_for_a_connection_being_opened(pool):
  opening = threading.Event()
  release = threading.Event()
  original = pool._create_new_connection

  def slow_open(database_name, read_only):
    info = original(database_name, read_only)
    opening.set()
    release.wait(5)
    return info

  with patch.object(pool, "_create_new_connection", side_effect=slow_open):
    opener = threading.Thread(target=lambda: pool._acquire_connection(GRAPH, False))
    opener.start()
    assert opening.wait(5)
    cleanup = threading.Thread(target=lambda: pool.force_database_cleanup(GRAPH))
    cleanup.start()
    cleanup.join(0.3)
    assert cleanup.is_alive()
    release.set()
    opener.join(5)
    cleanup.join(5)
  assert GRAPH not in pool._pools
  assert GRAPH not in pool._databases
  assert count(pool) == 1


def test_a_connection_left_on_a_dropped_database_is_closed_before_the_next_open(pool):
  assert count(pool) == 1
  stale = next(iter(pool._pools[GRAPH].values()))
  pool._databases.pop(GRAPH).close()

  assert count(pool) == 1
  assert stale not in pool._pools[GRAPH].values()
  assert all(
    held.database is pool._databases[GRAPH] for held in pool._pools[GRAPH].values()
  )
  with pool.get_connection(GRAPH) as conn:
    conn.execute("CREATE (:T {id: 2, x: 0})")
  assert count(pool) == 2
