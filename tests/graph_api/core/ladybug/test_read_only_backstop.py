"""A statement run as a read cannot write, whatever the analyzer made of it.

Real engine and the real pool, no mocks: the point is what the engine does
inside the transaction, and that the transaction is nobody else's.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import ladybug as lb
import pytest

from robosystems.config.storage.graph import graph_db_filename
from robosystems.graph_api.core.ladybug.pool import LadybugConnectionPool
from robosystems.graph_api.core.ladybug.service import LadybugService

pytestmark = pytest.mark.unit

GRAPH = "backstop"
ANALYZER = "robosystems.security.cypher_analyzer.is_write_operation"


@pytest.fixture
def pool(tmp_path):
  database = lb.Database(str(tmp_path / graph_db_filename(GRAPH)))
  connection = lb.Connection(database)
  connection.execute("CREATE NODE TABLE T(id INT64, x INT64, PRIMARY KEY(id))")
  connection.execute("CREATE NODE TABLE L(id SERIAL, PRIMARY KEY(id))")
  connection.execute("CREATE SEQUENCE sq")
  connection.execute("CREATE (:T {id: 1, x: 0})")
  connection.close()
  database.close()
  pool = LadybugConnectionPool(base_path=str(tmp_path))
  yield pool
  pool.close_all_connections()


def rows(result) -> list[tuple]:
  found = []
  while result.has_next():
    found.append(tuple(result.get_next()))
  return found


def state(pool) -> list[tuple]:
  with pool.get_connection(GRAPH) as conn:
    return rows(conn.execute("MATCH (t:T) RETURN t.id, t.x ORDER BY t.id"))


@pytest.mark.parametrize(
  "statement",
  [
    "MATCH (t:T) SET t.x = 9",
    "CREATE (:T {id: 2, x: 0})",
    "MATCH (t:T) DELETE t",
    "MERGE (:T {id: 3})",
    "RETURN 1; MATCH (t:T) SET t.x = 9",
    "CREATE NODE TABLE U(id INT64, PRIMARY KEY(id))",
    "RETURN nextval('sq')",
    "CHECKPOINT",
  ],
)
def test_the_engine_refuses_a_write_inside_it(pool, statement):
  with pool.read_only_transaction(GRAPH) as conn:
    with pytest.raises(RuntimeError):
      conn.execute(statement)
  assert state(pool) == [(1, 0)]
  with pool.get_connection(GRAPH) as conn:
    # The sequence's first value: nothing drew from it.
    assert rows(conn.execute("RETURN nextval('sq')")) == [(1,)]


def test_a_read_inside_it_returns_its_rows(pool):
  with pool.read_only_transaction(GRAPH) as conn:
    result = conn.execute("MATCH (t:T) WHERE t.id = $id RETURN t.x", {"id": 1})
    assert rows(result) == [(0,)]


def test_it_leaves_the_pooled_connections_alone(pool):
  """A transaction opened on a pooled connection would be inherited."""
  with pool.get_connection(GRAPH) as pooled:
    with pool.read_only_transaction(GRAPH) as conn:
      assert conn is not pooled
      pooled.execute("MATCH (t:T) SET t.x = 4")
      with pytest.raises(RuntimeError):
        conn.execute("MATCH (t:T) SET t.x = 5")
  assert state(pool) == [(1, 4)]


def test_overlapping_reads_and_writes_do_not_disturb_each_other(pool):
  """Reads in their transactions, a writer on the pool and a missed write, all
  at once: every read and write succeeds and no missed write lands."""
  stop = time.monotonic() + 3
  failures: list[str] = []
  landed: list[str] = []
  counts = {"reads": 0, "writes": 0, "refused": 0}

  def reader():
    while time.monotonic() < stop:
      try:
        with pool.read_only_transaction(GRAPH) as conn:
          assert rows(conn.execute("MATCH (t:T) RETURN count(t)")) == [(1,)]
        counts["reads"] += 1
      except Exception as error:
        failures.append(f"read: {error}")

  def writer():
    while time.monotonic() < stop:
      try:
        with pool.get_connection(GRAPH) as conn:
          conn.execute("MATCH (t:T) WHERE t.id = 1 SET t.x = t.x + 1")
        counts["writes"] += 1
      except Exception as error:
        failures.append(f"write: {error}")

  def missed_write():
    while time.monotonic() < stop:
      for statement in ("CREATE (:L)", "MERGE (:T {id: 7})"):
        try:
          with pool.read_only_transaction(GRAPH) as conn:
            conn.execute(statement)
          landed.append(statement)
        except RuntimeError:
          counts["refused"] += 1

  threads = [
    threading.Thread(target=target)
    for target in (reader, reader, writer, missed_write, missed_write)
  ]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()

  assert failures == []
  assert landed == []
  assert min(counts.values()) > 20
  with pool.get_connection(GRAPH) as conn:
    assert rows(conn.execute("MATCH (l:L) RETURN count(l)")) == [(0,)]
    assert rows(conn.execute("MATCH (t:T) RETURN count(t)")) == [(1,)]


class TestWhichStatementsGetOne:
  """The service's choice, on both query paths."""

  def service(self, read_only: bool) -> LadybugService:
    service = LadybugService.__new__(LadybugService)
    service.read_only = read_only
    service.db_manager = MagicMock()
    return service

  def test_a_read_on_a_writer_runs_in_the_transaction(self):
    service = self.service(read_only=False)
    connection = service._statement_connection(GRAPH, "MATCH (t:T) RETURN t.id")
    assert connection is service.db_manager.read_only_transaction.return_value
    service.db_manager.get_connection.assert_not_called()

  def test_a_write_on_a_writer_runs_on_the_pool(self):
    service = self.service(read_only=False)
    connection = service._statement_connection(GRAPH, "MATCH (t:T) SET t.x = 1")
    assert connection is service.db_manager.get_connection.return_value
    service.db_manager.read_only_transaction.assert_not_called()

  def test_a_replica_is_read_only_already(self):
    service = self.service(read_only=True)
    connection = service._statement_connection(GRAPH, "MATCH (t:T) RETURN t.id")
    assert connection is service.db_manager.get_connection.return_value

  def test_what_the_analyzer_misses_still_runs_in_the_transaction(self):
    service = self.service(read_only=False)
    with patch(ANALYZER, return_value=False):
      connection = service._statement_connection(GRAPH, "MATCH (t:T) SET t.x = 1")
    assert connection is service.db_manager.read_only_transaction.return_value
