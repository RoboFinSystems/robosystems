"""A statement run as a read cannot write, whatever the analyzer made of it.

Real engine, no mocks: the point is what the engine does inside the
transaction ``execute_statement`` opens.
"""

from unittest.mock import patch

import ladybug as lb
import pytest

from robosystems.graph_api.core.ladybug.service import execute_statement

pytestmark = pytest.mark.unit

ANALYZER = "robosystems.security.cypher_analyzer.is_write_operation"


@pytest.fixture
def conn(tmp_path):
  database = lb.Database(str(tmp_path / "t.lbdb"))
  connection = lb.Connection(database)
  connection.execute("CREATE NODE TABLE T(id INT64, x INT64, PRIMARY KEY(id))")
  connection.execute("CREATE (:T {id: 1, x: 0})")
  yield connection


def rows(result) -> list[tuple]:
  found = []
  while result.has_next():
    found.append(tuple(result.get_next()))
  return found


def state(connection) -> list[tuple]:
  return rows(connection.execute("MATCH (t:T) RETURN t.id, t.x ORDER BY t.id"))


@pytest.mark.parametrize(
  "statement",
  [
    "MATCH (t:T) SET t.x = 9",
    "CREATE (:T {id: 2, x: 0})",
    "MATCH (t:T) DELETE t",
    "MERGE (:T {id: 3})",
    "RETURN 1; MATCH (t:T) SET t.x = 9",
    "CREATE NODE TABLE U(id INT64, PRIMARY KEY(id))",
    "CHECKPOINT",
  ],
)
def test_a_write_the_analyzer_missed_is_refused_by_the_engine(conn, statement):
  with patch(ANALYZER, return_value=False):
    with pytest.raises(RuntimeError):
      execute_statement(conn, statement, None, read_only_database=False)
  assert state(conn) == [(1, 0)]


def test_a_read_returns_rows_after_its_transaction_has_ended(conn):
  result = execute_statement(
    conn, "MATCH (t:T) WHERE t.id = $id RETURN t.x", {"id": 1}, False
  )
  assert rows(result) == [(0,)]


@pytest.mark.parametrize(
  "failing",
  [
    "MATCH (t:Absent) RETURN t",
    "MATCH (t:T RETURN",
    "RETURN 1 / 0",
    "MATCH (t:T) SET t.x = 9",
  ],
)
def test_no_transaction_is_left_on_the_connection(conn, failing):
  """The connection goes back to a pool: the next request must find it clean."""
  with patch(ANALYZER, return_value=False):
    with pytest.raises(RuntimeError):
      execute_statement(conn, failing, None, False)
  execute_statement(conn, "MATCH (t:T) SET t.x = 5", None, False)
  assert state(conn) == [(1, 5)]
  assert rows(execute_statement(conn, "MATCH (t:T) RETURN t.x", None, False)) == [(5,)]


def test_a_write_runs_as_before(conn):
  execute_statement(conn, "MATCH (t:T) SET t.x = 4", None, False)
  assert state(conn) == [(1, 4)]


def test_a_read_only_database_opens_no_transaction(tmp_path, conn):
  database = lb.Database(str(tmp_path / "t.lbdb"), read_only=True)
  connection = lb.Connection(database)
  result = execute_statement(connection, "MATCH (t:T) RETURN t.id", None, True)
  assert rows(result) == [(1,)]
  with pytest.raises(RuntimeError, match="No active transaction"):
    connection.execute("COMMIT")
