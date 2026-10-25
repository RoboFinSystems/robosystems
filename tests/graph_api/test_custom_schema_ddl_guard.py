"""Creation DDL on the Graph API runs table definitions and nothing else."""

import pytest

from robosystems.graph_api.core.ladybug.manager import LadybugDatabaseManager

CREATE_LEAK = "CREATE NODE TABLE IF NOT EXISTS Leak(line STRING, PRIMARY KEY(line))"
CREATE_ITEM = "CREATE NODE TABLE IF NOT EXISTS Item(id STRING, PRIMARY KEY(id))"
CREATE_REL = "CREATE REL TABLE IF NOT EXISTS HAS(FROM Item TO Item)"


class _RecordingConnection:
  def __init__(self) -> None:
    self.executed: list[str] = []

  def execute(self, statement: str) -> None:
    self.executed.append(statement)


def _apply(conn: _RecordingConnection, ddl: str):
  # The manager only logs on this path; no instance state is read.
  try:
    return LadybugDatabaseManager._apply_custom_schema(None, conn, ddl)  # type: ignore[arg-type]
  except ValueError:
    return False


@pytest.mark.unit
def test_table_definitions_run():
  conn = _RecordingConnection()
  assert _apply(conn, f"{CREATE_ITEM};\n{CREATE_REL}") is True
  assert conn.executed == [CREATE_ITEM, CREATE_REL]


@pytest.mark.unit
@pytest.mark.parametrize(
  "injected",
  [
    "COPY Leak FROM '/tmp/dummy.csv' (header=false)",
    "ATTACH '/tmp/other' AS other",
    "INSTALL httpfs",
    "CALL show_tables() RETURN *",
    "MATCH (n:Leak) DETACH DELETE n",
    "DROP TABLE Leak",
  ],
)
def test_any_other_statement_stops_the_schema(injected):
  conn = _RecordingConnection()
  assert _apply(conn, f"{CREATE_LEAK}; {injected}; {CREATE_ITEM}") is False
  # The definition before it ran; the injected statement and the rest did not.
  assert conn.executed == [CREATE_LEAK]
