"""A schedule's planned amount is never an account's value in the fact grid.

Two depreciation schedules post to one account; one is projected years out.
The grid's own Cypher runs against a real embedded LadybugDB holding the
tables the ledger materializer writes. The schedules' OLTP rows are absent,
as they are between a schedule rebuild and the next materialize.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import ladybug as lbug
import pytest

from robosystems.operations.roboledger.views import fact_query

GRAPH = "kgdddddddddddddddd25"

SCHEMA = [
  "CREATE NODE TABLE Element(identifier STRING, qname STRING, name STRING, "
  "canonical_concept STRING, PRIMARY KEY(identifier))",
  "CREATE NODE TABLE Fact(identifier STRING, numeric_value DOUBLE, decimals STRING, "
  "has_dimensions BOOLEAN, PRIMARY KEY(identifier))",
  "CREATE NODE TABLE Period(identifier STRING, start_date STRING, end_date STRING, "
  "duration_type STRING, period_type STRING, PRIMARY KEY(identifier))",
  "CREATE NODE TABLE Unit(identifier STRING, value STRING, PRIMARY KEY(identifier))",
  "CREATE NODE TABLE Entity(identifier STRING, ticker STRING, name STRING, "
  "PRIMARY KEY(identifier))",
  "CREATE NODE TABLE FactSet(identifier STRING, factset_type STRING, "
  "PRIMARY KEY(identifier))",
  "CREATE REL TABLE FACT_HAS_ELEMENT(FROM Fact TO Element)",
  "CREATE REL TABLE FACT_HAS_ENTITY(FROM Fact TO Entity)",
  "CREATE REL TABLE FACT_HAS_PERIOD(FROM Fact TO Period)",
  "CREATE REL TABLE FACT_HAS_UNIT(FROM Fact TO Unit)",
  "CREATE REL TABLE FACT_SET_CONTAINS_FACT(FROM FactSet TO Fact)",
]

FACTS = [
  # fact id, fact set, set type, period (start, end), amount
  ("fact_close", "fs_close", "report", ("2026-07-01", "2026-07-31"), 1_471.42),
  ("fact_sched_a", "fs_sched_a", "schedule", ("2026-07-01", "2026-07-31"), 400.0),
  ("fact_sched_b", "fs_sched_b", "schedule", ("2026-07-01", "2026-07-31"), 1_071.42),
  ("fact_sched_2031", "fs_sched_a", "schedule", ("2031-01-01", "2031-01-31"), 400.0),
]


@pytest.fixture()
def graph(tmp_path):
  db = lbug.Database(str(tmp_path / "ledger.lbug"))
  conn = lbug.Connection(db)
  for statement in SCHEMA:
    conn.execute(statement)
  conn.execute(
    "CREATE (:Element {identifier: 'el_7000', qname: 'coa:7000', "
    "name: 'Depreciation', canonical_concept: ''})"
  )
  conn.execute("CREATE (:Entity {identifier: 'ent', ticker: '', name: 'Driftline'})")
  conn.execute("CREATE (:Unit {identifier: 'unit_usd', value: 'USD'})")
  for fact_id, set_id, set_type, (start, end), amount in FACTS:
    conn.execute(
      "MERGE (:Period {identifier: $p, start_date: $s, end_date: $e, "
      "duration_type: 'monthly', period_type: 'duration'})",
      {"p": f"{start}_{end}", "s": start, "e": end},
    )
    conn.execute(
      "MERGE (:FactSet {identifier: $id, factset_type: $t})",
      {"id": set_id, "t": set_type},
    )
    conn.execute(
      "CREATE (:Fact {identifier: $id, numeric_value: $v, decimals: '2', "
      "has_dimensions: false})",
      {"id": fact_id, "v": amount},
    )
    for rel, label, key in (
      ("FACT_HAS_ELEMENT", "Element", "el_7000"),
      ("FACT_HAS_ENTITY", "Entity", "ent"),
      ("FACT_HAS_PERIOD", "Period", f"{start}_{end}"),
      ("FACT_HAS_UNIT", "Unit", "unit_usd"),
    ):
      conn.execute(
        f"MATCH (f:Fact {{identifier: $f}}), (n:{label} {{identifier: $n}}) "
        f"CREATE (f)-[:{rel}]->(n)",
        {"f": fact_id, "n": key},
      )
    conn.execute(
      "MATCH (s:FactSet {identifier: $s}), (f:Fact {identifier: $f}) "
      "CREATE (s)-[:FACT_SET_CONTAINS_FACT]->(f)",
      {"s": set_id, "f": fact_id},
    )
  yield conn
  conn.close()
  db.close()


def _repository(conn):
  async def execute_query(query, parameters=None):
    result = conn.execute(query, parameters or {})
    columns = result.get_column_names()
    rows = []
    while result.has_next():
      rows.append(dict(zip(columns, result.get_next(), strict=True)))
    return rows

  repository = AsyncMock()
  repository.execute_query.side_effect = execute_query
  return repository


async def test_the_account_value_is_the_close_not_a_schedule(graph):
  with (
    patch.object(
      fact_query, "get_graph_repository", AsyncMock(return_value=_repository(graph))
    ),
    patch.object(fact_query, "_has_ledger_schema", return_value=True),
    patch.object(fact_query, "_report_standing", return_value={}),
  ):
    facts, truncated = await fact_query.query_fact_grid(GRAPH, elements=["coa:7000"])

  assert [(f["period_end"], f["value"]) for f in facts] == [("2026-07-31", 1_471.42)]
  assert truncated is False


class _PgError(Exception):
  def __init__(self, pgcode: str) -> None:
    super().__init__(pgcode)
    self.pgcode = pgcode


async def _grid_with_standing_error(graph, pgcode: str):
  from sqlalchemy.exc import OperationalError

  with (
    patch.object(
      fact_query, "get_graph_repository", AsyncMock(return_value=_repository(graph))
    ),
    patch.object(fact_query, "_has_ledger_schema", return_value=True),
    patch.object(
      fact_query,
      "_report_standing",
      side_effect=OperationalError("SELECT", {}, _PgError(pgcode)),
    ),
  ):
    return await fact_query.query_fact_grid(GRAPH, elements=["coa:7000"])


async def test_a_graph_without_a_ledger_schema_keeps_the_precision_rule(graph):
  facts, _ = await _grid_with_standing_error(graph, "3F000")
  assert [f["value"] for f in facts] == [1_471.42]


async def test_any_other_standing_failure_is_an_error_not_a_quieter_answer(graph):
  from sqlalchemy.exc import OperationalError

  with pytest.raises(OperationalError):
    await _grid_with_standing_error(graph, "57014")
