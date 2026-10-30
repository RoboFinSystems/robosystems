"""The replica warmup reads what the tools read, under a budget, and tells a
broken database from a cold one."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from robosystems.adapters.sec.mcp.report_resolver import build_sec_report_query
from robosystems.graph_api.core.ladybug import warmup
from robosystems.graph_api.core.ladybug.warmup import (
  EDGE_SCANS,
  FACT_SIDE_EDGES,
  HYPERCUBE_TABLES,
  STATEMENT_SHAPES,
  WarmupQuery,
  plan,
  run_warmup,
)
from robosystems.operations.roboledger.views.financial_statement_query import (
  build_financial_statement_query,
)

pytestmark = pytest.mark.unit

HYPERCUBE_CATALOG = [
  *(
    [i, name, "NODE", "main(graph)", ""]
    for i, name in enumerate(
      [
        *sorted(HYPERCUBE_TABLES),
        "Label",
        "Taxonomy",
        "Dimension",
        "Association",
        "Reference",
      ]
    )
  ),
  *(
    [100 + i, rel, "REL", "main(graph)", ""] for i, (_, rel, _) in enumerate(EDGE_SCANS)
  ),
  *([200 + i, rel, "REL", "main(graph)", ""] for i, rel in enumerate(FACT_SIDE_EDGES)),
]


class FakeResult:
  def __init__(self, rows):
    self._rows = list(rows)
    self.closed = False

  def has_next(self):
    return bool(self._rows)

  def get_next(self):
    return self._rows.pop(0)

  def close(self):
    self.closed = True


class FakeConnection:
  """Answers show_tables from a catalog and everything else with one row."""

  def __init__(self, catalog, *, fail_when=(), clock=None, seconds_per_query=0.0):
    self.catalog = catalog
    self.fail_when = tuple(fail_when)
    self.clock = clock
    self.seconds_per_query = seconds_per_query
    self.calls: list[tuple[str, dict | None]] = []
    self.results: list[FakeResult] = []

  def execute(self, cypher, params=None):
    self.calls.append((cypher, params))
    if self.clock is not None:
      self.clock.advance(self.seconds_per_query)
    if "show_tables" in cypher:
      if "show_tables" in self.fail_when:
        raise RuntimeError("catalog unreadable")
      result = FakeResult(self.catalog)
    else:
      for needle in self.fail_when:
        if needle in cypher:
          raise RuntimeError(f"engine refused: {needle}")
      result = FakeResult([[1]])
    self.results.append(result)
    return result


class FakeManager:
  def __init__(self, connection):
    self.connection = connection
    self.read_only_flags: list[bool] = []

  @contextmanager
  def get_connection(self, database, read_only=False):
    self.read_only_flags.append(read_only)
    yield self.connection


class FakeClock:
  def __init__(self):
    self.now = 1000.0

  def __call__(self):
    return self.now

  def advance(self, seconds):
    self.now += seconds


def _phases(connection: FakeConnection) -> list[str]:
  """The phase of each executed query, by shape, in execution order."""
  out = []
  for cypher, _ in connection.calls:
    if "show_tables" in cypher:
      out.append("discover")
    elif cypher.startswith("MATCH (n:") and "LIMIT 500" in cypher:
      out.append("tables")
    elif "RETURN count(r)" in cypher:
      out.append("edges")
    elif "ELEMENT_HAS_LABEL" in cypher:
      out.append("elements")
    else:
      out.append("filers")
  return out


class TestPlan:
  def test_filers_then_edges_then_elements_on_the_hypercube(self):
    queries = list(
      plan(sorted(HYPERCUBE_TABLES), {r for _, r, _ in EDGE_SCANS}, ["NVDA"])
    )
    phases = [q.phase for q in queries]
    assert phases.index("filers") < phases.index("edges") < phases.index("elements")
    assert phases == sorted(phases, key=["filers", "edges", "elements"].index)

  def test_statement_and_resolution_shapes_are_the_tools_own_text(self):
    queries = [
      (q.cypher, q.params) for q in plan(sorted(HYPERCUBE_TABLES), set(), ["AAPL"])
    ]
    for statement_type, period_type in STATEMENT_SHAPES:
      built = build_financial_statement_query(
        statement_type=statement_type, ticker="AAPL", period_type=period_type
      )
      assert built in queries
    for period_type in ("annual", "quarterly"):
      assert build_sec_report_query(ticker="AAPL", period_type=period_type) in queries

  def test_no_fact_side_edge_is_scanned_whole(self):
    tables = [
      *sorted(HYPERCUBE_TABLES),
      "Label",
      "Taxonomy",
      "Dimension",
      "Association",
      "Reference",
    ]
    rel_tables = {r for _, r, _ in EDGE_SCANS} | FACT_SIDE_EDGES
    scanned = {
      q.cypher.split("-[r:")[1].split("]")[0]
      for q in plan(tables, rel_tables, ["NVDA"])
      if q.phase == "edges"
    }
    assert scanned == {r for _, r, _ in EDGE_SCANS}
    assert not scanned & FACT_SIDE_EDGES

  def test_edge_scans_need_both_endpoints_and_the_table(self):
    queries = list(
      plan(
        ["Entity", "Report"], {"ENTITY_HAS_REPORT", "REPORT_USES_TAXONOMY"}, ["NVDA"]
      )
    )
    assert [q.name for q in queries] == ["edges ENTITY_HAS_REPORT"]

  def test_a_graph_without_the_hypercube_gets_no_filer_shapes(self):
    queries = list(
      plan(["Entity", "Report", "Transaction"], {"ENTITY_HAS_REPORT"}, ["NVDA"])
    )
    assert {q.phase for q in queries} == {"edges"}

  def test_filer_order_is_the_ticker_order(self):
    names = [
      q.name
      for q in plan(sorted(HYPERCUBE_TABLES), set(), ["MSFT", "AAPL"])
      if q.phase == "filers"
    ]
    first_aapl = next(i for i, n in enumerate(names) if n.startswith("AAPL"))
    assert all(n.startswith("MSFT") for n in names[:first_aapl])


class TestRunWarmup:
  def test_runs_every_phase_in_order_and_reports(self):
    connection = FakeConnection(HYPERCUBE_CATALOG)
    report = run_warmup(
      FakeManager(connection), "sec", budget_seconds=300, tickers=["NVDA", "AAPL"]
    )

    phases = _phases(connection)
    assert phases[0] == "discover"
    order = [p for i, p in enumerate(phases) if i == 0 or p != phases[i - 1]]
    assert order == ["discover", "tables", "filers", "edges", "elements"]
    assert report.failed == 0
    assert report.ran == len(connection.calls) - 1
    assert not report.budget_exhausted
    assert report.phases["tables"] == [
      len([r for r in HYPERCUBE_CATALOG if r[2] == "NODE"]),
      0,
    ]
    assert report.phases["edges"] == [len(EDGE_SCANS), 0]

  def test_results_are_consumed_and_closed(self):
    connection = FakeConnection(HYPERCUBE_CATALOG)
    run_warmup(FakeManager(connection), "sec", budget_seconds=300, tickers=["NVDA"])
    assert connection.results and all(r.closed for r in connection.results)
    assert all(not r.has_next() for r in connection.results)

  def test_connections_are_read_only(self):
    connection = FakeConnection(HYPERCUBE_CATALOG)
    manager = FakeManager(connection)
    run_warmup(manager, "sec", budget_seconds=60, tickers=["NVDA"])
    assert manager.read_only_flags and all(manager.read_only_flags)

  def test_budget_ends_the_warmup_where_it_is(self):
    clock = FakeClock()
    connection = FakeConnection(HYPERCUBE_CATALOG, clock=clock, seconds_per_query=10.0)
    tables = len([r for r in HYPERCUBE_CATALOG if r[2] == "NODE"])
    # Discovery and the table scans always run (10 s each here); 20 s of budget
    # then start two more queries, and the deadline is checked before each.
    report = run_warmup(
      FakeManager(connection),
      "sec",
      budget_seconds=(tables + 1) * 10 + 20,
      tickers=["NVDA"],
      clock=clock,
    )
    assert report.budget_exhausted
    assert report.stopped_in == "filers"
    assert len(connection.calls) == 1 + tables + 2
    assert report.ran == tables + 2

  def test_zero_budget_keeps_only_the_table_scans(self):
    connection = FakeConnection(HYPERCUBE_CATALOG)
    report = run_warmup(
      FakeManager(connection), "sec", budget_seconds=0, tickers=["NVDA"]
    )
    assert set(_phases(connection)) == {"discover", "tables"}
    assert report.phases.keys() == {"tables"}

  def test_a_failed_query_is_counted_and_the_rest_still_run(self):
    connection = FakeConnection(HYPERCUBE_CATALOG, fail_when=("ELEMENT_HAS_REFERENCE",))
    report = run_warmup(
      FakeManager(connection), "sec", budget_seconds=300, tickers=["NVDA"]
    )
    assert report.failed == 1
    assert report.phases["edges"] == [len(EDGE_SCANS) - 1, 1]
    assert report.phases["elements"][0] > 0

  def test_every_table_scan_failing_is_a_broken_database(self):
    connection = FakeConnection(HYPERCUBE_CATALOG, fail_when=("LIMIT 500",))
    with pytest.raises(RuntimeError, match="All warmup queries failed"):
      run_warmup(FakeManager(connection), "sec", budget_seconds=300, tickers=["NVDA"])

  def test_an_unreadable_catalog_is_a_broken_database(self):
    connection = FakeConnection(HYPERCUBE_CATALOG, fail_when=("show_tables",))
    with pytest.raises(RuntimeError, match="catalog unreadable"):
      run_warmup(FakeManager(connection), "sec", budget_seconds=300, tickers=[])

  def test_a_table_with_an_odd_name_is_skipped_not_interpolated(self):
    catalog = [
      [1, "Entity", "NODE", "main(graph)", ""],
      [2, "Bad Name) RETURN 1 //", "NODE", "main(graph)", ""],
    ]
    connection = FakeConnection(catalog)
    run_warmup(FakeManager(connection), "other", budget_seconds=60, tickers=["NVDA"])
    scanned = [c for c, _ in connection.calls if "LIMIT 500" in c]
    assert scanned == ["MATCH (n:Entity) RETURN n LIMIT 500"]

  def test_dict_catalog_rows_are_understood(self):
    catalog = [
      {"id": 1, "name": "Entity", "type": "NODE"},
      {"id": 2, "name": "ENTITY_HAS_REPORT", "type": "REL"},
    ]
    connection = FakeConnection(catalog)
    report = run_warmup(FakeManager(connection), "other", budget_seconds=60, tickers=[])
    assert report.phases["tables"] == [1, 0]


def test_warmup_query_defaults_to_no_params():
  assert WarmupQuery("tables", "x", "RETURN 1").params == {}


def test_the_module_names_every_edge_the_tools_traverse_as_fact_side():
  """The shapes reach the Fact-side edges one filer at a time; a scan of any
  of them whole would read tens of millions of rows."""
  for name in (
    "FACT_HAS_ENTITY",
    "FACT_HAS_ELEMENT",
    "FACT_HAS_PERIOD",
    "FACT_HAS_UNIT",
    "FACT_SET_CONTAINS_FACT",
    "REPORT_HAS_FACT",
  ):
    assert name in FACT_SIDE_EDGES
  assert warmup.REVENUE_GRID_QUERY.startswith("MATCH (ent:Entity {ticker: $ticker})")
