"""Replica warm start: read the working set the tools read before /health opens.

LadybugDB 0.22 opens a database lazily, so a shared replica enters service with
an empty buffer pool and pages every structure in from its volume on first use.
The health check stays 503 until this warmup has run (``routers/health.py``),
so what it reads is what the first callers find warm. It reads, in order and
under one wall-clock budget:

1. the first node group of every node table (the property columns);
2. for each filer in the configured ticker set, the typed tools' own query
   shapes — filing resolution, the statements, the consolidated revenue grid,
   segment facts — built by the functions the tools themselves call, so the
   warmup and the tools cannot drift apart;
3. the small and medium edge tables whole (``ENTITY_HAS_REPORT``, the
   ``STRUCTURE_*``, ``DIMENSION_*``, ``TAXONOMY_*``, ``ELEMENT_*`` and
   ``ASSOCIATION_*`` families), never the Fact-side edges;
4. the element-label lookups the resolver runs, per filer — the heaviest shape
   per filer, so last.

Steps 2-4 run only on a graph carrying the XBRL hypercube; any other database
on a replica gets step 1. A query that fails, or hits the engine's own per-query
timeout, is counted and skipped. When the budget runs out the warmup ends where
it is and the replica goes into service with what it has. Only a database whose
table discovery fails, or whose every table scan fails, is broken.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from robosystems.logger import logger

from .results import result_rows

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# The node tables the typed shapes anchor on or reach. Without all of them the
# graph is not the XBRL hypercube and the filer phases do not run.
HYPERCUBE_TABLES = frozenset(
  {"Entity", "Report", "Fact", "Element", "Period", "Unit", "Structure", "FactSet"}
)

# Edge tables read whole, smallest first, so a budget cut drops the largest.
# Both endpoints are labelled: an unlabelled endpoint makes the planner consider
# every node table.
EDGE_SCANS: tuple[tuple[str, str, str], ...] = (
  ("Entity", "ENTITY_HAS_REPORT", "Report"),
  ("Report", "REPORT_USES_TAXONOMY", "Taxonomy"),
  ("Structure", "STRUCTURE_HAS_TAXONOMY", "Taxonomy"),
  ("Report", "REPORT_HAS_FACT_SET", "FactSet"),
  ("Structure", "STRUCTURE_HAS_FACT_SET", "FactSet"),
  ("Dimension", "DIMENSION_HAS_AXIS_ELEMENT", "Element"),
  ("Dimension", "DIMENSION_HAS_MEMBER_ELEMENT", "Element"),
  ("Taxonomy", "TAXONOMY_HAS_LABEL", "Label"),
  ("Taxonomy", "TAXONOMY_HAS_REFERENCE", "Reference"),
  ("Element", "ELEMENT_HAS_REFERENCE", "Reference"),
  ("Element", "ELEMENT_HAS_LABEL", "Label"),
  ("Structure", "STRUCTURE_HAS_ASSOCIATION", "Association"),
  ("Association", "ASSOCIATION_HAS_FROM_ELEMENT", "Element"),
  ("Association", "ASSOCIATION_HAS_TO_ELEMENT", "Element"),
)

# Never scanned whole: tens of millions of rows each on the SEC graph, and
# FACT_SET_CONTAINS_FACT over a billion. The filer shapes reach them through
# one entity at a time.
FACT_SIDE_EDGES = frozenset(
  {
    "REPORT_HAS_FACT",
    "FACT_HAS_ELEMENT",
    "FACT_HAS_ENTITY",
    "FACT_HAS_PERIOD",
    "FACT_HAS_UNIT",
    "FACT_HAS_DIMENSION",
    "FACT_SET_CONTAINS_FACT",
  }
)

# (statement_type, period_type) as the statement tool asks for them; None is the
# balance sheet's instant default.
STATEMENT_SHAPES: tuple[tuple[str, str | None], ...] = (
  ("income_statement", "annual"),
  ("income_statement", "quarterly"),
  ("balance_sheet", None),
  ("cash_flow_statement", "annual"),
  ("equity_statement", "annual"),
)

REVENUE_QNAMES = (
  "us-gaap:Revenues",
  "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
)
STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"

# Mirrors the entity-anchored form of ``operations/roboledger/views/fact_query
# .query_fact_grid``: every edge extends from the filer's facts and the element
# and period predicates wait until after the MATCH.
REVENUE_GRID_QUERY = (
  "MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact)"
  "-[:FACT_HAS_ELEMENT]->(el:Element), "
  "(f)-[:FACT_HAS_PERIOD]->(p:Period), (f)-[:FACT_HAS_UNIT]->(u:Unit) "
  "WHERE f.has_dimensions = false "
  "WITH el, p, f, u, ent "
  "WHERE el.qname IN $qnames AND p.duration_type = 'annual' "
  "RETURN el.qname, p.end_date, f.numeric_value, u.value, ent.ticker"
)

# The example queries' segment breakdown, anchored on the filer.
SEGMENT_QUERY = (
  "MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]"
  "-(f:Fact {has_dimensions: true})-[:FACT_HAS_ELEMENT]->(e:Element), "
  "(f)-[:FACT_HAS_DIMENSION]->(d:Dimension) "
  "WHERE e.qname IN $qnames AND f.numeric_value IS NOT NULL "
  "RETURN d.axis_uri, d.member_uri, f.numeric_value LIMIT 50"
)

# resolve-element's label lookup and its ticker-scoped text search
# (``adapters/sec/mcp/element_resolver.py``).
LABEL_LOOKUP_QUERY = (
  "MATCH (e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label) "
  "WHERE e.qname IN $qnames AND l.type = $role "
  "RETURN e.qname AS qname, l.value AS label"
)
LABEL_SEARCH_QUERY = (
  "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label), "
  "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
  "WHERE l.value CONTAINS $search_term AND l.type = $role "
  "AND ent.ticker = $ticker AND f.has_dimensions = false "
  "RETURN DISTINCT e.qname AS qname, l.value AS label, count(f) AS fact_count "
  "ORDER BY fact_count DESC LIMIT 10"
)


@dataclass(frozen=True)
class WarmupQuery:
  phase: str
  name: str
  cypher: str
  params: dict[str, Any] = field(default_factory=dict)


@dataclass
class WarmupReport:
  """What one database's warmup did; logged, and returned for tests."""

  database: str
  ran: int = 0
  failed: int = 0
  seconds: float = 0.0
  budget_exhausted: bool = False
  stopped_in: str | None = None
  phases: dict[str, list[int]] = field(default_factory=dict)

  def count(self, phase: str, ok: bool) -> None:
    counts = self.phases.setdefault(phase, [0, 0])
    counts[0 if ok else 1] += 1
    if ok:
      self.ran += 1
    else:
      self.failed += 1


Executor = Callable[[WarmupQuery], list[Any]]


def table_scans(node_tables: Sequence[str]) -> Iterator[WarmupQuery]:
  """``RETURN n LIMIT 500`` per node table: LadybugDB is columnar, so this is
  what reads every property column's first node group."""
  for table in node_tables:
    yield WarmupQuery(
      "tables", f"properties {table}", f"MATCH (n:{table}) RETURN n LIMIT 500"
    )


def edge_scans(node_tables: set[str], rel_tables: set[str]) -> Iterator[WarmupQuery]:
  for src, rel, dst in EDGE_SCANS:
    if rel in rel_tables and src in node_tables and dst in node_tables:
      yield WarmupQuery(
        "edges", f"edges {rel}", f"MATCH (:{src})-[r:{rel}]->(:{dst}) RETURN count(r)"
      )


def filer_shapes(tickers: Sequence[str]) -> Iterator[WarmupQuery]:
  """The typed tools' shapes for each filer: what a session asks first."""
  # Imported here: the builders live in the API's operations layer, which the
  # Graph API process otherwise never loads.
  from robosystems.adapters.sec.mcp.report_resolver import build_sec_report_query
  from robosystems.operations.roboledger.views.financial_statement_query import (
    build_financial_statement_query,
  )

  qnames = list(REVENUE_QNAMES)
  for ticker in tickers:
    for period_type in ("annual", "quarterly"):
      cypher, params = build_sec_report_query(ticker=ticker, period_type=period_type)
      yield WarmupQuery("filers", f"{ticker} filing {period_type}", cypher, params)
    for statement_type, period_type in STATEMENT_SHAPES:
      cypher, params = build_financial_statement_query(
        statement_type=statement_type, ticker=ticker, period_type=period_type
      )
      yield WarmupQuery("filers", f"{ticker} {statement_type}", cypher, params)
    yield WarmupQuery(
      "filers",
      f"{ticker} revenue grid",
      REVENUE_GRID_QUERY,
      {"ticker": ticker, "qnames": qnames},
    )
    yield WarmupQuery(
      "filers",
      f"{ticker} segments",
      SEGMENT_QUERY,
      {"ticker": ticker, "qnames": qnames},
    )


def element_shapes(tickers: Sequence[str]) -> Iterator[WarmupQuery]:
  yield WarmupQuery(
    "elements",
    "labels for revenue",
    LABEL_LOOKUP_QUERY,
    {"qnames": list(REVENUE_QNAMES), "role": STANDARD_LABEL_ROLE},
  )
  for ticker in tickers:
    yield WarmupQuery(
      "elements",
      f"{ticker} label search",
      LABEL_SEARCH_QUERY,
      {"search_term": "revenue", "role": STANDARD_LABEL_ROLE, "ticker": ticker},
    )


def plan(
  node_tables: Sequence[str], rel_tables: set[str], tickers: Sequence[str]
) -> Iterator[WarmupQuery]:
  """Every query after the table scans, in the order the budget spends them."""
  tables = set(node_tables)
  hypercube = tables >= HYPERCUBE_TABLES
  if hypercube:
    yield from filer_shapes(tickers)
  yield from edge_scans(tables, rel_tables)
  if hypercube:
    yield from element_shapes(tickers)


def discover_tables(execute: Executor) -> tuple[list[str], set[str]]:
  """Node and relationship table names from the catalog. Raises when the
  catalog cannot be read: that database is broken, not cold."""
  rows = execute(WarmupQuery("discover", "show_tables", "CALL show_tables() RETURN *"))
  node_tables: list[str] = []
  rel_tables: set[str] = set()
  for row in rows:
    name, kind = _table_row(row)
    if not name or not _VALID_IDENT.match(name):
      logger.warning(f"Warmup skipping table with an unexpected name: {name!r}")
      continue
    if kind == "NODE":
      node_tables.append(name)
    elif kind == "REL":
      rel_tables.add(name)
  return node_tables, rel_tables


def _table_row(row: Any) -> tuple[str | None, str | None]:
  """``show_tables`` yields ``[id, name, type, ...]`` as a list, or a dict when
  a client has already keyed the columns."""
  if isinstance(row, dict):
    return row.get("name"), row.get("type")
  if isinstance(row, list | tuple) and len(row) >= 3:
    return (
      row[1] if isinstance(row[1], str) else None,
      row[2] if isinstance(row[2], str) else None,
    )
  return None, None


def run_warmup(
  db_manager: Any,
  database: str,
  *,
  budget_seconds: int,
  tickers: Sequence[str],
  clock: Callable[[], float] = time.monotonic,
) -> WarmupReport:
  """Warm one database through the pool's connections, so the engine's buffer
  pool fills and not only the OS cache. Raises when the database is broken."""
  started = clock()
  report = WarmupReport(database)

  def execute(query: WarmupQuery) -> list[Any]:
    with db_manager.get_connection(database, read_only=True) as conn:
      result = (
        conn.execute(query.cypher, query.params)
        if query.params
        else conn.execute(query.cypher)
      )
      results = result if isinstance(result, list) else [result]
      rows: list[Any] = []
      try:
        for one in results:
          rows.extend(result_rows(one))
      finally:
        for one in results:
          one.close()
      return rows

  def attempt(query: WarmupQuery) -> None:
    try:
      execute(query)
      report.count(query.phase, ok=True)
    except Exception as e:
      report.count(query.phase, ok=False)
      logger.warning(f"Warmup [{database}] {query.name}: {str(e)[:200]}")

  node_tables, rel_tables = discover_tables(execute)
  logger.info(
    f"Warmup [{database}] catalog: {len(node_tables)} node tables, "
    f"{len(rel_tables)} relationship tables"
  )

  for query in table_scans(node_tables):
    attempt(query)
  tables_ok, tables_failed = report.phases.get("tables", [0, 0])
  if node_tables and tables_ok == 0 and tables_failed > 0:
    raise RuntimeError(
      f"All warmup queries failed for {database} ({tables_failed} failures)"
    )
  _log_phase(report, "tables", clock() - started)

  if budget_seconds > 0:
    deadline = started + budget_seconds
    phase_started = clock()
    current_phase: str | None = None
    for query in plan(node_tables, rel_tables, tickers):
      if query.phase != current_phase:
        if current_phase is not None:
          _log_phase(report, current_phase, clock() - phase_started)
        current_phase = query.phase
        phase_started = clock()
      if clock() >= deadline:
        report.budget_exhausted = True
        report.stopped_in = query.phase
        logger.warning(
          f"Warmup [{database}] budget of {budget_seconds}s spent during "
          f"{query.phase}; serving with what is warm"
        )
        break
      attempt(query)
    if current_phase is not None and not report.budget_exhausted:
      _log_phase(report, current_phase, clock() - phase_started)

  report.seconds = clock() - started
  logger.info(
    f"Warmed up {database}: {report.ran} ok, {report.failed} failed "
    f"in {report.seconds:.1f}s"
  )
  return report


def _log_phase(report: WarmupReport, phase: str, seconds: float) -> None:
  ok, failed = report.phases.get(phase, [0, 0])
  logger.info(
    f"Warmup [{report.database}] {phase}: {ok} ok, {failed} failed, {seconds:.1f}s"
  )
