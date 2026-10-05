"""Fact grid Cypher query for the roboledger XBRL hypercube, shared by the
``build-fact-grid`` REST operation and the MCP tool.

Single-element / single-ticker filters are anchored as node-pattern
properties interpolated into the query (``(el:Element {qname: '...'})``);
``_safe_str`` / ``_is_ticker`` guard that interpolation. With an entity filter
the element, period and report predicates are applied after the MATCH instead,
so the entity's facts drive every join. Dedup, sort and ``limit`` run in
Python: dedup keeps the most precise fact, which DISTINCT cannot express, and
``ORDER BY ... LIMIT`` is not a cheap top-N in LadybugDB (it materializes and
sorts every match, which times out on large anchored patterns).
"""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy.exc import DBAPIError

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.views.view_config import DEFAULT_FACT_LIMIT
from robosystems.operations.roboledger.views.fact_dedup import (
  keep_most_precise,
  precision_rank,
)

_INVALID_SCHEMA_NAME = "3F000"

# The Graph API's buffered-result cap (``MAX_BUFFERED_ROWS``). A result this
# long is an arbitrary slice of the matches, cut before dedup and sort.
ENGINE_ROW_CAP = 10_000


class FactGridTooBroadError(ValueError):
  """The query matched more facts than one graph result returns."""

  def __init__(self) -> None:
    super().__init__(
      f"The query matched {ENGINE_ROW_CAP:,} or more facts before "
      "deduplication, more than one result returns, so any answer would be an "
      "arbitrary part of them. Narrow it: fewer entities or elements, or a "
      "tighter period filter."
    )


# Pre-compiled patterns for inline Cypher node filter sanitization.
_SAFE_STR_RE = re.compile(r"[\w:\-]+")
_TICKER_RE = re.compile(r"[A-Za-z][A-Za-z0-9.\-]{0,9}")


def is_ledger_graph(graph_id: str) -> bool:
  """A tenant's own graph: neither a shared repository nor a subgraph."""
  return not is_shared_repository_or_subgraph(graph_id) and not is_subgraph(graph_id)


def period_scope_hint(graph_id: str) -> str:
  """What a query must be scoped by, naming only what the graph can answer."""
  scopes = "periods or period_type"
  if not is_ledger_graph(graph_id):
    scopes = "periods, period_type, or fiscal_year"
  return f"Provide {scopes} to scope the query"


def shared_only_selectors(
  graph_id: str,
  *,
  canonical_concepts: list[str] | None = None,
  form: str | None = None,
  fiscal_year: int | None = None,
  fiscal_period: str | None = None,
) -> str | None:
  """Why a query cannot run on this graph as asked, or None when it can.

  Canonical concepts and a report's form and fiscal period are written by the
  shared-repository pipeline; a ledger's graph carries none of them, so each
  would match nothing and read as "no data". Refused by name instead. A
  subgraph is left alone, as :func:`query_fact_grid` leaves it: what it holds
  is whatever was written to it.
  """
  if not is_ledger_graph(graph_id):
    return None
  given = [
    name
    for name, value in (
      ("canonical_concepts", canonical_concepts),
      ("form", form),
      ("fiscal_year", fiscal_year),
      ("fiscal_period", fiscal_period),
    )
    if value not in (None, "", [])
  ]
  if not given:
    return None
  return (
    f"{', '.join(given)} "
    f"{'is' if len(given) == 1 else 'are'} only available on shared repositories "
    "(e.g. SEC): a ledger's graph has no canonical concepts, forms or fiscal "
    "periods. Select by elements (qnames such as 'rs-gaap:Assets') with periods "
    "or period_type."
  )


def _safe_str(value: str) -> str | None:
  """``value`` if safe to interpolate into an inline node filter, else ``None``
  (caller falls back to a parameterized WHERE)."""
  return value if _SAFE_STR_RE.fullmatch(value) else None


def _is_ticker(value: str) -> bool:
  """Tells a ticker from a CIK (all digits) or a company name (has spaces)."""
  return bool(_TICKER_RE.fullmatch(value))


def _build_element_match(
  elements: list[str] | None,
  canonical_concepts: list[str] | None,
  parameters: dict[str, Any],
  inline: bool = True,
) -> tuple[str, list[str]]:
  """The Element node pattern and its predicates. ``inline=False`` keeps the
  pattern bare, so every filter comes back as a predicate."""
  if elements and len(elements) == 1 and not canonical_concepts:
    safe = _safe_str(elements[0]) if inline else None
    if safe:
      return f"(el:Element {{qname: '{safe}'}})", []
    parameters["elements"] = elements
    return "(el:Element)", ["el.qname IN $elements"]

  if not elements and len(canonical_concepts or []) == 1:
    assert canonical_concepts is not None
    safe = _safe_str(canonical_concepts[0]) if inline else None
    if safe:
      return f"(el:Element {{canonical_concept: '{safe}'}})", []
    parameters["canonical_concepts"] = canonical_concepts
    return "(el:Element)", ["el.canonical_concept IN $canonical_concepts"]

  if elements and canonical_concepts:
    parameters["elements"] = elements
    parameters["canonical_concepts"] = canonical_concepts
    return (
      "(el:Element)",
      ["(el.qname IN $elements OR el.canonical_concept IN $canonical_concepts)"],
    )

  if elements:
    parameters["elements"] = elements
    return "(el:Element)", ["el.qname IN $elements"]

  if canonical_concepts:
    parameters["canonical_concepts"] = canonical_concepts
    return "(el:Element)", ["el.canonical_concept IN $canonical_concepts"]

  return "(el:Element)", []


def _build_entity_match(
  entity_list: list[str] | None,
  parameters: dict[str, Any],
) -> tuple[str | None, list[str]]:
  if not entity_list:
    return None, []

  if len(entity_list) == 1 and _is_ticker(entity_list[0]):
    safe_ticker = _safe_str(entity_list[0].upper()) or entity_list[0].upper()
    return f"(ent:Entity {{ticker: '{safe_ticker}'}})", []

  parameters["entities"] = entity_list
  return (
    "(ent:Entity)",
    ["(ent.ticker IN $entities OR ent.cik IN $entities OR ent.name IN $entities)"],
  )


# A tenant period's value comes from the report of record: a filed report,
# then the books' own close stamps (no report), then drafts, then archived.
_REPORT_STANDING = {"filed": 3, None: 2, "draft": 1, "under_review": 1, "archived": 0}


def _report_standing(graph_id: str, fact_ids: list[str]) -> dict[str, tuple]:
  """``fact_id -> (standing, last_generated)`` from the tenant's OLTP."""
  from sqlalchemy import text

  from robosystems.db.extensions import extensions_session

  with extensions_session(graph_id) as session:
    rows = session.execute(
      text("""
        SELECT f.id, r.filing_status, r.last_generated
        FROM facts f
        LEFT JOIN fact_sets fs ON fs.id = f.fact_set_id
        LEFT JOIN reports r ON r.id = fs.report_id
        WHERE f.id = ANY(:ids)
      """),
      {"ids": fact_ids},
    ).fetchall()
  return {
    row.id: (_REPORT_STANDING.get(row.filing_status, 1), str(row.last_generated or ""))
    for row in rows
  }


def _has_ledger_schema(graph_id: str) -> bool:
  """Whether the graph carries the roboledger schema, and so ``FactSet``."""
  from robosystems.database import SessionFactory
  from robosystems.middleware.extensions import load_graph_metadata

  session = SessionFactory()
  try:
    return "roboledger" in load_graph_metadata(graph_id, session).schema_extensions
  finally:
    session.close()


# A schedule fact is one schedule's planned amount, never the account's value:
# two schedules on one account would dedup to one of them, and projections
# years out would crowd the actuals. Filtered in the graph because that is the
# snapshot being read; OLTP ids change on every schedule rebuild.
_NOT_A_SCHEDULE_FACT = (
  "NOT EXISTS { MATCH (sfs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f) "
  "WHERE sfs.factset_type = 'schedule' }"
)


def _deduplicate_fact_rows(
  rows: list[dict[str, Any]], standing: dict[str, tuple] | None = None
) -> list[dict[str, Any]]:
  """Dedup on ``(element, period_start, period_end, entity, unit)``, then sort
  by ``period_end`` descending.

  Both period ends are in the key because a 10-Q reports the same element for
  the 3-month and 9-month windows ending on the same day; entity is in it so
  two filers never collapse into one row, and unit so a figure and its
  translation into another currency both survive. The most precise fact
  wins, unless ``standing`` (a tenant's report state per fact) says which
  report is of record: that decides first, and precision only breaks its ties.
  """

  def rank(row: dict[str, Any]) -> tuple:
    precision = precision_rank(row.get("decimals"))
    if standing is None:
      return (precision,)
    return (*standing.get(row.get("fact_id"), (_REPORT_STANDING[None], "")), precision)

  deduped = keep_most_precise(
    rows,
    key=lambda row: (
      row.get("element_id", ""),
      row.get("period_start", ""),
      row.get("period_end", ""),
      row.get("entity_ticker") or row.get("entity_name", ""),
      row.get("unit") or "",
    ),
    rank=rank,
  )
  deduped.sort(key=lambda r: r.get("period_end", "") or "", reverse=True)
  return deduped


async def query_fact_grid(
  graph_id: str,
  elements: list[str] | None = None,
  canonical_concepts: list[str] | None = None,
  periods: list[str] | None = None,
  entity: str | None = None,
  entities: list[str] | None = None,
  form: str | None = None,
  fiscal_year: int | None = None,
  fiscal_period: str | None = None,
  period_type: str | None = None,
  limit: int = DEFAULT_FACT_LIMIT,
) -> tuple[list[dict[str, Any]], bool]:
  """Query deduplicated facts for the roboledger XBRL hypercube.

  ``elements`` are qnames (``us-gaap:Assets``), ``canonical_concepts`` are
  canonical names (``revenue``), ``periods`` are ``YYYY-MM-DD`` end dates,
  ``entity`` accepts a ticker / CIK / name while ``entities`` takes tickers,
  and ``period_type`` is ``annual`` / ``quarterly`` / ``instant``. ``limit``
  applies after dedup + sort, so truncation keeps the most recent periods.

  Returns ``(facts, truncated)``. Each fact carries ``element_id``,
  ``element_name``, ``period_end``, ``value``, ``unit``, ``entity_ticker``
  and ``entity_name`` — entity identity always comes back, since without it
  facts from different filers are indistinguishable. Rows are deduplicated
  and sorted by ``period_end`` descending; ``truncated`` is True when
  ``limit`` dropped rows. Raises ``FactGridTooBroadError`` when the graph
  result itself was cut short.
  """
  parameters: dict[str, Any] = {}

  entity_list = entities if entities else ([entity] if entity else None)
  entity_filter, entity_where = _build_entity_match(entity_list, parameters)
  element_pattern, element_where = _build_element_match(
    elements, canonical_concepts, parameters, inline=entity_filter is None
  )

  if entity_filter:
    lead = f"{entity_filter}<-[:FACT_HAS_ENTITY]-(f:Fact)-[:FACT_HAS_ELEMENT]->{element_pattern}"
  else:
    lead = f"{element_pattern}<-[:FACT_HAS_ELEMENT]-(f:Fact)-[:FACT_HAS_ENTITY]->(ent:Entity)"

  match_parts = [
    lead,
    "(f)-[:FACT_HAS_PERIOD]->(p:Period)",
    "(f)-[:FACT_HAS_UNIT]->(u:Unit)",
  ]
  carried = ["el", "p", "f", "u", "ent"]

  fact_where = ["f.has_dimensions = false", *entity_where]
  # Predicates on the nodes around the fact: its element, period and report.
  dimension_where = [*element_where]

  tenant = is_ledger_graph(graph_id)
  if tenant and await run_off_loop(_has_ledger_schema, graph_id):
    fact_where.append(_NOT_A_SCHEDULE_FACT)

  if periods:
    dimension_where.append("p.end_date IN $periods")
    parameters["periods"] = periods

  if period_type == "instant":
    dimension_where.append("p.period_type = 'instant'")
  elif period_type == "annual":
    dimension_where.append("p.duration_type = 'annual'")
  elif period_type == "quarterly":
    dimension_where.append("p.duration_type = 'quarterly'")

  if form or fiscal_year is not None or fiscal_period:
    match_parts.append("(r:Report)-[:REPORT_HAS_FACT]->(f)")
    carried.append("r")
    if form:
      dimension_where.append("r.form = $form")
      parameters["form"] = form
    if fiscal_year is not None:
      dimension_where.append("r.fiscal_year_focus = $fiscal_year")
      parameters["fiscal_year"] = fiscal_year
    if fiscal_period:
      dimension_where.append("r.fiscal_period_focus = $fiscal_period")
      parameters["fiscal_period"] = fiscal_period

  # No DISTINCT / ORDER BY / LIMIT: dedup and sort run in Python (see module
  # docstring). Every materialized Fact has a FACT_HAS_ENTITY edge, report-owned
  # or not, so the entity join drops nothing. period_start is projected because
  # the dedup key needs it.
  return_clause = (
    "\n      RETURN\n"
    "        el.qname as element_id,\n"
    "        el.name as element_name,\n"
    "        p.start_date as period_start,\n"
    "        p.end_date as period_end,\n"
    "        p.duration_type as duration_type,\n"
    "        f.numeric_value as value,\n"
    "        f.decimals as decimals,\n"
    "        u.value as unit,\n"
    "        ent.ticker as entity_ticker,\n"
    "        ent.name as entity_name,\n"
    "        f.identifier as fact_id\n      "
  )

  # The planner orders joins by its own estimates, not by the order written: a
  # filtered Element, Period or Report inside the MATCH can be joined by
  # scanning its edges backwards across every filer. With an entity filter
  # those predicates wait until after the MATCH, so each edge extends from
  # that entity's facts.
  query = "MATCH " + ", ".join(match_parts)
  if entity_filter and dimension_where:
    query += "\nWHERE " + "\n  AND ".join(fact_where)
    query += f"\nWITH {', '.join(carried)}"
    query += "\nWHERE " + "\n  AND ".join(dimension_where)
  else:
    query += "\nWHERE " + "\n  AND ".join([*fact_where, *dimension_where])
  query += return_clause

  # "read" routes shared repos (SEC) to the replicas; tenant graphs ignore it.
  repository = await get_graph_repository(graph_id, operation_type="read")
  results = await repository.execute_query(query, parameters)

  if not results:
    return [], False
  if len(results) >= ENGINE_ROW_CAP:
    raise FactGridTooBroadError

  standing = None
  fact_ids = [row["fact_id"] for row in results if row.get("fact_id")]
  if fact_ids and tenant:
    try:
      standing = await run_off_loop(_report_standing, graph_id, fact_ids)
    except DBAPIError as exc:
      # Only a graph with no ledger schema keeps the precision rule; any other
      # failure would return wrong numbers as if nothing happened.
      if getattr(exc.orig, "pgcode", None) != _INVALID_SCHEMA_NAME:
        raise
      logger.warning(f"No ledger schema for {graph_id}; report standing skipped")
  deduped = _deduplicate_fact_rows(results, standing)
  for row in deduped:
    row.pop("fact_id", None)
  if len(deduped) > limit:
    return deduped[:limit], True
  return deduped, False
