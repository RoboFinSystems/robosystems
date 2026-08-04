"""Fact grid Cypher query for the roboledger XBRL hypercube.

Shared by the REST endpoint (`routers/extensions/roboledger/views.py`) and
the MCP tool (`middleware/mcp/tools/fact_grid_tool.py`). Both surfaces get
the same LadybugDB-specific optimizations:

- **Inline node anchoring** for single-element / single-ticker filters
  (e.g. ``(el:Element {qname: 'us-gaap:Assets'})``). This pins the
  traversal to specific nodes and avoids full Fact scans on large graphs.
  Parameterized node patterns are not supported by LadybugDB so string
  interpolation is required — ``_safe_str`` / ``_is_ticker`` guard the
  injection surface.
- **Inline boolean filters silently return zero results** in LadybugDB,
  so ``f.has_dimensions = false`` stays in WHERE.
- **``DISTINCT`` + ``ORDER BY`` together returns empty results**, so
  we run without either clause in Cypher and do dedup + sort in Python.
- **``ORDER BY`` + ``LIMIT`` is not a cheap top-N.** Ordering forces a
  full materialize-then-sort: over the ~269k ``us-gaap:Assets`` facts on
  the SEC repository, ``ORDER BY p.end_date DESC LIMIT 5`` times out at
  25s while a bare ``count(f)`` on the same anchored pattern returns
  promptly. ``limit`` is therefore applied in Python after dedup + sort,
  where it bounds the *payload* deterministically (most recent first).
  Bounding the *query* is the job of anchoring and the caller's filters.
"""

from __future__ import annotations

import re
from typing import Any

from robosystems.middleware.graph import get_graph_repository
from robosystems.models.api.views.view_config import DEFAULT_FACT_LIMIT

# Pre-compiled patterns for inline Cypher node filter sanitization.
_SAFE_STR_RE = re.compile(r"[\w:\-]+")
_TICKER_RE = re.compile(r"[A-Za-z][A-Za-z0-9.\-]{0,9}")


def _safe_str(value: str) -> str | None:
  """Return ``value`` if it's safe to interpolate into an inline Cypher node
  filter (only ``[a-zA-Z0-9_:-]`` characters), otherwise ``None`` so the
  caller falls back to a parameterized WHERE clause.
  """
  return value if _SAFE_STR_RE.fullmatch(value) else None


def _is_ticker(value: str) -> bool:
  """Heuristic: does ``value`` look like a stock ticker (letter-led,
  <=10 chars, alphanumeric with ``.`` or ``-``)?

  Distinguishes tickers from CIKs (all-digit) and company names
  (contain spaces). Only tickers get the inline ``{ticker: 'x'}``
  optimization; CIKs and names fall back to the parameterized WHERE.
  """
  return bool(_TICKER_RE.fullmatch(value))


def _build_element_match(
  elements: list[str] | None,
  canonical_concepts: list[str] | None,
  parameters: dict[str, Any],
) -> tuple[str, list[str]]:
  """Return the ``(el:Element …)`` match pattern and any extra WHERE
  clauses. Uses inline anchoring for single-value queries when safe.
  """
  if elements and len(elements) == 1 and not canonical_concepts:
    safe = _safe_str(elements[0])
    if safe:
      return f"(el:Element {{qname: '{safe}'}})", []
    parameters["elements"] = elements
    return "(el:Element)", ["el.qname IN $elements"]

  if not elements and len(canonical_concepts or []) == 1:
    assert canonical_concepts is not None
    safe = _safe_str(canonical_concepts[0])
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
  """Return the ``(ent:Entity …)`` match pattern (or ``None`` when no
  entity filter) plus any extra WHERE clauses.

  Single tickers get the inline anchor; CIKs, names, and multi-entity
  queries fall back to a parameterized WHERE.
  """
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


def _deduplicate_fact_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """Dedup on the full period signature, keeping the first occurrence,
  then sort by ``period_end`` DESC.

  DISTINCT + ORDER BY returns empty results in LadybugDB, so the query
  omits both and we handle dedup + sort here.

  The key is ``(element, period_start, period_end, entity)``. Both ends of
  the period are load-bearing: an XBRL duration fact is identified by
  *(start, end)*, not by end alone, so a 10-Q reports the same element for
  the 3-month AND the 9-month window ending on the same day. Keying on
  ``period_end`` alone collapsed those into one arbitrary row — the caller
  asked for a quarter and silently got year-to-date. Entity is in the key
  for the same reason: without it two filers reporting the same element for
  the same period collapse into one row.
  """
  seen: set[tuple] = set()
  deduped: list[dict[str, Any]] = []
  for row in rows:
    key = (
      row.get("element_id", ""),
      row.get("period_start", ""),
      row.get("period_end", ""),
      row.get("entity_ticker") or row.get("entity_name", ""),
    )
    if key not in seen:
      seen.add(key)
      deduped.append(row)
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

  Shared by REST (`build-fact-grid`) and MCP (`BuildFactGridTool`). See
  module docstring for the LadybugDB-specific optimizations applied here.

  Args:
      graph_id: Graph containing facts.
      elements: Element qnames (e.g., ``['us-gaap:Assets']``).
      canonical_concepts: Canonical concept names (e.g., ``['revenue']``).
      periods: Period end dates (``YYYY-MM-DD``).
      entity: Single entity ticker, CIK, or name.
      entities: Multiple entity tickers.
      form: SEC filing form type (e.g., ``'10-K'``).
      fiscal_year: Fiscal year filter.
      fiscal_period: Fiscal period filter (e.g., ``'FY'``, ``'Q1'``).
      period_type: ``'annual'``, ``'quarterly'``, or ``'instant'``.
      limit: Maximum fact records to return, applied after dedup + sort so
          truncation keeps the most recent periods.

  Returns:
      ``(facts, truncated)``. Each fact carries ``element_id``,
      ``element_name``, ``period_end``, ``value``, ``unit``,
      ``entity_ticker`` and ``entity_name`` — entity identity is always
      returned, since without it facts from different filers are
      indistinguishable. Deduplicated and sorted by ``period_end``
      descending. ``truncated`` is True when ``limit`` dropped rows.
  """
  parameters: dict[str, Any] = {}

  element_pattern, element_where = _build_element_match(
    elements, canonical_concepts, parameters
  )
  entity_list = entities if entities else ([entity] if entity else None)
  entity_filter, entity_where = _build_entity_match(entity_list, parameters)
  entity_pattern = entity_filter or "(ent:Entity)"

  # Lead the MATCH with whichever pattern is selective, never with (f:Fact) —
  # that forces a full scan of the 100M+ Fact nodes. With an entity filter the
  # planner seeds from the ~thousands of Entity nodes (a single ticker is
  # inline-anchored to one); without one it seeds from the Element, which for
  # a specific qname is a single node. Multi-entity / CIK / name comparisons
  # lost the single-ticker inline anchor and degraded to a full Fact scan
  # (25s+ timeout) before this.
  if entity_filter:
    lead = f"{entity_pattern}<-[:FACT_HAS_ENTITY]-(f:Fact)-[:FACT_HAS_ELEMENT]->{element_pattern}"
  else:
    lead = f"{element_pattern}<-[:FACT_HAS_ELEMENT]-(f:Fact)-[:FACT_HAS_ENTITY]->{entity_pattern}"

  match_parts = [
    lead,
    "(f)-[:FACT_HAS_PERIOD]->(p:Period)",
    "(f)-[:FACT_HAS_UNIT]->(u:Unit)",
  ]

  where_clauses = ["f.has_dimensions = false", *element_where, *entity_where]

  if periods:
    where_clauses.append("p.end_date IN $periods")
    parameters["periods"] = periods

  if period_type == "instant":
    where_clauses.append("p.period_type = 'instant'")
  elif period_type == "annual":
    where_clauses.append("p.duration_type = 'annual'")
  elif period_type == "quarterly":
    where_clauses.append("p.duration_type = 'quarterly'")

  if form or fiscal_year is not None or fiscal_period:
    match_parts.append("(r:Report)-[:REPORT_HAS_FACT]->(f)")
    if form:
      where_clauses.append("r.form = $form")
      parameters["form"] = form
    if fiscal_year is not None:
      where_clauses.append("r.fiscal_year_focus = $fiscal_year")
      parameters["fiscal_year"] = fiscal_year
    if fiscal_period:
      where_clauses.append("r.fiscal_period_focus = $fiscal_period")
      parameters["fiscal_period"] = fiscal_period

  # Omit DISTINCT + ORDER BY — both broken together in LadybugDB; dedup
  # and sort happen in Python below. Entity columns are always returned:
  # every Fact carries a FACT_HAS_ENTITY edge (verified on SEC — the
  # us-gaap:Assets fact count is identical with and without the traversal,
  # and materialize.py builds the edge for native and shared facts alike),
  # so the join drops nothing and the caller can always tell filers apart.
  #
  # period_start and duration_type are projected because an XBRL duration
  # fact is identified by (start, end): without start_date the dedup key
  # cannot distinguish a quarterly fact from the year-to-date fact sharing
  # its end_date, and the caller has no way to tell which one it received.
  return_clause = (
    "\n      RETURN\n"
    "        el.qname as element_id,\n"
    "        el.name as element_name,\n"
    "        p.start_date as period_start,\n"
    "        p.end_date as period_end,\n"
    "        p.duration_type as duration_type,\n"
    "        f.numeric_value as value,\n"
    "        u.value as unit,\n"
    "        ent.ticker as entity_ticker,\n"
    "        ent.name as entity_name\n      "
  )

  query = "MATCH " + ", ".join(match_parts)
  query += "\nWHERE " + "\n  AND ".join(where_clauses)
  query += return_clause

  # Read-only analytical query — route to read endpoint. On shared repos (SEC)
  # this hits the replica ALB rather than the shared master's slow discovery
  # path; on tenant graphs operation_type is ignored (single instance).
  repository = await get_graph_repository(graph_id, operation_type="read")
  results = await repository.execute_query(query, parameters)

  if not results:
    return [], False

  deduped = _deduplicate_fact_rows(results)
  if len(deduped) > limit:
    return deduped[:limit], True
  return deduped, False
