"""Financial statement analytical view — graph-backed Cypher query.

Traverses the roboledger XBRL hypercube (`Structure → FactSet → Fact`)
against LadybugDB. Works on any graph with the roboledger schema
materialized — the SEC shared repository today, any entity graph
post-materialization tomorrow.

SEC-specific knowledge (ticker auto-resolution, `10-K` / `10-Q` form
codes) lives in `adapters/sec/mcp/report_resolver.py`. This module is
purely a generic graph-schema query.
"""

from __future__ import annotations

from typing import Any

from robosystems.middleware.graph import get_graph_repository


async def query_financial_statement(
  graph_id: str,
  *,
  statement_type: str,
  report_id: str | None = None,
  ticker: str | None = None,
  period_type: str | None = None,
  limit: int = 50,
) -> list[dict[str, Any]]:
  """Run the `Fact → FactSet → Structure` traversal for a statement.

  Either ``report_id`` or ``ticker`` must be provided. **Both paths lead
  with the selective, indexed anchor and reach the globally-scoped
  ``Structure {canonical_type: ...}`` node LAST**, through facts that are
  already constrained:

  - ``report_id`` path: anchors on the single ``Report`` node, expands to
    its facts, then up to each fact's FactSet and that FactSet's Structure.
  - ``ticker`` path: anchors on the single ``Entity`` node, same expansion.

  This ordering matters. ``canonical_type`` is not indexed and ~53k
  ``Structure`` nodes share ``income_statement`` on the SEC repo (one per
  filing). Leading with ``(s:Structure {canonical_type})`` makes the
  planner scan every such structure → every FactSet → millions of facts
  before intersecting down to one report, which times out. Leading with
  the indexed anchor (``Report.identifier`` / ``Entity.ticker``) keeps the
  intermediate result set to a single filer's facts.

  ``period_type`` drives inline ``Period`` filters:

  - ``instant`` → point-in-time facts only
  - ``annual`` → duration facts with ``duration_type='annual'``
  - ``quarterly`` → duration facts with ``duration_type='quarterly'``
  - ``None`` for ``balance_sheet`` → instant (balance sheets are instant)
  - ``None`` otherwise → no period filter

  Returns a list of row dicts (raw, pre-dedup). Caller should run the
  result through ``deduplicate_facts`` and truncate to ``limit``.
  """
  if not report_id and not ticker:
    raise ValueError("Either report_id or ticker must be provided")

  parameters: dict[str, Any] = {"statement_type": statement_type}

  # Inline Period filter for planner hints.
  if period_type == "instant":
    period_props = " {period_type: 'instant'}"
  elif period_type == "annual":
    period_props = " {duration_type: 'annual'}"
  elif period_type == "quarterly":
    period_props = " {duration_type: 'quarterly'}"
  elif statement_type == "balance_sheet":
    period_props = " {period_type: 'instant'}"
  else:
    period_props = ""

  period_match = f"(f)-[:FACT_HAS_PERIOD]->(p:Period{period_props})"

  # Reach the globally-scoped Structure node LAST, through already-constrained
  # facts. The standalone Structure anchor is the FactSet→Structure tail —
  # never the scan root. See the docstring for why ordering is load-bearing.
  structure_match = (
    "(s:Structure {canonical_type: $statement_type})"
    + "-[:STRUCTURE_HAS_FACT_SET]->(fs)"
  )
  factset_match = "(fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f)"

  if report_id:
    # Anchor on the single indexed Report node.
    match_parts = [
      (
        "(r:Report {identifier: $report_id})"
        + "-[:REPORT_HAS_FACT]->(f:Fact {has_dimensions: false})"
        + "-[:FACT_HAS_ELEMENT]->(e:Element)"
      ),
      period_match,
      factset_match,
      structure_match,
    ]
    parameters["report_id"] = report_id
  else:
    # Anchor on the single indexed Entity node.
    match_parts = [
      (
        "(ent:Entity {ticker: $ticker})"
        + "<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})"
        + "-[:FACT_HAS_ELEMENT]->(e:Element)"
      ),
      period_match,
      factset_match,
      structure_match,
    ]
    parameters["ticker"] = ticker

  where_clauses = ["f.numeric_value IS NOT NULL"]

  # Fetch extra rows so the dedup step can retain the newest filing.
  fetch_limit = min(limit * 3, 1000)
  parameters["limit"] = fetch_limit

  query = (
    f"MATCH {', '.join(match_parts)} "
    f"WHERE {' AND '.join(where_clauses)} "
    "RETURN DISTINCT e.canonical_concept AS canonical_concept, e.qname AS qname, "
    "e.name AS name, f.numeric_value AS value, "
    "p.end_date AS end_date, p.period_type AS period_type, "
    "p.duration_type AS duration_type "
    "ORDER BY end_date DESC "
    "LIMIT $limit"
  )

  repository = await get_graph_repository(graph_id)
  return await repository.execute_query(query, parameters)


def deduplicate_facts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """Deduplicate facts by ``(qname, end_date)``, keeping the first occurrence.

  Results are ordered by ``end_date`` DESC at the query level, so the
  first occurrence for each ``(qname, end_date)`` pair comes from the
  most recent filing.
  """
  seen: set[tuple[str, str]] = set()
  deduped: list[dict[str, Any]] = []
  for row in rows:
    key = (row.get("qname", "") or "", row.get("end_date", "") or "")
    if key not in seen:
      seen.add(key)
      deduped.append(row)
  return deduped
