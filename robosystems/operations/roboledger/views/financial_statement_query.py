"""Financial statement view: the `Structure → FactSet → Fact` traversal over
any graph with the roboledger schema materialized.

SEC-specific resolution (tickers, form codes) lives in
`adapters/sec/mcp/report_resolver.py`.
"""

from __future__ import annotations

from typing import Any

from robosystems.middleware.graph import get_graph_repository
from robosystems.operations.roboledger.views.fact_dedup import keep_most_precise


async def query_financial_statement(
  graph_id: str,
  *,
  statement_type: str,
  report_id: str | None = None,
  ticker: str | None = None,
  period_type: str | None = None,
  limit: int = 50,
) -> list[dict[str, Any]]:
  """Run the statement traversal for ``report_id`` or ``ticker`` (one required).

  Both paths anchor on the indexed Report / Entity node and reach
  ``Structure {canonical_type}`` last: canonical_type is unindexed and shared
  by one structure per filing (~53k on SEC), so leading with it times out.

  ``period_type`` of ``None`` means instant for a balance sheet and no period
  filter otherwise. Rows come back raw; the caller runs ``deduplicate_facts``
  and truncates to ``limit``.
  """
  if not report_id and not ticker:
    raise ValueError("Either report_id or ticker must be provided")

  parameters: dict[str, Any] = {"statement_type": statement_type}

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

  structure_match = (
    "(s:Structure {canonical_type: $statement_type})"
    + "-[:STRUCTURE_HAS_FACT_SET]->(fs)"
  )
  factset_match = "(fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f)"

  if report_id:
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
    "p.start_date AS start_date, p.end_date AS end_date, "
    "p.period_type AS period_type, p.duration_type AS duration_type, "
    "f.decimals AS decimals "
    "ORDER BY end_date DESC "
    "LIMIT $limit"
  )

  # "read" is load-bearing on shared repos (SEC): the master's discovery path
  # outlasts the MCP tool timeout, so route to the replicas. Tenants ignore it.
  repository = await get_graph_repository(graph_id, operation_type="read")
  return await repository.execute_query(query, parameters)


def deduplicate_facts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """Deduplicate on the full period identity, keeping the most precise fact.

  Keyed on both period ends because an XBRL duration is identified by
  (start, end): Q4 and FY, or two stubs, can share an end date. ``start_date``
  is NULL for instants, whose identity is their end date.
  """
  return keep_most_precise(
    rows,
    key=lambda row: (
      row.get("qname", "") or "",
      row.get("start_date", "") or "",
      row.get("end_date", "") or "",
      row.get("period_type", "") or "",
      row.get("duration_type", "") or "",
    ),
  )
