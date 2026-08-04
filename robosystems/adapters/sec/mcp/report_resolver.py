"""SEC-specific report resolution.

Auto-resolves the latest matching filing for a ticker by EDGAR form
codes. Extracted from the old `GetFinancialStatementTool` so the
generic `financial-statement-analysis` view op can stay
schema-agnostic and call this helper only when the graph is a shared
SEC repository.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository

# Forms used by domestic filers
ANNUAL_FORMS: tuple[str, ...] = ("10-K", "20-F", "40-F")
# Quarterly forms — include annual since international filers may only have annual
QUARTERLY_FORMS: tuple[str, ...] = ("10-K", "20-F", "40-F", "10-Q")


class SECReportResolutionError(RuntimeError):
  """Raised when the report lookup itself fails.

  Distinct from a ``None`` return, which means the query ran and matched
  nothing. Callers treat ``None`` as "no such filing" and may fall back to
  a broader search; they must not do that when the lookup never completed.
  """


async def resolve_sec_report(
  graph_id: str,
  *,
  ticker: str,
  period_type: str | None = None,
  fiscal_year: int | None = None,
) -> dict[str, Any] | None:
  """Resolve the most recent SEC filing for a ticker.

  ``period_type`` selects which form codes to consider:

  - ``annual`` → only ``10-K`` / ``20-F`` / ``40-F``
  - ``quarterly`` / ``instant`` → include ``10-Q`` plus annual forms
    (international filers may only have annual)
  - ``None`` → defaults to annual

  Returns a dict with ``identifier``, ``form``, ``filing_date``,
  ``fiscal_year``, ``fiscal_period`` — or ``None`` if no match.
  """
  if period_type == "annual":
    forms: tuple[str, ...] = ANNUAL_FORMS
  elif period_type in ("quarterly", "instant"):
    forms = QUARTERLY_FORMS
  else:
    forms = ANNUAL_FORMS

  where_parts = ["ent.ticker = $ticker", "r.form IN $forms"]
  parameters: dict[str, Any] = {"ticker": ticker, "forms": list(forms)}

  if fiscal_year is not None:
    where_parts.append("r.fiscal_year_focus = $fiscal_year")
    parameters["fiscal_year"] = fiscal_year

  query = (
    "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) "
    f"WHERE {' AND '.join(where_parts)} "
    "RETURN r.identifier AS identifier, "
    "r.form AS form, r.filing_date AS filing_date, "
    "r.fiscal_year_focus AS fiscal_year, r.fiscal_period_focus AS fiscal_period "
    "ORDER BY r.filing_date DESC LIMIT 1"
  )

  try:
    # Read-only resolution against the shared SEC repo — route to the replica
    # ALB via operation_type="read"; the default "write" path resolves the
    # shared master (DynamoDB discovery + retry) and times out the MCP tool.
    repository = await get_graph_repository(graph_id, operation_type="read")
    rows = await repository.execute_query(query, parameters)
  except Exception as e:
    # Do NOT collapse an infrastructure failure into "no match". Callers
    # fall back to an unscoped ticker sweep when this returns None, so
    # swallowing here turned a transient graph timeout into a confident
    # wrong answer — the caller's fiscal_year silently became "whatever is
    # newest". A failed lookup must be distinguishable from an empty one.
    logger.warning(f"SEC report auto-resolve failed for {ticker}: {e}")
    raise SECReportResolutionError(
      f"Could not resolve an SEC filing for {ticker}: the report lookup failed."
    ) from e

  return rows[0] if rows else None
