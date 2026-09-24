"""Resolve a ticker's latest SEC filing by EDGAR form code, for views that run
on the shared SEC repository."""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository

ANNUAL_FORMS: tuple[str, ...] = ("10-K", "20-F", "40-F")
# Includes annual forms: foreign filers may file only annually.
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
  """The most recent filing for a ticker, or None when nothing matches.

  ``period_type`` "quarterly" or "instant" adds 10-Q to the annual forms;
  anything else means annual only.
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
    # "read" routes to the replicas; the default "write" path resolves the
    # shared master and times out the MCP tool.
    repository = await get_graph_repository(graph_id, operation_type="read")
    rows = await repository.execute_query(query, parameters)
  except Exception as e:
    # Never return None here: callers fall back to an unscoped sweep on None,
    # turning a transient failure into a confidently wrong filing.
    logger.warning(f"SEC report auto-resolve failed for {ticker}: {e}")
    raise SECReportResolutionError(
      f"Could not resolve an SEC filing for {ticker}: the report lookup failed."
    ) from e

  return rows[0] if rows else None
