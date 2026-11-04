"""Resolve a ticker's latest SEC filing by EDGAR form code, for views that run
on the shared SEC repository."""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any

from robosystems.config import env
from robosystems.config.storage.shared import get_filing_catalog_key
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
from robosystems.operations.aws.s3 import S3Client

ANNUAL_FORMS: tuple[str, ...] = ("10-K", "20-F", "40-F")
# Includes annual forms: foreign filers may file only annually.
QUARTERLY_FORMS: tuple[str, ...] = ("10-K", "20-F", "40-F", "10-Q")


SEC_GRAPH_ID = "sec"
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-/]{0,15}$")
_RENAMED_TTL_SECONDS = 300
_RENAMED_CACHE_LIMIT = 4096
_renamed_cache: dict[str, tuple[float, str | None]] = {}


def renamed_filer_cik_sync(ticker: str) -> str | None:
  """The CIK of a filer the catalog says changed ticker, asked for by either one.

  None for every other ticker, including when the catalog cannot be read, so
  those keep matching ``Entity.ticker``. Renamed filers go by CIK because the
  catalog moves first and the graph follows on the next nightly build.
  """
  symbol = ticker.strip().upper()
  if not _TICKER.match(symbol) or symbol.isdigit() or not env.PUBLIC_DATA_BUCKET:
    return None
  now = time.monotonic()
  cached = _renamed_cache.get(symbol)
  if cached and cached[0] > now:
    return cached[1]
  try:
    text = S3Client().read_string(
      env.PUBLIC_DATA_BUCKET, get_filing_catalog_key(symbol)
    )
    document = json.loads(text) if text else None
  except Exception as e:
    logger.warning(f"SEC catalog read failed for {symbol}: {e}")
    return None
  cik = None
  if isinstance(document, dict) and (
    document.get("moved_to") or document.get("former_tickers")
  ):
    cik = document.get("cik")
  if len(_renamed_cache) >= _RENAMED_CACHE_LIMIT:
    _renamed_cache.clear()
  _renamed_cache[symbol] = (now + _RENAMED_TTL_SECONDS, cik)
  return cik


async def renamed_filer_cik(graph_id: str, ticker: str | None) -> str | None:
  """:func:`renamed_filer_cik_sync`, on the SEC repository only."""
  if graph_id != SEC_GRAPH_ID or not ticker:
    return None
  return await asyncio.to_thread(renamed_filer_cik_sync, ticker)


class SECReportResolutionError(RuntimeError):
  """Raised when the report lookup itself fails.

  Distinct from a ``None`` return, which means the query ran and matched
  nothing. Callers treat ``None`` as "no such filing" and may fall back to
  a broader search; they must not do that when the lookup never completed.
  """


def build_sec_report_query(
  *,
  ticker: str,
  cik: str | None = None,
  period_type: str | None = None,
  fiscal_year: int | None = None,
) -> tuple[str, dict[str, Any]]:
  """The Cypher and parameters that resolve a ticker's most recent filing.

  ``cik`` matches the filer by CIK instead, for a renamed filer. ``period_type``
  "quarterly" or "instant" adds 10-Q to the annual forms; anything else means
  annual only. The replica warmup runs this same text at boot
  (``graph_api/core/ladybug/warmup.py``).
  """
  if period_type == "annual":
    forms: tuple[str, ...] = ANNUAL_FORMS
  elif period_type in ("quarterly", "instant"):
    forms = QUARTERLY_FORMS
  else:
    forms = ANNUAL_FORMS

  where_parts = [
    "ent.cik = $cik" if cik else "ent.ticker = $ticker",
    "r.form IN $forms",
  ]
  parameters: dict[str, Any] = {"forms": list(forms)}
  if cik:
    parameters["cik"] = cik
  else:
    parameters["ticker"] = ticker

  if fiscal_year is not None:
    where_parts.append("r.fiscal_year_focus = $fiscal_year")
    parameters["fiscal_year"] = fiscal_year

  query = (
    "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) "
    f"WHERE {' AND '.join(where_parts)} "
    "RETURN r.identifier AS identifier, "
    "r.form AS form, r.filing_date AS filing_date, "
    "r.fiscal_year_focus AS fiscal_year, r.fiscal_period_focus AS fiscal_period, "
    "r.accession_number AS accession, ent.cik AS cik, "
    "r.uri AS uri, r.is_inline_xbrl AS is_inline_xbrl "
    "ORDER BY r.filing_date DESC LIMIT 1"
  )
  return query, parameters


async def resolve_sec_report(
  graph_id: str,
  *,
  ticker: str,
  period_type: str | None = None,
  fiscal_year: int | None = None,
) -> dict[str, Any] | None:
  """The most recent filing for a ticker, or None when nothing matches."""
  query, parameters = build_sec_report_query(
    ticker=ticker,
    cik=await renamed_filer_cik(graph_id, ticker),
    period_type=period_type,
    fiscal_year=fiscal_year,
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
