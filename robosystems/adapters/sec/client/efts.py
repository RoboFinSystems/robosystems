"""
SEC EFTS (Electronic Filing Text Search) Client.

EFTS provides full-text search across all SEC filings, enabling:
- Discovery of filings by form type, date range, CIK, or text content
- Bulk queries that return all matching filings in one request
- Text search within filing content

This replaces the per-company iteration pattern with O(1) discovery.

API Documentation: https://efts.sec.gov/LATEST/
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode

import aiohttp

from robosystems.config import ExternalServicesConfig
from robosystems.logger import logger

from .rate_limiter import AsyncRateLimiter, RateMonitor

SEC_CONFIG = ExternalServicesConfig.SEC_CONFIG
SEC_HEADERS = SEC_CONFIG["headers"]

EFTS_BASE_URL = "https://efts.sec.gov/LATEST/search-index"
EFTS_MAX_PAGE_SIZE = 100
EFTS_MAX_RESULTS = 10000  # EFTS hard limit per query


@dataclass
class EFTSHit:
  """A single filing result from EFTS."""

  cik: str
  accession_number: str
  form_type: str
  file_number: str | None
  filing_date: str | None
  primary_document: str | None
  file_url: str | None

  @classmethod
  def from_hit(cls, hit: dict) -> "EFTSHit":
    """Parse an EFTS hit into a structured result."""
    source = hit.get("_source", {})
    hit_id = hit.get("_id", "")

    # Extract accession number from _id (format: "accno:filename")
    accession = hit_id.split(":")[0] if ":" in hit_id else hit_id

    # CIKs come as a list, take the first one
    ciks = source.get("ciks", [])
    cik = str(ciks[0]).zfill(10) if ciks else ""

    return cls(
      cik=cik,
      accession_number=accession,
      form_type=source.get("form", ""),
      file_number=source.get("file_num"),
      filing_date=source.get("file_date"),
      primary_document=source.get("display_names", [None])[0]
      if source.get("display_names")
      else None,
      file_url=source.get("file_url"),
    )


class EFTSClient:
  """
  Async client for SEC EFTS API.

  Example:
      async with EFTSClient() as client:
          filings = await client.query(
              form_types=["10-K", "10-Q"],
              start_date="2024-01-01",
              end_date="2024-12-31",
          )
          for filing in filings:
              print(f"{filing.cik}: {filing.form_type} - {filing.accession_number}")
  """

  def __init__(self, requests_per_second: float = 5.0):
    """
    Initialize EFTS client.

    Args:
        requests_per_second: Rate limit for SEC API (default: 5.0)
    """
    self.limiter = AsyncRateLimiter(rate=requests_per_second)
    self.monitor = RateMonitor()
    self._session: aiohttp.ClientSession | None = None

  async def __aenter__(self):
    self._session = aiohttp.ClientSession(headers=SEC_HEADERS)
    return self

  async def __aexit__(self, *args):
    if self._session:
      await self._session.close()
      self._session = None

  async def _fetch_page(
    self,
    params: dict,
    offset: int = 0,
    size: int = EFTS_MAX_PAGE_SIZE,
    retry_count: int = 0,
  ) -> dict:
    """Fetch a single page of results from EFTS."""
    MAX_RETRIES = 3
    MAX_RETRY_AFTER = 300  # Cap at 5 minutes to prevent DoS

    if not self._session:
      raise RuntimeError("Client not initialized. Use 'async with EFTSClient():'")

    url = f"{EFTS_BASE_URL}?{urlencode(params)}&from={offset}&size={size}"

    async with self.limiter:
      async with self._session.get(url) as response:
        content = await response.read()
        await self.monitor.record(len(content))

        if response.status == 429:
          if retry_count >= MAX_RETRIES:
            raise RuntimeError(f"EFTS max retries ({MAX_RETRIES}) exceeded for query")
          retry_after = min(
            int(response.headers.get("Retry-After", 60)), MAX_RETRY_AFTER
          )
          logger.warning(
            f"EFTS rate limited, waiting {retry_after}s "
            f"(retry {retry_count + 1}/{MAX_RETRIES})"
          )
          await asyncio.sleep(retry_after)
          return await self._fetch_page(params, offset, size, retry_count + 1)

        response.raise_for_status()
        return await response.json()

  def _build_params(
    self,
    form_types: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ciks: list[str] | None = None,
    text_query: str | None = None,
    include_amendments: bool = False,
  ) -> dict:
    """
    Build EFTS query parameters.

    Args:
        form_types: Form types to search for
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        ciks: CIKs to filter by (will be zero-padded to 10 digits)
        text_query: Full-text search query
        include_amendments: If False, excludes /A amendment forms

    Returns:
        Dict of query parameters for EFTS API.
    """
    params = {}

    # Form types - EFTS uses comma-separated list
    # Prefix with "-" to exclude, e.g., "-10-K/A" excludes 10-K amendments
    if form_types:
      forms = list(form_types)
      if not include_amendments:
        # Exclude amendment versions of requested forms
        for form in form_types:
          if not form.endswith("/A"):
            forms.append(f"-{form}/A")
      params["forms"] = ",".join(forms)

    # Date range - EFTS requires both dates
    params["startdt"] = start_date or "2001-01-01"
    params["enddt"] = end_date or datetime.now().strftime("%Y-%m-%d")

    # CIKs - must be zero-padded to 10 digits
    if ciks:
      params["ciks"] = ",".join(str(c).zfill(10) for c in ciks)

    # Full-text search
    if text_query:
      params["q"] = text_query

    return params

  async def query(
    self,
    form_types: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ciks: list[str] | None = None,
    text_query: str | None = None,
    max_results: int | None = None,
  ) -> list[EFTSHit]:
    """
    Query EFTS for filings matching criteria.

    Args:
        form_types: List of form types (e.g., ["10-K", "10-Q"])
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        ciks: List of CIKs to filter by
        text_query: Full-text search query
        max_results: Maximum results to return (default: all)

    Returns:
        List of EFTSHit objects representing matching filings.

    Example:
        # Get all 10-K filings for 2024
        hits = await client.query(
            form_types=["10-K"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        # Search for specific text
        hits = await client.query(
            text_query='"material weakness"',
            form_types=["10-K"],
        )
    """
    params = self._build_params(form_types, start_date, end_date, ciks, text_query)

    # Get first page to determine total
    logger.info(f"EFTS query: forms={form_types}, dates={start_date} to {end_date}")
    data = await self._fetch_page(params, offset=0, size=1)

    total_hits = data.get("hits", {}).get("total", {}).get("value", 0)
    logger.info(f"EFTS found {total_hits} matching filings")

    if total_hits == 0:
      return []

    # Determine how many to fetch
    if total_hits > EFTS_MAX_RESULTS:
      logger.warning(
        f"EFTS query returns {total_hits} results, exceeding {EFTS_MAX_RESULTS} limit. "
        "Consider narrowing date range or splitting by form type."
      )

    to_fetch = min(total_hits, max_results or total_hits, EFTS_MAX_RESULTS)

    # Fetch all pages
    all_hits: list[EFTSHit] = []
    offset = 0

    while offset < to_fetch:
      page_size = min(EFTS_MAX_PAGE_SIZE, to_fetch - offset)
      data = await self._fetch_page(params, offset=offset, size=page_size)

      hits = data.get("hits", {}).get("hits", [])
      for hit in hits:
        all_hits.append(EFTSHit.from_hit(hit))

      offset += len(hits)

      if len(hits) < page_size:
        break  # No more results

      # Log progress
      stats = self.monitor.get_stats()
      logger.debug(
        f"EFTS progress: {offset}/{to_fetch} ({stats.requests_per_second} req/s)"
      )

    logger.info(f"EFTS query complete: {len(all_hits)} filings retrieved")
    return all_hits

  async def query_by_year(
    self,
    year: int,
    form_types: list[str] | None = None,
    ciks: list[str] | None = None,
  ) -> list[EFTSHit]:
    """
    Convenience method to query filings for a specific year.

    Args:
        year: The fiscal year to query
        form_types: List of form types (default: ["10-K", "10-Q"])
        ciks: Optional list of CIKs to filter by

    Returns:
        List of EFTSHit objects for the year.
    """
    return await self.query(
      form_types=form_types or ["10-K", "10-Q"],
      start_date=f"{year}-01-01",
      end_date=f"{year}-12-31",
      ciks=ciks,
    )


async def query_efts(
  form_types: list[str] | None = None,
  start_date: str | None = None,
  end_date: str | None = None,
  ciks: list[str] | None = None,
  text_query: str | None = None,
  requests_per_second: float = 5.0,
) -> list[EFTSHit]:
  """
  Convenience function to query EFTS without managing client lifecycle.

  Example:
      from robosystems.adapters.sec.client.efts import query_efts

      filings = await query_efts(
          form_types=["10-K"],
          start_date="2024-01-01",
          end_date="2024-12-31",
      )
  """
  async with EFTSClient(requests_per_second=requests_per_second) as client:
    return await client.query(
      form_types=form_types,
      start_date=start_date,
      end_date=end_date,
      ciks=ciks,
      text_query=text_query,
    )


def query_efts_sync(
  form_types: list[str] | None = None,
  start_date: str | None = None,
  end_date: str | None = None,
  ciks: list[str] | None = None,
  text_query: str | None = None,
  requests_per_second: float = 5.0,
) -> list[EFTSHit]:
  """
  Synchronous wrapper for query_efts.

  Example:
      from robosystems.adapters.sec.client.efts import query_efts_sync

      filings = query_efts_sync(
          form_types=["10-K"],
          start_date="2024-01-01",
          end_date="2024-12-31",
      )
  """
  return asyncio.run(
    query_efts(
      form_types=form_types,
      start_date=start_date,
      end_date=end_date,
      ciks=ciks,
      text_query=text_query,
      requests_per_second=requests_per_second,
    )
  )
