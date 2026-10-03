"""EFTS discovery of current reports (8-K) with the items each one reports.

xbrlkit's ``EftsHit`` keeps the form and the filer but not ``items``, the
field that says whether an 8-K is an earnings release, so the pages are read
here through the client's own pager (its rate limiter and retry policy). A
quarter of 8-Ks runs past EFTS' 10,000-hit ceiling, so a window over the
ceiling is halved until each piece fits.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, timedelta

from xbrlkit.edgar import EftsClient
from xbrlkit.edgar.efts import EFTS_MAX_PAGE_SIZE, EFTS_MAX_RESULTS

from ..processors.current_reports import CURRENT_REPORT_FORM, CurrentReportHit
from .edgar import edgar_filing_folder_url


def filing_file_url(cik: str, accession: str, name: str) -> str:
  """A file in a filing's EDGAR folder."""
  return edgar_filing_folder_url(cik, accession) + name


def filing_zip_url(cik: str, accession: str) -> str:
  """The filing's ``-xbrl.zip``: the inline document and, for an 8-K, its
  exhibits."""
  return filing_file_url(cik, accession, f"{accession}-xbrl.zip")


def _total(efts: EftsClient, params: dict) -> tuple[int, bool]:
  """The window's hit count, and whether EFTS capped it: past 10,000 it
  answers ``{"value": 10000, "relation": "gte"}``, never the real count, so
  the relation is what says a window must split."""
  first = efts._fetch_page(params, offset=0, size=1)
  total = first.get("hits", {}).get("total", {}) or {}
  value = int(total.get("value", 0) or 0)
  return value, total.get("relation") == "gte" or value >= EFTS_MAX_RESULTS


def _windows(
  efts: EftsClient, start: date, end: date, log: Callable[[str], None]
) -> Iterable[tuple[dict, int]]:
  """``(params, total)`` for date windows that each fit under the ceiling."""
  params = EftsClient.build_params(
    forms=[CURRENT_REPORT_FORM], start_date=start.isoformat(), end_date=end.isoformat()
  )
  total, capped = _total(efts, params)
  if not capped or start >= end:
    if capped:
      log(
        f"EFTS: {start} alone holds over {EFTS_MAX_RESULTS} 8-Ks; only those are read"
      )
    yield params, min(total, EFTS_MAX_RESULTS)
    return
  middle = start + timedelta(days=(end - start).days // 2)
  yield from _windows(efts, start, middle, log)
  yield from _windows(efts, middle + timedelta(days=1), end, log)


def discover_current_reports(
  efts: EftsClient,
  start: date,
  end: date,
  items: Iterable[str],
  log: Callable[[str], None] = lambda _msg: None,
) -> tuple[int, list[CurrentReportHit]]:
  """Every 8-K filed in ``[start, end]`` reporting any of ``items``.

  Returns the number of 8-Ks seen and the wanted ones, one per accession
  (amendments are excluded, as ``build_params`` excludes them by default).
  """
  wanted = tuple(items)
  seen = 0
  found: dict[str, CurrentReportHit] = {}
  for params, total in _windows(efts, start, end, log):
    offset = 0
    while offset < total:
      size = min(EFTS_MAX_PAGE_SIZE, total - offset)
      page = efts._fetch_page(params, offset=offset, size=size)
      # EFTS answers a full page whatever size was asked; keep to the window.
      hits = (page.get("hits", {}).get("hits", []) or [])[:size]
      for raw in hits:
        seen += 1
        hit = CurrentReportHit.from_efts(raw)
        if hit is not None and hit.wanted(wanted):
          found.setdefault(hit.accession, hit)
      offset += len(hits)
      if len(hits) < size:
        break
  return seen, list(found.values())
