"""EDGAR's current ticker map, applied over the tickers in processed rows.

A processed Entity row carries the ticker of the submissions snapshot that wrote
it, so a filer's stored ticker lags EDGAR until its next filing and a row written
before a listing has none. The catalog applies this map; staging then stamps what
the catalog decided, so the graph and the catalog cannot disagree.
"""

from __future__ import annotations

import json
import re
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from robosystems.config.storage.shared import DataSourceType, get_raw_key
from robosystems.logger import logger

TICKER_MAP_KEY = get_raw_key(DataSourceType.SEC, "identity", "company_tickers.json")
# A fetch this much shorter than the last good one is a bad response, not a
# wave of delistings.
MIN_MAP_SHARE = 0.9

# Units, warrants and rights: five or six letters ending in the suffix.
_DERIVATIVE_TICKER = re.compile(r"^[A-Z]{4,5}(W|WS|U|R)$")


@dataclass(frozen=True)
class Listing:
  """What EDGAR lists for one CIK today."""

  tickers: tuple[str, ...]
  title: str | None = None


def parse_ticker_map(raw: dict[str, Any]) -> dict[str, Listing]:
  """``{cik: Listing}`` from EDGAR's ``company_tickers``, tickers in file order."""
  tickers: dict[str, list[str]] = {}
  titles: dict[str, str | None] = {}
  for row in raw.values():
    ticker = str(row.get("ticker") or "").strip().upper()
    cik = str(row.get("cik_str") or "").strip()
    if not ticker or not cik:
      continue
    cik = cik.zfill(10)
    tickers.setdefault(cik, []).append(ticker)
    titles.setdefault(cik, row.get("title"))
  return {
    cik: Listing(tuple(listed), titles.get(cik)) for cik, listed in tickers.items()
  }


def _stored_ticker_map(s3: Any, bucket: str) -> dict[str, Listing] | None:
  """The last good copy. None when there is none or it is corrupt: the fetch can
  replace it, and absence from a map never clears a ticker."""
  try:
    body = s3.get_object(Bucket=bucket, Key=TICKER_MAP_KEY)["Body"].read()
  except s3.exceptions.NoSuchKey:
    return None
  try:
    return parse_ticker_map(json.loads(body))
  except (ValueError, AttributeError, TypeError) as e:
    logger.warning(f"Stored EDGAR ticker map is corrupt, ignoring it: {e}")
    return None


def current_ticker_map(s3: Any, bucket: str) -> dict[str, Listing] | None:
  """Today's map, kept as the last good copy; that copy if the fetch fails or
  looks wrong; None when there is no good one."""
  from robosystems.adapters.sec.client.edgar import edgar_client

  stored = _stored_ticker_map(s3, bucket)

  try:
    raw = edgar_client().company_tickers()
    fresh = parse_ticker_map(raw)
  except Exception as e:
    logger.warning(f"EDGAR ticker map fetch failed, using the last good copy: {e}")
    return stored

  if stored and len(fresh) < MIN_MAP_SHARE * len(stored):
    logger.warning(
      f"EDGAR ticker map has {len(fresh)} filers against {len(stored)} last time; "
      "using the last good copy"
    )
    return stored
  if not fresh:
    return stored
  s3.put_object(
    Bucket=bucket,
    Key=TICKER_MAP_KEY,
    Body=json.dumps(raw),
    ContentType="application/json",
  )
  return fresh


def _plain(ticker: str) -> bool:
  return not ("-" in ticker or "." in ticker or _DERIVATIVE_TICKER.match(ticker))


def choose_ticker(current: str | None, listed: tuple[str, ...]) -> str:
  """The filer's ticker from what EDGAR lists: its current one while listed,
  unless that is a unit or warrant and a plain ticker exists; else the first
  plain ticker; else the first listed."""
  plain = [t for t in listed if _plain(t)]
  if current in listed and (plain == [] or current in plain):
    return current
  return (plain or list(listed))[0]


def _normalized(name: str | None) -> str:
  return "".join(ch for ch in str(name or "").upper() if ch.isalnum())


def apply_ticker_map(
  rows: dict[str, dict[str, Any]],
  ticker_map: dict[str, Listing],
  *,
  filed: Collection[str],
  prior: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
  """``rows`` with each filer's ticker and name corrected from the map.

  ``filed`` is the CIKs with listed filings, the only ones whose tickers count.
  ``prior`` is the ticker each filer last published under, so a filer that drops
  off the map keeps it rather than reverting to the one in its old rows. Absence
  from the map never clears a ticker.

  A ticker another filer still holds is held back: the mover stays where it is.
  Taking ``XOM`` from Exxon Mobil Corp for its new holding company, which has no
  annual report yet, would empty the ticker's annual requests.
  """
  prior = prior or {}
  current: dict[str, str | None] = {}
  for cik in filed:
    if cik in rows:
      ticker = prior.get(cik) or rows[cik].get("ticker")
      current[cik] = ticker.upper() if ticker else None

  final: dict[str, str | None] = {
    cik: choose_ticker(ticker, ticker_map[cik].tickers) if cik in ticker_map else ticker
    for cik, ticker in current.items()
  }
  while True:
    holders: dict[str, list[str]] = {}
    for cik, ticker in final.items():
      if ticker:
        holders.setdefault(ticker, []).append(cik)
    held_back = [
      cik
      for owners in holders.values()
      if len(owners) > 1
      for cik in owners
      if final[cik] != current[cik]
    ]
    if not held_back:
      break
    for cik in held_back:
      final[cik] = current[cik]

  out = dict(rows)
  for cik, ticker in final.items():
    filer = rows[cik]
    listing = ticker_map.get(cik)
    # An unchanged filer keeps its row's own spelling of the ticker.
    stored = filer.get("ticker")
    corrected = stored if ticker and stored and stored.upper() == ticker else ticker
    title = listing.title if listing else None
    name = (
      title
      if title and _normalized(title) != _normalized(filer.get("name"))
      else filer.get("name")
    )
    out[cik] = {**filer, "ticker": corrected, "name": name}
  return out


def updated_aliases(
  aliases: dict[str, dict[str, Any]],
  listed: dict[str, str],
  prior: dict[str, str],
  today: str,
) -> dict[str, dict[str, Any]]:
  """Old ticker → the filer that left it, with today's renames added.

  ``listed`` and ``prior`` are ``{cik: ticker}`` for this run and the last. An
  alias ends once another filer lists under its ticker, or its filer returns.
  """
  out = {old: dict(entry) for old, entry in aliases.items()}
  for cik, ticker in listed.items():
    old = prior.get(cik)
    if old and old.upper() != ticker.upper():
      out[old.upper()] = {"cik": cik, "ticker": ticker.upper(), "since": today}
  taken = {ticker.upper() for ticker in listed.values()}
  kept: dict[str, dict[str, Any]] = {}
  for old, entry in out.items():
    if old in taken:
      continue
    ticker = listed.get(entry["cik"])
    kept[old] = {**entry, "ticker": ticker.upper()} if ticker else entry
  return kept
