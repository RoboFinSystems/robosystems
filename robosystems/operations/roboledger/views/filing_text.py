"""``describe-filing``, ``search-text`` and ``read-text``: xbrlkit's text tools
over one filing read whole from its public folder.

On the SEC repository a filing is found the way the public pages find it —
the filer's catalog (``companies/{ticker}.json``) for an XBRL report, its
releases list (``current-reports/{cik}.json``) for an 8-K — and read from its
folder: the holon and the primary document of a 10-K / 10-Q / 20-F / 40-F,
or an 8-K with its EX-99 exhibits. Any processed year reads, not only the
graph's. A report whose folder holds no document reads as its tagged text
blocks. Neither the search index nor EDGAR is in the path; the graph is asked
only for a ``report_id`` named on its own. Shared repositories only: a ledger
files no document, and its sections are ``disclosures`` / ``information-block``.

A caller's query is matched as words, not run as a regular expression: the
pattern handed to xbrlkit is built here from escaped literals, so no input can
make the match backtrack.
"""

from __future__ import annotations

import asyncio
import json
import re
import zlib
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from xbrlkit.deserialize import HolonError, from_holon_report
from xbrlkit.model import EntityIdentity, FilingMeta, XbrlModel
from xbrlkit.serve import LoadedFiling, TextSection, ToolError, build_text

# Not in xbrlkit.serve's declared surface; held to the version pin
# (xbrlkit>=0.20.4,<0.21) until they are exported beside disclosures.
from xbrlkit.serve.tools import (
  DEFAULT_HITS,
  DEFAULT_READ,
  DEFAULT_WINDOW,
  MAX_HITS,
  MAX_READ,
  MAX_WINDOW,
  describe_filing,
  read_text,
  search_text,
)

from robosystems.adapters.sec.mcp.report_resolver import ANNUAL_FORMS, QUARTERLY_FORMS
from robosystems.adapters.sec.processors.current_reports import (
  CURRENT_REPORT_FORM,
  FiledDocument,
  current_report_text,
)
from robosystems.config import env
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_HOLON,
  FILING_ARTIFACT_MANIFEST,
  get_current_reports_list_key,
  get_filing_artifact_key,
  get_filing_catalog_key,
)
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.aws.s3 import S3Client

from .information_blocks import (
  COORDINATES_QUERY,
  HOLON_BUDGET_CHARS,
  MODEL_CACHE_TTL_SHARED_SECONDS,
  ReportNotFoundError,
  ReportNotPublishedError,
  ReportSelectorError,
  ReportTooLargeError,
  _cache,
  _external_text_blocks,
  _inline_fragments,
  public_filing_links,
)
from .information_blocks import _cache_key as model_cache_key
from .information_blocks import _freeze as freeze_model
from .information_blocks import _thaw as thaw_model

SEARCH_MAX_HITS = MAX_HITS
SEARCH_MAX_WINDOW = MAX_WINDOW
READ_MAX_LENGTH = MAX_READ

TEXT_CACHE_VERSION = "2"
# A published filing does not change once processed.
TEXT_CACHE_TTL_SECONDS = 6 * 60 * 60
# The most filed text read whole for one filing — a document, or an 8-K's
# documents together; a 10-K's HTML is a few MB. Checked against the sizes the
# manifest records before anything is downloaded.
DOCUMENT_BUDGET_CHARS = 40_000_000
# The releases an 8-K resolution lists beside the one it picked, so a caller
# can name an older one by accession; ``fiscal_year`` moves the list to the
# releases filed in that year.
RECENT_RELEASES = 12

QUERY_MAX_CHARS = 500
_QUERY_ALTERNATIVES = 10
_QUERY_WORDS = 20

ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
# What a ticker looks like on EDGAR (BRK.B, BF-B): the only input that reaches
# a catalog key.
TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")
_DOCUMENT_SUFFIXES = (".htm", ".html", ".txt")

# One text build at a time per process (a 10-K's HTML is CPU to read), and
# callers of the same filing share it.
_TEXT_SLOTS = asyncio.Semaphore(1)
_texts_in_flight: dict[str, asyncio.Task[LoadedFiling]] = {}


class QueryError(ValueError):
  """A search query with nothing to match."""


def query_pattern(query: str) -> str:
  """The regular expression a query means: each ``|``-separated phrase
  matched as its words in order across any whitespace, a trailing ``*`` a
  stem (``terminat*``), a bare ``*`` ignored. Every word is escaped, so the
  pattern is linear."""
  text = (query or "").strip()[:QUERY_MAX_CHARS]
  phrases: list[str] = []
  for phrase in text.split("|")[:_QUERY_ALTERNATIVES]:
    words = phrase.split()[:_QUERY_WORDS]
    parts = [
      re.escape(w[:-1]) + r"\w*" if w.endswith("*") and len(w) > 1 else re.escape(w)
      for w in words
      if w != "*"
    ]
    if parts:
      phrases.append(r"\s+".join(parts))
  if not phrases:
    raise QueryError("query is required: one or more words, phrases split by |")
  return "|".join(phrases)


# ── which filing ───────────────────────────────────────────────────────────


@dataclass
class FilingRef:
  """A filing the text tools can read: one public folder on the SEC
  repository (``accession`` / ``cik`` / ``filing_date``)."""

  report_id: str | None = None
  accession: str | None = None
  cik: str | None = None
  filing_date: str | None = None
  form: str | None = None
  resolved: dict[str, Any] | None = None

  @property
  def in_folder(self) -> bool:
    return bool(self.accession and self.cik and self.filing_date)

  @property
  def year(self) -> str:
    return (self.filing_date or "")[:4]

  @property
  def key(self) -> str:
    return self.accession or ""


async def resolve_filing(
  graph_id: str,
  *,
  report_id: str | None = None,
  ticker: str | None = None,
  fiscal_year: int | None = None,
  period_type: str | None = None,
  accession: str | None = None,
  form: str | None = None,
) -> FilingRef:
  """Which filing the request means.

  On SEC a ``ticker`` opens the filer's catalog: ``accession`` picks one
  filing from it (an 8-K's from its releases list), ``form="8-K"`` the latest
  earnings release (``fiscal_year`` then means the calendar year it was
  filed), anything else the latest annual (or, with ``period_type`` quarterly,
  any) report, narrowed by ``fiscal_year``. A CIK in place of the ticker
  reaches the 8-Ks of a filer the catalog does not list. A ``report_id`` names
  a report in the graph on its own; with a ticker, accession or form beside
  it the request is ambiguous and refused. A tenant graph is refused: a
  ledger files no document to read.
  """
  wants_8k = (form or "").strip().upper() == CURRENT_REPORT_FORM
  if not is_shared_repository_or_subgraph(graph_id):
    raise ReportSelectorError(
      "describe-filing, search-text and read-text read a filing as filed, which "
      "only the SEC repository holds; a ledger's report reads through "
      "disclosures and information-block."
    )

  if accession:
    accession = accession.strip()
    if not ACCESSION_RE.match(accession):
      raise ReportSelectorError(
        f"{accession!r} is not an accession number (0000320193-25-000077)."
      )
  if report_id:
    if ticker or accession or wants_8k:
      raise ReportSelectorError(
        "report_id names the filing on its own; give either report_id or a "
        "ticker (with accession or form to pick one filing), not both."
      )
    return await _graph_report(graph_id, report_id)
  if not ticker:
    raise ReportSelectorError(
      "ticker is required on the SEC repository (with accession or form to pick "
      "one filing), or a report_id."
    )
  symbol = ticker.strip().upper()
  if not TICKER_RE.match(symbol):
    raise ReportSelectorError(f"{ticker!r} is not a ticker symbol.")
  s3 = S3Client()
  if symbol.isdigit():
    # No ticker is all digits, so this is a CIK. The catalog is keyed by
    # ticker; the releases lists are keyed by CIK and need no catalog.
    if not (wants_8k or accession):
      raise ReportSelectorError(
        "A CIK finds a filer's 8-K earnings releases (form: 8-K, or an "
        "accession); its annual and quarterly reports are listed by ticker."
      )
    cik = symbol.zfill(10)
    return await _release_ref(s3, f"CIK {cik}", cik, accession, fiscal_year)
  catalog = await _read_json(s3, get_filing_catalog_key(symbol))
  if not catalog or not catalog.get("cik"):
    raise ReportNotFoundError(f"No filer {symbol} in the SEC catalog.")
  cik = str(catalog["cik"])
  filings = catalog.get("filings") or []

  if accession and not wants_8k:
    entry = next((f for f in filings if f.get("accession") == accession), None)
    if entry is not None:
      return _report_ref(cik, entry)
  if accession or wants_8k:
    return await _release_ref(s3, symbol, cik, accession, fiscal_year)

  forms = (
    QUARTERLY_FORMS if (period_type or "").lower() == "quarterly" else ANNUAL_FORMS
  )
  for entry in filings:
    if entry.get("form") not in forms:
      continue
    if fiscal_year is not None and entry.get("fiscal_year") != fiscal_year:
      continue
    if any(r.get("kind") == "holon" for r in entry.get("representations") or []):
      return _report_ref(cik, entry)
  scope = f" for fiscal year {fiscal_year}" if fiscal_year is not None else ""
  raise ReportNotFoundError(
    f"No published {period_type or 'annual'} filing for {symbol}{scope}."
  )


def _report_ref(cik: str, entry: dict[str, Any]) -> FilingRef:
  accession = str(entry.get("accession") or "")
  filing_date = str(entry.get("filing_date") or "")
  resolved: dict[str, Any] = {
    "report_id": entry.get("report_id"),
    "accession": accession,
    "form": entry.get("form"),
    "filing_date": entry.get("filing_date"),
    "fiscal_year": entry.get("fiscal_year"),
    "fiscal_period": entry.get("fiscal_period"),
  }
  if accession and len(filing_date) >= 4:
    if links := public_filing_links(
      cik, accession, filing_date, entry.get("representations") or []
    ):
      resolved["links"] = links
  return FilingRef(
    report_id=entry.get("report_id"),
    accession=accession or None,
    cik=cik,
    filing_date=entry.get("filing_date"),
    form=entry.get("form"),
    resolved=resolved,
  )


_RELEASE_FOLDER_RE = re.compile(r"/\d{4}/(\d{10})/(\d{10}-\d{2}-\d{6})/?$")


def _folder_cik(entry: dict[str, Any], listed_under: str) -> str:
  """The CIK a release's files sit under. A combined 8-K is published once,
  under its first registrant, and listed under every registrant: the entry's
  own folder says where it is, not the list it was found in."""
  match = _RELEASE_FOLDER_RE.search(str(entry.get("folder") or ""))
  if match and match.group(2) == entry.get("accession"):
    return match.group(1)
  return listed_under


async def _release_ref(
  s3: S3Client, symbol: str, cik: str, accession: str | None, year: int | None = None
) -> FilingRef:
  """An 8-K from the filer's releases list: the one named, else the latest
  reporting Item 2.02, else the latest — among those filed in ``year`` when
  one is given, which is how a release older than the listed few is reached."""
  listed = await _read_json(s3, get_current_reports_list_key(cik))
  releases = (listed or {}).get("releases") or []
  if accession:
    entry = next((r for r in releases if r.get("accession") == accession), None)
  else:
    if year is not None:
      releases = [
        r for r in releases if str(r.get("filing_date") or "")[:4] == str(year)
      ]
    entry = next((r for r in releases if "2.02" in (r.get("items") or [])), None)
    entry = entry or (releases[0] if releases else None)
  if entry is None:
    what = accession or "8-K earnings release"
    when = f" filed in {year}" if year is not None and not accession else ""
    raise ReportNotFoundError(f"No {what}{when} captured for {symbol}.")
  filing_date = str(entry.get("filing_date") or "")
  filed_under = _folder_cik(entry, cik)
  resolved: dict[str, Any] = {
    "accession": entry["accession"],
    "form": CURRENT_REPORT_FORM,
    "filing_date": entry.get("filing_date"),
    "items": entry.get("items"),
    "recent_releases": [
      {k: r.get(k) for k in ("accession", "filing_date", "items")}
      for r in releases[:RECENT_RELEASES]
    ],
  }
  if len(filing_date) >= 4:
    representations: list[dict[str, Any]] = []
    if entry.get("document"):
      representations.append({"kind": "document", "name": entry["document"]})
    for exhibit, name in (entry.get("exhibits") or {}).items():
      representations.append({"kind": "exhibit", "name": name, "exhibit": exhibit})
    if links := public_filing_links(
      filed_under, entry["accession"], filing_date, representations, has_holon=False
    ):
      resolved["links"] = links
  return FilingRef(
    accession=entry["accession"],
    cik=filed_under,
    filing_date=entry.get("filing_date"),
    form=CURRENT_REPORT_FORM,
    resolved=resolved,
  )


async def _graph_report(graph_id: str, report_id: str) -> FilingRef:
  """A report named by id: its folder from the graph's Report node."""
  repository = await get_graph_repository(graph_id, operation_type="read")
  rows = await repository.execute_query(COORDINATES_QUERY, {"report": report_id})
  if not rows:
    raise ReportNotFoundError(f"No report {report_id!r} on graph {graph_id}.")
  row = rows[0]
  accession = str(row.get("accession") or "")
  cik = str(row.get("cik") or "")
  filing_date = str(row.get("filing_date") or "")[:10]
  if not (accession and cik and len(filing_date) >= 4):
    raise ReportNotPublishedError(
      f"Report {report_id!r} carries no accession, filer or filing date, so it "
      "has no published filing to read."
    )
  # Narrower than a catalog entry: the coordinates query carries no form or
  # fiscal period, and a caller who named the report already knows them.
  resolved: dict[str, Any] = {
    "report_id": report_id,
    "accession": accession,
    "filing_date": filing_date,
  }
  if links := public_filing_links(cik, accession, filing_date):
    resolved["links"] = links
  ref = FilingRef(
    report_id=report_id,
    accession=accession,
    cik=cik,
    filing_date=filing_date,
    resolved=resolved,
  )
  return ref


def filing_info(ref: FilingRef) -> dict[str, Any] | None:
  """The ``resolved_report`` block: how the filing was picked, when it was."""
  return ref.resolved


# ── reading the folder ─────────────────────────────────────────────────────


async def _read_json(s3: S3Client, key: str) -> dict[str, Any] | None:
  text = await run_off_loop(s3.download_string, env.PUBLIC_DATA_BUCKET, key)
  return json.loads(text) if text else None


async def _read_text(s3: S3Client, key: str, what: str) -> str | None:
  text = await run_off_loop(s3.download_string, env.PUBLIC_DATA_BUCKET, key)
  if text is not None and len(text) > DOCUMENT_BUDGET_CHARS:
    raise ReportTooLargeError(_too_large(what))
  return text


def _too_large(what: str) -> str:
  return f"{what} is too large to read whole here; load the filing with xbrlkit."


def _within_budget(what: str, chars: int) -> None:
  """Refuse before downloading what the manifest says would not fit."""
  if chars > DOCUMENT_BUDGET_CHARS:
    raise ReportTooLargeError(_too_large(what))


def _filed(manifest: dict[str, Any], kind: str) -> list[dict[str, Any]]:
  """The manifest's representations of one kind that are readable text: a
  classic filing whose "primary document" is the XBRL instance names an
  ``.xml`` the text tools would read as markup soup."""
  return [
    r
    for r in manifest.get("representations") or []
    if r.get("kind") == kind
    and r.get("name")
    and str(r["name"]).lower().endswith(_DOCUMENT_SUFFIXES)
  ]


def _named(manifest: dict[str, Any], kind: str) -> dict[str, Any] | None:
  return next(iter(_filed(manifest, kind)), None)


async def _folder_manifest(s3: S3Client, ref: FilingRef) -> dict[str, Any]:
  assert ref.accession and ref.cik
  manifest = await _read_json(
    s3,
    get_filing_artifact_key(ref.year, ref.cik, ref.accession, FILING_ARTIFACT_MANIFEST),
  )
  if not manifest:
    raise ReportNotPublishedError(
      f"{ref.accession} was processed before its filing artifacts existed; it is "
      "published on the next reprocess of the repository."
    )
  return manifest


async def _holon_model(
  s3: S3Client, ref: FilingRef, manifest: dict[str, Any]
) -> XbrlModel:
  assert ref.accession and ref.cik
  rep = next(
    (r for r in manifest.get("representations") or [] if r.get("kind") == "holon"),
    None,
  )
  name = str((rep or {}).get("name") or FILING_ARTIFACT_HOLON)
  if rep and int(rep.get("bytes") or 0) > HOLON_BUDGET_CHARS:
    raise ReportTooLargeError(_too_large(ref.accession))
  text = await run_off_loop(
    s3.download_string,
    env.PUBLIC_DATA_BUCKET,
    get_filing_artifact_key(ref.year, ref.cik, ref.accession, name),
  )
  if text is None:
    raise ReportNotPublishedError(f"{ref.accession} has no published holon.")
  if len(text) > HOLON_BUDGET_CHARS:
    raise ReportTooLargeError(_too_large(ref.accession))
  try:
    model, _gaps = await run_off_loop(from_holon_report, text)
  except HolonError as exc:
    raise ReportNotPublishedError(
      f"The published holon for {ref.accession} could not be read: {exc}"
    ) from exc
  return model


async def _report_from_folder(
  graph_id: str, s3: S3Client, ref: FilingRef
) -> LoadedFiling:
  """A 10-K / 10-Q / 20-F / 40-F: its holon, and its document when the folder
  holds one. Its text blocks' fragments are read in unless the document is
  inline XBRL, which carries the blocks itself."""
  assert ref.accession and ref.cik
  manifest = await _folder_manifest(s3, ref)
  if (manifest.get("form") or "").upper() == CURRENT_REPORT_FORM:
    return await _current_report_from_folder(graph_id, s3, ref, manifest)
  model = await _holon_model(s3, ref, manifest)
  html = None
  if document := _named(manifest, "document"):
    what = f"{ref.accession}'s document"
    _within_budget(what, int(document.get("bytes") or 0))
    html = await _read_text(
      s3,
      get_filing_artifact_key(ref.year, ref.cik, ref.accession, str(document["name"])),
      what,
    )
  if html is None or not manifest.get("is_inline_xbrl"):
    await run_off_loop(_inline_fragments, s3, model, _external_text_blocks(model))
  text, sections = await run_off_loop(build_text, model, html)
  return LoadedFiling(
    id=ref.key,
    source=graph_id,
    model=model,
    text=text,
    sections=sections,
    has_document=html is not None,
  )


async def _current_report_from_folder(
  graph_id: str, s3: S3Client, ref: FilingRef, manifest: dict[str, Any]
) -> LoadedFiling:
  """An 8-K as one text: the form, then its exhibits."""
  assert ref.accession and ref.cik and ref.filing_date
  filed = _filed(manifest, "document") + _filed(manifest, "exhibit")
  _within_budget(ref.accession, sum(int(r.get("bytes") or 0) for r in filed))
  documents: list[FiledDocument] = []
  for rep in filed:
    body = await _read_text(
      s3,
      get_filing_artifact_key(ref.year, ref.cik, ref.accession, str(rep["name"])),
      f"{ref.accession}'s {rep['name']}",
    )
    if body is not None:
      documents.append(
        FiledDocument(
          str(rep["name"]), str(rep["kind"]), body.encode("utf-8"), rep.get("exhibit")
        )
      )
  documents.sort(key=lambda d: (d.kind != "document", d.exhibit or "~", d.name))
  assembled = await run_off_loop(current_report_text, documents)
  entity = manifest.get("entity") or {}
  report_date = manifest.get("report_date")
  model = XbrlModel(
    filing=FilingMeta(
      accession=ref.accession,
      cik=ref.cik,
      form=CURRENT_REPORT_FORM,
      filing_date=date.fromisoformat(ref.filing_date),
      report_date=date.fromisoformat(report_date) if report_date else None,
      primary_document=manifest.get("primary_document"),
      document_name=manifest.get("primary_document"),
      items=list(manifest.get("items") or []),
    ),
    entity=EntityIdentity(
      cik=ref.cik, name=entity.get("name"), ticker=entity.get("ticker")
    ),
  )
  return LoadedFiling(
    id=ref.key,
    source=graph_id,
    model=model,
    text=assembled.text,
    sections=assembled.sections,
    has_document=bool(documents),
  )


# ── the text, cached ───────────────────────────────────────────────────────


def _text_key(graph_id: str, ref: FilingRef) -> str:
  return f"ft:text:v{TEXT_CACHE_VERSION}:{graph_id}:{ref.key}"


def _freeze(lf: LoadedFiling) -> bytes:
  """The text, its sections and the filing's identity — not the report's
  facts, which only ``describe-filing`` reads (:func:`_with_full_model`)."""
  payload = {
    "filing": lf.model.filing.model_dump(mode="json"),
    "entity": lf.model.entity.model_dump(mode="json"),
    "text": lf.text,
    "sections": [asdict(s) for s in lf.sections],
    "has_document": lf.has_document,
  }
  return zlib.compress(json.dumps(payload).encode("utf-8"))


def _thaw(blob: bytes, graph_id: str, key: str) -> LoadedFiling:
  payload = json.loads(zlib.decompress(blob))
  return LoadedFiling(
    id=key,
    source=graph_id,
    model=XbrlModel(
      filing=FilingMeta.model_validate(payload["filing"]),
      entity=EntityIdentity.model_validate(payload["entity"]),
    ),
    text=payload["text"],
    sections=[TextSection(**s) for s in payload["sections"]],
    has_document=payload["has_document"],
  )


async def load_filing_text(graph_id: str, ref: FilingRef) -> LoadedFiling:
  """The filing as xbrlkit's text tools take it, built once and cached."""
  key = _text_key(graph_id, ref)
  cache = _cache()
  if cache is not None:
    try:
      blob = await cache.get(key)
    except Exception as exc:
      logger.warning(f"filing text cache read failed for {key}: {exc}")
      blob = None
    if blob:
      return await run_off_loop(_thaw, blob, graph_id, ref.key)

  build = _texts_in_flight.get(key)
  if build is None:
    build = asyncio.create_task(_build_once(key, graph_id, ref, cache))
    _texts_in_flight[key] = build
    build.add_done_callback(lambda done, k=key: _build_finished(k, done))
  # Shielded: a caller that goes away does not cancel the build others await.
  return await asyncio.shield(build)


def _build_finished(key: str, done: asyncio.Task[LoadedFiling]) -> None:
  _texts_in_flight.pop(key, None)
  if not done.cancelled():
    # Retrieved so a build whose callers all left doesn't log as unhandled.
    done.exception()


async def _build_once(
  key: str, graph_id: str, ref: FilingRef, cache: Any
) -> LoadedFiling:
  async with _TEXT_SLOTS:
    lf = await _report_from_folder(graph_id, S3Client(), ref)
  if cache is not None:
    try:
      await cache.set(key, await run_off_loop(_freeze, lf), ex=TEXT_CACHE_TTL_SECONDS)
    except Exception as exc:
      logger.warning(f"filing text cache write failed for {key}: {exc}")
  return lf


async def _with_full_model(
  graph_id: str, ref: FilingRef, lf: LoadedFiling
) -> LoadedFiling:
  """The filing with its whole model, for the read that counts facts and
  networks. An 8-K has none beyond its identity. A report's holon is read
  under the build slot and kept in the information-block cache, under the key
  that lane uses for the report, so the two share one copy."""
  if lf.model.facts or lf.model.filing.form == CURRENT_REPORT_FORM:
    return lf
  key = model_cache_key(graph_id, ref.report_id or ref.key)
  cache = _cache()
  blob = None
  if cache is not None:
    try:
      blob = await cache.get(key)
    except Exception as exc:
      logger.warning(f"filing model cache read failed for {key}: {exc}")
  if blob:
    model = await run_off_loop(thaw_model, blob)
  else:
    async with _TEXT_SLOTS:
      s3 = S3Client()
      model = await _holon_model(s3, ref, await _folder_manifest(s3, ref))
    if cache is not None:
      try:
        frozen = await run_off_loop(freeze_model, model)
        await cache.set(key, frozen, ex=MODEL_CACHE_TTL_SHARED_SECONDS)
      except Exception as exc:
        logger.warning(f"filing model cache write failed for {key}: {exc}")
  return LoadedFiling(
    id=lf.id,
    source=lf.source,
    model=model,
    text=lf.text,
    sections=lf.sections,
    has_document=lf.has_document,
  )


# ── the three reads ────────────────────────────────────────────────────────


def _stamp(out: dict[str, Any], graph_id: str, ref: FilingRef) -> dict[str, Any]:
  stamp: dict[str, Any] = {"graph_id": graph_id}
  if ref.report_id:
    stamp["report_id"] = ref.report_id
  if ref.accession:
    stamp["accession"] = ref.accession
  return {**stamp, **out}


async def query_describe_filing(graph_id: str, ref: FilingRef) -> dict[str, Any]:
  lf = await _with_full_model(graph_id, ref, await load_filing_text(graph_id, ref))
  out = await run_off_loop(describe_filing, lf)
  return _stamp(out, graph_id, ref)


async def query_search_text(
  graph_id: str,
  ref: FilingRef,
  query: str,
  *,
  window: int | None = None,
  max_hits: int | None = None,
) -> dict[str, Any]:
  pattern = query_pattern(query)
  lf = await load_filing_text(graph_id, ref)
  try:
    out = await run_off_loop(
      search_text,
      lf,
      pattern,
      window or DEFAULT_WINDOW,
      max_hits or DEFAULT_HITS,
    )
  except ToolError as exc:
    raise QueryError(str(exc)) from exc
  out.pop("pattern", None)
  out["note"] = str(out.get("note", "")).replace("read_text", "read-text")
  out["text"] = "primary document" if lf.has_document else "tagged text blocks"
  return _stamp({"query": query, **out}, graph_id, ref)


async def query_read_text(
  graph_id: str,
  ref: FilingRef,
  *,
  offset: int = 0,
  length: int | None = None,
) -> dict[str, Any]:
  lf = await load_filing_text(graph_id, ref)
  try:
    out = read_text(lf, offset=offset, length=length or DEFAULT_READ)
  except ToolError as exc:
    raise QueryError(str(exc)) from exc
  return _stamp(out, graph_id, ref)
