"""The ``disclosures`` and ``information-block`` views, served from the same
xbrlkit functions ``xbrlkit serve`` runs, over a report held whole in memory.

The graph is not in the path: on a shared repository the report is the
published holon in the public bucket (a filing processed before artifacts
existed answers *not published yet*, never a whole-report graph walk); on a
tenant it is the live report bundle built from the extensions database. The
model is cached per report in Valkey (``MCP_CACHE``).

Nor is it in the lookup, for a ticker: the public catalog says which report a
ticker means and where its folder is, so a filing processed today is readable
before the night's rebuild. The graph is asked when the catalog cannot say,
and for a report named by id alone.
"""

from __future__ import annotations

import asyncio
import json
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

from xbrlkit import serve as xbrlkit_serve
from xbrlkit.deserialize import HolonError, from_holon_report
from xbrlkit.model import XbrlModel
from xbrlkit.serve import LoadedFiling, ToolError

from robosystems.config import env
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_HOLON,
  FILING_ARTIFACT_MANIFEST,
  FILING_ARTIFACT_TAVI,
  get_filing_artifact_key,
  get_filing_catalog_key,
  get_public_data_url,
  get_viewer_link,
)
from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.aws.s3 import S3Client


class ReportSelectorError(ValueError):
  """The request names no report the graph could resolve — a caller error."""


class ReportNotFoundError(ValueError):
  """No report matched the selector, or the graph holds no such report."""


class ReportNotPublishedError(ReportNotFoundError):
  """The report exists on the shared repository but its filing artifacts do
  not: it was processed before they existed, and is published on the next
  reprocess."""


class BlockNotFoundError(ValueError):
  """No block (or disclosure family) by that name on the report."""


class ReportTooLargeError(ValueError):
  """The report exists, and is larger than this reader holds in memory."""


class PublicStorageError(RuntimeError):
  """The public bucket could not be read. Not a ``ValueError``: nothing about
  the request is wrong, and "the file is not there" would be a wrong answer."""


# The repository the public catalog describes. A subgraph of it, or another
# shared repository, holds a different set of reports and answers for itself.
CATALOG_GRAPH_ID = "sec"


def _hosted_hint(message: str) -> str:
  """xbrlkit's errors name its own tools. Hosted, the map of a report's blocks
  is ``disclosures``, on a ledger and on SEC alike; ``describe_filing`` is not
  a tool a ledger has."""
  return message.replace("describe_filing", "disclosures")


# xbrlkit's notes name its own tools; hosted, the same steps are these.
_HOSTED_NOTE_PHRASES = (
  (
    "`id` is what information_block and statement take",
    "`id` is the `block` `information-block` takes",
  ),
  ("call disclosures with a topic", "call `disclosures` with a `topic`"),
  ("then information_block for the one", "then `information-block` for the one"),
)
# On a shared repository a text block is also in the filing's whole text; a
# ledger's report has no text beyond its blocks.
_READ_TEXT_PHRASE = "read one with read_text from its offset"
_SHARED_READ_TEXT = (
  "`search-text` finds one in the filing's whole text, `read-text` reads it"
)
_TENANT_READ_TEXT = "each previewed here"


def _hosted_note(out: dict[str, Any], graph_id: str) -> dict[str, Any]:
  note = out.get("note")
  if not note:
    return out
  for theirs, ours in _HOSTED_NOTE_PHRASES:
    note = note.replace(theirs, ours)
  read = (
    _SHARED_READ_TEXT
    if is_shared_repository_or_subgraph(graph_id)
    else _TENANT_READ_TEXT
  )
  out["note"] = note.replace(_READ_TEXT_PHRASE, read)
  return out


# xbrlkit's own caps, restated for the request models that cannot import them.
MAX_BLOCK_ROWS = xbrlkit_serve.MAX_BLOCK_ROWS
MAX_BLOCK_MEMBERS = xbrlkit_serve.MAX_BLOCK_MEMBERS_CAP

# A published filing does not change once processed; a tenant's report is
# live and regenerates, so its model goes stale sooner.
MODEL_CACHE_TTL_SHARED_SECONDS = 6 * 60 * 60
MODEL_CACHE_TTL_TENANT_SECONDS = 5 * 60
# Bump when the readers or the emitters change what a cached model holds.
MODEL_CACHE_VERSION = "4"
# Text-block fragments fetched from the public bucket per request.
FRAGMENT_WORKERS = 8
# What a filed document the tools can read is named like.
_DOCUMENT_SUFFIXES = (".htm", ".html", ".txt")
# What one response may read into the model: the text of the block or family
# it returns, never the filing's. A fragment past the budget stays a pointer
# and the entry says so.
FRAGMENT_TEXT_BUDGET_CHARS = 8_000_000
# The largest published holon read whole.
HOLON_BUDGET_CHARS = 48_000_000

# The filing's coordinates in the public bucket, from its Report node.
COORDINATES_QUERY = (
  "MATCH (r:Report {identifier: $report})<-[:ENTITY_HAS_REPORT]-(e:Entity) "
  "RETURN r.accession_number AS accession, r.filing_date AS filing_date, "
  "e.cik AS cik"
)

EXTERNAL_TEXT_NOTE = (
  "a `text` entry marked external is a text block whose fragment could not "
  "be read; read it with `search-documents` and `get-document-section`"
)

_redis_client: Any = None
_redis_unavailable = False


def _cache() -> Any:
  """The MCP_CACHE client, or ``None`` when unreachable: a Valkey outage costs
  a read, not the call."""
  global _redis_client, _redis_unavailable
  if _redis_client is None and not _redis_unavailable:
    try:
      _redis_client = create_async_redis_client(
        ValkeyDatabase.MCP_CACHE, decode_responses=False
      )
    except Exception as exc:
      _redis_unavailable = True
      logger.warning(f"information-block model cache unavailable: {exc}")
  return _redis_client


# A cold build holds a whole report in memory (~250 MB for a large filing), so
# a process builds one at a time, and callers of the same report share it.
_BUILD_SLOTS = asyncio.Semaphore(1)
_builds_in_flight: dict[str, asyncio.Task[XbrlModel]] = {}


def _cache_key(graph_id: str, report_id: str) -> str:
  return f"ib:model:v{MODEL_CACHE_VERSION}:{graph_id}:{report_id}"


# Where a published filing's folder is: (accession, cik, filing_date).
Coordinates = tuple[str, str, str]


def report_coordinates(resolved: dict[str, Any] | None) -> Coordinates | None:
  """The folder of the filing a ticker resolved to, when the resolver said:
  the catalog and the graph both do. Known, it spares the graph a lookup —
  and it is the only way to a filing the graph does not hold yet."""
  if not resolved:
    return None
  accession = str(resolved.get("accession") or "")
  cik = str(resolved.get("cik") or "")
  filing_date = str(resolved.get("filing_date") or "")[:10]
  if accession and cik and len(filing_date) >= 4:
    return accession, cik, filing_date
  return None


async def load_report_model(
  graph_id: str, report_id: str, coordinates: Coordinates | None = None
) -> tuple[XbrlModel, bool]:
  """The report as xbrlkit's model, and whether the cache served it."""
  key = _cache_key(graph_id, report_id)
  cache = _cache()
  if cache is not None:
    try:
      blob = await cache.get(key)
    except Exception as exc:
      logger.warning(f"information-block model cache read failed for {key}: {exc}")
      blob = None
    if blob:
      return await run_off_loop(_thaw, blob), True

  build = _builds_in_flight.get(key)
  joined = build is not None
  if build is None:
    build = asyncio.create_task(
      _build_once(key, graph_id, report_id, cache, coordinates)
    )
    _builds_in_flight[key] = build
    build.add_done_callback(lambda done, k=key: _build_finished(k, done))
  try:
    # Shielded: a caller that goes away does not cancel the build others await.
    return await asyncio.shield(build), False
  except ReportNotFoundError:
    if not joined or coordinates is None:
      raise
    # The build this caller joined was started without the folder and looked
    # for the report on the graph. This caller was told where it is.
    return await _build_once(key, graph_id, report_id, cache, coordinates), False


async def _build_once(
  key: str,
  graph_id: str,
  report_id: str,
  cache: Any,
  coordinates: Coordinates | None = None,
) -> XbrlModel:
  async with _BUILD_SLOTS:
    return await _build_and_cache(key, graph_id, report_id, cache, coordinates)


def _build_finished(key: str, done: asyncio.Task[XbrlModel]) -> None:
  _builds_in_flight.pop(key, None)
  if not done.cancelled():
    # Retrieved so a build whose callers all left doesn't log as unhandled.
    done.exception()


async def _build_and_cache(
  key: str,
  graph_id: str,
  report_id: str,
  cache: Any,
  coordinates: Coordinates | None = None,
) -> XbrlModel:
  started = time.perf_counter()
  shared = is_shared_repository_or_subgraph(graph_id)
  if shared:
    model = await _published_model(graph_id, report_id, coordinates)
  else:
    model = await run_off_loop(_tenant_model, graph_id, report_id)
  logger.info(
    f"information-block model {graph_id}/{report_id}: {len(model.facts)} facts, "
    f"{len(model.networks)} networks, {len(model.concepts)} concepts in "
    f"{time.perf_counter() - started:.2f}s from the "
    f"{'published filing' if shared else 'report bundle'}"
  )

  if cache is not None:
    ttl = MODEL_CACHE_TTL_SHARED_SECONDS if shared else MODEL_CACHE_TTL_TENANT_SECONDS
    frozen = await run_off_loop(_freeze, model)
    try:
      await cache.set(key, frozen, ex=ttl)
    except Exception as exc:
      logger.warning(f"information-block model cache write failed for {key}: {exc}")
  return model


def _freeze(model: XbrlModel) -> bytes:
  return zlib.compress(model.model_dump_json().encode("utf-8"))


def _thaw(blob: bytes) -> XbrlModel:
  return XbrlModel.model_validate_json(zlib.decompress(blob))


async def _published_model(
  graph_id: str, report_id: str, coordinates: Coordinates | None = None
) -> XbrlModel:
  """A shared repository's report from its published holon. Text blocks stay
  pointers to their fragments (read per response by ``_inline_text``), so the
  cached model stays the size of the holon.

  The folder comes from ``coordinates`` when the resolver gave them; a report
  named by id alone is looked up on the graph."""
  if coordinates is None:
    repository = await get_graph_repository(graph_id, operation_type="read")
    rows = await repository.execute_query(COORDINATES_QUERY, {"report": report_id})
    if not rows:
      raise ReportNotFoundError(
        f"No report {report_id!r} on graph {graph_id}. A report found through a "
        "ticker is read by giving the ticker again (with its fiscal_year), which "
        "does not need the graph."
      )
    row = rows[0]
    accession = str(row.get("accession") or "")
    filing_date = str(row.get("filing_date") or "")
    cik = str(row.get("cik") or "")
    if not (accession and cik and len(filing_date) >= 4):
      raise ReportNotPublishedError(
        f"Report {report_id!r} carries no accession, filer or filing date, so it "
        "has no published filing to read."
      )
  else:
    accession, cik, filing_date = coordinates
  s3 = S3Client()
  key = get_filing_artifact_key(filing_date[:4], cik, accession, FILING_ARTIFACT_HOLON)
  try:
    text = await run_off_loop(s3.read_string, env.PUBLIC_DATA_BUCKET, key)
  except Exception as exc:
    raise PublicStorageError(
      f"The published filing for {accession} could not be read; try again."
    ) from exc
  if text is None:
    raise ReportNotPublishedError(
      f"{accession} was processed before its filing artifacts existed; it is "
      "published on the next reprocess of the repository."
    )
  if len(text) > HOLON_BUDGET_CHARS:
    raise ReportTooLargeError(
      f"{accession} is too large to read whole here; read it section by section "
      "with `search-documents` and `get-document-section`, or load its published "
      "holon with xbrlkit."
    )
  try:
    # Reading a 10-K's holon into the model is a few hundred milliseconds of
    # CPU; the API runs one worker, so it stays off the loop like the fetch.
    model, _gaps = await run_off_loop(from_holon_report, text)
  except HolonError as exc:
    raise ReportNotPublishedError(
      f"The published holon for {accession} could not be read: {exc}"
    ) from exc
  return model


def _fragment_key(url: str, bucket: str) -> str:
  """The bucket key behind a fragment's public URL — the CDN serves the key
  as its path, and a path-style endpoint (dev) leads with the bucket."""
  path = urlparse(url).path.lstrip("/")
  if path.startswith(f"{bucket}/"):
    path = path[len(bucket) + 1 :]
  return path


def _inline_fragments(
  s3: S3Client, model: XbrlModel, concepts: set[str], *, strict: bool = False
) -> int:
  """Replace these text blocks' fragment URLs with the fragments, in place.

  A fragment that cannot be read, or would exceed the response budget, stays a
  URL; with ``strict``, one whose read fails raises instead. Returns how many
  were inlined.
  """
  pending = [
    fact
    for fact in model.facts
    if fact.concept_qname in concepts and _is_external_text(model, fact)
  ]
  if not pending:
    return 0
  bucket = env.PUBLIC_DATA_BUCKET
  keys = [_fragment_key(fact.value_str or "", bucket) for fact in pending]

  def read_strict(key: str) -> str | None:
    try:
      return s3.read_string(bucket, key)
    except Exception as exc:
      logger.warning(f"public fragment read failed for {key}: {exc}")
      raise PublicStorageError(
        "The published filing could not be read; try again."
      ) from exc

  with ThreadPoolExecutor(max_workers=FRAGMENT_WORKERS) as pool:
    if strict:
      bodies = list(pool.map(read_strict, keys))
    else:
      bodies = list(pool.map(lambda key: s3.download_string(bucket, key), keys))
  inlined = 0
  budget = FRAGMENT_TEXT_BUDGET_CHARS
  for fact, body in zip(pending, bodies, strict=True):
    if body and len(body) <= budget:
      budget -= len(body)
      fact.value_str = body
      fact.raw_value = body
      inlined += 1
  return inlined


async def _inline_text(model: XbrlModel, entries: list[dict[str, Any]]) -> bool:
  """Read the fragments behind a response's text entries into the model.
  True when any was read, and the response is worth building again."""
  concepts = {str(entry.get("concept")) for entry in entries}
  concepts &= _external_text_blocks(model)
  if not concepts:
    return False
  inlined = await run_off_loop(_inline_fragments, S3Client(), model, concepts)
  return inlined > 0


def _is_external_text(model: XbrlModel, fact: Any) -> bool:
  if fact.value_kind != "text" or not (fact.value_str or "").startswith(
    ("http://", "https://")
  ):
    return False
  concept = model.concepts.get(fact.concept_qname)
  return concept is not None and concept.is_textblock


def _tenant_model(graph_id: str, report_id: str) -> XbrlModel:
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.serialization import (
    build_report_bundle,
    bundle_to_xbrl_model,
  )

  with extensions_session(graph_id) as session:
    try:
      bundle = build_report_bundle(session, graph_id, report_id)
    except LookupError as exc:
      raise ReportNotFoundError(
        f"No report {report_id!r} on graph {graph_id}: {exc}"
      ) from exc
  return bundle_to_xbrl_model(bundle)


async def resolve_report(
  graph_id: str,
  *,
  report_id: str | None = None,
  ticker: str | None = None,
  fiscal_year: int | None = None,
  period_type: str | None = None,
) -> tuple[str, dict[str, Any] | None]:
  """Which report the request means, and how it was resolved.

  The selectors ``financial-statement-analysis`` takes: a ``report_id`` is
  taken as given; on a shared repository a ``ticker`` resolves the latest
  filing of the form ``period_type`` selects (annual by default), narrowed by
  ``fiscal_year``; a tenant graph needs the ``report_id``.

  On the SEC repository a ticker is looked up in the public catalog first and
  on the graph only when the catalog cannot say. These views read the
  published filing, which the catalog lists as soon as it is processed; the
  graph learns of it at the night's rebuild, and holds fewer years. So for a
  few hours, and for those years, this resolves a report that
  ``financial-statement-analysis`` (which reads the graph) does not.
  """
  shared = is_shared_repository_or_subgraph(graph_id)
  if not shared and is_subgraph(graph_id):
    # A subgraph has no ledger of its own; fail here rather than deep in the read.
    raise ReportSelectorError(
      "A subgraph has no ledger of its own; read the report on its parent graph."
    )
  if report_id:
    return report_id, None
  if not shared:
    raise ReportSelectorError("report_id is required for tenant graphs.")
  if not ticker:
    raise ReportSelectorError(
      "ticker is required on shared-repository graphs (e.g. SEC), or a report_id."
    )
  from robosystems.adapters.sec.mcp import resolve_sec_report

  symbol = ticker.strip().upper()
  period = (period_type or "").strip().lower() or None
  resolved = await _catalog_report(graph_id, symbol, fiscal_year, period)
  if resolved is None:
    resolved = await resolve_sec_report(
      graph_id, ticker=symbol, period_type=period, fiscal_year=fiscal_year
    )
  if not resolved or not resolved.get("identifier"):
    scope = f" in fiscal year {fiscal_year}" if fiscal_year is not None else ""
    raise ReportNotFoundError(
      f"No {period_type or 'annual'} filing found for {symbol}{scope}."
    )
  return str(resolved["identifier"]), resolved


async def _catalog_report(
  graph_id: str, symbol: str, fiscal_year: int | None, period_type: str | None
) -> dict[str, Any] | None:
  """The filer's latest report by the public catalog, in the shape the graph
  resolver answers in, or None when the catalog cannot say and the graph
  should: another graph than the one the catalog describes, no catalog for
  the filer, nothing it lists that fits, or a newest fit that is not readable
  (the graph then names it and the read says it is not published — never an
  older filing passed off as the latest).

  A catalog that cannot be read raises: falling back then would answer with
  one filing on this call and another on the next.
  """
  from robosystems.adapters.sec.mcp.report_resolver import (
    ANNUAL_FORMS,
    QUARTERLY_FORMS,
    SECReportResolutionError,
  )
  from robosystems.operations.roboledger.views.filing_text import (
    TICKER_RE,
    _report_ref,
  )

  if graph_id != CATALOG_GRAPH_ID or not env.PUBLIC_DATA_BUCKET:
    return None
  if not TICKER_RE.match(symbol) or symbol.isdigit():
    return None
  try:
    text = await run_off_loop(
      S3Client().read_string, env.PUBLIC_DATA_BUCKET, get_filing_catalog_key(symbol)
    )
    catalog = json.loads(text) if text else None
  except Exception as exc:
    logger.warning(f"SEC catalog read failed for {symbol}: {exc}")
    raise SECReportResolutionError(
      f"Could not resolve an SEC filing for {symbol}: the catalog lookup failed."
    ) from exc
  if not isinstance(catalog, dict) or not catalog.get("cik"):
    return None

  # The graph resolver reads "instant" as quarterly too: a balance can come
  # from either kind of report.
  forms = QUARTERLY_FORMS if period_type in ("quarterly", "instant") else ANNUAL_FORMS
  newest = next(
    (
      entry
      for entry in catalog.get("filings") or []
      if entry.get("form") in forms
      and (fiscal_year is None or entry.get("fiscal_year") == fiscal_year)
    ),
    None,
  )
  if newest is None:
    return None
  ref = _report_ref(str(catalog["cik"]), newest)
  has_holon = any(r.get("kind") == "holon" for r in newest.get("representations") or [])
  if not (has_holon and ref.report_id and ref.accession and ref.filing_date):
    return None
  listed = ref.resolved or {}
  return {
    "identifier": ref.report_id,
    "form": ref.form,
    "filing_date": ref.filing_date,
    "fiscal_year": listed.get("fiscal_year"),
    "fiscal_period": listed.get("fiscal_period"),
    "accession": ref.accession,
    "cik": ref.cik,
    "links": listed.get("links"),
  }


def resolved_report_info(resolved: dict[str, Any] | None) -> dict[str, Any] | None:
  """The ``resolved_report`` block the sibling views return, with where the
  filing is served when the resolver said which filing it is."""
  if not resolved:
    return None
  info: dict[str, Any] = {
    "report_id": resolved.get("identifier"),
    "form": resolved.get("form"),
    "filing_date": resolved.get("filing_date"),
    "fiscal_year": resolved.get("fiscal_year"),
    "fiscal_period": resolved.get("fiscal_period"),
  }
  if resolved.get("links"):
    # From the catalog, which names the files the folder holds.
    info["links"] = resolved["links"]
    return info
  accession = str(resolved.get("accession") or "")
  cik = str(resolved.get("cik") or "")
  filing_date = str(resolved.get("filing_date") or "")
  if accession and cik and len(filing_date) >= 4:
    # The Report's uri is the primary document's EDGAR URL. The processor
    # copies an inline document into the folder; a classic filing's arrives
    # only with the document backfill, which the graph does not record.
    representations = []
    name = str(resolved.get("uri") or "").rsplit("/", 1)[-1]
    if resolved.get("is_inline_xbrl") and name.lower().endswith(_DOCUMENT_SUFFIXES):
      representations.append({"kind": "document", "name": name})
    if links := public_filing_links(cik, accession, filing_date, representations):
      info["links"] = links
  return info


def public_filing_links(
  cik: str,
  accession: str,
  filing_date: str,
  representations: list[dict[str, Any]] | None = None,
  *,
  has_holon: bool = True,
) -> dict[str, Any]:
  """Where one filing is served: ``viewer`` (the xbrlkit viewer over the
  published holon — what to hand a person who wants to see the filing),
  ``holon`` and ``tavi``, ``as_filed`` when the document's name is known,
  ``exhibits`` (an 8-K's EX-99 files), the ``manifest`` of the files in its
  folder and the filing on ``edgar``. The holon and the Tavi sit at their fixed names unless
  ``representations`` names them; an 8-K has neither (``has_holon=False``).
  Empty when no public bucket is configured: there is nowhere to link.
  """
  from robosystems.adapters.sec.client.edgar import edgar_filing_folder_url

  bucket, cdn = env.PUBLIC_DATA_BUCKET, env.PUBLIC_DATA_CDN_URL
  if not bucket:
    return {}
  year = filing_date[:4]

  def url(name: str) -> str:
    return get_public_data_url(
      bucket, get_filing_artifact_key(year, cik, accession, name), cdn
    )

  links: dict[str, Any] = {}
  names: dict[str, str] = (
    {"holon": FILING_ARTIFACT_HOLON, "tavi": FILING_ARTIFACT_TAVI} if has_holon else {}
  )
  exhibits: dict[str, str] = {}
  for rep in representations or []:
    kind, name = rep.get("kind"), rep.get("name")
    if not name:
      continue
    if kind == "exhibit":
      # Two files under one exhibit number keep the first; a file with no
      # number is keyed by its name, which is unique in the folder.
      exhibits.setdefault(str(rep.get("exhibit") or name), url(str(name)))
    elif kind in ("holon", "tavi", "document"):
      names[str(kind)] = str(name)
  if "holon" in names:
    links["holon"] = url(names["holon"])
    links["viewer"] = get_viewer_link(env.VIEWER_URL, links["holon"])
  if "tavi" in names:
    links["tavi"] = url(names["tavi"])
  if "document" in names:
    links["as_filed"] = url(names["document"])
  if exhibits:
    links["exhibits"] = exhibits
  # No link to the folder itself: it is a key prefix, which opens nothing. The
  # manifest lists what is in it.
  links["manifest"] = url(FILING_ARTIFACT_MANIFEST)
  links["edgar"] = edgar_filing_folder_url(cik, accession)
  return links


async def query_disclosures(
  graph_id: str,
  report_id: str,
  *,
  topic: str | None = None,
  coordinates: Coordinates | None = None,
) -> dict[str, Any]:
  """The map: one row per disclosure family, or one family's blocks."""
  model, _cached = await load_report_model(graph_id, report_id, coordinates)
  try:
    out = xbrlkit_serve.disclosures(_loaded(graph_id, report_id, model), topic)
    # A family's blocks carry each text block's length, which is the text's.
    entries = [
      entry
      for block in out.get("blocks") or []
      for entry in block.get("text_blocks") or []
    ]
    if await _inline_text(model, entries):
      out = xbrlkit_serve.disclosures(_loaded(graph_id, report_id, model), topic)
  except ToolError as exc:
    raise BlockNotFoundError(_hosted_hint(str(exc))) from exc
  external = _external_text_blocks(model)
  if external:
    for block in out.get("blocks") or []:
      if block.get("text_blocks"):
        block["text_blocks"] = _hosted_text(block["text_blocks"], external)
  return _stamp(_hosted_note(out, graph_id), graph_id, report_id)


async def query_information_block(
  graph_id: str,
  report_id: str,
  block: str,
  *,
  periods: list[str] | None = None,
  member: str | None = None,
  max_rows: int | None = None,
  max_members: int | None = None,
  offset: int | None = None,
  coordinates: Coordinates | None = None,
) -> dict[str, Any]:
  """The block: one section read whole, or — past ``max_rows`` — one page of
  it, continued from the ``next_offset`` a truncated page returns."""
  model, _cached = await load_report_model(graph_id, report_id, coordinates)
  kwargs: dict[str, Any] = {}
  if max_rows is not None:
    kwargs["max_rows"] = max_rows
  if offset:
    kwargs["offset"] = offset

  def read() -> dict[str, Any]:
    return xbrlkit_serve.information_block(
      _loaded(graph_id, report_id, model),
      block,
      periods=periods,
      member=member,
      max_members=max_members,
      **kwargs,
    )

  try:
    out = read()
    if await _inline_text(model, out.get("text") or []):
      out = read()
  except ToolError as exc:
    raise BlockNotFoundError(_hosted_hint(str(exc))) from exc
  out = _hosted_note(out, graph_id)
  external = _external_text_blocks(model)
  if external and out.get("text"):
    out["text"] = _hosted_text(out["text"], external)
    note = out.get("note")
    out["note"] = f"{note}; {EXTERNAL_TEXT_NOTE}" if note else EXTERNAL_TEXT_NOTE
  return _stamp(out, graph_id, report_id)


def _loaded(graph_id: str, report_id: str, model: XbrlModel) -> LoadedFiling:
  """The model as the xbrlkit tools take it. No document: the text tools'
  offsets point into the tagged text blocks, which is what the report holds."""
  return LoadedFiling(id=report_id, source=graph_id, model=model, text="", sections=[])


def _external_text_blocks(model: XbrlModel) -> set[str]:
  """Text-block concepts whose value is still a pointer to the text — a
  fragment that could not be read."""
  return {fact.concept_qname for fact in model.facts if _is_external_text(model, fact)}


def _hosted_text(
  entries: list[dict[str, Any]], external: set[str]
) -> list[dict[str, Any]]:
  """A text entry whose value is a pointer says so instead of previewing it."""
  out: list[dict[str, Any]] = []
  for entry in entries:
    if entry.get("concept") in external:
      entry = {
        k: v for k, v in entry.items() if k not in ("preview", "chars", "offset")
      }
      entry["external"] = True
    out.append(entry)
  return out


def _stamp(out: dict[str, Any], graph_id: str, report_id: str) -> dict[str, Any]:
  return {"graph_id": graph_id, "report_id": report_id, **out}
