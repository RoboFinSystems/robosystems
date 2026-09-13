"""Information-block analytical views — the map and the block, over a report held whole.

``disclosures`` (the families a report's sections form) and
``information-block`` (one section read whole: rows in presentation order,
the same rows by the section's own axes, its calculation arcs footed, its
text blocks) are the two shaped tools ``xbrlkit serve`` runs over a loaded
filing. This module serves them over a report the platform holds, from the
same two functions in the xbrlkit library, so the hosted tools and the local
server answer from one implementation.

**A whole filing is a file; the graph answers what crosses filings.** A
section read whole wants the whole report in memory, and the platform has
two ways to get one that never touch the graph:

- On a shared repository the report is a published filing: the SEC pipeline
  writes every processed filing as a holon to the public data bucket while
  it holds the parsed model (``ref/shared-data.md`` §5), and xbrlkit's holon
  reader gives the model back in well under a second. A filing processed
  before the artifacts existed answers *not published yet* — never a
  whole-report walk of the corpus-scale graph, which costs tens of seconds
  of replica IO per filing.
- On a tenant graph the report is the ledger's own: the bundle the Tavi and
  holon exports already build from the extensions database
  (``build_report_bundle`` → ``bundle_to_xbrl_model``), live, with its
  presentation and calculation networks and its facts pinned to their
  structures.

The model is cached per report in Valkey (``MCP_CACHE``), so every block on
a report after the first is served from memory. Registered on shared repos
and tenants alike; the report is chosen the way ``financial-statement-
analysis`` chooses one.
"""

from __future__ import annotations

import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

from xbrlkit.deserialize import HolonError, from_holon_report
from xbrlkit.model import XbrlModel
from xbrlkit.serve import tools as xbrlkit_tools
from xbrlkit.serve.session import LoadedFiling
from xbrlkit.serve.tools import ToolError

from robosystems.config import env
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_HOLON,
  get_filing_artifact_key,
)
from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository
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


# xbrlkit's own caps, restated for the request models that cannot import them.
MAX_BLOCK_ROWS = xbrlkit_tools.MAX_BLOCK_ROWS
MAX_BLOCK_MEMBERS = xbrlkit_tools.MAX_BLOCK_MEMBERS_CAP

# A published filing does not change once processed; a tenant's report is
# live and regenerates, so its model goes stale sooner.
MODEL_CACHE_TTL_SHARED_SECONDS = 6 * 60 * 60
MODEL_CACHE_TTL_TENANT_SECONDS = 5 * 60
# Bump when the readers or the emitters change what a cached model holds.
MODEL_CACHE_VERSION = "2"
# Text-block fragments fetched from the public bucket per published filing.
FRAGMENT_WORKERS = 8

# The filing's coordinates in the public bucket, from the report it is on the
# graph: one anchored statement, the kind the shared replica answers in a
# fraction of a second.
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
  """The shared MCP_CACHE client, created on first use; ``None`` when the
  cache is unreachable, so a Valkey outage costs a read, not the call."""
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


def _cache_key(graph_id: str, report_id: str) -> str:
  return f"ib:model:v{MODEL_CACHE_VERSION}:{graph_id}:{report_id}"


async def load_report_model(graph_id: str, report_id: str) -> tuple[XbrlModel, bool]:
  """The report as xbrlkit's model: from the cache when it is there, else
  read from where the platform holds it and cached. Returns the model and
  whether the cache served it."""
  key = _cache_key(graph_id, report_id)
  cache = _cache()
  if cache is not None:
    try:
      blob = await cache.get(key)
    except Exception as exc:
      logger.warning(f"information-block model cache read failed for {key}: {exc}")
      blob = None
    if blob:
      return XbrlModel.model_validate_json(zlib.decompress(blob)), True

  started = time.perf_counter()
  shared = is_shared_repository_or_subgraph(graph_id)
  if shared:
    model = await _published_model(graph_id, report_id)
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
    try:
      await cache.set(
        key, zlib.compress(model.model_dump_json().encode("utf-8")), ex=ttl
      )
    except Exception as exc:
      logger.warning(f"information-block model cache write failed for {key}: {exc}")
  return model, False


async def _published_model(graph_id: str, report_id: str) -> XbrlModel:
  """A shared repository's report from its published holon in the public
  bucket, its text-block fragments read beside it."""
  repository = await get_graph_repository(graph_id, operation_type="read")
  rows = await repository.execute_query(COORDINATES_QUERY, {"report": report_id})
  if not rows:
    raise ReportNotFoundError(f"No report {report_id!r} on graph {graph_id}.")
  row = rows[0]
  accession = str(row.get("accession") or "")
  filing_date = str(row.get("filing_date") or "")
  cik = str(row.get("cik") or "")
  if not (accession and cik and len(filing_date) >= 4):
    raise ReportNotPublishedError(
      f"Report {report_id!r} carries no accession, filer or filing date, so it "
      "has no published filing to read."
    )
  s3 = S3Client()
  key = get_filing_artifact_key(filing_date[:4], cik, accession, FILING_ARTIFACT_HOLON)
  text = await run_off_loop(s3.download_string, env.PUBLIC_DATA_BUCKET, key)
  if text is None:
    raise ReportNotPublishedError(
      f"{accession} was processed before its filing artifacts existed; it is "
      "published on the next reprocess of the repository."
    )
  try:
    model, _gaps = from_holon_report(text)
  except HolonError as exc:
    raise ReportNotPublishedError(
      f"The published holon for {accession} could not be read: {exc}"
    ) from exc
  inlined = await run_off_loop(_inline_fragments, s3, model)
  if inlined:
    logger.info(f"inlined {inlined} text-block fragments for {accession}")
  return model


def _fragment_key(url: str, bucket: str) -> str:
  """The bucket key behind a fragment's public URL — the CDN serves the key
  as its path, and a path-style endpoint (dev) leads with the bucket."""
  path = urlparse(url).path.lstrip("/")
  if path.startswith(f"{bucket}/"):
    path = path[len(bucket) + 1 :]
  return path


def _inline_fragments(s3: S3Client, model: XbrlModel) -> int:
  """Replace a text block's fragment URL with the fragment.

  The pipeline externalizes a large text block to the public bucket and the
  holon carries its URL; the block tool wants the text, for its preview and
  its length, so the fragments are read here in parallel. One that cannot be
  read stays a URL and the block says so.
  """
  pending = [fact for fact in model.facts if _is_external_text(model, fact)]
  if not pending:
    return 0
  bucket = env.PUBLIC_DATA_BUCKET
  keys = [_fragment_key(fact.value_str or "", bucket) for fact in pending]
  with ThreadPoolExecutor(max_workers=FRAGMENT_WORKERS) as pool:
    bodies = list(pool.map(lambda key: s3.download_string(bucket, key), keys))
  inlined = 0
  for fact, body in zip(pending, bodies, strict=True):
    if body:
      fact.value_str = body
      fact.raw_value = body
      inlined += 1
  return inlined


def _is_external_text(model: XbrlModel, fact: Any) -> bool:
  if fact.value_kind != "text" or not (fact.value_str or "").startswith(
    ("http://", "https://")
  ):
    return False
  concept = model.concepts.get(fact.concept_qname)
  return concept is not None and concept.is_textblock


def _tenant_model(graph_id: str, report_id: str) -> XbrlModel:
  """A tenant's report from the ledger, as the exports build it: live, with
  its networks and its facts pinned to their structures."""
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

  The same contract as ``financial-statement-analysis``: a ``report_id`` is
  taken as given; on a shared repository a ``ticker`` resolves the latest
  filing of the form ``period_type`` selects (annual by default), narrowed by
  ``fiscal_year``; a tenant graph needs the ``report_id``.
  """
  if report_id:
    return report_id, None
  if not is_shared_repository_or_subgraph(graph_id):
    raise ReportSelectorError("report_id is required for tenant graphs.")
  if not ticker:
    raise ReportSelectorError(
      "ticker is required on shared-repository graphs (e.g. SEC), or a report_id."
    )
  from robosystems.adapters.sec.mcp import resolve_sec_report

  symbol = ticker.strip().upper()
  resolved = await resolve_sec_report(
    graph_id, ticker=symbol, period_type=period_type, fiscal_year=fiscal_year
  )
  if not resolved or not resolved.get("identifier"):
    scope = f" in fiscal year {fiscal_year}" if fiscal_year is not None else ""
    raise ReportNotFoundError(
      f"No {period_type or 'annual'} filing found for {symbol}{scope}."
    )
  return str(resolved["identifier"]), resolved


def resolved_report_info(resolved: dict[str, Any] | None) -> dict[str, Any] | None:
  """The ``resolved_report`` block the sibling views return."""
  if not resolved:
    return None
  return {
    "report_id": resolved.get("identifier"),
    "form": resolved.get("form"),
    "filing_date": resolved.get("filing_date"),
    "fiscal_year": resolved.get("fiscal_year"),
    "fiscal_period": resolved.get("fiscal_period"),
  }


async def query_disclosures(
  graph_id: str, report_id: str, *, topic: str | None = None
) -> dict[str, Any]:
  """The map: one row per disclosure family, or one family's blocks."""
  model, _cached = await load_report_model(graph_id, report_id)
  try:
    out = xbrlkit_tools.disclosures(_loaded(graph_id, report_id, model), topic)
  except ToolError as exc:
    raise BlockNotFoundError(str(exc)) from exc
  external = _external_text_blocks(model)
  if external:
    for block in out.get("blocks") or []:
      if block.get("text_blocks"):
        block["text_blocks"] = _hosted_text(block["text_blocks"], external)
  return _stamp(out, graph_id, report_id)


async def query_information_block(
  graph_id: str,
  report_id: str,
  block: str,
  *,
  periods: list[str] | None = None,
  member: str | None = None,
  max_rows: int | None = None,
  max_members: int | None = None,
) -> dict[str, Any]:
  """The block: one section read whole."""
  model, _cached = await load_report_model(graph_id, report_id)
  kwargs: dict[str, Any] = {}
  if max_rows is not None:
    kwargs["max_rows"] = max_rows
  try:
    out = xbrlkit_tools.information_block(
      _loaded(graph_id, report_id, model),
      block,
      periods=periods,
      member=member,
      max_members=max_members,
      **kwargs,
    )
  except ToolError as exc:
    raise BlockNotFoundError(str(exc)) from exc
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
