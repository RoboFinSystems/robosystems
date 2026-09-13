"""Information-block analytical views — the map and the block, over the graph.

``disclosures`` (the families a report's sections form) and
``information-block`` (one section read whole: rows in presentation order,
the same rows by the section's own axes, its calculation arcs footed, its
text blocks) are the two shaped tools ``xbrlkit serve`` runs over a loaded
filing. This module serves them over a report that lives in LadybugDB — the
SEC shared repository, or a tenant graph whose ledger has been materialized.

Not a second implementation of the block rules in Cypher. The report's slice
is read out of the graph with xbrlkit's own ``SLICE_QUERIES``, run through
the graph repository, into xbrlkit's ``XbrlModel``; then xbrlkit's
``disclosures`` / ``information_block`` run over it unchanged, so the hosted
tools and ``xbrlkit serve`` answer from one implementation
(``specs/ai-operators/information-block-tools.md`` §4.2). The model is cached
per report in Valkey (``MCP_CACHE``), so every block on a filing after the
first is served from memory.

Two producers fill the graph's tables — the SEC processor projects a filing
through xbrlkit, a tenant materialization writes its own rows — and the
reader keys on the schema's columns, not on either producer's habits. What
the graph does not carry (an arc's ``targetRole``, fact language, the text
of a text block the platform stores on a CDN) is declared by the reader and
handled here where it shows.
"""

from __future__ import annotations

import time
import zlib
from typing import Any

from xbrlkit.deserialize.graph import (
  ELEMENT_QUERIES,
  SLICE_QUERIES,
  GraphError,
  ImportGaps,
  SliceQuery,
  from_graph_report,
  tables_from_slice,
)
from xbrlkit.model import XbrlModel
from xbrlkit.serialize.lpg import GraphTables
from xbrlkit.serve import tools as xbrlkit_tools
from xbrlkit.serve.session import LoadedFiling
from xbrlkit.serve.tools import ToolError

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.logger import logger
from robosystems.middleware.graph import get_graph_repository


class ReportSelectorError(ValueError):
  """The request names no report the graph could resolve — a caller error."""


class ReportNotFoundError(ValueError):
  """No report matched the selector, or the graph holds no rows for the id."""


class BlockNotFoundError(ValueError):
  """No block (or disclosure family) by that name on the report."""


# xbrlkit's own caps, restated for the request models that cannot import them.
MAX_BLOCK_ROWS = xbrlkit_tools.MAX_BLOCK_ROWS
MAX_BLOCK_MEMBERS = xbrlkit_tools.MAX_BLOCK_MEMBERS_CAP

# A filing on the shared repository does not change once processed; a tenant
# graph is replaced at every materialization, so its model goes stale sooner.
MODEL_CACHE_TTL_SHARED_SECONDS = 6 * 60 * 60
MODEL_CACHE_TTL_TENANT_SECONDS = 15 * 60
# Bump when the reader or the projection changes what a cached model holds.
MODEL_CACHE_VERSION = "1"
# Element ids per statement in the second pass — under the Graph API's
# 1,000-entry cap on a parameter array.
ELEMENT_BATCH = 500

EXTERNAL_TEXT_NOTE = (
  "text blocks on this graph are held outside it — a `text` entry carries the "
  "concept and label; read the text with `search-documents` and "
  "`get-document-section`"
)

_redis_client: Any = None
_redis_unavailable = False


def _cache() -> Any:
  """The shared MCP_CACHE client, created on first use; ``None`` when the
  cache is unreachable, so a Valkey outage costs a slice read, not the call."""
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


def _identifier(row: dict[str, Any]) -> str | None:
  for column, value in row.items():
    if column == "identifier" or column.endswith(".identifier"):
      return None if value is None else str(value)
  return None


async def fetch_report_slice(graph_id: str, report_id: str) -> GraphTables:
  """One report's rows, as the projection's own tables.

  xbrlkit defines the slice — one Cypher statement per table, by ``$report``
  (the report's identifier, URI or accession) — and this runs it through the
  graph repository. A read: on the shared repository ``operation_type="read"``
  routes to the replicas, as the sibling views do.
  """
  repository = await get_graph_repository(graph_id, operation_type="read")
  results: list[tuple[SliceQuery, list[dict[str, Any]]]] = []
  for query in SLICE_QUERIES:
    rows = await repository.execute_query(query.cypher, {"report": report_id})
    results.append((query, rows))
  elements = sorted(
    {
      identifier
      for query, rows in results
      if query.table == "Element"
      for identifier in (_identifier(row) for row in rows)
      if identifier
    }
  )
  # The Graph API caps a parameter array at 1,000 entries and a 10-K reaches
  # more elements than that (3M: 1,329), so the element pass goes in batches;
  # `tables_from_slice` keeps a row reached twice once.
  for start in range(0, len(elements), ELEMENT_BATCH):
    batch = elements[start : start + ELEMENT_BATCH]
    for query in ELEMENT_QUERIES:
      rows = await repository.execute_query(query.cypher, {"elements": batch})
      results.append((query, rows))
  return tables_from_slice(results)


async def load_report_model(
  graph_id: str, report_id: str
) -> tuple[XbrlModel, ImportGaps | None, bool]:
  """The report as xbrlkit's model: from the cache when it is there, else read
  out of the graph and cached. Returns the model, the reader's gap report
  (``None`` on a cache hit), and whether the cache served it."""
  key = _cache_key(graph_id, report_id)
  cache = _cache()
  if cache is not None:
    try:
      blob = await cache.get(key)
    except Exception as exc:
      logger.warning(f"information-block model cache read failed for {key}: {exc}")
      blob = None
    if blob:
      model = XbrlModel.model_validate_json(zlib.decompress(blob))
      return model, None, True

  started = time.perf_counter()
  tables = await fetch_report_slice(graph_id, report_id)
  fetched = time.perf_counter()
  if not tables.nodes.get("Report"):
    raise ReportNotFoundError(f"No report {report_id!r} on graph {graph_id}.")
  try:
    model, gaps = from_graph_report(tables)
  except GraphError as exc:
    raise ReportNotFoundError(str(exc)) from exc
  built = time.perf_counter()
  logger.info(
    f"information-block slice {graph_id}/{report_id}: {len(model.facts)} facts, "
    f"{len(model.networks)} networks, {len(model.concepts)} concepts in "
    f"{fetched - started:.2f}s fetch + {built - fetched:.2f}s model"
  )

  if cache is not None:
    ttl = (
      MODEL_CACHE_TTL_SHARED_SECONDS
      if is_shared_repository_or_subgraph(graph_id)
      else MODEL_CACHE_TTL_TENANT_SECONDS
    )
    try:
      await cache.set(
        key, zlib.compress(model.model_dump_json().encode("utf-8")), ex=ttl
      )
    except Exception as exc:
      logger.warning(f"information-block model cache write failed for {key}: {exc}")
  return model, gaps, False


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
  model, gaps, _cached = await load_report_model(graph_id, report_id)
  try:
    out = xbrlkit_tools.disclosures(_loaded(graph_id, report_id, model), topic)
  except ToolError as exc:
    raise BlockNotFoundError(str(exc)) from exc
  external = _external_text_blocks(model)
  if external:
    for block in out.get("blocks") or []:
      if block.get("text_blocks"):
        block["text_blocks"] = _hosted_text(block["text_blocks"], external)
  return _stamp(out, graph_id, report_id, gaps)


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
  model, gaps, _cached = await load_report_model(graph_id, report_id)
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
  return _stamp(out, graph_id, report_id, gaps)


def _loaded(graph_id: str, report_id: str, model: XbrlModel) -> LoadedFiling:
  """The model as the xbrlkit tools take it. No document: the graph holds
  tagged facts, and on the shared repository a text block's value is a
  pointer, so the text tools' offsets have nothing to point into."""
  return LoadedFiling(id=report_id, source=graph_id, model=model, text="", sections=[])


def _external_text_blocks(model: XbrlModel) -> set[str]:
  """Text-block concepts whose value on this graph is a pointer to the text
  (the platform's ``value_type`` ``external``: a CDN URL), not the text."""
  external: set[str] = set()
  for fact in model.facts:
    if fact.value_kind != "text":
      continue
    concept = model.concepts.get(fact.concept_qname)
    if concept is None or not concept.is_textblock:
      continue
    if (fact.value_str or "").startswith(("http://", "https://")):
      external.add(fact.concept_qname)
  return external


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


def _stamp(
  out: dict[str, Any], graph_id: str, report_id: str, gaps: ImportGaps | None
) -> dict[str, Any]:
  stamped: dict[str, Any] = {"graph_id": graph_id, "report_id": report_id, **out}
  if gaps is not None and gaps.unresolved_references:
    stamped["graph_warnings"] = [
      f"{gaps.unresolved_references} rows referenced a node the report's slice "
      "did not carry and were skipped"
    ]
  return stamped
