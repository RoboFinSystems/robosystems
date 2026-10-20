"""Read operations for report definitions and rendered statements."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.orm import Session

if TYPE_CHECKING:
  from robosystems.models.api.extensions.report_package import (
    ReportPackageEnvelope,
  )

from sqlalchemy import ColumnElement, or_, true

from robosystems.config import env
from robosystems.config.storage.graph import get_report_bundle_key
from robosystems.logger import logger
from robosystems.models.api.extensions.dimensions import (
  UNALLOCATED,
  StatementDimension,
  StatementDimensionColumn,
  StatementDimensionResponse,
)
from robosystems.models.api.extensions.reports import (
  ConsolidationWorksheetResponse,
  FactRowResponse,
  LiveFinancialStatementResponse,
  LiveStatementFactRow,
  PeriodSpec,
  ReportBundleDownloadResponse,
  ReportLifecycle,
  ReportListResponse,
  ReportResponse,
  StatementResponse,
  StructureSummary,
  ValidationCheckResponse,
  WorksheetColumn,
  WorksheetRow,
)
from robosystems.models.extensions import Dimension, Entity, Report
from robosystems.models.extensions.roboledger import Structure
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.roboledger.entity_scope import (
  GroupMember,
  books_entity_id,
  find_entity_id,
  group_members,
  is_group_parent,
  membership_window,
  resolve_entity_id,
)
from robosystems.operations.roboledger.entry_status import landed_entry_bindparam
from robosystems.operations.roboledger.line_dimensions import (
  RESERVED_DIMENSION_TYPES,
  LineSlice,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  FactGrid,
  FactRow,
  _compute_prior_period,
  columns_before_the_books,
  generate_report_facts,
  group_columns_before_the_books,
  render_structure_view,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec as FactPeriodSpec,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  ReportFact as ReportFactData,
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report
from robosystems.operations.roboledger.reports.network_picker import (
  load_close_target_concept,
  load_entity_reporting_style,
  load_primary_reporting_style,
)
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)

VALID_BLOCK_TYPES = {
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
  "custom",
}

# Statement types accepted by the live (OLTP) path, shared by REST and MCP.
LIVE_STATEMENT_TYPES: tuple[str, ...] = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
)

# The statements offered to an AI operator: every live statement, the equity
# roll-forward included now that it articulates.
MCP_LIVE_STATEMENT_TYPES: tuple[str, ...] = LIVE_STATEMENT_TYPES

# What a live statement renders: one entity's books; the group parent and
# every member consolidated in full, summed for the months each was a member
# (the pre-adjustment total); or that sum plus the consolidation entity.
LIVE_STATEMENT_SCOPES: tuple[str, ...] = ("entity", "combined", "consolidated")

ANALYSIS_STATEMENT_TYPES: tuple[str, ...] = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
)


class StatementStructureNotFoundError(LookupError):
  """Raised when a statement's block_type isn't in the report's taxonomy."""


class CoaMappingNotFoundError(LookupError):
  """Raised when no CoA→GAAP mapping exists for ad-hoc statement generation."""


class ReportBundleNotAvailableError(LookupError):
  """Raised when a Report exists but has no published serialization bundle.

  Distinct from "report not found", which the read surfaces as ``None``.
  """


class BundleSigningError(RuntimeError):
  """Raised when the bundle artifact can't be signed or materialized.

  An infrastructure fault, as opposed to a missing bundle.
  """


# Clients follow the URL immediately; long-lived URLs are a share path.
PRESIGN_DEFAULT_SECONDS = 300
PRESIGN_MAX_SECONDS = 3600

_RDF_FLAVOR_VALUES: frozenset[str] = frozenset(f.value for f in RdfFlavor)
_XBRL_FLAVOR_VALUES: frozenset[str] = frozenset(f.value for f in XbrlFlavor)
_ALL_DOWNLOAD_FLAVORS: tuple[str, ...] = tuple(
  sorted(_RDF_FLAVOR_VALUES | _XBRL_FLAVOR_VALUES)
)


def generate_adhoc_private_statement(
  session: Session,
  *,
  statement_type: str,
  periods: list[FactPeriodSpec],
  reporting_style_id: str,
  entity_id: str | None = None,
  line_slice: LineSlice | None = None,
):
  """Build a one-shot statement from one entity's current ledger, with no
  saved Report; ``line_slice`` sums one management dimension member's lines.

  The Network comes from the entity's Reporting Style. The arc walk is scoped
  to rs-gaap-presentation, where every Default Style Network lives; a Style
  citing Networks from another taxonomy is not yet supported.

  Returns ``(grid, unmapped_count)``. Raises `CoaMappingNotFoundError` if the
  tenant has no CoA→GAAP mapping yet.
  """
  mapping = find_entity_mapping(session, entity_id, BOOK_FRAMEWORK)
  if mapping is None:
    raise CoaMappingNotFoundError(
      "No CoA→GAAP mapping found. Run the mapping workflow first."
    )

  taxonomy_row = session.execute(
    text(
      "SELECT id FROM taxonomies WHERE standard = 'rs-gaap-presentation' "
      "ORDER BY version DESC LIMIT 1"
    )
  ).fetchone()
  resolved_taxonomy_id = taxonomy_row.id if taxonomy_row else ""

  facts = generate_report_facts(
    session=session,
    taxonomy_id=resolved_taxonomy_id,
    mapping_id=mapping.id,
    periods=periods,
    close_target_qname=load_close_target_concept(session, reporting_style_id),
    entity_id=entity_id,
    line_slice=line_slice,
  )

  grid = render_structure_view(
    session=session,
    facts=facts.facts,
    block_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )
  if statement_type == "cash_flow_statement":
    grid.warnings = facts.warnings

  return grid, facts.unmapped_count


def build_periods(
  period_start: date | None,
  period_end: date | None,
  comparative: bool,
  periods_json: list | None = None,
) -> list[FactPeriodSpec]:
  """Build period specs from request data.

  `periods_json` (multi-period mode) is used directly when present; otherwise
  the window comes from period_start/period_end/comparative.
  """
  if periods_json:
    result = []
    for p in periods_json:
      if isinstance(p, dict):
        # JSONB returns dates as strings — parse them
        start = (
          date.fromisoformat(p["start"]) if isinstance(p["start"], str) else p["start"]
        )
        end = date.fromisoformat(p["end"]) if isinstance(p["end"], str) else p["end"]
        result.append(FactPeriodSpec(start=start, end=end, label=p["label"]))
      else:
        result.append(FactPeriodSpec(start=p.start, end=p.end, label=p.label))
    return result

  if period_start is None or period_end is None:
    return []

  specs = [FactPeriodSpec(start=period_start, end=period_end, label="Current")]
  if comparative:
    prior_start, prior_end = _compute_prior_period(period_start, period_end)
    specs.append(FactPeriodSpec(start=prior_start, end=prior_end, label="Prior"))
  return specs


def periods_to_json(periods: list[FactPeriodSpec]) -> list[dict]:
  """Serialize period specs for JSONB storage."""
  return [{"start": str(p.start), "end": str(p.end), "label": p.label} for p in periods]


def load_structures(session: Session, taxonomy_id: str) -> list[StructureSummary]:
  """Load available (statement-ish) structures for a taxonomy."""
  result = session.execute(
    text("""
      SELECT id, name, block_type FROM structures
      WHERE taxonomy_id = :taxonomy_id
        AND block_type NOT IN ('chart_of_accounts', 'coa_mapping')
        AND is_active = true
      ORDER BY block_type
    """),
    {"taxonomy_id": taxonomy_id},
  )
  return [
    StructureSummary(id=r.id, name=r.name, block_type=r.block_type) for r in result
  ]


def resolve_entity_name(session: Session, report_def: Report) -> str | None:
  """The name of the entity the report is about: for a shared-in report, the
  linked row standing for the sender's entity; else the report's own entity,
  falling back to the group parent. The consolidation entity's report is the
  group's consolidated statements, named for the group parent."""
  from robosystems.operations.roboledger.entity_scope import (
    find_linked_entity_id,
    find_parent_entity,
    report_entity_id,
  )

  facts_entity_id = report_entity_id(session, report_def.id)
  if report_def.source_graph_id:
    linked_id = find_linked_entity_id(
      session, report_def.source_graph_id, facts_entity_id
    )
    entity = session.get(Entity, linked_id) if linked_id else None
  else:
    own_id = report_def.entity_id or facts_entity_id
    entity = session.get(Entity, own_id) if own_id else None
    if entity is None:
      entity = find_parent_entity(session)
    elif entity.entity_role == "consolidation":
      parent = find_parent_entity(session)
      return consolidated_name(parent.name) if parent else entity.name
  return entity.name if entity else None


def consolidated_name(group_name: str) -> str:
  """How a consolidated report names the group it reports."""
  return f"{group_name} (consolidated)"


def report_to_response(
  report_def: Report,
  structures: list[StructureSummary],
  entity_name: str | None = None,
) -> ReportResponse:
  periods = None
  if report_def.periods:
    periods = [
      PeriodSpec(start=p["start"], end=p["end"], label=p["label"])
      for p in report_def.periods
    ]

  return ReportResponse(
    id=report_def.id,
    name=report_def.name,
    taxonomy_id=report_def.taxonomy_id,
    generation_status=report_def.generation_status,
    period_type=report_def.period_type,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    comparative=report_def.comparative,
    periods=periods,
    mapping_id=report_def.mapping_id,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    last_generated=report_def.last_generated,
    structures=structures,
    entity_id=report_def.entity_id,
    entity_name=entity_name,
    filing_status=report_def.filing_status,
    filed_at=report_def.filed_at,
    filed_by=report_def.filed_by,
    supersedes_id=report_def.supersedes_id,
    superseded_by_id=report_def.superseded_by_id,
    source_graph_id=report_def.source_graph_id,
    source_report_id=report_def.source_report_id,
    shared_at=report_def.shared_at,
  )


def report_entity_clause(entity_id: str | None, is_parent: bool) -> ColumnElement[bool]:
  """Predicate for the reports that are ``entity_id``'s. A report with no
  entity (shared in, or from before the column) is the group parent's; on a
  graph with no entity yet every report passes."""
  if entity_id is None:
    return true()
  if is_parent:
    # The group's consolidated reports, the consolidation entity's, are the
    # parent's to see too: the group is reported from its parent.
    consolidated = select(Entity.id).where(
      Entity.entity_role == "consolidation", Entity.source != "linked"
    )
    return or_(
      Report.entity_id == entity_id,
      Report.entity_id.is_(None),
      Report.entity_id.in_(consolidated),
    )
  return Report.entity_id == entity_id


def list_reports(
  session: Session,
  lifecycle: ReportLifecycle = ReportLifecycle.CURRENT,
  *,
  entity_id: str | None = None,
) -> ReportListResponse:
  """List one entity's report definitions, default the group parent's, most
  recent first.

  ``current`` (the default) leaves out archived reports, ``archived`` returns
  only those, and ``all`` returns every report.
  """
  entity_id = find_entity_id(session, entity_id)
  is_parent = entity_id is not None and is_group_parent(session, entity_id)
  query = (
    select(Report)
    .where(report_entity_clause(entity_id, is_parent))
    .order_by(Report.created_at.desc())
  )
  if lifecycle == ReportLifecycle.CURRENT:
    query = query.where(Report.filing_status != "archived")
  elif lifecycle == ReportLifecycle.ARCHIVED:
    query = query.where(Report.filing_status == "archived")
  rows = session.execute(query).scalars().all()

  structure_cache: dict[str, list[StructureSummary]] = {}
  reports = []
  for r in rows:
    if r.taxonomy_id not in structure_cache:
      structure_cache[r.taxonomy_id] = load_structures(session, r.taxonomy_id)
    entity_name = resolve_entity_name(session, r)
    reports.append(report_to_response(r, structure_cache[r.taxonomy_id], entity_name))

  return ReportListResponse(reports=reports)


def get_report(session: Session, report_id: str) -> ReportResponse | None:
  """Return a report definition with its structures, or None if not found."""
  report_def = session.get(Report, report_id)
  if report_def is None:
    return None
  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def get_report_download_url(
  session: Session,
  graph_id: str,
  report_id: str,
  flavor: str = RdfFlavor.HOLON_JSONLD.value,
  expires_in: int = PRESIGN_DEFAULT_SECONDS,
) -> ReportBundleDownloadResponse | None:
  """Resolve a presigned URL for a published Report's serialization bundle.

  The API never streams bytes. The holon is stamped at publish and again at
  filing; the Tavi and XBRL 2.1 are materialized on first download and cached
  under a ``generation_count``-versioned key, which never goes stale. (A generation
  stamped before the holon became the anchor materializes it the same way.)

  Returns ``None`` when ``report_id`` doesn't resolve. Raises
  :class:`ReportBundleNotAvailableError` when the report exists but has
  no published bundle, :class:`BundleSigningError` on a signing /
  materialization fault, and ``ValueError`` for an unrecognized flavor.
  """
  report = session.get(Report, report_id)
  if report is None:
    return None
  generation_count = int(report.generation_count or 0)

  # The holon and the Tavi gate on publication, not ``bundle_url``, because a
  # shared-in copy can carry either without the other.
  if flavor == RdfFlavor.HOLON_JSONLD.value:
    if report.generation_status != "published":
      raise ReportBundleNotAvailableError(
        f"Report '{report_id}' is not published — publish or regenerate the "
        f"report to produce a holon."
      )
    return _materialize_and_presign_holon(
      session=session,
      graph_id=graph_id,
      report_id=report_id,
      generation_count=generation_count,
      expires_in=expires_in,
      source_graph_id=report.source_graph_id,
    )

  # TAVI is in XbrlFlavor, so it precedes that branch.
  if flavor == XbrlFlavor.TAVI.value:
    if report.generation_status != "published":
      raise ReportBundleNotAvailableError(
        f"Report '{report_id}' is not published — publish or regenerate the "
        f"report to produce a Tavi model."
      )
    return _materialize_and_presign_tavi(
      session=session,
      graph_id=graph_id,
      report_id=report_id,
      generation_count=generation_count,
      expires_in=expires_in,
      source_graph_id=report.source_graph_id,
    )

  if not report.bundle_url:
    raise ReportBundleNotAvailableError(
      f"Report '{report_id}' has no published bundle — publish or "
      f"regenerate the report to produce one."
    )
  if flavor in _XBRL_FLAVOR_VALUES:
    return _materialize_and_presign_xbrl(
      session=session,
      graph_id=graph_id,
      report_id=report_id,
      flavor=XbrlFlavor(flavor),
      generation_count=generation_count,
      expires_in=expires_in,
      source_graph_id=report.source_graph_id,
    )
  raise ValueError(
    f"Unsupported download format '{flavor}'. "
    f"Supported flavors: {', '.join(_ALL_DOWNLOAD_FLAVORS)}."
  )


def _refuse_local_rederivation(
  report_id: str, source_graph_id: str | None, artifact: str
) -> None:
  """Refuse to re-derive a received report's artifact from local rows.

  For a shared-in report the sender's artifact is the truth and the rows are a
  partial projection of it, so a cache miss is a miss, not a build order.
  """
  if source_graph_id is None:
    return
  raise ReportBundleNotAvailableError(
    f"Report '{report_id}' was shared from another graph and its {artifact} is "
    f"no longer in storage. A received report is served as the sender's "
    f"published artifact and is never re-derived locally; ask the sender to "
    f"share it again."
  )


def _materialize_and_presign_xbrl(
  session: Session,
  graph_id: str,
  report_id: str,
  flavor: XbrlFlavor,
  generation_count: int,
  expires_in: int,
  source_graph_id: str | None = None,
) -> ReportBundleDownloadResponse:
  """Presign the XBRL zip, materializing + caching it on first download.

  Serialization imports are deferred to keep Arelle off the hot path.
  """
  bucket = env.USER_DATA_BUCKET
  key = get_report_bundle_key(graph_id, report_id, generation_count, extension=".zip")
  s3 = S3Client()

  if not s3.object_exists(bucket, key):
    _refuse_local_rederivation(report_id, source_graph_id, "XBRL bundle")
    from robosystems.operations.serialization import (
      build_report_bundle,
      serialize_to_xbrl,
    )

    bundle = build_report_bundle(session, graph_id, report_id)
    zip_bytes = serialize_to_xbrl(bundle, flavor)
    uploaded = s3.upload_bytes(
      content=zip_bytes,
      bucket=bucket,
      key=key,
      content_type="application/zip",
      metadata={"report-id": report_id, "graph-id": graph_id},
    )
    if not uploaded:
      logger.error(
        "XBRL bundle upload failed for report %s at s3://%s/%s", report_id, bucket, key
      )
      raise BundleSigningError(
        f"Failed to materialize XBRL bundle for report '{report_id}'."
      )

  download_url = s3.generate_presigned_url(
    bucket=bucket,
    key=key,
    expires_in=expires_in,
    response_content_type="application/zip",
    response_content_disposition=(
      f'attachment; filename="{report_id}-g{generation_count}.zip"'
    ),
  )
  if download_url is None:
    raise BundleSigningError(
      f"Failed to sign download URL for report '{report_id}' XBRL bundle."
    )

  return ReportBundleDownloadResponse(
    download_url=download_url,
    expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
    content_type="application/zip",
    format=flavor.value,
    generation_count=generation_count,
    # The emitter always strips tenant-authored notes; declared on cache hits
    # too, so the file is not silently incomplete.
    omitted_content=["disclosure_notes"],
  )


def _materialize_and_presign_holon(
  session: Session,
  graph_id: str,
  report_id: str,
  generation_count: int,
  expires_in: int,
  source_graph_id: str | None = None,
) -> ReportBundleDownloadResponse:
  """Presign the JSON-LD holon, materializing + caching it on first download."""
  bucket = env.USER_DATA_BUCKET
  key = get_report_bundle_key(
    graph_id, report_id, generation_count, extension=".holon.jsonld"
  )
  s3 = S3Client()

  if not s3.object_exists(bucket, key):
    _refuse_local_rederivation(report_id, source_graph_id, "holon")
    from robosystems.operations.serialization import (
      build_report_bundle,
      serialize_to_holon_jsonld,
    )

    bundle = build_report_bundle(session, graph_id, report_id)
    holon = serialize_to_holon_jsonld(bundle)
    uploaded = s3.upload_string(
      content=holon,
      bucket=bucket,
      key=key,
      content_type="application/ld+json",
      metadata={"report-id": report_id, "graph-id": graph_id},
    )
    if not uploaded:
      logger.error(
        "Holon upload failed for report %s at s3://%s/%s", report_id, bucket, key
      )
      raise BundleSigningError(
        f"Failed to materialize holon bundle for report '{report_id}'."
      )

  download_url = s3.generate_presigned_url(
    bucket=bucket,
    key=key,
    expires_in=expires_in,
    response_content_type="application/ld+json",
    response_content_disposition=(
      f'attachment; filename="{report_id}-g{generation_count}.holon.jsonld"'
    ),
  )
  if download_url is None:
    raise BundleSigningError(
      f"Failed to sign download URL for report '{report_id}' holon bundle."
    )

  return ReportBundleDownloadResponse(
    download_url=download_url,
    expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
    content_type="application/ld+json",
    format=RdfFlavor.HOLON_JSONLD.value,
    generation_count=generation_count,
  )


def _materialize_and_presign_tavi(
  session: Session,
  graph_id: str,
  report_id: str,
  generation_count: int,
  expires_in: int,
  source_graph_id: str | None = None,
) -> ReportBundleDownloadResponse:
  """Presign the Tavi compiled model, materializing + caching it on first download."""
  bucket = env.USER_DATA_BUCKET
  key = get_report_bundle_key(
    graph_id, report_id, generation_count, extension=".tavi.json"
  )
  s3 = S3Client()

  if not s3.object_exists(bucket, key):
    _refuse_local_rederivation(report_id, source_graph_id, "Tavi model")
    from robosystems.operations.serialization import (
      build_report_bundle,
      serialize_to_tavi,
    )

    bundle = build_report_bundle(session, graph_id, report_id)
    uploaded = s3.upload_bytes(
      content=serialize_to_tavi(bundle),
      bucket=bucket,
      key=key,
      content_type="application/json",
      metadata={"report-id": report_id, "graph-id": graph_id},
    )
    if not uploaded:
      logger.error(
        "Tavi upload failed for report %s at s3://%s/%s", report_id, bucket, key
      )
      raise BundleSigningError(
        f"Failed to materialize Tavi model for report '{report_id}'."
      )

  download_url = s3.generate_presigned_url(
    bucket=bucket,
    key=key,
    expires_in=expires_in,
    response_content_type="application/json",
    response_content_disposition=(
      f'attachment; filename="{report_id}-g{generation_count}.tavi.json"'
    ),
  )
  if download_url is None:
    raise BundleSigningError(
      f"Failed to sign download URL for report '{report_id}' Tavi model."
    )

  from robosystems.operations.serialization.xbrl.tavi import TAVI_OMITTED_CONTENT

  return ReportBundleDownloadResponse(
    download_url=download_url,
    expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
    content_type="application/json",
    format=XbrlFlavor.TAVI.value,
    generation_count=generation_count,
    # Bundle content the compiled model has no home for; declared on cache hits.
    omitted_content=list(TAVI_OMITTED_CONTENT),
  )


# ``ReportPackageItem.display_order`` by block type; unlisted types sort at 50.
_BLOCK_TYPE_DISPLAY_ORDER: dict[str, int] = {
  "balance_sheet": 1,
  "income_statement": 2,
  "cash_flow_statement": 3,
  "equity_statement": 4,
  "schedule": 100,
}


def get_report_package(
  session: Session, report_id: str
) -> ReportPackageEnvelope | None:
  """Rehydrate a Report as metadata plus one full envelope per FactSet.

  Returns ``None`` when the Report doesn't exist. Items are ordered by block
  type (BS → IS → CF → Equity → Schedule), ties broken by FactSet id.
  """
  from robosystems.models.api.extensions.report_package import (
    ReportPackageEnvelope,
    ReportPackageItem,
  )
  from robosystems.models.extensions.roboledger import FactSet
  from robosystems.operations.information_block.reads import (
    get_information_block_for_fact_set,
  )

  report_def = session.get(Report, report_id)
  if report_def is None:
    return None

  entity_name = resolve_entity_name(session, report_def)

  # Inner join: a structure-less FactSet cannot be rendered as a block.
  rows = session.execute(
    select(FactSet, Structure)
    .join(Structure, Structure.id == FactSet.structure_id)
    .where(FactSet.report_id == report_id)
    .order_by(FactSet.created_at)
  ).all()

  items: list[ReportPackageItem] = []
  for fs, structure in rows:
    envelope = get_information_block_for_fact_set(session, fs.id)
    if envelope is None:
      # Structure is not a registered block type.
      continue
    block_type = structure.block_type if structure is not None else None
    items.append(
      ReportPackageItem(
        fact_set_id=fs.id,
        structure_id=fs.structure_id,
        display_order=_BLOCK_TYPE_DISPLAY_ORDER.get(block_type or "", 50),
        block=envelope,
      )
    )

  items.sort(key=lambda it: (it.display_order, it.fact_set_id))

  return ReportPackageEnvelope(
    id=report_def.id,
    name=report_def.name,
    description=report_def.description,
    taxonomy_id=report_def.taxonomy_id,
    period_type=report_def.period_type,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    generation_status=report_def.generation_status,
    last_generated=report_def.last_generated,
    filing_status=report_def.filing_status,
    filed_at=report_def.filed_at,
    filed_by=report_def.filed_by,
    supersedes_id=report_def.supersedes_id,
    superseded_by_id=report_def.superseded_by_id,
    source_graph_id=report_def.source_graph_id,
    source_report_id=report_def.source_report_id,
    shared_at=report_def.shared_at,
    entity_name=entity_name,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    created_by=report_def.created_by,
    items=items,
  )


def get_statement(
  session: Session,
  graph_id: str,
  report_id: str,
  block_type: str,
  reporting_style_id: str | None = None,
) -> StatementResponse | None:
  """Render a financial statement for a report + block_type.

  Returns `None` when the report doesn't exist. Raises
  `StatementStructureNotFoundError` when the block_type isn't in the report's
  taxonomy. ``reporting_style_id`` defaults to the Style of the entity the
  report is about (the group parent's for a report with none recorded).
  """
  if block_type not in VALID_BLOCK_TYPES:
    raise ValueError(
      f"Invalid block_type '{block_type}'. "
      f"Must be one of: {', '.join(sorted(VALID_BLOCK_TYPES))}"
    )

  report_def = session.get(Report, report_id)
  if report_def is None:
    return None

  periods = build_periods(
    report_def.period_start,
    report_def.period_end,
    report_def.comparative,
    report_def.periods,
  )

  if not periods:
    return StatementResponse(
      report_id=report_def.id,
      structure_id="",
      structure_name="",
      block_type=block_type,
    )

  # Both guard against double-counting: the trait subquery keeps one row per
  # fact when an element has primary traits in several categories, and the
  # block_type filter drops copies of the same element in other statements'
  # FactSets (NetIncomeLoss sits in IS, CF and SE; facts sum per element).
  fact_rows = session.execute(
    text("""
      SELECT rf.element_id, rf.value, rf.period_start, rf.period_end,
             rf.period_type, e.qname, e.name,
             trait_info.identifier AS trait, e.balance_type
      FROM facts rf
      JOIN fact_sets fs ON fs.id = rf.fact_set_id
      JOIN structures s ON s.id = fs.structure_id
      JOIN elements e ON e.id = rf.element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) trait_info ON trait_info.element_id = e.id
      WHERE fs.report_id = :report_id
        AND s.block_type = :block_type
        AND rf.fact_type = 'Numeric'
    """),
    {"report_id": report_id, "block_type": block_type},
  )

  facts = [
    ReportFactData(
      element_id=r.element_id,
      element_qname=r.qname,
      element_name=r.name,
      classification=r.trait,
      balance_type=r.balance_type or "debit",
      value=r.value,
      period_start=r.period_start,
      period_end=r.period_end,
      period_type=r.period_type,
    )
    for r in fact_rows
  ]

  if not facts:
    return StatementResponse(
      report_id=report_def.id,
      structure_id="",
      structure_name="",
      block_type=block_type,
      periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in periods],
    )

  if reporting_style_id is None:
    # A consolidated report renders under the group parent's Style.
    reporting_style_id = (
      load_entity_reporting_style(
        session, books_entity_id(session, report_def.entity_id)
      )
      if report_def.entity_id
      else load_primary_reporting_style(session)
    )
  grid = render_structure_view(
    session=session,
    facts=facts,
    block_type=block_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )

  if not grid.structure_id:
    raise StatementStructureNotFoundError(block_type)

  validation = validate_report(
    block_type,
    grid.rows,
    period_labels=[p.label for p in grid.periods],
    before_the_books=columns_before_the_books(
      session,
      block_type,
      grid.periods,
      [resolve_entity_id(session, report_def.entity_id)],
    ),
  )

  # Abstract rows are presentation scaffolding carrying their descendants' sum.
  rows = [
    FactRowResponse(
      element_id=r.element_id,
      element_qname=r.element_qname,
      element_name=r.element_name,
      trait=r.classification,
      values=r.values,
      is_subtotal=r.is_subtotal,
      depth=r.depth,
      period_role=r.period_role,
    )
    for r in grid.rows
    if not r.is_abstract
  ]

  validation_resp = (
    ValidationCheckResponse(
      passed=validation.passed,
      status=validation.status,
      checks=validation.checks,
      failures=validation.failures,
      warnings=validation.warnings,
    )
    if validation
    else None
  )

  return StatementResponse(
    report_id=report_def.id,
    structure_id=grid.structure_id,
    structure_name=grid.structure_name,
    block_type=block_type,
    periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in grid.periods],
    rows=rows,
    validation=validation_resp,
    unmapped_count=grid.unmapped_count,
  )


# ── Live (ad-hoc OLTP) financial statement ────────────────────────────────


def _last_day_of_month(year: int, month: int) -> date:
  if month == 12:
    return date(year, 12, 31)
  return date(year, month + 1, 1) - timedelta(days=1)


def resolve_reporting_window(
  session: Session,
  *,
  period_start: date | None,
  period_end: date | None,
  period_type: str | None,
  fiscal_year: int | None,
) -> tuple[date, date]:
  """Pick a reporting window from fuzzy inputs.

  Explicit `period_start` / `period_end` always win. Otherwise the
  window is derived from `period_type`:

  - ``annual`` — fiscal year aligned on the graph's
    ``fiscal_year_start_month`` (January when no calendar exists).
    ``fiscal_year`` selects which year; defaults to the current year.
  - ``quarterly`` — current calendar quarter (3 months).
  - anything else (``instant`` or unset) — current calendar month.
  """
  today = date.today()

  if period_end or period_start:
    end = period_end or _last_day_of_month(today.year, today.month)
    start = period_start or end.replace(day=1)
    return start, end

  if period_type == "annual":
    fy_start_month = get_fiscal_year_start_month(session)
    year = fiscal_year if fiscal_year is not None else today.year
    start = date(year, fy_start_month, 1)
    end_year = year if fy_start_month == 1 else year + 1
    end_month = fy_start_month - 1 if fy_start_month > 1 else 12
    end = _last_day_of_month(end_year, end_month)
    return start, end

  if period_type == "quarterly":
    quarter = (today.month - 1) // 3
    q_start_month = quarter * 3 + 1
    start = date(today.year, q_start_month, 1)
    end = _last_day_of_month(today.year, q_start_month + 2)
    return start, end

  end = _last_day_of_month(today.year, today.month)
  start = end.replace(day=1)
  return start, end


def build_current_and_prior_periods(start: date, end: date) -> list[FactPeriodSpec]:
  """Return [current, prior] period specs of matching duration."""
  prior_start, prior_end = _compute_prior_period(start, end)
  return [
    FactPeriodSpec(start=start, end=end, label="Current"),
    FactPeriodSpec(start=prior_start, end=prior_end, label="Prior"),
  ]


class InvalidStatementScopeError(ValueError):
  """A scope that is not one of ``LIVE_STATEMENT_SCOPES``."""

  def __init__(self, scope: str) -> None:
    super().__init__(
      f"Unknown scope {scope!r}. Valid scopes: {', '.join(LIVE_STATEMENT_SCOPES)}."
    )


class CombinedViewOnParentOnlyError(ValueError):
  """A combined statement is read on the group parent, never on a subsidiary."""

  def __init__(self, entity_id: str) -> None:
    super().__init__(
      f"Entity {entity_id!r} is a subsidiary; the combined statement of the "
      "group is read on the group parent (omit entity_id)."
    )


def _combined_statement(
  session: Session,
  *,
  statement_type: str,
  periods: list[FactPeriodSpec],
  reporting_style_id: str,
  parent_id: str,
  consolidated: bool,
  line_slice: LineSlice | None = None,
):
  """The group's statement under the parent's Style, summed row by row at
  the rs-gaap concepts, where every entity's own chart has been mapped: the
  parent and every operating entity consolidated in full, each for the
  periods it was a member, and — ``consolidated`` — the consolidation
  entity, whose entries are the worksheet's adjustments column (``combined``
  is the same sum without it). An equity-method or cost member is never
  summed. An entity with no mapping is left out; with none mapped at all
  the parent's error stands.

  Returns ``(grid, unmapped_count, combined_entity_ids, omitted)``, where
  ``omitted`` are the columns a member belongs to only part of, which the
  statement cannot state (``member_windows``).
  """
  base: FactGrid | None = None
  unmapped_count = 0
  combined: list[str] = []
  omitted: set[int] = set()
  first_error: CoaMappingNotFoundError | None = None
  for member in group_members(session, parent_id, consolidated=consolidated):
    windows, partial = member_windows(member, statement_type, periods)
    summed = [w is not None for w in windows]
    # An unmapped member is never summed, so it holds no column back.
    if partial and find_entity_mapping(session, member.id, BOOK_FRAMEWORK):
      omitted |= partial
    if not any(summed):
      continue
    try:
      grid, unmapped = generate_adhoc_private_statement(
        session,
        statement_type=statement_type,
        periods=[w or p for w, p in zip(windows, periods, strict=True)],
        reporting_style_id=reporting_style_id,
        entity_id=member.id,
        line_slice=line_slice,
      )
    except CoaMappingNotFoundError as exc:
      first_error = first_error or exc
      continue
    if not all(summed):
      _keep_periods(grid, summed)
    combined.append(member.id)
    unmapped_count += unmapped
    if base is None:
      base = grid
      continue
    _add_into(base, grid)
  if base is None:
    assert first_error is not None
    raise first_error
  return base, unmapped_count, combined, omitted


_BEFORE_THE_BOOKS = (
  "Every column reaches back before the books begin; record the window "
  "from the first month the books cover."
)
_NOTHING_TO_STATE = (
  "Every column spans a member joining or leaving the group, which a cash "
  "flow or equity statement cannot state; read the months on either side."
)


def member_windows(
  member: GroupMember, statement_type: str, periods: list[FactPeriodSpec]
) -> tuple[list[FactPeriodSpec | None], set[int]]:
  """Per column, the window a member is read over, None where it is not
  summed, and the columns it belongs to only part of that the statement
  cannot state. A balance sheet holds the members of the month it is struck
  in; an income statement sums a member's own months of the column; a cash
  flow or equity statement over part of a member's time would show balances
  arriving or leaving with no line for them (an acquisition, a
  deconsolidation), so that column is left out."""
  windows: list[FactPeriodSpec | None] = []
  partial: set[int] = set()
  for i, p in enumerate(periods):
    if statement_type == "balance_sheet":
      held = membership_window(member, p.end.replace(day=1), p.end)
      windows.append(p if held is not None else None)
      continue
    window = membership_window(member, p.start, p.end)
    if window is None or window == (p.start, p.end):
      windows.append(p if window is not None else None)
    elif statement_type == "income_statement":
      windows.append(FactPeriodSpec(start=window[0], end=window[1], label=p.label))
    else:
      partial.add(i)
      windows.append(None)
  return windows, partial


def _keep_periods(grid: FactGrid, summed: list[bool]) -> None:
  """Zero the columns of the periods the entity was not a member for, so a
  subsidiary that joined this period adds nothing to the prior column."""
  for row in grid.rows:
    row.values = [
      value if keep else 0.0 for value, keep in zip(row.values, summed, strict=False)
    ]


def _row_key(row: FactRow) -> tuple[str, str | None]:
  """A row's identity across grids: its concept, and on a roll-forward which
  of the two balances it is."""
  return row.element_qname, row.period_role


def _add_into(base: FactGrid, other: FactGrid) -> None:
  """Sum ``other``'s rows into ``base``'s, matched by concept (and on a
  roll-forward by which balance the row is). Both come from
  the same Style, and every row carries its place in that Style's full
  presentation order (``FactRow.position``), so a row only ``other`` has
  (the parent's was all-zero and dropped) is inserted where that order puts
  it — inside its subtotal, and a line like the noncontrolling-interest
  share above net income even when the entity that carries it rendered none
  of the subtotals in between. Adjacency in ``other``'s own sparse grid is
  not the order: the consolidation entity's income statement has no
  operating-income or pre-tax rows, and the share would have landed between
  them."""
  shared: dict[tuple[str, str | None], FactRow] = {}
  for row in base.rows:
    shared.setdefault(_row_key(row), row)
  for row in other.rows:
    target = shared.get(_row_key(row))
    if target is None:
      at = next(
        (i for i, r in enumerate(base.rows) if r.position > row.position),
        len(base.rows),
      )
      base.rows.insert(at, row)
      shared[_row_key(row)] = row
    else:
      target.values = [
        (a or 0.0) + (b or 0.0) if a is not None or b is not None else None
        for a, b in zip(target.values, row.values, strict=False)
      ]


@dataclass(frozen=True)
class _WorksheetEntity:
  member: GroupMember
  name: str
  method: str
  ownership_pct: float | None

  @property
  def kind(self) -> str:
    if self.member.role == "consolidation":
      return "adjustments"
    return "member" if self.method == "full" else "not_summed"


def _worksheet_entities(session: Session, parent_id: str) -> list[_WorksheetEntity]:
  """Every own entity in worksheet order: the parent, the members
  consolidated in full by creation, the consolidation entity, then the
  members outside the sum."""
  rows = session.execute(
    text(
      "SELECT id, name, entity_role, consolidation_method, ownership_pct, "
      "       joined_on, left_on FROM entities "
      "WHERE source <> 'linked' "
      "ORDER BY (id <> :parent), "
      "  CASE WHEN entity_role = 'consolidation' THEN 1 "
      "       WHEN consolidation_method = 'full' THEN 0 ELSE 2 END, "
      "  created_at ASC, id ASC"
    ),
    {"parent": parent_id},
  ).all()
  return [
    _WorksheetEntity(
      member=GroupMember(
        str(row.id),
        str(row.entity_role),
        # The parent belongs to its own group for every period.
        None if row.id == parent_id else row.joined_on,
        None if row.id == parent_id else row.left_on,
      ),
      name=str(row.name),
      method="full" if row.id == parent_id else str(row.consolidation_method),
      ownership_pct=float(row.ownership_pct) if row.ownership_pct is not None else None,
    )
    for row in rows
  ]


def _sum_grids(grids: list[FactGrid]) -> FactGrid | None:
  """The row-by-row sum of ``grids``, leaving each of them untouched."""
  total: FactGrid | None = None
  for grid in grids:
    if total is None:
      total = copy.deepcopy(grid)
    else:
      _add_into(total, copy.deepcopy(grid))
  return total


def get_consolidation_worksheet(
  session: Session,
  *,
  graph_id: str,
  statement_type: str,
  period_start: date,
  period_end: date,
  limit: int = 1000,
) -> ConsolidationWorksheetResponse:
  """The group's statement for one window with the entity as the column:
  the parent and each member consolidated in full, their ``combined`` sum,
  the consolidation entity's adjustments, the ``consolidated`` total, and
  each equity-method or cost member beside the sum, never in it. Every
  column renders under the parent's Style; the totals are the same sums
  ``get_live_financial_statement`` reads for its two group scopes.

  Raises ``CoaMappingNotFoundError`` when no summed entity is mapped."""
  parent_id = resolve_entity_id(session, None)
  reporting_style_id = load_entity_reporting_style(session, parent_id)
  periods = build_current_and_prior_periods(period_start, period_end)

  # Each list holds (column, grid), already in worksheet order.
  summed_columns: list[tuple[WorksheetColumn, FactGrid]] = []
  adjustment_columns: list[tuple[WorksheetColumn, FactGrid]] = []
  outside_columns: list[tuple[WorksheetColumn, FactGrid]] = []
  members: list[FactGrid] = []
  adjustments: list[FactGrid] = []
  unmapped_entity_ids: list[str] = []
  unmapped_count = 0
  first_error: CoaMappingNotFoundError | None = None
  partly: set[int] = set()
  summed_members: list[GroupMember] = []
  for entity in _worksheet_entities(session, parent_id):
    windows, partial = member_windows(entity.member, statement_type, periods)
    summed = [w is not None for w in windows]
    # A member outside the sum matters only as its own column.
    needed = summed[0] if entity.kind == "not_summed" else any(summed)
    if (
      entity.kind != "not_summed"
      and partial
      and find_entity_mapping(session, entity.member.id, BOOK_FRAMEWORK)
    ):
      partly |= partial
    if not needed:
      continue
    try:
      grid, unmapped = generate_adhoc_private_statement(
        session,
        statement_type=statement_type,
        periods=[w or p for w, p in zip(windows, periods, strict=True)],
        reporting_style_id=reporting_style_id,
        entity_id=entity.member.id,
      )
    except CoaMappingNotFoundError as exc:
      first_error = first_error or exc
      unmapped_entity_ids.append(entity.member.id)
      continue
    if not all(summed):
      _keep_periods(grid, summed)
    kind = entity.kind
    if kind != "not_summed":
      unmapped_count += unmapped
      summed_members.append(entity.member)
      (members if kind == "member" else adjustments).append(grid)
    # The prior window still counts a member that joined in this one; only a
    # member for the window itself gets a column.
    if not summed[0]:
      continue
    column = WorksheetColumn(
      key=entity.member.id,
      label=entity.name,
      kind=kind,
      entity_id=entity.member.id,
      consolidation_method=entity.method,
      ownership_pct=entity.ownership_pct,
      in_sum=kind != "not_summed",
    )
    {
      "member": summed_columns,
      "adjustments": adjustment_columns,
      "not_summed": outside_columns,
    }[kind].append((column, grid))

  combined = _sum_grids(members)
  if combined is None:
    # The parent is summed for every period, so only its missing mapping
    # leaves nothing to sum.
    raise first_error or CoaMappingNotFoundError(
      "No CoA→GAAP mapping found. Run the mapping workflow first."
    )
  consolidated = _sum_grids([combined, *adjustments])
  assert consolidated is not None
  ordered = [
    *summed_columns,
    (
      WorksheetColumn(key="combined", label="Combined", kind="combined", in_sum=True),
      combined,
    ),
    *adjustment_columns,
    (
      WorksheetColumn(
        key="consolidated", label="Consolidated", kind="consolidated", in_sum=True
      ),
      consolidated,
    ),
    *outside_columns,
  ]

  # The live statement's omissions: a column it would leave out is not a
  # total here either.
  window = periods[0]
  omitted = partly | group_columns_before_the_books(
    session, statement_type, periods, summed_members
  )
  if omitted:
    kept = [i for i in range(len(periods)) if i not in omitted]
    for _, grid in ordered:
      for row in grid.rows:
        row.values = [row.values[i] for i in kept if i < len(row.values)]
  omitted_periods = [periods[i] for i in sorted(omitted)]
  periods = [p for i, p in enumerate(periods) if i not in omitted]

  validation = validate_report(
    statement_type,
    consolidated.rows,
    period_labels=[p.label for p in periods],
    warnings=consolidated.warnings,
  )
  if 0 in omitted:
    validation.passed = False
    validation.status = "inconclusive"
    validation.warnings = [
      _NOTHING_TO_STATE if omitted <= partly else _BEFORE_THE_BOOKS
    ]

  # Every column's lines in the Style's order: the consolidated sum and the
  # members outside it hold each line once, where the Style puts it.
  skeleton = _sum_grids([consolidated, *(g for _, g in outside_columns)])
  assert skeleton is not None
  by_column = [{_row_key(r): r for r in g.rows} for _, g in ordered]
  rows: list[WorksheetRow] = []
  for line in [] if 0 in omitted else skeleton.rows:
    if line.is_abstract:
      continue
    values: list[float | None] = []
    for index in by_column:
      found = index.get(_row_key(line))
      values.append(found.values[0] if found is not None and found.values else 0.0)
    if line.period_role is None and not any(v for v in values):
      continue
    rows.append(
      WorksheetRow(
        qname=line.element_qname,
        name=line.element_name,
        trait=line.classification,
        values=values,
        depth=line.depth,
        is_subtotal=line.is_subtotal,
        period_role=line.period_role,
      )
    )

  truncated = len(rows) > limit
  return ConsolidationWorksheetResponse(
    graph_id=graph_id,
    entity_id=parent_id,
    statement_type=statement_type,
    period=PeriodSpec(start=window.start, end=window.end, label=window.label),
    omitted_periods=[
      PeriodSpec(start=p.start, end=p.end, label=p.label) for p in omitted_periods
    ],
    columns=[c for c, _ in ordered],
    rows=rows[:limit],
    validation=ValidationCheckResponse(
      passed=validation.passed,
      status=validation.status,
      checks=validation.checks,
      failures=validation.failures,
      warnings=validation.warnings,
    ),
    unmapped_entity_ids=unmapped_entity_ids,
    unmapped_count=unmapped_count,
    truncated=truncated,
  )


class DimensionalStatementError(ValueError):
  """A management slice asked of a statement it cannot slice, or of an axis or
  member the graph does not have."""


@dataclass
class _Rendered:
  grid: FactGrid
  unmapped_count: int
  combined_entity_ids: list[str]
  # Columns a member belongs to only part of (group scopes).
  partly: set[int]


def _render_live(
  session: Session,
  *,
  statement_type: str,
  periods: list[FactPeriodSpec],
  reporting_style_id: str,
  entity_id: str,
  scope: str,
  line_slice: LineSlice | None = None,
) -> _Rendered:
  if scope != "entity":
    grid, unmapped_count, combined_entity_ids, partly = _combined_statement(
      session,
      statement_type=statement_type,
      periods=periods,
      reporting_style_id=reporting_style_id,
      parent_id=entity_id,
      consolidated=scope == "consolidated",
      line_slice=line_slice,
    )
    return _Rendered(grid, unmapped_count, combined_entity_ids, partly)
  grid, unmapped_count = generate_adhoc_private_statement(
    session,
    statement_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
    entity_id=entity_id,
    line_slice=line_slice,
  )
  return _Rendered(grid, unmapped_count, [], set())


def _check_dimension(
  session: Session, statement_type: str, dimension: StatementDimension
) -> None:
  if statement_type != "income_statement":
    raise DimensionalStatementError(
      "A statement is read by dimension on the income statement only: the "
      "cash and equity lines a balance sheet, cash flow or equity statement "
      "sums are not tagged. Read the trial balance by dimension for balances."
    )
  if dimension.type in RESERVED_DIMENSION_TYPES:
    raise DimensionalStatementError(
      f"The {dimension.type} axis tags forecast facts, not ledger lines."
    )
  if dimension.member_id in (None, UNALLOCATED):
    return
  member = session.get(Dimension, dimension.member_id)
  if member is None or member.dimension_type != dimension.type:
    raise DimensionalStatementError(
      f"No {dimension.type} member {dimension.member_id}."
    )


def _slice_of(dimension: StatementDimension) -> LineSlice:
  member = dimension.member_id
  return LineSlice(dimension.type, None if member == UNALLOCATED else member)


def _members_in_window(
  session: Session,
  dimension_type: str,
  entity_ids: list[str],
  start: date,
  end: date,
) -> list[tuple[str, str]]:
  """The axis's members on landed lines of these entities in the window, by
  name: the columns of a statement read by member."""
  rows = session.execute(
    text("""
      SELECT DISTINCT d.id, d.name
      FROM dimensions d
      JOIN line_item_dimensions lid ON lid.dimension_id = d.id
      JOIN line_items li ON li.id = lid.line_item_id
      JOIN entries e ON e.id = li.entry_id
      WHERE d.dimension_type = :dimension_type
        AND e.status IN :landed_entry_statuses
        AND e.entity_id = ANY(:entity_ids)
        AND e.posting_date BETWEEN :start AND :end
      ORDER BY d.name, d.id
    """).bindparams(landed_entry_bindparam()),
    {
      "dimension_type": dimension_type,
      "entity_ids": entity_ids,
      "start": start,
      "end": end,
    },
  ).all()
  return [(str(r.id), r.name) for r in rows]


def _by_member(
  session: Session,
  *,
  statement_type: str,
  window: FactPeriodSpec,
  reporting_style_id: str,
  entity_id: str,
  scope: str,
  dimension_type: str,
) -> tuple[_Rendered, list[StatementDimensionColumn], list[str]]:
  """One column per member, then Unallocated, then Total, for one window,
  and the rows whose members plus Unallocated miss the Total."""
  total = _render_live(
    session,
    statement_type=statement_type,
    periods=[window],
    reporting_style_id=reporting_style_id,
    entity_id=entity_id,
    scope=scope,
  )
  entity_ids = total.combined_entity_ids or [entity_id]
  columns = [
    StatementDimensionColumn(member_id=member_id, name=name)
    for member_id, name in _members_in_window(
      session, dimension_type, entity_ids, window.start, window.end
    )
  ]
  columns.append(StatementDimensionColumn(member_id=UNALLOCATED, name="Unallocated"))
  values: dict[tuple[str, str | None], list[float]] = {
    _row_key(row): [] for row in total.grid.rows
  }
  for column in columns:
    sliced = _render_live(
      session,
      statement_type=statement_type,
      periods=[window],
      reporting_style_id=reporting_style_id,
      entity_id=entity_id,
      scope=scope,
      line_slice=LineSlice(
        dimension_type,
        None if column.member_id == UNALLOCATED else column.member_id,
      ),
    )
    by_key = {_row_key(row): row.values for row in sliced.grid.rows}
    for key, row_values in values.items():
      found = by_key.get(key) or [0.0]
      row_values.append(float(found[0] or 0.0))
  columns.append(StatementDimensionColumn(member_id=None, name="Total"))
  differences: list[str] = []
  for row in total.grid.rows:
    row_total = float((row.values or [0.0])[0] or 0.0)
    parts = values[_row_key(row)]
    if abs(sum(parts) - row_total) > 0.005:
      differences.append(row.element_qname)
    row.values = [*parts, row_total]
  return total, columns, differences


def get_live_financial_statement(
  session: Session,
  *,
  graph_id: str,
  statement_type: str,
  period_start: date,
  period_end: date,
  limit: int = 1000,
  reporting_style_id: str | None = None,
  entity_id: str | None = None,
  consolidated: bool = False,
  scope: str | None = None,
  dimension: StatementDimension | None = None,
) -> LiveFinancialStatementResponse:
  """Render a current + prior ad-hoc statement from OLTP data: ``scope``
  ``entity`` (the default) is one entity's books, default the group
  parent's; ``combined`` sums the parent and every member consolidated in
  full under the parent's Style, per concept, for the months each was a
  member — the pre-adjustment total, nothing eliminated; ``consolidated``
  adds the consolidation entity, whose entries are the adjustments column.
  ``consolidated=True`` is the older spelling of ``scope="consolidated"``.

  ``dimension`` slices an income statement by a management dimension: with
  a ``member_id``, that member's lines only (``unallocated``: the lines no
  member of the axis tags), current and prior; without one, the current
  window with one column per member, then Unallocated, then Total, and
  whether every row's members plus Unallocated equal its Total.

  Drops abstract and all-zero rows and caps at ``limit`` (``truncated``).
  ``reporting_style_id`` defaults to the entity's Style. Raises
  ``CoaMappingNotFoundError`` when no CoA→GAAP mapping exists,
  ``CombinedViewOnParentOnlyError`` for a group scope on a subsidiary,
  ``InvalidStatementScopeError`` for a scope not in ``LIVE_STATEMENT_SCOPES``,
  and ``DimensionalStatementError`` for a slice of another statement, a
  reserved axis, or a member not of the axis.
  """
  scope = scope or ("consolidated" if consolidated else "entity")
  if scope not in LIVE_STATEMENT_SCOPES:
    raise InvalidStatementScopeError(scope)
  if dimension is not None:
    _check_dimension(session, statement_type, dimension)
  entity_id = resolve_entity_id(session, entity_id)
  if reporting_style_id is None:
    reporting_style_id = load_entity_reporting_style(session, entity_id)
  if scope != "entity" and not is_group_parent(session, entity_id):
    raise CombinedViewOnParentOnlyError(entity_id)
  periods = build_current_and_prior_periods(period_start, period_end)
  by_member = dimension is not None and dimension.member_id is None
  columns: list[StatementDimensionColumn] = []
  differences: list[str] = []
  if by_member:
    assert dimension is not None
    periods = periods[:1]
    rendered, columns, differences = _by_member(
      session,
      statement_type=statement_type,
      window=periods[0],
      reporting_style_id=reporting_style_id,
      entity_id=entity_id,
      scope=scope,
      dimension_type=dimension.type,
    )
  else:
    rendered = _render_live(
      session,
      statement_type=statement_type,
      periods=periods,
      reporting_style_id=reporting_style_id,
      entity_id=entity_id,
      scope=scope,
      line_slice=_slice_of(dimension) if dimension is not None else None,
    )
  grid = rendered.grid
  unmapped_count = rendered.unmapped_count
  combined_entity_ids = rendered.combined_entity_ids
  partly = rendered.partly
  # Left out rather than rendered.
  if scope != "entity":
    omitted = partly | group_columns_before_the_books(
      session,
      statement_type,
      periods,
      [
        m
        for m in group_members(session, entity_id, consolidated=scope == "consolidated")
        if m.id in combined_entity_ids
      ],
    )
  else:
    omitted = columns_before_the_books(session, statement_type, periods, [entity_id])
  if by_member:
    # One window spread across the members: its columns stand or fall together.
    if omitted:
      for row in grid.rows:
        row.values = []
      omitted_periods = list(periods)
      columns = []
      differences = []
      periods = []
    else:
      omitted_periods = []
      periods = [
        FactPeriodSpec(start=periods[0].start, end=periods[0].end, label=c.name)
        for c in columns
      ]
  elif omitted:
    keep = [i for i in range(len(periods)) if i not in omitted]
    for row in grid.rows:
      row.values = [row.values[i] for i in keep if i < len(row.values)]
    omitted_periods = [periods[i] for i in sorted(omitted)]
    periods = [periods[i] for i in keep]
  else:
    omitted_periods = []

  # Validate the full grid: all-zero children still foot their subtotals.
  validation = validate_report(
    statement_type,
    grid.rows,
    period_labels=[p.label for p in periods],
    warnings=grid.warnings,
  )
  if not periods:
    # Nothing left to check is not a pass.
    validation.passed = False
    validation.status = "inconclusive"
    validation.warnings = [
      _NOTHING_TO_STATE if omitted <= partly else _BEFORE_THE_BOOKS
    ]

  facts: list[LiveStatementFactRow] = []
  for row in grid.rows:
    # Abstract rows duplicate the concrete subtotal beneath them.
    if row.is_abstract:
      continue
    values = row.values
    # A roll-forward's balances stay even at zero: a company's first period
    # opens on nothing.
    if row.period_role is None and not any(v != 0.0 for v in values):
      continue
    facts.append(
      LiveStatementFactRow(
        qname=row.element_qname,
        name=row.element_name,
        trait=row.classification,
        values=values,
        depth=row.depth,
        is_subtotal=row.is_subtotal,
        period_role=row.period_role,
      )
    )

  truncated = len(facts) > limit
  if truncated:
    facts = facts[:limit]

  return LiveFinancialStatementResponse(
    graph_id=graph_id,
    entity_id=entity_id,
    scope=scope,
    consolidated=scope != "entity",
    combined_entity_ids=combined_entity_ids,
    statement_type=statement_type,
    periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in periods],
    omitted_periods=[
      PeriodSpec(start=p.start, end=p.end, label=p.label) for p in omitted_periods
    ],
    facts=facts,
    fact_count=len(facts),
    validation=ValidationCheckResponse(
      passed=validation.passed,
      status=validation.status,
      checks=validation.checks,
      failures=validation.failures,
      warnings=validation.warnings,
    ),
    unmapped_count=unmapped_count,
    truncated=truncated,
    dimension=(
      StatementDimensionResponse(
        type=dimension.type,
        member_id=dimension.member_id,
        columns=columns,
        ties_out=not differences if columns else None,
        differences=differences,
      )
      if dimension is not None
      else None
    ),
  )
