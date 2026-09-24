"""Read operations for report definitions and rendered statements."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.orm import Session

if TYPE_CHECKING:
  from robosystems.models.api.extensions.report_package import (
    ReportPackageEnvelope,
  )

from robosystems.config import env
from robosystems.config.storage.graph import get_report_bundle_key
from robosystems.models.api.extensions.reports import (
  FactRowResponse,
  LiveFinancialStatementResponse,
  LiveStatementFactRow,
  PeriodSpec,
  ReportBundleDownloadResponse,
  ReportListResponse,
  ReportResponse,
  StatementResponse,
  StructureSummary,
  ValidationCheckResponse,
)
from robosystems.models.extensions import Report
from robosystems.models.extensions.roboledger import Structure
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  FactRow,
  _compute_prior_period,
  generate_report_facts,
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
  load_primary_reporting_style,
)
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor

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

# The subset offered to an AI operator. ``equity_statement`` renders equity
# balances, not a rollforward, so it stays off MCP until it articulates.
MCP_LIVE_STATEMENT_TYPES: tuple[str, ...] = tuple(
  t for t in LIVE_STATEMENT_TYPES if t != "equity_statement"
)

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
):
  """Build a one-shot statement from the current ledger, with no saved Report.

  The Network comes from the entity's Reporting Style. The arc walk is scoped
  to rs-gaap-presentation, where every Default Style Network lives; a Style
  citing Networks from another taxonomy is not yet supported.

  Returns ``(grid, unmapped_count)``. Raises `CoaMappingNotFoundError` if the
  tenant has no CoA→GAAP mapping yet.
  """
  mapping = (
    session.query(Structure).filter(Structure.block_type == "coa_mapping").first()
  )
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
  )

  grid = render_structure_view(
    session=session,
    facts=facts.facts,
    block_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )

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
  """The linked entity's name for a shared-in report, else the parent entity's."""
  if report_def.source_graph_id:
    row = session.execute(
      text(
        "SELECT name FROM entities WHERE metadata->>'source_graph_id' = :sgid LIMIT 1"
      ),
      {"sgid": report_def.source_graph_id},
    ).first()
  else:
    row = session.execute(
      text("SELECT name FROM entities WHERE is_parent = true LIMIT 1")
    ).first()
  return row.name if row else None


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


def list_reports(session: Session) -> ReportListResponse:
  """List all report definitions, most recent first."""
  rows = (
    session.execute(select(Report).order_by(Report.created_at.desc())).scalars().all()
  )

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
  flavor: str = RdfFlavor.JSONLD.value,
  expires_in: int = PRESIGN_DEFAULT_SECONDS,
) -> ReportBundleDownloadResponse | None:
  """Resolve a presigned URL for a published Report's serialization bundle.

  The API never streams bytes. JSON-LD is stamped at publish; XBRL, the
  holon and the Tavi model are materialized on first download and cached
  under a ``generation_count``-versioned key, which never goes stale.

  Returns ``None`` when ``report_id`` doesn't resolve. Raises
  :class:`ReportBundleNotAvailableError` when the report exists but has
  no published bundle, :class:`BundleSigningError` on a signing /
  materialization fault, and ``ValueError`` for an unrecognized flavor.
  """
  report = session.get(Report, report_id)
  if report is None:
    return None
  generation_count = int(report.generation_count or 0)

  # HOLON_JSONLD is in RdfFlavor, so this must precede the _RDF_FLAVOR_VALUES
  # branch. It gates on publication, not ``bundle_url`` (the flat JSON-LD),
  # because a shared-in copy can carry a holon without a flat bundle.
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

  # Same gate as the holon; TAVI is in XbrlFlavor, so it precedes that branch.
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
  if flavor in _RDF_FLAVOR_VALUES:
    return _presign_stored_rdf_bundle(
      bundle_uri=str(report.bundle_url),
      report_id=report_id,
      flavor=RdfFlavor(flavor),
      generation_count=generation_count,
      expires_in=expires_in,
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


def _presign_stored_rdf_bundle(
  bundle_uri: str,
  report_id: str,
  flavor: RdfFlavor,
  generation_count: int,
  expires_in: int,
) -> ReportBundleDownloadResponse:
  """Presign the JSON-LD bundle already stamped to S3 at publish time."""
  if flavor is not RdfFlavor.JSONLD:
    raise ValueError(f"Format '{flavor.value}' is reserved for future use.")

  bucket, key = _parse_s3_uri(bundle_uri)
  if bucket is None or key is None:
    raise BundleSigningError(
      f"Bundle URL for report '{report_id}' is malformed; cannot sign a download link."
    )

  download_url = S3Client().generate_presigned_url(
    bucket=bucket,
    key=key,
    expires_in=expires_in,
    response_content_type="application/ld+json",
    response_content_disposition=(
      f'attachment; filename="{report_id}-g{generation_count}.jsonld"'
    ),
  )
  if download_url is None:
    raise BundleSigningError(
      f"Failed to sign download URL for report '{report_id}' bundle."
    )

  return ReportBundleDownloadResponse(
    download_url=download_url,
    expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
    content_type="application/ld+json",
    format=flavor.value,
    generation_count=generation_count,
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
      raise BundleSigningError(
        f"Failed to materialize XBRL bundle for report '{report_id}' "
        f"to s3://{bucket}/{key}."
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
      raise BundleSigningError(
        f"Failed to materialize holon bundle for report '{report_id}' "
        f"to s3://{bucket}/{key}."
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
      raise BundleSigningError(
        f"Failed to materialize Tavi model for report '{report_id}' "
        f"to s3://{bucket}/{key}."
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


def _parse_s3_uri(uri: str) -> tuple[str | None, str | None]:
  """Split an ``s3://bucket/key`` URI into ``(bucket, key)``.

  Returns ``(None, None)`` on malformed input. Deliberately not checked against
  ``env.USER_DATA_BUCKET``: a bundle stamped under another bucket must resolve.
  """
  if not uri.startswith("s3://"):
    return None, None
  remainder = uri[len("s3://") :]
  if "/" not in remainder:
    return None, None
  bucket, _, key = remainder.partition("/")
  if not bucket or not key:
    return None, None
  return bucket, key


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
  taxonomy. ``reporting_style_id`` defaults to the primary entity's Style.
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
  periods = [periods[i] for i in rendered_period_indexes(block_type, periods)]

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
    reporting_style_id = load_primary_reporting_style(session)
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
    block_type, grid.rows, period_labels=[p.label for p in grid.periods]
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


def rendered_period_indexes(
  statement_type: str, periods: list[FactPeriodSpec]
) -> list[int]:
  """Column indexes a statement renders, in the order ``periods`` was built.

  Every period renders, except the earliest on a cash flow statement when two
  or more were pivoted: the indirect method uses it only as the delta basis,
  so rendered it would foot while missing every working-capital delta.
  """
  indexes = list(range(len(periods)))
  if statement_type != "cash_flow_statement" or len(periods) < 2:
    return indexes
  earliest = min(indexes, key=lambda i: periods[i].end)
  return [i for i in indexes if i != earliest]


def get_live_financial_statement(
  session: Session,
  *,
  graph_id: str,
  statement_type: str,
  period_start: date,
  period_end: date,
  limit: int = 1000,
  reporting_style_id: str | None = None,
) -> LiveFinancialStatementResponse:
  """Render a current + prior ad-hoc statement from OLTP data.

  Drops abstract and all-zero rows and caps at ``limit`` (``truncated``).
  ``reporting_style_id`` defaults to the primary entity's Style. Raises
  ``CoaMappingNotFoundError`` when no CoA→GAAP mapping exists.
  """
  if reporting_style_id is None:
    reporting_style_id = load_primary_reporting_style(session)
  periods = build_current_and_prior_periods(period_start, period_end)
  grid, unmapped_count = generate_adhoc_private_statement(
    session,
    statement_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )
  columns = rendered_period_indexes(statement_type, periods)
  rendered_periods = [periods[i] for i in columns]

  # Validate the full grid (all-zero children still foot their subtotals),
  # but only the rendered columns.
  validation = validate_report(
    statement_type,
    [_project_row(row, columns) for row in grid.rows],
    period_labels=[p.label for p in rendered_periods],
  )

  facts: list[LiveStatementFactRow] = []
  for row in grid.rows:
    # Abstract rows duplicate the concrete subtotal beneath them.
    if row.is_abstract:
      continue
    values = (
      row.values
      if len(rendered_periods) == len(periods)
      else [row.values[i] for i in columns]
    )
    if not any(v != 0.0 for v in values):
      continue
    facts.append(
      LiveStatementFactRow(
        qname=row.element_qname,
        name=row.element_name,
        trait=row.classification,
        values=values,
        depth=row.depth,
        is_subtotal=row.is_subtotal,
      )
    )

  truncated = len(facts) > limit
  if truncated:
    facts = facts[:limit]

  return LiveFinancialStatementResponse(
    graph_id=graph_id,
    statement_type=statement_type,
    periods=[
      PeriodSpec(start=p.start, end=p.end, label=p.label) for p in rendered_periods
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
  )


def _project_row(row: FactRow, columns: list[int]) -> FactRow:
  return FactRow(
    element_id=row.element_id,
    element_qname=row.element_qname,
    element_name=row.element_name,
    classification=row.classification,
    balance_type=row.balance_type,
    values=[row.values[i] if i < len(row.values) else None for i in columns],
    is_subtotal=row.is_subtotal,
    is_abstract=row.is_abstract,
    depth=row.depth,
  )
