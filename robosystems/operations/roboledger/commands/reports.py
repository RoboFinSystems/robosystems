"""Write operations for report definitions.

Includes the share path which copies a report + its facts into a target
graph's tenant schema — that helper (`_share_to_target`) stays internal
here because it's only used by `share_report`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.storage.graph import (
  get_report_bundle_key,
  get_report_bundle_uri,
)
from robosystems.logger import logger
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ReportResponse,
  RevokeReportShareRequest,
  RevokeReportShareResponse,
  ShareReportRequest,
  ShareReportResponse,
  ShareResultItem,
)
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions import (
  Fact,
  PublishList,
  PublishListMember,
  Report,
  ReportShare,
)
from robosystems.models.extensions.structure import TEXT_BLOCK_CAPS
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.reads.blocked_source_graphs import (
  is_source_blocked,
)
from robosystems.operations.roboledger.reads.reports import (
  build_periods,
  load_structures,
  periods_to_json,
  report_to_response,
  resolve_entity_name,
)
from robosystems.operations.roboledger.reports.fact_grid import generate_report_facts
from robosystems.operations.roboledger.reports.network_picker import (
  load_close_target_concept,
  load_entity_reporting_style,
)

# The statement-set production core lives in ``statement_sets`` so the close
# path can mint canonical (report_id NULL) sets through the same machinery.
# Re-exported here because tests and the router import these names from this
# module.
from robosystems.operations.roboledger.reports.statement_sets import (  # noqa: F401
  _TAXONOMY_SCOPE_CTE,
  NoEntityError,
  _build_structure_mapping,
  _evaluate_report_structures,
  _get_entity_id,
  _persist_report_facts,
  _pick_disclosure_structures,
  _pre_create_report_fact_sets,
)
from robosystems.operations.serialization import (
  RdfFlavor,
  StatementBundle,
  build_report_bundle,
  serialize_to_rdf,
)


class ReportNotFoundError(LookupError):
  """Raised when a report_id does not resolve to a row."""


class NotAuthorizedError(Exception):
  """Raised when the caller does not own the report they're acting on."""


class PublishListNotFoundError(LookupError):
  """Raised when a publish_list_id does not resolve to a row."""


class PublishListEmptyError(Exception):
  """Raised when a share operation targets a publish list with no members."""


class ReportNotPublishedError(Exception):
  """Raised when trying to share a report that isn't in 'published' state."""


class ReportShareNotFoundError(LookupError):
  """Raised when revoking a share that doesn't exist or is already revoked."""


class TaxonomyNotFoundError(LookupError):
  """Raised when `create_report` references a missing taxonomy."""


class BundleUploadError(RuntimeError):
  """Raised when the publish-time JSON-LD bundle upload to S3 fails.

  The publish path is fail-loud: a Report cannot transition to
  ``published`` without a bundle artifact at ``Report.bundle_url``.
  Callers (router layer) translate this to HTTP 502.
  """


def _snapshot_text_block_facts(
  session: Session,
  report_id: str,
  entity_id: str,
  created_by: str,
  taxonomy_id: str,
  periods,
) -> int:
  """Snapshot standing text-block bindings into this report's FactSets.

  Text-block disclosure structures carry no pivot facts, so the
  fact-driven picker never sees them. Their render membership comes from
  the standing ``factset_type='disclosure'`` FactSets that
  ``bind-text-block`` maintains: for every text-block-CAP disclosure
  structure in the report's taxonomy closure whose standing binding
  falls inside the report window, copy the latest standing set's facts
  into a fresh ``factset_type='report'`` FactSet stamped with this
  ``report_id`` — carrying the standing set's document provenance
  verbatim (NOT ``PivotProvenance``; the narrative was asserted from a
  document, not pivoted from the ledger).

  The copy is the immutability seam: a filed report keeps the text as
  bound at generation time even if the document — or the standing
  binding — changes later. Regeneration's DELETE-by-``report_id``
  clears prior snapshots; the standing sets (``report_id`` NULL)
  survive. Containment window semantics (binding period inside the
  report envelope) mirror the rule engine's fact binds.

  Returns the number of structures snapshotted.
  """
  if not periods:
    return 0
  starts = [p.start for p in periods if getattr(p, "start", None) is not None]
  envelope_start = min(starts) if starts else None
  envelope_end = max(p.end for p in periods)

  rows = session.execute(
    text(
      _TAXONOMY_SCOPE_CTE
      + """
      SELECT DISTINCT ON (fs.structure_id) fs.id AS fact_set_id,
             fs.structure_id, fs.period_start, fs.period_end, fs.provenance
      FROM fact_sets fs
      JOIN structures s ON s.id = fs.structure_id
      WHERE fs.factset_type = 'disclosure'
        AND fs.entity_id = :entity_id
        AND s.block_type = :disclosure_block_type
        AND s.is_active IS TRUE
        AND s.taxonomy_id IN (SELECT id FROM scoped)
        AND s.concept_arrangement = ANY(:text_caps)
        AND fs.period_end <= :envelope_end
        AND (CAST(:envelope_start AS DATE) IS NULL
             OR fs.period_start >= :envelope_start)
      ORDER BY fs.structure_id, fs.created_at DESC, fs.id DESC
      """
    ),
    {
      "taxonomy_id": taxonomy_id,
      "entity_id": entity_id,
      "disclosure_block_type": DISCLOSURE_BLOCK_TYPE,
      "text_caps": sorted(TEXT_BLOCK_CAPS),
      "envelope_start": envelope_start,
      "envelope_end": envelope_end,
    },
  ).fetchall()

  for row in rows:
    snapshot = create_fact_set(
      session,
      structure_id=row.structure_id,
      period_start=row.period_start,
      period_end=row.period_end,
      factset_type="report",
      entity_id=entity_id,
      report_id=report_id,
      created_by=created_by,
      provenance=row.provenance,
    )
    session.flush()
    source_facts = (
      session.execute(select(Fact).where(Fact.fact_set_id == row.fact_set_id))
      .scalars()
      .all()
    )
    for f in source_facts:
      session.add(
        Fact(
          element_id=f.element_id,
          value=f.value,
          string_value=f.string_value,
          fact_type=f.fact_type,
          value_type=f.value_type,
          content_type=f.content_type,
          decimals=f.decimals,
          period_start=f.period_start,
          period_end=f.period_end,
          period_type=f.period_type,
          unit=f.unit,
          entity_id=f.entity_id,
          structure_id=f.structure_id,
          fact_set_id=snapshot.id,
        )
      )
  session.flush()
  return len(rows)


def _record_bundle_validation(bundle: StatementBundle, report_def: Report) -> None:
  """Optionally SHACL-validate the bundle and record the result on the Report.

  Gated by ``env.REPORT_BUNDLE_SHACL_VALIDATION`` (``off`` | ``warn`` |
  ``strict``) — opt-in, so the publish path stays fast by default. When it
  runs, the structured outcome is logged onto
  ``report.metadata['bundle_validation']`` (audit trail), and ``strict``
  additionally blocks the publish on non-conformance.

  Validation-infrastructure failures (e.g. pyshacl raising) never break a
  ``warn`` publish: they are logged and skipped. Only ``strict`` re-raises,
  since strict opted into blocking on a bad/unverifiable bundle.
  """
  mode = (env.REPORT_BUNDLE_SHACL_VALIDATION or "off").strip().lower()
  if mode == "off":
    return
  from robosystems.operations.serialization.rdf.jsonld import (
    BundleValidationError,
    build_graph,
    shacl_report,
  )

  try:
    result = shacl_report(build_graph(bundle))
  except Exception:
    logger.exception(
      "SHACL validation errored for report %s (mode=%s)", report_def.id, mode
    )
    if mode == "strict":
      raise
    return
  # Reassign (not mutate) so SQLAlchemy flags the JSONB column dirty.
  report_def.metadata_ = {
    **(report_def.metadata_ or {}),
    "bundle_validation": {
      **result.as_dict(),
      "validated_at": datetime.now(UTC).isoformat(),
    },
  }
  logger.info(
    "Bundle SHACL for report %s: ran=%s conforms=%s violations=%d (mode=%s)",
    report_def.id,
    result.ran,
    result.conforms,
    result.violations,
    mode,
  )
  if mode == "strict" and result.ran and not result.conforms:
    raise BundleValidationError(
      f"Report {report_def.id} bundle failed SHACL conformance "
      f"({result.violations} violation(s)); aborting publish (strict mode)."
    )


def _stamp_report_bundle(
  session: Session,
  graph_id: str,
  report_def: Report,
) -> None:
  """Produce + stash the JSON-LD bundle for a Report about to publish.

  Called from the publish-hook in :func:`create_report` and
  :func:`regenerate_report` after facts are stamped and rules have run,
  before the transaction commits. Sequence:

  1. ``session.flush()`` so freshly-stamped FactSet + Fact rows are
     visible to the bundler's ORM reads (the extensions session is
     ``autoflush=False``).
  2. Bump ``report_def.generation_count`` so the S3 key bumps a new
     version even on regenerate.
  3. Build the ``StatementBundle`` via :func:`build_report_bundle`.
  4. Serialize to JSON-LD via :func:`serialize_to_rdf`.
  5. Upload to S3 under ``report-bundles/{graph_id}/{report_id}/v{n}.jsonld``.
  6. Stamp ``report_def.bundle_url`` with the full ``s3://`` URI.

  Fail-loud: any S3 failure raises :class:`BundleUploadError` so the
  caller's transaction never commits. Orphan S3 objects are
  acceptable; orphan published-Reports without a bundle are not.
  """
  session.flush()
  report_def.generation_count = (report_def.generation_count or 0) + 1
  bundle = build_report_bundle(session, graph_id, report_def.id)
  _record_bundle_validation(bundle, report_def)
  jsonld_doc = serialize_to_rdf(bundle, RdfFlavor.JSONLD)
  bucket = env.USER_DATA_BUCKET
  key = get_report_bundle_key(graph_id, report_def.id, report_def.generation_count)
  ok = S3Client().upload_string(
    content=jsonld_doc,
    bucket=bucket,
    key=key,
    content_type="application/ld+json",
    metadata={"report-id": report_def.id, "graph-id": graph_id},
  )
  if not ok:
    raise BundleUploadError(
      f"Failed to upload JSON-LD bundle for report {report_def.id} "
      f"to s3://{bucket}/{key}; aborting publish."
    )
  report_def.bundle_url = get_report_bundle_uri(
    bucket, graph_id, report_def.id, report_def.generation_count
  )
  logger.info(
    "Stamped bundle for report %s (g%d) at %s",
    report_def.id,
    report_def.generation_count,
    report_def.bundle_url,
  )


def create_report(
  session: Session,
  graph_id: str,
  body: CreateReportRequest,
  created_by: str,
) -> ReportResponse:
  """Create a report definition, generate facts, and mark as published.

  Raises `TaxonomyNotFoundError`, `NoEntityError` — caller translates
  to HTTP 422.
  """
  # Resolve taxonomy: accept either an exact ID (tenant-specific UUID) or a
  # standard name (e.g. 'rs-gaap'). Standard-name resolution prefers the
  # 'reporting_standard' type so callers don't accidentally pick a linkbase
  # taxonomy. Per-tenant UUIDs make hardcoding an ID in the request default
  # unreliable, so the request defaults to the standard name 'rs-gaap'.
  tax_result = session.execute(
    text(
      "SELECT id, standard FROM taxonomies "
      "WHERE id = :tid "
      "   OR (standard = :tid AND taxonomy_type = 'reporting_standard') "
      "ORDER BY (id = :tid) DESC, version DESC LIMIT 1"
    ),
    {"tid": body.taxonomy_id},
  )
  tax_row = tax_result.fetchone()
  if tax_row is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)
  # Use the resolved UUID for downstream queries even if caller passed a
  # standard name.
  resolved_taxonomy_id = tax_row[0]

  periods = build_periods(
    body.period_start, body.period_end, body.comparative, body.periods
  )

  report_def = Report(
    name=body.name,
    taxonomy_id=resolved_taxonomy_id,
    mapping_id=body.mapping_id,
    period_type=body.period_type,
    period_start=body.period_start,
    period_end=body.period_end,
    comparative=body.comparative,
    periods=periods_to_json(periods),
    generation_status="generating",
    created_by=created_by,
  )
  session.add(report_def)
  session.flush()

  # Resolve the reporting entity, then its active Style's earnings home
  # before generating facts, so derived cumulative earnings close to the
  # form's capital concept (CORP→RetainedEarnings, PART→PartnersCapital,
  # LLC→MembersEquity) and the balance sheet foots for non-corporate forms.
  entity_id = _get_entity_id(session, graph_id)
  reporting_style_id = load_entity_reporting_style(session, entity_id)
  close_target = load_close_target_concept(session, reporting_style_id)

  facts = generate_report_facts(
    session=session,
    taxonomy_id=resolved_taxonomy_id,
    mapping_id=body.mapping_id,
    periods=periods,
    close_target_qname=close_target,
  )

  element_to_structures, structure_to_factset = _build_structure_mapping(
    session,
    reporting_style_id,
    fact_element_ids={f.element_id for f in facts.facts},
    taxonomy_id=resolved_taxonomy_id,
  )
  # Create fact_sets rows first so the facts we stamp reference a row
  # that already exists, letting the DB enforce facts.fact_set_id →
  # fact_sets.id.
  _pre_create_report_fact_sets(
    session,
    report_def.id,
    entity_id,
    created_by,
    periods,
    structure_to_factset,
    body.mapping_id,
  )
  _persist_report_facts(
    session,
    report_def.id,
    facts,
    entity_id,
    element_to_structures,
    structure_to_factset,
  )
  _snapshot_text_block_facts(
    session,
    report_def.id,
    entity_id,
    created_by,
    resolved_taxonomy_id,
    periods,
  )
  summary = _evaluate_report_structures(
    session,
    facts,
    element_to_structures,
    structure_to_factset,
    body.period_start,
    body.period_end,
    created_by,
  )

  _stamp_report_bundle(session, graph_id, report_def)

  report_def.generation_status = "published"
  report_def.last_generated = datetime.now(UTC)
  session.commit()

  structures = load_structures(session, body.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  resp = report_to_response(report_def, structures, entity_name)
  resp.rule_summary = summary
  return resp


def regenerate_report(
  session: Session,
  graph_id: str,
  report_id: str,
  body: RegenerateReportRequest,
  acting_user_id: str,
) -> ReportResponse:
  """Regenerate a report with new period dates.

  `graph_id` is only used by the internal `_get_entity_id` error
  message — `Report` rows don't store a graph_id column (the tenant
  graph is the schema, not a field). Passed through explicitly so
  the route handler's path parameter flows to the ops layer rather
  than relying on a non-existent model attribute.

  ``filed`` and ``archived`` Reports are immutable artifacts — they
  carry stamped facts and a published bundle that downstream consumers
  may already reference. Regenerating one would silently mutate that
  state; the only legal path past ``filed`` is a restatement (a new
  Report row with ``supersedes_id``). ``delete_report`` is already
  gated this way; this check brings ``regenerate_report`` in line.

  Raises:
    ReportNotFoundError: report_id doesn't resolve.
    NotAuthorizedError: caller doesn't own the report.
    InvalidFilingTransitionError: report is ``filed`` or ``archived``
      — restate instead of regenerating.
    ValueError: if period_end < period_start in the new body.
  """
  report_def = session.get(Report, report_id)
  if report_def is None:
    raise ReportNotFoundError(report_id)
  if report_def.created_by != acting_user_id:
    raise NotAuthorizedError("Not authorized to modify this report.")
  if report_def.filing_status in {"filed", "archived"}:
    raise InvalidFilingTransitionError(
      f"Report '{report_id}' is in '{report_def.filing_status}'; "
      f"create a restatement (new Report with supersedes_id) instead of "
      f"regenerating."
    )

  # Resolve new periods
  if body.periods:
    periods = build_periods(None, None, False, body.periods)
    report_def.periods = periods_to_json(periods)
    report_def.period_start = body.periods[0].start
    report_def.period_end = body.periods[0].end
  elif body.period_start and body.period_end:
    if body.period_end < body.period_start:
      raise ValueError("period_end must be >= period_start")
    report_def.period_start = body.period_start
    report_def.period_end = body.period_end
    periods = build_periods(body.period_start, body.period_end, report_def.comparative)
    report_def.periods = periods_to_json(periods)
  else:
    periods = build_periods(
      report_def.period_start,
      report_def.period_end,
      report_def.comparative,
      report_def.periods,
    )

  report_def.generation_status = "generating"
  session.flush()

  entity_id = _get_entity_id(session, graph_id)
  reporting_style_id = load_entity_reporting_style(session, entity_id)
  close_target = load_close_target_concept(session, reporting_style_id)

  facts = generate_report_facts(
    session=session,
    taxonomy_id=report_def.taxonomy_id,
    mapping_id=report_def.mapping_id or "",
    periods=periods,
    close_target_qname=close_target,
  )

  element_to_structures, structure_to_factset = _build_structure_mapping(
    session,
    reporting_style_id,
    fact_element_ids={f.element_id for f in facts.facts},
    taxonomy_id=report_def.taxonomy_id,
  )
  # Stale rows from the prior generation must clear before fresh ULIDs
  # land. The FK `facts.fact_set_id → fact_sets.id` is ON DELETE CASCADE,
  # so dropping the parent fact_sets cleans the child facts in one
  # statement.
  session.execute(
    text("DELETE FROM fact_sets WHERE report_id = :report_id"),
    {"report_id": report_def.id},
  )
  _pre_create_report_fact_sets(
    session,
    report_def.id,
    entity_id,
    acting_user_id,
    periods,
    structure_to_factset,
    report_def.mapping_id or "",
  )
  _persist_report_facts(
    session,
    report_def.id,
    facts,
    entity_id,
    element_to_structures,
    structure_to_factset,
  )
  _snapshot_text_block_facts(
    session,
    report_def.id,
    entity_id,
    acting_user_id,
    report_def.taxonomy_id,
    periods,
  )
  summary = _evaluate_report_structures(
    session,
    facts,
    element_to_structures,
    structure_to_factset,
    report_def.period_start,
    report_def.period_end,
    acting_user_id,
  )

  _stamp_report_bundle(session, graph_id, report_def)

  report_def.generation_status = "published"
  report_def.last_generated = datetime.now(UTC)
  session.commit()

  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  resp = report_to_response(report_def, structures, entity_name)
  resp.rule_summary = summary
  return resp


# ── Filing lifecycle ──────────────────────────────────────────────────────


class InvalidFilingTransitionError(Exception):
  """Raised when a filing-status transition isn't on the legal lifecycle graph."""


# Legal filing-status transitions:
#   draft ↔ under_review → filed ↔ archived
# ``filed`` is reached via :func:`file_report` so audit fields land cleanly;
# this map covers the non-file moves available to the generic transition op.
_LEGAL_NON_FILE_TRANSITIONS: dict[str, set[str]] = {
  "draft": {"under_review"},
  "under_review": {"draft"},
  "filed": {"archived"},
}


class ReportNotFiledError(Exception):
  """Raised when an op requires a ``filed`` Report and got something else."""


def file_report(session: Session, report_id: str, filed_by: str) -> ReportResponse:
  """Transition a Report to ``filed`` — locks the package.

  Allowed from ``draft`` or ``under_review`` and only when generation
  has reached ``published``. Stamps ``filed_at`` and ``filed_by`` for
  audit. Raises :class:`ReportNotFoundError` when the Report doesn't
  exist and :class:`InvalidFilingTransitionError` when the current
  filing or generation status isn't a legal source for filing.

  ``filing_status`` and ``generation_status`` are orthogonal axes, but
  filing an in-progress or failed report would lock an empty / partial
  snapshot — so the server gates on ``generation_status='published'``.
  """
  from datetime import UTC, datetime

  from robosystems.operations.roboledger.reads.reports import (
    load_structures,
    report_to_response,
    resolve_entity_name,
  )

  report_def = session.get(Report, report_id)
  if report_def is None:
    raise ReportNotFoundError(report_id)

  if report_def.filing_status not in {"draft", "under_review"}:
    raise InvalidFilingTransitionError(
      f"Report '{report_id}' is in '{report_def.filing_status}'; "
      f"can only file from 'draft' or 'under_review'."
    )
  # ``complete`` and ``published`` both mean "generation finished
  # successfully" in this codebase (see closing_book.py:63 which treats
  # them interchangeably). Filing a ``pending`` / ``generating`` /
  # ``failed`` report would lock an empty or partial snapshot.
  if report_def.generation_status not in {"complete", "published"}:
    raise InvalidFilingTransitionError(
      f"Report '{report_id}' has generation_status="
      f"'{report_def.generation_status}'; can only file once generation "
      f"has reached 'complete' or 'published'."
    )

  report_def.filing_status = "filed"
  report_def.filed_at = datetime.now(UTC)
  report_def.filed_by = filed_by
  session.flush()

  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def transition_filing_status(
  session: Session, report_id: str, target_status: str
) -> ReportResponse:
  """Move a Report along the non-file legs of the filing lifecycle.

  Use :func:`file_report` to reach ``filed`` (so audit fields land).
  Other transitions (submit for review, withdraw, archive) are routed
  through here so the legal-transition graph stays in one place.
  """
  from robosystems.operations.roboledger.reads.reports import (
    load_structures,
    report_to_response,
    resolve_entity_name,
  )

  report_def = session.get(Report, report_id)
  if report_def is None:
    raise ReportNotFoundError(report_id)

  legal_targets = _LEGAL_NON_FILE_TRANSITIONS.get(report_def.filing_status, set())
  if target_status not in legal_targets:
    raise InvalidFilingTransitionError(
      f"Report '{report_id}' cannot transition from "
      f"'{report_def.filing_status}' to '{target_status}'. "
      f"Legal targets from here: {sorted(legal_targets)}."
    )

  report_def.filing_status = target_status
  session.flush()

  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def delete_report(
  session: Session,
  report_id: str,
  acting_user_id: str,
  *,
  acting_user_is_graph_admin: bool = False,
) -> bool:
  """Delete a report and its generated facts.

  Raises `NotAuthorizedError` if the caller doesn't own the report.
  Raises `ReportNotFiledError` if the report is in a locked filing
  state (``filed`` or ``archived``) — the Report Block lifecycle treats
  filed/archived as immutable so the audit trail can't be erased.
  Returns True if a row was deleted, False if the report did not exist.

  A report shared in from another graph (``source_graph_id`` set) is the one
  exception to the owner rule: its ``created_by`` is the *sender's* user id, so
  no one in the receiving graph could ever match it. An admin of the receiving
  graph may delete such a copy — the recipient's exit from a share they did not
  ask for. Native reports are unaffected; only their owner can delete them.

  Deleting the copy deliberately leaves the sender's ``ReportShare`` row alone:
  the sender's record that they sent it is theirs, not the recipient's to erase.
  """
  report_def = session.get(Report, report_id)
  if report_def is None:
    return False
  if report_def.created_by != acting_user_id:
    is_shared_copy = report_def.source_graph_id is not None
    if not (is_shared_copy and acting_user_is_graph_admin):
      raise NotAuthorizedError("Not authorized to delete this report.")
  if report_def.filing_status in {"filed", "archived"}:
    raise ReportNotFiledError(
      f"Report '{report_id}' is '{report_def.filing_status}' and cannot "
      f"be deleted. Reach 'archived' via transition-filing-status if "
      f"retiring; deletion is only available for 'draft' or 'under_review'."
    )

  # Facts cascade from their parent fact_sets on delete.
  session.execute(
    text("DELETE FROM fact_sets WHERE report_id = :report_id"),
    {"report_id": report_id},
  )
  session.delete(report_def)
  session.commit()
  return True


def share_report(
  graph_id: str,
  report_id: str,
  body: ShareReportRequest,
  acting_user_id: str,
) -> ShareReportResponse:
  """Share a published report to every target graph in a publish list.

  Takes `graph_id` instead of a session so it can open multiple
  sessions (source graph + each target graph + platform DB) as the
  share workflow requires. Raises `PublishListNotFoundError`,
  `PublishListEmptyError`, `ReportNotFoundError`,
  `NotAuthorizedError`, or `ReportNotPublishedError` — caller
  translates to appropriate HTTP status codes.
  """
  from robosystems.db.extensions import extensions_session

  results: list[ShareResultItem] = []

  with extensions_session(graph_id) as source_session:
    publish_list = source_session.execute(
      select(PublishList).where(PublishList.id == body.publish_list_id)
    ).scalar_one_or_none()
    if publish_list is None:
      raise PublishListNotFoundError(body.publish_list_id)

    members = (
      source_session.execute(
        select(PublishListMember).where(
          PublishListMember.publish_list_id == body.publish_list_id
        )
      )
      .scalars()
      .all()
    )
    if not members:
      raise PublishListEmptyError(body.publish_list_id)

    target_graph_ids = [m.target_graph_id for m in members]

    report_def = source_session.get(Report, report_id)
    if report_def is None:
      raise ReportNotFoundError(report_id)
    if report_def.created_by != acting_user_id:
      raise NotAuthorizedError("Not authorized to share this report.")
    if report_def.generation_status != "published":
      raise ReportNotPublishedError("Only published reports can be shared.")

    report_snapshot = {
      "id": report_def.id,
      "name": report_def.name,
      "description": report_def.description,
      "taxonomy_id": report_def.taxonomy_id,
      "mapping_id": report_def.mapping_id,
      "period_type": report_def.period_type,
      "period_start": report_def.period_start,
      "period_end": report_def.period_end,
      "comparative": report_def.comparative,
      "periods": report_def.periods,
    }

    fact_rows = source_session.execute(
      text("""
        SELECT f.id, f.element_id, f.value, f.string_value, f.fact_type,
               f.value_type, f.content_type, f.decimals, f.period_start,
               f.period_end, f.period_type, f.unit, f.entity_id, f.created_at
        FROM facts f
        JOIN fact_sets fs ON fs.id = f.fact_set_id
        WHERE fs.report_id = :report_id
      """),
      {"report_id": report_id},
    ).fetchall()
    source_facts = [row._asdict() for row in fact_rows]

  for target_graph_id in target_graph_ids:
    result = _share_to_target(
      source_graph_id=graph_id,
      report_snapshot=report_snapshot,
      source_facts=source_facts,
      target_graph_id=target_graph_id,
      shared_by=acting_user_id,
    )
    results.append(result)

  successful = [r for r in results if r.status == "shared"]
  if successful:
    with extensions_session(graph_id) as source_session:
      for result in successful:
        source_session.add(
          ReportShare(
            report_id=report_id,
            target_graph_id=result.target_graph_id,
            shared_by=acting_user_id,
            fact_count=result.fact_count,
          )
        )
      source_session.commit()

  return ShareReportResponse(report_id=report_id, results=results)


def revoke_report_share(
  graph_id: str,
  report_id: str,
  body: RevokeReportShareRequest,
  acting_user_id: str,
) -> RevokeReportShareResponse:
  """Withdraw a shared report from one recipient graph.

  The sender's half of the share controls. Takes `graph_id` rather than a
  session for the same reason `share_report` does — it spans the source and
  target tenant schemas.

  Stamps `ReportShare.revoked_at` in the source and deletes the copied Report
  (and its fact sets, whose facts cascade) from the target. A recipient who
  already deleted the copy themselves is not an error: the share is still
  marked revoked and `copy_deleted` comes back False.

  The linked `Entity` in the target is deliberately left in place. An investor's
  `Security` points at it, and the relationship survives one report being
  pulled — deleting it would break the link over a single withdrawal.

  Raises `ReportNotFoundError`, `NotAuthorizedError`, or
  `ReportShareNotFoundError`.
  """
  from robosystems.db.extensions import extensions_session

  now = datetime.now(UTC)

  with extensions_session(graph_id) as source_session:
    report_def = source_session.get(Report, report_id)
    if report_def is None:
      raise ReportNotFoundError(report_id)
    if report_def.created_by != acting_user_id:
      raise NotAuthorizedError("Not authorized to revoke shares of this report.")

    share = source_session.execute(
      select(ReportShare).where(
        ReportShare.report_id == report_id,
        ReportShare.target_graph_id == body.target_graph_id,
        ReportShare.revoked_at.is_(None),
      )
    ).scalar_one_or_none()
    if share is None:
      raise ReportShareNotFoundError(
        f"No active share of report '{report_id}' to '{body.target_graph_id}'."
      )

  copy_deleted = _delete_shared_copy(
    source_graph_id=graph_id,
    source_report_id=report_id,
    target_graph_id=body.target_graph_id,
  )

  # Stamp only after the copy is gone. If the target write fails, the share
  # stays un-revoked and the operation is safe to retry — the alternative
  # leaves a record claiming the data was withdrawn while it is still there.
  with extensions_session(graph_id) as source_session:
    share = source_session.execute(
      select(ReportShare).where(
        ReportShare.report_id == report_id,
        ReportShare.target_graph_id == body.target_graph_id,
        ReportShare.revoked_at.is_(None),
      )
    ).scalar_one_or_none()
    if share is not None:
      share.revoked_at = now
      source_session.commit()

  return RevokeReportShareResponse(
    report_id=report_id,
    target_graph_id=body.target_graph_id,
    revoked_at=now,
    copy_deleted=copy_deleted,
  )


def _delete_shared_copy(
  source_graph_id: str, source_report_id: str, target_graph_id: str
) -> bool:
  """Delete the copy of a shared report from the target tenant schema.

  Returns True when a copy was found and removed. Matches on the provenance
  pair, so it can only ever reach a report that arrived from this sender — a
  report the target authored has a null `source_graph_id` and cannot match.
  """
  from robosystems.db.extensions import extensions_session

  with extensions_session(target_graph_id) as target_session:
    copy_ids = list(
      target_session.execute(
        select(Report.id).where(
          Report.source_graph_id == source_graph_id,
          Report.source_report_id == source_report_id,
        )
      )
      .scalars()
      .all()
    )
    if not copy_ids:
      return False

    target_session.execute(
      text("DELETE FROM fact_sets WHERE report_id = ANY(:report_ids)"),
      {"report_ids": copy_ids},
    )
    target_session.execute(
      text("DELETE FROM reports WHERE id = ANY(:report_ids)"),
      {"report_ids": copy_ids},
    )
    target_session.commit()
    return True


def _share_to_target(
  source_graph_id: str,
  report_snapshot: dict[str, Any],
  source_facts: list[dict[str, Any]],
  target_graph_id: str,
  shared_by: str,
) -> ShareResultItem:
  """Copy report definition + facts to a target graph's tenant schema."""
  from robosystems.db.extensions import extensions_session
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph

  try:
    with SessionFactory() as platform_session:
      target_graph = platform_session.execute(
        select(Graph).where(Graph.graph_id == target_graph_id)
      ).scalar_one_or_none()

      if not target_graph:
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error=f"Graph '{target_graph_id}' not found.",
        )

      extensions = target_graph.schema_extensions or []
      if "roboledger" not in extensions:
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error="Target graph does not have 'roboledger' schema extension.",
        )
  except Exception as e:
    logger.error(f"Failed to validate target graph {target_graph_id}: {e}")
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error="Failed to validate target graph.",
    )

  try:
    now = datetime.now(UTC)
    with extensions_session(target_graph_id) as target_session:
      # The recipient's exit, checked before anything is written. Blocked
      # senders are told rather than silently dropped: they already had a
      # relationship with the recipient, so a bounce is more honest than a
      # shadow ban and stops them retrying forever. (If the block table is
      # somehow missing — code ahead of migration — this raises and the
      # handler below turns it into an error item, so the share fails closed.)
      if is_source_blocked(target_session, source_graph_id):
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error="Recipient has blocked shares from this graph.",
        )

      shared_report = Report(
        name=report_snapshot["name"],
        description=report_snapshot.get("description"),
        taxonomy_id=report_snapshot["taxonomy_id"],
        mapping_id=report_snapshot.get("mapping_id"),
        period_type=report_snapshot["period_type"],
        period_start=report_snapshot.get("period_start"),
        period_end=report_snapshot.get("period_end"),
        comparative=report_snapshot["comparative"],
        periods=report_snapshot.get("periods"),
        generation_status="published",
        created_by=shared_by,
        source_graph_id=source_graph_id,
        source_report_id=report_snapshot["id"],
        shared_at=now,
      )
      target_session.add(shared_report)
      target_session.flush()

      # Cross-graph share: source-graph structure_id references rows in
      # the source tenant schema and is meaningless in the target.
      # Target Networks aren't populated per-structure on share; instead
      # every shared fact is stamped against a single cross-graph FactSet
      # that back-references the shared report, with a period envelope
      # spanning all incoming facts.
      starts = [
        fd["period_start"] for fd in source_facts if fd.get("period_start") is not None
      ]
      ends = [
        fd["period_end"] for fd in source_facts if fd.get("period_end") is not None
      ]
      # The originating ledger is not present in the target graph, so the
      # shared facts collapse to `asserted` provenance referencing the
      # source graph/report rather than a re-derivable pivot.
      shared_fact_set = create_fact_set(
        target_session,
        structure_id=None,
        period_start=min(starts) if starts else None,
        period_end=max(ends) if ends else None,
        factset_type="report",
        entity_id=source_facts[0]["entity_id"] if source_facts else "",
        report_id=shared_report.id,
        created_by=shared_by,
        provenance=AssertedProvenance(
          source_system="cross_graph_share",
          asserted_by=shared_by,
          basis_note=f"source_graph={source_graph_id} source_report={report_snapshot['id']}",
        ),
      )
      target_session.flush()

      for fact_data in source_facts:
        rf = Fact(
          element_id=fact_data["element_id"],
          value=fact_data["value"],
          string_value=fact_data["string_value"],
          fact_type=fact_data["fact_type"],
          value_type=fact_data["value_type"],
          content_type=fact_data["content_type"],
          decimals=fact_data["decimals"],
          period_start=fact_data["period_start"],
          period_end=fact_data["period_end"],
          period_type=fact_data["period_type"],
          unit=fact_data["unit"],
          entity_id=fact_data["entity_id"],
          fact_set_id=shared_fact_set.id,
        )
        target_session.add(rf)

      _ensure_linked_entity(target_session, source_graph_id, shared_by)

      target_session.commit()

      return ShareResultItem(
        target_graph_id=target_graph_id,
        status="shared",
        fact_count=len(source_facts),
      )

  except Exception as e:
    logger.error(f"Failed to share report to {target_graph_id}: {e}")
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error=f"Failed to copy report data: {e!s}",
    )


def _ensure_linked_entity(
  target_session: Session, source_graph_id: str, shared_by: str
) -> None:
  """Create or update a linked Entity in the target graph for the source company.

  When a company shares a report to an investor's graph, the investor
  needs an Entity row representing that company. This function:
  1. Reads the source graph's parent entity for current metadata
  2. Creates a linked entity if none exists, or updates metadata if one does
  3. Auto-links any securities with matching source_graph_id
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions.entity import Entity

  try:
    with extensions_session(source_graph_id) as source_session:
      source_entity = source_session.execute(
        select(Entity).where(Entity.is_parent.is_(True)).limit(1)
      ).scalar_one_or_none()

      if not source_entity:
        return

      entity_data = {
        "name": source_entity.name,
        "legal_name": source_entity.legal_name,
        "entity_type": source_entity.entity_type,
        "industry": source_entity.industry,
        "cik": source_entity.cik,
        "ticker": source_entity.ticker,
        "state_of_incorporation": source_entity.state_of_incorporation,
      }
  except Exception:
    logger.warning(f"Could not read source entity from {source_graph_id}")
    entity_data = {"name": f"Entity ({source_graph_id})"}

  existing = target_session.execute(
    text("SELECT id FROM entities WHERE metadata->>'source_graph_id' = :sgid LIMIT 1"),
    {"sgid": source_graph_id},
  ).scalar_one_or_none()

  if existing:
    target_session.execute(
      text("""
        UPDATE entities SET
          name = :name,
          legal_name = :legal_name,
          entity_type = :entity_type,
          industry = :industry,
          cik = :cik,
          ticker = :ticker,
          state_of_incorporation = :state_of_incorporation,
          updated_at = now()
        WHERE id = :entity_id
      """),
      {
        "entity_id": existing,
        "name": entity_data["name"],
        "legal_name": entity_data.get("legal_name"),
        "entity_type": entity_data.get("entity_type"),
        "industry": entity_data.get("industry"),
        "cik": entity_data.get("cik"),
        "ticker": entity_data.get("ticker"),
        "state_of_incorporation": entity_data.get("state_of_incorporation"),
      },
    )
    entity_id = existing
  else:
    from robosystems.utils.ulid import generate_prefixed_ulid

    entity_id = generate_prefixed_ulid("ent")
    linked_entity = Entity(
      id=entity_id,
      name=entity_data["name"],
      legal_name=entity_data.get("legal_name"),
      entity_type=entity_data.get("entity_type"),
      industry=entity_data.get("industry"),
      cik=entity_data.get("cik"),
      ticker=entity_data.get("ticker"),
      state_of_incorporation=entity_data.get("state_of_incorporation"),
      source="linked",
      is_parent=False,
      status="active",
      address_country="US",
      metadata_={"source_graph_id": source_graph_id},
      created_by=shared_by,
    )
    target_session.add(linked_entity)
    target_session.flush()

  target_session.execute(
    text("""
      UPDATE securities SET entity_id = :entity_id, updated_at = now()
      WHERE source_graph_id = :source_graph_id AND entity_id IS NULL
    """),
    {"entity_id": entity_id, "source_graph_id": source_graph_id},
  )
