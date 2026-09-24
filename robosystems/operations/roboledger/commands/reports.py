"""Write operations for report definitions: generate, file, delete, share."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.storage.graph import (
  get_report_bundle_key,
  get_report_bundle_prefix,
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

# Re-exported: tests and the router import these names from this module.
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
  serialize_to_holon_jsonld,
  serialize_to_rdf,
  serialize_to_tavi,
)

# Extensions that make a graph a legitimate share recipient. Mirrors
# `_REPORT_EXTENSIONS` on the GraphQL read side.
_RECEIVING_EXTENSIONS = ("roboledger", "roboinvestor")


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


class ReportHasActiveSharesError(Exception):
  """Raised when deleting a report that is still shared to other graphs."""

  def __init__(self, report_id: str, target_graph_ids: list[str]) -> None:
    super().__init__(
      f"Report '{report_id}' is still shared to {len(target_graph_ids)} "
      f"graph(s). Revoke each share first — deleting the report here would "
      f"strand the delivered copies in their schemas with no way to withdraw "
      f"them."
    )
    self.report_id = report_id
    self.target_graph_ids = target_graph_ids


class TaxonomyNotFoundError(LookupError):
  """Raised when `create_report` references a missing taxonomy."""


class BundleUploadError(RuntimeError):
  """The publish-time JSON-LD bundle upload failed; the publish must not commit.

  Routers translate this to HTTP 502.
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

  Text-block structures carry no pivot facts, so the fact-driven picker never
  sees them. For each text-block disclosure whose latest standing
  ``factset_type='disclosure'`` set falls inside the report window, copy its
  facts into a ``report`` FactSet for this report, keeping the standing set's
  document provenance. The copy is what keeps a filed report's text fixed when
  the document or binding later changes.

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
  """SHACL-validate the bundle per ``env.REPORT_BUNDLE_SHACL_VALIDATION``.

  ``off`` | ``warn`` | ``strict``. The outcome is recorded on
  ``report.metadata['bundle_validation']``; only ``strict`` blocks the publish,
  on non-conformance or on the validator itself failing.
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
  """Build, upload and stamp the JSON-LD bundle for a Report about to publish.

  Runs before the caller commits. Any S3 failure raises
  :class:`BundleUploadError`: an orphan S3 object is acceptable, a published
  Report without a bundle is not.
  """
  # The extensions session is autoflush=False; the bundler reads the new rows.
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
  body: CreateReportRequest,
  *,
  graph_id: str,
  created_by: str,
) -> ReportResponse:
  """Create a report definition, generate facts, and mark as published.

  Raises `TaxonomyNotFoundError`, `NoEntityError` — caller translates
  to HTTP 422.
  """
  # Accept a tenant taxonomy id or a standard name ('rs-gaap'); a name
  # resolves to the reporting_standard taxonomy, never a linkbase one.
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

  # The Style's close target decides where derived cumulative earnings land
  # (RetainedEarnings / PartnersCapital / MembersEquity by entity form).
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
  # FactSets first, so the facts.fact_set_id FK holds.
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

  structures = load_structures(session, resolved_taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  resp = report_to_response(report_def, structures, entity_name)
  resp.rule_summary = summary
  return resp


def _assert_report_mutable_by(
  report_def: Report, acting_user_id: str, verb: str
) -> None:
  """Only the author may change a report; a shared-in copy is read-only to all.

  That includes the sender, whose ``created_by`` the copy carries. Deleting a
  shared-in copy is widened to graph admins separately in ``delete_report``.
  """
  if report_def.source_graph_id is not None:
    raise NotAuthorizedError(
      f"Report '{report_def.id}' is a copy shared in from another graph and "
      f"cannot be changed here; re-share it from the source graph instead."
    )
  if report_def.created_by != acting_user_id:
    raise NotAuthorizedError(f"Not authorized to {verb} this report.")


def regenerate_report(
  session: Session,
  body: RegenerateReportRequest,
  *,
  graph_id: str,
  created_by: str,
) -> ReportResponse:
  """Regenerate a report with new period dates.

  ``filed`` and ``archived`` reports are immutable; the path past ``filed`` is
  a restatement (a new Report with ``supersedes_id``).

  Raises:
    ReportNotFoundError, NotAuthorizedError.
    InvalidFilingTransitionError: report is ``filed`` or ``archived``.
    ValueError: period_end < period_start.
  """
  # Locked so a concurrent file cannot land between the immutability check
  # and the fact rewrite.
  from robosystems.operations.locking import lock_by_id

  report_def = lock_by_id(
    session,
    Report,
    body.report_id,
    f"Report {body.report_id} is being written by another process. Retry in a moment.",
  )
  if report_def is None:
    raise ReportNotFoundError(body.report_id)
  _assert_report_mutable_by(report_def, created_by, "modify")
  if report_def.filing_status in {"filed", "archived"}:
    raise InvalidFilingTransitionError(
      f"Report '{body.report_id}' is in '{report_def.filing_status}'; "
      f"create a restatement (new Report with supersedes_id) instead of "
      f"regenerating."
    )

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
  delete_report_fact_sets(session, [report_def.id])
  _pre_create_report_fact_sets(
    session,
    report_def.id,
    entity_id,
    created_by,
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
    created_by,
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
    created_by,
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


# draft ↔ under_review → filed → archived. ``filed`` is reached only through
# :func:`file_report` (it stamps the audit fields); this map is everything else.
_LEGAL_NON_FILE_TRANSITIONS: dict[str, set[str]] = {
  "draft": {"under_review"},
  "under_review": {"draft"},
  "filed": {"archived"},
}


class ReportNotFiledError(Exception):
  """Raised when an op requires a ``filed`` Report and got something else."""


def file_report(session: Session, report_id: str, filed_by: str) -> ReportResponse:
  """Transition a Report to ``filed``, stamping ``filed_at`` / ``filed_by``.

  Allowed from ``draft`` or ``under_review``, and only once generation has
  finished (filing an in-progress or failed report would lock a partial
  snapshot).

  Raises ReportNotFoundError, NotAuthorizedError (not the author, or a
  shared-in copy), InvalidFilingTransitionError.
  """
  from datetime import UTC, datetime

  # Locked: last-writer-wins on the filing audit stamp is not acceptable.
  from robosystems.operations.locking import lock_by_id
  from robosystems.operations.roboledger.reads.reports import (
    load_structures,
    report_to_response,
    resolve_entity_name,
  )

  report_def = lock_by_id(
    session,
    Report,
    report_id,
    f"Report {report_id} is being written by another process. Retry in a moment.",
  )
  if report_def is None:
    raise ReportNotFoundError(report_id)
  _assert_report_mutable_by(report_def, filed_by, "file")

  if report_def.filing_status not in {"draft", "under_review"}:
    raise InvalidFilingTransitionError(
      f"Report '{report_id}' is in '{report_def.filing_status}'; "
      f"can only file from 'draft' or 'under_review'."
    )
  # ``complete`` and ``published`` both mean generation finished.
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
  session: Session, report_id: str, target_status: str, acting_user_id: str
) -> ReportResponse:
  """Move a Report along the non-file legs of the filing lifecycle.

  Use :func:`file_report` to reach ``filed``. Same actor rule as filing.
  """
  # Locked: unlocked, a concurrent file could be overwritten, leaving
  # `filed_at` / `filed_by` set on a report back in `draft`.
  from robosystems.operations.locking import lock_by_id
  from robosystems.operations.roboledger.reads.reports import (
    load_structures,
    report_to_response,
    resolve_entity_name,
  )

  report_def = lock_by_id(
    session,
    Report,
    report_id,
    f"Report {report_id} is being written by another process. Retry in a moment.",
  )
  if report_def is None:
    raise ReportNotFoundError(report_id)
  _assert_report_mutable_by(report_def, acting_user_id, "change the status of")

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

  Returns False if the report did not exist.

  A copy shared in from another graph carries the sender's ``created_by``, so
  a graph admin here may delete it instead, and the filed/archived lock (which
  guards an author's audit trail) does not apply to it. The sender's
  ``ReportShare`` row is theirs and is left alone.

  Raises:
    NotAuthorizedError: not the owner (nor an admin deleting a shared-in copy).
    ReportHasActiveSharesError: still shared out; revoke first, since
      `revoke_report_share` needs this row to withdraw the delivered copies.
    ReportNotFiledError: an authored report that is ``filed`` or ``archived``.
  """
  # Locked so a concurrent file cannot slip past the immutability guard.
  from robosystems.operations.locking import lock_by_id

  report_def = lock_by_id(
    session,
    Report,
    report_id,
    f"Report {report_id} is being written by another process. Retry in a moment.",
  )
  if report_def is None:
    return False
  if report_def.created_by != acting_user_id:
    is_shared_copy = report_def.source_graph_id is not None
    if not (is_shared_copy and acting_user_is_graph_admin):
      raise NotAuthorizedError("Not authorized to delete this report.")
  active_share_targets = list(
    session.execute(
      select(ReportShare.target_graph_id).where(
        ReportShare.report_id == report_id,
        ReportShare.revoked_at.is_(None),
      )
    )
    .scalars()
    .all()
  )
  if active_share_targets:
    raise ReportHasActiveSharesError(report_id, active_share_targets)
  # A shared-in copy carries the sender's filing status; guarding it would
  # close the recipient's only exit once a sender filed before sharing.
  if report_def.source_graph_id is None and report_def.filing_status in {
    "filed",
    "archived",
  }:
    raise ReportNotFiledError(
      f"Report '{report_id}' is '{report_def.filing_status}' and cannot "
      f"be deleted. Reach 'archived' via transition-filing-status if "
      f"retiring; deletion is only available for 'draft' or 'under_review'."
    )

  delete_report_fact_sets(session, [report_id])
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

  Takes `graph_id` rather than a session because it opens the source, each
  target, and the platform DB. Raises `PublishListNotFoundError`,
  `PublishListEmptyError`, `ReportNotFoundError`, `NotAuthorizedError`,
  `ReportNotPublishedError`, or `RowLockedError`.

  The source report stays row-locked for the whole share (snapshot, recipient
  copies, ``ReportShare`` rows), including the S3 read, so a concurrent
  delete/regenerate/file or second share cannot interleave and leave copies
  the sender can no longer revoke.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.locking import lock_by_id

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

    report_def = lock_by_id(
      source_session,
      Report,
      report_id,
      f"Report {report_id} is being written by another process. Retry in a moment.",
    )
    if report_def is None:
      raise ReportNotFoundError(report_id)
    # Strict ownership, no admin widening: it guarantees a shared-in copy never
    # has outbound shares, which is what lets `_purge_shared_reports` and the
    # re-share replace delete copies in raw SQL without orphaning share rows.
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
      "generation_count": int(report_def.generation_count or 0),
      # Lets a recipient tell a shared draft from final statements. `filed_by`
      # stays behind: a sender user id means nothing in the recipient graph.
      "filing_status": report_def.filing_status,
      "filed_at": report_def.filed_at,
    }

    # FactSets travel as themselves: every recipient read path resolves a
    # FactSet's Structure, so a flat fact list would be unrenderable.
    fact_set_rows = source_session.execute(
      text("""
        SELECT fs.id, fs.structure_id, fs.factset_type, fs.period_start,
               fs.period_end, fs.entity_id
        FROM fact_sets fs
        WHERE fs.report_id = :report_id
        ORDER BY fs.created_at
      """),
      {"report_id": report_id},
    ).fetchall()
    source_fact_sets = [row._asdict() for row in fact_set_rows]

    fact_rows = source_session.execute(
      text("""
        SELECT f.id, f.fact_set_id, f.element_id, f.value, f.string_value,
               f.fact_type, f.value_type, f.content_type, f.decimals,
               f.period_start, f.period_end, f.period_type, f.unit,
               f.entity_id, f.created_at
        FROM facts f
        JOIN fact_sets fs ON fs.id = f.fact_set_id
        WHERE fs.report_id = :report_id
      """),
      {"report_id": report_id},
    ).fetchall()
    source_facts = [row._asdict() for row in fact_rows]

    publication_artifacts = _load_publication_artifacts(
      graph_id, report_id, int(report_snapshot["generation_count"])
    )

    for target_graph_id in target_graph_ids:
      result = _share_to_target(
        source_graph_id=graph_id,
        report_snapshot=report_snapshot,
        source_fact_sets=source_fact_sets,
        source_facts=source_facts,
        publication_artifacts=publication_artifacts,
        target_graph_id=target_graph_id,
        shared_by=acting_user_id,
      )
      results.append(result)

    successful = [r for r in results if r.status == "shared"]
    if successful:
      now = datetime.now(UTC)
      for result in successful:
        # One active share row per (report, recipient): a re-share replaces
        # the recipient's copy, so it refreshes the row instead of adding one.
        existing = (
          source_session.execute(
            select(ReportShare).where(
              ReportShare.report_id == report_id,
              ReportShare.target_graph_id == result.target_graph_id,
              ReportShare.revoked_at.is_(None),
            )
          )
          .scalars()
          .all()
        )
        if existing:
          for share in existing:
            share.shared_by = acting_user_id
            share.shared_at = now
            share.fact_count = result.fact_count
        else:
          source_session.add(
            ReportShare(
              report_id=report_id,
              target_graph_id=result.target_graph_id,
              shared_by=acting_user_id,
              shared_at=now,
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
  *,
  acting_user_is_graph_admin: bool = False,
) -> RevokeReportShareResponse:
  """Withdraw a shared report from one recipient graph.

  Deletes every copy from the target, then stamps every active share row for
  the target revoked. A copy the recipient already deleted is not an error
  (`copy_deleted` comes back False). The linked `Entity` in the target stays:
  an investor's `Security` may point at it.

  The author or a graph admin may revoke, so an author's departure does not
  strand delivered copies.

  Raises `ReportNotFoundError`, `NotAuthorizedError`, or
  `ReportShareNotFoundError`.
  """
  from robosystems.db.extensions import extensions_session

  active_shares = (
    ReportShare.report_id == report_id,
    ReportShare.target_graph_id == body.target_graph_id,
    ReportShare.revoked_at.is_(None),
  )

  with extensions_session(graph_id) as source_session:
    report_def = source_session.get(Report, report_id)
    if report_def is None:
      raise ReportNotFoundError(report_id)
    if report_def.created_by != acting_user_id and not acting_user_is_graph_admin:
      raise NotAuthorizedError("Not authorized to revoke shares of this report.")

    if not source_session.execute(select(ReportShare.id).where(*active_shares)).first():
      raise ReportShareNotFoundError(
        f"No active share of report '{report_id}' to '{body.target_graph_id}'."
      )

  copy_deleted = _delete_shared_copy(
    source_graph_id=graph_id,
    source_report_id=report_id,
    target_graph_id=body.target_graph_id,
  )

  # Stamp only after the copy is gone, so a failed target write stays
  # retryable instead of recording a withdrawal that did not happen.
  now = datetime.now(UTC)
  with extensions_session(graph_id) as source_session:
    for share in (
      source_session.execute(select(ReportShare).where(*active_shares)).scalars().all()
    ):
      share.revoked_at = now
    source_session.commit()

  return RevokeReportShareResponse(
    report_id=report_id,
    target_graph_id=body.target_graph_id,
    revoked_at=now,
    copy_deleted=copy_deleted,
  )


PUBLICATION_MEDIA_TYPES: dict[str, str] = {
  ".jsonld": "application/ld+json",
  ".holon.jsonld": "application/ld+json",
  ".tavi.json": "application/json",
}

# The artifacts derived on demand off the bundle; the flat JSON-LD is stamped
# at publish and is never rebuilt here.
DERIVED_ARTIFACT_EXTENSIONS: tuple[str, ...] = (".holon.jsonld", ".tavi.json")


def _encode_derived_artifact(extension: str, bundle: StatementBundle) -> str:
  if extension == ".holon.jsonld":
    return serialize_to_holon_jsonld(bundle)
  if extension == ".tavi.json":
    return serialize_to_tavi(bundle).decode("utf-8")
  raise ValueError(f"No derived encoder for {extension!r}")


def _load_publication_artifacts(
  graph_id: str, report_id: str, generation_count: int
) -> dict[str, str]:
  """Read the sender's published artifacts so a share can carry them across.

  Carrying the sender's objects, not re-deriving them from the copied rows,
  makes the recipient's view the exact publication the sender made. The holon
  omits the ``#lineage`` graph by construction, so the books never cross.

  Returns the artifacts that resolved, keyed by file extension. A miss is
  logged and omitted: the row copy is the load-bearing half of a share, so an
  object-store fault degrades rendering rather than failing the delivery.
  """
  from robosystems.db.extensions import extensions_session

  bucket = env.USER_DATA_BUCKET
  s3 = S3Client()
  artifacts: dict[str, str] = {}

  flat = s3.download_string(
    bucket, get_report_bundle_key(graph_id, report_id, generation_count)
  )
  if flat is not None:
    artifacts[".jsonld"] = flat
  else:
    logger.warning(
      "Report %s has no readable JSON-LD bundle; the recipient's copy will "
      "carry no downloadable publication.",
      report_id,
    )

  # Derived flavors exist only once someone downloaded them; build the missing
  # ones off one bundle (keys are immutable per generation, so this also
  # warms the sender's cache).
  missing: dict[str, str] = {}
  for extension in DERIVED_ARTIFACT_EXTENSIONS:
    key = get_report_bundle_key(
      graph_id, report_id, generation_count, extension=extension
    )
    content = s3.download_string(bucket, key)
    if content is None:
      missing[extension] = key
    else:
      artifacts[extension] = content
  if not missing:
    return artifacts

  try:
    with extensions_session(graph_id) as build_session:
      bundle = build_report_bundle(build_session, graph_id, report_id)
  except Exception:
    logger.exception(
      "Failed to bundle report %s; the recipient's copy will fall back to the "
      "package renderer.",
      report_id,
    )
    return artifacts
  for extension, key in missing.items():
    try:
      content = _encode_derived_artifact(extension, bundle)
    except Exception:
      logger.exception(
        "Failed to materialize the %s artifact for report %s; the recipient's "
        "copy will carry the others.",
        extension,
        report_id,
      )
      continue
    s3.upload_string(
      content=content,
      bucket=bucket,
      key=key,
      content_type=PUBLICATION_MEDIA_TYPES[extension],
      metadata={"report-id": report_id, "graph-id": graph_id},
    )
    artifacts[extension] = content

  return artifacts


def _copy_publication_artifacts(
  artifacts: dict[str, str],
  target_graph_id: str,
  shared_report: Report,
  generation_count: int,
) -> None:
  """Write the sender's artifacts under the recipient's own bundle keys.

  Re-keyed because presigning is scoped per graph. ``bundle_url`` must be set
  too: every download flavor is gated on it.
  """
  if not artifacts:
    return
  bucket = env.USER_DATA_BUCKET
  s3 = S3Client()
  for extension, content in artifacts.items():
    key = get_report_bundle_key(
      target_graph_id, shared_report.id, generation_count, extension=extension
    )
    if not s3.upload_string(
      content=content,
      bucket=bucket,
      key=key,
      content_type=PUBLICATION_MEDIA_TYPES.get(extension, "application/octet-stream"),
      metadata={"report-id": shared_report.id, "graph-id": target_graph_id},
    ):
      logger.warning(
        "Failed to copy %s artifact for shared report %s into %s.",
        extension,
        shared_report.id,
        target_graph_id,
      )
      return

  shared_report.generation_count = generation_count
  if ".jsonld" in artifacts:
    shared_report.bundle_url = get_report_bundle_uri(
      bucket, target_graph_id, shared_report.id, generation_count
    )


def delete_report_fact_sets(session: Session, report_ids: Sequence[str]) -> None:
  """Delete the statement sets of these reports, and what hangs off them.

  Facts cascade from ``fact_sets``; ``verification_results.fact_set_id`` has
  no FK, so it is swept here.
  """
  ids = list(report_ids)
  if not ids:
    return
  session.execute(
    text(
      "DELETE FROM verification_results WHERE fact_set_id IN "
      "(SELECT id FROM fact_sets WHERE report_id = ANY(:report_ids))"
    ),
    {"report_ids": ids},
  )
  session.execute(
    text("DELETE FROM fact_sets WHERE report_id = ANY(:report_ids)"),
    {"report_ids": ids},
  )


def delete_report_artifacts(graph_id: str, report_ids: list[str]) -> None:
  """Remove the object-store artifacts of reports whose rows are gone.

  Deletes by prefix: every generation and flavor. **Call after the row
  deletion commits**: a rollback after this would destroy a live report's
  publication, while a crash before it only leaves an orphan.
  """
  if not report_ids:
    return
  bucket = env.USER_DATA_BUCKET
  try:
    s3 = S3Client()
  except Exception as exc:
    # The rows are already gone; don't fail a completed withdrawal.
    logger.warning("Object store unavailable; leaving report artifacts: %s", exc)
    return
  for report_id in report_ids:
    prefix = get_report_bundle_prefix(graph_id, report_id)
    try:
      for key in s3.iter_object_keys(bucket, prefix=prefix):
        if not s3.delete_object(bucket, key):
          logger.warning(
            "Failed to delete withdrawn report artifact s3://%s/%s.",
            bucket,
            key,
          )
    except Exception as exc:
      logger.warning(
        "Failed to list withdrawn report artifacts under s3://%s/%s: %s",
        bucket,
        prefix,
        exc,
      )


def _delete_copies_in_session(
  target_session: Session, source_graph_id: str, source_report_id: str
) -> list[str]:
  """Delete copies of one shared report from an open target session.

  Returns the ids removed, for :func:`delete_report_artifacts` after commit.
  Matches on the provenance pair, so it can never reach a report the target
  authored. Does not commit.
  """
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
    return []

  delete_report_fact_sets(target_session, copy_ids)
  target_session.execute(
    text("DELETE FROM reports WHERE id = ANY(:report_ids)"),
    {"report_ids": copy_ids},
  )
  return copy_ids


def _delete_shared_copy(
  source_graph_id: str, source_report_id: str, target_graph_id: str
) -> bool:
  """Delete the copy of a shared report from the target tenant schema.

  Returns True when a copy was found and removed. A deprovisioned recipient
  (no schema) counts as already removed; raising would leave the share
  unrevocable and the report undeletable.
  """
  from robosystems.db.extensions import extensions_session, tenant_schema_exists

  if not tenant_schema_exists(target_graph_id):
    logger.info(
      f"Recipient {target_graph_id} has no extensions schema; treating the "
      f"shared copy of report {source_report_id} as already removed."
    )
    return False

  with extensions_session(target_graph_id) as target_session:
    deleted = _delete_copies_in_session(
      target_session, source_graph_id, source_report_id
    )
    target_session.commit()

  delete_report_artifacts(target_graph_id, deleted)
  return bool(deleted)


def _share_to_target(
  source_graph_id: str,
  report_snapshot: dict[str, Any],
  source_fact_sets: list[dict[str, Any]],
  source_facts: list[dict[str, Any]],
  target_graph_id: str,
  shared_by: str,
  publication_artifacts: dict[str, str] | None = None,
) -> ShareResultItem:
  """Copy report definition + FactSets + facts to a target tenant schema."""
  from robosystems.db.extensions import extensions_session, tenant_schema_exists
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph
  from robosystems.models.core.graph import GraphStatus

  try:
    with SessionFactory() as platform_session:
      # Teardown never prunes publish lists, so a deprovisioned recipient
      # must read as absent.
      target_graph = platform_session.execute(
        select(Graph).where(
          Graph.graph_id == target_graph_id,
          Graph.status != GraphStatus.DEPROVISIONED.value,
          Graph.deleted_at.is_(None),
        )
      ).scalar_one_or_none()

      if not target_graph:
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error=f"Graph '{target_graph_id}' not found.",
        )

      # Receiving is not authoring: any extensions tenant has the report
      # tables, so an investor-only graph can receive without a ledger.
      extensions = target_graph.schema_extensions or []
      if not any(ext in extensions for ext in _RECEIVING_EXTENSIONS):
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error=(
            "Target graph has no extensions schema — it must have one of: "
            f"{', '.join(_RECEIVING_EXTENSIONS)}."
          ),
        )
  except Exception as e:
    logger.error(f"Failed to validate target graph {target_graph_id}: {e}")
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error="Failed to validate target graph.",
    )

  # The Graph row can outlive the schema; report that per target rather than
  # failing generically when the session bind refuses it.
  if not tenant_schema_exists(target_graph_id):
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error="Target graph has no extensions tenant schema.",
    )

  try:
    now = datetime.now(UTC)
    with extensions_session(target_graph_id) as target_session:
      # Blocked senders are told, not silently dropped. Fails closed: if the
      # block table is missing this raises into the error item below.
      if is_source_blocked(target_session, source_graph_id):
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error="Recipient has blocked shares from this graph.",
        )

      # A re-share replaces the previous copy rather than duplicating it.
      replaced_copy_ids = _delete_copies_in_session(
        target_session, source_graph_id, report_snapshot["id"]
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
        # A shared-in copy can never transition, so it must arrive with the
        # sender's status rather than the `draft` default.
        filing_status=report_snapshot.get("filing_status") or "draft",
        filed_at=report_snapshot.get("filed_at"),
        created_by=shared_by,
        source_graph_id=source_graph_id,
        source_report_id=report_snapshot["id"],
        shared_at=now,
      )
      target_session.add(shared_report)
      target_session.flush()

      # Needs the flushed id: the recipient's bundle keys use its own report id.
      _copy_publication_artifacts(
        publication_artifacts or {},
        target_graph_id,
        shared_report,
        int(report_snapshot.get("generation_count") or 0),
      )

      # Library structures have deterministic UUID5 ids in every tenant, so
      # they resolve on the far side; tenant-local `struct_*` ones do not, and
      # their facts fall back to one structure-less set (queryable, not
      # renderable as a statement).
      candidate_structure_ids = {
        fs["structure_id"] for fs in source_fact_sets if fs.get("structure_id")
      }
      resolvable_structure_ids: set[str] = set()
      if candidate_structure_ids:
        resolvable_structure_ids = {
          row[0]
          for row in target_session.execute(
            text("SELECT id FROM structures WHERE id = ANY(:ids)"),
            {"ids": list(candidate_structure_ids)},
          ).fetchall()
        }

      facts_by_source_set: dict[str, list[dict[str, Any]]] = {}
      for fact_data in source_facts:
        facts_by_source_set.setdefault(fact_data["fact_set_id"], []).append(fact_data)

      # The originating ledger is not present in the target graph, so the
      # shared facts collapse to `asserted` provenance referencing the
      # source graph/report rather than a re-derivable pivot.
      def _provenance(source_fact_set_id: str | None) -> AssertedProvenance:
        basis = f"source_graph={source_graph_id} source_report={report_snapshot['id']}"
        if source_fact_set_id is not None:
          basis = f"{basis} source_fact_set={source_fact_set_id}"
        return AssertedProvenance(
          source_system="cross_graph_share",
          asserted_by=shared_by,
          basis_note=basis,
        )

      # One target FactSet per source FactSet whose Structure resolves; the
      # remainder pool into one structure-less set so nothing is dropped.
      target_set_for_source: dict[str, str] = {}
      unresolved_facts: list[dict[str, Any]] = []
      for source_set in source_fact_sets:
        source_set_id = source_set["id"]
        set_facts = facts_by_source_set.get(source_set_id, [])
        if not set_facts:
          continue
        structure_id = source_set.get("structure_id")
        if not structure_id or structure_id not in resolvable_structure_ids:
          unresolved_facts.extend(set_facts)
          continue
        copied_set = create_fact_set(
          target_session,
          structure_id=structure_id,
          period_start=source_set["period_start"],
          period_end=source_set["period_end"],
          factset_type=source_set["factset_type"],
          entity_id=source_set["entity_id"],
          report_id=shared_report.id,
          created_by=shared_by,
          provenance=_provenance(source_set_id),
        )
        target_session.flush()
        target_set_for_source[source_set_id] = str(copied_set.id)

      catch_all_set_id: str | None = None
      if unresolved_facts:
        starts = [
          fd["period_start"]
          for fd in unresolved_facts
          if fd.get("period_start") is not None
        ]
        ends = [
          fd["period_end"]
          for fd in unresolved_facts
          if fd.get("period_end") is not None
        ]
        # `facts.period_end` is NOT NULL, so `ends` is non-empty here.
        catch_all_set = create_fact_set(
          target_session,
          structure_id=None,
          period_start=min(starts) if starts else None,
          period_end=max(ends),
          factset_type="report",
          entity_id=unresolved_facts[0]["entity_id"],
          report_id=shared_report.id,
          created_by=shared_by,
          provenance=_provenance(None),
        )
        target_session.flush()
        catch_all_set_id = str(catch_all_set.id)

      # Concepts must land before the facts that cite them.
      _ensure_shared_elements(
        target_session,
        source_graph_id,
        {fd["element_id"] for fd in source_facts if fd.get("element_id")},
        shared_by,
        report_taxonomy_id=report_snapshot["taxonomy_id"],
      )

      for fact_data in source_facts:
        target_set_id = target_set_for_source.get(
          fact_data["fact_set_id"], catch_all_set_id
        )
        if target_set_id is None:
          continue
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
          fact_set_id=target_set_id,
        )
        target_session.add(rf)

      _ensure_linked_entity(target_session, source_graph_id, shared_by)

      # Re-checked: a block committed mid-copy must win, or the recipient's
      # purge (already run) would miss this copy.
      if is_source_blocked(target_session, source_graph_id):
        target_session.rollback()
        return ShareResultItem(
          target_graph_id=target_graph_id,
          status="error",
          error="Recipient has blocked shares from this graph.",
        )
      target_session.commit()

    delete_report_artifacts(target_graph_id, replaced_copy_ids)

    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="shared",
      fact_count=len(source_facts),
    )

  except Exception as e:
    # Detail stays in the log: the error is about the recipient's schema, and
    # the sender is a different tenant.
    logger.error(f"Failed to share report to {target_graph_id}: {e}")
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error="Failed to copy report data.",
    )


def _ensure_shared_elements(
  target_session: Session,
  source_graph_id: str,
  element_ids: set[str],
  shared_by: str,
  report_taxonomy_id: str | None = None,
) -> None:
  """Copy the sender's own concepts into the recipient's schema.

  ``facts.element_id`` has no FK, and one dangling concept makes the
  recipient's whole next materialization fail. Library concepts resolve by
  deterministic UUID5 in every tenant; the sender's own reporting-extension
  ``elem_*`` ids do not, so those taxonomies are copied whole (the self-FK
  ``parent_id`` points at abstract heads no fact cites).

  ``report_taxonomy_id`` is ensured independently of the facts: it can be
  missing even when every fact resolves, and ``reports.taxonomy_id`` has no
  FK either.

  Copies are ``source='linked'``, outside ``COA_SOURCES``, so they never show
  in the recipient's chart of accounts. Only ``reporting_extension``
  taxonomies travel; a foreign chart of accounts is never copied.

  Fails closed: a source read failure propagates so the caller rolls back the
  target instead of writing dangling references.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Element, Taxonomy

  missing: set[str] = set()
  if element_ids:
    present = {
      row[0]
      for row in target_session.execute(
        text("SELECT id FROM elements WHERE id = ANY(:ids)"),
        {"ids": list(element_ids)},
      ).fetchall()
    }
    missing = element_ids - present

  report_taxonomy_missing = (
    bool(report_taxonomy_id)
    and not target_session.execute(
      text("SELECT 1 FROM taxonomies WHERE id = :tid"),
      {"tid": report_taxonomy_id},
    ).first()
  )

  if not missing and not report_taxonomy_missing:
    return

  # Built through the models, not raw INSERT, so Python-side defaults on
  # NOT NULL columns apply.
  _TAX_FIELDS = (
    "id",
    "name",
    "description",
    "taxonomy_type",
    "version",
    "standard",
    "namespace_uri",
    "parent_taxonomy_id",
    "extension_type",
    "effective_date",
  )
  _ELEM_FIELDS = (
    "id",
    "code",
    "name",
    "description",
    "qname",
    "namespace",
    "uri",
    "balance_type",
    "period_type",
    "substitution_group",
    "is_abstract",
    "is_monetary",
    "element_type",
    "item_type",
    "taxonomy_id",
    "parent_id",
    "depth",
    "path",
    "currency",
    "is_active",
  )

  with extensions_session(source_graph_id) as source_session:
    taxonomy_ids: set[str] = set()
    if missing:
      taxonomy_ids = {
        row[0]
        for row in source_session.execute(
          text("""
            SELECT DISTINCT e.taxonomy_id
            FROM elements e
            JOIN taxonomies t ON t.id = e.taxonomy_id
            WHERE e.id = ANY(:ids)
              AND t.taxonomy_type = 'reporting_extension'
          """),
          {"ids": list(missing)},
        ).fetchall()
      }
      if not taxonomy_ids:
        logger.warning(
          f"Shared facts from {source_graph_id} cite {len(missing)} concept(s) "
          "outside any reporting extension; not copied."
        )

    if report_taxonomy_missing:
      # Same reporting_extension restriction as the element path.
      if source_session.execute(
        text(
          "SELECT 1 FROM taxonomies "
          "WHERE id = :tid AND taxonomy_type = 'reporting_extension'"
        ),
        {"tid": report_taxonomy_id},
      ).first():
        taxonomy_ids.add(str(report_taxonomy_id))
      else:
        logger.warning(
          f"Shared report from {source_graph_id} cites taxonomy "
          f"{report_taxonomy_id!r}, which the recipient does not have and "
          "which is not a reporting extension; not copied."
        )

    if not taxonomy_ids:
      return

    copy_ids = sorted(taxonomy_ids)

    # Plain dicts: these become new rows in another schema.
    taxonomies = [
      ({f: getattr(t, f) for f in _TAX_FIELDS}, dict(t.metadata_ or {}))
      for t in source_session.execute(select(Taxonomy).where(Taxonomy.id.in_(copy_ids)))
      .scalars()
      .all()
    ]
    elements = [
      {f: getattr(e, f) for f in _ELEM_FIELDS}
      for e in source_session.execute(
        select(Element).where(Element.taxonomy_id.in_(copy_ids))
      )
      .scalars()
      .all()
    ]

  existing_taxonomies = {
    row[0]
    for row in target_session.execute(
      text("SELECT id FROM taxonomies WHERE id = ANY(:ids)"),
      {"ids": [f["id"] for f, _ in taxonomies]},
    ).fetchall()
  }

  # Two passes: `parent_taxonomy_id` is a self-FK, so insert parentless first.
  tax_parents: dict[str, str] = {}
  for fields, metadata in taxonomies:
    if fields["id"] in existing_taxonomies:
      continue
    parent_id = fields.pop("parent_taxonomy_id")
    if parent_id:
      tax_parents[fields["id"]] = parent_id
    target_session.add(
      Taxonomy(
        **fields,
        parent_taxonomy_id=None,
        metadata_={**metadata, "source_graph_id": source_graph_id},
        is_shared=False,
        is_locked=True,
        created_by=shared_by,
      )
    )
  target_session.flush()

  # Wire the parents that resolve; leave the rest null rather than fail the
  # share over a taxonomy the recipient happens not to have.
  if tax_parents:
    resolvable_tax = {
      row[0]
      for row in target_session.execute(
        text("SELECT id FROM taxonomies WHERE id = ANY(:ids)"),
        {"ids": list(set(tax_parents.values()))},
      ).fetchall()
    }
    for child_id, parent_id in tax_parents.items():
      if parent_id in resolvable_tax:
        target_session.execute(
          text("UPDATE taxonomies SET parent_taxonomy_id = :pid WHERE id = :cid"),
          {"pid": parent_id, "cid": child_id},
        )
    target_session.flush()

  element_ids_in = [f["id"] for f in elements]
  existing_elements = {
    row[0]
    for row in target_session.execute(
      text("SELECT id FROM elements WHERE id = ANY(:ids)"),
      {"ids": element_ids_in},
    ).fetchall()
  }
  # qname is UNIQUE per tenant; two senders can share a prefix. Skip the
  # colliding concept rather than fail the share (the materializer's inner
  # join drops its edge).
  taken_qnames = {
    row[0]
    for row in target_session.execute(
      text("SELECT qname FROM elements WHERE qname = ANY(:qnames) AND id != ALL(:ids)"),
      {
        "qnames": [f["qname"] for f in elements if f["qname"]],
        "ids": element_ids_in,
      },
    ).fetchall()
  }

  # Two passes for the `parent_id` self-FK (`depth` is always 0, so it cannot
  # order inserts). Parents outside the copied taxonomies stay null.
  parents: dict[str, str] = {}
  skipped_qname = 0
  for fields in elements:
    if fields["id"] in existing_elements:
      continue
    if fields["qname"] and fields["qname"] in taken_qnames:
      logger.warning(
        f"Concept {fields['qname']!r} from {source_graph_id} collides with an "
        "existing qname in the recipient; skipped."
      )
      skipped_qname += 1
      continue
    parent_id = fields.pop("parent_id")
    if parent_id:
      parents[fields["id"]] = parent_id
    target_session.add(
      Element(**fields, parent_id=None, source="linked", created_by=shared_by)
    )
  target_session.flush()

  resolvable = {
    row[0]
    for row in target_session.execute(
      text("SELECT id FROM elements WHERE id = ANY(:ids)"),
      {"ids": list(set(parents.values()))},
    ).fetchall()
  }
  for child_id, parent_id in parents.items():
    if parent_id in resolvable:
      target_session.execute(
        text("UPDATE elements SET parent_id = :pid WHERE id = :cid"),
        {"pid": parent_id, "cid": child_id},
      )
  target_session.flush()

  copied = len(elements) - len(existing_elements) - skipped_qname
  uncovered = missing - {f["id"] for f in elements}
  if uncovered:
    logger.warning(
      f"{len(uncovered)} concept(s) cited by facts from {source_graph_id} "
      f"belong to no reporting extension and were not copied: "
      f"{sorted(uncovered)[:5]}"
    )

  logger.info(
    f"Copied {copied} concept(s) in {len(taxonomies)} reporting extension(s) "
    f"from {source_graph_id} alongside the shared report."
  )


def _ensure_linked_entity(
  target_session: Session, source_graph_id: str, shared_by: str
) -> None:
  """Upsert a linked Entity for the source company in the target graph.

  Also links unlinked securities carrying the same ``source_graph_id``.
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
