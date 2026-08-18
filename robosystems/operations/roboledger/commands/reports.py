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
  serialize_to_holon_jsonld,
  serialize_to_rdf,
)

# Extensions that give a graph a tenant schema, and so make it a legitimate
# recipient of a shared report. Mirrors `_REPORT_EXTENSIONS` on the GraphQL
# read side — the two ends of the same seam.
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
  # Locked: the guard below decides from `filing_status` — filed and archived
  # reports are immutable — and the regeneration then rewrites the report's
  # facts. Unlocked, a file landing between that check and the rewrite gets a
  # report stamped `filed` whose contents were replaced underneath it, which
  # is the state the immutability check exists to prevent.
  from robosystems.operations.locking import lock_by_id

  report_def = lock_by_id(
    session,
    Report,
    report_id,
    f"Report {report_id} is being written by another process. Retry in a moment.",
  )
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

  # Locked: the two guards below decide from `filing_status` and
  # `generation_status`, then write the first. Lower stakes than the ledger
  # transitions — a concurrent double-file overwrites the audit stamp rather
  # than duplicating anything — but "who filed this, and when" is exactly the
  # field an auditor reads, so last-writer-wins is not good enough for it.
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

  Raises `ReportHasActiveSharesError` if the report is still shared out to
  other graphs. `revoke_report_share` needs this row to find and authorize the
  withdrawal, so deleting first would strand every delivered copy in its
  recipient's schema permanently — precisely when withdrawal matters most,
  since a report deleted after distribution is usually one that was wrong.
  Revoke each recipient first. Shared *copies* are unaffected: the recipient's
  schema holds no share rows for a report they did not send.
  """
  report_def = session.get(Report, report_id)
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
    # Strict ownership, deliberately — NOT the graph-admin widening that
    # `delete_report` and `revoke_report_share` carry. Sharing is a publishing
    # act, and this rule is load-bearing beyond authorization: because only an
    # author can share, a report that arrived from elsewhere can never acquire
    # outbound shares of its own. That is what lets `_purge_shared_reports` and
    # the re-share replace in `_share_to_target` delete copies in raw SQL
    # without tripping `ReportHasActiveSharesError` — they cannot orphan a share
    # row, because a shared copy has none. Widen this to admins and both paths
    # start stranding delivered copies in recipients' schemas with no way to
    # withdraw them, which is precisely what that guard exists to prevent.
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
    }

    # The FactSets travel as themselves, not as one flat fact list. Every
    # read path on the recipient's side resolves a FactSet's Structure —
    # `get_report_package` joins it, `load_fact_set_by_id_for_structure`
    # pins on it — so a share that collapses N sets into one structure-less
    # set delivers facts that nothing downstream can render.
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

  # Read once, after the sender's session closes — S3 round-trips have no
  # business holding a database connection open — and reused for every target,
  # since each gets the same bytes.
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
    with extensions_session(graph_id) as source_session:
      for result in successful:
        # One active share row per (report, recipient). A re-share replaces
        # the recipient's copy rather than adding a second, so the record of
        # it must not fan out either — a second active row would describe a
        # delivery that no longer exists, and revocation would have to guess
        # which one it withdrew.
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

  The sender's half of the share controls. Takes `graph_id` rather than a
  session for the same reason `share_report` does — it spans the source and
  target tenant schemas.

  Stamps `ReportShare.revoked_at` in the source and deletes the copied Report
  (and its fact sets, whose facts cascade) from the target. A recipient who
  already deleted the copy themselves is not an error: the share is still
  marked revoked and `copy_deleted` comes back False.

  Revocation is *plural on both sides*: `_delete_shared_copy` removes every
  copy carrying this provenance pair, so every active share row for the target
  is stamped to match. Two shares of one report to one recipient are ordinary —
  two overlapping publish lists, or a resend after a correction — and leaving
  one row active would claim a delivery that no longer exists.

  The linked `Entity` in the target is deliberately left in place. An investor's
  `Security` points at it, and the relationship survives one report being
  pulled — deleting it would break the link over a single withdrawal.

  Normally restricted to the report's author, who is also the only user who
  could have shared it. A graph admin may also revoke, so an author's departure
  does not strand delivered copies in recipients' schemas — the same reasoning
  that widened `delete_report`.

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

  # Stamp only after the copy is gone. If the target write fails, the shares
  # stay un-revoked and the operation is safe to retry — the alternative
  # leaves a record claiming the data was withdrawn while it is still there.
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


def _load_publication_artifacts(
  graph_id: str, report_id: str, generation_count: int
) -> dict[str, str]:
  """Read the sender's published artifacts so a share can carry them across.

  The **holon** — the report as ``#scene`` / ``#boundary`` / ``#projection``
  named graphs — is the intended cross-tenant wire format: one self-contained
  serialization that renders outside the system entirely, and whose partition
  omits the ``#lineage`` graph by construction, so the books never cross with
  the report. Carrying the sender's *object* rather than re-deriving one from
  the recipient's copied rows is what makes the recipient's view the
  publication the sender actually made, byte for byte, instead of a
  reconstruction that can drift from it.

  Returns the artifacts that resolved, keyed by file extension. A miss is
  logged and omitted rather than raised: the row copy is the load-bearing half
  of a share — it is what materializes into the recipient's graph and carries
  the cross-graph traversal — so an object-store fault degrades the
  recipient's renderers rather than failing the delivery outright.

  Called after the sender's session closes, so none of this object-store I/O
  holds a database connection open. The holon build below needs a session and
  opens its own short-lived one, on the cold path only.
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

  # The holon is derived on demand, not stamped at publish, so a report nobody
  # has downloaded in that flavor yet has no object. Build it once here — the
  # key is immutable per generation, so this also warms the sender's cache.
  holon_key = get_report_bundle_key(
    graph_id, report_id, generation_count, extension=".holon.jsonld"
  )
  holon = s3.download_string(bucket, holon_key)
  if holon is None:
    try:
      with extensions_session(graph_id) as build_session:
        holon = serialize_to_holon_jsonld(
          build_report_bundle(build_session, graph_id, report_id)
        )
      s3.upload_string(
        content=holon,
        bucket=bucket,
        key=holon_key,
        content_type="application/ld+json",
        metadata={"report-id": report_id, "graph-id": graph_id},
      )
    except Exception:
      logger.exception(
        "Failed to materialize the holon for report %s; the recipient's copy "
        "will fall back to the package renderer.",
        report_id,
      )
      holon = None
  if holon is not None:
    artifacts[".holon.jsonld"] = holon

  return artifacts


def _copy_publication_artifacts(
  artifacts: dict[str, str],
  target_graph_id: str,
  shared_report: Report,
  generation_count: int,
) -> None:
  """Write the sender's artifacts under the recipient's own bundle keys.

  Re-keying rather than sharing the sender's object is deliberate: the
  recipient's copy is a distinct Report with its own id, and presigning is
  scoped per graph. ``bundle_url`` has to land too — every download flavor is
  gated on it (``get_report_bundle_download``), so without it the recipient's
  Holon and download surfaces stay dark even with the objects in place.
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
      content_type="application/ld+json",
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


def delete_report_artifacts(graph_id: str, report_ids: list[str]) -> None:
  """Remove the object-store artifacts of reports whose rows are gone.

  Withdrawal has to reach object storage now that a shared copy carries
  artifacts of its own. Before the holon travelled with the rows, a recipient's
  copy had none — `bundle_url` was always NULL — so revoke and block-and-purge
  were complete by construction. They no longer are, and an exit that leaves
  the sender's published report sitting in the recipient's prefix is not the
  exit either control advertises.

  Deletes by prefix, so every generation and both flavors go together.

  **Call this after the row deletion commits, never before.** Deleting an
  artifact for a transaction that then rolls back destroys a live report's
  publication; an artifact left behind by a crash is an orphan a later sweep
  can take. The asymmetry decides the ordering.
  """
  if not report_ids:
    return
  bucket = env.USER_DATA_BUCKET
  s3 = S3Client()
  for report_id in report_ids:
    prefix = get_report_bundle_prefix(graph_id, report_id)
    for key in s3.list_objects(bucket, prefix=prefix):
      if not s3.delete_object(bucket, key):
        logger.warning(
          "Failed to delete withdrawn report artifact s3://%s/%s.", bucket, key
        )


def _delete_copies_in_session(
  target_session: Session, source_graph_id: str, source_report_id: str
) -> list[str]:
  """Delete copies of one shared report from an open target session.

  Returns the ids removed, so the caller can drop their object-store artifacts
  once the transaction commits (see :func:`delete_report_artifacts`). Matches
  on the provenance pair, so it can only ever reach a report that arrived from
  this sender — a report the target authored has a null `source_graph_id` and
  cannot match. Does not commit; the caller owns the transaction.
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

  # Facts cascade from their parent fact_sets, so the fact_sets go first.
  target_session.execute(
    text("DELETE FROM fact_sets WHERE report_id = ANY(:report_ids)"),
    {"report_ids": copy_ids},
  )
  target_session.execute(
    text("DELETE FROM reports WHERE id = ANY(:report_ids)"),
    {"report_ids": copy_ids},
  )
  return copy_ids


def _delete_shared_copy(
  source_graph_id: str, source_report_id: str, target_graph_id: str
) -> bool:
  """Delete the copy of a shared report from the target tenant schema.

  Returns True when a copy was found and removed.

  A recipient whose graph has been deprovisioned has no schema left, and is
  treated as "the copy is already gone" rather than an error. Raising would
  strand the share permanently: revocation stamps ``revoked_at`` only after
  this returns, and ``delete_report`` refuses a report that still has an
  active share row — so the sender could neither withdraw the report nor
  delete it, over a recipient that no longer exists. Deleting the schema
  deleted the copy; that is the outcome revocation was asking for.
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

      # Receiving is not authoring. `provision_tenant_schema` creates every
      # tenant table regardless of the graph's extensions, so an investor-only
      # tenant can hold a report perfectly well — and requiring `roboledger`
      # of a recipient would mean a fund had to provision a ledger it will
      # never post to just to read what its portfolio companies send it. The
      # gate stays, narrowed to "has an extensions tenant at all", so a share
      # still cannot land in a graph that never opted into the OLTP surface.
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

      # A re-share replaces rather than accumulates. Sharing one report to one
      # recipient twice is ordinary — two overlapping publish lists, or a
      # resend after a correction — and without this the recipient's books
      # collect duplicate copies of the same statement, each materializing
      # into their graph. The provenance pair can only ever match a copy from
      # this sender; a report the recipient authored has a null
      # `source_graph_id`.
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
        created_by=shared_by,
        source_graph_id=source_graph_id,
        source_report_id=report_snapshot["id"],
        shared_at=now,
      )
      target_session.add(shared_report)
      target_session.flush()

      # The holon crosses with the rows. Needs the flushed id, since the
      # recipient's bundle keys are scoped by their own report id.
      _copy_publication_artifacts(
        publication_artifacts or {},
        target_graph_id,
        shared_report,
        int(report_snapshot.get("generation_count") or 0),
      )

      # A Structure id crosses the graph boundary only if it resolves on the
      # far side. Library-seeded structures do: `provision_tenant_schema`
      # copies the canonical library into every tenant schema, and library
      # ids are deterministic UUID5 over the taxonomy source — so the
      # sender's "rs-gaap — Balance Sheet — Classified" *is* the recipient's,
      # same id, same presentation arcs, no copying required. Tenant-local
      # structures (`struct_*` ULIDs — a custom disclosure, a bespoke
      # schedule) do not resolve, and their facts fall back to a single
      # structure-less set below: delivered and queryable, but not
      # renderable as a statement until the holon import half can recreate
      # the structure itself in the recipient.
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
        # `facts.period_end` is NOT NULL on both sides, so `ends` is non-empty
        # whenever `unresolved_facts` is — no fallback, which would only be
        # able to supply the NULL that `fact_sets.period_end` rejects.
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

      # The concepts have to land before the facts that cite them. A fact
      # whose element_id resolves nowhere is not a partial delivery — it
      # takes down the recipient's entire next materialization (see
      # `_ensure_shared_elements`). The report's own taxonomy is passed
      # separately because it can be missing when every fact resolves.
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

      target_session.commit()

    # After the commit, and only for the ids the replace actually removed — the
    # new copy has a fresh report id, so its own artifacts are under a
    # different prefix and cannot be caught by this.
    delete_report_artifacts(target_graph_id, replaced_copy_ids)

    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="shared",
      fact_count=len(source_facts),
    )

  except Exception as e:
    # The detail stays in the log, not in the response: this exception was
    # raised against the *recipient's* schema, and the sender is a different
    # tenant. A raw driver message would tell them about the recipient's
    # database — the same reason `enrich_blocks` resolves a name and not an org.
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

  A shared fact carries an ``element_id``, and that column has no foreign
  key — so a fact citing a concept the recipient has never heard of is
  written without complaint and only surfaces two steps later, in the
  graph. **Library concepts are fine**: `copy_library_into_tenant` gives
  every tenant the same deterministic UUID5 ids, so rs-gaap resolves
  identically on both sides. The sender's *own* reporting extension does
  not — those are ``elem_*`` ULIDs minted in the sender's schema alone,
  and the recipient has no row for them.

  The blast radius is why this is not a cosmetic gap. LadybugDB rejects an
  edge whose endpoint has no primary key, blue/green treats any table
  error as a partial run and abandons the whole WIP database, so **one**
  unresolvable concept stops the recipient's entire graph from
  materializing — their portfolios and positions included, none of which
  had anything to do with the share. A disclosure note or a custom metric
  is enough to trigger it, which makes it the ordinary case rather than an
  edge one.

  The unit copied is the *taxonomy*, not the individual element: elements
  carry a self-referencing ``parent_id``, and the abstract head a note
  hangs off is typically not itself cited by any fact. Copying element-wise
  would violate that FK on the first note.

  ``report_taxonomy_id`` is ensured **independently of the facts**, because
  the two can go missing separately. A report built on a sender-specific
  reporting extension whose facts all cite standard concepts leaves
  ``element_ids`` fully resolvable and the taxonomy absent — and
  ``reports.taxonomy_id`` has no foreign key either, so the copy is written
  without complaint and ``REPORT_USES_TAXONOMY`` then points at nothing.
  Keying the taxonomy copy off missing *elements* made delivery of the one
  depend on the absence of the other.

  Copies are stamped ``source='linked'``, which is deliberately outside
  ``COA_SOURCES``. The recipient needs the sender's concepts to read the
  report; they must never appear in the recipient's own chart of accounts.
  Only ``reporting_extension`` taxonomies travel — a fact citing the
  sender's CoA would be a different (and wrong) situation, and is left for
  the materializer's own guard to absorb rather than dragging a foreign
  chart of accounts across the boundary.

  **Fails closed.** A read failure against the source propagates, and
  ``_share_to_target``'s handler turns it into a per-recipient error item
  with the target transaction rolled back. Swallowing it would let the
  fact-insert loop below write the exact dangling references this function
  exists to prevent — the original defect, behind a rarer trigger.
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

  # Copied field-by-field rather than by raw INSERT: half of both tables is
  # NOT NULL with a Python-side default and no database default, so a
  # hand-written column list is wrong the day someone adds a column.
  # Constructing the models lets those defaults apply.
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
      # Same `reporting_extension` restriction the element path applies, for
      # the same two reasons: a report built on a library framework resolves
      # by deterministic id in every tenant and needs no copy, and a report
      # somehow bound to the sender's chart of accounts must not drag that
      # chart across the boundary. When neither holds, the edge is dropped by
      # the materializer's own join rather than copied.
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

    # Detached from the source session before it closes — these become new
    # rows in a different schema, not attached instances.
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

  # Same two-pass shape as the elements below, and for the same reason:
  # `parent_taxonomy_id` is a self-FK, so an extension whose parent is
  # another extension in this batch would depend on insert order. It names a
  # library taxonomy in every case seen so far — but "every case seen so far"
  # is what the depth ordering relied on too.
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
  # `idx_elements_qname` is UNIQUE across the whole tenant, so a second
  # sender using the same namespace prefix — `acme:` is nobody's reserved
  # word — would otherwise fail the entire share on a collision neither
  # party controls. Skip the colliding concept instead; its facts land
  # without a concept edge, which the materializer's inner join already
  # absorbs.
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

  # Two passes, because `parent_id` is a self-FK and **`depth` cannot order
  # it**: the column defaults to 0 and nothing populates it, so every row in
  # a tenant carries depth 0 and sorting by it is a no-op. An earlier version
  # ordered by depth and passed its tests on whatever order the heap happened
  # to return. Insert parentless, then wire parents that resolve — which also
  # covers a parent living outside the copied taxonomies (the sender's CoA,
  # say) without failing the share, matching how `parent_taxonomy_id` is
  # handled above.
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
  # Concepts cited by a fact but not carried by any copied extension — a
  # mixed batch copies what it can and drops the rest, so name them rather
  # than leaving the omission silent.
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
