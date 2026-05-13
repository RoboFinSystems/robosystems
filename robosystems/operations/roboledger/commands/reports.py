"""Write operations for report definitions.

Ported from `routers/ledger/reports.py`. Includes the complex share
path which copies a report + its facts into a target graph's tenant
schema — that helper (`_share_to_target`) stays internal here because
it's only used by `share_report`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ReportResponse,
  ShareReportRequest,
  ShareReportResponse,
  ShareResultItem,
)
from robosystems.models.extensions import (
  Fact,
  FactSet,
  PublishList,
  PublishListMember,
  Report,
  ReportShare,
)
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands._guards import (
  rule_summary as _rule_summary,
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
  NoNetworkForStatementTypeError,
  get_render_network,
  load_graph_reporting_style,
)
from robosystems.utils.ulid import generate_prefixed_ulid


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


class NoEntityError(Exception):
  """Raised when `create_report` can't find an Entity to tag facts to."""


class TaxonomyNotFoundError(LookupError):
  """Raised when `create_report` references a missing taxonomy."""


def _get_entity_id(session: Session, graph_id: str) -> str:
  """Get the earliest-created entity ID — the primary entity for single-entity graphs."""
  result = session.execute(
    text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
  )
  row = result.fetchone()
  if row is None:
    raise NoEntityError("No entity found. Import data before creating reports.")
  return row.id


def _evaluate_report_structures(
  session: Session,
  facts,
  element_to_structure: dict[str, str],
  structure_to_factset: dict[str, str],
  period_start,
  period_end,
  created_by: str,
) -> dict[str, int] | None:
  """Run rule evaluation for every structure that received report facts.

  Returns an aggregated rule_summary across all structures, or None if
  no structures have rules.
  """
  structures_with_facts = {
    element_to_structure[f.element_id]
    for f in facts.facts
    if f.element_id in element_to_structure
  }
  all_results = []
  for structure_id in structures_with_facts:
    results = evaluate_rules_for_structure(
      session,
      structure_id,
      fact_set_id=structure_to_factset[structure_id],
      period_start=period_start,
      period_end=period_end,
      created_by=created_by,
    )
    all_results.extend(results)
  return _rule_summary(all_results)


_RENDER_TARGET_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


def _build_structure_mapping(
  session: Session,
  reporting_style_id: str,
) -> tuple[dict[str, str], dict[str, str]]:
  """Return (element_id→structure_id, structure_id→fact_set_id) for the
  Reporting Style composed for this graph.

  Walks ``reporting_style_networks`` to pick one Network per statement
  type (per §3.2 Phase 1), then enumerates each Network's association
  rows to map elements → structure id. Pre-generates a fact_set_id
  ULID per picked structure.

  Networks for statement types the Style doesn't compose are skipped
  silently — a Style is free to omit, say, the comprehensive_income
  Network. Statement types the renderer asks for that the Style
  *does* try to render but lacks a composition row for surface as
  ``NoNetworkForStatementTypeError`` at picker time (the caller wraps
  the loop and decides whether to fail closed).
  """
  element_to_structure: dict[str, str] = {}
  structure_to_factset: dict[str, str] = {}
  picked_structure_ids: list[str] = []

  for statement_type in _RENDER_TARGET_STATEMENT_TYPES:
    try:
      network = get_render_network(session, reporting_style_id, statement_type)
    except NoNetworkForStatementTypeError:
      # Style doesn't compose this statement type — skip silently so a
      # Style that ships without (say) an equity Network still renders.
      continue
    picked_structure_ids.append(network.structure_id)

  if not picked_structure_ids:
    return {}, {}

  rows = session.execute(
    text(
      """
      SELECT DISTINCT
        a.structure_id AS structure_id,
        a.to_element_id AS element_id
      FROM associations a
      WHERE a.structure_id = ANY(:struct_ids)
      """
    ),
    {"struct_ids": picked_structure_ids},
  ).fetchall()

  element_to_structure = {row.element_id: row.structure_id for row in rows}
  structure_to_factset = {
    sid: generate_prefixed_ulid("fs") for sid in picked_structure_ids
  }
  return element_to_structure, structure_to_factset


def _persist_report_facts(
  session: Session,
  report_id: str,
  facts,
  entity_id: str,
  element_to_structure: dict[str, str] | None = None,
  structure_to_factset: dict[str, str] | None = None,
) -> None:
  """Clear any existing facts for this report and persist the new set.

  If `element_to_structure` / `structure_to_factset` are provided, each
  Fact is stamped with `structure_id` and `fact_set_id` so the Information
  Block envelope can resolve its FactSet row. `report_id` is still set —
  the CHECK constraint requires at least one of the two to be non-null.
  """
  session.execute(
    text("DELETE FROM facts WHERE report_id = :report_id"),
    {"report_id": report_id},
  )
  elem_map = element_to_structure or {}
  fs_map = structure_to_factset or {}
  for fact in facts.facts:
    structure_id = elem_map.get(fact.element_id)
    fact_set_id = fs_map.get(structure_id) if structure_id else None
    rf = Fact(
      report_id=report_id,
      element_id=fact.element_id,
      value=fact.value,
      period_start=fact.period_start,
      period_end=fact.period_end,
      period_type=fact.period_type,
      unit="USD",
      entity_id=entity_id,
      structure_id=structure_id,
      fact_set_id=fact_set_id,
    )
    session.add(rf)


def _pre_create_report_fact_sets(
  session: Session,
  report_id: str,
  entity_id: str,
  created_by: str,
  periods,
  structure_to_factset: dict[str, str],
) -> None:
  """Insert one FactSet row per picked Network — before facts are stamped.

  Per §3.5/§6.5 of information-block.md: ``create_report`` creates the
  ``fact_sets`` row first, then stamps facts referencing its id. The
  period envelope comes from the report's ``periods`` (min start, max
  end) rather than being derived post-hoc from filtered facts. That
  pattern keeps the dataflow forward and lets us add a real FK from
  ``facts.fact_set_id`` → ``fact_sets.id`` without orphan risk.

  Every picked Network gets a FactSet row, even one whose Network the
  CoA hasn't reached yet (e.g., the demo's CF Network with no
  cash-flow CoA mappings). The envelope read
  (``ORDER BY period_end DESC LIMIT 1``) still resolves cleanly per
  structure; consumers can detect "no facts in this period" by the
  zero fact_count on the resulting envelope.
  """
  if not periods:
    return

  starts = [p.start for p in periods if getattr(p, "start", None) is not None]
  envelope_start = min(starts) if starts else None
  envelope_end = max(p.end for p in periods)

  for structure_id, fact_set_id in structure_to_factset.items():
    session.add(
      FactSet(
        id=fact_set_id,
        structure_id=structure_id,
        period_start=envelope_start,
        period_end=envelope_end,
        factset_type="report",
        entity_id=entity_id,
        report_id=report_id,
        created_by=created_by,
      )
    )
  # Explicit flush so the new fact_sets rows hit the DB before the fact
  # INSERTs that reference them. SQLAlchemy's session-flush ordering is
  # by INSERT statement type, not by FK dependency.
  session.flush()


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
  # Verify taxonomy exists
  tax_result = session.execute(
    text("SELECT id, standard FROM taxonomies WHERE id = :tid LIMIT 1"),
    {"tid": body.taxonomy_id},
  )
  tax_row = tax_result.fetchone()
  if tax_row is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)

  periods = build_periods(
    body.period_start, body.period_end, body.comparative, body.periods
  )

  report_def = Report(
    name=body.name,
    taxonomy_id=body.taxonomy_id,
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

  facts = generate_report_facts(
    session=session,
    taxonomy_id=body.taxonomy_id,
    mapping_id=body.mapping_id,
    periods=periods,
  )

  entity_id = _get_entity_id(session, graph_id)
  reporting_style_id = load_graph_reporting_style(graph_id)
  element_to_structure, structure_to_factset = _build_structure_mapping(
    session, reporting_style_id
  )
  # §6.5: create fact_sets rows first so the facts we stamp reference
  # a row that already exists. Lets us enforce facts.fact_set_id →
  # fact_sets.id at the DB layer (post-§3.5).
  _pre_create_report_fact_sets(
    session,
    report_def.id,
    entity_id,
    created_by,
    periods,
    structure_to_factset,
  )
  _persist_report_facts(
    session,
    report_def.id,
    facts,
    entity_id,
    element_to_structure,
    structure_to_factset,
  )
  summary = _evaluate_report_structures(
    session,
    facts,
    element_to_structure,
    structure_to_factset,
    body.period_start,
    body.period_end,
    created_by,
  )

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

  Raises:
    ReportNotFoundError: report_id doesn't resolve.
    NotAuthorizedError: caller doesn't own the report.
    ValueError: if period_end < period_start in the new body.
  """
  report_def = session.get(Report, report_id)
  if report_def is None:
    raise ReportNotFoundError(report_id)
  if report_def.created_by != acting_user_id:
    raise NotAuthorizedError("Not authorized to modify this report.")

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

  facts = generate_report_facts(
    session=session,
    taxonomy_id=report_def.taxonomy_id,
    mapping_id=report_def.mapping_id or "",
    periods=periods,
  )

  entity_id = _get_entity_id(session, graph_id)
  reporting_style_id = load_graph_reporting_style(graph_id)
  element_to_structure, structure_to_factset = _build_structure_mapping(
    session, reporting_style_id
  )
  # Stale rows from the prior generation must clear before fresh ULIDs
  # land. Order matters once the facts → fact_sets FK exists (§6.5):
  # facts go first (they reference fact_sets), then fact_sets. The
  # ``_persist_report_facts`` call below also DELETEs by report_id —
  # idempotent here, but doing the explicit fact delete first keeps
  # the FK ordering correct even before SQLAlchemy autoflushes.
  session.execute(
    text("DELETE FROM facts WHERE report_id = :report_id"),
    {"report_id": report_def.id},
  )
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
  )
  _persist_report_facts(
    session,
    report_def.id,
    facts,
    entity_id,
    element_to_structure,
    structure_to_factset,
  )
  summary = _evaluate_report_structures(
    session,
    facts,
    element_to_structure,
    structure_to_factset,
    report_def.period_start,
    report_def.period_end,
    acting_user_id,
  )

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


# Legal transitions per the Plan-C lifecycle:
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


def delete_report(session: Session, report_id: str, acting_user_id: str) -> bool:
  """Delete a report and its generated facts.

  Raises `NotAuthorizedError` if the caller doesn't own the report.
  Raises `ReportNotFiledError` if the report is in a locked filing
  state (``filed`` or ``archived``) — the Report Block lifecycle treats
  filed/archived as immutable so the audit trail can't be erased.
  Returns True if a row was deleted, False if the report did not exist.
  """
  report_def = session.get(Report, report_id)
  if report_def is None:
    return False
  if report_def.created_by != acting_user_id:
    raise NotAuthorizedError("Not authorized to delete this report.")
  if report_def.filing_status in {"filed", "archived"}:
    raise ReportNotFiledError(
      f"Report '{report_id}' is '{report_def.filing_status}' and cannot "
      f"be deleted. Reach 'archived' via transition-filing-status if "
      f"retiring; deletion is only available for 'draft' or 'under_review'."
    )

  session.execute(
    text("DELETE FROM facts WHERE report_id = :report_id"),
    {"report_id": report_id},
  )
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
        SELECT id, report_id, element_id, value, period_start, period_end,
               period_type, unit, entity_id, fact_set_id, created_at
        FROM facts WHERE report_id = :report_id
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

      for fact_data in source_facts:
        # Cross-graph share: source-graph structure_id/fact_set_id
        # reference rows in the source tenant schema and are meaningless
        # in the target. Drop both on copy — shared facts identify
        # themselves via report_id alone. Populating target FactSet rows
        # per-structure on share is expand-pass work.
        rf = Fact(
          report_id=shared_report.id,
          element_id=fact_data["element_id"],
          value=fact_data["value"],
          period_start=fact_data["period_start"],
          period_end=fact_data["period_end"],
          period_type=fact_data["period_type"],
          unit=fact_data["unit"],
          entity_id=fact_data["entity_id"],
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
