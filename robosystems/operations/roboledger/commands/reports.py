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
  PublishList,
  PublishListMember,
  Report,
  ReportShare,
)
from robosystems.operations.roboledger.reads.reports import (
  build_periods,
  load_structures,
  periods_to_json,
  report_to_response,
  resolve_entity_name,
)
from robosystems.operations.roboledger.reports.fact_grid import generate_report_facts


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


def _persist_report_facts(
  session: Session, report_id: str, facts, entity_id: str
) -> None:
  """Clear any existing facts for this report and persist the new set."""
  session.execute(
    text("DELETE FROM facts WHERE report_id = :report_id"),
    {"report_id": report_id},
  )
  for fact in facts.facts:
    rf = Fact(
      report_id=report_id,
      element_id=fact.element_id,
      value=fact.value,
      period_start=fact.period_start,
      period_end=fact.period_end,
      period_type=fact.period_type,
      unit="USD",
      entity_id=entity_id,
    )
    session.add(rf)


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
    text("SELECT id FROM taxonomies WHERE id = :tid LIMIT 1"),
    {"tid": body.taxonomy_id},
  )
  if tax_result.fetchone() is None:
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
  _persist_report_facts(session, report_def.id, facts, entity_id)

  report_def.generation_status = "published"
  report_def.last_generated = datetime.now(UTC)
  session.commit()

  structures = load_structures(session, body.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def regenerate_report(
  session: Session,
  report_id: str,
  body: RegenerateReportRequest,
  acting_user_id: str,
) -> ReportResponse:
  """Regenerate a report with new period dates.

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

  entity_id = _get_entity_id(session, report_def.graph_id or "")
  _persist_report_facts(session, report_def.id, facts, entity_id)

  report_def.generation_status = "published"
  report_def.last_generated = datetime.now(UTC)
  session.commit()

  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def delete_report(session: Session, report_id: str, acting_user_id: str) -> bool:
  """Delete a report and its generated facts.

  Raises `NotAuthorizedError` if the caller doesn't own the report.
  Returns True if a row was deleted, False if the report did not exist.
  """
  report_def = session.get(Report, report_id)
  if report_def is None:
    return False
  if report_def.created_by != acting_user_id:
    raise NotAuthorizedError("Not authorized to delete this report.")

  session.execute(
    text("DELETE FROM facts WHERE report_id = :report_id"),
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
    return ShareResultItem(
      target_graph_id=target_graph_id,
      status="error",
      error=f"Failed to validate target graph: {e!s}",
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
        rf = Fact(
          report_id=shared_report.id,
          element_id=fact_data["element_id"],
          value=fact_data["value"],
          period_start=fact_data["period_start"],
          period_end=fact_data["period_end"],
          period_type=fact_data["period_type"],
          unit=fact_data["unit"],
          entity_id=fact_data["entity_id"],
          fact_set_id=fact_data.get("fact_set_id"),
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
