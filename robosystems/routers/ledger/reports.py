"""Report CRUD, fact generation, and structure rendering endpoints."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import select, text
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  FactRowResponse,
  RegenerateReportRequest,
  ReportListResponse,
  ReportResponse,
  ShareReportRequest,
  ShareReportResponse,
  ShareResultItem,
  StatementResponse,
  StructureSummary,
  ValidationCheckResponse,
)
from robosystems.models.core import User
from robosystems.models.extensions import ReportDefinition, ReportFact, ReportShare
from robosystems.operations.reports.fact_grid import (
  ReportFact as ReportFactData,
)
from robosystems.operations.reports.fact_grid import (
  _compute_prior_period,
  generate_report_facts,
  render_structure_view,
)
from robosystems.operations.reports.guard_rails import validate_report

router = APIRouter()

VALID_STRUCTURE_TYPES = {
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
  "custom",
}


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def _report_404(report_id: str):
  return HTTPException(
    status_code=404,
    detail=f"Report '{report_id}' not found.",
  )


def _structure_404(structure_type: str):
  return HTTPException(
    status_code=404,
    detail=f"Structure '{structure_type}' not found in this report's taxonomy.",
  )


def _load_structures(session, taxonomy_id: str) -> list[StructureSummary]:
  """Load available structures for a taxonomy."""
  result = session.execute(
    text("""
      SELECT id, name, structure_type FROM structures
      WHERE taxonomy_id = :taxonomy_id
        AND structure_type NOT IN ('chart_of_accounts', 'coa_mapping')
        AND is_active = true
      ORDER BY structure_type
    """),
    {"taxonomy_id": taxonomy_id},
  )
  return [
    StructureSummary(id=r.id, name=r.name, structure_type=r.structure_type)
    for r in result
  ]


def _report_to_response(
  report_def: ReportDefinition,
  structures: list[StructureSummary],
  entity_name: str | None = None,
) -> ReportResponse:
  return ReportResponse(
    id=report_def.id,
    name=report_def.name,
    taxonomy_id=report_def.taxonomy_id,
    generation_status=report_def.generation_status,
    period_type=report_def.period_type,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    comparative=report_def.comparative,
    mapping_id=report_def.mapping_id,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    last_generated=report_def.last_generated,
    structures=structures,
    entity_name=entity_name,
    source_graph_id=report_def.source_graph_id,
    source_report_id=report_def.source_report_id,
    shared_at=report_def.shared_at,
  )


def _resolve_entity_name(session, report_def: ReportDefinition) -> str | None:
  """Resolve the entity name for a report.

  For shared reports: look up the linked entity by source_graph_id.
  For native reports: look up the parent entity.
  """
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


def _persist_report_facts(session, report_id: str, report_facts, entity_id: str):
  """Write generated facts to the report_facts OLTP table."""
  # Clear any existing facts for this report
  session.execute(
    text("DELETE FROM report_facts WHERE report_id = :report_id"),
    {"report_id": report_id},
  )

  for fact in report_facts.facts:
    rf = ReportFact(
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


def _get_entity_id(session, graph_id: str) -> str:
  """Get the primary entity ID for a graph.

  Returns the earliest-created entity, which is the primary entity
  for single-entity graphs (the common case).
  """
  result = session.execute(
    text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
  )
  row = result.fetchone()
  if not row:
    raise HTTPException(
      status_code=422,
      detail="No entity found. Import data before creating reports.",
    )
  return row.id


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.post(
  "/reports",
  response_model=ReportResponse,
  status_code=201,
  operation_id="createReport",
  summary="Create Report",
  tags=["Ledger"],
)
async def create_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: CreateReportRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Create a report definition, generate facts for all mapped elements."""
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  try:
    with extensions_session(graph_id) as session:
      # Verify taxonomy exists
      tax_result = session.execute(
        text("SELECT id FROM taxonomies WHERE id = :tid LIMIT 1"),
        {"tid": body.taxonomy_id},
      )
      if not tax_result.fetchone():
        raise HTTPException(
          status_code=422,
          detail=f"Taxonomy '{body.taxonomy_id}' not found.",
        )

      # Create the report definition
      report_def = ReportDefinition(
        name=body.name,
        taxonomy_id=body.taxonomy_id,
        mapping_id=body.mapping_id,
        period_type=body.period_type,
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=body.comparative,
        generation_status="generating",
        created_by=current_user.id,
      )
      session.add(report_def)
      session.flush()

      # Generate facts for all mapped elements (structure-agnostic)
      report_facts = generate_report_facts(
        session=session,
        taxonomy_id=body.taxonomy_id,
        mapping_id=body.mapping_id,
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=body.comparative,
      )

      # Persist facts to OLTP for materialization and rendering
      entity_id = _get_entity_id(session, graph_id)
      _persist_report_facts(session, report_def.id, report_facts, entity_id)

      # Update status
      report_def.generation_status = "published"
      report_def.last_generated = datetime.now(UTC)
      session.commit()

      structures = _load_structures(session, body.taxonomy_id)
      entity_name = _resolve_entity_name(session, report_def)
      return _report_to_response(report_def, structures, entity_name)

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report creation failed: {e}")
    raise


@router.get(
  "/reports",
  response_model=ReportListResponse,
  operation_id="listReports",
  summary="List Reports",
  tags=["Ledger"],
)
async def list_reports(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List all report definitions for this graph."""
  try:
    with extensions_session(graph_id) as session:
      rows = (
        session.execute(
          select(ReportDefinition).order_by(ReportDefinition.created_at.desc())
        )
        .scalars()
        .all()
      )

      reports = []
      for r in rows:
        structures = _load_structures(session, r.taxonomy_id)
        entity_name = _resolve_entity_name(session, r)
        reports.append(_report_to_response(r, structures, entity_name))

      return ReportListResponse(reports=reports)
  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report listing failed: {e}")
    raise


@router.get(
  "/reports/{report_id}",
  response_model=ReportResponse,
  operation_id="getReport",
  summary="Get Report",
  tags=["Ledger"],
)
async def get_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get a report definition with its available structures."""
  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      structures = _load_structures(session, report_def.taxonomy_id)
      entity_name = _resolve_entity_name(session, report_def)
      return _report_to_response(report_def, structures, entity_name)

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report retrieval failed: {e}")
    raise


@router.get(
  "/reports/{report_id}/statements/{structure_type}",
  response_model=StatementResponse,
  operation_id="getStatement",
  summary="Get Statement",
  tags=["Ledger"],
)
async def get_statement(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  structure_type: str = Path(
    ...,
    description="Structure type: income_statement, balance_sheet, cash_flow_statement",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Render a financial statement — facts viewed through a structure's hierarchy.

  Same report, different structure_type = different view (IS, BS, CF).
  """
  if structure_type not in VALID_STRUCTURE_TYPES:
    raise HTTPException(
      status_code=422,
      detail=f"Invalid structure_type '{structure_type}'. Must be one of: {', '.join(sorted(VALID_STRUCTURE_TYPES))}",
    )

  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      if not report_def.period_start or not report_def.period_end:
        return StatementResponse(
          report_id=report_def.id,
          structure_id="",
          structure_name="",
          structure_type=structure_type,
          period_start=report_def.period_start or report_def.period_end,
          period_end=report_def.period_end or report_def.period_start,
        )

      # Load persisted facts from report_facts
      fact_rows = session.execute(
        text("""
          SELECT rf.element_id, rf.value, rf.period_start, rf.period_end,
                 rf.period_type, e.qname, e.name, e.classification, e.balance_type
          FROM report_facts rf
          JOIN elements e ON e.id = rf.element_id
          WHERE rf.report_id = :report_id
        """),
        {"report_id": report_id},
      )

      facts = [
        ReportFactData(
          element_id=r.element_id,
          element_qname=r.qname,
          element_name=r.name,
          classification=r.classification,
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
          structure_type=structure_type,
          period_start=report_def.period_start,
          period_end=report_def.period_end,
        )

      # Compute prior period dates
      comp_start = None
      comp_end = None
      if report_def.comparative:
        comp_start, comp_end = _compute_prior_period(
          report_def.period_start, report_def.period_end
        )

      # Render the structure view
      grid = render_structure_view(
        session=session,
        facts=facts,
        structure_type=structure_type,
        period_start=report_def.period_start,
        period_end=report_def.period_end,
        comparative_period_start=comp_start,
        comparative_period_end=comp_end,
      )

      if not grid.structure_id:
        raise _structure_404(structure_type)

      # Validate
      validation = validate_report(structure_type, grid.rows)

      rows = [
        FactRowResponse(
          element_id=r.element_id,
          element_qname=r.element_qname,
          element_name=r.element_name,
          classification=r.classification,
          current_value=r.current_value,
          prior_value=r.prior_value,
          is_subtotal=r.is_subtotal,
          depth=r.depth,
        )
        for r in grid.rows
      ]

      validation_resp = (
        ValidationCheckResponse(
          passed=validation.passed,
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
        structure_type=structure_type,
        period_start=grid.period_start,
        period_end=grid.period_end,
        comparative_period_start=grid.comparative_period_start,
        comparative_period_end=grid.comparative_period_end,
        rows=rows,
        validation=validation_resp,
        unmapped_count=grid.unmapped_count,
      )

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Statement rendering failed: {e}")
    raise


@router.post(
  "/reports/{report_id}/regenerate",
  response_model=ReportResponse,
  operation_id="regenerateReport",
  summary="Regenerate Report",
  tags=["Ledger"],
)
async def regenerate_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  body: RegenerateReportRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Regenerate a report with new period dates.

  Same report configuration (taxonomy, mapping, settings), new dates.
  Re-generates facts for all elements.
  """
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)
      if report_def.created_by != current_user.id:
        raise HTTPException(
          status_code=403, detail="Not authorized to modify this report."
        )

      # Update period
      report_def.period_start = body.period_start
      report_def.period_end = body.period_end
      report_def.generation_status = "generating"
      session.flush()

      # Re-generate facts
      report_facts = generate_report_facts(
        session=session,
        taxonomy_id=report_def.taxonomy_id,
        mapping_id=report_def.mapping_id or "",
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=report_def.comparative,
      )

      # Persist
      entity_id = _get_entity_id(session, graph_id)
      _persist_report_facts(session, report_def.id, report_facts, entity_id)

      report_def.generation_status = "published"
      report_def.last_generated = datetime.now(UTC)
      session.commit()

      structures = _load_structures(session, report_def.taxonomy_id)
      entity_name = _resolve_entity_name(session, report_def)
      return _report_to_response(report_def, structures, entity_name)

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report regeneration failed: {e}")
    raise


@router.delete(
  "/reports/{report_id}",
  status_code=204,
  operation_id="deleteReport",
  summary="Delete Report",
  tags=["Ledger"],
)
async def delete_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Delete a report definition and its generated facts."""
  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)
      if report_def.created_by != current_user.id:
        raise HTTPException(
          status_code=403, detail="Not authorized to delete this report."
        )

      # Delete generated facts first
      session.execute(
        text("DELETE FROM report_facts WHERE report_id = :report_id"),
        {"report_id": report_id},
      )

      session.delete(report_def)
      session.commit()

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report deletion failed: {e}")
    raise


@router.post(
  "/reports/{report_id}/share",
  response_model=ShareReportResponse,
  operation_id="shareReport",
  summary="Share Report",
  tags=["Ledger"],
)
async def share_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  body: ShareReportRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Share a published report to other graphs.

  Copies the report definition and its generated facts to each target graph's
  tenant schema. The target graph must have 'roboledger' in its schema_extensions.
  This is a snapshot — the target gets an immutable copy of the facts at this
  point in time.
  """
  results: list[ShareResultItem] = []

  try:
    with extensions_session(graph_id) as source_session:
      # Resolve publish list → target graph IDs
      from robosystems.models.extensions import PublishList, PublishListMember

      publish_list = source_session.execute(
        select(PublishList).where(PublishList.id == body.publish_list_id)
      ).scalar_one_or_none()
      if not publish_list:
        raise HTTPException(status_code=404, detail="Publish list not found.")

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
        raise HTTPException(status_code=422, detail="Publish list has no members.")

      target_graph_ids = [m.target_graph_id for m in members]

      # Verify report exists and is published
      report_def = source_session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      if report_def.created_by != current_user.id:
        raise HTTPException(
          status_code=403, detail="Not authorized to share this report."
        )

      if report_def.generation_status != "published":
        raise HTTPException(
          status_code=422,
          detail="Only published reports can be shared.",
        )

      # Snapshot report fields before session closes (avoid detached instance)
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
      }

      # Read the source facts with explicit columns
      fact_rows = source_session.execute(
        text("""
          SELECT id, report_id, element_id, value, period_start, period_end,
                 period_type, unit, entity_id, fact_set_id, created_at
          FROM report_facts WHERE report_id = :report_id
        """),
        {"report_id": report_id},
      ).fetchall()

      source_facts = [row._asdict() for row in fact_rows]

    # Now share to each target graph
    for target_graph_id in target_graph_ids:
      result = _share_to_target(
        source_graph_id=graph_id,
        report_snapshot=report_snapshot,
        source_facts=source_facts,
        target_graph_id=target_graph_id,
        shared_by=current_user.id,
      )
      results.append(result)

      # Record the share in the source graph
      if result.status == "shared":
        with extensions_session(graph_id) as source_session:
          share_record = ReportShare(
            report_id=report_id,
            target_graph_id=target_graph_id,
            shared_by=current_user.id,
            fact_count=result.fact_count,
          )
          source_session.add(share_record)
          source_session.commit()

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Report sharing failed: {e}")
    raise

  return ShareReportResponse(report_id=report_id, results=results)


def _share_to_target(
  source_graph_id: str,
  report_snapshot: dict,
  source_facts: list[dict],
  target_graph_id: str,
  shared_by: str,
) -> ShareResultItem:
  """Copy report definition + facts to a target graph's tenant schema."""
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph

  # Validate the target graph exists and has roboledger schema
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

  # Copy to the target schema
  try:
    now = datetime.now(UTC)
    with extensions_session(target_graph_id) as target_session:
      # Create the report definition in the target schema with provenance
      shared_report = ReportDefinition(
        name=report_snapshot["name"],
        description=report_snapshot.get("description"),
        taxonomy_id=report_snapshot["taxonomy_id"],
        mapping_id=report_snapshot.get("mapping_id"),
        period_type=report_snapshot["period_type"],
        period_start=report_snapshot.get("period_start"),
        period_end=report_snapshot.get("period_end"),
        comparative=report_snapshot["comparative"],
        generation_status="published",
        created_by=shared_by,
        source_graph_id=source_graph_id,
        source_report_id=report_snapshot["id"],
        shared_at=now,
      )
      target_session.add(shared_report)
      target_session.flush()

      # Copy facts with the new report ID
      for fact_data in source_facts:
        rf = ReportFact(
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

      # Ensure a linked Entity exists on the target graph for the source company.
      # This is the bridge between the company's graph and the investor's portfolio.
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
  target_session,
  source_graph_id: str,
  shared_by: str,
) -> None:
  """Create or update a linked Entity in the target graph for the source company.

  When a company shares a report to an investor's graph, the investor needs an
  Entity row representing that company. This function:
  1. Reads the source graph's parent entity for current metadata
  2. Creates a linked entity if none exists, or updates metadata if one does
  3. Auto-links any securities with matching source_graph_id

  Entity metadata is refreshed on each share so name/industry/etc. changes
  propagate with each published report.
  """
  from robosystems.models.extensions.entity import Entity

  # Read the source graph's primary entity for current metadata
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

  # Check if a linked entity for this source graph already exists
  existing = target_session.execute(
    text(
      "SELECT id FROM entities WHERE metadata->>'source_graph_id' = :source_graph_id LIMIT 1"
    ),
    {"source_graph_id": source_graph_id},
  ).scalar_one_or_none()

  if existing:
    # Update metadata on existing linked entity (living updates)
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
    # Create new linked entity
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

  # Auto-link securities that reference this source graph
  target_session.execute(
    text("""
      UPDATE securities SET entity_id = :entity_id, updated_at = now()
      WHERE source_graph_id = :source_graph_id AND entity_id IS NULL
    """),
    {"entity_id": entity_id, "source_graph_id": source_graph_id},
  )
