"""Receiving a report into a fund's graph: the issuer it is about, and the
ingest itself.

The importer is shared (``operations/roboledger/commands/report_import``);
this is the receiver's side of it. A report arrives either as a holon or as
rows, and rows become a holon first, so every report in the graph was loaded
from a serialization that is stored and can be downloaded.
"""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from xbrlkit.author import (
  AuthoringError,
  ConceptRow,
  EntityRow,
  FactRow,
  LineRow,
  ReportRows,
  SectionRow,
  build_report,
)
from xbrlkit.namespaces import ENTITY_SCHEME
from xbrlkit.serialize import to_holon

from robosystems.models.api.extensions.investor import (
  CreateIssuerRequest,
  IngestReportRequest,
  IngestReportResponse,
  IssuerResponse,
  ReportRowsInput,
)
from robosystems.models.extensions.entity import Entity
from robosystems.operations.roboledger.commands.report_import import (
  ReportImportEntityNotFoundError,
  ReportImportRefusedError,
  import_report,
)

LEI_SCHEME = "http://standards.iso.org/iso/17442"
CIK_SCHEME = "http://www.sec.gov/CIK"
# How a report assembled from rows describes its own composition.
ROWS_REPORTING_STYLE = "authored-package"


class IssuerAlreadyExistsError(Exception):
  """An issuer with the same identifier is already in the graph."""

  def __init__(self, entity_id: str, name: str) -> None:
    self.entity_id = entity_id
    super().__init__(
      f"An issuer with that identifier already exists: {name} ({entity_id})."
    )


class IngestRequestError(ValueError):
  """The request does not say one thing to ingest."""


def create_issuer(
  session: Session, body: CreateIssuerRequest, created_by: str
) -> IssuerResponse:
  """Add a company that keeps its books elsewhere, for securities and
  received reports to attach to. Raises ``IssuerAlreadyExistsError`` when an
  entity already carries one of its identifiers."""
  identifiers = [
    column == value
    for column, value in (
      (Entity.lei, body.lei),
      (Entity.cik, body.cik),
      (Entity.tax_id, body.tax_id),
    )
    if value
  ]
  if identifiers:
    existing = (
      session.execute(select(Entity).where(or_(*identifiers))).scalars().first()
    )
    if existing is not None:
      raise IssuerAlreadyExistsError(str(existing.id), str(existing.name))

  entity = Entity(
    name=body.name,
    legal_name=body.legal_name,
    lei=body.lei,
    cik=body.cik,
    tax_id=body.tax_id,
    entity_type=body.entity_type,
    industry=body.industry,
    website=body.website,
    # Not a member of the fund's own reporting group: a counterparty it holds.
    source="linked",
    is_parent=False,
    status="active",
    metadata_={"issuer": True},
    created_by=created_by,
  )
  session.add(entity)
  session.flush()
  return IssuerResponse(
    id=str(entity.id),
    name=str(entity.name),
    legal_name=entity.legal_name,
    lei=entity.lei,
    cik=entity.cik,
    tax_id=entity.tax_id,
    entity_type=entity.entity_type,
  )


def ingest_report(
  session: Session,
  body: IngestReportRequest,
  created_by: str,
  graph_id: str,
) -> IngestReportResponse:
  """Load one report onto an entity, from a holon or from rows.

  Raises ``IngestRequestError`` unless exactly one of the two is given,
  ``ReportImportEntityNotFoundError`` for an entity outside the graph, and
  ``ReportImportRefusedError`` with the reasons when the report cannot be
  read, built or checked.
  """
  if (body.holon is None) == (body.report is None):
    raise IngestRequestError("Give exactly one of `holon` or `report`.")

  expected: int | None = None
  if body.report is not None:
    holon_text, expected = _holon_from_rows(session, body.entity_id, body.report)
  else:
    holon_text = body.holon or ""

  result = import_report(
    session,
    graph_id,
    holon_text=holon_text,
    entity_id=body.entity_id,
    created_by=created_by,
    name=body.name,
    expected_facts=expected,
    require_source=body.require_source,
    dry_run=body.dry_run,
  )
  return IngestReportResponse(
    dry_run=result.dry_run,
    report_id=result.report_id or None,
    entity_id=result.entity_id,
    period_end=result.period_end,
    kind=result.kind,
    fact_count=result.fact_count,
    section_count=result.structure_count,
    calculations_checked=result.calculations_checked,
    balance_checked=result.balance_checked,
    concepts_added=result.concepts_added,
    dimensional_facts_not_loaded=result.dimensional_facts_not_loaded,
    replaced_report_ids=result.replaced_report_ids,
  )


def _identity(entity: Entity) -> tuple[str, str]:
  """The entity's identifier and the scheme that names it."""
  if entity.lei:
    return str(entity.lei), LEI_SCHEME
  if entity.cik:
    return str(entity.cik), CIK_SCHEME
  return str(entity.id), ENTITY_SCHEME


def _holon_from_rows(
  session: Session, entity_id: str, rows: ReportRowsInput
) -> tuple[str, int]:
  """Assemble the rows into a holon; returns it with the fact count sent."""
  entity = session.get(Entity, entity_id)
  if entity is None:
    raise ReportImportEntityNotFoundError(f"Entity '{entity_id}' is not in this graph.")
  identifier, scheme = _identity(entity)
  try:
    model = build_report(
      ReportRows(
        report_id=f"{entity_id}-{rows.kind}-{rows.period_end.isoformat()}",
        entity=EntityRow(
          identifier=identifier,
          scheme=scheme,
          name=str(entity.name),
          legal_name=entity.legal_name,
        ),
        reporting_style=ROWS_REPORTING_STYLE,
        period_end=rows.period_end,
        form=rows.kind,
        attributed_to=rows.attributed_to,
        namespaces=rows.namespaces,
        concepts=[ConceptRow.model_validate(c.model_dump()) for c in rows.concepts],
        sections=[
          SectionRow(
            name=s.name,
            block_type=s.block_type,
            head=s.head,
            lines=[LineRow.model_validate(line.model_dump()) for line in s.lines],
          )
          for s in rows.sections
        ],
        facts=[FactRow.model_validate(f.model_dump()) for f in rows.facts],
      )
    )
  except AuthoringError as exc:
    raise ReportImportRefusedError(
      [{"code": "row", "message": problem} for problem in exc.problems]
    ) from exc
  except ValueError as exc:
    raise ReportImportRefusedError([{"code": "row", "message": str(exc)}]) from exc
  # Identical rows collapse to one fact, so the count is the model's own.
  return to_holon(model), len(model.facts)
