"""Receiving a report into a fund's graph: the issuer it is about, and the
ingest itself.

The importer is shared (``operations/roboledger/commands/report_import``);
this is the receiver's side of it. A report arrives as a holon, as a Tavi
compiled model or as rows, and the last two become a holon first, so every
report in the graph was loaded from a serialization that is stored and can be
downloaded.
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
from xbrlkit.deserialize import TaviError, from_tavi_report
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
from robosystems.operations.library.reads.statement_layout import (
  LAID_OUT_STATEMENTS,
  lay_out_statement,
  library_concepts,
)
from robosystems.operations.roboledger.commands.report_import import (
  ReportImportEntityNotFoundError,
  ReportImportRefusedError,
  import_report,
)

LEI_SCHEME = "http://standards.iso.org/iso/17442"
CIK_SCHEME = "http://www.sec.gov/CIK"
# How a report assembled from rows describes its own composition.
ROWS_REPORTING_STYLE = "authored-package"
# The heading a statement laid out by the server hangs from.
SECTION_HEAD_PREFIX = "rs-report"
SECTION_HEAD_NAMESPACE = "https://robosystems.ai/report-sections#"
_SECTION_HEADS = {
  "balance_sheet": ("BalanceSheet", "Balance Sheet"),
  "income_statement": ("IncomeStatement", "Income Statement"),
}


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
  """Load one report onto an entity, from a holon, a Tavi model or rows.

  Raises ``IngestRequestError`` unless exactly one of the three is given,
  ``ReportImportEntityNotFoundError`` for an entity outside the graph, and
  ``ReportImportRefusedError`` with the reasons when the report cannot be
  read, built or checked.
  """
  given = [v for v in (body.holon, body.report, body.tavi) if v is not None]
  if len(given) != 1:
    raise IngestRequestError("Give exactly one of `holon`, `tavi` or `report`.")

  expected: int | None = None
  unplaced: list[str] = []
  if body.report is not None:
    holon_text, expected, unplaced = _holon_from_rows(
      session, body.entity_id, body.report
    )
  elif body.tavi is not None:
    holon_text, expected = _holon_from_tavi(body.tavi)
  else:
    holon_text = body.holon or ""

  try:
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
  except ReportImportRefusedError as exc:
    if not unplaced:
      raise
    # A value on no statement is the usual reason a total above it is short.
    raise ReportImportRefusedError(
      [
        *exc.problems,
        {
          "code": "unplaced",
          "message": (
            "No section places these concepts, so no total counts them: "
            f"{', '.join(unplaced)}."
          ),
        },
      ]
    ) from exc
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
    unplaced_concepts=unplaced,
  )


def _holon_from_tavi(tavi_text: str) -> tuple[str, int]:
  """Read a Tavi compiled model and write it as the holon that is stored;
  returns it with the fact count read. A document that would lose a value on
  the way is refused."""
  try:
    model, gaps = from_tavi_report(tavi_text)
  except TaviError as exc:
    raise ReportImportRefusedError(
      [{"code": "unreadable", "message": f"Not a Tavi compiled model: {exc}"}]
    ) from exc
  if gaps.skipped_facts:
    raise ReportImportRefusedError(
      [
        {
          "code": "unreadable",
          "message": (
            f"{gaps.skipped_facts} value(s) in the Tavi model name no concept "
            "or no readable period, so the report would not load whole."
          ),
        }
      ]
    )
  return to_holon(model), len(model.facts)


def _identity(entity: Entity) -> tuple[str, str]:
  """The entity's identifier and the scheme that names it."""
  if entity.lei:
    return str(entity.lei), LEI_SCHEME
  if entity.cik:
    return str(entity.cik), CIK_SCHEME
  return str(entity.id), ENTITY_SCHEME


def _sections(
  session: Session, rows: ReportRowsInput
) -> tuple[list[SectionRow], dict[str, str]]:
  """The report's sections, with the lines of any primary statement sent
  without them laid out from the library; and the headings that needed."""
  reported = {fact.concept for fact in rows.facts}
  sections: list[SectionRow] = []
  heads: dict[str, str] = {}
  problems: list[dict] = []
  for section in rows.sections:
    lines = [LineRow.model_validate(line.model_dump()) for line in section.lines]
    head = section.head
    if not lines:
      if section.block_type not in LAID_OUT_STATEMENTS:
        problems.append(
          {
            "code": "row",
            "message": (
              f"section {section.name!r}: no lines. Only a balance_sheet or an "
              "income_statement is laid out from the library; give this one "
              "its lines."
            ),
          }
        )
        continue
      lines = [
        LineRow(concept=line.concept, parent=line.parent, weight=line.weight)
        for line in lay_out_statement(session, section.block_type, reported)
      ]
      if not lines:
        problems.append(
          {
            "code": "row",
            "message": (
              f"section {section.name!r}: none of the values sent is for a "
              f"concept the library places on a {section.block_type}."
            ),
          }
        )
        continue
      if head is None:
        local, label = _SECTION_HEADS[section.block_type]
        head = f"{SECTION_HEAD_PREFIX}:{local}"
        heads[head] = label
    sections.append(
      SectionRow(
        name=section.name, block_type=section.block_type, head=head, lines=lines
      )
    )
  if problems:
    raise ReportImportRefusedError(problems)
  return sections, heads


def _concepts(
  session: Session,
  rows: ReportRowsInput,
  sections: list[SectionRow],
  heads: dict[str, str],
) -> list[ConceptRow]:
  """The declared concepts, then the library's own description of any library
  concept the report uses without declaring it."""
  concepts = [ConceptRow.model_validate(c.model_dump()) for c in rows.concepts]
  declared = {c.qname for c in concepts}
  for qname, label in heads.items():
    if qname not in declared:
      concepts.append(ConceptRow(qname=qname, label=label, abstract=True))
      declared.add(qname)
  used = {fact.concept for fact in rows.facts}
  for section in sections:
    used.update(line.concept for line in section.lines)
    if section.head:
      used.add(section.head)
  for qname, concept in sorted(library_concepts(session, used - declared).items()):
    concepts.append(
      ConceptRow(
        qname=qname,
        label=concept.label,
        kind="monetary" if concept.monetary else "string",
        period_type=concept.period_type,
        balance=concept.balance if concept.monetary else None,
        abstract=concept.abstract,
      )
    )
  return concepts


def _holon_from_rows(
  session: Session, entity_id: str, rows: ReportRowsInput
) -> tuple[str, int, list[str]]:
  """Assemble the rows into a holon; returns it with the fact count sent and
  the reported concepts no section places."""
  entity = session.get(Entity, entity_id)
  if entity is None:
    raise ReportImportEntityNotFoundError(f"Entity '{entity_id}' is not in this graph.")
  identifier, scheme = _identity(entity)
  sections, heads = _sections(session, rows)
  namespaces = dict(rows.namespaces)
  if heads:
    namespaces.setdefault(SECTION_HEAD_PREFIX, SECTION_HEAD_NAMESPACE)
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
        namespaces=namespaces,
        concepts=_concepts(session, rows, sections, heads),
        sections=sections,
        facts=[FactRow.model_validate(f.model_dump()) for f in rows.facts],
      )
    )
  except AuthoringError as exc:
    raise ReportImportRefusedError(
      [{"code": "row", "message": problem} for problem in exc.problems]
    ) from exc
  except ValueError as exc:
    raise ReportImportRefusedError([{"code": "row", "message": str(exc)}]) from exc
  placed = {line.concept for section in sections for line in section.lines}
  unplaced = sorted({fact.concept for fact in rows.facts} - placed)
  # Identical rows collapse to one fact, so the count is the model's own.
  return to_holon(model), len(model.facts), unplaced
