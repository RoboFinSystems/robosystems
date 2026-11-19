"""Import a report from its serialization: a holon in, rows out.

The file is the record of what was received and the rows are downstream of
it: it is read, checked, stored whole under the report's bundle key, and only
then projected into the tenant. Concepts and structures go through the
reporting-extension command rather than being written here, so a received
report's vocabulary is an ordinary extension of the library; this module
writes the report, its fact sets and its facts.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session
from xbrlkit.check import check_report
from xbrlkit.deserialize import from_holon_report
from xbrlkit.deserialize.holon import HolonError
from xbrlkit.information_block import InformationBlock, fact_membership, plan_blocks
from xbrlkit.model import Concept, Network, XbrlFact, XbrlModel

from robosystems.config import env
from robosystems.config.storage.graph import (
  REPORT_ANCHOR_EXTENSION,
  get_report_bundle_key,
  get_report_bundle_uri,
)
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockAssociationRequest,
  TaxonomyBlockElementRequest,
  TaxonomyBlockStructureRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element, Fact, Report, Structure, Taxonomy
from robosystems.models.extensions.entity import Entity
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.taxonomy_block import reporting_extension
from robosystems.taxonomy.writer import pull_library_elements

# The door a fact came through, on its provenance.
UPLOAD_SOURCE_SYSTEM = "report_upload"
# The library a received report's own concepts extend.
BOOK_STANDARD = "rs-gaap"
RECEIVED_TAXONOMY_NAME = "Received Reports"
RECEIVED_TAXONOMY_KEY = "received_reports"
MAX_HOLON_BYTES = 25 * 1024 * 1024
HOLON_MEDIA_TYPE = "application/ld+json"

_STATEMENT_BLOCK_TYPES = frozenset(
  {"balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"}
)
_STRUCTURE_BLOCK_TYPES = _STATEMENT_BLOCK_TYPES | frozenset(
  {"schedule", "rollforward", "reconciliation", "policy", "metric"}
)


class ReportImportRefusedError(Exception):
  """The file was not loaded. ``problems`` says why, one entry per reason."""

  def __init__(self, problems: list[dict[str, Any]]) -> None:
    self.problems = problems
    first = problems[0]["message"] if problems else "no reason given"
    more = f" (and {len(problems) - 1} more)" if len(problems) > 1 else ""
    super().__init__(f"The report was not imported: {first}{more}")


class ReportImportEntityNotFoundError(Exception):
  """The entity the report is to hang on is not in this graph."""


@dataclass
class ImportedReport:
  report_id: str
  entity_id: str
  period_end: date
  kind: str
  fact_count: int
  structure_count: int
  concepts_added: list[str] = field(default_factory=list)
  # Facts qualified by a dimension stay in the stored file; the tenant rows
  # hold the report's totals.
  dimensional_facts_not_loaded: int = 0
  # Reports this one replaced, whose stored artifacts the caller deletes
  # after commit.
  replaced_report_ids: list[str] = field(default_factory=list)


def import_report(
  session: Session,
  graph_id: str,
  *,
  holon_text: str,
  entity_id: str,
  created_by: str,
  name: str | None = None,
  expected_facts: int | None = None,
  require_source: bool = False,
) -> ImportedReport:
  """Load one holon onto ``entity_id`` as an ``uploaded`` report.

  A second import for the same entity, period end and kind replaces the
  first. Raises ``ReportImportRefusedError`` with the reasons when the file
  cannot be read or does not check, and writes nothing. Does not commit.
  """
  model = _read(holon_text)
  verdict = check_report(
    model, expected_facts=expected_facts, require_source=require_source
  )
  if not verdict.ok:
    raise ReportImportRefusedError([p.to_dict() for p in verdict.problems])

  entity = session.get(Entity, entity_id)
  if entity is None:
    raise ReportImportEntityNotFoundError(f"Entity '{entity_id}' is not in this graph.")

  loadable = [f for f in model.facts if not f.dims]
  if not loadable:
    raise ReportImportRefusedError(
      [
        {
          "code": "no_totals",
          "message": "Every fact in the report is qualified by a dimension.",
        }
      ]
    )
  period_end = model.filing.report_date or _latest_period_end(model, loadable)
  if period_end is None:
    raise ReportImportRefusedError(
      [{"code": "no_period", "message": "The report states no dated period."}]
    )
  kind = model.filing.form or "report"

  replaced = _delete_previous(session, entity_id, period_end, kind)
  sections = _sections(model, loadable)
  extension, structure_ids, concepts_added = _write_vocabulary(
    session,
    model,
    sections,
    loadable,
    report_label=f"{entity.name} {period_end.isoformat()} {kind}",
    remove_structure_ids=replaced.structure_ids,
    created_by=created_by,
  )
  element_ids = _element_ids(session, {f.concept_qname for f in loadable})

  digest = hashlib.sha256(holon_text.encode("utf-8")).hexdigest()
  starts = [p.start for p in model.periods if p.start is not None]
  report = Report(
    name=name or f"{entity.name}: {kind} {period_end.isoformat()}",
    entity_id=entity_id,
    taxonomy_id=extension.id,
    period_type=_period_type(model, period_end),
    period_start=min(starts) if starts else None,
    period_end=period_end,
    comparative=len({p.id for p in model.periods if p.end is not None}) > 2,
    generation_status="published",
    generation_count=1,
    filing_status="draft",
    origin="uploaded",
    created_by=created_by,
    metadata_={
      "import": {
        "kind": kind,
        "report_id": model.filing.report_id,
        "reporting_style": model.filing.reporting_style,
        "sha256": digest,
        "structure_ids": sorted(structure_ids.values()),
        "entity_identifier": model.entity.identifier,
        "entity_scheme": model.entity.scheme,
      }
    },
  )
  session.add(report)
  session.flush()

  basis = f"report={model.filing.report_id} sha256={digest}"
  set_provenance = AssertedProvenance(
    source_system=UPLOAD_SOURCE_SYSTEM, asserted_by=created_by, basis_note=basis
  )
  periods = {p.id: p for p in model.periods}
  units = {u.id: u for u in model.units}
  written = 0
  for role, facts in sections.membership.items():
    dated = [periods[f.period_id] for f in facts if periods[f.period_id].end]
    if not dated:
      continue
    set_starts = [p.start for p in dated if p.start is not None]
    fact_set = create_fact_set(
      session,
      structure_id=structure_ids.get(role),
      period_start=min(set_starts) if set_starts else None,
      period_end=max(p.end for p in dated if p.end is not None),
      factset_type="report",
      entity_id=entity_id,
      report_id=str(report.id),
      created_by=created_by,
      provenance=set_provenance,
    )
    session.flush()
    for fact in facts:
      period = periods[fact.period_id]
      if period.end is None:
        continue
      session.add(
        _fact_row(
          fact,
          model.concepts[fact.concept_qname],
          element_id=element_ids[fact.concept_qname],
          period_start=period.start,
          period_end=period.end,
          unit=units[fact.unit_id].measure if fact.unit_id else None,
          entity_id=entity_id,
          fact_set_id=str(fact_set.id),
          created_by=created_by,
          basis=basis,
        )
      )
      written += 1
  session.flush()

  _store(graph_id, report, holon_text)
  return ImportedReport(
    report_id=str(report.id),
    entity_id=entity_id,
    period_end=period_end,
    kind=kind,
    fact_count=written,
    structure_count=len(structure_ids),
    concepts_added=concepts_added,
    dimensional_facts_not_loaded=len(model.facts) - len(loadable),
    replaced_report_ids=replaced.report_ids,
  )


def _read(holon_text: str) -> XbrlModel:
  if len(holon_text.encode("utf-8")) > MAX_HOLON_BYTES:
    raise ReportImportRefusedError(
      [
        {
          "code": "too_large",
          "message": f"The file is larger than {MAX_HOLON_BYTES // (1024 * 1024)} MB.",
        }
      ]
    )
  try:
    model, gaps = from_holon_report(holon_text)
  except HolonError as exc:
    raise ReportImportRefusedError(
      [{"code": "unreadable", "message": f"The file is not a readable holon: {exc}"}]
    ) from exc
  if gaps.unresolved_references:
    raise ReportImportRefusedError(
      [
        {
          "code": "facts_dropped",
          "message": (
            f"{gaps.unresolved_references} fact(s) in the file cite an element "
            "or a period it does not define."
          ),
        }
      ]
    )
  return model


def _latest_period_end(model: XbrlModel, facts: list[XbrlFact]) -> date | None:
  cited = {f.period_id for f in facts}
  ends = [p.end for p in model.periods if p.id in cited and p.end is not None]
  return max(ends) if ends else None


def _period_type(model: XbrlModel, period_end: date) -> str:
  spans = [
    (period_end - p.start).days + 1
    for p in model.periods
    if p.start is not None and p.end == period_end
  ]
  if not spans:
    return "custom"
  shortest = min(spans)
  if shortest <= 31:
    return "monthly"
  if shortest <= 93:
    return "quarterly"
  if 350 <= shortest <= 380:
    return "annual"
  return "custom"


@dataclass
class _Replaced:
  report_ids: list[str]
  structure_ids: list[str]


def _delete_previous(
  session: Session, entity_id: str, period_end: date, kind: str
) -> _Replaced:
  """Remove the earlier import this one restates: the same entity, period end
  and kind. Its structures are returned for the vocabulary to drop."""
  from robosystems.operations.roboledger.commands.reports import (
    delete_report_fact_sets,
  )

  rows = (
    session.execute(
      select(Report).where(
        Report.origin == "uploaded",
        Report.entity_id == entity_id,
        Report.period_end == period_end,
      )
    )
    .scalars()
    .all()
  )
  previous = [
    r for r in rows if ((r.metadata_ or {}).get("import") or {}).get("kind") == kind
  ]
  if not previous:
    return _Replaced([], [])
  ids = [str(r.id) for r in previous]
  structure_ids = sorted(
    {
      sid
      for r in previous
      for sid in ((r.metadata_ or {}).get("import") or {}).get("structure_ids", [])
    }
  )
  delete_report_fact_sets(session, ids)
  for report in previous:
    session.delete(report)
  session.flush()
  return _Replaced(ids, structure_ids)


def remove_report_sections(session: Session, report: Report, removed_by: str) -> None:
  """Drop the sections an uploaded report brought, when the report goes. Its
  fact sets must already be gone; the concepts stay, as other reports of the
  same vocabulary cite them."""
  wanted = ((report.metadata_ or {}).get("import") or {}).get("structure_ids", [])
  if not wanted:
    return
  live = session.execute(
    select(Structure.id, Structure.taxonomy_id).where(Structure.id.in_(wanted))
  ).all()
  by_taxonomy: dict[str, list[str]] = {}
  for structure_id, taxonomy_id in live:
    by_taxonomy.setdefault(str(taxonomy_id), []).append(str(structure_id))
  for taxonomy_id, structure_ids in by_taxonomy.items():
    reporting_extension.update(
      session,
      UpdateTaxonomyBlockRequest(
        taxonomy_id=taxonomy_id, structures_to_remove=sorted(structure_ids)
      ),
      removed_by,
    )


@dataclass
class _Sections:
  blocks: list[InformationBlock]
  # Role URI to its facts; the empty role holds facts no section presents.
  membership: dict[str, list[XbrlFact]]


def _sections(model: XbrlModel, loadable: list[XbrlFact]) -> _Sections:
  """Each fact in one section: the first, in report order, that presents it."""
  blocks = [b for b in plan_blocks(model) if b.renderable]
  by_role = fact_membership(model, blocks)
  membership: dict[str, list[XbrlFact]] = {}
  placed: set[str] = set()
  wanted = {f.id for f in loadable}
  for block in blocks:
    for fact in by_role.get(block.role_uri, []):
      if fact.id in wanted and fact.id not in placed:
        placed.add(fact.id)
        membership.setdefault(block.role_uri, []).append(fact)
  loose = [f for f in loadable if f.id not in placed]
  if loose:
    membership[""] = loose
  return _Sections([b for b in blocks if b.role_uri in membership], membership)


def _received_taxonomy(session: Session) -> Taxonomy | None:
  for taxonomy in session.execute(
    select(Taxonomy).where(
      Taxonomy.taxonomy_type == "reporting_extension", Taxonomy.is_active.is_(True)
    )
  ).scalars():
    if (taxonomy.metadata_ or {}).get(RECEIVED_TAXONOMY_KEY):
      return taxonomy
  return None


def _library_taxonomy(session: Session) -> Taxonomy:
  library = session.execute(
    select(Taxonomy).where(
      Taxonomy.taxonomy_type == "reporting_standard",
      Taxonomy.standard == BOOK_STANDARD,
    )
  ).scalar_one_or_none()
  if library is None:
    raise ReportImportRefusedError(
      [
        {
          "code": "no_library",
          "message": f"This graph has no {BOOK_STANDARD} library to extend.",
        }
      ]
    )
  return library


def _arcs(block: InformationBlock) -> list[tuple[str, Network]]:
  return [("presentation", n) for n in block.presentation] + [
    ("calculation", n) for n in block.calculation
  ]


def _write_vocabulary(
  session: Session,
  model: XbrlModel,
  sections: _Sections,
  loadable: list[XbrlFact],
  *,
  report_label: str,
  remove_structure_ids: list[str],
  created_by: str,
) -> tuple[Taxonomy, dict[str, str], list[str]]:
  """Put the report's concepts and sections into the graph's received-reports
  extension. Returns the extension, role URI to structure id, and the qnames
  of the concepts it had to add."""
  library = _library_taxonomy(session)
  cited = {f.concept_qname for f in loadable}
  for block in sections.blocks:
    for _, network in _arcs(block):
      for arc in network.arcs:
        cited.update((arc.from_qname, arc.to_qname))

  known = _element_ids(session, cited)
  schema = session.execute(text("SELECT current_schema()")).scalar()
  known.update(
    pull_library_elements(
      session.connection(), str(schema), str(library.id), cited - set(known)
    )
  )
  new_qnames = sorted(cited - set(known))
  elements = [_element_request(q, model.concepts.get(q)) for q in new_qnames]

  structures: list[TaxonomyBlockStructureRequest] = []
  associations: list[TaxonomyBlockAssociationRequest] = []
  names: dict[str, str] = {}
  for block in sections.blocks:
    structure_name = f"{report_label}: {block.name}"
    names[block.role_uri] = structure_name
    block_type = (
      block.block_type if block.block_type in _STRUCTURE_BLOCK_TYPES else None
    )
    structures.append(
      TaxonomyBlockStructureRequest(
        name=structure_name,
        block_type=block_type or "custom",  # type: ignore[arg-type]
        role_uri=block.role_uri,
        description=block.definition,
        metadata={"received": True},
      )
    )
    for kind, network in _arcs(block):
      for arc in network.arcs:
        associations.append(
          TaxonomyBlockAssociationRequest(
            structure_ref=structure_name,
            from_ref=arc.from_qname,
            to_ref=arc.to_qname,
            association_type=kind,  # type: ignore[arg-type]
            order_value=arc.order,
            arcrole=arc.arcrole,
            weight=arc.weight if kind == "calculation" else None,
          )
        )

  extension = _received_taxonomy(session)
  if extension is None:
    taxonomy_id = reporting_extension.create(
      session,
      CreateTaxonomyBlockRequest(
        name=RECEIVED_TAXONOMY_NAME,
        taxonomy_type="reporting_extension",
        parent_taxonomy_id=str(library.id),
        description=(
          "Concepts and sections of reports received from outside this graph."
        ),
        elements=elements,
        structures=structures,
        associations=associations,
        metadata={RECEIVED_TAXONOMY_KEY: True},
      ),
      created_by,
    )
  else:
    live = set(
      session.execute(
        select(Structure.id).where(
          Structure.taxonomy_id == extension.id,
          Structure.id.in_(remove_structure_ids),
        )
      ).scalars()
    )
    taxonomy_id = reporting_extension.update(
      session,
      UpdateTaxonomyBlockRequest(
        taxonomy_id=str(extension.id),
        elements_to_add=elements,
        structures_to_add=structures,
        structures_to_remove=sorted(str(s) for s in live),
        associations_to_add=associations,
      ),
      created_by,
    )
  session.flush()
  extension = session.get(Taxonomy, taxonomy_id)
  assert extension is not None
  by_name = dict(
    session.execute(
      select(Structure.name, Structure.id).where(
        Structure.taxonomy_id == taxonomy_id,
        Structure.name.in_(list(names.values())),
      )
    ).all()
  )
  structure_ids = {role: str(by_name[name]) for role, name in names.items()}
  return extension, structure_ids, new_qnames


def _element_request(
  qname: str, concept: Concept | None
) -> TaxonomyBlockElementRequest:
  local = qname.rpartition(":")[2]
  if concept is None:
    return TaxonomyBlockElementRequest(qname=qname, name=local)
  if concept.is_dimension_item:
    element_type = "axis"
  elif concept.is_domain_member:
    element_type = "member"
  elif concept.is_hypercube_item:
    element_type = "hypercube"
  elif concept.is_abstract:
    element_type = "abstract"
  else:
    element_type = "concept"
  return TaxonomyBlockElementRequest(
    qname=qname,
    name=concept.pref_label or local,
    balance_type=concept.balance,
    period_type=concept.period_type if concept.period_type != "forever" else None,
    element_type=element_type,
    is_monetary=concept.item_type == "monetaryItemType",
    metadata={"received": True},
  )


def _element_ids(session: Session, qnames: set[str]) -> dict[str, str]:
  if not qnames:
    return {}
  return {
    str(qname): str(element_id)
    for qname, element_id in session.execute(
      select(Element.qname, Element.id).where(Element.qname.in_(qnames))
    ).all()
  }


def _fact_row(
  fact: XbrlFact,
  concept: Concept,
  *,
  element_id: str,
  period_start: date | None,
  period_end: date,
  unit: str | None,
  entity_id: str,
  fact_set_id: str,
  created_by: str,
  basis: str,
) -> Fact:
  source = fact.provenance.source if fact.provenance else None
  provenance = AssertedProvenance(
    source_system=UPLOAD_SOURCE_SYSTEM,
    asserted_by=created_by,
    basis_note=source or basis,
  ).model_dump(mode="json")
  common: dict[str, Any] = {
    "element_id": element_id,
    "period_start": period_start,
    "period_end": period_end,
    "period_type": "instant" if period_start is None else "duration",
    "entity_id": entity_id,
    "fact_set_id": fact_set_id,
    "provenance": provenance,
  }
  if concept.is_numeric and fact.numeric_value is not None:
    return Fact(
      **common,
      value=fact.numeric_value,
      fact_type="Numeric",
      decimals=fact.decimals,
      unit=(unit or "").rpartition(":")[2] or "USD",
    )
  return Fact(
    **common,
    string_value=fact.value_str or "",
    fact_type="Nonnumeric",
    content_type=fact.content_type,
  )


def _store(graph_id: str, report: Report, holon_text: str) -> None:
  """Keep the file as it arrived, under the report's own bundle key."""
  bucket = env.USER_DATA_BUCKET
  key = get_report_bundle_key(
    graph_id, str(report.id), 1, extension=REPORT_ANCHOR_EXTENSION
  )
  stored = S3Client().upload_string(
    content=holon_text,
    bucket=bucket,
    key=key,
    content_type=HOLON_MEDIA_TYPE,
    metadata={"report-id": str(report.id), "graph-id": graph_id},
  )
  if not stored:
    raise RuntimeError(f"The report file could not be stored for {report.id}.")
  report.bundle_url = get_report_bundle_uri(bucket, graph_id, str(report.id), 1)
