"""``StatementBundle`` — the envelope both the RDF and XBRL encoders read.

Schema concepts and linkbases (grouped by ELR) are XBRL-aligned. The instance
slice is graph-native: facts reference period, unit and entity directly with no
``<context>`` nodes, and the XBRL encoder re-derives contexts at emit time.
``rs:`` extensions (IB envelopes, reporting style) carry what XBRL has no
standard for; the XBRL emitter ignores them.
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger

# ── Metadata sub-shapes ────────────────────────────────────────────────────


class EntityMeta(BaseModel):
  """Org-level reporting entity; contexts reference it by ``identifier``."""

  id: str
  name: str
  legal_name: str | None = None
  ein: str | None = None
  country: str | None = None


class PeriodMeta(BaseModel):
  """One reporting period column (instant or duration, per ``period_type``)."""

  start: date
  end: date
  label: str
  period_type: Literal["duration", "instant"] = "duration"


class ReportMeta(BaseModel):
  """``mode='report'`` metadata: the identity a cross-tenant importer needs."""

  report_id: str
  generation_count: int
  filing_status: Literal["draft", "under_review", "filed", "archived"]
  filed_at: datetime | None = None
  supersedes_id: str | None = None
  source_graph_id: str | None = None
  source_report_id: str | None = None
  shared_at: datetime | None = None


class LiveMeta(BaseModel):
  """``mode='live'`` metadata.

  ``non_authoritative`` is always ``True``; it exists so raw JSON-LD readers
  see the "cannot be imported as a Report" flag without inspecting ``@type``.
  """

  snapshot_at: datetime
  non_authoritative: Literal[True] = True


class FrameworkPin(BaseModel):
  """One framework version pin; a list of these renders better in RDF than a map."""

  framework: str
  version: str


# ── Schema concepts (XBRL concept declarations) ────────────────────────────


class BundleElement(BaseModel):
  """An XBRL concept declaration; maps 1:1 to an ``<xs:element>``."""

  id: str
  qname: str
  # Holds the prefix, not the namespace IRI (that is ``namespace_uri``).
  namespace: str | None = None
  # Resolved once by the producer so every encoder reads the same IRI.
  namespace_uri: str | None = None
  name: str
  label: str | None = None
  balance_type: Literal["debit", "credit"] | None = None
  period_type: Literal["duration", "instant"]
  is_abstract: bool = False
  is_monetary: bool = True
  element_type: Literal["concept", "abstract", "axis", "member", "hypercube"] = (
    "concept"
  )
  substitution_group: str | None = None
  source: str
  # Wire vocabulary (camelCase, e.g. 'textBlock'); 'textBlock' gates narrative
  # rendering in report-components.
  item_type: str | None = None


def concept_label(concept: BundleElement) -> str | None:
  """The display label for a concept, or ``None`` when nothing is worth labelling.

  Every encoder reads this so the XBRL label linkbase and JSON-LD
  ``skos:prefLabel`` agree. Prefers the authored label; falls back to
  ``name``, where a tenant extension concept's wording lives (it has no
  ``ElementLabel`` row). A ``name`` that only echoes the QName local part is
  skipped; an authored label is kept even when it does.
  """
  authored = (concept.label or "").strip()
  if authored:
    return authored
  name = (concept.name or "").strip()
  if name and name != concept.qname.split(":", 1)[-1]:
    return name
  return None


# ── Linkbases (XBRL linkbase content) ──────────────────────────────────────


class BundleArc(BaseModel):
  """A single linkbase arc. ``weight`` is set on calculation arcs only."""

  arc_type: Literal["presentationArc", "calculationArc", "definitionArc"]
  arcrole: str
  from_qname: str
  to_qname: str
  order_value: float | None = None
  weight: float | None = None


class BundleLinkbaseLink(BaseModel):
  """A ``<link:X>`` wrapping the arcs of one Extended Link Role.

  The Structure id, name and block_type ride as ``rs:`` extensions so
  consumers can recover the Network identity.
  """

  link_type: Literal["presentationLink", "calculationLink", "definitionLink"]
  role_uri: str
  structure_id: str
  structure_name: str
  block_type: str | None = None
  arcs: list[BundleArc] = Field(default_factory=list)


class BundleLinkbases(BaseModel):
  """Presentation / calculation / definition links, one per ELR.

  Labels ride on ``BundleElement.label``; reference linkbases are not carried.
  """

  presentation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  calculation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  definition_links: list[BundleLinkbaseLink] = Field(default_factory=list)


# ── Instance: contexts, units, facts ───────────────────────────────────────


class BundlePeriod(BaseModel):
  """An ``rs:Period`` node, one per distinct period a fact references.

  ``period_start`` is null for instant periods.
  """

  id: str
  period_start: date | None = None
  period_end: date
  period_type: Literal["duration", "instant"]


class BundleContext(BaseModel):
  """An ``<xbrli:context>``, one per entity + period combination.

  Not stored on the bundle: the XBRL encoder derives these at emit time.
  """

  id: str
  entity_identifier: str
  entity_scheme: str = "http://robosystems.ai/entity"
  period_start: date | None = None
  period_end: date
  period_type: Literal["duration", "instant"]


class BundleUnit(BaseModel):
  """An ``rs:Unit`` node. Simple measures only; divide units are unsupported."""

  id: str
  measure: str


class BundleFact(BaseModel):
  """An ``rs:Fact`` node referencing its period, unit and entity by id."""

  id: str
  element_id: str
  element_qname: str
  value: float | None = None
  text_value: str | None = None
  fact_type: str = "Numeric"
  content_type: str | None = None
  period_ref: str
  # None for Nonnumeric facts, which carry no unitRef.
  unit_ref: str | None = None
  entity_ref: str
  decimals: str = "INF"
  fact_set_id: str | None = None
  structure_id: str | None = None


# ── Bundle root ────────────────────────────────────────────────────────────


class StatementBundle(BaseModel):
  """The portable Report (or live snapshot) artifact.

  ``mode='report'`` bundles carry ``report_meta`` and are stamped to S3 at
  publish; ``mode='live'`` bundles carry ``live_meta``, are response-only, and
  cannot be imported as a Report.
  """

  model_config = ConfigDict(arbitrary_types_allowed=True)

  entity: EntityMeta
  periods: list[PeriodMeta]
  reporting_style: str
  framework_pins: list[FrameworkPin]

  schema_concepts: list[BundleElement]

  linkbases: BundleLinkbases

  period_nodes: list[BundlePeriod]
  units: list[BundleUnit]
  facts: list[BundleFact]

  ib_envelopes: list[Any] = Field(
    default_factory=list,
    description=(
      "Per-Network InformationBlockEnvelope payloads. Typed as Any to "
      "avoid a circular import from operations → models.api; the encoder "
      "consumes each as a Pydantic-dumpable mapping."
    ),
  )

  # Disclosure structure_id → sort code. Disclosures have no inherent order,
  # so the bundle pins one; JSON-LD publishes it as ``rs:structureOrder``.
  structure_display_order: dict[str, int] = Field(default_factory=dict)

  mode: Literal["report", "live"]
  report_meta: ReportMeta | None = None
  live_meta: LiveMeta | None = None


# ── Producer ────────────────────────────────────────────────────────────────

# Keep in sync with ``_RENDER_TARGET_STATEMENT_TYPES`` in
# ``operations/roboledger/commands/reports.py`` (not imported, to keep the
# serialization kernel free of that dependency).
_STATEMENT_BLOCK_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)

# ``association_type`` → XBRL linkbase; everything but presentation and
# calculation lives on the definition linkbase.
_LINKBASE_GROUP_FOR_TYPE: dict[
  str, Literal["presentation", "calculation", "definition"]
] = {
  "presentation": "presentation",
  "calculation": "calculation",
  "equivalence": "definition",
  "general-special": "definition",
  "derivation": "definition",
  "essence-alias": "definition",
  "mapping": "definition",
}

# Default arcroles when a row carries none; the inverse (ingest) mapping is
# ``arelle/extractor.py:ARCROLE_MAPPING``.
_DEFAULT_ARCROLE_FOR_TYPE: dict[str, str] = {
  "presentation": "http://www.xbrl.org/2003/arcrole/parent-child",
  "calculation": "http://www.xbrl.org/2003/arcrole/summation-item",
  "general-special": "http://www.xbrl.org/2003/arcrole/general-special",
  "essence-alias": "http://www.xbrl.org/2003/arcrole/essence-alias",
  "equivalence": (
    "http://xbrlsite.azurewebsites.net/2016/conceptual-model/"
    "arcrole/class-equivalentClass"
  ),
}


def build_report_bundle(
  session: Session,
  graph_id: str,
  report_id: str,
) -> StatementBundle:
  """Assemble a ``mode='report'`` bundle from a published Report.

  Runs inside the publish transaction, after facts are stamped. The
  extensions session has ``autoflush=False``, so this flushes first to make
  pending Fact rows visible. ``session`` must have the tenant search_path set.

  Raises:
    LookupError: the report, the entity, or the platform Graph row is missing.
  """
  # Deferred so the encoder side can import this package without the
  # roboledger reads tree.
  from robosystems.database import platform_session
  from robosystems.models.core.graph.graph import Graph
  from robosystems.models.extensions.association import Association
  from robosystems.models.extensions.element import Element
  from robosystems.models.extensions.element_label import ElementLabel
  from robosystems.models.extensions.entity import Entity
  from robosystems.models.extensions.roboledger.fact import Fact
  from robosystems.models.extensions.roboledger.fact_set import FactSet
  from robosystems.models.extensions.roboledger.report import Report
  from robosystems.models.extensions.structure import Structure
  from robosystems.operations.information_block.envelope import (
    DISCLOSURE_BLOCK_TYPE,
  )
  from robosystems.operations.information_block.statement import (
    _build_statement_envelope,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    get_render_network,
    load_primary_reporting_style,
  )
  from robosystems.taxonomy.pins import resolve_pin

  session.flush()

  report = session.get(Report, report_id)
  if report is None:
    raise LookupError(f"Report {report_id!r} not found in active session.")

  reporting_style_id = load_primary_reporting_style(session)

  # The framework pin is Graph-level, so it comes from the platform DB.
  with platform_session() as pdb:
    graph = pdb.query(Graph).filter(Graph.graph_id == graph_id).first()
    if graph is None:
      raise LookupError(f"Graph {graph_id!r} not found in platform DB.")
    framework_pin_dict = resolve_pin(graph)
  framework_pins = [
    FrameworkPin(framework=name, version=ver)
    for name, ver in framework_pin_dict.items()
  ]

  fact_sets: list[FactSet] = list(
    session.execute(select(FactSet).where(FactSet.report_id == report_id)).scalars()
  )
  fact_set_ids = [str(fs.id) for fs in fact_sets]
  structure_to_fact_set: dict[str, str] = {
    str(fs.structure_id): str(fs.id) for fs in fact_sets if fs.structure_id
  }

  facts: list[Fact] = []
  if fact_set_ids:
    facts = list(
      session.execute(select(Fact).where(Fact.fact_set_id.in_(fact_set_ids))).scalars()
    )

  element_ids: set[str] = {str(f.element_id) for f in facts}
  elements_by_id: dict[str, Element] = {}
  if element_ids:
    elements_by_id = {
      str(e.id): e
      for e in session.execute(
        select(Element).where(Element.id.in_(element_ids))
      ).scalars()
    }

  structure_ids: set[str] = {
    str(fs.structure_id) for fs in fact_sets if fs.structure_id
  }
  associations: list[Association] = []
  structures_by_id: dict[str, Structure] = {}
  if structure_ids:
    associations = list(
      session.execute(
        select(Association).where(Association.structure_id.in_(structure_ids))
      ).scalars()
    )
    structures_by_id = {
      str(s.id): s
      for s in session.execute(
        select(Structure).where(Structure.id.in_(structure_ids))
      ).scalars()
    }
    # Arc endpoints with no facts of their own (parents, subtotals).
    assoc_element_ids: set[str] = {str(a.from_element_id) for a in associations} | {
      str(a.to_element_id) for a in associations
    }
    missing_element_ids = assoc_element_ids - set(elements_by_id)
    if missing_element_ids:
      for e in session.execute(
        select(Element).where(Element.id.in_(missing_element_ids))
      ).scalars():
        elements_by_id[str(e.id)] = e

    # Library calc arcs live on separate rs-gaap-calculation Structures, so
    # they must be pulled in or the calculation linkbase ships empty.
    associations.extend(
      _source_calculation_arcs(session, associations, structures_by_id, elements_by_id)
    )

  # Reuse the read-side renderer so envelopes match the API response shape.
  ib_envelopes: list[Any] = []
  for block_type in _STATEMENT_BLOCK_TYPES:
    try:
      network = get_render_network(session, reporting_style_id, block_type)
    except Exception as exc:
      logger.debug(
        "build_report_bundle: skipping %s — no render network: %s",
        block_type,
        exc,
      )
      continue
    fact_set_id = structure_to_fact_set.get(network.structure_id)
    envelope = _build_statement_envelope(
      session,
      network.structure_id,
      fact_set_id,
      block_type=block_type,
    )
    if envelope is not None:
      ib_envelopes.append(envelope)

  from robosystems.operations.information_block.disclosure import (
    build_envelope as _build_disclosure_envelope,
  )

  # The 100 offset keeps note sort codes clear of the statements' slots.
  disclosure_rows = sorted(
    (s for s in structures_by_id.values() if s.block_type == DISCLOSURE_BLOCK_TYPE),
    key=_disclosure_sort_key,
  )
  structure_display_order: dict[str, int] = {}
  for i, s in enumerate(disclosure_rows):
    sid = str(s.id)
    structure_display_order[sid] = 100 + i
    envelope = _build_disclosure_envelope(session, sid, structure_to_fact_set.get(sid))
    if envelope is not None:
      ib_envelopes.append(envelope)

  # Single-entity assumption, matching ``create_report``.
  entity = (
    session.execute(select(Entity).order_by(Entity.created_at.asc())).scalars().first()
  )
  if entity is None:
    raise LookupError("No entity rows in tenant — Report cannot be bundled.")

  entity_meta = EntityMeta(
    id=str(entity.id),
    name=str(entity.name),
    legal_name=entity.legal_name,
    ein=entity.tax_id,
    country=entity.address_country,
  )

  # element.name is the bare localname for single-word concepts (Assets,
  # Cash, …), so the standard label is the display source.
  standard_label_by_id: dict[str, str] = {}
  if elements_by_id:
    standard_label_by_id = {
      str(eid): txt
      for eid, txt in session.execute(
        select(ElementLabel.element_id, ElementLabel.text).where(
          ElementLabel.element_id.in_(list(elements_by_id)),
          ElementLabel.role == "standard",
          ElementLabel.language == "en",
        )
      )
    }

  schema_concepts = [
    _element_to_bundle(elements_by_id[eid], standard_label_by_id.get(eid))
    for eid in sorted(elements_by_id)
  ]
  linkbases = _associations_to_linkbases(associations, structures_by_id, elements_by_id)
  period_nodes, period_ref_for_fact = _mint_periods(facts)
  units, unit_ref_for_fact = _mint_units(facts)
  bundle_facts = [
    _fact_to_bundle(
      f,
      elements_by_id.get(str(f.element_id)),
      period_ref_for_fact[str(f.id)],
      unit_ref_for_fact.get(str(f.id)),
      entity_meta.id,
    )
    for f in facts
  ]

  return StatementBundle(
    entity=entity_meta,
    periods=_period_metas_for_report(report, fact_sets),
    reporting_style=reporting_style_id,
    framework_pins=framework_pins,
    schema_concepts=schema_concepts,
    linkbases=linkbases,
    period_nodes=period_nodes,
    units=units,
    facts=bundle_facts,
    ib_envelopes=ib_envelopes,
    structure_display_order=structure_display_order,
    mode="report",
    report_meta=ReportMeta(
      report_id=str(report.id),
      generation_count=int(report.generation_count or 0),
      filing_status=str(report.filing_status),
      filed_at=report.filed_at,
      supersedes_id=report.supersedes_id,
      source_graph_id=report.source_graph_id,
      source_report_id=report.source_report_id,
      shared_at=report.shared_at,
    ),
  )


# ── Internal projection helpers ────────────────────────────────────────────


def _disclosure_sort_key(structure: Any) -> tuple[int, float, str, str]:
  """Explicit ``note_order`` metadata first, then creation order, then id."""
  meta = structure.metadata_ if isinstance(structure.metadata_, dict) else {}
  note_order = meta.get("note_order")
  has_order = isinstance(note_order, (int, float)) and not isinstance(note_order, bool)
  return (
    0 if has_order else 1,
    float(note_order) if has_order else 0.0,
    str(structure.created_at or ""),
    str(structure.id),
  )


def _period_metas_for_report(report: Any, fact_sets: list[Any]) -> list[PeriodMeta]:
  """Period columns from the stamped FactSets, else the Report's requested periods."""
  seen: set[tuple[date | None, date]] = set()
  metas: list[PeriodMeta] = []
  for fs in fact_sets:
    key = (fs.period_start, fs.period_end)
    if key in seen:
      continue
    seen.add(key)
    start = fs.period_start or fs.period_end
    period_type = "instant" if fs.period_start is None else "duration"
    metas.append(
      PeriodMeta(
        start=start,
        end=fs.period_end,
        label=f"{start.isoformat()} → {fs.period_end.isoformat()}",
        period_type=period_type,
      )
    )
  if metas:
    metas.sort(key=lambda p: (p.start, p.end))
    return metas

  json_periods = report.periods or []
  for entry in json_periods:
    start_str = entry.get("start") or entry.get("end")
    end_str = entry.get("end")
    if not end_str:
      continue
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    metas.append(
      PeriodMeta(
        start=start,
        end=end,
        label=entry.get("label") or f"{start.isoformat()} → {end.isoformat()}",
      )
    )
  if metas:
    return metas

  if report.period_start and report.period_end:
    return [
      PeriodMeta(
        start=report.period_start,
        end=report.period_end,
        label=f"{report.period_start.isoformat()} → {report.period_end.isoformat()}",
      )
    ]
  return []


# Where a prefix nothing registered resolves: a tenant's own ontology prefix
# (``custom``) still needs a stable IRI for its concepts to be declared under.
MINTED_TAXONOMY_BASE = "https://robosystems.ai/taxonomy/"


def namespace_uri_for(
  prefix: str | None,
  *,
  concept_iri: str | None = None,
  local_name: str | None = None,
) -> str:
  """The namespace IRI an element's concept is declared under.

  ``Element.namespace`` holds only the prefix. Tries, in order: the concept
  IRI minus its local name (it is built as ``namespace + local``), the
  canonical context's binding for the prefix, then a minted IRI.
  """
  from robosystems.arelle.context import CANONICAL_CONTEXT

  if concept_iri and local_name and concept_iri.endswith(local_name):
    namespace = concept_iri[: -len(local_name)]
    if namespace.endswith(("/", "#")):
      return namespace
  if not prefix:
    return f"{MINTED_TAXONOMY_BASE}unqualified/"
  bound = CANONICAL_CONTEXT.get(prefix)
  if isinstance(bound, str):
    return bound
  return f"{MINTED_TAXONOMY_BASE}{prefix}/"


def _wire_item_type(item_type: str | None) -> str | None:
  """OLTP snake_case value domain → wire camelCase ('text_block' → 'textBlock')."""
  if not item_type:
    return None
  head, *rest = str(item_type).split("_")
  return head + "".join(part.capitalize() for part in rest)


def _element_to_bundle(e: Any, standard_label: str | None = None) -> BundleElement:
  qname = str(e.qname or e.name)
  prefix = e.namespace or (qname.split(":", 1)[0] if ":" in qname else None)
  return BundleElement(
    id=str(e.id),
    qname=qname,
    namespace=e.namespace,
    namespace_uri=namespace_uri_for(
      prefix,
      concept_iri=getattr(e, "uri", None),
      local_name=qname.rsplit(":", 1)[-1],
    ),
    name=str(e.name),
    label=standard_label or e.description,
    balance_type=e.balance_type if e.balance_type in {"debit", "credit"} else None,
    period_type=str(e.period_type),
    is_abstract=bool(e.is_abstract),
    is_monetary=bool(e.is_monetary),
    element_type=str(e.element_type),
    substitution_group=e.substitution_group,
    source=str(e.source),
    item_type=_wire_item_type(getattr(e, "item_type", None)),
  )


def _source_calculation_arcs(
  session: Session,
  loaded_associations: list[Any],
  structures_by_id: dict[str, Any],
  elements_by_id: dict[str, Any],
) -> list[Any]:
  """Calculation arcs for the declared concepts, re-hosted on rendered Networks.

  Only arcs whose endpoints are both declared are pulled, each hosted under
  the first rendered Network (statement order) carrying both endpoints, so the
  linkbase references only declared concepts. Returns stand-ins, not ORM rows,
  so the reassigned ``structure_id`` never dirties the session.

  Arcs already in ``loaded_associations`` on the same structure (a tenant
  disclosure's own calc arcs) are skipped, or they would emit twice and
  double the footing.
  """
  from robosystems.models.extensions.association import Association

  declared_ids = set(elements_by_id)
  if not declared_ids:
    return []

  pres_concepts_by_structure: dict[str, set[str]] = {}
  already_bundled: set[tuple[str, str, str]] = set()
  for a in loaded_associations:
    structure_id = str(a.structure_id)
    pres_concepts_by_structure.setdefault(structure_id, set()).update(
      (str(a.from_element_id), str(a.to_element_id))
    )
    if str(a.association_type) == "calculation":
      already_bundled.add((structure_id, str(a.from_element_id), str(a.to_element_id)))
  if not pres_concepts_by_structure:
    return []

  block_order = {bt: i for i, bt in enumerate(_STATEMENT_BLOCK_TYPES)}
  ordered_structures = sorted(
    pres_concepts_by_structure,
    key=lambda sid: (
      block_order.get(str(getattr(structures_by_id.get(sid), "block_type", "")), 99),
      sid,
    ),
  )

  calc_rows = list(
    session.execute(
      select(Association).where(
        Association.association_type == "calculation",
        Association.from_element_id.in_(declared_ids),
        Association.to_element_id.in_(declared_ids),
      )
    ).scalars()
  )

  groups: dict[tuple[str, str], list[Any]] = {}
  seen: set[tuple[str, str, str]] = set(already_bundled)
  for a in calc_rows:
    frm, to = str(a.from_element_id), str(a.to_element_id)
    host = next(
      (
        sid
        for sid in ordered_structures
        if frm in pres_concepts_by_structure[sid]
        and to in pres_concepts_by_structure[sid]
      ),
      None,
    )
    if host is None:
      continue
    key = (host, frm, to)
    if key in seen:
      continue
    seen.add(key)
    groups.setdefault((host, frm), []).append(a)

  def _balance(element_id: str) -> str | None:
    return getattr(elements_by_id.get(element_id), "balance_type", None)

  # Emit a subtotal only when EVERY child's weight sign matches the XBRL
  # balance rule (2.1 §5.1.1.2: same balance +, opposite -). Filtering per
  # subtotal keeps each summation complete; it drops indirect-method cash-flow
  # rollups, where real filers omit the calc linkbase too.
  sourced: list[Any] = []
  for (host, subtotal), arcs in groups.items():
    parent_balance = _balance(subtotal)
    if parent_balance not in ("debit", "credit"):
      continue
    legal = True
    for a in arcs:
      child_balance = _balance(str(a.to_element_id))
      if child_balance not in ("debit", "credit"):
        legal = False
        break
      legal_positive = parent_balance == child_balance
      stored_weight = float(a.weight) if a.weight is not None else 1.0
      if (stored_weight >= 0) is not legal_positive:
        legal = False
        break
    if not legal:
      continue
    for a in arcs:
      sourced.append(
        SimpleNamespace(
          association_type="calculation",
          structure_id=host,
          from_element_id=a.from_element_id,
          to_element_id=a.to_element_id,
          arcrole=a.arcrole,
          order_value=a.order_value,
          weight=a.weight,
        )
      )
  return sourced


def _associations_to_linkbases(
  associations: list[Any],
  structures_by_id: dict[str, Any],
  elements_by_id: dict[str, Any] | None = None,
) -> BundleLinkbases:
  """Group Association rows into one link per (linkbase group, ELR), sorted.

  Without ``elements_by_id`` arc endpoints fall back to raw element ids,
  which fail XBRL validation as locators; only fixture tests should omit it.
  """
  buckets: dict[tuple[str, str], list[tuple[Any, str]]] = {}
  for a in associations:
    group = _LINKBASE_GROUP_FOR_TYPE.get(str(a.association_type))
    if group is None:
      logger.debug("Skipping association with unknown type %r", a.association_type)
      continue
    key = (group, str(a.structure_id))
    buckets.setdefault(key, []).append((a, group))

  presentation_links: list[BundleLinkbaseLink] = []
  calculation_links: list[BundleLinkbaseLink] = []
  definition_links: list[BundleLinkbaseLink] = []

  def _qname_for(element_id: str) -> str:
    if elements_by_id is None:
      return element_id
    e = elements_by_id.get(element_id)
    if e is None:
      return element_id
    return str(getattr(e, "qname", None) or getattr(e, "name", element_id))

  for (group, structure_id), arc_rows in buckets.items():
    structure = structures_by_id.get(structure_id)
    role_uri = ""
    structure_name = ""
    block_type: str | None = None
    if structure is not None:
      metadata = structure.metadata_ or {}
      role_uri = str(metadata.get("role_uri") or "")
      structure_name = str(structure.name or "")
      block_type = str(structure.block_type) if structure.block_type else None

    arc_type_for_group: dict[
      str, Literal["presentationArc", "calculationArc", "definitionArc"]
    ] = {
      "presentation": "presentationArc",
      "calculation": "calculationArc",
      "definition": "definitionArc",
    }
    arcs = sorted(
      (
        BundleArc(
          arc_type=arc_type_for_group[group],
          arcrole=(
            str(a.arcrole)
            if a.arcrole
            else _DEFAULT_ARCROLE_FOR_TYPE.get(str(a.association_type), "")
          ),
          from_qname=_qname_for(str(a.from_element_id)),
          to_qname=_qname_for(str(a.to_element_id)),
          order_value=float(a.order_value) if a.order_value is not None else None,
          weight=(
            float(a.weight) if a.weight is not None and group == "calculation" else None
          ),
        )
        for a, _ in arc_rows
      ),
      key=lambda arc: (
        arc.order_value if arc.order_value is not None else 0.0,
        arc.from_qname,
        arc.to_qname,
      ),
    )

    link_type_for_group: dict[
      str, Literal["presentationLink", "calculationLink", "definitionLink"]
    ] = {
      "presentation": "presentationLink",
      "calculation": "calculationLink",
      "definition": "definitionLink",
    }
    link = BundleLinkbaseLink(
      link_type=link_type_for_group[group],
      role_uri=role_uri,
      structure_id=structure_id,
      structure_name=structure_name,
      block_type=block_type,
      arcs=arcs,
    )
    if group == "presentation":
      presentation_links.append(link)
    elif group == "calculation":
      calculation_links.append(link)
    else:
      definition_links.append(link)

  presentation_links.sort(key=lambda lk: (lk.role_uri, lk.structure_id))
  calculation_links.sort(key=lambda lk: (lk.role_uri, lk.structure_id))
  definition_links.sort(key=lambda lk: (lk.role_uri, lk.structure_id))
  return BundleLinkbases(
    presentation_links=presentation_links,
    calculation_links=calculation_links,
    definition_links=definition_links,
  )


def _mint_periods(
  facts: list[Any],
) -> tuple[list[BundlePeriod], dict[str, str]]:
  """Dedupe facts' periods; returns ``(period_nodes, fact_id -> period ref)``.

  Period kind comes from ``Fact.period_type``, not from whether
  ``period_start`` is set: stamping writes ``period_start`` on instant facts
  too, and carrying it would put instant concepts in duration contexts.
  """
  seen: dict[tuple[date | None, date, str], str] = {}
  periods: list[BundlePeriod] = []
  fact_to_ref: dict[str, str] = {}
  for f in facts:
    period_type = str(f.period_type or "duration")
    period_end = f.period_end
    period_start = None if period_type == "instant" else f.period_start
    key = (period_start, period_end, period_type)
    if key not in seen:
      pid = f"p_{len(periods) + 1}"
      seen[key] = pid
      periods.append(
        BundlePeriod(
          id=pid,
          period_start=period_start,
          period_end=period_end,
          period_type=period_type,  # type: ignore[arg-type]
        )
      )
    fact_to_ref[str(f.id)] = seen[key]
  return periods, fact_to_ref


def _mint_units(facts: list[Any]) -> tuple[list[BundleUnit], dict[str, str]]:
  """Dedupe facts' units; returns ``(units, fact_id -> unit ref)``.

  Three-letter upper-case codes become ``iso4217:``; anything else passes
  through for the encoder to prefix. Nonnumeric facts get no unit.
  """
  seen: dict[str, str] = {}
  units: list[BundleUnit] = []
  fact_to_ref: dict[str, str] = {}
  for f in facts:
    if getattr(f, "fact_type", "Numeric") == "Nonnumeric":
      continue
    raw_unit = str(f.unit or "USD")
    measure = (
      f"iso4217:{raw_unit}" if len(raw_unit) == 3 and raw_unit.isupper() else raw_unit
    )
    if measure not in seen:
      unit_id = f"u_{raw_unit}" if len(raw_unit) == 3 else f"u_{len(units) + 1}"
      seen[measure] = unit_id
      units.append(BundleUnit(id=unit_id, measure=measure))
    fact_to_ref[str(f.id)] = seen[measure]
  return units, fact_to_ref


def _fact_to_bundle(
  f: Any,
  element: Any | None,
  period_ref: str,
  unit_ref: str | None,
  entity_ref: str,
) -> BundleFact:
  qname = str(element.qname) if element and element.qname else str(f.element_id)
  common = {
    "id": str(f.id),
    "element_id": str(f.element_id),
    "element_qname": qname,
    "period_ref": period_ref,
    "entity_ref": entity_ref,
    "fact_set_id": str(f.fact_set_id) if f.fact_set_id else None,
    "structure_id": str(f.structure_id) if f.structure_id else None,
  }
  if getattr(f, "fact_type", "Numeric") == "Nonnumeric":
    return BundleFact(
      **common,
      value=None,
      text_value=str(f.string_value),
      fact_type="Nonnumeric",
      content_type=getattr(f, "content_type", None),
      unit_ref=None,
    )
  # The stored precision; a row stamped before facts carried one reads as INF,
  # which is what every export of it has always claimed.
  decimals = getattr(f, "decimals", None)
  return BundleFact(
    **common,
    value=float(f.value),
    unit_ref=unit_ref,
    decimals=str(decimals) if decimals else "INF",
  )
