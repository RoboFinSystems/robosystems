"""``StatementBundle`` envelope — XBRL-aligned shape (v1.0).

The bundle is the design unit shared by both encoder families and
(eventually) both producers (Report + LiveSnapshot). v1.0 of the
serialization ontology treats the bundle as **XBRL expressed in RDF,
plus our extensions**:

* The schema portion (``schema_concepts``) maps 1:1 to XBRL
  ``<xs:element>`` declarations with ``xbrli:`` attributes.
* The linkbases portion (``linkbases.presentation_links`` /
  ``calculation_links`` / ``definition_links``) maps to XBRL
  ``<link:presentationLink>`` / ``<link:calculationLink>`` /
  ``<link:definitionLink>`` containers, grouped by ``xlink:role``
  (the Extended Link Role).
* The instance portion is XBRL-native: dedupe'd ``contexts`` (one per
  distinct entity+period combo), dedupe'd ``units``, and ``facts``
  that reference contexts/units by id via ``xbrli:contextRef`` /
  ``xbrli:unitRef``. Facts carry the concept qname as their type
  (``@type: rs-gaap:Assets``) and the value as ``rdf:value`` — matching
  XBRL's "the element name IS the type tag" pattern.

The ``rs:`` extension surface (IB envelopes, reporting style,
verification, provenance) carries everything XBRL has no standard
for. The XBRL 2.1 emitter walks the same bundle, ignores the ``rs:``
extensions when projecting to XML, and produces a valid XBRL instance
+ linkbase set.
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
  """Reporting entity identity carried in the bundle header.

  This is the org-level entity, not the instance-level
  ``xbrli:entity`` (which lives on contexts). Customers usually have
  one entity per graph, so the header carries the canonical identity
  and contexts reference it via their ``identifier`` field.
  """

  id: str
  name: str
  legal_name: str | None = None
  ein: str | None = None
  country: str | None = None


class PeriodMeta(BaseModel):
  """One reporting period column in the bundle.

  Both instant and duration periods serialize through this shape;
  encoders interpret ``period_type`` to pick the XBRL context shape
  (``<xbrli:instant>`` vs ``<xbrli:startDate>/<xbrli:endDate>``).
  """

  start: date
  end: date
  label: str
  period_type: Literal["duration", "instant"] = "duration"


class ReportMeta(BaseModel):
  """Mode-specific metadata for ``mode='report'`` bundles.

  Carries the filing-lifecycle + restatement-chain + share-provenance
  fields that distinguish a stamped Report from an ephemeral live
  snapshot. A future importer uses these to drive cross-tenant
  identity reconstruction.
  """

  report_id: str
  generation_count: int
  filing_status: Literal["draft", "under_review", "filed", "archived"]
  filed_at: datetime | None = None
  supersedes_id: str | None = None
  source_graph_id: str | None = None
  source_report_id: str | None = None
  shared_at: datetime | None = None


class LiveMeta(BaseModel):
  """Mode-specific metadata for ``mode='live'`` bundles.

  ``non_authoritative`` is a constant ``True`` — the type itself
  carries the "cannot be imported as a Report" invariant; this field
  exists so consumers reading raw JSON-LD see the flag without needing
  to inspect the ``@type``.
  """

  snapshot_at: datetime
  non_authoritative: Literal[True] = True


class FrameworkPin(BaseModel):
  """One framework version pin carried in the bundle header.

  Replaces the flat ``dict[str, str]`` so the JSON-LD output renders
  as ``[{framework, version}]`` — friendlier to RDF consumers than a
  bare object map.
  """

  framework: str
  version: str


# ── Schema concepts (XBRL concept declarations) ────────────────────────────


class BundleElement(BaseModel):
  """An XBRL concept declaration carried in the bundle's schema slice.

  Maps 1:1 to an ``<xs:element>`` declaration in the XBRL emitter.
  Attributes use XBRL's vocabulary: ``xbrli:substitutionGroup``,
  ``xbrli:periodType``, ``xbrli:balance``, ``xsd:type`` (resolved to
  ``xbrli:monetaryItemType`` or ``xbrli:stringItemType`` based on
  ``is_monetary``).
  """

  id: str
  qname: str
  namespace: str | None = None
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


# ── Linkbases (XBRL linkbase content) ──────────────────────────────────────


class BundleArc(BaseModel):
  """A single linkbase arc — presentation / calculation / definition.

  ``arcrole`` carries the XBRL arcrole URI (e.g.
  ``http://www.xbrl.org/2003/arcrole/parent-child``). ``arc_type``
  is the discriminator the XBRL emitter uses to pick the right
  ``<link:presentationArc>`` / ``<link:calculationArc>`` /
  ``<link:definitionArc>`` element. ``weight`` is only meaningful on
  calculation arcs; null elsewhere.
  """

  arc_type: Literal["presentationArc", "calculationArc", "definitionArc"]
  arcrole: str
  from_qname: str
  to_qname: str
  order_value: float | None = None
  weight: float | None = None


class BundleLinkbaseLink(BaseModel):
  """A ``<link:X>`` link wrapping arcs scoped to one Extended Link Role.

  Mirrors XBRL XML where each link element wraps arcs for one ELR
  (``xlink:role``). The JSON-LD encoder emits this as a node with
  ``@type: link:presentationLink`` (or calc/def). Carries the
  Structure identity + name + block_type as ``rs:`` extensions so
  consumers can recover the Network identity.
  """

  link_type: Literal["presentationLink", "calculationLink", "definitionLink"]
  role_uri: str
  structure_id: str
  structure_name: str
  block_type: str | None = None
  arcs: list[BundleArc] = Field(default_factory=list)


class BundleLinkbases(BaseModel):
  """The bundle's linkbase content, grouped by link type.

  v1.0 carries presentation / calculation / definition; label and
  reference linkbases are not yet carried here (labels live on
  ``BundleElement.label`` for now). Each list is a sequence of
  link-per-ELR groupings; the XBRL emitter walks each in order.
  """

  presentation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  calculation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  definition_links: list[BundleLinkbaseLink] = Field(default_factory=list)


# ── Instance: contexts, units, facts ───────────────────────────────────────


class BundlePeriod(BaseModel):
  """An ``rs:Period`` node — one per distinct period a fact references.

  The bundle collapses the XBRL ``<context>`` (entity + period bundled): a Fact
  references its Period directly (mirroring the graph's ``FACT_HAS_PERIOD``
  edge). The XBRL encoder re-derives ``<xbrli:context>`` from these +
  the bundle entity at emit time (XBRL 2.1 requires shared contexts).

  ``period_start`` is null for instant periods (the period IS ``period_end``).
  """

  id: str
  period_start: date | None = None
  period_end: date
  period_type: Literal["duration", "instant"]


class BundleContext(BaseModel):
  """An ``<xbrli:context>`` — entity + period, one per distinct combo.

  **Not stored on the bundle** (the graph-native bundle collapses contexts onto facts). The
  XBRL 2.1 encoder *derives* these from the bundle's entity + ``period_nodes``
  at emit time, because XBRL requires shared ``<context>`` elements with
  ``contextRef``. The JSON-LD encoder never produces them.
  """

  id: str
  entity_identifier: str
  entity_scheme: str = "http://robosystems.ai/entity"
  period_start: date | None = None
  period_end: date
  period_type: Literal["duration", "instant"]


class BundleUnit(BaseModel):
  """An ``rs:Unit`` node — one per distinct measure.

  The bundle carries simple-measure units only (e.g., ``iso4217:USD``). Complex
  units (per-share with divide, ratios) are deferred until a customer
  needs them. The XBRL encoder emits these as ``<xbrli:unit>``.
  """

  id: str
  measure: str


class BundleFact(BaseModel):
  """A single ``rs:Fact`` node — aspects referenced directly (no context).

  Mirrors the graph's Fact + ``FACT_HAS_*`` edges: a Fact references its
  ``period`` (``BundlePeriod`` id), ``unit`` (``BundleUnit`` id), and
  ``entity`` (the bundle entity id) directly — there is no XBRL context
  node. The fidelity bar: every Fact emits with matching (concept,
  period → dates, unit, value, decimals).
  """

  id: str
  element_id: str
  element_qname: str
  value: float | None = None
  text_value: str | None = None
  fact_type: str = "Numeric"
  content_type: str | None = None
  period_ref: str
  # None for Nonnumeric (text-block) facts — XBRL nonNumeric facts carry
  # no unitRef, so no unit is minted for them.
  unit_ref: str | None = None
  entity_ref: str
  decimals: str = "INF"
  fact_set_id: str | None = None
  structure_id: str | None = None


# ── Bundle root ────────────────────────────────────────────────────────────


class StatementBundle(BaseModel):
  """The portable Report (or live snapshot) artifact.

  Mode-tagged: ``mode='report'`` bundles carry ``report_meta`` and are
  S3-stamped at publish; ``mode='live'`` bundles carry ``live_meta``,
  are response-body-only, and are rejected by the (future) importer
  by construction. The mode discriminator is a first-class JSON-LD
  type, not a flag, which enforces the report/live split structurally.

  ``ib_envelopes`` reuses :class:`InformationBlockEnvelope` from
  ``models/api/information_block.py`` directly. This is intentional
  coupling: the IB envelope is the canonical shape the read APIs
  already serve.
  """

  model_config = ConfigDict(arbitrary_types_allowed=True)

  # Header
  entity: EntityMeta
  periods: list[PeriodMeta]
  reporting_style: str
  framework_pins: list[FrameworkPin]

  # Schema — Element (concept) declarations
  schema_concepts: list[BundleElement]

  # Linkbases — reified Structure/Association content, grouped by type + ELR
  linkbases: BundleLinkbases

  # Instance — graph-native: facts reference period/unit/entity directly.
  # Period nodes replace XBRL contexts (the XBRL encoder re-derives contexts).
  period_nodes: list[BundlePeriod]
  units: list[BundleUnit]
  facts: list[BundleFact]

  # IB envelopes (RS extension — no XBRL equivalent)
  ib_envelopes: list[Any] = Field(
    default_factory=list,
    description=(
      "Per-Network InformationBlockEnvelope payloads. Typed as Any to "
      "avoid a circular import from operations → models.api; the encoder "
      "consumes each as a Pydantic-dumpable mapping."
    ),
  )

  # Mode discriminator + arm
  mode: Literal["report", "live"]
  report_meta: ReportMeta | None = None
  live_meta: LiveMeta | None = None


# ── Producer ────────────────────────────────────────────────────────────────

# The four statement Networks the bundle attempts to resolve. Equity and
# comprehensive-income variants slot in once their Reporting Style picker
# rows are seeded — this mirrors ``_RENDER_TARGET_STATEMENT_TYPES`` in
# ``operations/roboledger/commands/reports.py`` (kept in sync rather than
# imported to avoid a cross-module dep from the serialization kernel).
_STATEMENT_BLOCK_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)

# Map our internal ``association_type`` enum to the XBRL linkbase
# grouping it lives in. Presentation arcs stay on presentation; calc
# stays on calc; everything else (equivalence, general-special,
# derivation, essence-alias) lives on the definition linkbase per XBRL
# Dimensions / XBRL 2.1 conventions.
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

# XBRL standard arcrole URIs — used as the default when our DB rows
# don't carry an explicit arcrole. The inverse direction lives in
# ``arelle/extractor.py:ARCROLE_MAPPING`` (used during framework
# ingest); these are the export direction.
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
  """Assemble a ``mode='report'`` ``StatementBundle`` from a published Report.

  Called from the publish-hook in ``create_report`` /
  ``regenerate_report`` after facts are stamped and rules have run,
  before the transaction commits. The extensions session has
  ``autoflush=False`` (``db/extensions.py``); callers are responsible
  for an explicit ``session.flush()`` before invoking so pending Fact
  rows are visible to ORM reads inside the assembler. ORM-only reads —
  no raw SQL pulls of newly-persisted rows.

  Assembly produces the XBRL-aligned shape: concepts in
  ``schema_concepts``; arcs grouped into
  ``linkbases.{presentation,calculation,definition}_links``
  by association_type, with each link wrapping arcs scoped to one
  Structure (ELR); contexts dedupe'd by (entity, period); units
  dedupe'd by measure; facts hold context/unit refs instead of inline
  period/unit values.

  Args:
    session: Extensions session with tenant search_path active.
    graph_id: The owning graph; resolves reporting style + framework pin
      via a short-lived platform-session lookup.
    report_id: The Report whose FactSets + Facts + IB envelopes the
      bundle wraps.

  Raises:
    LookupError: ``report_id`` doesn't resolve in the active session,
      or the owning Graph row is missing from the platform DB.
  """
  # Imports are deferred to keep the operations/serialization package
  # importable without dragging the roboledger reads tree in transitively
  # (the encoder side runs in narrower contexts than the producer).
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

  # Flush pending writes so freshly-stamped facts are visible to the
  # ORM reads below. Idempotent — repeat calls are cheap.
  session.flush()

  report = session.get(Report, report_id)
  if report is None:
    raise LookupError(f"Report {report_id!r} not found in active session.")

  # Reporting Style lives on the entity — resolve it from the extensions
  # session already in scope (the primary entity's Style).
  reporting_style_id = load_primary_reporting_style(session)

  # Framework pin is still Graph-level; resolve it against the platform DB.
  # Short-lived; doesn't bleed extensions-side state into the platform session.
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

  # Schema concepts — Elements referenced by facts. Bounded by the
  # report's own structures + element ids; bundle stays self-contained.
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
    # Pick up any additional Elements referenced only via association
    # endpoints (parents / subtotals with no facts of their own).
    assoc_element_ids: set[str] = {str(a.from_element_id) for a in associations} | {
      str(a.to_element_id) for a in associations
    }
    missing_element_ids = assoc_element_ids - set(elements_by_id)
    if missing_element_ids:
      for e in session.execute(
        select(Element).where(Element.id.in_(missing_element_ids))
      ).scalars():
        elements_by_id[str(e.id)] = e

    # Calculation arcs live on separate rs-gaap-calculation Structures, not on
    # the rendered presentation Networks — so without this they never reach the
    # bundle and the XBRL calculation linkbase ships empty. Pull calc arcs whose
    # BOTH endpoints are concepts we already declare, and bind each to the
    # rendered Network (ELR) that carries both endpoints: it inherits that
    # Network's role (conventional shared presentation/calculation ELR) and
    # references only declared concepts, so the emitted linkbase is Arelle-safe.
    associations.extend(
      _source_calculation_arcs(session, associations, structures_by_id, elements_by_id)
    )

  # Per-statement IB envelopes — reuse the read-side renderer so the
  # bundle's per-Network payload matches the API response shape exactly.
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

  # Disclosure envelopes — every picked disclosure structure (fact-driven
  # numeric notes AND snapshotted text-block notes) rides in the bundle,
  # pinned to this report's FactSet. Deferred import mirrors
  # ``_build_statement_envelope``'s cycle-avoidance.
  from robosystems.operations.information_block.disclosure import (
    build_envelope as _build_disclosure_envelope,
  )

  for sid, s in structures_by_id.items():
    if s.block_type != DISCLOSURE_BLOCK_TYPE:
      continue
    envelope = _build_disclosure_envelope(session, sid, structure_to_fact_set.get(sid))
    if envelope is not None:
      ib_envelopes.append(envelope)

  # Entity header — single-entity assumption matches ``_get_entity_id``
  # used by ``create_report``. Multi-entity graphs lock in once
  # consolidation lands (see ``architecture_multientity_consolidation``).
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

  # Standard display labels (role=standard, en) for the bundled concepts — the
  # XBRL label-linkbase + JSON-LD skos:prefLabel source. element.name is the
  # human label for most concepts but the bare localname for single-word
  # fundamentals (Assets, Cash, Liabilities, Revenues, Goodwill, …); sourcing
  # the standard label here keeps those from collapsing to the QName in arelle.
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


def _period_metas_for_report(report: Any, fact_sets: list[Any]) -> list[PeriodMeta]:
  """Derive the bundle's period columns.

  Prefer the FactSet rows' (period_start, period_end) tuples — they
  reflect what was actually stamped. Fall back to the Report's
  ``periods`` JSON for empty-fact bundles (no facts → no FactSets →
  still want the bundle to carry the requested period header).
  """
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


def _element_to_bundle(e: Any, standard_label: str | None = None) -> BundleElement:
  return BundleElement(
    id=str(e.id),
    qname=str(e.qname or e.name),
    namespace=e.namespace,
    name=str(e.name),
    # The standard label linkbase entry is the authoritative display label
    # (skos:prefLabel / XBRL standard label); fall back to the element's
    # description only when no standard label exists.
    label=standard_label or e.description,
    balance_type=e.balance_type if e.balance_type in {"debit", "credit"} else None,
    period_type=str(e.period_type),
    is_abstract=bool(e.is_abstract),
    is_monetary=bool(e.is_monetary),
    element_type=str(e.element_type),
    substitution_group=e.substitution_group,
    source=str(e.source),
  )


def _source_calculation_arcs(
  session: Session,
  presentation_associations: list[Any],
  structures_by_id: dict[str, Any],
  elements_by_id: dict[str, Any],
) -> list[Any]:
  """Return calculation arcs (as lightweight stand-ins) for the report's
  concepts, each bound to a rendered Network so it shares that Network's ELR.

  Calc relationships live on dedicated rs-gaap-calculation Structures; the
  bundle otherwise only loads arcs on the rendered presentation Networks. We
  pull calc associations whose endpoints are BOTH already declared
  (``elements_by_id``) and host each under the rendered Network whose
  presentation concepts contain both endpoints (deterministic statement
  order). Stand-ins — not ORM rows — avoid dirtying the session with a
  reassigned ``structure_id``.
  """
  from robosystems.models.extensions.association import Association

  declared_ids = set(elements_by_id)
  if not declared_ids:
    return []

  pres_concepts_by_structure: dict[str, set[str]] = {}
  for a in presentation_associations:
    pres_concepts_by_structure.setdefault(str(a.structure_id), set()).update(
      (str(a.from_element_id), str(a.to_element_id))
    )
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

  # Host each arc under a rendered Network and group by (host, subtotal).
  groups: dict[tuple[str, str], list[Any]] = {}
  seen: set[tuple[str, str, str]] = set()
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

  # Emit a subtotal's children only when EVERY child's stored weight sign
  # agrees with the XBRL balance-derived legal sign (§5.1.1.2 Table 6: same
  # balance -> +weight, opposite -> -weight). Filtering per subtotal (not per
  # arc) keeps each emitted summation complete so it still foots. This drops
  # the cash-flow rollups — whose indirect-method children mix debit/credit
  # under a cash subtotal, the canonical case where XBRL calc weights are
  # illegal and real filers omit the calculation linkbase — while keeping the
  # well-behaved balance-sheet / income-statement summations.
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
  """Group raw Association rows into XBRL-aligned linkbase containers.

  Buckets by ``association_type`` → linkbase group (presentation /
  calculation / definition), then by ``structure_id`` (the ELR) within
  each bucket. Each unique ``(group, structure_id)`` pair becomes one
  ``BundleLinkbaseLink``; arcs sort by ``(order_value, from_qname,
  to_qname)`` so the JSON-LD output is deterministic.

  ``elements_by_id`` is an optional ``{element_id: Element-like}`` map
  the caller already has from the bundle producer. When supplied, arc
  endpoints (``from_qname`` / ``to_qname``) resolve to the element's
  actual qname (``rs-gaap:Assets``); otherwise they fall back to the
  raw element_id (ULID). The encoders consume whatever is on the arc:
  qnames produce valid XBRL linkbase locators, ULIDs produce Arelle
  ``xlink:to type NCName`` validation errors. Always pass the map
  unless you're writing a fixture test that doesn't care about
  linkbase resolution.
  """
  # First pass: bucket arcs by (group, structure_id)
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
    """Resolve an element_id to its qname using the producer's
    pre-loaded element map. Falls back to the id itself if no map is
    available or the id isn't in it — encoders surface that as an
    NCName-shaped error during validation so the gap is obvious."""
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
  """Dedupe facts' periods into a flat ``rs:Period`` node array.

  Returns ``(period_nodes, fact_id_to_period_ref)`` so the caller can
  populate ``BundleFact.period_ref`` without re-walking. Period ids are
  stable: ``p_1``, ``p_2``, … in first-seen order. Facts reference
  these directly (no XBRL context); the XBRL encoder re-derives contexts
  from them + the bundle entity.

  **Period kind comes from ``Fact.period_type``, not from whether
  ``period_start`` is populated.** The fact-stamping side writes
  ``period_start`` even on instant facts; carrying it would route
  instant BS facts into duration periods (XBRL processors reject an
  instant-typed concept against a duration period) and multiply the
  dedup. So the kind drives normalization: instant → ``period_start=None``,
  keyed on ``period_end`` only.
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
  """Dedupe facts' units into a flat unit array.

  v1.0 supports simple-measure units only. ``unit`` strings like
  ``USD`` resolve to ``iso4217:USD``; non-currency units pass through
  as-is (e.g., ``shares`` → ``xbrli:shares``). Encoders apply final
  prefix resolution.
  """
  seen: dict[str, str] = {}
  units: list[BundleUnit] = []
  fact_to_ref: dict[str, str] = {}
  for f in facts:
    if getattr(f, "fact_type", "Numeric") == "Nonnumeric":
      # Text-block facts carry no unit; mint nothing and leave the fact
      # without a unit ref.
      continue
    raw_unit = str(f.unit or "USD")
    # Currency codes get the iso4217: prefix; anything else passes
    # through unchanged for the encoder to handle.
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
  return BundleFact(
    **common,
    value=float(f.value),
    unit_ref=unit_ref,
    decimals="INF",
  )
