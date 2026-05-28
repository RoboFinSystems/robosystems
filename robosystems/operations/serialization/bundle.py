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

See ``local/docs/specs/bundle-ontology-v1.md`` for the full ontology
spec including ``@context`` mapping tables and migration path.
"""

from __future__ import annotations

from datetime import date, datetime
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
  snapshot. Importer (Phase 2) uses these to drive cross-tenant
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
  """Mode-specific metadata for ``mode='live'`` bundles (Phase 3).

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
  reference linkbases are deferred to v1.1 (labels live on
  ``BundleElement.label`` for now). Each list is a sequence of
  link-per-ELR groupings; the XBRL emitter walks each in order.
  """

  presentation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  calculation_links: list[BundleLinkbaseLink] = Field(default_factory=list)
  definition_links: list[BundleLinkbaseLink] = Field(default_factory=list)


# ── Instance: contexts, units, facts ───────────────────────────────────────


class BundleContext(BaseModel):
  """An ``<xbrli:context>`` — one per distinct (entity, period) combo.

  Dedupe'd at the bundle level so the JSON-LD output has a flat
  ``xbrli:context`` array and the XBRL emitter writes a clean set of
  ``<xbrli:context id="...">`` elements. Facts reference contexts by
  id via ``xbrli:contextRef``.

  ``entity_scheme`` defaults to RoboSystems' native scheme; SEC-sourced
  entities preserve their CIK scheme. ``period_start`` is null for
  instant periods (the period IS ``period_end`` in that case).
  """

  id: str
  entity_identifier: str
  entity_scheme: str = "http://robosystems.ai/entity"
  period_start: date | None = None
  period_end: date
  period_type: Literal["duration", "instant"]


class BundleUnit(BaseModel):
  """An ``<xbrli:unit>`` — one per distinct measure.

  v1.0 carries simple-measure units only (e.g., ``iso4217:USD``).
  Complex units (per-share with divide, ratios) are deferred until a
  customer needs them.
  """

  id: str
  measure: str


class BundleFact(BaseModel):
  """A single Fact node in the bundle.

  Carries the value + decimals + references into contexts/units; the
  period and unit data live on the referenced ``BundleContext`` /
  ``BundleUnit`` objects. The round-trip fidelity bar is at this
  granularity: every Fact in the source emits a Fact with matching
  (concept, contextRef → period, unitRef → unit, value, decimals).
  """

  id: str
  element_id: str
  element_qname: str
  value: float
  context_ref: str
  unit_ref: str
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
  type, not a flag — see ``bundle-ontology-v1.md`` §4 for the
  structural enforcement.

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

  # Schema (XBRL concept declarations)
  schema_concepts: list[BundleElement]

  # Linkbases (XBRL linkbase content, grouped by link type + ELR)
  linkbases: BundleLinkbases

  # Instance — XBRL contexts, units, facts (dedupe'd at producer time)
  contexts: list[BundleContext]
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


# ── Producer (Phase 1a) ────────────────────────────────────────────────────

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

  Assembly produces the XBRL-aligned shape per
  ``bundle-ontology-v1.md`` §4: concepts in ``schema_concepts``;
  arcs grouped into ``linkbases.{presentation,calculation,definition}_links``
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
  from robosystems.models.extensions.entity import Entity
  from robosystems.models.extensions.roboledger.fact import Fact
  from robosystems.models.extensions.roboledger.fact_set import FactSet
  from robosystems.models.extensions.roboledger.report import Report
  from robosystems.models.extensions.structure import Structure
  from robosystems.operations.information_block.statement import (
    _build_statement_envelope,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    get_render_network,
  )
  from robosystems.taxonomy.pins import resolve_pin

  # Flush pending writes so freshly-stamped facts are visible to the
  # ORM reads below. Idempotent — repeat calls are cheap.
  session.flush()

  report = session.get(Report, report_id)
  if report is None:
    raise LookupError(f"Report {report_id!r} not found in active session.")

  # Resolve Graph-level metadata (reporting style + framework pin)
  # against the platform DB. Short-lived; doesn't bleed extensions-side
  # state into the platform session.
  with platform_session() as pdb:
    graph = pdb.query(Graph).filter(Graph.graph_id == graph_id).first()
    if graph is None:
      raise LookupError(f"Graph {graph_id!r} not found in platform DB.")
    reporting_style_id = str(graph.reporting_style_id)
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

  schema_concepts = [
    _element_to_bundle(elements_by_id[eid]) for eid in sorted(elements_by_id)
  ]
  linkbases = _associations_to_linkbases(associations, structures_by_id)
  contexts, context_ref_for_fact = _mint_contexts(facts, entity_meta)
  units, unit_ref_for_fact = _mint_units(facts)
  bundle_facts = [
    _fact_to_bundle(
      f,
      elements_by_id.get(str(f.element_id)),
      context_ref_for_fact[str(f.id)],
      unit_ref_for_fact[str(f.id)],
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
    contexts=contexts,
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


def _element_to_bundle(e: Any) -> BundleElement:
  return BundleElement(
    id=str(e.id),
    qname=str(e.qname or e.name),
    namespace=e.namespace,
    name=str(e.name),
    label=e.description,
    balance_type=e.balance_type if e.balance_type in {"debit", "credit"} else None,
    period_type=str(e.period_type),
    is_abstract=bool(e.is_abstract),
    is_monetary=bool(e.is_monetary),
    element_type=str(e.element_type),
    substitution_group=e.substitution_group,
    source=str(e.source),
  )


def _associations_to_linkbases(
  associations: list[Any],
  structures_by_id: dict[str, Any],
) -> BundleLinkbases:
  """Group raw Association rows into XBRL-aligned linkbase containers.

  Buckets by ``association_type`` → linkbase group (presentation /
  calculation / definition), then by ``structure_id`` (the ELR) within
  each bucket. Each unique ``(group, structure_id)`` pair becomes one
  ``BundleLinkbaseLink``; arcs sort by ``(order_value, from_qname,
  to_qname)`` so the JSON-LD output is deterministic.
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

  # Element id → qname lookup for arc endpoints. Built lazily from
  # association data; we don't have direct Element rows here so endpoints
  # carry the raw id which gets resolved at encoding time. Instead store
  # the from/to *element_id* on the arc and let encoders look up the
  # qname from the bundle's schema_concepts. Simpler: use element_id as
  # the qname placeholder for now; encoder swaps to actual qname via the
  # schema lookup. (This is the only place producer↔encoder coupling
  # leaks; documented as such.)
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
          from_qname=str(a.from_element_id),
          to_qname=str(a.to_element_id),
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


def _mint_contexts(
  facts: list[Any],
  entity_meta: EntityMeta,
) -> tuple[list[BundleContext], dict[str, str]]:
  """Dedupe facts' (entity, period) tuples into a flat context array.

  Returns ``(contexts, fact_id_to_context_ref)`` so the caller can
  populate ``BundleFact.context_ref`` without re-walking the facts.
  Context ids are stable: ``ctx_1``, ``ctx_2``, … assigned in
  first-seen order. Encoders consume the same ids.

  v1.0 assumes one entity per bundle (matches the single-entity
  assumption in ``_get_entity_id``); when consolidation lands, this
  helper accepts per-fact entity overrides.
  """
  seen: dict[tuple[str, date | None, date, str], str] = {}
  contexts: list[BundleContext] = []
  fact_to_ref: dict[str, str] = {}
  for f in facts:
    period_start = f.period_start
    period_end = f.period_end
    period_type = "instant" if period_start is None else "duration"
    key = (entity_meta.id, period_start, period_end, period_type)
    if key not in seen:
      ctx_id = f"ctx_{len(contexts) + 1}"
      seen[key] = ctx_id
      contexts.append(
        BundleContext(
          id=ctx_id,
          entity_identifier=entity_meta.id,
          period_start=period_start,
          period_end=period_end,
          period_type=period_type,  # type: ignore[arg-type]
        )
      )
    fact_to_ref[str(f.id)] = seen[key]
  return contexts, fact_to_ref


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
  context_ref: str,
  unit_ref: str,
) -> BundleFact:
  return BundleFact(
    id=str(f.id),
    element_id=str(f.element_id),
    element_qname=(
      str(element.qname) if element and element.qname else str(f.element_id)
    ),
    value=float(f.value),
    context_ref=context_ref,
    unit_ref=unit_ref,
    decimals="INF",
    fact_set_id=str(f.fact_set_id) if f.fact_set_id else None,
    structure_id=str(f.structure_id) if f.structure_id else None,
  )
