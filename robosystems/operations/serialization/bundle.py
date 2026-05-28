"""``StatementBundle`` envelope — the design unit shared by both encoder
families and (eventually) both producers (Report + LiveSnapshot).

The envelope is intentionally format-neutral: the JSON-LD encoder and the
XBRL 2.1 encoder both consume the same ``StatementBundle`` and emit the
same fact set in their respective shapes. Mode-tagging
(``mode='report'`` vs ``mode='live'``) carries the persistence /
authority semantics — see spec §3 for the importer rejection invariant.

Phase 1 ships ``build_report_bundle``; Phase 3 will add
``build_live_bundle`` over the same envelope with no encoder changes.
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
  """Reporting entity identity carried in the bundle header."""

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


# ── Framework slice ────────────────────────────────────────────────────────


class BundleElement(BaseModel):
  """A taxonomy element node in the bundle's framework slice.

  Carries only the XBRL-intrinsic attributes the encoders need to emit
  a self-describing JSON-LD ``rs:Element`` node or an XBRL ``<xs:element>``
  declaration. Linkbase arcs (parent, summationOf, etc.) live on
  ``BundleAssociation`` rows, not on the element itself.
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


class BundleAssociation(BaseModel):
  """A linkbase arc in the bundle's framework slice.

  One row per arc — presentation, calculation, definition, label.
  ``arcrole`` carries the XBRL arcrole URI (e.g.
  ``http://www.xbrl.org/2003/arcrole/parent-child``); the JSON-LD
  encoder maps it back to a predicate via ``ARCROLE_MAPPING`` (the
  inverse of the framework-ingest direction).
  """

  structure_id: str
  from_element_id: str
  to_element_id: str
  arcrole: str
  association_type: str
  order_value: float | None = None
  weight: float | None = None
  role: str | None = None


class FrameworkSlice(BaseModel):
  """The subset of the framework taxonomy referenced by this bundle.

  Bundles are self-contained: the slice includes only Elements +
  Associations actually used by the facts and IB envelopes. A reader
  can reconstruct the rendered statements + linkbase relationships
  from the slice alone without dereferencing external taxonomies.
  """

  elements: list[BundleElement] = Field(default_factory=list)
  associations: list[BundleAssociation] = Field(default_factory=list)


# ── Fact ───────────────────────────────────────────────────────────────────


class BundleFact(BaseModel):
  """A single Fact node in the bundle.

  The round-trip fidelity bar is at this granularity: every Fact in
  the source must emit a Fact with matching (concept, period, unit,
  value) on the way out. ``decimals`` follows XBRL convention
  (``"INF"`` for exact integers, integer N for N-decimal precision).
  """

  id: str
  element_id: str
  element_qname: str
  value: float
  period_start: date
  period_end: date
  period_type: Literal["duration", "instant"]
  unit: str = "USD"
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
  type, not a flag — see spec §3 for the structural enforcement.

  ``ib_envelopes`` reuses :class:`InformationBlockEnvelope` from
  ``models/api/information_block.py`` directly. This is intentional
  coupling: the IB envelope is the canonical shape the read APIs
  already serve, and round-tripping requires the same shape.
  """

  model_config = ConfigDict(arbitrary_types_allowed=True)

  # Header
  entity: EntityMeta
  periods: list[PeriodMeta]
  reporting_style: str
  framework_pins: dict[str, str]

  # Body
  framework_slice: FrameworkSlice
  ib_envelopes: list[Any] = Field(
    default_factory=list,
    description=(
      "Per-Network InformationBlockEnvelope payloads. Typed as Any to "
      "avoid a circular import from operations → models.api; the encoder "
      "consumes each as a Pydantic-dumpable mapping."
    ),
  )
  facts: list[BundleFact]

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
    framework_pins = resolve_pin(graph)

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

  # Framework slice — Elements referenced by facts + Associations on the
  # structures the bundle covers. Both queries are bounded by the
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
  if structure_ids:
    associations = list(
      session.execute(
        select(Association).where(Association.structure_id.in_(structure_ids))
      ).scalars()
    )
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
      # Not all Reporting Styles compose every statement type
      # (equity / comprehensive-income are commonly absent on the
      # default style). Skip gracefully — the bundle still wraps what
      # the report rendered.
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

  return StatementBundle(
    entity=EntityMeta(
      id=entity.id,
      name=entity.name,
      legal_name=entity.legal_name,
      ein=entity.tax_id,
      country=entity.address_country,
    ),
    periods=_period_metas_for_report(report, fact_sets),
    reporting_style=reporting_style_id,
    framework_pins=framework_pins,
    framework_slice=FrameworkSlice(
      elements=[
        _element_to_bundle(elements_by_id[eid]) for eid in sorted(elements_by_id)
      ],
      associations=[_association_to_bundle(a) for a in associations],
    ),
    ib_envelopes=ib_envelopes,
    facts=[_fact_to_bundle(f, elements_by_id.get(f.element_id)) for f in facts],
    mode="report",
    report_meta=ReportMeta(
      report_id=report.id,
      generation_count=report.generation_count,
      filing_status=report.filing_status,
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
    id=e.id,
    qname=e.qname or e.name,
    namespace=e.namespace,
    name=e.name,
    label=e.description,
    balance_type=e.balance_type if e.balance_type in {"debit", "credit"} else None,
    period_type=e.period_type,
    is_abstract=e.is_abstract,
    is_monetary=e.is_monetary,
    element_type=e.element_type,
    substitution_group=e.substitution_group,
    source=e.source,
  )


def _association_to_bundle(a: Any) -> BundleAssociation:
  return BundleAssociation(
    structure_id=a.structure_id,
    from_element_id=a.from_element_id,
    to_element_id=a.to_element_id,
    arcrole=a.arcrole or "",
    association_type=a.association_type,
    order_value=a.order_value,
    weight=a.weight,
    role=(a.metadata_ or {}).get("role"),
  )


def _fact_to_bundle(f: Any, element: Any | None) -> BundleFact:
  return BundleFact(
    id=f.id,
    element_id=f.element_id,
    element_qname=(element.qname if element and element.qname else f.element_id),
    value=f.value,
    period_start=f.period_start or f.period_end,
    period_end=f.period_end,
    period_type=f.period_type,
    unit=f.unit,
    decimals="INF",
    fact_set_id=f.fact_set_id,
    structure_id=f.structure_id,
  )
