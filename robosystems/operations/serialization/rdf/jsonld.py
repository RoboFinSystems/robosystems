"""RDF-graph encoder for ``StatementBundle`` — v1.0 graph-native shape.

The graph-native shape is the first *published* bundle ontology (v1.0): the
earlier XBRL-aligned draft never shipped beyond a one-day demo, so there is no
released predecessor to supersede. The design history (XBRL-aligned →
graph-native) is recorded in the specs; the artifact itself is v1.

Builds an :class:`rdflib.Graph` from the bundle and serializes it as JSON-LD
using the canonical ``CANONICAL_CONTEXT`` (``robosystems/arelle/context.py``),
so the export bundle speaks the *same* vocabulary as the framework seeds.

Shape:
* Concepts are ``rs:Element`` nodes carrying XBRL item attributes
  (``xbrli:balance`` / ``xbrli:periodType``).
* Taxonomy arcs are reified ``rs:Association`` nodes (``xlink:from``/``to`` +
  ``xlink:arcrole`` + ``link:weight``/``order``) grouped under ``rs:Structure``.
* Facts are ``rs:Fact`` nodes that reference their aspects **directly** —
  ``rs:element`` / ``rs:entity`` / ``rs:period`` / ``rs:unit`` — mirroring the
  graph's ``FACT_HAS_*`` edges. There is **no** XBRL ``context``; ``rs:Period``
  / ``rs:Unit`` are first-class nodes. The XBRL encoder re-derives contexts.
* IB envelopes embed under ``rs:informationBlocks`` (top-level fields as
  triples; deep mechanics as a JSON literal — the pragmatic v1 boundary).

Validation is decoupled from serialization: ``shacl_report`` (non-raising,
structured) / ``validate_graph`` (raising) run SHACL over the built graph
against ``frameworks/ontology/v1/shapes.ttl`` — the same shapes that gate the
seeds. The publish hook runs it opt-in per ``REPORT_BUNDLE_SHACL_VALIDATION``
and records the outcome on the Report; serialization itself never blocks.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from rdflib import RDF, XSD, Graph, Literal, Namespace, URIRef

from robosystems.arelle.context import CANONICAL_CONTEXT
from robosystems.logger import logger
from robosystems.operations.serialization.bundle import StatementBundle

# Bundle ontology version emitted on the root node. This is the first published
# bundle ontology — the XBRL-aligned draft never shipped, so it's v1, not v2.
SERIALIZATION_VERSION = "1.0"

_REPO_ROOT = Path(__file__).resolve().parents[4]
_SHAPES_PATH = _REPO_ROOT / "frameworks" / "ontology" / "v1" / "shapes.ttl"

# ── Namespaces ─────────────────────────────────────────────────────────────

RS = Namespace("https://robosystems.ai/vocab/")
XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
ISO4217 = Namespace("http://www.xbrl.org/2003/iso4217#")

# Framework taxonomy namespaces — bind so concept qnames compact.
_PREFIX_NS: dict[str, Namespace] = {
  "rs-gaap": Namespace("https://robosystems.ai/taxonomy/rs-gaap/v1/"),
  "fac": Namespace("http://www.xbrlsite.com/fac#"),
  "us-gaap": Namespace("http://fasb.org/us-gaap/"),
  "ifrs": Namespace("http://xbrl.ifrs.org/taxonomy/"),
  "dei": Namespace("http://xbrl.sec.gov/dei/"),
  "disclosures": Namespace("https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/"),
  "iso4217": ISO4217,
  "xbrli": XBRLI,
}

# Standard XBRL arcrole compact tokens accepted on arcs.
_ARCROLE_URIS: dict[str, str] = {
  "parent-child": "http://www.xbrl.org/2003/arcrole/parent-child",
  "summation-item": "http://www.xbrl.org/2003/arcrole/summation-item",
  "general-special": "http://www.xbrl.org/2003/arcrole/general-special",
  "domain-member": "http://xbrl.org/int/dim/arcrole/domain-member",
}

# Report/live-meta predicates the bundle adds beyond CANONICAL_CONTEXT.
_BUNDLE_CONTEXT_EXTRA: dict[str, Any] = {
  "reportMeta": {"@id": f"{RS}reportMeta", "@type": "@id"},
  "liveMeta": {"@id": f"{RS}liveMeta", "@type": "@id"},
  "reportId": {"@id": f"{RS}reportId"},
  "generationCount": {"@id": f"{RS}generationCount", "@type": "xsd:integer"},
  "filingStatus": {"@id": f"{RS}filingStatus"},
  "filedAt": {"@id": f"{RS}filedAt", "@type": "xsd:dateTime"},
  "supersedesId": {"@id": f"{RS}supersedesId"},
  "sourceGraphId": {"@id": f"{RS}sourceGraphId"},
  "sourceReportId": {"@id": f"{RS}sourceReportId"},
  "sharedAt": {"@id": f"{RS}sharedAt", "@type": "xsd:dateTime"},
  "snapshotAt": {"@id": f"{RS}snapshotAt", "@type": "xsd:dateTime"},
  "nonAuthoritative": {"@id": f"{RS}nonAuthoritative", "@type": "xsd:boolean"},
  "frameworkPins": {"@id": f"{RS}frameworkPins", "@type": "@id"},
  "framework": {"@id": f"{RS}framework"},
  "version": {"@id": f"{RS}version"},
  "periods": {"@id": f"{RS}periods", "@type": "@id"},
  "start": {"@id": f"{RS}start", "@type": "xsd:date"},
  "end": {"@id": f"{RS}end", "@type": "xsd:date"},
  "schemaConcepts": {"@id": f"{RS}schemaConcepts", "@type": "@id"},
  "structures": {"@id": f"{RS}structures", "@type": "@id"},
  "periodNodes": {"@id": f"{RS}periodNodes", "@type": "@id"},
  "units": {"@id": f"{RS}units", "@type": "@id"},
  "facts": {"@id": f"{RS}facts", "@type": "@id"},
  "informationBlocks": {"@id": f"{RS}informationBlocks", "@type": "@id"},
  "envelopeJson": {"@id": f"{RS}envelopeJson"},
  "taxonomyId": {"@id": f"{RS}taxonomyId"},
  "taxonomyName": {"@id": f"{RS}taxonomyName"},
  "name": {"@id": f"{RS}name"},
}


# ── Public entry points ────────────────────────────────────────────────────


def serialize_to_jsonld(bundle: StatementBundle) -> str:
  """Serialize a ``StatementBundle`` to a v1.0 JSON-LD string.

  Serialization does not validate — that's a separate, caller-controlled
  concern (``shacl_report`` / ``validate_graph``). The publish hook decides
  whether to validate per the ``REPORT_BUNDLE_SHACL_VALIDATION`` mode; the
  standalone ``examples/_common/validate.py`` and the SHACL regression test
  validate the on-disk artifact; downloads just serialize.
  """
  graph = build_graph(bundle)
  return graph.serialize(
    format="json-ld",
    auto_compact=True,
    context=_build_context(),
    indent=2,
    sort_keys=True,
  )


def serialize_to_turtle(bundle: StatementBundle) -> str:
  """Serialize the bundle to Turtle. Free given the rdflib graph."""
  graph = build_graph(bundle)
  return graph.serialize(format="turtle")


# ── Graph construction ─────────────────────────────────────────────────────


def build_graph(bundle: StatementBundle) -> Graph:
  g = Graph()
  root = _root_uri(bundle)
  _add_root_triples(g, bundle, root)
  _add_schema_concepts(g, bundle, root)
  _add_structures(g, bundle, root)
  _add_periods(g, bundle, root)
  _add_units(g, bundle, root)
  _add_facts(g, bundle, root)
  _add_information_blocks(g, bundle, root)
  return g


def _build_context() -> dict[str, Any]:
  """The bundle @context = canonical vocabulary + bundle-header terms."""
  return {**CANONICAL_CONTEXT, **_BUNDLE_CONTEXT_EXTRA}


# ── URI minting ────────────────────────────────────────────────────────────


def _root_uri(bundle: StatementBundle) -> URIRef:
  if bundle.mode == "report" and bundle.report_meta is not None:
    return URIRef(f"https://robosystems.ai/report/{bundle.report_meta.report_id}")
  if bundle.live_meta is not None:
    return URIRef(
      f"https://robosystems.ai/snapshot/{bundle.live_meta.snapshot_at.isoformat()}"
    )
  return URIRef("https://robosystems.ai/report/anonymous")


def _scoped(root: URIRef, segment: str, ident: str) -> URIRef:
  return URIRef(f"{root!s}/{segment}/{ident}")


def _concept_uri(qname: str) -> URIRef:
  if ":" not in qname:
    return URIRef(f"https://robosystems.ai/concept/{qname}")
  prefix, local = qname.split(":", 1)
  ns = _PREFIX_NS.get(prefix)
  if ns is None:
    return URIRef(f"https://robosystems.ai/concept/{qname}")
  return URIRef(str(ns) + local)


def _measure_uri(measure: str) -> URIRef:
  if measure.startswith("iso4217:"):
    return URIRef(str(ISO4217) + measure[len("iso4217:") :])
  if ":" not in measure:
    return URIRef(f"https://robosystems.ai/measure/{measure}")
  return _concept_uri(measure)


# ── Root header ────────────────────────────────────────────────────────────


def _add_root_triples(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  g.add((root, RDF.type, RS.Report if bundle.mode == "report" else RS.LiveSnapshot))
  g.add((root, RS.serializationVersion, Literal(SERIALIZATION_VERSION)))
  g.add((root, RS.mode, Literal(bundle.mode)))
  g.add((root, RS.reportingStyle, Literal(bundle.reporting_style)))

  entity_node = _scoped(root, "entity", bundle.entity.id)
  g.add((root, RS.entity, entity_node))
  g.add((entity_node, RDF.type, RS.Entity))
  g.add((entity_node, SKOS.prefLabel, Literal(bundle.entity.name)))
  g.add((entity_node, RS.internalId, Literal(bundle.entity.id)))
  if bundle.entity.legal_name:
    g.add((entity_node, RS.legalName, Literal(bundle.entity.legal_name)))
  if bundle.entity.ein:
    g.add((entity_node, RS.ein, Literal(bundle.entity.ein)))
  if bundle.entity.country:
    g.add((entity_node, RS.country, Literal(bundle.entity.country)))

  for idx, period in enumerate(bundle.periods):
    pnode = _scoped(root, "period-column", str(idx))
    g.add((root, RS.periods, pnode))
    g.add((pnode, RS.start, Literal(period.start.isoformat(), datatype=XSD.date)))
    g.add((pnode, RS.end, Literal(period.end.isoformat(), datatype=XSD.date)))
    g.add((pnode, SKOS.prefLabel, Literal(period.label)))

  for pin in bundle.framework_pins:
    pin_node = _scoped(root, "framework-pin", pin.framework)
    g.add((root, RS.frameworkPins, pin_node))
    g.add((pin_node, RS.framework, Literal(pin.framework)))
    g.add((pin_node, RS.version, Literal(pin.version)))

  if bundle.mode == "report" and bundle.report_meta is not None:
    m = bundle.report_meta
    meta = _scoped(root, "report-meta", m.report_id)
    g.add((root, RS.reportMeta, meta))
    g.add((meta, RS.reportId, Literal(m.report_id)))
    g.add((meta, RS.generationCount, Literal(m.generation_count, datatype=XSD.integer)))
    g.add((meta, RS.filingStatus, Literal(m.filing_status)))
    if m.filed_at:
      g.add((meta, RS.filedAt, Literal(m.filed_at.isoformat(), datatype=XSD.dateTime)))
    if m.supersedes_id:
      g.add((meta, RS.supersedesId, Literal(m.supersedes_id)))
    if m.source_graph_id:
      g.add((meta, RS.sourceGraphId, Literal(m.source_graph_id)))
    if m.source_report_id:
      g.add((meta, RS.sourceReportId, Literal(m.source_report_id)))
    if m.shared_at:
      g.add(
        (meta, RS.sharedAt, Literal(m.shared_at.isoformat(), datatype=XSD.dateTime))
      )
  elif bundle.mode == "live" and bundle.live_meta is not None:
    m = bundle.live_meta
    meta = _scoped(root, "live-meta", m.snapshot_at.isoformat())
    g.add((root, RS.liveMeta, meta))
    g.add(
      (meta, RS.snapshotAt, Literal(m.snapshot_at.isoformat(), datatype=XSD.dateTime))
    )
    g.add((meta, RS.nonAuthoritative, Literal(True, datatype=XSD.boolean)))


# ── Concepts (rs:Element) ──────────────────────────────────────────────────


def _add_schema_concepts(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    uri = _concept_uri(concept.qname)
    g.add((root, RS.schemaConcepts, uri))
    g.add((uri, RDF.type, RS.Element))
    if concept.balance_type:
      g.add((uri, XBRLI.balance, Literal(concept.balance_type)))
    g.add((uri, XBRLI.periodType, Literal(concept.period_type)))
    g.add((uri, RS.monetary, Literal(concept.is_monetary, datatype=XSD.boolean)))
    g.add((uri, RS.abstract, Literal(concept.is_abstract, datatype=XSD.boolean)))
    g.add((uri, RS.elementType, Literal(concept.element_type)))
    if concept.item_type:
      # Wire value domain ('textBlock', 'monetary', ...) — 'textBlock'
      # gates the narrative rendering arm in report-components.
      g.add((uri, RS.itemType, Literal(concept.item_type)))
    if concept.label:
      g.add((uri, SKOS.prefLabel, Literal(concept.label)))
    if concept.substitution_group:
      g.add((uri, RS.substitutionGroup, _concept_uri(concept.substitution_group)))
    g.add((uri, RS.internalId, Literal(concept.id)))
    g.add((uri, RS.source, Literal(concept.source)))


# ── Structures + reified Associations ──────────────────────────────────────


def _add_structures(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  all_links = (
    *bundle.linkbases.presentation_links,
    *bundle.linkbases.calculation_links,
    *bundle.linkbases.definition_links,
  )
  # A Structure's *logical* type is the concept-arrangement pattern, derived
  # from its arcs + block_type across all its link groups (a structure_id can
  # span a presentation + a calculation link). Precompute once so the type is
  # consistent no matter which link group we're emitting.
  calc_structures: set[str] = {
    link.structure_id for link in bundle.linkbases.calculation_links if link.arcs
  }
  block_type_by_structure: dict[str, str] = {}
  for link in all_links:
    if link.block_type and link.structure_id not in block_type_by_structure:
      block_type_by_structure[link.structure_id] = link.block_type

  for link in all_links:
    s_uri = _scoped(root, "structure", link.structure_id)
    g.add((root, RS.structure, s_uri))
    g.add((s_uri, RDF.type, RS.Structure))
    # Logical structure type (additive, alongside rs:Structure + rs:blockType):
    # a roll-up network — or a roll-forward for the equity statement — when it
    # carries calculation arcs, else a presentation hierarchy. Lets consumers
    # query `?s a rs:RollUp` instead of string-matching blockType/associationType.
    g.add(
      (
        s_uri,
        RDF.type,
        _structure_arrangement(
          link.structure_id in calc_structures,
          block_type_by_structure.get(link.structure_id),
        ),
      )
    )
    g.add((s_uri, RS.internalId, Literal(link.structure_id)))
    g.add((s_uri, RS.structureName, Literal(link.structure_name)))
    # Promote the legible name to the predicate consumers render; the UUID
    # stays on rs:internalId. (§ logical-naming pass.)
    g.add((s_uri, SKOS.prefLabel, Literal(link.structure_name)))
    if link.role_uri:
      g.add((s_uri, RS.roleUri, Literal(link.role_uri)))
    if link.block_type:
      g.add((s_uri, RS.blockType, Literal(link.block_type)))
    # The same Structure (ELR) can host more than one linkbase group —
    # e.g. a presentation network whose calculation arcs were sourced onto
    # it shares the structure_id across a presentationLink and a
    # calculationLink. Scope the Association IRI by link group so arcs at the
    # same index in different groups don't collapse onto one node (which would
    # give it multiple xlink:from and fail AssociationShape).
    group = link.link_type.removesuffix("Link")
    for idx, arc in enumerate(link.arcs):
      a_uri = _scoped(root, f"association/{link.structure_id}/{group}", str(idx))
      g.add((s_uri, RS.hasAssociation, a_uri))
      g.add((a_uri, RDF.type, RS.Association))
      assoc_type = _assoc_type_for_arc(arc.arc_type)
      # A calculation summation-item arc IS a roll-up relationship — type it
      # first-class so the rule is queryable without dereferencing the arcrole.
      if assoc_type == "calculation":
        g.add((a_uri, RDF.type, RS.RollUpRelationship))
      g.add((a_uri, XLINK["from"], _concept_uri(arc.from_qname)))
      g.add((a_uri, XLINK.to, _concept_uri(arc.to_qname)))
      g.add((a_uri, RS.associationType, Literal(assoc_type)))
      if arc.arcrole:
        g.add((a_uri, XLINK.arcrole, _arcrole_uri(arc.arcrole)))
      if link.role_uri:
        g.add((a_uri, XLINK.role, URIRef(link.role_uri)))
      if arc.order_value is not None:
        g.add(
          (
            a_uri,
            LINK.order,
            Literal(Decimal(str(arc.order_value)), datatype=XSD.decimal),
          )
        )
      if arc.weight is not None:
        g.add(
          (a_uri, LINK.weight, Literal(Decimal(str(arc.weight)), datatype=XSD.decimal))
        )


def _assoc_type_for_arc(arc_type: str) -> str:
  return {
    "presentationArc": "presentation",
    "calculationArc": "calculation",
    "definitionArc": "definition",
  }.get(arc_type, "presentation")


def _structure_arrangement(has_calc: bool, block_type: str | None) -> URIRef:
  """The Structure's logical concept-arrangement type.

  A network that carries calculation (summation) arcs is a **roll-up** — the
  children sum to the parent — except the equity statement, whose calculation
  is a **roll-forward** (opening balance + period changes = closing balance). A
  network with no calculation arcs is a presentation **hierarchy**. This is the
  first-class type consumers query (`?s a rs:RollUp`) instead of inferring the
  pattern from blockType + associationType.
  """
  if not has_calc:
    return RS.Hierarchy
  if block_type == "equity_statement":
    return RS.RollForward
  return RS.RollUp


def _arcrole_uri(arcrole: str) -> URIRef:
  if arcrole.startswith("http"):
    return URIRef(arcrole)
  return URIRef(_ARCROLE_URIS.get(arcrole, arcrole))


# ── Periods + Units (aspect nodes) ─────────────────────────────────────────


def _add_periods(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for period in bundle.period_nodes:
    uri = _scoped(root, "period", period.id)
    g.add((root, RS.periodNodes, uri))
    g.add((uri, RDF.type, RS.Period))
    g.add((uri, XBRLI.periodType, Literal(period.period_type)))
    if period.period_type == "instant":
      g.add(
        (uri, XBRLI.instant, Literal(period.period_end.isoformat(), datatype=XSD.date))
      )
    else:
      start = period.period_start or period.period_end
      g.add((uri, XBRLI.startDate, Literal(start.isoformat(), datatype=XSD.date)))
      g.add(
        (uri, XBRLI.endDate, Literal(period.period_end.isoformat(), datatype=XSD.date))
      )


def _add_units(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for unit in bundle.units:
    uri = _scoped(root, "unit", unit.id)
    g.add((root, RS.units, uri))
    g.add((uri, RDF.type, RS.Unit))
    g.add((uri, XBRLI.measure, _measure_uri(unit.measure)))


# ── Facts (rs:Fact — aspects referenced directly) ──────────────────────────


def _add_facts(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for fact in bundle.facts:
    uri = _scoped(root, "fact", fact.id)
    g.add((root, RS.facts, uri))
    g.add((uri, RDF.type, RS.Fact))
    g.add((uri, RS.element, _concept_uri(fact.element_qname)))
    g.add((uri, RS.entity, _scoped(root, "entity", fact.entity_ref)))
    g.add((uri, RS.period, _scoped(root, "period", fact.period_ref)))
    # Wire casing is lowercase ('numeric' / 'nonnumeric') — matching the
    # xbrl-holon converter, the other producer of this vocabulary. The
    # OLTP/graph columns keep their capitalized values; this is a
    # boundary translation like item_type's snake->camel.
    g.add((uri, RS.factType, Literal(fact.fact_type.lower())))
    if fact.value is not None:
      # Numeric arm — value + unit + decimals, unchanged shape.
      if fact.unit_ref is not None:
        g.add((uri, RS.unit, _scoped(root, "unit", fact.unit_ref)))
      g.add(
        (uri, RS.numericValue, Literal(Decimal(str(fact.value)), datatype=XSD.decimal))
      )
      g.add((uri, RS.decimals, Literal(fact.decimals)))
    if fact.text_value is not None:
      # Nonnumeric (text-block) arm — string value, no unit/decimals
      # (XBRL nonNumeric facts carry neither). rs:stringValue is the
      # predicate @robosystems/report-components reads for narrative.
      g.add((uri, RS.stringValue, Literal(fact.text_value, datatype=XSD.string)))
      if fact.content_type:
        g.add((uri, RS.contentType, Literal(fact.content_type)))
    g.add((uri, RS.internalId, Literal(fact.id)))
    if fact.fact_set_id:
      g.add(
        (uri, RS.factSet, URIRef(f"https://robosystems.ai/factset/{fact.fact_set_id}"))
      )
    if fact.structure_id:
      g.add((uri, RS.structure, _scoped(root, "structure", fact.structure_id)))


# ── IB envelopes (rs: extension; opaque inner content) ─────────────────────


def _add_information_blocks(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for envelope in bundle.ib_envelopes:
    body = _envelope_to_dict(envelope)
    ib_id = body.get("id", "unknown")
    ib_uri = _scoped(root, "ib", ib_id)
    g.add((root, RS.informationBlocks, ib_uri))
    g.add((ib_uri, RDF.type, RS.InformationBlock))
    g.add((ib_uri, RS.internalId, Literal(ib_id)))
    if "block_type" in body:
      g.add((ib_uri, RS.blockType, Literal(body["block_type"])))
    if "name" in body:
      g.add((ib_uri, SKOS.prefLabel, Literal(body["name"])))
    if body.get("taxonomy_id"):
      g.add((ib_uri, RS.taxonomyId, Literal(body["taxonomy_id"])))
    if body.get("taxonomy_name"):
      g.add((ib_uri, RS.taxonomyName, Literal(body["taxonomy_name"])))
    if isinstance(body.get("fact_set"), dict) and body["fact_set"].get("id"):
      g.add(
        (
          ib_uri,
          RS.factSet,
          URIRef(f"https://robosystems.ai/factset/{body['fact_set']['id']}"),
        )
      )
    g.add(
      (
        ib_uri,
        RS.envelopeJson,
        Literal(
          json.dumps(body, default=_json_default, sort_keys=True),
          datatype=URIRef(
            "https://robosystems.ai/datatype/v1/InformationBlockEnvelopeJSON"
          ),
        ),
      )
    )


def _envelope_to_dict(envelope: Any) -> dict[str, Any]:
  if isinstance(envelope, BaseModel):
    return envelope.model_dump(exclude_none=True, mode="json")
  if isinstance(envelope, dict):
    return dict(envelope)
  return dict(getattr(envelope, "__dict__", {}))


# ── Validation (SHACL) ─────────────────────────────────────────────────────


class BundleValidationError(ValueError):
  """Raised when the bundle graph fails SHACL conformance."""


@dataclass(frozen=True)
class ShaclResult:
  """Structured outcome of a SHACL run — capturable / loggable.

  ``ran`` is False when the shapes file is unavailable (validation skipped);
  callers treat that as "not validated", not "conformant". ``report`` is the
  pyshacl text report (empty when conforming).
  """

  ran: bool
  conforms: bool
  violations: int
  shapes_checked: int
  report: str

  def as_dict(self) -> dict[str, Any]:
    """A compact, JSON-storable summary (for ``Report.metadata``)."""
    return {
      "ran": self.ran,
      "conforms": self.conforms,
      "violations": self.violations,
      "shapes_checked": self.shapes_checked,
      # Keep the stored excerpt bounded — the full report can be large.
      "report_excerpt": self.report[:4000] if self.report else "",
      "shapes_version": str(_SHAPES_PATH.relative_to(_REPO_ROOT)),
    }


_SHAPES_CACHE: Graph | None = None
_SHAPES_LOCK = threading.Lock()
_SH = Namespace("http://www.w3.org/ns/shacl#")


def _shapes_graph() -> Graph | None:
  global _SHAPES_CACHE
  if _SHAPES_CACHE is None:
    with _SHAPES_LOCK:
      # Re-check inside the lock: another thread may have parsed it already.
      if _SHAPES_CACHE is None:
        if not _SHAPES_PATH.exists():
          logger.warning(
            "SHACL shapes not found at %s — skipping validation", _SHAPES_PATH
          )
          return None
        _SHAPES_CACHE = Graph().parse(str(_SHAPES_PATH), format="turtle")
  return _SHAPES_CACHE


def shacl_report(g: Graph) -> ShaclResult:
  """Run SHACL over the bundle graph and return a structured result.

  Non-raising — produces an outcome the caller can log (e.g. onto
  ``Report.metadata``) or escalate. Checks the positive instance shapes
  (a Fact has element/period; an Association has from/to/associationType)
  and the negative shapes that ban the retired dialects (``xbrli:contextRef``,
  ``arcFrom``, direct ``summationOf``) — the same shapes that gate the seeds.
  """
  shapes = _shapes_graph()
  if shapes is None:
    return ShaclResult(
      ran=False, conforms=True, violations=0, shapes_checked=0, report=""
    )
  from pyshacl import validate as _shacl_validate

  conforms, results_graph, text = _shacl_validate(
    g, shacl_graph=shapes, inference="none"
  )
  violations = len(list(results_graph.subjects(RDF.type, _SH.ValidationResult)))
  shapes_checked = len(list(shapes.subjects(RDF.type, _SH.NodeShape)))
  return ShaclResult(
    ran=True,
    conforms=bool(conforms),
    violations=violations,
    shapes_checked=shapes_checked,
    report="" if conforms else text,
  )


def validate_graph(g: Graph, bundle: StatementBundle) -> None:
  """Strict SHACL check — raises on non-conformance.

  Thin wrapper over :func:`shacl_report` for callers that want fail-loud
  behavior (tests; the ``strict`` publish mode). Skips silently when the
  shapes file is unavailable.
  """
  result = shacl_report(g)
  if result.ran and not result.conforms:
    raise BundleValidationError(
      f"Bundle graph failed SHACL conformance:\n{result.report}"
    )


# ── JSON encoder default ───────────────────────────────────────────────────


def _json_default(obj: Any) -> Any:
  if isinstance(obj, datetime):
    return obj.isoformat()
  if isinstance(obj, date):
    return obj.isoformat()
  if isinstance(obj, BaseModel):
    return obj.model_dump(mode="json", exclude_none=True)
  raise TypeError(f"Unsupported type for JSON literal: {type(obj).__name__}")
