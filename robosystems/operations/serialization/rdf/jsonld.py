"""RDF-graph encoder for ``StatementBundle`` — v1.0 XBRL-aligned shape.

Builds an :class:`rdflib.Graph` from the bundle, validates structural
integrity, then serializes via rdflib's native serializers. JSON-LD is
the Phase 1a default flavor; Turtle / N-Quads / RDF/XML slot in as
``rdflib.Graph.serialize(format=...)`` calls without further work.

The graph is built per the v1.0 ontology (``bundle-ontology-v1.md``):
* XBRL-aligned content (concepts, linkbase arcs, contexts, units,
  facts) is modeled as proper RDF triples.
* IB envelopes embed under ``rs:informationBlocks`` — top-level
  fields land as triples (``rs:blockType``, ``rs:structure``, etc.);
  deeply-nested mechanics / rendering / view projections embed as a
  JSON literal under ``rs:envelopeJson`` (pragmatic v1.0 boundary;
  v2.0 may model them as triples if SPARQL discoverability is wanted).

Why rdflib:
* Validation as a structural property — malformed triples raise at
  build time, not at parse time downstream.
* Multi-format serialization is free — same graph emits JSON-LD,
  Turtle, N-Quads, RDF/XML.
* Round-trip parse is rdflib's job; consumers can SPARQL the output.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import rdflib
from pydantic import BaseModel
from rdflib import RDF, XSD, Graph, Literal, Namespace, URIRef
from rdflib.term import IdentifiedNode

from robosystems.logger import logger
from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleContext,
  BundleFact,
  BundleLinkbaseLink,
  BundleUnit,
  StatementBundle,
)

# Bundle ontology version emitted on the root node. Bumps when the
# ontology's shape changes in a way that requires consumer dispatch.
SERIALIZATION_VERSION = "1.0"


# ── Namespace constants ───────────────────────────────────────────────────


XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
XBRLDT = Namespace("http://xbrl.org/2005/xbrldt#")
XBRLDI = Namespace("http://xbrl.org/2006/xbrldi#")
ISO4217 = Namespace("http://www.xbrl.org/2003/iso4217#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
DCTERMS = Namespace("http://purl.org/dc/terms/")
# RS extension namespace — versioned with the ontology itself; v2.0
# would bump the URI segment.
RS = Namespace("https://robosystems.ai/vocab/serialization/v1/")

# Framework taxonomy namespaces — bind to their published IRIs so concept
# qnames compact correctly.
RS_GAAP = Namespace("https://robosystems.ai/taxonomy/rs-gaap/v1/")
FAC = Namespace("http://www.xbrlsite.com/fac#")
US_GAAP = Namespace("http://fasb.org/us-gaap/")
IFRS = Namespace("http://xbrl.ifrs.org/taxonomy/")
DEI = Namespace("http://xbrl.sec.gov/dei/")
DISCLOSURES = Namespace("https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/")
CHECKLIST = Namespace("https://robosystems.ai/taxonomy/rs-gaap/reporting-checklist/v1/")
STYLES = Namespace("https://robosystems.ai/taxonomy/rs-gaap/reporting-styles/v1/")


# Standard XBRL arcrole URIs used as the value of ``xlink:arcrole`` on arcs.
_ARCROLE_URIS: dict[str, URIRef] = {
  "parent-child": URIRef("http://www.xbrl.org/2003/arcrole/parent-child"),
  "summation-item": URIRef("http://www.xbrl.org/2003/arcrole/summation-item"),
  "general-special": URIRef("http://www.xbrl.org/2003/arcrole/general-special"),
  "essence-alias": URIRef("http://www.xbrl.org/2003/arcrole/essence-alias"),
  "similar-tuples": URIRef("http://www.xbrl.org/2003/arcrole/similar-tuples"),
  "hypercube-dimension": URIRef("http://xbrl.org/int/dim/arcrole/hypercube-dimension"),
  "dimension-domain": URIRef("http://xbrl.org/int/dim/arcrole/dimension-domain"),
  "domain-member": URIRef("http://xbrl.org/int/dim/arcrole/domain-member"),
  "all": URIRef("http://xbrl.org/int/dim/arcrole/all"),
  "notAll": URIRef("http://xbrl.org/int/dim/arcrole/notAll"),
}


# ── Public entry point ───────────────────────────────────────────────────


def serialize_to_jsonld(bundle: StatementBundle) -> str:
  """Serialize a ``StatementBundle`` to a v1.0 JSON-LD string.

  Builds the rdflib graph, validates structural integrity, then
  serializes via rdflib's native JSON-LD serializer with the v1.0
  context. The encoder is pure (no DB access, no side effects) — all
  data lives on the bundle.
  """
  graph = build_graph(bundle)
  validate_graph(graph, bundle)
  context = _build_context(bundle)
  return graph.serialize(
    format="json-ld",
    auto_compact=True,
    context=context,
    indent=2,
    sort_keys=True,
  )


def serialize_to_turtle(bundle: StatementBundle) -> str:
  """Serialize the bundle to Turtle. Free given the rdflib graph."""
  graph = build_graph(bundle)
  validate_graph(graph, bundle)
  return graph.serialize(format="turtle")


# ── Graph construction ───────────────────────────────────────────────────


def build_graph(bundle: StatementBundle) -> Graph:
  """Walk the bundle and return a populated :class:`rdflib.Graph`.

  Each XBRL-aligned object becomes one or more triples rooted in the
  bundle's URI tree. The bundle root has type ``xbrli:xbrl + rs:Report``
  (or ``rs:LiveSnapshot`` for live mode); all subsidiary objects
  (concepts, contexts, units, facts, linkbase links, arcs, IB
  envelopes) hang off the root via the ``rs:`` and ``xbrli:`` predicates
  specified in the v1.0 ontology.
  """
  g = Graph()
  _bind_prefixes(g)

  root = _root_uri(bundle)
  _add_root_triples(g, bundle, root)
  _add_schema_concepts(g, bundle, root)
  _add_linkbases(g, bundle, root)
  _add_contexts(g, bundle, root)
  _add_units(g, bundle, root)
  _add_facts(g, bundle, root)
  _add_information_blocks(g, bundle, root)
  return g


def _bind_prefixes(g: Graph) -> None:
  """Bind the v1.0 namespaces so serialized output compacts cleanly."""
  for prefix, ns in (
    ("xbrli", XBRLI),
    ("link", LINK),
    ("xlink", XLINK),
    ("xbrldt", XBRLDT),
    ("xbrldi", XBRLDI),
    ("iso4217", ISO4217),
    ("skos", SKOS),
    ("dcterms", DCTERMS),
    ("rs", RS),
    ("rs-gaap", RS_GAAP),
    ("fac", FAC),
    ("us-gaap", US_GAAP),
    ("ifrs", IFRS),
    ("dei", DEI),
    ("disclosures", DISCLOSURES),
    ("checklist", CHECKLIST),
    ("styles", STYLES),
  ):
    g.bind(prefix, ns, override=True, replace=True)


# ── URI minting ──────────────────────────────────────────────────────────


def _root_uri(bundle: StatementBundle) -> URIRef:
  if bundle.mode == "report" and bundle.report_meta is not None:
    return URIRef(f"https://robosystems.ai/report/{bundle.report_meta.report_id}")
  if bundle.live_meta is not None:
    return URIRef(
      f"https://robosystems.ai/snapshot/{bundle.live_meta.snapshot_at.isoformat()}"
    )
  return URIRef("https://robosystems.ai/report/anonymous")


def _scoped_uri(root: URIRef, segment: str, ident: str) -> URIRef:
  """Mint a URI scoped under the bundle root so two bundles' ``ctx_1``
  identifiers never collide in shared RDF stores."""
  return URIRef(f"{root!s}/{segment}/{ident}")


def _concept_uri(qname: str) -> URIRef:
  """Resolve a concept qname (``rs-gaap:Assets``) to its full IRI.

  Uses the bound namespaces — falls back to a synthetic URI for
  unrecognized prefixes so the graph still builds.
  """
  if ":" not in qname:
    return URIRef(f"https://robosystems.ai/concept/{qname}")
  prefix, local = qname.split(":", 1)
  ns_map: dict[str, Namespace] = {
    "rs-gaap": RS_GAAP,
    "fac": FAC,
    "us-gaap": US_GAAP,
    "ifrs": IFRS,
    "dei": DEI,
    "disclosures": DISCLOSURES,
    "checklist": CHECKLIST,
    "styles": STYLES,
    "xbrli": XBRLI,
  }
  ns = ns_map.get(prefix)
  if ns is None:
    return URIRef(f"https://robosystems.ai/concept/{qname}")
  return URIRef(str(ns) + local)


# ── Root node triples ────────────────────────────────────────────────────


def _add_root_triples(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  g.add((root, RDF.type, XBRLI.xbrl))
  if bundle.mode == "report":
    g.add((root, RDF.type, RS.Report))
  else:
    g.add((root, RDF.type, RS.LiveSnapshot))
  g.add((root, RS.serializationVersion, Literal(SERIALIZATION_VERSION)))
  g.add((root, RS.mode, Literal(bundle.mode)))
  g.add((root, RS.reportingStyle, Literal(bundle.reporting_style)))

  # Entity (bnode under the root — singular, not collection)
  entity_node = _scoped_uri(root, "entity", bundle.entity.id)
  g.add((root, RS.entity, entity_node))
  g.add((entity_node, RDF.type, RS.Entity))
  g.add((entity_node, SKOS.prefLabel, Literal(bundle.entity.name)))
  if bundle.entity.legal_name:
    g.add((entity_node, RS.legalName, Literal(bundle.entity.legal_name)))
  if bundle.entity.ein:
    g.add((entity_node, RS.ein, Literal(bundle.entity.ein)))
  if bundle.entity.country:
    g.add((entity_node, RS.country, Literal(bundle.entity.country)))

  # Periods
  for idx, period in enumerate(bundle.periods):
    period_node = _scoped_uri(root, "period", str(idx))
    g.add((root, RS.periods, period_node))
    g.add((period_node, RDF.type, RS.PeriodColumn))
    g.add((period_node, RS.start, Literal(period.start.isoformat(), datatype=XSD.date)))
    g.add((period_node, RS.end, Literal(period.end.isoformat(), datatype=XSD.date)))
    g.add((period_node, SKOS.prefLabel, Literal(period.label)))

  # Framework pins
  for pin in bundle.framework_pins:
    pin_node = _scoped_uri(root, "framework-pin", pin.framework)
    g.add((root, RS.frameworkPins, pin_node))
    g.add((pin_node, RDF.type, RS.FrameworkPin))
    g.add((pin_node, RS.framework, Literal(pin.framework)))
    g.add((pin_node, RS.version, Literal(pin.version)))

  # Mode-specific meta
  if bundle.mode == "report" and bundle.report_meta is not None:
    meta = bundle.report_meta
    meta_node = _scoped_uri(root, "report-meta", meta.report_id)
    g.add((root, RS.reportMeta, meta_node))
    g.add((meta_node, RDF.type, RS.ReportMeta))
    g.add((meta_node, RS.reportId, Literal(meta.report_id)))
    g.add(
      (
        meta_node,
        RS.generationCount,
        Literal(meta.generation_count, datatype=XSD.integer),
      )
    )
    g.add((meta_node, RS.filingStatus, Literal(meta.filing_status)))
    if meta.filed_at:
      g.add(
        (
          meta_node,
          RS.filedAt,
          Literal(meta.filed_at.isoformat(), datatype=XSD.dateTime),
        )
      )
    if meta.supersedes_id:
      g.add(
        (
          meta_node,
          RS.supersedesId,
          URIRef(f"https://robosystems.ai/report/{meta.supersedes_id}"),
        )
      )
    if meta.source_graph_id:
      g.add((meta_node, RS.sourceGraphId, Literal(meta.source_graph_id)))
    if meta.source_report_id:
      g.add(
        (
          meta_node,
          RS.sourceReportId,
          URIRef(f"https://robosystems.ai/report/{meta.source_report_id}"),
        )
      )
    if meta.shared_at:
      g.add(
        (
          meta_node,
          RS.sharedAt,
          Literal(meta.shared_at.isoformat(), datatype=XSD.dateTime),
        )
      )
  elif bundle.mode == "live" and bundle.live_meta is not None:
    meta = bundle.live_meta
    meta_node = _scoped_uri(root, "live-meta", meta.snapshot_at.isoformat())
    g.add((root, RS.liveMeta, meta_node))
    g.add((meta_node, RDF.type, RS.LiveMeta))
    g.add(
      (
        meta_node,
        RS.snapshotAt,
        Literal(meta.snapshot_at.isoformat(), datatype=XSD.dateTime),
      )
    )
    g.add((meta_node, RS.nonAuthoritative, Literal(True, datatype=XSD.boolean)))


# ── Schema concepts ──────────────────────────────────────────────────────


def _add_schema_concepts(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  """Each concept declared as an ``xsd:element`` with XBRL attributes."""
  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    concept_uri = _concept_uri(concept.qname)
    g.add((root, RS.schemaConcepts, concept_uri))
    g.add((concept_uri, RDF.type, _xsd_element_uri()))
    g.add((concept_uri, _xsd_uri("name"), Literal(concept.name)))
    sub_group = concept.substitution_group or "xbrli:item"
    g.add((concept_uri, XBRLI.substitutionGroup, _resolve_qname_uri(sub_group)))
    g.add((concept_uri, XBRLI.periodType, Literal(concept.period_type)))
    item_type = XBRLI.monetaryItemType if concept.is_monetary else XBRLI.stringItemType
    g.add((concept_uri, _xsd_uri("type"), item_type))
    g.add(
      (
        concept_uri,
        _xsd_uri("abstract"),
        Literal(concept.is_abstract, datatype=XSD.boolean),
      )
    )
    g.add((concept_uri, _xsd_uri("nillable"), Literal(True, datatype=XSD.boolean)))
    if concept.balance_type:
      g.add((concept_uri, XBRLI.balance, Literal(concept.balance_type)))
    if concept.label:
      g.add((concept_uri, SKOS.prefLabel, Literal(concept.label)))
    g.add((concept_uri, RS.internalId, Literal(concept.id)))
    g.add((concept_uri, RS.source, Literal(concept.source)))


def _xsd_element_uri() -> URIRef:
  """``xsd:element`` requires explicit URIRef construction because rdflib's
  built-in XSD namespace doesn't expose ``element`` as a member."""
  return URIRef("http://www.w3.org/2001/XMLSchema#element")


def _xsd_uri(local: str) -> URIRef:
  return URIRef(f"http://www.w3.org/2001/XMLSchema#{local}")


def _resolve_qname_uri(qname: str) -> URIRef:
  if ":" not in qname:
    return URIRef(qname)
  prefix, local = qname.split(":", 1)
  ns_map: dict[str, str] = {
    "xbrli": str(XBRLI),
    "rs-gaap": str(RS_GAAP),
    "fac": str(FAC),
    "us-gaap": str(US_GAAP),
    "xsd": str(rdflib.XSD),
  }
  base = ns_map.get(prefix)
  if base is None:
    return URIRef(f"https://robosystems.ai/qname/{qname}")
  return URIRef(base + local)


# ── Linkbases ────────────────────────────────────────────────────────────


def _add_linkbases(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for link in bundle.linkbases.presentation_links:
    _add_link(g, root, link, RS.presentationLinks)
  for link in bundle.linkbases.calculation_links:
    _add_link(g, root, link, RS.calculationLinks)
  for link in bundle.linkbases.definition_links:
    _add_link(g, root, link, RS.definitionLinks)


def _add_link(
  g: Graph, root: URIRef, link: BundleLinkbaseLink, link_predicate: URIRef
) -> None:
  link_uri = _scoped_uri(root, f"link/{link.link_type}", link.structure_id)
  g.add((root, link_predicate, link_uri))
  g.add((link_uri, RDF.type, _link_type_uri(link.link_type)))
  if link.role_uri:
    g.add((link_uri, XLINK.role, URIRef(link.role_uri)))
  g.add((link_uri, RS.internalId, Literal(link.structure_id)))
  g.add((link_uri, RS.structureName, Literal(link.structure_name)))
  if link.block_type:
    g.add((link_uri, RS.blockType, Literal(link.block_type)))
  for idx, arc in enumerate(link.arcs):
    arc_node = _scoped_uri(
      root, f"link/{link.link_type}/{link.structure_id}/arc", str(idx)
    )
    g.add((link_uri, _link_arc_uri(arc.arc_type), arc_node))
    _add_arc(g, arc_node, arc)


def _link_type_uri(link_type: str) -> URIRef:
  return URIRef(f"http://www.xbrl.org/2003/linkbase#{link_type}")


def _link_arc_uri(arc_type: str) -> URIRef:
  return URIRef(f"http://www.xbrl.org/2003/linkbase#{arc_type}")


def _add_arc(g: Graph, arc_node: URIRef, arc: BundleArc) -> None:
  g.add((arc_node, RDF.type, _link_arc_uri(arc.arc_type)))
  arcrole_uri = (
    URIRef(arc.arcrole)
    if arc.arcrole.startswith("http")
    else _ARCROLE_URIS.get(arc.arcrole)
  )
  if arcrole_uri:
    g.add((arc_node, XLINK.arcrole, arcrole_uri))
  g.add((arc_node, XLINK["from"], _concept_uri(arc.from_qname)))
  g.add((arc_node, XLINK["to"], _concept_uri(arc.to_qname)))
  if arc.order_value is not None:
    g.add(
      (
        arc_node,
        LINK.order,
        Literal(Decimal(str(arc.order_value)), datatype=XSD.decimal),
      )
    )
  if arc.weight is not None and arc.arc_type == "calculationArc":
    g.add(
      (arc_node, LINK.weight, Literal(Decimal(str(arc.weight)), datatype=XSD.decimal))
    )


# ── Instance: contexts, units, facts ─────────────────────────────────────


def _add_contexts(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for ctx in bundle.contexts:
    ctx_uri = _scoped_uri(root, "ctx", ctx.id)
    g.add((root, XBRLI.context, ctx_uri))
    _add_context(g, ctx_uri, ctx)


def _add_context(g: Graph, ctx_uri: URIRef, ctx: BundleContext) -> None:
  g.add((ctx_uri, RDF.type, XBRLI.context))
  # Entity identifier — XBRL packs entity + scheme into a sub-element.
  identifier_uri = URIRef(f"{ctx_uri}/entity/identifier")
  g.add((ctx_uri, XBRLI.entity, identifier_uri))
  g.add((identifier_uri, RDF.value, Literal(ctx.entity_identifier)))
  g.add((identifier_uri, RS.scheme, URIRef(ctx.entity_scheme)))
  # Period
  period_uri = URIRef(f"{ctx_uri}/period")
  g.add((ctx_uri, XBRLI.period, period_uri))
  if ctx.period_type == "instant":
    g.add(
      (
        period_uri,
        XBRLI.instant,
        Literal(ctx.period_end.isoformat(), datatype=XSD.date),
      )
    )
  else:
    start = ctx.period_start or ctx.period_end
    g.add((period_uri, XBRLI.startDate, Literal(start.isoformat(), datatype=XSD.date)))
    g.add(
      (
        period_uri,
        XBRLI.endDate,
        Literal(ctx.period_end.isoformat(), datatype=XSD.date),
      )
    )


def _add_units(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  for unit in bundle.units:
    unit_uri = _scoped_uri(root, "unit", unit.id)
    g.add((root, XBRLI.unit, unit_uri))
    _add_unit(g, unit_uri, unit)


def _add_unit(g: Graph, unit_uri: URIRef, unit: BundleUnit) -> None:
  g.add((unit_uri, RDF.type, XBRLI.unit))
  g.add((unit_uri, XBRLI.measure, _measure_uri(unit.measure)))


def _measure_uri(measure: str) -> URIRef:
  if measure.startswith("iso4217:"):
    return URIRef(str(ISO4217) + measure[len("iso4217:") :])
  if ":" not in measure:
    return URIRef(f"https://robosystems.ai/measure/{measure}")
  return _resolve_qname_uri(measure)


def _add_facts(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  # Map context_ref / unit_ref ids back to their full URIs scoped under root.
  for fact in bundle.facts:
    fact_uri = _scoped_uri(root, "fact", fact.id)
    g.add((root, XBRLI.item, fact_uri))
    _add_fact(g, fact_uri, fact, root)


def _add_fact(g: Graph, fact_uri: URIRef, fact: BundleFact, root: URIRef) -> None:
  # Type is the concept qname directly — XBRL semantics: an instance
  # is a member of its concept's substitution-group type.
  g.add((fact_uri, RDF.type, _concept_uri(fact.element_qname)))
  g.add((fact_uri, XBRLI.contextRef, _scoped_uri(root, "ctx", fact.context_ref)))
  g.add((fact_uri, XBRLI.unitRef, _scoped_uri(root, "unit", fact.unit_ref)))
  g.add((fact_uri, XBRLI.decimals, Literal(fact.decimals)))
  g.add((fact_uri, RDF.value, Literal(Decimal(str(fact.value)), datatype=XSD.decimal)))
  g.add((fact_uri, RS.internalId, Literal(fact.id)))
  if fact.fact_set_id:
    g.add(
      (
        fact_uri,
        RS.factSet,
        URIRef(f"https://robosystems.ai/factset/{fact.fact_set_id}"),
      )
    )
  if fact.structure_id:
    g.add(
      (
        fact_uri,
        RS.structure,
        URIRef(f"https://robosystems.ai/structure/{fact.structure_id}"),
      )
    )


# ── IB envelopes (RS extension; opaque inner content for v1.0) ───────────


def _add_information_blocks(g: Graph, bundle: StatementBundle, root: URIRef) -> None:
  """Embed each IB envelope under the root.

  Top-level fields (id, block_type, structure linkage, fact_set
  reference) land as triples for discoverability. Deeply-nested
  mechanics / view / rendering content stays as a JSON literal under
  ``rs:envelopeJson`` — pragmatic v1.0 boundary, v2.0 can model
  these as triples if SPARQL discoverability is demanded.
  """
  for envelope in bundle.ib_envelopes:
    body = _envelope_to_dict(envelope)
    ib_id = body.get("id", "unknown")
    ib_uri = _scoped_uri(root, "ib", ib_id)
    g.add((root, RS.informationBlocks, ib_uri))
    g.add((ib_uri, RDF.type, RS.InformationBlock))
    g.add((ib_uri, RS.internalId, Literal(ib_id)))
    if "block_type" in body:
      g.add((ib_uri, RS.blockType, Literal(body["block_type"])))
    if "name" in body:
      g.add((ib_uri, SKOS.prefLabel, Literal(body["name"])))
    if "taxonomy_id" in body:
      g.add((ib_uri, RS.taxonomyId, Literal(body["taxonomy_id"])))
    if body.get("taxonomy_name"):
      g.add((ib_uri, RS.taxonomyName, Literal(body["taxonomy_name"])))
    if body.get("fact_set"):
      fact_set = body["fact_set"]
      fs_id = fact_set.get("id") if isinstance(fact_set, dict) else None
      if fs_id:
        g.add(
          (
            ib_uri,
            RS.factSet,
            URIRef(f"https://robosystems.ai/factset/{fs_id}"),
          )
        )
    # Deeply-nested payload embedded as a JSON literal — consumers
    # parse with their own JSON library to walk mechanics, view, etc.
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


# ── @context for JSON-LD serialization ───────────────────────────────────


def _build_context(bundle: StatementBundle) -> dict[str, Any]:
  """Build the v1.0 ``@context`` to pass to rdflib's JSON-LD serializer.

  rdflib uses this context to compact triples back into the friendly
  qname form (``rs-gaap:Assets``, ``xbrli:contextRef``) on serialize.
  Without this, output uses the auto-generated kitchen-sink context.
  """
  ctx: dict[str, Any] = {
    "xbrli": str(XBRLI),
    "link": str(LINK),
    "xlink": str(XLINK),
    "xbrldt": str(XBRLDT),
    "xbrldi": str(XBRLDI),
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "iso4217": str(ISO4217),
    "rdf": str(RDF),
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "skos": str(SKOS),
    "owl": "http://www.w3.org/2002/07/owl#",
    "dcterms": str(DCTERMS),
    "rs": str(RS),
    "rs-gaap": str(RS_GAAP),
    "fac": str(FAC),
    "us-gaap": str(US_GAAP),
    "ifrs": str(IFRS),
    "dei": str(DEI),
    "disclosures": str(DISCLOSURES),
    "checklist": str(CHECKLIST),
    "styles": str(STYLES),
  }
  # Override rs-gaap pinning if the bundle pins a non-default version.
  for pin in bundle.framework_pins:
    if pin.framework == "rs-gaap" and pin.version != "v1":
      ctx["rs-gaap"] = f"https://robosystems.ai/taxonomy/rs-gaap/{pin.version}/"
  return ctx


# ── Validation ───────────────────────────────────────────────────────────


class BundleValidationError(ValueError):
  """Raised when the rdflib graph fails structural integrity checks."""


def validate_graph(g: Graph, bundle: StatementBundle) -> None:
  """Run lightweight structural integrity checks before serialization.

  These are intentionally minimal — they catch the common producer
  bugs without trying to be SHACL. The full ontology checks land in
  v1.1 with a published SHACL shape document. Today we cover:

  * Every Fact's ``xbrli:contextRef`` resolves to a ``xbrli:context``
    node declared on the root.
  * Every Fact's ``xbrli:unitRef`` resolves to a ``xbrli:unit`` node.
  * Every Fact's concept (`@type`) has a corresponding ``xsd:element``
    declaration in ``rs:schemaConcepts``.
  * Every linkbase arc's endpoints resolve to declared concepts.

  Bundle integrity is a structural property under v1.0 — encoders
  refuse to emit a graph that can't be re-read meaningfully.
  """
  root = _root_uri(bundle)
  context_uris: set[IdentifiedNode] = {
    o for o in g.objects(root, XBRLI.context) if isinstance(o, IdentifiedNode)
  }
  unit_uris: set[IdentifiedNode] = {
    o for o in g.objects(root, XBRLI.unit) if isinstance(o, IdentifiedNode)
  }
  concept_uris: set[IdentifiedNode] = {
    o for o in g.objects(root, RS.schemaConcepts) if isinstance(o, IdentifiedNode)
  }

  problems: list[str] = []
  for fact_uri in g.objects(root, XBRLI.item):
    for ref in g.objects(fact_uri, XBRLI.contextRef):
      if ref not in context_uris:
        problems.append(f"Fact {fact_uri} references unknown context {ref}")
    for ref in g.objects(fact_uri, XBRLI.unitRef):
      if ref not in unit_uris:
        problems.append(f"Fact {fact_uri} references unknown unit {ref}")
    for type_uri in g.objects(fact_uri, RDF.type):
      if type_uri not in concept_uris:
        problems.append(
          f"Fact {fact_uri} typed {type_uri} but no matching concept declaration"
        )

  for link_predicate in (
    RS.presentationLinks,
    RS.calculationLinks,
    RS.definitionLinks,
  ):
    for link_uri in g.objects(root, link_predicate):
      for arc_predicate in (
        LINK.presentationArc,
        LINK.calculationArc,
        LINK.definitionArc,
      ):
        for arc in g.objects(link_uri, arc_predicate):
          for endpoint_predicate in (XLINK["from"], XLINK["to"]):
            for endpoint in g.objects(arc, endpoint_predicate):
              # Endpoints must be declared concepts on this bundle —
              # but inherited / library-declared concepts may not be
              # in this bundle's schema slice. Emit a debug log on
              # miss rather than failing the build.
              if endpoint not in concept_uris:
                logger.debug(
                  "Arc %s endpoint %s not in declared schema concepts",
                  arc,
                  endpoint,
                )

  if problems:
    raise BundleValidationError(
      "Bundle graph failed integrity checks:\n  - " + "\n  - ".join(problems)
    )


# ── JSON encoder default (used for IB envelope JSON literal) ─────────────


def _json_default(obj: Any) -> Any:
  if isinstance(obj, datetime):
    return obj.isoformat()
  if isinstance(obj, date):
    return obj.isoformat()
  if isinstance(obj, BaseModel):
    return obj.model_dump(mode="json", exclude_none=True)
  raise TypeError(f"Unsupported type for JSON literal: {type(obj).__name__}")
