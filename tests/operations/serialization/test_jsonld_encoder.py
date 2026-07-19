"""Unit tests for the JSON-LD encoder (``rdf/jsonld.py``) — v1 graph-native.

The bundle serializes to the canonical RoboSystems RDF ontology: facts are
``rs:Fact`` nodes referencing their aspects directly (``rs:element`` /
``rs:entity`` / ``rs:period`` / ``rs:unit``) — no XBRL ``context``. Concepts
are ``rs:Element``, arcs are reified ``rs:Association`` under ``rs:Structure``.
``validate_graph`` runs SHACL over the built graph.
"""

from __future__ import annotations

from datetime import date

import pytest
import rdflib
from rdflib import Graph, Literal, Namespace, URIRef

from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleLinkbases,
  BundlePeriod,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
)
from robosystems.operations.serialization.rdf.holon import (
  partition_report_graph,
  serialize_to_holon_jsonld,
)
from robosystems.operations.serialization.rdf.jsonld import (
  SERIALIZATION_VERSION,
  BundleValidationError,
  _structure_arrangement,
  build_graph,
  serialize_to_jsonld,
  serialize_to_turtle,
  validate_graph,
)

# ── Namespaces (canonical) ──────────────────────────────────────────────

RS = Namespace("https://robosystems.ai/vocab/")
XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")
RS_GAAP = Namespace("https://robosystems.ai/taxonomy/rs-gaap/v1/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
RDF = rdflib.RDF


# ── Fixture bundle ───────────────────────────────────────────────────────


def _bundle(*, with_arc: bool = True) -> StatementBundle:
  concepts = [
    BundleElement(
      id="Assets",
      qname="rs-gaap:Assets",
      name="Assets",
      label="Assets",
      period_type="instant",
      is_monetary=True,
      balance_type="debit",
      source="rs-gaap",
    ),
    BundleElement(
      id="AssetsCurrent",
      qname="rs-gaap:AssetsCurrent",
      name="AssetsCurrent",
      period_type="instant",
      is_monetary=True,
      balance_type="debit",
      source="rs-gaap",
    ),
  ]
  linkbases = BundleLinkbases()
  if with_arc:
    linkbases.calculation_links.append(
      BundleLinkbaseLink(
        link_type="calculationLink",
        role_uri="http://robosystems.ai/role/BS",
        structure_id="struct_01",
        structure_name="Balance Sheet",
        block_type="balance_sheet",
        arcs=[
          BundleArc(
            arc_type="calculationArc",
            arcrole="http://www.xbrl.org/2003/arcrole/summation-item",
            from_qname="rs-gaap:Assets",
            to_qname="rs-gaap:AssetsCurrent",
            weight=1.0,
            order_value=1.0,
          )
        ],
      )
    )
  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co", legal_name="Test Co, LLC"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=concepts,
    linkbases=linkbases,
    period_nodes=[
      BundlePeriod(id="p_1", period_end=date(2024, 12, 31), period_type="instant")
    ],
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=[
      BundleFact(
        id="fact_01",
        element_id="Assets",
        element_qname="rs-gaap:Assets",
        value=295_183_000.0,
        period_ref="p_1",
        unit_ref="u_USD",
        entity_ref="ent_01",
        structure_id="struct_01",
        fact_set_id="fs_01",
      )
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="filed"
    ),
  )


def _root(g: Graph) -> URIRef:
  return next(g.subjects(RDF.type, RS.Report))  # type: ignore[return-value]


# ── Root header ──────────────────────────────────────────────────────────


class TestRoot:
  def test_root_is_rs_report_with_version_1(self) -> None:
    g = build_graph(_bundle())
    root = _root(g)
    assert (root, RS.serializationVersion, Literal(SERIALIZATION_VERSION)) in g
    assert SERIALIZATION_VERSION == "1.0"
    assert (root, RS.mode, Literal("report")) in g
    assert (root, RS.reportingStyle, Literal("BSC-CORP-IS02-CF1")) in g

  def test_entity_node(self) -> None:
    g = build_graph(_bundle())
    entity = next(g.objects(_root(g), RS.entity))
    assert (entity, RDF.type, RS.Entity) in g
    assert (entity, SKOS.prefLabel, Literal("Test Co")) in g
    assert (entity, RS.legalName, Literal("Test Co, LLC")) in g


# ── Concepts ─────────────────────────────────────────────────────────────


class TestConcepts:
  def test_element_carries_xbrli_attributes(self) -> None:
    g = build_graph(_bundle())
    assets = RS_GAAP.Assets
    assert (assets, RDF.type, RS.Element) in g
    assert (assets, XBRLI.balance, Literal("debit")) in g
    assert (assets, XBRLI.periodType, Literal("instant")) in g
    assert (assets, SKOS.prefLabel, Literal("Assets")) in g


# ── Structures + reified associations ────────────────────────────────────


class TestStructures:
  def test_structure_and_association(self) -> None:
    g = build_graph(_bundle(with_arc=True))
    structure = next(g.subjects(RDF.type, RS.Structure))
    assert (structure, RS.blockType, Literal("balance_sheet")) in g
    assoc = next(g.objects(structure, RS.hasAssociation))
    assert (assoc, RDF.type, RS.Association) in g
    assert (assoc, XLINK["from"], RS_GAAP.Assets) in g
    assert (assoc, XLINK.to, RS_GAAP.AssetsCurrent) in g
    assert (assoc, RS.associationType, Literal("calculation")) in g
    assert (
      assoc,
      XLINK.arcrole,
      URIRef("http://www.xbrl.org/2003/arcrole/summation-item"),
    ) in g
    assert (assoc, LINK.weight, Literal("1.0", datatype=rdflib.XSD.decimal)) in g


# ── Logical naming: RollUp / RollForward / Hierarchy (§6, additive) ───────


class TestStructureArrangement:
  def test_arrangement_mapping(self) -> None:
    # No calc arcs → presentation hierarchy.
    assert _structure_arrangement(False, "balance_sheet") == RS.Hierarchy
    assert _structure_arrangement(False, None) == RS.Hierarchy
    # Calc arcs → roll-up, except equity which rolls forward.
    assert _structure_arrangement(True, "balance_sheet") == RS.RollUp
    assert _structure_arrangement(True, None) == RS.RollUp
    assert _structure_arrangement(True, "equity_statement") == RS.RollForward

  def test_calc_structure_typed_rollup_additively(self) -> None:
    g = build_graph(_bundle(with_arc=True))
    structure = next(g.subjects(RDF.type, RS.RollUp))
    # The logical type is added alongside — never replaces — rs:Structure.
    assert (structure, RDF.type, RS.Structure) in g
    # The legible name is promoted to skos:prefLabel; the UUID stays internal.
    assert (structure, SKOS.prefLabel, Literal("Balance Sheet")) in g
    assert (structure, RS.internalId, Literal("struct_01")) in g

  def test_calc_arc_typed_rollup_relationship_additively(self) -> None:
    g = build_graph(_bundle(with_arc=True))
    arc = next(g.subjects(RDF.type, RS.RollUpRelationship))
    # Still a first-class rs:Association (AssociationShape still applies).
    assert (arc, RDF.type, RS.Association) in g
    assert (arc, RS.associationType, Literal("calculation")) in g

  def test_enrichment_keeps_shacl_conformance(self) -> None:
    # Additive types + skos:prefLabel don't trip any positive/negative shape.
    validate_graph(build_graph(_bundle(with_arc=True)), _bundle(with_arc=True))


# ── Periods / units (aspect nodes) ───────────────────────────────────────


class TestAspectNodes:
  def test_period_node_is_instant(self) -> None:
    g = build_graph(_bundle())
    period = next(g.subjects(RDF.type, RS.Period))
    assert (period, XBRLI.periodType, Literal("instant")) in g
    assert (
      period,
      XBRLI.instant,
      Literal("2024-12-31", datatype=rdflib.XSD.date),
    ) in g
    # The collapse: no XBRL context anywhere in the graph.
    assert not list(g.subjects(RDF.type, XBRLI.context))

  def test_unit_node(self) -> None:
    g = build_graph(_bundle())
    unit = next(g.subjects(RDF.type, RS.Unit))
    assert (unit, XBRLI.measure, URIRef("http://www.xbrl.org/2003/iso4217#USD")) in g


# ── Facts (aspects referenced directly) ──────────────────────────────────


class TestFacts:
  def test_fact_references_aspects_directly(self) -> None:
    g = build_graph(_bundle())
    fact = next(g.subjects(RDF.type, RS.Fact))
    assert (fact, RS.element, RS_GAAP.Assets) in g
    # entity / period / unit are IRI references to the aspect nodes
    assert next(g.objects(fact, RS.entity)) is not None
    assert next(g.objects(fact, RS.period)) is not None
    assert next(g.objects(fact, RS.unit)) is not None
    assert (
      fact,
      RS.numericValue,
      Literal("295183000.0", datatype=rdflib.XSD.decimal),
    ) in g
    # No XBRL contextRef on the fact — the collapse.
    assert not list(g.objects(fact, XBRLI.contextRef))


# ── Serialize round-trip ─────────────────────────────────────────────────


class TestSerialize:
  def test_jsonld_reparses_to_same_triples(self) -> None:
    bundle = _bundle()
    doc = serialize_to_jsonld(bundle)
    reparsed = Graph().parse(data=doc, format="json-ld")
    original = build_graph(bundle)
    assert len(reparsed) == len(original)

  def test_turtle_serializes(self) -> None:
    ttl = serialize_to_turtle(_bundle())
    assert "rs:Fact" in ttl or "Fact" in ttl


# ── SHACL validation ─────────────────────────────────────────────────────


class TestValidation:
  def test_good_bundle_validates(self) -> None:
    g = build_graph(_bundle())
    validate_graph(g, _bundle())  # no raise

  def test_fact_missing_element_is_rejected(self) -> None:
    g = build_graph(_bundle())
    root = _root(g)
    bad = URIRef(f"{root!s}/fact/orphan")
    g.add((bad, RDF.type, RS.Fact))
    g.add((bad, RS.period, URIRef(f"{root!s}/period/p_1")))
    # missing rs:element → FactShape minCount violation
    with pytest.raises(BundleValidationError):
      validate_graph(g, _bundle())

  def test_banned_xbrl_context_ref_is_rejected(self) -> None:
    """The negative shape: re-introducing XBRL context syntax fails."""
    g = build_graph(_bundle())
    fact = next(g.subjects(RDF.type, RS.Fact))
    g.add((fact, XBRLI.contextRef, Literal("ctx_1")))
    with pytest.raises(BundleValidationError):
      validate_graph(g, _bundle())


# ── Holon (dataset-form JSON-LD, named graphs) ───────────────────────────


class TestHolon:
  def _named_graphs(self, holon: str) -> dict[str, list[dict]]:
    """{suffix: node-list} for each named graph in a dataset-form JSON-LD doc."""
    import json

    doc = json.loads(holon)
    out: dict[str, list[dict]] = {}
    for node in doc.get("@graph", []):
      if isinstance(node, dict) and "@graph" in node:
        out[str(node["@id"]).rsplit("#", 1)[-1]] = node["@graph"]
    return out

  def test_partition_places_facts_and_calc_arcs(self) -> None:
    parts = partition_report_graph(build_graph(_bundle(with_arc=True)))
    assert set(parts) == {"scene", "boundary", "projection"}
    # The fact lands in scene; the calculation arc in boundary.
    assert list(parts["scene"].subjects(RDF.type, RS.Fact))
    assert list(parts["boundary"].subjects(RDF.type, RS.Association))
    # Lineage is never emitted — a report is an aggregation of the books.
    assert "lineage" not in parts

  def test_serializes_dataset_form_with_three_named_graphs(self) -> None:
    holon = serialize_to_holon_jsonld(_bundle(with_arc=True))
    named = self._named_graphs(holon)
    assert set(named) == {"scene", "boundary", "projection"}
    assert named["scene"], "scene graph is non-empty"

  def test_holon_round_trips_losslessly(self) -> None:
    from rdflib import Dataset

    holon = serialize_to_holon_jsonld(_bundle(with_arc=True))
    src = partition_report_graph(build_graph(_bundle(with_arc=True)))
    ds = Dataset()
    ds.parse(data=holon, format="json-ld")
    rt = {
      str(c.identifier).rsplit("#", 1)[-1]: len(list(c))
      for c in ds.graphs()
      if str(c.identifier) != "urn:x-rdflib:default" and len(list(c))
    }
    assert rt == {k: len(v) for k, v in src.items()}


# ── Non-numeric (text-block) fact arm ────────────────────────────────────


def _text_fact() -> BundleFact:
  return BundleFact(
    id="fact_txt",
    element_id="PolicyTB",
    element_qname="driftline:InventoryPolicyTextBlock",
    value=None,
    text_value="# Inventory Policy\n\nFIFO, lower of cost or NRV.",
    fact_type="Nonnumeric",
    content_type="text/markdown",
    period_ref="p_1",
    unit_ref=None,
    entity_ref="ent_01",
    structure_id="struct_note",
    fact_set_id="fs_note",
  )


class TestNonnumericFacts:
  def test_text_fact_emits_string_value_no_numeric_no_unit(self) -> None:
    bundle = _bundle()
    bundle.facts.append(_text_fact())
    g = build_graph(bundle)
    uri = URIRef(f"{_root(g)}/fact/fact_txt")
    # rs:stringValue is the predicate report-components reads — NOT rs:value.
    assert (uri, RS.stringValue, None) in g
    text_literal = next(g.objects(uri, RS.stringValue))
    assert "FIFO" in str(text_literal)
    assert (uri, RS.factType, Literal("Nonnumeric")) in g
    assert (uri, RS.contentType, Literal("text/markdown")) in g
    assert (uri, RS.numericValue, None) not in g
    assert (uri, RS.unit, None) not in g
    assert (uri, RS.decimals, None) not in g

  def test_text_block_concept_declares_item_type(self) -> None:
    bundle = _bundle()
    bundle.schema_concepts.append(
      BundleElement(
        id="PolicyTB",
        qname="driftline:InventoryPolicyTextBlock",
        name="InventoryPolicyTextBlock",
        period_type="duration",
        is_monetary=False,
        source="native",
        item_type="textBlock",
      )
    )
    g = build_graph(bundle)
    uri = next(g.subjects(RS.itemType, Literal("textBlock")))
    assert "InventoryPolicyTextBlock" in str(uri)

  def test_numeric_fact_arm_unchanged(self) -> None:
    bundle = _bundle()
    bundle.facts.append(_text_fact())
    g = build_graph(bundle)
    uri = URIRef(f"{_root(g)}/fact/fact_01")
    assert (uri, RS.numericValue, None) in g
    assert (uri, RS.unit, None) in g
    assert (uri, RS.factType, Literal("Numeric")) in g

  def test_text_fact_bundle_keeps_shacl_conformance(self) -> None:
    bundle = _bundle()
    bundle.facts.append(_text_fact())
    # Raises BundleValidationError on non-conformance.
    validate_graph(build_graph(bundle), bundle)
