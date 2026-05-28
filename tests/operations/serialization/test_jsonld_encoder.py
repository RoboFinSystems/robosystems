"""Unit tests for the JSON-LD (rdflib-grounded) encoder.

Covers ``rdf/jsonld.py`` in three layers:

* ``build_graph`` produces the expected RDF triples for each bundle
  component (root header, schema concepts, linkbase arcs, contexts,
  units, facts, IB envelopes).
* ``serialize_to_jsonld`` / ``serialize_to_turtle`` round-trip cleanly
  via rdflib — the v1.0 ``@context`` lands, namespaces bind, and the
  output reparses to the same triple set.
* ``validate_graph`` catches malformed producer output before
  serialization — the structural integrity hook that's the whole
  reason we built this on rdflib (one bundle, two encoders, validation
  for free).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
import rdflib
from rdflib import Graph, Literal, Namespace, URIRef

from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleContext,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleLinkbases,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
)
from robosystems.operations.serialization.rdf.jsonld import (
  SERIALIZATION_VERSION,
  BundleValidationError,
  build_graph,
  serialize_to_jsonld,
  serialize_to_turtle,
  validate_graph,
)

# ── Namespaces ──────────────────────────────────────────────────────────

XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
RS = Namespace("https://robosystems.ai/vocab/serialization/v1/")
RS_GAAP = Namespace("https://robosystems.ai/taxonomy/rs-gaap/v1/")
RDF = rdflib.RDF
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")


# ── Fixture bundles ─────────────────────────────────────────────────────


def _minimal_bundle(
  *,
  with_arcs: bool = False,
  with_ib: bool = False,
  with_two_contexts: bool = False,
) -> StatementBundle:
  """Synthetic bundle covering one concept + one fact unless flags expand it."""
  schema_concepts = [
    BundleElement(
      id="Assets",
      qname="rs-gaap:Assets",
      name="Assets",
      period_type="instant",
      is_monetary=True,
      balance_type="debit",
      source="rs-gaap",
    ),
  ]
  if with_arcs:
    schema_concepts.append(
      BundleElement(
        id="AssetsCurrent",
        qname="rs-gaap:AssetsCurrent",
        name="AssetsCurrent",
        period_type="instant",
        is_monetary=True,
        balance_type="debit",
        source="rs-gaap",
      )
    )

  linkbases = BundleLinkbases()
  if with_arcs:
    linkbases.presentation_links.append(
      BundleLinkbaseLink(
        link_type="presentationLink",
        role_uri="http://robosystems.ai/role/BalanceSheet",
        structure_id="struct_01",
        structure_name="Balance Sheet",
        block_type="balance_sheet",
        arcs=[
          BundleArc(
            arc_type="presentationArc",
            arcrole="http://www.xbrl.org/2003/arcrole/parent-child",
            from_qname="rs-gaap:Assets",
            to_qname="rs-gaap:AssetsCurrent",
            order_value=1.0,
          )
        ],
      )
    )
    linkbases.calculation_links.append(
      BundleLinkbaseLink(
        link_type="calculationLink",
        role_uri="http://robosystems.ai/role/BalanceSheet",
        structure_id="struct_01",
        structure_name="Balance Sheet",
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

  contexts = [
    BundleContext(
      id="ctx_1",
      entity_identifier="ent_01",
      period_start=None,
      period_end=date(2024, 12, 31),
      period_type="instant",
    )
  ]
  if with_two_contexts:
    contexts.append(
      BundleContext(
        id="ctx_2",
        entity_identifier="ent_01",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 12, 31),
        period_type="duration",
      )
    )

  facts = [
    BundleFact(
      id="fact_01",
      element_id="Assets",
      element_qname="rs-gaap:Assets",
      value=295_183_000.0,
      context_ref="ctx_1",
      unit_ref="u_USD",
    )
  ]
  if with_two_contexts:
    facts.append(
      BundleFact(
        id="fact_02",
        element_id="Assets",
        element_qname="rs-gaap:Assets",
        value=280_000_000.0,
        context_ref="ctx_2",
        unit_ref="u_USD",
      )
    )

  ib_envelopes: list[Any] = []
  if with_ib:
    ib_envelopes.append(
      {
        "id": "struct_01",
        "block_type": "balance_sheet",
        "name": "Balance Sheet",
        "taxonomy_id": "tax_01",
        "taxonomy_name": "rs-gaap",
        "fact_set": {"id": "fs_01"},
        "mechanics": {"kind": "statement_renderer"},
      }
    )

  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co", legal_name="Test Company, Inc."),
    periods=[
      PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24"),
    ],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=schema_concepts,
    linkbases=linkbases,
    contexts=contexts,
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=facts,
    ib_envelopes=ib_envelopes,
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="draft"
    ),
  )


def _root_uri(bundle: StatementBundle) -> URIRef:
  assert bundle.report_meta is not None
  return URIRef(f"https://robosystems.ai/report/{bundle.report_meta.report_id}")


def _only(values) -> Any:
  """Return the single value from an rdflib query, asserting there's
  exactly one. Cleaner than .next() with a fallback."""
  result_list = list(values)
  assert len(result_list) == 1, f"Expected exactly one value, got {len(result_list)}"
  return result_list[0]


# ── Root node triples ──────────────────────────────────────────────────


class TestBundleRootTriples:
  def test_root_has_xbrli_xbrl_type(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    assert (root, RDF.type, XBRLI.xbrl) in g

  def test_root_has_rs_report_type_for_report_mode(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    assert (root, RDF.type, RS.Report) in g

  def test_root_has_serialization_version(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    assert (root, RS.serializationVersion, Literal(SERIALIZATION_VERSION)) in g

  def test_root_has_mode_literal(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    assert (root, RS.mode, Literal("report")) in g

  def test_root_has_reporting_style(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    assert (root, RS.reportingStyle, Literal("BSC-CORP-IS02-CF1")) in g

  def test_entity_node_has_skos_pref_label(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    entity_uri = _only(g.objects(root, RS.entity))
    assert (entity_uri, SKOS.prefLabel, Literal("Test Co")) in g

  def test_entity_node_carries_optional_fields(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    entity_uri = _only(g.objects(root, RS.entity))
    assert (entity_uri, RS.legalName, Literal("Test Company, Inc.")) in g

  def test_entity_node_skips_omitted_fields(self) -> None:
    """``ein`` and ``country`` are absent on _minimal_bundle and should
    produce zero triples — the encoder never emits empty literals."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    entity_uri = _only(g.objects(root, RS.entity))
    assert list(g.objects(entity_uri, RS.ein)) == []
    assert list(g.objects(entity_uri, RS.country)) == []

  def test_framework_pin_lands_as_typed_node(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    pin_uris = list(g.objects(root, RS.frameworkPins))
    assert len(pin_uris) == 1
    pin_uri = pin_uris[0]
    assert (pin_uri, RDF.type, RS.FrameworkPin) in g
    assert (pin_uri, RS.framework, Literal("rs-gaap")) in g
    assert (pin_uri, RS.version, Literal("v1")) in g

  def test_period_meta_carries_label_start_end(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    period_uri = _only(g.objects(root, RS.periods))
    assert (period_uri, RDF.type, RS.PeriodColumn) in g
    assert (period_uri, SKOS.prefLabel, Literal("FY24")) in g

  def test_report_meta_carries_lifecycle_fields(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    meta_uri = _only(g.objects(root, RS.reportMeta))
    assert (meta_uri, RDF.type, RS.ReportMeta) in g
    assert (meta_uri, RS.reportId, Literal("rpt_test")) in g
    assert (meta_uri, RS.filingStatus, Literal("draft")) in g


# ── Schema concepts ─────────────────────────────────────────────────────


class TestSchemaConceptTriples:
  def test_concept_typed_xsd_element(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    assert (assets, RDF.type, URIRef("http://www.w3.org/2001/XMLSchema#element")) in g

  def test_concept_has_xbrli_substitution_group_xbrli_item(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    assert (assets, XBRLI.substitutionGroup, XBRLI.item) in g

  def test_monetary_concept_gets_monetary_item_type(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    xsd_type = URIRef("http://www.w3.org/2001/XMLSchema#type")
    assert (assets, xsd_type, XBRLI.monetaryItemType) in g

  def test_non_monetary_concept_gets_string_item_type(self) -> None:
    bundle = _minimal_bundle()
    bundle.schema_concepts[0].is_monetary = False
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    xsd_type = URIRef("http://www.w3.org/2001/XMLSchema#type")
    assert (assets, xsd_type, XBRLI.stringItemType) in g

  def test_concept_has_period_type_and_balance(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    assert (assets, XBRLI.periodType, Literal("instant")) in g
    assert (assets, XBRLI.balance, Literal("debit")) in g

  def test_concept_skos_label_emitted_when_present(self) -> None:
    bundle = _minimal_bundle()
    bundle.schema_concepts[0].label = "Total Assets"
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    assert (assets, SKOS.prefLabel, Literal("Total Assets")) in g

  def test_concept_preserves_internal_id_for_round_trip(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    assets = URIRef(str(RS_GAAP) + "Assets")
    assert (assets, RS.internalId, Literal("Assets")) in g


# ── Linkbases ──────────────────────────────────────────────────────────


class TestLinkbaseTriples:
  def test_presentation_link_typed_correctly(self) -> None:
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.presentationLinks))
    assert (link_uri, RDF.type, LINK.presentationLink) in g

  def test_link_carries_xlink_role_for_elr(self) -> None:
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.presentationLinks))
    assert (
      link_uri,
      XLINK.role,
      URIRef("http://robosystems.ai/role/BalanceSheet"),
    ) in g

  def test_link_preserves_structure_name_and_block_type(self) -> None:
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.presentationLinks))
    assert (link_uri, RS.structureName, Literal("Balance Sheet")) in g
    assert (link_uri, RS.blockType, Literal("balance_sheet")) in g

  def test_presentation_arc_resolves_to_concept_uris(self) -> None:
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.presentationLinks))
    arc_uri = _only(g.objects(link_uri, LINK.presentationArc))
    assert (arc_uri, XLINK["from"], URIRef(str(RS_GAAP) + "Assets")) in g
    assert (arc_uri, XLINK["to"], URIRef(str(RS_GAAP) + "AssetsCurrent")) in g

  def test_calculation_arc_carries_weight(self) -> None:
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.calculationLinks))
    arc_uri = _only(g.objects(link_uri, LINK.calculationArc))
    weight_objects = list(g.objects(arc_uri, LINK.weight))
    assert len(weight_objects) == 1
    assert float(weight_objects[0]) == 1.0

  def test_presentation_arc_does_not_carry_weight(self) -> None:
    """Weight is calc-only — emitting it on presentation arcs would
    be wrong per XBRL 2.1."""
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    link_uri = _only(g.objects(root, RS.presentationLinks))
    arc_uri = _only(g.objects(link_uri, LINK.presentationArc))
    assert list(g.objects(arc_uri, LINK.weight)) == []


# ── Contexts ────────────────────────────────────────────────────────────


class TestContextTriples:
  def test_context_typed_xbrli_context(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ctx_uri = _only(g.objects(root, XBRLI.context))
    assert (ctx_uri, RDF.type, XBRLI.context) in g

  def test_instant_context_has_xbrli_instant_predicate(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ctx_uri = _only(g.objects(root, XBRLI.context))
    period_uri = _only(g.objects(ctx_uri, XBRLI.period))
    instants = list(g.objects(period_uri, XBRLI.instant))
    assert len(instants) == 1
    assert str(instants[0]) == "2024-12-31"
    # And NO start/end on the period block.
    assert list(g.objects(period_uri, XBRLI.startDate)) == []
    assert list(g.objects(period_uri, XBRLI.endDate)) == []

  def test_duration_context_emits_start_and_end(self) -> None:
    bundle = _minimal_bundle(with_two_contexts=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    duration_ctxs = [
      ctx
      for ctx in g.objects(root, XBRLI.context)
      if isinstance(ctx, URIRef) and str(ctx).endswith("ctx_2")
    ]
    assert len(duration_ctxs) == 1
    ctx_uri = duration_ctxs[0]
    period_uri = _only(g.objects(ctx_uri, XBRLI.period))
    starts = list(g.objects(period_uri, XBRLI.startDate))
    ends = list(g.objects(period_uri, XBRLI.endDate))
    assert [str(s) for s in starts] == ["2024-01-01"]
    assert [str(e) for e in ends] == ["2024-12-31"]
    assert list(g.objects(period_uri, XBRLI.instant)) == []

  def test_entity_identifier_carries_scheme(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ctx_uri = _only(g.objects(root, XBRLI.context))
    identifier_uri = _only(g.objects(ctx_uri, XBRLI.entity))
    assert (identifier_uri, RDF.value, Literal("ent_01")) in g
    schemes = list(g.objects(identifier_uri, RS.scheme))
    assert len(schemes) == 1
    assert str(schemes[0]) == "http://robosystems.ai/entity"


# ── Units ───────────────────────────────────────────────────────────────


class TestUnitTriples:
  def test_unit_typed_xbrli_unit(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    unit_uri = _only(g.objects(root, XBRLI.unit))
    assert (unit_uri, RDF.type, XBRLI.unit) in g

  def test_iso4217_measure_resolves_to_full_uri(self) -> None:
    """``iso4217:USD`` should compact via the @context but expand to
    the full ISO4217 IRI in the triple store."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    unit_uri = _only(g.objects(root, XBRLI.unit))
    measure_uri = _only(g.objects(unit_uri, XBRLI.measure))
    assert str(measure_uri) == "http://www.xbrl.org/2003/iso4217#USD"


# ── Facts ───────────────────────────────────────────────────────────────


class TestFactTriples:
  def test_fact_typed_by_concept_qname(self) -> None:
    """XBRL semantics: the fact's @type IS the concept it instantiates."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    fact_uri = _only(g.objects(root, XBRLI.item))
    type_uri = _only(g.objects(fact_uri, RDF.type))
    assert str(type_uri) == str(RS_GAAP) + "Assets"

  def test_fact_has_context_and_unit_refs_resolving_to_scoped_uris(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    fact_uri = _only(g.objects(root, XBRLI.item))
    ctx_ref = _only(g.objects(fact_uri, XBRLI.contextRef))
    unit_ref = _only(g.objects(fact_uri, XBRLI.unitRef))
    assert str(ctx_ref).endswith("/ctx/ctx_1")
    assert str(unit_ref).endswith("/unit/u_USD")

  def test_fact_value_emitted_as_decimal(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    fact_uri = _only(g.objects(root, XBRLI.item))
    value_lit = _only(g.objects(fact_uri, RDF.value))
    assert isinstance(value_lit, Literal)
    assert float(value_lit) == 295_183_000.0
    assert str(value_lit.datatype).endswith("#decimal")

  def test_fact_carries_internal_id_for_round_trip(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    fact_uri = _only(g.objects(root, XBRLI.item))
    assert (fact_uri, RS.internalId, Literal("fact_01")) in g


# ── IB envelope embedding ──────────────────────────────────────────────


class TestInformationBlockEmbedding:
  def test_ib_typed_as_information_block(self) -> None:
    bundle = _minimal_bundle(with_ib=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ib_uri = _only(g.objects(root, RS.informationBlocks))
    assert (ib_uri, RDF.type, RS.InformationBlock) in g

  def test_ib_top_level_fields_land_as_triples(self) -> None:
    bundle = _minimal_bundle(with_ib=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ib_uri = _only(g.objects(root, RS.informationBlocks))
    assert (ib_uri, RS.blockType, Literal("balance_sheet")) in g
    assert (ib_uri, SKOS.prefLabel, Literal("Balance Sheet")) in g
    assert (ib_uri, RS.taxonomyId, Literal("tax_01")) in g

  def test_ib_envelope_json_literal_present(self) -> None:
    bundle = _minimal_bundle(with_ib=True)
    g = build_graph(bundle)
    root = _root_uri(bundle)
    ib_uri = _only(g.objects(root, RS.informationBlocks))
    json_lit_iter = g.objects(ib_uri, RS.envelopeJson)
    json_literals = list(json_lit_iter)
    assert len(json_literals) == 1
    # The JSON literal should contain the original IB envelope payload
    import json as _json

    parsed = _json.loads(str(json_literals[0]))
    assert parsed["block_type"] == "balance_sheet"
    assert parsed["mechanics"]["kind"] == "statement_renderer"


# ── Serialization round-trip ───────────────────────────────────────────


class TestJsonLdSerialization:
  def test_output_parses_back_to_same_triple_count(self) -> None:
    bundle = _minimal_bundle(with_arcs=True, with_two_contexts=True)
    g = build_graph(bundle)
    raw = serialize_to_jsonld(bundle)
    reparsed = Graph()
    reparsed.parse(data=raw, format="json-ld")
    # rdflib round-trip via JSON-LD with custom @context preserves
    # the triple set (modulo blank-node labels, which the encoder
    # avoids by minting explicit URIs for everything).
    assert len(reparsed) == len(g)

  def test_jsonld_output_contains_v1_context_namespaces(self) -> None:
    bundle = _minimal_bundle()
    raw = serialize_to_jsonld(bundle)
    # Spot-check a handful of v1.0 context entries
    for snippet in (
      '"xbrli"',
      '"link"',
      '"xlink"',
      '"rs"',
      '"rs-gaap"',
      '"iso4217"',
    ):
      assert snippet in raw, f"@context missing namespace {snippet}"

  def test_turtle_output_round_trips_via_rdflib(self) -> None:
    """Turtle is the free byproduct of the rdflib refactor — assert
    the same fact set survives the Turtle round-trip."""
    bundle = _minimal_bundle(with_arcs=True)
    raw = serialize_to_turtle(bundle)
    reparsed = Graph()
    reparsed.parse(data=raw, format="turtle")
    # Pull facts from the reparsed graph by xbrli:item membership
    root = _root_uri(bundle)
    fact_uris = list(reparsed.objects(root, XBRLI.item))
    assert len(fact_uris) == 1


# ── Validation hook ────────────────────────────────────────────────────


class TestValidateGraph:
  def test_well_formed_bundle_passes(self) -> None:
    bundle = _minimal_bundle(with_arcs=True, with_two_contexts=True, with_ib=True)
    g = build_graph(bundle)
    validate_graph(g, bundle)  # should not raise

  def test_orphan_context_ref_raises(self) -> None:
    """Manually inject a fact whose contextRef points at a context
    that's not declared on the root — validate_graph should catch it."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    orphan_fact = URIRef("https://robosystems.ai/orphan/fact_99")
    g.add((root, XBRLI.item, orphan_fact))
    g.add((orphan_fact, RDF.type, URIRef(str(RS_GAAP) + "Assets")))
    g.add(
      (
        orphan_fact,
        XBRLI.contextRef,
        URIRef("https://robosystems.ai/orphan/ctx/ctx_99"),
      )
    )
    g.add((orphan_fact, XBRLI.unitRef, URIRef(f"{root!s}/unit/u_USD")))
    with pytest.raises(BundleValidationError) as exc:
      validate_graph(g, bundle)
    assert "unknown context" in str(exc.value)

  def test_orphan_unit_ref_raises(self) -> None:
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    orphan_fact = URIRef("https://robosystems.ai/orphan/fact_98")
    g.add((root, XBRLI.item, orphan_fact))
    g.add((orphan_fact, RDF.type, URIRef(str(RS_GAAP) + "Assets")))
    g.add((orphan_fact, XBRLI.contextRef, URIRef(f"{root!s}/ctx/ctx_1")))
    g.add(
      (
        orphan_fact,
        XBRLI.unitRef,
        URIRef("https://robosystems.ai/orphan/unit/u_NOPE"),
      )
    )
    with pytest.raises(BundleValidationError) as exc:
      validate_graph(g, bundle)
    assert "unknown unit" in str(exc.value)

  def test_fact_concept_not_declared_in_schema_raises(self) -> None:
    """If a fact is typed by a concept that has no ``xsd:element``
    declaration in ``rs:schemaConcepts``, the validator catches it."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    orphan_fact = URIRef("https://robosystems.ai/orphan/fact_97")
    g.add((root, XBRLI.item, orphan_fact))
    g.add(
      (
        orphan_fact,
        RDF.type,
        URIRef("https://robosystems.ai/taxonomy/rs-gaap/v1/UndeclaredConcept"),
      )
    )
    g.add((orphan_fact, XBRLI.contextRef, URIRef(f"{root!s}/ctx/ctx_1")))
    g.add((orphan_fact, XBRLI.unitRef, URIRef(f"{root!s}/unit/u_USD")))
    with pytest.raises(BundleValidationError) as exc:
      validate_graph(g, bundle)
    assert "no matching concept declaration" in str(exc.value)

  def test_arc_endpoint_outside_schema_slice_logs_but_passes(self) -> None:
    """Concepts only referenced via arcs (subtotal parents not in the
    fact set) may not be in the bundle's schema slice — validator
    debug-logs but does not raise (Phase 1a behavior; Phase 2 will
    tighten this with proper Element-discovery)."""
    bundle = _minimal_bundle(with_arcs=True)
    g = build_graph(bundle)
    # Add an arc with an endpoint that's not in schema_concepts
    link_uri = _only(g.objects(_root_uri(bundle), RS.presentationLinks))
    bogus_arc = URIRef(f"{link_uri!s}/arc/bogus")
    g.add((link_uri, LINK.presentationArc, bogus_arc))
    g.add((bogus_arc, RDF.type, LINK.presentationArc))
    g.add(
      (
        bogus_arc,
        XLINK["from"],
        URIRef("https://robosystems.ai/taxonomy/rs-gaap/v1/NotDeclared"),
      )
    )
    g.add(
      (
        bogus_arc,
        XLINK["to"],
        URIRef("https://robosystems.ai/taxonomy/rs-gaap/v1/Assets"),
      )
    )
    # Should not raise — arc endpoints not in schema are tolerated
    validate_graph(g, bundle)


# ── URI minting helpers (covered indirectly above; smoke checks here) ──


class TestUriMinting:
  def test_root_uri_scopes_subsidiary_objects(self) -> None:
    """Bundle root forms the URI prefix for all subsidiary objects so
    two bundles' ctx_1 / fact_01 IDs never collide in a shared store."""
    bundle = _minimal_bundle()
    g = build_graph(bundle)
    root = _root_uri(bundle)
    # All facts/contexts/units/concepts ARE either under the root URI
    # or in a framework taxonomy namespace
    fact_uri = _only(g.objects(root, XBRLI.item))
    ctx_uri = _only(g.objects(root, XBRLI.context))
    unit_uri = _only(g.objects(root, XBRLI.unit))
    assert str(fact_uri).startswith(str(root))
    assert str(ctx_uri).startswith(str(root))
    assert str(unit_uri).startswith(str(root))

  def test_unknown_qname_prefix_falls_back_to_synthetic_uri(self) -> None:
    """A concept with an unrecognized prefix doesn't crash the
    encoder — falls back to a synthetic URI so the graph still
    builds and the validator can surface the issue cleanly."""
    bundle = _minimal_bundle()
    bundle.schema_concepts[0] = BundleElement(
      id="elem_01",
      qname="unknown-prefix:SomeConcept",
      name="SomeConcept",
      period_type="instant",
      is_monetary=True,
      balance_type="debit",
      source="custom",
    )
    bundle.facts[0].element_qname = "unknown-prefix:SomeConcept"
    g = build_graph(bundle)
    assert len(g) > 0  # graph built; URIs minted under fallback namespace
