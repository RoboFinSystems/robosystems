"""Smoke tests for the Arelle → rdflib → JSON-LD pipeline.

These tests avoid live XBRL fetches by relying on the committed seed
artifacts at `robosystems/taxonomy/seeds/`. They exercise the
serializer/loader round-trip and the TaxonomyPackage + library_writer
shape, but not the extractor (which needs a live ModelXbrl).

Extractor unit tests are Phase 1 work — they require either a mocked
Arelle model or a lightweight fixture package.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from rdflib import Graph

from robosystems.arelle import CANONICAL_CONTEXT, serialize_jsonld
from robosystems.arelle.context import context_document
from robosystems.taxonomy.loaders import load_taxonomy_package
from robosystems.taxonomy.model import (
  AssociationSpec,
  ElementSpec,
  TaxonomyPackage,
)

SEEDS_DIR = Path(__file__).parent.parent.parent / "robosystems" / "taxonomy" / "seeds"

FAC_SEED = SEEDS_DIR / "fac" / "v1" / "taxonomy.jsonld"
FAC_TO_RS_GAAP_SEED = SEEDS_DIR / "fac-to-rs-gaap" / "v1" / "taxonomy.jsonld"


@pytest.fixture(scope="module")
def fac_package() -> TaxonomyPackage:
  if not FAC_SEED.exists():
    pytest.skip(f"Seed not built: {FAC_SEED}")
  return load_taxonomy_package(FAC_SEED)


class TestCanonicalContext:
  def test_context_document_has_context_key(self) -> None:
    doc = context_document()
    assert "@context" in doc
    assert doc["@context"] is CANONICAL_CONTEXT

  def test_core_prefixes_present(self) -> None:
    required = {"rdfs", "skos", "owl", "xbrli", "fac", "us-gaap", "rs"}
    missing = required - set(CANONICAL_CONTEXT.keys())
    assert not missing, f"Context missing required prefixes: {missing}"

  def test_xbrl_namespaces_use_hash_separator(self) -> None:
    # XBRL concept IRIs use # between namespace and local name.
    # Prefixes must reflect that so compaction works.
    for prefix in ("fac", "us-gaap-2017"):
      val = CANONICAL_CONTEXT[prefix]
      assert isinstance(val, str), f"{prefix} should map to a string IRI"
      assert val.endswith(("#", "/")), f"{prefix} should end with # or /"


class TestSerializer:
  def test_empty_graph_produces_valid_jsonld(self) -> None:
    graph = Graph()
    output = serialize_jsonld(graph, standard="test", version="v1")
    doc = json.loads(output)
    assert "@context" in doc
    assert "@graph" in doc
    assert doc["standard"] == "test"
    assert doc["version"] == "v1"


class TestFacRoundTrip:
  def test_loads_fac_concepts(self, fac_package: TaxonomyPackage) -> None:
    """Core FAC concepts should all be present."""
    required = {"fac:Assets", "fac:Liabilities", "fac:Equity", "fac:Revenues"}
    present = {e.qname for e in fac_package.elements}
    missing = required - present
    assert not missing, f"Missing FAC elements: {missing}"

  def test_has_efs_classification_assignments(
    self, fac_package: TaxonomyPackage
  ) -> None:
    # FAC concepts carry rs:classifiedAs arcs pointing at
    # elementsOfFinancialStatements identifiers.
    efs_assignments = [
      a
      for a in fac_package.classification_assignments
      if a.category == "elementsOfFinancialStatements"
    ]
    assert len(efs_assignments) >= 80, (
      f"Expected >= 80 EFS assignments on FAC, got {len(efs_assignments)}"
    )

  def test_package_metadata(self, fac_package: TaxonomyPackage) -> None:
    assert fac_package.standard == "fac"
    assert fac_package.version == "v1"
    assert fac_package.is_shared is True


class TestFacMappingRoundTrip:
  """Equivalence arcs live in the `fac-to-rs-gaap/v1` mapping seed, not in
  the FAC concept seed — the architecture deliberately separates concept
  definitions from cross-taxonomy bridges (see `taxonomy-library.md`).
  """

  def test_has_equivalence_arcs(self) -> None:
    if not FAC_TO_RS_GAAP_SEED.exists():
      pytest.skip(f"Seed not built: {FAC_TO_RS_GAAP_SEED}")
    pkg = load_taxonomy_package(FAC_TO_RS_GAAP_SEED)
    equiv = [a for a in pkg.associations if a.association_type == "equivalence"]
    # At least 200 unique equivalence arcs (FAC → rs-gaap); the seed
    # description claims ~221 so 200 is a conservative floor.
    assert len(equiv) >= 200, f"Expected >= 200 equivalence arcs, got {len(equiv)}"

  def test_cost_of_revenue_collapses_rs_gaap_variants(self) -> None:
    """fac:CostOfRevenue should collapse several rs-gaap variants.

    fac-to-rs-gaap's equivalence arcs point at our namespace (not
    us-gaap-2017). The library is fully our own; external us-gaap lives
    in optional interop seeds.
    """
    if not FAC_TO_RS_GAAP_SEED.exists():
      pytest.skip(f"Seed not built: {FAC_TO_RS_GAAP_SEED}")
    pkg = load_taxonomy_package(FAC_TO_RS_GAAP_SEED)
    cost_of_rev_targets = {
      a.to_qname
      for a in pkg.associations
      if a.association_type == "equivalence" and a.from_qname == "fac:CostOfRevenue"
    }
    expected_present = {
      "rs-gaap:CostOfGoodsSold",
      "rs-gaap:CostOfGoodsAndServicesSold",
      "rs-gaap:CostOfServices",
    }
    missing = expected_present - cost_of_rev_targets
    assert not missing, (
      f"fac:CostOfRevenue is missing expected rs-gaap variants: {missing}"
    )


class TestPackageShape:
  def test_element_spec_required_fields(self) -> None:
    el = ElementSpec(
      qname="fac:Assets",
      namespace="fac",
      namespace_uri="http://xbrlsite.com/fac#",
      name="Assets",
      classification="asset",
      balance_type="debit",
      period_type="instant",
      source="fac",
    )
    assert el.is_abstract is False
    assert el.labels == []
    assert el.references == []

  def test_association_spec_required_fields(self) -> None:
    assoc = AssociationSpec(
      from_qname="fac:CostOfRevenue",
      to_qname="us-gaap-2017:CostOfGoodsSold",
      association_type="equivalence",
      arcrole="http://xbrlsite.azurewebsites.net/2016/conceptual-model/arcrole/class-equivalentClass",
    )
    assert assoc.role is None
    assert assoc.weight is None
