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
from collections import Counter
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

SFAC6_SEED = SEEDS_DIR / "sfac6" / "v1" / "taxonomy.jsonld"
FAC_SEED = SEEDS_DIR / "fac" / "v1" / "taxonomy.jsonld"
FAC_TO_RS_GAAP_SEED = SEEDS_DIR / "fac-to-rs-gaap" / "v1" / "taxonomy.jsonld"


@pytest.fixture(scope="module")
def sfac6_package() -> TaxonomyPackage:
  if not SFAC6_SEED.exists():
    pytest.skip(f"Seed not built: {SFAC6_SEED}")
  return load_taxonomy_package(SFAC6_SEED)


class TestCanonicalContext:
  def test_context_document_has_context_key(self) -> None:
    doc = context_document()
    assert "@context" in doc
    assert doc["@context"] is CANONICAL_CONTEXT

  def test_core_prefixes_present(self) -> None:
    required = {"rdfs", "skos", "owl", "xbrli", "sfac6", "fac", "us-gaap", "rs"}
    missing = required - set(CANONICAL_CONTEXT.keys())
    assert not missing, f"Context missing required prefixes: {missing}"

  def test_xbrl_namespaces_use_hash_separator(self) -> None:
    # XBRL concept IRIs use # between namespace and local name.
    # Prefixes must reflect that so compaction works.
    for prefix in ("sfac6", "fac", "us-gaap-2017"):
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


class TestSfac6RoundTrip:
  def test_loads_all_ten_sfac6_elements(self, sfac6_package: TaxonomyPackage) -> None:
    """The 10 FASB SFAC 6 elements must all be present."""
    required = {
      "sfac6:Assets",
      "sfac6:Liabilities",
      "sfac6:Equity",
      "sfac6:InvestmentsByOwners",
      "sfac6:DistributionsToOwners",
      "sfac6:ComprehensiveIncome",
      "sfac6:Revenues",
      "sfac6:Expenses",
      "sfac6:Gains",
      "sfac6:Losses",
    }
    present = {e.qname for e in sfac6_package.elements}
    missing = required - present
    assert not missing, f"Missing SFAC 6 elements: {missing}"

  def test_assets_has_label_and_documentation(
    self, sfac6_package: TaxonomyPackage
  ) -> None:
    assets = next(
      (e for e in sfac6_package.elements if e.qname == "sfac6:Assets"), None
    )
    assert assets is not None
    roles = {lab.role for lab in assets.labels}
    assert "standard" in roles
    assert "documentation" in roles
    doc_label = next(lab for lab in assets.labels if lab.role == "documentation")
    assert "probable future economic" in doc_label.text.lower()

  def test_has_structural_associations(self, sfac6_package: TaxonomyPackage) -> None:
    counts = Counter(a.association_type for a in sfac6_package.associations)
    # SFAC 6 package includes parent-child presentation + general-special +
    # calculation arcs for its BS/IS/Equity hypercubes.
    assert counts["presentation"] > 0
    assert counts["calculation"] > 0

  def test_package_metadata(self, sfac6_package: TaxonomyPackage) -> None:
    assert sfac6_package.standard == "sfac6"
    assert sfac6_package.version == "v1"
    assert sfac6_package.is_shared is True


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
      qname="sfac6:Assets",
      namespace="sfac6",
      namespace_uri="http://xbrlsite.com/seattlemethod/sfac6#",
      name="Assets",
      classification="asset",
      balance_type="debit",
      period_type="instant",
      source="sfac6",
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
