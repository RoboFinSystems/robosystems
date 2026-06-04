"""Guards for the rs-gaap-type-subtype 3-way linkbase split.

``rs-gaap-type-subtype`` was split into three single-purpose packages:

  * ``rs-gaap-type-subtype`` — general-special arcs only (definition linkbase),
  * ``rs-gaap-references``   — ASC citations (reference linkbase), attach-by-qname,
  * ``rs-gaap-labels``       — supplementary / total-role labels (label linkbase),
    attach-by-qname.

The references/labels packages define **no concepts** — they carry label /
reference content that attaches to the rs-gaap base concepts by qname, emitted
by the loader as ``reference_assignments`` / ``label_assignments`` and attached
in the seeder's cross-package arcs pass. These DB-free tests pin the linkbase
shape and the by-qname-attach contract; the byte-identical reseed (labels /
references / associations row counts unchanged) is verified end-to-end on
``just reset-local``.
"""

from __future__ import annotations

import json

import pytest
from rdflib import Graph, URIRef

from robosystems.arelle.context import CANONICAL_CONTEXT
from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import _extract_element, _is_concept
from robosystems.taxonomy.model import TaxonomyPackage

_PACKAGES = framework_root("rs-gaap") / "packages"


@pytest.fixture(scope="module")
def arcs() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-type-subtype" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def references() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-references" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def labels() -> TaxonomyPackage:
  return load_taxonomy_package(_PACKAGES / "rs-gaap-labels" / "v1" / "taxonomy.jsonld")


@pytest.mark.unit
class TestArcsPackage:
  def test_arcs_only_no_concepts(self, arcs: TaxonomyPackage) -> None:
    """The definition linkbase carries arcs (+ its classification ELR
    structures) and re-defines no concepts — base rs-gaap owns those now."""
    assert arcs.elements == []
    assert len(arcs.associations) > 1900
    assert all(a.association_type == "general-special" for a in arcs.associations)


@pytest.mark.unit
class TestReferenceLinkbase:
  def test_attaches_by_qname_defines_no_concepts(
    self, references: TaxonomyPackage
  ) -> None:
    assert references.elements == []
    assert references.label_assignments == []
    assert len(references.reference_assignments) > 3000
    # A reference linkbase only references concepts in the base namespace.
    assert all(
      ra.element_qname.startswith("rs-gaap:") for ra in references.reference_assignments
    )

  def test_citations_are_asc(self, references: TaxonomyPackage) -> None:
    assert all(ra.citation for ra in references.reference_assignments)
    assert any(ra.ref_type == "ASC" for ra in references.reference_assignments)


@pytest.mark.unit
class TestLabelLinkbase:
  def test_attaches_by_qname_defines_no_concepts(self, labels: TaxonomyPackage) -> None:
    assert labels.elements == []
    assert labels.reference_assignments == []
    assert len(labels.label_assignments) > 1000
    assert all(
      la.element_qname.startswith("rs-gaap:") for la in labels.label_assignments
    )

  def test_total_role_labels_present(self, labels: TaxonomyPackage) -> None:
    """The supplementary linkbase carries the ``total`` role labels that have
    no twin in the base (the redundant ``standard`` labels were deduped out)."""
    assert any(la.role == "total" for la in labels.label_assignments)


@pytest.mark.unit
class TestLoaderNonConceptExtraction:
  """A node with labels / references but no concept attributes (balance /
  periodType / elementType) must be read as a by-qname assignment, never as an
  element — otherwise it would upsert and clobber the owning package's concept
  attributes."""

  def _graph(self, node: dict) -> tuple[Graph, URIRef]:
    node = {"@id": "rs-gaap:Demo", **node}
    doc = {"@context": CANONICAL_CONTEXT, "@graph": [node]}
    g = Graph()
    g.parse(data=json.dumps(doc), format="json-ld")
    subj = next(s for s in g.subjects() if isinstance(s, URIRef))
    return g, subj

  def test_label_only_node_is_not_a_concept(self) -> None:
    g, subj = self._graph({"rdfs:label": [{"@value": "Demo", "@language": "en"}]})
    # No balance / periodType / elementType → not a concept; the loader routes
    # it to label_assignments rather than coercing it into an element.
    assert _is_concept(g, subj) is False

  def test_concept_node_still_extracts_inline(self) -> None:
    g, subj = self._graph(
      {
        "rs:elementType": "concept",
        "rdfs:label": [{"@value": "Demo", "@language": "en"}],
      }
    )
    assert _is_concept(g, subj) is True
    element = _extract_element(g, subj)
    assert element is not None
    assert any(lbl.role == "standard" and lbl.text == "Demo" for lbl in element.labels)


@pytest.mark.unit
def test_split_assignments_collapse_to_deterministic_seed_rows(
  arcs: TaxonomyPackage,
  references: TaxonomyPackage,
  labels: TaxonomyPackage,
) -> None:
  """The seed row identity is qname-keyed: ``uuid5(element_id:citation)`` for a
  reference, ``uuid5(element_id:role:language)`` for a label. So the assignment
  *emit list* may carry id-equivalent duplicates (a concept that repeats an ASC
  citation), and the seeder collapses them via ON CONFLICT DO NOTHING — exactly
  as the original inline form did. Assert the deduped (seed-id) sets are stable
  and that the arcs package contributes none. (The byte-identical-to-inline row
  count is verified end-to-end on reset-local.)"""
  ref_ids = {(ra.element_qname, ra.citation) for ra in references.reference_assignments}
  lbl_ids = {
    (la.element_qname, la.role, la.language) for la in labels.label_assignments
  }
  # Dedup may collapse the emit list, never expand it.
  assert 0 < len(ref_ids) <= len(references.reference_assignments)
  assert 0 < len(lbl_ids) <= len(labels.label_assignments)
  assert len(ref_ids) > 3000
  assert len(lbl_ids) > 1000
  # Arcs package contributes no labels/refs; the split is clean.
  assert arcs.label_assignments == []
  assert arcs.reference_assignments == []
