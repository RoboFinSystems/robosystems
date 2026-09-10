"""The holon names what the flat JSON-LD names, the same way.

The two are projections of one bundle — the flat JSON-LD stamped at publish,
the holon derived on download through xbrlkit — and a consumer holding both
must be able to join them on IRIs: the report, its periods and units, every
fact, every structure and association, the Information Blocks and their fact
sets. These pin that identity with every fact pinned to a structure, the way
a published report's are. The only arcs the holon lacks are the definition
arcs the bridge does not carry (``model.py`` says why).
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

import pytest

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.rdf.holon import serialize_to_holon_jsonld
from robosystems.operations.serialization.rdf.jsonld import serialize_to_jsonld
from tests.operations.serialization.test_model_bridge import _bundle


def _pinned_bundle() -> StatementBundle:
  """The bridge fixture with each fact pinned to the structure that shows its
  concept, and a fact set per structure — a published report's shape."""
  bundle = _bundle()
  concept_to_structure: dict[str, str] = {}
  for link in bundle.linkbases.presentation_links:
    for arc in link.arcs:
      concept_to_structure.setdefault(arc.from_qname, link.structure_id)
      concept_to_structure.setdefault(arc.to_qname, link.structure_id)
  first = bundle.linkbases.presentation_links[0].structure_id
  facts = []
  for fact in bundle.facts:
    structure_id = concept_to_structure.get(fact.element_qname, first)
    facts.append(
      fact.model_copy(
        update={"structure_id": structure_id, "fact_set_id": f"fs_{structure_id}"}
      )
    )
  return bundle.model_copy(update={"facts": facts})


def _types(node: dict[str, Any]) -> set[str]:
  types = node.get("@type", [])
  if isinstance(types, str):
    types = [types]
  return {t.rsplit(":", 1)[-1] for t in types}


def _by_type(nodes: list[dict[str, Any]]) -> dict[str, set[str]]:
  out: dict[str, set[str]] = defaultdict(set)
  for node in nodes:
    for t in _types(node):
      out[t].add(node["@id"])
  return out


@pytest.fixture(scope="module")
def projections() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  bundle = _pinned_bundle()
  flat = json.loads(serialize_to_jsonld(bundle))["@graph"]
  holon = json.loads(serialize_to_holon_jsonld(bundle))
  return flat, [n for graph in holon["@graph"] for n in graph["@graph"]]


@pytest.mark.parametrize(
  "kind", ["Report", "Entity", "Period", "Unit", "Fact", "Structure"]
)
def test_shared_nodes_have_identical_iris(projections, kind: str) -> None:
  flat, holon = projections
  assert _by_type(flat)[kind] == _by_type(holon)[kind]


def test_arcs_are_identical_but_for_the_definition_arcs(projections) -> None:
  flat, holon = projections
  flat_arcs, holon_arcs = _by_type(flat)["Association"], _by_type(holon)["Association"]
  assert holon_arcs <= flat_arcs
  assert all("/definition/" in arc for arc in flat_arcs - holon_arcs)
  assert holon_arcs, "the fixture carries presentation and calculation arcs"


def test_each_fact_links_to_the_same_structure_and_fact_set(projections) -> None:
  flat, holon = projections
  in_flat = {n["@id"]: n for n in flat if n.get("@type") == "rs:Fact"}
  in_holon = {n["@id"]: n for n in holon if n.get("@type") == "rs:Fact"}
  assert in_flat
  for iri, fact in in_flat.items():
    assert in_holon[iri]["structure"] == fact["structure"]
    assert in_holon[iri]["factSet"] == fact["factSet"]


def test_information_blocks_are_named_by_their_structure(projections) -> None:
  """The flat bundle names an IB by its structure id (``ib/<structure_id>``)
  and links it to ``factset/<its fact set>``; the holon's IBs must match."""
  flat, holon = projections
  root = next(n["@id"] for n in flat if "Report" in _types(n))
  structures = {n["@id"].rsplit("/", 1)[-1] for n in holon if "Structure" in _types(n)}
  blocks = [n for n in holon if n.get("@type") == "rs:InformationBlock"]
  assert blocks
  for block in blocks:
    structure_id = block["@id"].rsplit("/", 1)[-1]
    assert structure_id in structures
    assert block["@id"] == f"{root}/ib/{structure_id}"
    assert block["structure"] == f"{root}/structure/{structure_id}"
    assert block["factSet"] == f"https://robosystems.ai/factset/fs_{structure_id}"
