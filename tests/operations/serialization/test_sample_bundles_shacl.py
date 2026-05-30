"""SHACL conformance of the committed demo sample bundles.

The demos' ``sample_output/*.jsonld`` are committed reference artifacts. This
pins that they conform to the published RoboSystems RDF ontology
(``frameworks/ontology/v1/shapes.ttl``) — the positive instance shapes *and*
the negative shapes that ban the retired dialects. If a future encoder change
(or a hand-edit of a sample) drifts the shape, this fails loudly.

It's the same validation ``serialize_to_jsonld`` runs at publish time, applied
to the checked-in evidence so the repo can't carry a non-conformant sample.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import rdflib
from pyshacl import validate

REPO_ROOT = Path(__file__).resolve().parents[3]
SHAPES_PATH = REPO_ROOT / "frameworks" / "ontology" / "v1" / "shapes.ttl"
SAMPLE_BUNDLES = sorted(REPO_ROOT.glob("examples/*/sample_output/*.jsonld"))


@pytest.fixture(scope="module")
def shapes() -> rdflib.Graph:
  return rdflib.Graph().parse(str(SHAPES_PATH), format="turtle")


def test_sample_bundles_exist() -> None:
  """Guard against the glob silently matching nothing."""
  assert SAMPLE_BUNDLES, "no sample bundles found under examples/*/sample_output/"


@pytest.mark.parametrize(
  "bundle_path", SAMPLE_BUNDLES, ids=[p.name for p in SAMPLE_BUNDLES]
)
def test_sample_bundle_conforms_to_ontology(
  bundle_path: Path, shapes: rdflib.Graph
) -> None:
  graph = rdflib.Graph().parse(str(bundle_path), format="json-ld")
  conforms, _, report = validate(graph, shacl_graph=shapes, inference="none")
  assert conforms, f"{bundle_path.name} violates the ontology shapes:\n{report}"
