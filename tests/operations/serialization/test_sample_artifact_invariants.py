"""Content invariants over the committed demo artifacts (issue #1393).

Shape checks alone let two content drifts sit in the tree undetected: every
committed holon carried zero element labels, and tenant concepts had lost
their labels in one form but not the other. These are the checks that fail on
that class — pure Python over checked-in JSON, no stack. The two forms paired
here are the holon and the Tavi (the anchor stamped at publish).

Each failure names the artifact and the invariant, so the fix is "regenerate
that demo".
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from xbrlkit.deserialize import from_holon_report, from_tavi_report

REPO_ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = sorted(REPO_ROOT.glob("examples/*/sample_output/*.holon.jsonld"))
# One demo's two forms, keyed by demo: (tavi, holon).
PAIRS = sorted(
  (holon.with_name(holon.name.replace(".holon.jsonld", ".tavi.json")), holon)
  for holon in ARTIFACTS
)


def _nodes(path: Path) -> Iterator[dict[str, Any]]:
  """Every node in an artifact, flat or dataset-form."""
  doc = json.loads(path.read_text())
  for entry in doc.get("@graph", []):
    if isinstance(entry, dict) and isinstance(entry.get("@graph"), list):
      yield from entry["@graph"]  # a named graph
    elif isinstance(entry, dict):
      yield entry


def _typed(path: Path, rs_type: str) -> Iterator[dict[str, Any]]:
  for node in _nodes(path):
    declared = node.get("@type")
    types = declared if isinstance(declared, list) else [declared]
    if rs_type in types:
      yield node


def _labels(path: Path) -> dict[str, str]:
  return {
    n["@id"]: n["prefLabel"] for n in _typed(path, "rs:Element") if n.get("prefLabel")
  }


def _model_labels(path: Path) -> dict[str, str]:
  """``qname → standard label`` as xbrlkit reads the artifact."""
  text = path.read_text()
  model, _ = (from_tavi_report if path.suffix == ".json" else from_holon_report)(text)
  return {
    qname: label.value
    for qname, concept in model.concepts.items()
    for label in concept.labels
    if label.value and (label.role or "").endswith("/label")
  }


def test_artifacts_exist() -> None:
  assert ARTIFACTS, "no holons under examples/*/sample_output/"
  for tavi, holon in PAIRS:
    assert tavi.exists(), f"{holon.name} has no Tavi beside it"


@pytest.mark.parametrize("path", ARTIFACTS, ids=[p.name for p in ARTIFACTS])
def test_every_element_is_labelled(path: Path) -> None:
  """Drift 1: all five holons once carried zero element labels."""
  unlabelled = sorted(
    n["@id"] for n in _typed(path, "rs:Element") if not n.get("prefLabel")
  )
  assert not unlabelled, (
    f"{path.name}: {len(unlabelled)} rs:Element nodes carry no prefLabel, so a "
    f"reader sees a QName where a name belongs — regenerate this demo. "
    f"First few: {unlabelled[:5]}"
  )


@pytest.mark.parametrize("pair", PAIRS, ids=[p[1].stem for p in PAIRS])
def test_both_forms_agree_on_every_shared_concept_label(
  pair: tuple[Path, Path],
) -> None:
  """Drift 2: tenant concepts kept their label in one form and not the other."""
  tavi, holon = pair
  tavi_labels, holon_labels = _model_labels(tavi), _model_labels(holon)
  shared = set(tavi_labels) & set(holon_labels)
  assert shared, f"{holon.stem}: the two forms declare no concept in common"
  disagree = {
    q: (tavi_labels[q], holon_labels[q])
    for q in shared
    if tavi_labels[q] != holon_labels[q]
  }
  assert not disagree, (
    f"{holon.stem}: the Tavi and the holon give different labels for "
    f"{len(disagree)} concept(s) — regenerate this demo. "
    f"{dict(list(disagree.items())[:3])}"
  )


@pytest.mark.parametrize("path", ARTIFACTS, ids=[p.name for p in ARTIFACTS])
def test_a_tenant_holon_never_claims_sec_as_filed(path: Path) -> None:
  """Drift 3: the holon claimed ``sec-as-filed`` for a tenant report."""
  reports = [n for n in _typed(path, "rs:Report") if "reportingStyle" in n]
  assert reports, f"{path.name}: no rs:Report node carries a reportingStyle"
  assert reports[0]["reportingStyle"] != "sec-as-filed", (
    f"{path.name}: a tenant report claims the SEC as-filed style — regenerate this demo"
  )


@pytest.mark.parametrize("pair", PAIRS, ids=[p[1].stem for p in PAIRS])
def test_both_forms_name_the_same_concepts(pair: tuple[Path, Path]) -> None:
  """The two forms project one model, so they declare one concept set."""
  tavi, holon = pair
  only_tavi = sorted(set(_model_labels(tavi)) - set(_model_labels(holon)))
  only_holon = sorted(set(_model_labels(holon)) - set(_model_labels(tavi)))
  assert not (only_tavi or only_holon), (
    f"{holon.stem}: a concept is named differently by the two forms — "
    f"tavi-only {only_tavi[:3]}, holon-only {only_holon[:3]}"
  )


@pytest.mark.parametrize("path", ARTIFACTS, ids=[p.name for p in ARTIFACTS])
def test_every_arc_endpoint_is_declared(path: Path) -> None:
  """The failure mode of a holon partition that seeded its scene from facts."""
  declared = {n["@id"] for n in _typed(path, "rs:Element")}
  missing = sorted(
    {
      endpoint
      for arc in _typed(path, "rs:Association")
      for endpoint in (arc.get("from"), arc.get("to"))
      if endpoint and endpoint not in declared
    }
  )
  assert not missing, (
    f"{path.name}: {len(missing)} arc endpoint(s) are not declared as elements, "
    f"so a row renders unlabelled — regenerate this demo. First few: {missing[:5]}"
  )


@pytest.mark.parametrize("path", ARTIFACTS, ids=[p.name for p in ARTIFACTS])
def test_no_calculation_arc_is_emitted_twice(path: Path) -> None:
  """A summation cannot name the same child twice on one ELR (#1395)."""
  arcs = Counter(
    (arc.get("from"), arc.get("to"), arc.get("role"))
    for arc in _typed(path, "rs:Association")
    if arc.get("associationType") == "calculation"
  )
  duplicated = {k: v for k, v in arcs.items() if v > 1}
  assert not duplicated, (
    f"{path.name}: {len(duplicated)} calculation arc(s) appear more than once on "
    f"the same role, which doubles the subtotal they foot — regenerate this demo. "
    f"First: {list(duplicated)[:2]}"
  )
