"""Content invariants over the committed demo artifacts (issue #1393).

SHACL (``test_sample_bundles_shacl.py``) checks their *shape*, which is why two
content drifts sat in the tree undetected: every committed holon carried zero
element labels, and tenant concepts had lost their labels in the flat JSON-LD.
These are the checks that fail on that class — pure Python over checked-in
JSON, no stack.

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

REPO_ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = sorted(REPO_ROOT.glob("examples/*/sample_output/*.jsonld"))
# One demo's two RDF forms, keyed by demo: (flat, holon).
PAIRS = sorted(
  (flat, flat.with_suffix("").with_suffix(".holon.jsonld"))
  for flat in ARTIFACTS
  if not flat.name.endswith(".holon.jsonld")
)

# The flat encoder's fallthrough for a prefix its static table lacks, which is
# every tenant's. Only the demos that author their own concepts hit it.
_UNMAPPED_PREFIX = "https://robosystems.ai/concept/"


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


def test_artifacts_exist() -> None:
  assert ARTIFACTS, "no artifacts under examples/*/sample_output/"
  assert PAIRS, "no flat/holon pairs found"
  for flat, holon in PAIRS:
    assert holon.exists(), f"{flat.name} has no holon beside it"


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


@pytest.mark.parametrize("pair", PAIRS, ids=[p[0].stem for p in PAIRS])
def test_both_forms_agree_on_every_shared_concept_label(
  pair: tuple[Path, Path],
) -> None:
  """Drift 2: tenant concepts kept their label in one form and not the other."""
  flat, holon = pair
  flat_labels, holon_labels = _labels(flat), _labels(holon)
  shared = set(flat_labels) & set(holon_labels)
  assert shared, f"{flat.stem}: the two forms declare no concept in common"
  disagree = {
    q: (flat_labels[q], holon_labels[q])
    for q in shared
    if flat_labels[q] != holon_labels[q]
  }
  assert not disagree, (
    f"{flat.stem}: the flat bundle and the holon give different labels for "
    f"{len(disagree)} concept(s) — regenerate this demo. "
    f"{dict(list(disagree.items())[:3])}"
  )


@pytest.mark.parametrize("pair", PAIRS, ids=[p[0].stem for p in PAIRS])
def test_both_forms_agree_on_the_reporting_style(pair: tuple[Path, Path]) -> None:
  """Drift 3: the holon claimed ``sec-as-filed`` for a tenant report.

  ``rs:reportingStyle`` sits on the ``rs:Report`` node of both forms under one
  IRI, so the two cannot disagree about it and both be right.
  """
  flat, holon = pair
  styles = {}
  for label, path in (("flat", flat), ("holon", holon)):
    reports = [n for n in _typed(path, "rs:Report") if "reportingStyle" in n]
    assert reports, f"{path.name}: no rs:Report node carries a reportingStyle"
    styles[label] = reports[0]["reportingStyle"]
  assert styles["flat"] == styles["holon"], (
    f"{flat.stem}: the flat bundle says reportingStyle={styles['flat']!r} and "
    f"the holon says {styles['holon']!r} — regenerate this demo"
  )


def _authors_own_concepts(flat: Path) -> bool:
  return any(n["@id"].startswith(_UNMAPPED_PREFIX) for n in _typed(flat, "rs:Element"))


@pytest.mark.parametrize(
  "pair",
  [
    pytest.param(
      pair,
      marks=pytest.mark.xfail(
        strict=True,
        reason="rdf/jsonld.py::_concept_uri mints a tenant concept as "
        "https://robosystems.ai/concept/<prefix>:<local> where the holon mints "
        "https://robosystems.ai/taxonomy/<tenant>/<local> — issue #1397",
      )
      if _authors_own_concepts(pair[0])
      else (),
    )
    for pair in PAIRS
  ],
  ids=[p[0].stem for p in PAIRS],
)
def test_both_forms_name_the_same_concepts(pair: tuple[Path, Path]) -> None:
  """The two forms are one Dataset, so a concept has one IRI in both."""
  flat, holon = pair
  only_flat = sorted(set(_labels(flat)) - set(_labels(holon)))
  only_holon = sorted(set(_labels(holon)) - set(_labels(flat)))
  assert not (only_flat or only_holon), (
    f"{flat.stem}: a concept is named differently by the two forms — "
    f"flat-only {only_flat[:3]}, holon-only {only_holon[:3]}"
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
