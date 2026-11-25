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
# One demo's two forms, keyed by demo: (tavi, holon). The lineage holon is the
# same holon with its ``#lineage`` graph added, so it pairs with the same Tavi.
PAIRS = sorted(
  (
    holon.with_name(
      holon.name.replace(".lineage.holon.jsonld", ".holon.jsonld").replace(
        ".holon.jsonld", ".tavi.json"
      )
    ),
    holon,
  )
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
def test_every_holon_meets_the_format_and_its_report_holds(path: Path) -> None:
  """Each named graph against its own shapes, the graphs together, and the
  report read back: whole, footing, balanced. The external check on the
  primary serialization, as Arelle is on the XBRL 2.1 package."""
  from xbrlkit.holon import render, validate_holon_file

  result = validate_holon_file(path)
  assert result.ok, f"{path.name}:\n{render(result)}"
  expected = {"scene", "projection", "boundary"}
  if ".lineage." in path.name:
    expected.add("lineage")
  present = {name for name, graph in result.graphs.items() if graph.present}
  assert present == expected, f"{path.name} carries {sorted(present)}"


PUBLISHED = [p for p in ARTIFACTS if ".lineage." not in p.name]


@pytest.mark.parametrize("path", PUBLISHED, ids=[p.name for p in PUBLISHED])
def test_the_databook_beside_a_holon_is_a_projection_of_that_holon(path: Path) -> None:
  """A DataBook names the holon it was written from by digest, and every
  triple in its RDF blocks is one the holon's ``#scene`` states."""
  import hashlib
  import re

  from rdflib import Dataset, Graph

  book = path.with_name(path.name.replace(".holon.jsonld", ".databook.md"))
  assert book.exists(), f"{path.name} has no DataBook beside it: regenerate that demo"
  text = book.read_text(encoding="utf-8")
  digest = hashlib.sha256(path.read_bytes()).hexdigest()
  # The digest is what binds the two. The server names the holon by the
  # filename it downloads under, which a demo saving it under its own stem
  # does not keep, so the name is checked only for being a holon's.
  assert f'sha256: "{digest}"' in text, f"{book.name} was written from another holon"
  assert re.search(r'href: "[^"]+\.holon\.jsonld"', text)

  dataset = Dataset()
  dataset.parse(data=path.read_text(encoding="utf-8"), format="json-ld")
  scene = next(g for g in dataset.graphs() if str(g.identifier).endswith("#scene"))
  blocks = re.findall(r"```turtle\n(.*?)\n```", text, re.S)
  assert blocks, f"{book.name} carries no RDF"
  inline = Graph()
  for block in blocks:
    inline.parse(data=block, format="turtle")
  assert all(triple in scene for triple in inline)


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


@pytest.mark.parametrize("path", PUBLISHED, ids=[p.name for p in PUBLISHED])
def test_the_workbook_of_a_holon_opens_and_names_that_holon(path: Path) -> None:
  """The workbook download, written from a real holon: it opens, it names the
  holon by digest, and every section with a fact has a sheet of numbers."""
  import hashlib
  from io import BytesIO

  import openpyxl
  from xbrlkit.holon.pivot import pivot_sections

  from robosystems.operations.serialization.rdf.workbook import (
    serialize_holon_to_workbook,
  )

  text = path.read_text(encoding="utf-8")
  book = openpyxl.load_workbook(
    BytesIO(serialize_holon_to_workbook(text, holon_filename=path.name))
  )
  contents = book["Contents"]
  stated = {row[0].value: row[1].value for row in contents.iter_rows(max_row=12)}
  assert stated["Holon SHA-256"] == hashlib.sha256(text.encode("utf-8")).hexdigest()
  assert stated["Holon file"] == path.name

  sections = pivot_sections(from_holon_report(text)[0])
  assert len(book.sheetnames) == len(sections) + 1
  for sheet in book.worksheets[1:]:
    cells = [c for row in sheet.iter_rows(min_row=6) for c in row]
    assert all(c.data_type != "f" for c in cells), f"{sheet.title} holds a formula"
  assert any(
    isinstance(c.value, (int, float))
    for sheet in book.worksheets[1:]
    for row in sheet.iter_rows(min_row=6)
    for c in row
  )
