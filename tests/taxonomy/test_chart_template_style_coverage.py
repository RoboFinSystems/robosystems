"""Every concept a chart template maps to has a line on its Style's statements.

A chart template maps each account to an rs-gaap concept, with a variant per
entity form for the accounts that differ. An entity of that form gets the
form's Reporting Style. If the Style has no place for a concept the template
maps to, the amount is in the books and on no line: the subtotal that sums it
does not foot, or the statement leaves it out. Observed live (2026-11-12): a
consolidation entity on the LLC Style holding additional paid-in capital, on
no line of its balance sheet and absent from its equity statement.

Two invariants hold over the package and template files alone, for every
template and form:

1. a concept the template maps to has a line on the Style's balance sheet or
   income statement: its own row, a row it rolls up into (a general-special
   ancestor), a row placed under a calculation ancestor the statement
   presents, or the net row the engine synthesizes it into;
2. an equity concept the template maps to is carried by the Style's equity
   statement: one of its roll-forward roots, or summed by one.

The declared exceptions to the second are in ``EQUITY_LEFT_OUT``.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

from robosystems.operations.roboledger.reports.calc_dag import SYNTHESIZED_PARENTS
from robosystems.taxonomy.discovery import framework_root

_PACKAGES = framework_root("rs-gaap") / "packages"
_TEMPLATES = framework_root("rs-gaap").parent / "chart-templates"
_HAS_TRAIT = "https://robosystems.ai/vocab/hasTrait"
_STYLE_OF_FORM = {
  "corporation": "BSC-CORP-IS02-CF1",
  "partnership": "BSC-PART-IS02-CF1",
  "llc": "BSC-LLC-IS02-CF1",
}

# A template and form whose equity the form's Style does not carry whole.
# The consolidation chart holds each member's capital in that member's form
# and the parent's adjustments in the parent's, so only a Style whose equity
# statement rolls forward every form (the corporate one) carries it all. Each
# entry needs a reason, and fails once it is no longer true.
_CONSOLIDATION = (
  "the consolidation entity holds capital of every form; on this Style its own "
  "equity statement leaves the other forms' out, and only the consolidated "
  "statements, on the parent's Style, are read as statements"
)
EQUITY_LEFT_OUT: dict[tuple[str, str], str] = {
  ("consolidation", "llc"): _CONSOLIDATION,
  ("consolidation", "partnership"): _CONSOLIDATION,
}


def _graph(name: str) -> list[dict]:
  return json.loads((_PACKAGES / name / "v1" / "taxonomy.jsonld").read_text())["@graph"]


@pytest.fixture(scope="module")
def networks() -> dict[str, dict[str, list[str]]]:
  out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
  for n in _graph("rs-gaap-presentation"):
    if n.get("associationType") == "presentation" and n.get("role"):
      out[n["role"]][n["from"]["@id"]].append(n["to"]["@id"])
  return out


@pytest.fixture(scope="module")
def styles() -> dict[str, dict[str, str]]:
  return {
    n["reportingStyleCode"]: {
      net["statementType"]: net["networkRoleUri"] for net in n["reportingStyleNetworks"]
    }
    for n in _graph("rs-gaap-reporting-styles")
    if n.get("reportingStyleNetworks")
  }


@pytest.fixture(scope="module")
def parents() -> dict[str, dict[str, set[str]]]:
  """``{arc type: {child: parents}}`` for the roll-up and calculation arcs."""
  out: dict[str, dict[str, set[str]]] = {
    "general-special": defaultdict(set),
    "calculation": defaultdict(set),
  }
  for package in ("rs-gaap-hierarchy", "rs-gaap-calculations"):
    for n in _graph(package):
      kind = n.get("associationType")
      if kind in out:
        out[kind][_ref(n["to"])].add(_ref(n["from"]))
  # Property is stated net: the engine sums gross and accumulated depreciation
  # into the net row with no calculation arc between them.
  for child, parent in SYNTHESIZED_PARENTS.items():
    out["calculation"][child].add(parent)
  return out


@pytest.fixture(scope="module")
def equity_concepts() -> set[str]:
  return {
    n["@id"]
    for n in _graph("rs-gaap-traits")
    for t in n.get(_HAS_TRAIT, [])
    if t["@id"].endswith("elementsOfFinancialStatements/equity")
  }


def _ref(end: str | dict) -> str:
  """An arc's end, written as a qname or as a node reference."""
  return end if isinstance(end, str) else end["@id"]


def _nodes(network: dict[str, list[str]]) -> set[str]:
  return set(network) | {c for cs in network.values() for c in cs}


def _reaches(concept: str, targets: set[str], parents: dict[str, set[str]]) -> bool:
  """Whether ``concept`` or an ancestor of it along ``parents`` is a target."""
  seen, frontier = {concept}, [concept]
  while frontier:
    current = frontier.pop()
    if current in targets:
      return True
    for parent in parents.get(current, ()):
      if parent not in seen:
        seen.add(parent)
        frontier.append(parent)
  return False


def _mapped(template: str, form: str) -> set[str]:
  mapping = json.loads(
    (_TEMPLATES / template / "v1" / "mappings" / "rs-gaap.jsonld").read_text()
  )
  arcs = mapping["arcs"] + (mapping.get("variants") or {}).get(form, [])
  return {arc["to"] for arc in arcs}


_CASES = [
  (template.name, form)
  for template in sorted(_TEMPLATES.iterdir())
  if (template / "v1" / "mappings" / "rs-gaap.jsonld").exists()
  for form in _STYLE_OF_FORM
]


def test_every_template_is_covered() -> None:
  assert {template for template, _form in _CASES} >= {
    "consolidation",
    "product",
    "saas",
    "services",
  }


@pytest.mark.parametrize(("template", "form"), _CASES)
def test_every_mapped_concept_has_a_line(
  template, form, networks, styles, parents
) -> None:
  composition = styles[_STYLE_OF_FORM[form]]
  presented = _nodes(networks[composition["balance_sheet"]]) | _nodes(
    networks[composition["income_statement"]]
  )
  lineless = sorted(
    concept
    for concept in _mapped(template, form)
    if not _reaches(concept, presented, parents["general-special"])
    and not _reaches(concept, presented, parents["calculation"])
  )
  assert not lineless, (
    f"{template} on {_STYLE_OF_FORM[form]}: mapped to a concept with no line "
    f"on the balance sheet or income statement: {lineless}"
  )


@pytest.mark.parametrize(("template", "form"), _CASES)
def test_every_mapped_equity_concept_is_on_the_equity_statement(
  template, form, networks, styles, parents, equity_concepts
) -> None:
  composition = styles[_STYLE_OF_FORM[form]]
  rolled_forward = _nodes(networks[composition["equity_statement"]])
  left_out = sorted(
    concept
    for concept in _mapped(template, form) & equity_concepts
    if not _reaches(concept, rolled_forward, parents["calculation"])
  )
  if (template, form) in EQUITY_LEFT_OUT:
    assert left_out, (
      f"{template} is now carried whole by {_STYLE_OF_FORM[form]}; drop it from "
      "EQUITY_LEFT_OUT"
    )
    return
  assert not left_out, (
    f"{template} on {_STYLE_OF_FORM[form]}: equity mapped to a concept its "
    f"equity statement does not carry: {left_out}"
  )
