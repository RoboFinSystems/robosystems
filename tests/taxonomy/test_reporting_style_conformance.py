"""Every concept one Reporting Style presents has a place in every other.

A group's statements render under its parent's Style while each member keeps
its own, so a member's concept has to land somewhere on a statement of
another form: its own row, a row it rolls up into (a general-special
ancestor), or a row under a calculation ancestor that statement presents.
A concept with none of those would be summed into a subtotal and shown on no
line, or left out. A new Style has to pass this against every Style already
seeded before a group can mix it with them.

Over the package files alone, for every ordered pair of Styles:

1. each concept the first presents on its balance sheet, income statement
   or cash flow has a place on the second's;
2. each equity balance the first's balance sheet presents is carried by the
   second's equity statement: a roll-forward root, or summed by one.

The second does not hold toward the forms in ``EQUITY_NOT_ROLLED_FORWARD``.

This is about where a concept can go, not whether the renderer puts it
there: the cash flow gives a row only to a concept in its own network or
one that rolls up to it. An equity statement's movement rows are left out,
since the engine states a movement on the rendering Style's own rows
whichever form the line was tagged in.
"""

from __future__ import annotations

import json
from collections import defaultdict
from itertools import permutations

import pytest

from robosystems.taxonomy.discovery import framework_root

_PACKAGES = framework_root("rs-gaap") / "packages"
_HAS_TRAIT = "https://robosystems.ai/vocab/hasTrait"
_STYLES = ("BSC-CORP-IS02-CF1", "BSC-PART-IS02-CF1", "BSC-LLC-IS02-CF1")
_PLACED = ("balance_sheet", "income_statement", "cash_flow_statement")

# A Style whose equity statement rolls forward its own form's capital and
# nothing else, so another form's equity is outside it. The corporate one
# rolls forward total equity and carries every form. Each entry needs a
# reason, and fails once it is no longer true.
_OWN_FORM_ONLY = (
  "its roll-forward root is the form's own capital concept, a calculation "
  "leaf; equity in another form is on its balance sheet and off its equity "
  "statement"
)
EQUITY_NOT_ROLLED_FORWARD: dict[str, str] = {
  "BSC-PART-IS02-CF1": _OWN_FORM_ONLY,
  "BSC-LLC-IS02-CF1": _OWN_FORM_ONLY,
}


def _graph(name: str) -> list[dict]:
  return json.loads((_PACKAGES / name / "v1" / "taxonomy.jsonld").read_text())["@graph"]


def _ref(end: str | dict) -> str:
  return end if isinstance(end, str) else end["@id"]


@pytest.fixture(scope="module")
def networks() -> dict[str, dict[str, list[str]]]:
  out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
  for n in _graph("rs-gaap-presentation"):
    if n.get("associationType") == "presentation" and n.get("role"):
      out[n["role"]][_ref(n["from"])].append(_ref(n["to"]))
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
  for package in ("rs-gaap-hierarchy", "rs-gaap-type-subtype", "rs-gaap-calculations"):
    for n in _graph(package):
      kind = n.get("associationType")
      if kind in out:
        out[kind][_ref(n["to"])].add(_ref(n["from"]))
  return out


@pytest.fixture(scope="module")
def equity_balances() -> set[str]:
  """The concepts that are an equity balance: the trait, at an instant."""
  instants = {
    n["@id"]
    for n in _graph("rs-gaap")
    if any(v.get("@value") == "instant" for v in n.get("periodType", []))
  }
  return instants & {
    n["@id"]
    for n in _graph("rs-gaap-traits")
    for t in n.get(_HAS_TRAIT, [])
    if t["@id"].endswith("elementsOfFinancialStatements/equity")
  }


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


_PAIRS = list(permutations(_STYLES, 2))


def test_every_seeded_style_is_compared(styles) -> None:
  composed = {code for code, nets in styles.items() if set(_PLACED) <= set(nets)}
  assert composed == set(_STYLES), (
    "a Style was added or removed; list it in _STYLES so it is compared with "
    f"the others: {sorted(composed ^ set(_STYLES))}"
  )


@pytest.mark.parametrize("statement", _PLACED)
@pytest.mark.parametrize(("member", "group"), _PAIRS)
def test_every_presented_concept_has_a_place_on_the_other_style(
  member, group, statement, networks, styles, parents
) -> None:
  presented = _nodes(networks[styles[member][statement]])
  on_the_other = _nodes(networks[styles[group][statement]])
  placeless = sorted(
    concept
    for concept in presented - on_the_other
    if not _reaches(concept, on_the_other, parents["general-special"])
    and not _reaches(concept, on_the_other, parents["calculation"])
  )
  assert not placeless, (
    f"{member} presents these on its {statement.replace('_', ' ')} and {group} "
    f"has no row, broader row or subtotal for them: {placeless}"
  )


@pytest.mark.parametrize(("member", "group"), _PAIRS)
def test_every_equity_balance_is_rolled_forward_on_the_other_style(
  member, group, networks, styles, parents, equity_balances
) -> None:
  held = _nodes(networks[styles[member]["balance_sheet"]]) & equity_balances
  rolled_forward = _nodes(networks[styles[group]["equity_statement"]])
  left_out = sorted(
    concept
    for concept in held
    if not _reaches(concept, rolled_forward, parents["calculation"])
  )
  if group in EQUITY_NOT_ROLLED_FORWARD:
    assert left_out, (
      f"{group} now rolls forward every equity balance of {member}; drop it "
      "from EQUITY_NOT_ROLLED_FORWARD"
    )
    return
  assert not left_out, (
    f"{member} holds equity that {group}'s equity statement does not roll "
    f"forward: {left_out}"
  )
