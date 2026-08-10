"""Reachability of rs-gaap-calculations/v1 cash-flow ``derivation`` arcs.

A ``derivation`` arc encodes "this cash-flow leaf is the period change in
that balance-sheet concept" (``from`` = flow concept, ``to`` = BS concept).
The renderer's element-default fallback — the load-bearing path for
untagged QuickBooks data — routes a line to its *mapped* rs-gaap element's
derivation arc. So an arc whose ``to`` is a concept nothing can map to can
never fire, and the flow it derives silently falls through to operating
working capital instead of investing/financing.

That is not hypothetical: the 2026-06-06 Default Style split (``bfa0ebc7``)
replaced four combined leaves, and the derivation arcs keying on two of
them were not repointed. Long-term debt issuance and repayment kept
deriving from ``LongTermDebtNoncurrent``, which no calc network wires — so
a note issuance rendered as an operating working-capital change with
financing at zero, and the statement still tied, because the residual had
nowhere else to go.

A mappable ``to`` is either a calc leaf (a calculation child that is not
itself a parent) or one of the sanctioned synthesized grains the candidate
suggester admits regardless of presentation-set membership.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_SYNTHESIZED_DETAIL_ALLOW,
)
from robosystems.taxonomy.discovery import framework_root

CALC_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-calculations"
  / "v1"
  / "taxonomy.jsonld"
)


@pytest.fixture(scope="module")
def graph() -> list[dict]:
  return json.loads(CALC_PATH.read_text())["@graph"]


@pytest.fixture(scope="module")
def mappable(graph: list[dict]) -> frozenset[str]:
  """Concepts a CoA account can actually map to.

  Calc leaves (children that are not themselves parents), plus the
  synthesized PP&E grains the suggester admits so CF Investing can read
  capex as the change in Gross rather than the change in Net.
  """
  children: dict[str, list[str]] = defaultdict(list)
  parents: set[str] = set()
  for node in graph:
    if "from" not in node or node.get("associationType") != "calculation":
      continue
    children[node["from"]["@id"]].append(node["to"]["@id"])
    parents.add(node["from"]["@id"])
  leaves = {c for cs in children.values() for c in cs} - parents
  return frozenset(leaves | set(RS_GAAP_SYNTHESIZED_DETAIL_ALLOW))


# Arcs knowingly dormant: the target concept is declared but no Reporting
# Style wires it into a calc network, so nothing can map to it and the arc
# never fires. Distinct from the bfa0ebc7 breakage above — these were never
# reachable, rather than having been silently orphaned by a split. Retained
# so the flow is already described if a Style later wires the concept.
# Delete the entry (not the arc) when that happens.
KNOWN_DORMANT: frozenset[str] = frozenset(
  {
    # No income-taxes-payable line in the Default Style's current
    # LiabilitiesCurrent composition.
    "_:rs-gaap-deriv-cf-income-taxes-payable-arc-1",
  }
)


def test_every_derivation_target_is_mappable(
  graph: list[dict], mappable: frozenset[str]
) -> None:
  unreachable = [
    (node["@id"], node["from"]["@id"], node["to"]["@id"])
    for node in graph
    if node.get("associationType") == "derivation"
    and node["to"]["@id"] not in mappable
    and node["@id"] not in KNOWN_DORMANT
  ]
  assert not unreachable, (
    "derivation arcs whose target nothing can map to:\n"
    + "\n".join(f"  {arc}: {flow} -> {target}" for arc, flow, target in unreachable)
  )


def test_known_dormant_arcs_are_still_dormant(
  graph: list[dict], mappable: frozenset[str]
) -> None:
  """Retires an allowlist entry once its concept becomes mappable.

  Without this the allowlist silently outlives its reason, which is how the
  arcs it excuses stop being reviewed.
  """
  now_reachable = [
    node["@id"]
    for node in graph
    if node.get("@id") in KNOWN_DORMANT and node["to"]["@id"] in mappable
  ]
  assert not now_reachable, (
    f"KNOWN_DORMANT entries whose target is now mappable — drop them "
    f"from the allowlist: {now_reachable}"
  )


def test_derivation_arcs_reference_declared_concepts(graph: list[dict]) -> None:
  """Both ends resolve to a concept declared in the rs-gaap catalog."""
  declared = {
    node["@id"]
    for node in json.loads(
      (
        framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
      ).read_text()
    )["@graph"]
    if str(node.get("@id", "")).startswith("rs-gaap:")
  }
  missing = [
    (node["@id"], side, node[side]["@id"])
    for node in graph
    if "from" in node
    for side in ("from", "to")
    if node[side]["@id"].startswith("rs-gaap:") and node[side]["@id"] not in declared
  ]
  assert not missing, f"arcs referencing undeclared concepts: {missing}"


def test_debt_and_intangible_flows_reach_their_split_successors(
  graph: list[dict],
) -> None:
  """Pins the bfa0ebc7 repointing so a future split can't silently undo it."""
  targets = {
    node["from"]["@id"]: node["to"]["@id"]
    for node in graph
    if node.get("associationType") == "derivation"
  }
  assert (
    targets["rs-gaap:ProceedsFromIssuanceOfLongTermDebt"]
    == "rs-gaap:LongTermDebtAndCapitalLeaseObligations"
  )
  assert (
    targets["rs-gaap:RepaymentsOfLongTermDebt"]
    == "rs-gaap:LongTermDebtAndCapitalLeaseObligations"
  )
  assert (
    targets["rs-gaap:PaymentsToAcquireIntangibleAssets"]
    == "rs-gaap:IntangibleAssetsNetExcludingGoodwill"
  )
