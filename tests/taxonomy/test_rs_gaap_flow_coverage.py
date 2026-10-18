"""Flow-element coverage of the rs-gaap package set, per Reporting Style.

The cash flow statement's investing and financing sections are built from
per-line flows: a tagged flow, or the *default* flow of the balance-sheet
concept an account maps to (a ``derivation`` arc). A gap on either side is a
plug, not an error: a concept with no default lands its cash in the operating
reconciling adjustment, and a flow with no row on the statement computes a
fact nothing shows. Observed live (2026-10-10): an LLC's note draw and member
contribution rendered as a $10,100 operating adjustment.

Two invariants hold for every seeded Style, over the package files alone:

1. every balance-sheet leaf a chart account can map to has a default flow,
   or is declared flowless (cash, the anchor) or operating (its movement is
   a working-capital row or a non-cash item — named below, so a reader can
   tell a decision from an omission);
2. every investing / financing flow reachable as a default of that Style's
   leaves, and every flow the Style's cash flow network presents, is a leaf
   row of that network with a calc arc (row ⟺ arc), and carries the
   ``activityType`` trait the renderer and the tag validator key on.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

from robosystems.operations.roboledger.reports.fact_grid import _CASH_ANCHOR_QNAMES
from robosystems.taxonomy.discovery import framework_root

_PACKAGES = framework_root("rs-gaap") / "packages"
_PRES = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/"
_HAS_TRAIT = "https://robosystems.ai/vocab/hasTrait"
_SUBTOTALS = {
  "investingActivity": "rs-gaap:NetCashProvidedByUsedInInvestingActivities",
  "financingActivity": "rs-gaap:NetCashProvidedByUsedInFinancingActivities",
}

# Balance-sheet leaves whose movement is not an investing or financing flow.
# Operating ones reach the statement as working-capital rows or stay in the
# reconciling adjustment (the right section, if not yet their own row);
# non-cash ones never move cash. Each needs a reason to stay here.
OPERATING_OR_NON_CASH: dict[str, str] = {
  "rs-gaap:OperatingLeaseRightOfUseAsset": "non-cash: the ROU asset",
  "rs-gaap:FinanceLeaseRightOfUseAsset": "non-cash: the ROU asset",
  "rs-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax": "non-cash: OCI",
  "rs-gaap:RetainedEarningsAccumulatedDeficit": (
    "earnings close without cash; dividends paid are its one default (checked)"
  ),
}


def _graph(name: str) -> list[dict]:
  return json.loads((_PACKAGES / name / "v1" / "taxonomy.jsonld").read_text())["@graph"]


@pytest.fixture(scope="module")
def declared() -> set[str]:
  return {
    n["@id"] for n in _graph("rs-gaap") if str(n.get("@id", "")).startswith("rs-gaap:")
  }


@pytest.fixture(scope="module")
def activity() -> dict[str, str]:
  """``{qname: investingActivity | financingActivity}`` from the traits package."""
  out: dict[str, str] = {}
  for n in _graph("rs-gaap-traits"):
    for t in n.get(_HAS_TRAIT, []):
      _cat, _, ident = t["@id"].rpartition("/")
      if _cat.endswith("activityType") and ident in _SUBTOTALS:
        out[n["@id"]] = ident
  return out


@pytest.fixture(scope="module")
def reconciling() -> set[str]:
  """Concepts carrying the indirect-method reconciling-item trait: the
  operating section's working-capital and non-cash rows."""
  return {
    n["@id"]
    for n in _graph("rs-gaap-traits")
    for t in n.get(_HAS_TRAIT, [])
    if t["@id"].endswith("/indirectCashFlowMethodReconcilingItem")
  }


@pytest.fixture(scope="module")
def calc() -> dict:
  derivations: dict[str, list[tuple[str, float]]] = defaultdict(
    list
  )  # bs -> [(flow, w)]
  children: dict[str, list[str]] = defaultdict(list)  # parent -> children
  for n in _graph("rs-gaap-calculations"):
    if n.get("associationType") == "derivation":
      derivations[n["to"]["@id"]].append((n["from"]["@id"], float(n["weight"])))
    elif n.get("associationType") == "calculation":
      children[n["from"]["@id"]].append(n["to"]["@id"])
  return {"derivations": derivations, "children": children}


@pytest.fixture(scope="module")
def networks() -> dict[str, dict[str, list[str]]]:
  """``{role_uri: {parent: [children]}}`` for every presentation network."""
  out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
  for n in _graph("rs-gaap-presentation"):
    if n.get("associationType") == "presentation" and n.get("role"):
      out[n["role"]][n["from"]["@id"]].append(n["to"]["@id"])
  return out


@pytest.fixture(scope="module")
def styles() -> dict[str, dict[str, str]]:
  """``{code: {statement_type: network role}}`` for each composed Style."""
  return {
    n["reportingStyleCode"]: {
      net["statementType"]: net["networkRoleUri"] for net in n["reportingStyleNetworks"]
    }
    for n in _graph("rs-gaap-reporting-styles")
    if n.get("reportingStyleNetworks")
  }


def _leaves(network: dict[str, list[str]]) -> set[str]:
  return {c for cs in network.values() for c in cs} - set(network)


@pytest.fixture(
  scope="module", params=["BSC-CORP-IS02-CF1", "BSC-PART-IS02-CF1", "BSC-LLC-IS02-CF1"]
)
def style(request, styles) -> tuple[str, dict[str, str]]:
  return request.param, styles[request.param]


def test_every_mapping_candidate_has_a_default_flow_or_is_declared(
  style, networks, calc
) -> None:
  code, composition = style
  bs_leaves = _leaves(networks[composition["balance_sheet"]])
  uncovered = sorted(
    leaf
    for leaf in bs_leaves
    if leaf not in _CASH_ANCHOR_QNAMES
    and leaf not in OPERATING_OR_NON_CASH
    and not calc["derivations"].get(leaf)
  )
  assert not uncovered, (
    f"{code}: balance-sheet leaves with no default flow — a cash entry on an "
    f"account mapped to one lands in the operating plug: {uncovered}"
  )


def test_declared_exceptions_stay_undefaulted(calc) -> None:
  """A declared exception that gains a default has outlived its reason."""
  defaulted = sorted(q for q in OPERATING_OR_NON_CASH if q in calc["derivations"])
  # Retained earnings keeps one default (dividends) by design.
  defaulted = [
    q for q in defaulted if q != "rs-gaap:RetainedEarningsAccumulatedDeficit"
  ]
  assert not defaulted, (
    f"drop from OPERATING_OR_NON_CASH, they have defaults: {defaulted}"
  )


def test_every_reachable_flow_is_a_row_with_a_calc_arc(
  style, networks, calc, activity
) -> None:
  """Invariant 2 — the defaults of this Style's leaves, and the rows its
  cash flow network presents, are each a leaf row that foots a subtotal."""
  code, composition = style
  cf = networks[composition["cash_flow_statement"]]
  bs_leaves = _leaves(networks[composition["balance_sheet"]])
  reachable = {
    flow
    for leaf in bs_leaves
    for flow, _w in calc["derivations"].get(leaf, [])
    if flow in activity
  }
  presented = {c for c in _leaves(cf) if c in activity}
  problems: list[str] = []
  for flow in sorted(reachable | presented):
    section = activity[flow]
    if flow not in _leaves(cf):
      problems.append(f"{flow}: no leaf row on {composition['cash_flow_statement']}")
      continue
    if flow not in cf.get(_SUBTOTALS[section], []):
      problems.append(f"{flow}: presented outside its {section} section")
    if flow not in calc["children"].get(_SUBTOTALS[section], []):
      problems.append(f"{flow}: no calc arc under {_SUBTOTALS[section]}")
  assert not problems, f"{code}:\n  " + "\n  ".join(problems)


def test_every_default_flow_carries_its_activity_trait(
  calc, activity, declared, reconciling
) -> None:
  """The renderer emits, and the tag validator admits, only flows with an
  ``activityType`` trait; a default without one is silently dropped. An
  operating row (a working-capital change, a non-cash adjustment) is not a
  flow: it carries the reconciling-item trait instead."""
  operating_prefix = "rs-gaap:IncreaseDecreaseIn"
  untraited = sorted(
    flow
    for flows in calc["derivations"].values()
    for flow, _w in flows
    if flow not in activity
    and flow not in reconciling
    and not flow.startswith(operating_prefix)
    and flow != "rs-gaap:DepreciationDepletionAndAmortization"
  )
  assert not untraited, (
    f"investing/financing defaults with no activityType trait: {untraited}"
  )
  undeclared = sorted(
    flow
    for flows in calc["derivations"].values()
    for flow, _w in flows
    if flow not in declared
  )
  assert not undeclared, f"defaults naming undeclared concepts: {undeclared}"


def test_the_equity_statement_has_rows_of_its_own(styles, networks) -> None:
  """The roll-forward's change rows are equity movement concepts, never the
  cash flow's (Joey, 2026-10-16, replacing the 2026-10-13 shared-concept
  decision): an owner who pays a company liability contributes capital and
  moves no cash, so a cash flow row cannot carry it. Each form keeps its
  cash flow equity rows on the cash flow statement alone."""
  for code, movement_rows in (
    (
      "BSC-CORP-IS02-CF1",
      {
        "rs-gaap:StockIssuedDuringPeriodValueNewIssues",
        "rs-gaap:StockRepurchasedDuringPeriodValue",
        "rs-gaap:Dividends",
        "rs-gaap:StockholdersEquityOther",
      },
    ),
    (
      "BSC-LLC-IS02-CF1",
      {
        "rs-gaap:PartnersCapitalAccountContributions",
        "rs-gaap:PartnersCapitalAccountDistributions",
        "rs-gaap:PartnersCapitalOther",
      },
    ),
    (
      "BSC-PART-IS02-CF1",
      {
        "rs-gaap:PartnersCapitalAccountContributions",
        "rs-gaap:PartnersCapitalAccountDistributions",
        "rs-gaap:PartnersCapitalOther",
      },
    ),
  ):
    composition = styles[code]
    cf_rows = _leaves(networks[composition["cash_flow_statement"]])
    equity_rows = _leaves(networks[composition["equity_statement"]])
    assert movement_rows <= equity_rows, (code, movement_rows - equity_rows)
    # Net income, and the outside owners' share of it, open both statements.
    shared = {
      "rs-gaap:NetIncomeLoss",
      "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest",
    }
    assert not (equity_rows & cf_rows) - shared, (code, equity_rows & cf_rows)


def test_the_form_cash_flow_networks_differ_only_in_equity_rows(
  styles, networks
) -> None:
  """LLC and PART clone the corporate cash flow statement and swap the
  financing section's equity rows — the balance-sheet pattern."""
  corp = networks[styles["BSC-CORP-IS02-CF1"]["cash_flow_statement"]]
  corp_rows = _leaves(corp)
  corp_equity = {
    "rs-gaap:ProceedsFromIssuanceOfCommonStock",
    "rs-gaap:PaymentsForRepurchaseOfCommonStock",
    "rs-gaap:ProceedsFromIssuanceOfPreferredStockAndPreferenceStock",
    "rs-gaap:PaymentsForRepurchaseOfPreferredStockAndPreferenceStock",
    "rs-gaap:PaymentsOfDividends",
    "rs-gaap:ProceedsFromSaleOfTreasuryStock",
  }
  assert corp_equity <= corp_rows
  for code, own in (
    (
      "BSC-LLC-IS02-CF1",
      {
        "rs-gaap:ProceedsFromContributedCapital",
        "rs-gaap:PaymentsOfCapitalDistribution",
      },
    ),
    (
      "BSC-PART-IS02-CF1",
      {
        "rs-gaap:ProceedsFromPartnershipContribution",
        "rs-gaap:PaymentsOfCapitalDistribution",
      },
    ),
  ):
    form_rows = _leaves(networks[styles[code]["cash_flow_statement"]])
    assert form_rows == (corp_rows - corp_equity) | own, code


def test_short_term_debt_and_investments_are_declared_with_traits(
  declared, activity
) -> None:
  for qname, section in (
    ("rs-gaap:ProceedsFromShortTermDebt", "financingActivity"),
    ("rs-gaap:RepaymentsOfShortTermDebt", "financingActivity"),
    ("rs-gaap:PaymentsToAcquireShortTermInvestments", "investingActivity"),
    ("rs-gaap:ProceedsFromSaleOfShortTermInvestments", "investingActivity"),
  ):
    assert qname in declared, qname
    assert activity.get(qname) == section, qname
