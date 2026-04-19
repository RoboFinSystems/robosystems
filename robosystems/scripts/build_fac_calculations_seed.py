"""Generate fac-calculations/v1/taxonomy.jsonld from FAC's BS/IS/CF rules.

FAC encodes its accounting identities as human-readable documentation
strings on fac:BS1..BS5, fac:IS1..IS11, fac:CF1..CF6 concepts. These
rules are semantically the same as XBRL calculation linkbase arcs:
each rule says "aggregate = +child1 ± child2 ± …".

This script generates a calc-arc seed by hand-mapping each rule's head
and its weighted children to fac qnames. The output is a standard
mapping-taxonomy JSON-LD file that the library loader consumes.

Weights aren't carried in the simple JSON-LD predicate format we use
today (summationOf → [@id,…]) — we emit the arc topology only and
track +1/-1 sign semantics in the structure `name` field for now.
Upgrading to weighted arcs is a follow-up when we adopt a richer arc
serialization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Rule:
  code: str  # BS1, IS1, CF1 …
  doc: str  # human-readable identity
  head: str  # fac:Assets, fac:GrossProfit …
  children: tuple[tuple[str, int], ...]  # (fac qname, ±1 weight)


RULES: tuple[Rule, ...] = (
  # ── Balance sheet ─────────────────────────────────────────────────────
  Rule(
    "BS1",
    "Equity = Parent + Noncontrolling",
    "fac:Equity",
    (
      ("fac:EquityAttributableToParent", +1),
      ("fac:EquityAttributableToNoncontrollingInterest", +1),
    ),
  ),
  Rule(
    "BS2",
    "Assets = Liabilities + Equity",
    "fac:Assets",
    (("fac:LiabilitiesAndEquity", +1),),
  ),
  Rule(
    "BS3",
    "Assets = Current + Noncurrent (classified BS)",
    "fac:Assets",
    (("fac:CurrentAssets", +1), ("fac:NoncurrentAssets", +1)),
  ),
  Rule(
    "BS4",
    "Liabilities = Current + Noncurrent (classified BS)",
    "fac:Liabilities",
    (("fac:CurrentLiabilities", +1), ("fac:NoncurrentLiabilities", +1)),
  ),
  Rule(
    "BS5",
    "L+E = Liabilities + Commitments + Temp. Equity + Equity",
    "fac:LiabilitiesAndEquity",
    (
      ("fac:Liabilities", +1),
      ("fac:CommitmentsAndContingencies", +1),
      ("fac:TemporaryEquity", +1),
      ("fac:Equity", +1),
    ),
  ),
  # ── Income statement ──────────────────────────────────────────────────
  Rule(
    "IS1",
    "Gross Profit = Revenues − Cost of Revenue (multi-step)",
    "fac:GrossProfit",
    (("fac:Revenues", +1), ("fac:CostOfRevenue", -1)),
  ),
  Rule(
    "IS2",
    "Operating Income = Gross Profit − Operating Exp. + Other Operating (multi-step)",
    "fac:OperatingIncomeLoss",
    (
      ("fac:GrossProfit", +1),
      ("fac:OperatingExpenses", -1),
      ("fac:OtherOperatingIncomeExpenses", +1),
    ),
  ),
  Rule(
    "IS3",
    "Income Before Equity-Method Investments = Operating + Nonoperating − Interest & Debt",
    "fac:IncomeLossBeforeEquityMethodInvestments",
    (
      ("fac:OperatingIncomeLoss", +1),
      ("fac:NonoperatingIncomeLoss", +1),
      ("fac:InterestAndDebtExpense", -1),
    ),
  ),
  Rule(
    "IS4",
    "Income from Continuing Ops Before Tax = Before Equity-Method + Equity-Method Investments",
    "fac:IncomeLossFromContinuingOperationsBeforeTax",
    (
      ("fac:IncomeLossBeforeEquityMethodInvestments", +1),
      ("fac:IncomeLossFromEquityMethodInvestments", +1),
    ),
  ),
  Rule(
    "IS5",
    "Income from Continuing Ops After Tax = Before Tax − Income Tax Expense",
    "fac:IncomeLossFromContinuingOperationsAfterTax",
    (
      ("fac:IncomeLossFromContinuingOperationsBeforeTax", +1),
      ("fac:IncomeTaxExpenseBenefit", -1),
    ),
  ),
  Rule(
    "IS6",
    "Net Income = Continuing (After Tax) + Discontinued (Net of Tax)",
    "fac:NetIncomeLoss",
    (
      ("fac:IncomeLossFromContinuingOperationsAfterTax", +1),
      ("fac:IncomeLossFromDiscontinuedOperationsNetOfTax", +1),
    ),
  ),
  Rule(
    "IS7",
    "Net Income = Parent + Noncontrolling",
    "fac:NetIncomeLoss",
    (
      ("fac:NetIncomeLossAttributableToParent", +1),
      ("fac:NetIncomeLossAttributableToNoncontrollingInterest", +1),
    ),
  ),
  Rule(
    "IS8",
    "NI Available to Common (Basic) = Parent − Preferred Dividends & Other",
    "fac:NetIncomeLossAvailableToCommonStockholdersBasic",
    (
      ("fac:NetIncomeLossAttributableToParent", +1),
      ("fac:PreferredStockDividendsAndOtherAdjustments", -1),
    ),
  ),
  Rule(
    "IS9",
    "Comprehensive Income = Parent + Noncontrolling",
    "fac:ComprehensiveIncomeLoss",
    (
      ("fac:ComprehensiveIncomeLossAttributableToParent", +1),
      ("fac:ComprehensiveIncomeLossAttributableToNoncontrollingInterest", +1),
    ),
  ),
  Rule(
    "IS10",
    "Comprehensive Income = Net Income + Other Comprehensive Income",
    "fac:ComprehensiveIncomeLoss",
    (("fac:NetIncomeLoss", +1), ("fac:OtherComprehensiveIncomeLoss", +1)),
  ),
  Rule(
    "IS11",
    "Operating Income = Revenues − Costs&Expenses + Other Operating (single-step)",
    "fac:OperatingIncomeLoss",
    (
      ("fac:Revenues", +1),
      ("fac:CostsAndExpenses", -1),
      ("fac:OtherOperatingIncomeExpenses", +1),
    ),
  ),
  # ── Cash flow statement ───────────────────────────────────────────────
  Rule(
    "CF1",
    "Net Cash Flow = Operating + Investing + Financing + Exchange Gain/Loss",
    "fac:NetCashFlow",
    (
      ("fac:NetCashFlowFromOperatingActivities", +1),
      ("fac:NetCashFlowFromInvestingActivities", +1),
      ("fac:NetCashFlowFromFinancingActivities", +1),
      ("fac:ExchangeGainsLosses", +1),
    ),
  ),
  Rule(
    "CF2",
    "Net Cash Flows Continuing = Operating + Investing + Financing (Continuing)",
    "fac:NetCashFlowContinuing",
    (
      ("fac:NetCashFlowFromOperatingActivitiesContinuing", +1),
      ("fac:NetCashFlowFromInvestingActivitiesContinuing", +1),
      ("fac:NetCashFlowFromFinancingActivitiesContinuing", +1),
    ),
  ),
  Rule(
    "CF3",
    "Net Cash Flows Discontinued = Operating + Investing + Financing (Discontinued)",
    "fac:NetCashFlowDiscontinued",
    (
      ("fac:NetCashFlowFromOperatingActivitiesDiscontinued", +1),
      ("fac:NetCashFlowFromInvestingActivitiesDiscontinued", +1),
      ("fac:NetCashFlowFromFinancingActivitiesDiscontinued", +1),
    ),
  ),
  Rule(
    "CF4",
    "Operating Net Cash Flow = Continuing + Discontinued",
    "fac:NetCashFlowFromOperatingActivities",
    (
      ("fac:NetCashFlowFromOperatingActivitiesContinuing", +1),
      ("fac:NetCashFlowFromOperatingActivitiesDiscontinued", +1),
    ),
  ),
  Rule(
    "CF5",
    "Investing Net Cash Flow = Continuing + Discontinued",
    "fac:NetCashFlowFromInvestingActivities",
    (
      ("fac:NetCashFlowFromInvestingActivitiesContinuing", +1),
      ("fac:NetCashFlowFromInvestingActivitiesDiscontinued", +1),
    ),
  ),
  Rule(
    "CF6",
    "Financing Net Cash Flow = Continuing + Discontinued",
    "fac:NetCashFlowFromFinancingActivities",
    (
      ("fac:NetCashFlowFromFinancingActivitiesContinuing", +1),
      ("fac:NetCashFlowFromFinancingActivitiesDiscontinued", +1),
    ),
  ),
)


SEED_PATH = (
  Path(__file__).resolve().parent.parent
  / "taxonomy/seeds/fac-calculations/v1/taxonomy.jsonld"
)

RS_VOCAB = "https://robosystems.ai/vocab/"
CM_ROLES = "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/"


def _context() -> dict:
  return {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "fac": "http://www.xbrlsite.com/fac#",
    "rs": RS_VOCAB,
    "summationOf": {"@id": f"{RS_VOCAB}summationOf", "@type": "@id"},
    "structureName": {"@id": f"{RS_VOCAB}structureName"},
    "structureType": {"@id": f"{RS_VOCAB}structureType"},
    "roleUri": {"@id": f"{RS_VOCAB}roleUri"},
    "label": "rdfs:label",
    "documentation": "rdfs:comment",
  }


def build() -> dict:
  # One structure per rule so users can see which identity an arc
  # belongs to. Role URI + name are what the loader extracts.
  structures: list[dict] = []
  for rule in RULES:
    structures.append(
      {
        "@id": f"_:fac-calc-{rule.code.lower()}",
        "roleUri": f"{CM_ROLES}fac-calculations/{rule.code}",
        "structureName": f"FAC {rule.code} — {rule.doc}",
        "structureType": (
          "balance_sheet"
          if rule.code.startswith("BS")
          else "income_statement"
          if rule.code.startswith("IS")
          else "cash_flow_statement"
        ),
      }
    )

  # Collapse by head element — the current JSON-LD arc format puts
  # summationOf as a predicate list on the head, so multiple rules
  # sharing a head (Assets in BS2+BS3, OperatingIncomeLoss in IS2+IS11)
  # merge their children. The rule→arc provenance lives in the
  # per-rule Structure's name, not on the arc itself.
  by_head: dict[str, list[str]] = {}
  for rule in RULES:
    bucket = by_head.setdefault(rule.head, [])
    for child, _weight in rule.children:
      if child not in bucket:
        bucket.append(child)

  arc_nodes: list[dict] = [
    {
      "@id": head,
      "summationOf": [{"@id": child} for child in children],
    }
    for head, children in sorted(by_head.items())
  ]

  doc = {
    "@context": _context(),
    "standard": "fac-calculations",
    "version": "v1",
    "namespace_uri": "http://www.xbrlsite.com/fac/calculations/",
    "taxonomy_type": "mapping",
    "description": (
      "FAC's BS/IS/CF accounting identity rules rendered as calculation "
      "arcs. 22 identities × ~2.5 children each = ~55 summationOf arcs "
      "from aggregate heads to their primitive constituents. Structures "
      "carry the human-readable identity; arcs carry the topology."
    ),
    "@graph": structures + arc_nodes,
  }
  return doc


def main() -> int:
  doc = build()
  SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
  SEED_PATH.write_text(json.dumps(doc, indent=2))
  n_arcs = sum(len(n["summationOf"]) for n in doc["@graph"] if "summationOf" in n)
  n_structs = sum(1 for n in doc["@graph"] if "roleUri" in n)
  print(f"Wrote {SEED_PATH}")
  print(f"  structures: {n_structs}  arcs: {n_arcs}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
