"""Curate FAC seed with the three classification axes.

Pattern-based heuristic that stamps `classification` (economic_nature),
`statementContext`, and `derivationRole` on every `fac:*` node in the
JSON-LD seed. Idempotent: re-running overwrites previous values based
on current rules, so tweaking a rule and re-running is safe.

Rules, first-match-wins:

1. Entity/document/fiscal identifiers → metadata + identifier, no economic nature
2. Return/ratio/growth computations → analysis + ratio, no economic nature
3. NetCashFlow* / CashFlowsFrom* → cash_flow + reconciliation
4. IncreaseDecrease* → cash_flow + movement
5. NetIncome* (top-line totals) → income_statement + total
6. Known subtotals (GrossProfit, OperatingIncomeLoss, IncomeLossFromContinuing*, …) → income_statement + subtotal
7. OtherComprehensive* → equity_changes + primitive
8. Structural abstracts (*RollUp, *LineItems, *Hypercube, *Table, *Arithmetic, *RollForward, *BreakdownLineItems, *BreakdownTable) → derivation_role=structural, economic_nature null, statement_context inferred from prefix
9. BS1-5 / IS1-11 / CF1-6 format placeholders → disclosure + identifier
10. Fallback primitives — SFAC 6 bucket inferred from existing classification, statement_context from balance+period, derivation_role=primitive

Usage:
  uv run python -m robosystems.scripts.curate_fac_axes
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

SEED_PATH = (
  Path(__file__).resolve().parent.parent / "taxonomy/seeds/fac/v1/taxonomy.jsonld"
)

RS_VOCAB = "https://robosystems.ai/vocab/"
CLS = f"{RS_VOCAB}classification"
SC = f"{RS_VOCAB}statementContext"
DR = f"{RS_VOCAB}derivationRole"
BAL = f"{RS_VOCAB}balance"
PER = f"{RS_VOCAB}periodType"
ABS = f"{RS_VOCAB}abstract"
MON = f"{RS_VOCAB}monetary"


def _single(node: dict, key: str) -> str | bool | None:
  vals = node.get(key, [])
  if not vals:
    return None
  v = vals[0]
  return v.get("@value") if isinstance(v, dict) else None


# ──────────────────────────────────────────────────────────────────────
# Pattern tables
# ──────────────────────────────────────────────────────────────────────

# Known income-statement subtotals — RollUp heads one level above primitives.
# Both subtotals and totals collapse to derivation_role='aggregate'; these
# two sets are kept separate only so statement_context inference stays
# correct (Comprehensive totals live on equity_changes).
SUBTOTAL_NAMES = {
  "GrossProfit",
  "OperatingIncomeLoss",
  "NonoperatingIncomeLoss",
  "NonoperatingIncomeLossPlusInterestAndDebtExpense",
  "NonoperatingIncomePlusInterestAndDebtExpensePlusIncomeFromEquityMethodInvestments",
  "IncomeLossBeforeEquityMethodInvestments",
  "IncomeLossFromContinuingOperationsBeforeTax",
  "IncomeLossFromContinuingOperationsAfterTax",
  "IncomeLossFromEquityMethodInvestments",
  "IncomeLossFromDiscontinuedOperationsNetOfTax",
  "IncomeLossFromDiscontinuedOperationsNetOfTaxAdjustmentToPriorYearGainLossOnDisposal",
  "IncomeLossFromDiscontinuedOperationsNetOfTaxDuringPhaseOut",
  "IncomeLossFromDiscontinuedOperationsNetOfTaxGainLossOnDisposal",
  "IncomeLossFromDiscontinuedOperationsNetOfTaxProvisionForGainLossOnDisposal",
  "IncomeLossAttributableToNoncontrollingInterestPlusPreferredStockDividendsAndOtherAdjustments",
  "NetIncomeLossAttributableToNoncontrollingInterestPlusPreferredStockDividendsAndOtherAdjustments",
  "CostsAndExpenses",
  "IndirectOperatingNonoperatingCostsExpenses",
  "OperatingAndNonoperatingCostsAndExpenses",
  "OperatingAndNonoperatingRevenues",
  "BenefitsCostsExpenses",
  "OtherOperatingIncomeExpenses",
  "GainLossOnSalePreviouslyUnissuedStockBySubsidiaryOrEquityInvesteeNonoperatingIncome",
  "RevenuesNetInterestExpense",
  "InterestIncomeExpenseAfterProvisionForLosses",
  "InterestIncomeExpenseOperatingNet",
}

# Income-statement totals (the bottom-line result).
TOTAL_NAMES = {
  "NetIncomeLoss",
  "NetIncomeLossAttributableToParent",
  "NetIncomeLossAttributableToNoncontrollingInterest",
  "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest",
  "NetIncomeLossAttributableToRedeemableNoncontrollingInterest",
  "NetIncomeLossAvailableToCommonStockholdersBasic",
  "NetIncomeLossAvailableToCommonStockholdersBasicAfterAllocations",
  "ComprehensiveIncomeLoss",
  "ComprehensiveIncomeLossAttributableToParent",
  "ComprehensiveIncomeLossAttributableToNoncontrollingInterest",
  "ComprehensiveIncomeNetOfTax",
  "ComprehensiveIncomeNetOfTaxAttributableToParent",
  "ComprehensiveIncomeNetOfTaxAttributableToNoncontrollingInterest",
  "ParticipatingSecuritiesDistributedAndUndistributedEarningsLossBasic",
  "UndistributedEarningsLossAllocatedToParticipatingSecuritiesBasic",
}

METADATA_NAMES = {
  "BalanceSheetDate",
  "BalanceSheetFormat",
  "DocumentType",
  "EntityCentralIndexKey",
  "EntityFilerCategory",
  "EntityRegistrantName",
  "FiscalPeriodFocus",
  "FiscalYearEnd",
  "FiscalYearFocus",
  "IncomeStatementFormat",
  "IncomeStatementStartPeriodYearToDate",
  "TradingSymbol",
}

RATIO_NAMES = {
  "ReturnOnAssets",
  "ReturnOnEquity",
  "ReturnOnSales",
  "SustainableGrowthRate",
}

# BS1-5, IS1-11, CF1-6 — legacy structure-format numeric placeholders.
_FORMAT_CODE_RE = re.compile(r"^(BS|IS|CF)[0-9]+$")

# Structural suffixes (appear on abstracts + hypercubes).
STRUCTURAL_SUFFIXES = (
  "RollUp",
  "LineItems",
  "Hypercube",
  "Table",
  "BreakdownLineItems",
  "BreakdownTable",
  "RollForward",
  "Arithmetic",
)


def _statement_from_prefix(local: str) -> str | None:
  """Infer statement_context for structural abstracts from the name prefix."""
  lower = local.lower()
  if (
    lower.startswith("balancesheet")
    or lower.startswith("assets")
    or lower.startswith("liabilities")
    or lower.startswith("capitalization")
    or lower == "equity"
    or lower.startswith("equityrollup")
  ):
    return "balance_sheet"
  if lower.startswith("cashflow") or lower.startswith("netcashflow"):
    return "cash_flow"
  if (
    lower.startswith("changesinequity")
    or lower.startswith("investmentsbyowners")
    or lower.startswith("distributionstoowners")
    or lower.startswith("comprehensiveincome")
    or lower.startswith("othercomprehensive")
    or lower.startswith("statementcomprehensiveincome")
  ):
    return "equity_changes"
  if (
    lower.startswith("incomestatement")
    or lower.startswith("revenues")
    or lower.startswith("cost")
    or lower.startswith("netincome")
    or lower.startswith("operatingincome")
    or lower.startswith("nonoperatingincome")
    or lower.startswith("grossprofit")
    or lower.startswith("incomeloss")
    or lower.startswith("interestincomeexpense")
    or lower.startswith("benefits")
  ):
    return "income_statement"
  if lower.startswith("keyratios"):
    return "analysis"
  if (
    lower.startswith("basicinformation")
    or lower.startswith("generalinformation")
    or lower.startswith("otherinformation")
    or lower.startswith("validationresults")
  ):
    return "disclosure"
  return None


def _collapse_nature(classification: str | None) -> str | None:
  """Collapse legacy SFAC 6 R/E/G/L values to inflow/outflow."""
  if classification in {"revenue", "gain"}:
    return "inflow"
  if classification in {"expense", "loss"}:
    return "outflow"
  return classification


def _inferred_context(
  classification: str | None, balance: str | None, period: str | None
) -> str | None:
  """Default statement_context from economic_nature + balance/period."""
  if classification in {"asset", "liability"}:
    return "balance_sheet"
  if classification == "equity":
    return "balance_sheet"
  if classification in {"inflow", "outflow"}:
    return "income_statement"
  if classification == "cashflow":
    return "cash_flow"
  # Heuristic fallback for things with balance/period but no classification.
  if period == "instant":
    return "balance_sheet"
  if period == "duration":
    return "income_statement"
  return None


def classify(
  qname: str,
  local: str,
  classification: str | None,
  balance: str | None,
  period: str | None,
  abstract: bool,
  element_type: str | None,
) -> tuple[str | None, str | None, str | None]:
  """Return (economic_nature, statement_context, derivation_role)."""

  # Normalize the incoming classification to the 6-value vocabulary.
  classification = _collapse_nature(classification)

  # Rule 1: entity/document/fiscal metadata identifiers
  if local in METADATA_NAMES:
    return (None, "metadata", "identifier")

  # Rule 2: computed ratios / analytical metrics
  if local in RATIO_NAMES:
    return (None, "analysis", "ratio")

  # Rule 9: BS1-5 / IS1-11 / CF1-6 format codes
  if _FORMAT_CODE_RE.match(local):
    return (None, "disclosure", "identifier")

  # Rule 3: cash flow aggregates — NetCashFlow* / CashFlowsFrom* are
  # RollUp heads summing individual investing/financing/operating
  # movements. They're aggregates, not primitive movements.
  if local.startswith("NetCashFlow") or local.startswith("CashFlowsFrom"):
    if abstract or any(local.endswith(s) for s in STRUCTURAL_SUFFIXES):
      return (None, "cash_flow", "structural")
    return ("cashflow", "cash_flow", "aggregate")

  # Rule 4: period-over-period deltas (used in indirect CF method) —
  # IncreaseDecreaseInInventory, IncreaseDecreaseInAccountsReceivable,
  # etc. These are primitive leaves on the CF statement; the delta-ness
  # is carried by economic_nature=cashflow.
  if local.startswith("IncreaseDecrease"):
    return ("cashflow", "cash_flow", "primitive")

  # Rule 8: structural abstracts + hypercubes (RollUp, LineItems, …)
  if abstract or element_type in {"abstract", "hypercube", "axis", "member"}:
    return (None, _statement_from_prefix(local), "structural")

  # Rule 7: OCI primitives — equity-change flows, not income-statement
  # primitives. OCI bypasses net income and lands directly in AOCI.
  # Direction (inflow/outflow) follows balance_type since these are
  # duration flows, not balance-sheet stocks.
  if local.startswith("OtherComprehensiveIncomeLoss"):
    flow = "outflow" if balance == "debit" else "inflow"
    return (flow, "equity_changes", "primitive")

  # Rule 6: named subtotals + totals — both are RollUp heads. The
  # subtotal-vs-total distinction is positional (depth in a RollUp
  # chain), derivable from the Structure graph when populated.
  if local in SUBTOTAL_NAMES:
    return (classification, "income_statement", "aggregate")

  if local in TOTAL_NAMES:
    ctx = "equity_changes" if "Comprehensive" in local else "income_statement"
    return (classification, ctx, "aggregate")

  # Rule 10: fallback — concrete primitive. Keep existing SFAC 6 bucket,
  # derive statement_context, role=primitive.
  return (
    classification,
    _inferred_context(classification, balance, period),
    "primitive",
  )


def _set_predicate(node: dict, key: str, value: str | None) -> None:
  if value is None:
    node.pop(key, None)
  else:
    node[key] = [{"@value": value}]


def curate(doc: dict) -> tuple[int, Counter]:
  """Mutate doc in place. Returns (count_updated, counter_of_(role_ctx))."""
  concepts = [
    n
    for n in doc.get("@graph", [])
    if isinstance(n.get("@id"), str) and n["@id"].startswith("fac:")
  ]
  updated = 0
  tallies: Counter = Counter()
  for node in concepts:
    qname = node["@id"]
    local = qname.split(":", 1)[1]
    cls = _single(node, CLS)
    bal = _single(node, BAL)
    per = _single(node, PER)
    abstract = bool(_single(node, ABS))
    element_type = _single(node, f"{RS_VOCAB}elementType")

    cls_v = cls if isinstance(cls, str) else None
    bal_v = bal if isinstance(bal, str) else None
    per_v = per if isinstance(per, str) else None
    et_v = element_type if isinstance(element_type, str) else None

    econ, ctx, role = classify(qname, local, cls_v, bal_v, per_v, abstract, et_v)
    _set_predicate(node, CLS, econ)
    _set_predicate(node, SC, ctx)
    _set_predicate(node, DR, role)
    updated += 1
    tallies[(ctx or "—", role or "—")] += 1

  # Ensure @context has the new predicates.
  ctx_doc = doc.get("@context", {})
  if isinstance(ctx_doc, dict):
    ctx_doc.setdefault("statementContext", {"@id": f"{RS_VOCAB}statementContext"})
    ctx_doc.setdefault("derivationRole", {"@id": f"{RS_VOCAB}derivationRole"})

  return updated, tallies


def main() -> int:
  doc = json.loads(SEED_PATH.read_text())
  updated, tallies = curate(doc)
  SEED_PATH.write_text(json.dumps(doc, indent=2))

  print(f"Curated {updated} FAC elements → {SEED_PATH}")
  print()
  print("(statement_context, derivation_role) distribution:")
  for (ctx, role), n in sorted(tallies.items(), key=lambda x: (-x[1], x[0])):
    print(f"  {ctx:20s} / {role:15s} : {n}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
