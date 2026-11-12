"""What the statements should say, worked out from the trial balance alone.

Nothing here calls the reporting engine. A trial balance row is an account's
debits and credits; the chart-of-accounts mapping says which concept each
account reports under; the rest is addition. The checks compare the engine's
rendered rows with these sums.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

DEBIT_TRAITS = frozenset({"asset", "expense", "loss"})
CREDIT_TRAITS = frozenset({"liability", "equity", "revenue", "gain"})
BALANCE_TRAITS = frozenset({"asset", "liability", "equity"})
EARNINGS_TRAITS = frozenset({"revenue", "expense", "gain", "loss"})

# Concepts the statements never show on their own row: the balance sheet
# states property net of its accumulated depreciation.
PRESENTED_AS = {
  "rs-gaap:PropertyPlantAndEquipmentGross": "rs-gaap:PropertyPlantAndEquipmentNet",
  "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment": (
    "rs-gaap:PropertyPlantAndEquipmentNet"
  ),
}


# The two rows the cash flow statement's cash total sums, by the taxonomy's
# own calculation arcs.
CASH_CONCEPTS = frozenset(
  {
    "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
    "rs-gaap:RestrictedCashAndInvestmentsCurrent",
  }
)


@dataclass
class Ledger:
  """Net debits (debits less credits) of a set of accounts."""

  by_concept: dict[str, float] = field(default_factory=dict)
  by_trait: dict[str, float] = field(default_factory=dict)
  concept_trait: dict[str, str] = field(default_factory=dict)
  cash: float = 0.0
  unmapped: dict[str, float] = field(default_factory=dict)

  def natural(self, trait: str) -> float:
    """The trait's total on its own side: assets as debits, revenue as credits."""
    net = self.by_trait.get(trait, 0.0)
    return net if trait in DEBIT_TRAITS else -net

  @property
  def earnings(self) -> float:
    return -sum(self.by_trait.get(t, 0.0) for t in EARNINGS_TRAITS)

  @property
  def equity(self) -> float:
    """Equity accounts plus the earnings no closing entry has moved into them."""
    return self.natural("equity") + self.earnings

  def concept(self, qname: str, trait: str) -> float:
    net = self.by_concept.get(qname, 0.0)
    return net if trait in DEBIT_TRAITS else -net


def fold(rows: Iterable[Mapping], concept_by_code: Mapping[str, str]) -> Ledger:
  """One entity's trial balance rows, summed by concept and by account trait."""
  ledger = Ledger()
  for row in rows:
    net = float(row.get("totalDebits") or 0) - float(row.get("totalCredits") or 0)
    trait = row.get("trait") or ""
    ledger.by_trait[trait] = ledger.by_trait.get(trait, 0.0) + net
    qname = concept_by_code.get(row["accountCode"])
    if qname is None:
      if abs(net) > 0.005:
        ledger.unmapped[row["accountCode"]] = net
      continue
    qname = PRESENTED_AS.get(qname, qname)
    ledger.by_concept[qname] = ledger.by_concept.get(qname, 0.0) + net
    ledger.concept_trait.setdefault(qname, trait)
    if qname in CASH_CONCEPTS:
      ledger.cash += net
  return ledger


def combine(ledgers: Iterable[Ledger]) -> Ledger:
  total = Ledger()
  for ledger in ledgers:
    for qname, net in ledger.by_concept.items():
      total.by_concept[qname] = total.by_concept.get(qname, 0.0) + net
    for trait, net in ledger.by_trait.items():
      total.by_trait[trait] = total.by_trait.get(trait, 0.0) + net
    for qname, trait in ledger.concept_trait.items():
      total.concept_trait.setdefault(qname, trait)
    total.cash += ledger.cash
    total.unmapped.update(ledger.unmapped)
  return total


def less(later: Ledger, earlier: Ledger) -> Ledger:
  """The activity between two cumulative ledgers."""
  period = Ledger(concept_trait={**earlier.concept_trait, **later.concept_trait})
  for qname in later.by_concept.keys() | earlier.by_concept.keys():
    period.by_concept[qname] = later.by_concept.get(
      qname, 0.0
    ) - earlier.by_concept.get(qname, 0.0)
  for trait in later.by_trait.keys() | earlier.by_trait.keys():
    period.by_trait[trait] = later.by_trait.get(trait, 0.0) - earlier.by_trait.get(
      trait, 0.0
    )
  period.cash = later.cash - earlier.cash
  period.unmapped = dict(later.unmapped)
  return period
