"""What the statements should say, worked out from the trial balance alone.

Nothing here calls the reporting engine. A trial balance row is an account's
debits and credits; the chart-of-accounts mapping says which concept each
account reports under; the rest is addition. The checks compare the engine's
rendered rows with these sums.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
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

LIBRARY = "rs-gaap:"

# The two sides of a balance one entity of a group holds against another,
# and the trait of each.
INTERCOMPANY = {
  "rs-gaap:DueFromAffiliateCurrent": ("rs-gaap:DueToAffiliateCurrent", "liability"),
  "rs-gaap:DueToAffiliateCurrent": ("rs-gaap:DueFromAffiliateCurrent", "asset"),
}


def base_trait(trait: str | None) -> str:
  """A contra account belongs to the trait it offsets: ``contraAsset`` is an asset."""
  trait = trait or ""
  if trait.startswith("contra") and len(trait) > 6:
    return trait[6].lower() + trait[7:]
  return trait


@dataclass(frozen=True)
class Line:
  """One account: the concepts it is mapped to, its trait, its net debits."""

  targets: tuple[str, ...]
  trait: str
  net: float


@dataclass
class Ledger:
  """Net debits (debits less credits) of a set of accounts, keyed by the
  entity that owns each and its code."""

  lines: dict[tuple[str, str], Line] = field(default_factory=dict)

  @property
  def by_trait(self) -> dict[str, float]:
    totals: dict[str, float] = {}
    for line in self.lines.values():
      totals[line.trait] = totals.get(line.trait, 0.0) + line.net
    return totals

  def natural(self, trait: str) -> float:
    """The trait's total on its own side: assets as debits, revenue as credits."""
    net = self.by_trait.get(trait, 0.0)
    return net if trait in DEBIT_TRAITS else -net

  @property
  def earnings(self) -> float:
    by_trait = self.by_trait
    return -sum(by_trait.get(t, 0.0) for t in EARNINGS_TRAITS)

  @property
  def equity(self) -> float:
    """Equity accounts plus the earnings no closing entry has moved into them."""
    return self.natural("equity") + self.earnings

  @property
  def cash(self) -> float:
    return sum(
      line.net
      for line in self.lines.values()
      if CASH_CONCEPTS.intersection(line.targets)
    )

  @property
  def unmapped(self) -> dict[str, float]:
    return {
      code: line.net
      for (_owner, code), line in self.lines.items()
      if not line.targets and abs(line.net) > 0.005
    }

  def placed(self, shown: Collection[str] = ()) -> dict[str, tuple[float, str]]:
    """Each concept's net debits and trait. An account mapped to several
    concepts (a tenant's own member beside the library concept it rolls into)
    counts once: on the one the statement shows, the tenant's own first."""
    out: dict[str, tuple[float, str]] = {}
    for line in self.lines.values():
      if not line.targets:
        continue
      target = next((t for t in line.targets if t in shown), line.targets[-1])
      net, trait = out.get(target, (0.0, line.trait))
      out[target] = (net + line.net, trait)
    return out


def _targets(mapped: str | Sequence[str] | None) -> tuple[str, ...]:
  if not mapped:
    return ()
  names = [mapped] if isinstance(mapped, str) else list(mapped)
  names = list(dict.fromkeys(PRESENTED_AS.get(n, n) for n in names))
  return tuple(sorted(names, key=lambda n: n.startswith(LIBRARY)))


def fold(
  rows: Iterable[Mapping],
  concepts_by_code: Mapping[str, str | Sequence[str]],
  owner: str = "",
) -> Ledger:
  """One entity's trial balance rows as lines, each with its mapped concepts."""
  ledger = Ledger()
  for row in rows:
    net = float(row.get("totalDebits") or 0) - float(row.get("totalCredits") or 0)
    code = row["accountCode"]
    ledger.lines[(owner, code)] = Line(
      _targets(concepts_by_code.get(code)), base_trait(row.get("trait")), net
    )
  return ledger


def combine(ledgers: Iterable[Ledger]) -> Ledger:
  total = Ledger()
  for ledger in ledgers:
    total.lines.update(ledger.lines)
  return total


def less(later: Ledger, earlier: Ledger) -> Ledger:
  """The activity between two cumulative ledgers."""
  period = Ledger()
  for key in later.lines.keys() | earlier.lines.keys():
    after, before = later.lines.get(key), earlier.lines.get(key)
    shape = after or before
    assert shape is not None
    period.lines[key] = Line(
      shape.targets,
      shape.trait,
      (after.net if after else 0.0) - (before.net if before else 0.0),
    )
  return period


def continuing_opening(staying: Iterable[Ledger], leaving: Iterable[Ledger]) -> Ledger:
  """The opening balances a month's flows are measured from, for the
  entities in the group at its end.

  A member that left takes its own balances with it: they are not a flow.
  What the others held against it is no longer a balance inside the group,
  so the elimination of both sides is taken back and the other side stands
  as what it now is, owed by or to an outside party."""
  opening = combine(staying)
  for ledger in leaving:
    for (owner, code), line in ledger.lines.items():
      own = next((t for t in line.targets if t in INTERCOMPANY), None)
      if own is None or not line.net:
        continue
      other, other_trait = INTERCOMPANY[own]
      opening.lines[(f"left:{owner}", code)] = Line((own,), line.trait, line.net)
      opening.lines[(f"left:{owner}", f"{code}:other")] = Line(
        (other,), other_trait, -line.net
      )
  return opening
