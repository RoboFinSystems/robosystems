"""What each cash flow row should say, from the journal lines and the taxonomy.

Investing and financing are stated gross, one row per flow, so they come from
the lines: a line's own flow tag, or the default flow the taxonomy gives the
concept its account reports under. Operating is indirect: net income, the
income statement rows it adds back, and the change in each working-capital
balance. The taxonomy's arcs are data, read from the library; nothing here
calls the reporting engine.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field

from .expected import CASH_CONCEPTS, Ledger

OPERATING = "operating"
INVESTING = "investing"
FINANCING = "financing"
PAYABLES = frozenset({"rs-gaap:AccountsPayableCurrent"})
# ASC 230: a business bought or sold is one row, net of the cash it held.
ACQUISITION = "rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"
DIVESTITURE = "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates"
_SECTION_OF_SUBTOTAL = {
  "rs-gaap:NetCashProvidedByUsedInOperatingActivities": OPERATING,
  "rs-gaap:NetCashProvidedByUsedInInvestingActivities": INVESTING,
  "rs-gaap:NetCashProvidedByUsedInFinancingActivities": FINANCING,
}


@dataclass(frozen=True)
class JournalLine:
  owner: str
  code: str
  debit: float
  credit: float
  flow: str | None = None

  @property
  def cash_effect(self) -> float:
    """What a non-cash line did to cash: a credit brought it in."""
    return self.credit - self.debit


@dataclass
class Flows:
  """The cash flow statement's shape in the taxonomy: which section each row
  sits in and at what weight, and each balance concept's default rows."""

  section: dict[str, str] = field(default_factory=dict)
  weight: dict[str, float] = field(default_factory=dict)
  defaults: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
  derived_from: dict[str, list[str]] = field(default_factory=dict)

  @classmethod
  def from_arcs(cls, arcs: Iterable[Mapping]) -> Flows:
    flows = cls()
    for arc in arcs:
      parent, child = arc["fromElementQname"], arc["toElementQname"]
      weight = float(arc.get("weight") or 1.0)
      if arc.get("associationType") == "calculation":
        section = _SECTION_OF_SUBTOTAL.get(parent)
        if section:
          flows.section[child] = section
          flows.weight[child] = weight
      elif arc.get("associationType") == "derivation":
        flows.defaults.setdefault(child, []).append((parent, weight))
        flows.derived_from.setdefault(parent, []).append(child)
    return flows

  @property
  def working(self) -> frozenset[str]:
    """The balance concepts whose change is an operating row."""
    return frozenset(
      concept
      for flow, concepts in self.derived_from.items()
      if self.section.get(flow) == OPERATING
      for concept in concepts
    )

  def default_for(self, concept: str, cash_effect: float) -> str | None:
    """The row a change in ``concept`` lands on by default: the arc whose
    sign is the direction the cash moved."""
    for flow, weight in self.defaults.get(concept, []):
      if (weight > 0) == (cash_effect > 0):
        return flow
    return None


@dataclass
class LineFlows:
  """What a month's entries say about the cash flow rows.

  ``paid``: investing and financing rows from entries that move cash.
  ``billed``: the same rows from bills, which reach the statement when paid.
  ``backed_out``: by working-capital concept, the part of its change that an
  entry with no cash line set against an investing or financing line. That
  part is not an operating flow, so it comes out of the concept's row."""

  paid: dict[str, float] = field(default_factory=dict)
  billed: dict[str, float] = field(default_factory=dict)
  backed_out: dict[str, float] = field(default_factory=dict)


def line_flows(
  entries: Iterable[Iterable[JournalLine]],
  concept_of: Callable[[JournalLine], str | None],
  flows: Flows,
  eliminating: Collection[str] = (),
) -> LineFlows:
  """Read the entries for their investing and financing flows.

  An entry that moves cash: a tagged cash line is its own flow, and an
  untagged one leaves each other line to its tag or its concept's default.
  An entry with no cash line moves none, so its investing or financing lines
  are matched to the working-capital lines on the other side and backed out
  of them; set against payables, they are a bill.

  An entity in ``eliminating`` is a group's consolidation entity. Its entries
  move no cash; each tagged line takes that much off, or adds it to, the row
  the members' own entries put it on."""
  out = LineFlows()
  working = flows.working

  def gross(flow: str | None) -> bool:
    return flows.section.get(flow or "") in (INVESTING, FINANCING)

  def add(to: dict[str, float], key: str | None, amount: float) -> None:
    if key and amount:
      to[key] = to.get(key, 0.0) + amount

  def flow_of(line: JournalLine) -> str | None:
    concept = concept_of(line)
    default = flows.default_for(concept, line.cash_effect) if concept else None
    flow = line.flow or default
    return flow if gross(flow) else None

  for entry in entries:
    lines = list(entry)
    cash: list[JournalLine] = []
    others: list[JournalLine] = []
    for line in lines:
      (cash if concept_of(line) in CASH_CONCEPTS else others).append(line)
    if cash:
      moved = sum(abs(line.debit - line.credit) for line in cash)
      tagged = [line for line in cash if line.flow]
      for line in tagged:
        if gross(line.flow):
          add(out.paid, line.flow, line.debit - line.credit)
      # A line carrying a cash line's tag is that cash line's other side and
      # is already counted. The rest answer for the untagged cash.
      claimed = {line.flow for line in tagged}
      if len(tagged) < len(cash):
        for line in others:
          if line.flow not in claimed:
            add(out.paid, flow_of(line), line.cash_effect)
      # A tagged payment of a bill: the payable it clears was the row's, not
      # operations', and the bill is no longer owed.
      for payment in tagged:
        if not gross(payment.flow) or not moved:
          continue
        share = abs(payment.debit - payment.credit) / moved
        for line in others:
          if concept_of(line) in PAYABLES:
            add(out.backed_out, concept_of(line), -line.cash_effect * share)
            add(out.billed, payment.flow, -line.cash_effect * share)
      continue
    if lines and lines[0].owner in eliminating:
      for line in lines:
        if gross(line.flow):
          add(out.paid, line.flow, line.cash_effect)
      continue
    for line in others:
      flow = flow_of(line)
      if not flow:
        continue
      against = [
        w
        for w in others
        if concept_of(w) in working and w.cash_effect * line.cash_effect < 0
      ]
      total = sum(abs(w.cash_effect) for w in against)
      if not total:
        continue
      matched = min(abs(line.cash_effect), total)
      sign = 1.0 if line.cash_effect > 0 else -1.0
      for w in against:
        add(out.backed_out, concept_of(w), sign * matched * abs(w.cash_effect) / total)
      if any(concept_of(w) in PAYABLES for w in against):
        add(out.billed, flow, sign * matched)
  return out


def settle(
  stated: Mapping[str, float],
  paid: Mapping[str, float],
  owed: dict[str, float],
  tolerance: float,
) -> tuple[list[tuple[str, float, float]], float]:
  """Each row the ledger's lines do not explain, as (row, stated, paid), and
  how much of the open bills the month settled. A row may run ahead of the
  cash entries by what is still owed on bills for it; that much is taken off
  ``owed``."""
  unexplained: list[tuple[str, float, float]] = []
  settled = 0.0
  for flow in sorted(stated.keys() | paid.keys()):
    actual, expected = stated.get(flow, 0.0), paid.get(flow, 0.0)
    extra = actual - expected
    if abs(extra) <= tolerance:
      continue
    open_bills = owed.get(flow, 0.0)
    if extra * open_bills > 0 and abs(extra) <= abs(open_bills) + tolerance:
      owed[flow] = open_bills - extra
      settled += extra
      continue
    unexplained.append((flow, actual, expected))
  return unexplained, settled


def operating_rows(
  rows: Iterable[Mapping],
  income: Mapping[str, float],
  before: Ledger,
  at_end: Ledger,
  flows: Flows,
  backed_out: Mapping[str, float] | None = None,
) -> list[tuple[str, float, float]]:
  """(row, stated, expected) for each operating row that can be worked out:
  a row the income statement also carries is that figure at the row's
  weight, and a working-capital row is the cash effect of its balances'
  change, less what ``backed_out`` says was never an operating flow. A
  working-capital row the statement leaves out is stated as nothing. The
  reconciling item and other cash-flow-only rows are left out."""
  opening, closing = before.placed(), at_end.placed()
  backed_out = backed_out or {}
  stated = {
    fact["qname"]: float((fact.get("values") or [0.0])[0] or 0.0)
    for fact in rows
    if flows.section.get(fact["qname"]) == OPERATING and not fact.get("is_subtotal")
  }
  out: list[tuple[str, float, float]] = []
  for qname in [*stated, *(q for q in flows.derived_from if q not in stated)]:
    if flows.section.get(qname) != OPERATING:
      continue
    if qname in income:
      expected = flows.weight.get(qname, 1.0) * income[qname]
    elif qname in flows.derived_from and qname in flows.weight:
      concepts = flows.derived_from[qname]
      change = sum(
        closing.get(c, (0.0, ""))[0] - opening.get(c, (0.0, ""))[0] for c in concepts
      )
      expected = sum(backed_out.get(c, 0.0) for c in concepts) - change
    else:
      continue
    out.append((qname, stated.get(qname, 0.0), expected))
  return out
