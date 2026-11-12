"""The comparisons: one rendered statement against the ledger's own sums."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass

from .cashflow import (
  FINANCING,
  INVESTING,
  PAYABLES,
  Flows,
  LineFlows,
  operating_rows,
  settle,
)
from .expected import (
  BALANCE_TRAITS,
  DEBIT_TRAITS,
  EARNINGS_TRAITS,
  Ledger,
)

TOLERANCE = 0.015
NET_INCOME = "rs-gaap:NetIncomeLoss"
CASH_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
RECONCILING_ITEM = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"


@dataclass
class Check:
  scope: str
  month: str
  statement: str
  name: str
  passed: bool
  detail: str

  def as_dict(self) -> dict:
    return asdict(self)


def _first(fact: dict) -> float:
  values = fact.get("values") or []
  return float(values[0]) if values and values[0] is not None else 0.0


def _rows(statement: dict) -> list[dict]:
  return [f for f in statement.get("facts") or [] if not f.get("period_role")]


def _value(statement: dict, qname: str) -> float | None:
  for fact in _rows(statement):
    if fact.get("qname") == qname:
      return _first(fact)
  return None


def _stated(statement: dict) -> bool:
  """False when the engine declined the window: before the books begin."""
  return (statement.get("validation") or {}).get("status") != "inconclusive"


def _money(value: float) -> str:
  return f"{value:,.2f}"


def _close(actual: float, expected: float) -> bool:
  return abs(actual - expected) <= TOLERANCE


def expected_rows(statement: dict, ledger: Ledger) -> list[tuple[dict, float]]:
  """Each row's net debits in the ledger: its own concept's accounts plus the
  rows beneath it. A statement lists children before their parent, so the
  deeper rows still waiting when a row arrives are its children."""
  rows = _rows(statement)
  placed = ledger.placed({fact["qname"] for fact in rows})
  waiting: list[tuple[int, float]] = []
  out: list[tuple[dict, float]] = []
  for fact in rows:
    depth = int(fact.get("depth") or 0)
    net = placed.get(fact["qname"], (0.0, ""))[0]
    while waiting and waiting[-1][0] > depth:
      net += waiting.pop()[1]
    waiting.append((depth, net))
    out.append((fact, net))
  return out


class Checker:
  """Collects the checks for one scope and month."""

  def __init__(self, scope: str, month: str) -> None:
    self.scope = scope
    self.month = month
    self.checks: list[Check] = []

  def _add(self, statement: str, name: str, passed: bool, detail: str) -> None:
    self.checks.append(Check(self.scope, self.month, statement, name, passed, detail))

  def _equal(self, statement: str, name: str, actual: float, expected: float) -> None:
    self._add(
      statement,
      name,
      _close(actual, expected),
      f"statement {_money(actual)}, ledger {_money(expected)}",
    )

  def _validates(self, kind: str, statement: dict) -> None:
    validation = statement.get("validation") or {}
    self._add(
      kind,
      "the engine's own validation",
      bool(validation.get("passed")),
      "; ".join(validation.get("failures") or []) or "passed",
    )

  def _rows_match(
    self,
    kind: str,
    statement: dict,
    ledger: Ledger,
    traits: Iterable[str],
    present: Iterable[str] = (),
  ) -> None:
    """Rows of ``traits`` equal their accounts; concepts of those traits, and
    of ``present``, that hold a balance have a row."""
    traits = frozenset(traits)
    present = traits | frozenset(present)
    wrong: list[str] = []
    compared = 0
    seen = {fact["qname"] for fact in _rows(statement)}
    for fact, net in expected_rows(statement, ledger):
      trait = fact.get("trait")
      if trait not in traits:
        continue
      compared += 1
      expected = net if trait in DEBIT_TRAITS else -net
      if not _close(_first(fact), expected):
        wrong.append(f"{fact['qname']} {_money(_first(fact))} vs {_money(expected)}")
    missing = [
      f"{qname} {_money(net if trait in DEBIT_TRAITS else -net)} absent"
      for qname, (net, trait) in sorted(ledger.placed(seen).items())
      if qname not in seen and trait in present and abs(net) > TOLERANCE
    ]
    problems = wrong + missing
    self._add(
      kind,
      "every row equals its accounts",
      not problems,
      "; ".join(problems[:4]) or f"{compared} rows",
    )

  def statements(
    self,
    statements: Mapping[str, dict | str],
    before: Ledger,
    at_end: Ledger,
    period: Ledger,
    from_lines: LineFlows | None = None,
    flows: Flows | None = None,
    owed: dict[str, float] | None = None,
    flow_opening: Ledger | None = None,
  ) -> None:
    """Every check for the scope and month. A statement that would not
    render arrives as its error and fails on its own; the rest still run.
    ``flow_opening`` is what the working-capital flows are measured from when
    that is not ``before``: a month the group's members changed."""
    rendered: dict[str, dict] = {}
    for kind, statement in statements.items():
      if isinstance(statement, str):
        self._add(kind, "the statement renders", False, statement)
      else:
        rendered[kind] = statement
    income = rendered.get("income_statement")
    net_income = _value(income, NET_INCOME) if income else None
    # The month's bills are owed whether or not its cash flow could be read.
    if from_lines is not None and owed is not None:
      for flow, amount in from_lines.billed.items():
        owed[flow] = owed.get(flow, 0.0) + amount
    self.unmapped(at_end)
    if "balance_sheet" in rendered:
      self.balance_sheet(rendered["balance_sheet"], at_end)
    if income:
      self.income_statement(income, period)
    if "cash_flow_statement" in rendered:
      self.cash_flow(rendered["cash_flow_statement"], before, at_end, net_income)
      if flows is not None and from_lines is not None:
        self.cash_flow_rows(
          rendered["cash_flow_statement"],
          income,
          flow_opening or before,
          at_end,
          from_lines,
          flows,
          owed if owed is not None else {},
        )
    if "equity_statement" in rendered:
      self.equity_statement(rendered["equity_statement"], before, at_end, net_income)

  def unmapped(self, ledger: Ledger) -> None:
    self._add(
      "ledger",
      "every account with a balance is mapped",
      not ledger.unmapped,
      ", ".join(sorted(ledger.unmapped)[:6]) or "all mapped",
    )

  def balance_sheet(self, statement: dict, at_end: Ledger) -> None:
    kind = "balance_sheet"
    if not _stated(statement):
      return
    self._validates(kind, statement)
    rows = _rows(statement)
    top = [f for f in rows if int(f.get("depth") or 0) == 0]
    assets = sum(_first(f) for f in top if f.get("trait") == "asset")
    both = sum(_first(f) for f in top if f.get("trait") not in BALANCE_TRAITS)
    self._add(
      kind,
      "assets = liabilities + equity",
      _close(assets, both),
      f"{_money(assets)} vs {_money(both)}",
    )
    self._equal(kind, "total assets", assets, at_end.natural("asset"))
    self._equal(
      kind,
      "total liabilities",
      _shallowest(rows, "liability"),
      at_end.natural("liability"),
    )
    self._equal(
      kind,
      "total equity = equity accounts + earnings to date",
      _shallowest(rows, "equity"),
      at_end.equity,
    )
    self._rows_match(kind, statement, at_end, ("asset", "liability"), ("equity",))

  def income_statement(self, statement: dict, period: Ledger) -> None:
    kind = "income_statement"
    if not _stated(statement):
      return
    self._validates(kind, statement)
    net_income = _value(statement, NET_INCOME)
    if net_income is None and abs(period.earnings) <= TOLERANCE:
      self._add(kind, "net income", True, "no activity")
    else:
      self._equal(kind, "net income", net_income or 0.0, period.earnings)
    self._rows_match(kind, statement, period, EARNINGS_TRAITS)

  def cash_flow(
    self, statement: dict, before: Ledger, at_end: Ledger, net_income: float | None
  ) -> None:
    kind = "cash_flow_statement"
    if not _stated(statement):
      return
    self._validates(kind, statement)
    facts = statement.get("facts") or []
    opening = sum(_first(f) for f in facts if f.get("period_role") == "start")
    closing = sum(_first(f) for f in facts if f.get("period_role") == "end")
    self._equal(kind, "opening cash", opening, before.cash)
    self._equal(kind, "closing cash", closing, at_end.cash)
    change = _value(statement, CASH_CHANGE)
    self._equal(kind, "change in cash", change or 0.0, at_end.cash - before.cash)
    sections = sum(
      _first(f)
      for f in _rows(statement)
      if f["qname"].startswith("rs-gaap:NetCashProvidedByUsedIn")
    )
    self._add(
      kind,
      "the sections sum to the change in cash",
      _close(sections, change or 0.0),
      f"{_money(sections)} vs {_money(change or 0.0)}",
    )
    plug = _value(statement, RECONCILING_ITEM) or 0.0
    self._add(
      kind,
      "operating activities need no reconciling item",
      _close(plug, 0.0),
      _money(plug),
    )
    stated = _value(statement, NET_INCOME)
    if stated is not None and net_income is not None:
      self._add(
        kind,
        "starts from the income statement's net income",
        _close(stated, net_income),
        f"{_money(stated)} vs {_money(net_income)}",
      )

  def cash_flow_rows(
    self,
    statement: dict,
    income: dict | None,
    before: Ledger,
    at_end: Ledger,
    from_lines: LineFlows,
    flows: Flows,
    owed: dict[str, float],
  ) -> None:
    """The rows themselves: investing and financing against the journal
    lines, operating against the income statement and the balance changes."""
    kind = "cash_flow_statement"
    if not _stated(statement):
      return
    rows = _rows(statement)
    stated = {
      f["qname"]: _first(f)
      for f in rows
      if flows.section.get(f["qname"]) in (INVESTING, FINANCING)
      and not f.get("is_subtotal")
    }
    unexplained, settled = settle(stated, from_lines.paid, owed, TOLERANCE)
    wrong = [
      f"{q} {_money(actual)} vs {_money(expected)}"
      for q, actual, expected in unexplained
    ]
    self._add(
      kind,
      "investing and financing rows equal their ledger lines",
      not wrong,
      "; ".join(wrong[:4]) or f"{len(stated)} rows",
    )
    # A bill paid this month left payables for its own row, not for operations.
    backed_out = dict(from_lines.backed_out)
    for payable in PAYABLES:
      backed_out[payable] = backed_out.get(payable, 0.0) - settled
    figures = {f["qname"]: _first(f) for f in _rows(income)} if income else {}
    worked = operating_rows(rows, figures, before, at_end, flows, backed_out)
    wrong = [
      f"{q} {_money(actual)} vs {_money(expected)}"
      for q, actual, expected in worked
      if not _close(actual, expected)
    ]
    self._add(
      kind,
      "operating rows equal the income statement and the balance changes",
      not wrong,
      "; ".join(wrong[:4]) or f"{len(worked)} rows",
    )

  def equity_statement(
    self, statement: dict, before: Ledger, at_end: Ledger, net_income: float | None
  ) -> None:
    kind = "equity_statement"
    if not _stated(statement):
      return
    self._validates(kind, statement)
    facts = statement.get("facts") or []
    opening = sum(_first(f) for f in facts if f.get("period_role") == "start")
    closing = sum(_first(f) for f in facts if f.get("period_role") == "end")
    self._equal(kind, "opening equity", opening, before.equity)
    self._equal(kind, "closing equity", closing, at_end.equity)
    movements = sum(_first(f) for f in _rows(statement))
    self._add(
      kind,
      "the movements carry opening to closing",
      _close(opening + movements, closing),
      f"{_money(opening)} + {_money(movements)} vs {_money(closing)}",
    )
    stated = _value(statement, NET_INCOME)
    if stated is not None and net_income is not None:
      self._add(
        kind,
        "carries the income statement's net income",
        _close(stated, net_income),
        f"{_money(stated)} vs {_money(net_income)}",
      )


def _shallowest(rows: list[dict], trait: str) -> float:
  """The trait's total: its rows at the least depth they appear at."""
  matching = [f for f in rows if f.get("trait") == trait]
  if not matching:
    return 0.0
  depth = min(int(f.get("depth") or 0) for f in matching)
  return sum(_first(f) for f in matching if int(f.get("depth") or 0) == depth)
