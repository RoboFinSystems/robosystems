"""The combined view sums each entity's statement row by row. A row only one
entity has must land where the Style's presentation order puts it — not after
the final total, or its subtotal does not foot, and not where the other grid's
adjacency puts it, or a line lands mid-statement when that entity rendered
none of the rows in between.
"""

from __future__ import annotations

from robosystems.operations.roboledger.reads.reports import _add_into
from robosystems.operations.roboledger.reports.fact_grid import FactGrid, FactRow

# One Style's presentation order: a grid's rows are a subsequence of it, and
# each row carries its place in it.
ORDER = [
  "revenue",
  "opex",
  "operating_income",
  "pretax",
  "nci_share",
  "net_income",
  "ap",
  "debt",
  "accrued",
  "other",
  "current",
  "members",
  "re",
  "equity",
  "total",
]


def _row(
  qname: str, *values: float | None, depth: int, subtotal: bool = False
) -> FactRow:
  return FactRow(
    element_id=qname,
    element_qname=f"rs-gaap:{qname}",
    element_name=qname,
    classification="liability",
    balance_type="credit",
    values=list(values),
    is_subtotal=subtotal,
    depth=depth,
    position=ORDER.index(qname),
  )


def _grid(*rows: FactRow) -> FactGrid:
  return FactGrid(
    structure_id="s",
    structure_name="BS",
    block_type="balance_sheet",
    periods=[],
    rows=list(rows),
  )


def _shape(grid: FactGrid) -> list[tuple[str, int, list[float | None]]]:
  return [(r.element_id, r.depth, r.values) for r in grid.rows]


def test_a_row_only_the_subsidiary_has_lands_under_its_subtotal():
  """The parent owes no short-term debt, so its grid dropped the row; the
  subsidiary's note must still sit inside current liabilities."""
  parent = _grid(
    _row("ap", 10.0, depth=3),
    _row("other", 5.0, depth=3),
    _row("current", 15.0, depth=2, subtotal=True),
    _row("total", 15.0, depth=0, subtotal=True),
  )
  sub = _grid(
    _row("ap", 2.0, depth=3),
    _row("debt", 10.0, depth=3),
    _row("current", 12.0, depth=2, subtotal=True),
    _row("total", 12.0, depth=0, subtotal=True),
  )

  _add_into(parent, sub)

  assert _shape(parent) == [
    ("ap", 3, [12.0]),
    ("debt", 3, [10.0]),
    ("other", 3, [5.0]),
    ("current", 2, [27.0]),
    ("total", 0, [27.0]),
  ]


def test_a_leading_row_only_the_subsidiary_has_goes_first():
  parent = _grid(
    _row("re", 30.0, depth=2), _row("equity", 30.0, depth=1, subtotal=True)
  )
  sub = _grid(
    _row("members", 100.0, depth=2),
    _row("equity", 100.0, depth=1, subtotal=True),
  )

  _add_into(parent, sub)

  assert [r.element_id for r in parent.rows] == ["members", "re", "equity"]


def test_consecutive_subsidiary_only_rows_keep_their_order():
  parent = _grid(
    _row("current", 0.0, depth=2, subtotal=True), _row("total", 0.0, depth=0)
  )
  sub = _grid(
    _row("ap", 1.0, depth=3),
    _row("debt", 2.0, depth=3),
    _row("current", 3.0, depth=2, subtotal=True),
    _row("total", 3.0, depth=0),
  )

  _add_into(parent, sub)

  assert [r.element_id for r in parent.rows] == ["ap", "debt", "current", "total"]


def test_three_entities_each_with_their_own_rows_keep_one_order():
  """The parent, then two subsidiaries that each bring a row the others
  lack: every row lands inside its subtotal, in the Style's one order."""
  parent = _grid(
    _row("ap", 10.0, depth=3),
    _row("current", 10.0, depth=2, subtotal=True),
    _row("re", 30.0, depth=2),
    _row("equity", 30.0, depth=1, subtotal=True),
  )
  first_sub = _grid(
    _row("debt", 5.0, depth=3),
    _row("current", 5.0, depth=2, subtotal=True),
    _row("members", 100.0, depth=2),
    _row("equity", 100.0, depth=1, subtotal=True),
  )
  second_sub = _grid(
    _row("ap", 2.0, depth=3),
    _row("accrued", 1.0, depth=3),
    _row("current", 3.0, depth=2, subtotal=True),
    _row("members", 40.0, depth=2),
    _row("equity", 40.0, depth=1, subtotal=True),
  )

  _add_into(parent, first_sub)
  _add_into(parent, second_sub)

  assert _shape(parent) == [
    ("ap", 3, [12.0]),
    ("debt", 3, [5.0]),
    ("accrued", 3, [1.0]),
    ("current", 2, [18.0]),
    ("members", 2, [140.0]),
    ("re", 2, [30.0]),
    ("equity", 1, [170.0]),
  ]


def test_a_row_lands_by_the_styles_order_not_the_other_grids_adjacency():
  """The consolidation entity's income statement is the noncontrolling
  share and net income alone — its operating income and pre-tax income are
  zero and dropped. Merged into the parent's, the share belongs just above
  net income, after the subtotals that entity never rendered; the row it
  happens to follow in its own sparse grid says nothing about that."""
  parent = _grid(
    _row("revenue", 100.0, depth=2),
    _row("opex", 60.0, depth=2),
    _row("operating_income", 40.0, depth=1, subtotal=True),
    _row("pretax", 40.0, depth=1, subtotal=True),
    _row("net_income", 40.0, depth=1, subtotal=True),
  )
  consolidation = _grid(
    _row("nci_share", 3.0, depth=1),
    _row("net_income", -3.0, depth=1, subtotal=True),
  )

  _add_into(parent, consolidation)

  assert _shape(parent) == [
    ("revenue", 2, [100.0]),
    ("opex", 2, [60.0]),
    ("operating_income", 1, [40.0]),
    ("pretax", 1, [40.0]),
    ("nci_share", 1, [3.0]),
    ("net_income", 1, [37.0]),
  ]


def test_shared_rows_sum_and_a_column_both_lack_stays_none():
  parent = _grid(_row("ap", 10.0, None, depth=3))
  sub = _grid(_row("ap", 2.0, None, depth=3))

  _add_into(parent, sub)

  assert parent.rows[0].values == [12.0, None]


def _balance(role: str, value: float, position: float) -> FactRow:
  """One of a roll-forward's two balance rows: the same concept on both."""
  return FactRow(
    element_id="equity",
    element_qname="rs-gaap:equity",
    element_name=f"equity, {role}",
    classification="equity",
    balance_type="credit",
    values=[value],
    is_subtotal=role == "end",
    period_role=role,
    position=position,
  )


def test_a_rollforwards_two_balances_sum_apart():
  """The beginning and ending balances share a concept; each entity's
  beginning sums into the group's beginning and its ending into the ending,
  and a change row only the subsidiary has lands between them."""
  parent = _grid(
    _balance("start", 100.0, -1.0),
    _row("net_income", 20.0, depth=1),
    _balance("end", 120.0, ORDER.index("equity")),
  )
  sub = _grid(
    _balance("start", 50.0, -1.0),
    _row("members", 5.0, depth=1),
    _balance("end", 55.0, ORDER.index("equity")),
  )
  _add_into(parent, sub)

  assert [(r.element_id, r.period_role, r.values) for r in parent.rows] == [
    ("equity", "start", [150.0]),
    ("net_income", None, [20.0]),
    ("members", None, [5.0]),
    ("equity", "end", [175.0]),
  ]
