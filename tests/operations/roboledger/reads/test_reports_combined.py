"""The combined view sums each entity's statement row by row. A row only one
entity has must land where the Style's presentation order puts it, not after
the final total, or its subtotal does not foot.
"""

from __future__ import annotations

from robosystems.operations.roboledger.reads.reports import _add_into
from robosystems.operations.roboledger.reports.fact_grid import FactGrid, FactRow


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


def test_shared_rows_sum_and_a_column_both_lack_stays_none():
  parent = _grid(_row("ap", 10.0, None, depth=3))
  sub = _grid(_row("ap", 2.0, None, depth=3))

  _add_into(parent, sub)

  assert parent.rows[0].values == [12.0, None]
