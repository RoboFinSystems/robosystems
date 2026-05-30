"""Unit tests for ``_source_calculation_arcs`` (``bundle.py``).

The calc-sourcing helper pulls calculation arcs for a report's concepts and
hosts each under the rendered Network that carries both endpoints, then emits a
subtotal's children ONLY when every child's stored weight sign is XBRL-legal
for the endpoints' balance types (§5.1.1.2 Table 6). The logic is pure
computation (the only DB touch is a single ``session.execute`` whose rows we
stub), so it's exercised here without a database.
"""

from __future__ import annotations

from types import SimpleNamespace

from robosystems.operations.serialization.bundle import _source_calculation_arcs


class _Result:
  def __init__(self, rows: list[object]) -> None:
    self._rows = rows

  def scalars(self) -> list[object]:
    return list(self._rows)


class _Session:
  """Minimal stand-in: returns the seeded calc rows for any query."""

  def __init__(self, rows: list[object]) -> None:
    self._rows = rows

  def execute(self, *_args: object, **_kwargs: object) -> _Result:
    return _Result(self._rows)


def _elem(balance: str | None) -> SimpleNamespace:
  return SimpleNamespace(balance_type=balance)


def _pres(structure_id: str, frm: str, to: str) -> SimpleNamespace:
  return SimpleNamespace(
    structure_id=structure_id, from_element_id=frm, to_element_id=to
  )


def _calc(frm: str, to: str, weight: float = 1.0) -> SimpleNamespace:
  return SimpleNamespace(
    association_type="calculation",
    from_element_id=frm,
    to_element_id=to,
    arcrole=None,
    order_value=1.0,
    weight=weight,
  )


# Shared world: a balance-sheet network, a cash-flow network, an equity network.
_ELEMENTS = {
  "assets": _elem("debit"),
  "assets_cur": _elem("debit"),
  "assets_noncur": _elem("debit"),
  "ncf_op": _elem("debit"),  # NetCashProvidedByUsedInOperatingActivities
  "inc_ar": _elem("credit"),  # IncreaseDecreaseInAccountsReceivable
  "equity": _elem("credit"),
  "ni": _elem("credit"),  # legal under credit parent
  "dividends": _elem("debit"),  # illegal under credit parent at +weight
}
_PRES = [
  _pres("bs_struct", "assets", "assets_cur"),
  _pres("bs_struct", "assets", "assets_noncur"),
  _pres("cf_struct", "ncf_op", "inc_ar"),
  _pres("se_struct", "equity", "ni"),
  _pres("se_struct", "equity", "dividends"),
]
_STRUCTURES = {
  "bs_struct": SimpleNamespace(block_type="balance_sheet"),
  "cf_struct": SimpleNamespace(block_type="cash_flow_statement"),
  "se_struct": SimpleNamespace(block_type="equity_statement"),
}


def _run(calc_rows: list[object]) -> list[SimpleNamespace]:
  return _source_calculation_arcs(_Session(calc_rows), _PRES, _STRUCTURES, _ELEMENTS)


def test_same_balance_summation_included() -> None:
  result = _run([_calc("assets", "assets_cur"), _calc("assets", "assets_noncur")])
  assert {(r.from_element_id, r.to_element_id) for r in result} == {
    ("assets", "assets_cur"),
    ("assets", "assets_noncur"),
  }
  assert all(r.structure_id == "bs_struct" for r in result)
  assert all(r.association_type == "calculation" for r in result)


def test_mixed_balance_cf_subtotal_dropped() -> None:
  # Debit parent summing a credit child at +1 is illegal (Table 6) → drop.
  result = _run([_calc("ncf_op", "inc_ar")])
  assert result == []


def test_subtotal_dropped_entirely_when_any_child_illegal() -> None:
  # equity (credit) ← ni (credit, legal +1) AND dividends (debit, illegal +1).
  # The WHOLE subtotal drops so the emitted summation stays complete/footing —
  # not just the offending arc.
  result = _run([_calc("equity", "ni"), _calc("equity", "dividends")])
  assert result == []


def test_arc_with_no_host_network_excluded() -> None:
  # Neither endpoint appears in any rendered presentation network.
  result = _run([_calc("orphan_parent", "orphan_child")])
  assert result == []


def test_legal_subtotal_kept_alongside_dropped_one() -> None:
  result = _run(
    [
      _calc("assets", "assets_cur"),
      _calc("assets", "assets_noncur"),
      _calc("ncf_op", "inc_ar"),  # illegal CF arc
      _calc("equity", "dividends"),  # illegal SE arc
    ]
  )
  assert {(r.from_element_id, r.to_element_id) for r in result} == {
    ("assets", "assets_cur"),
    ("assets", "assets_noncur"),
  }


def test_negative_weight_legal_for_opposite_balance() -> None:
  # A debit parent summing a credit child is legal at weight -1.
  result = _run([_calc("ncf_op", "inc_ar", weight=-1.0)])
  assert {(r.from_element_id, r.to_element_id) for r in result} == {
    ("ncf_op", "inc_ar")
  }
  assert result[0].weight == -1.0


def test_empty_inputs_return_empty() -> None:
  assert _source_calculation_arcs(_Session([]), [], {}, {}) == []
  # Declared elements but no presentation networks → nothing to host under.
  assert _source_calculation_arcs(_Session([_calc("a", "b")]), [], {}, _ELEMENTS) == []
