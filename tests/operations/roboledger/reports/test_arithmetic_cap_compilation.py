"""Tests for the Arithmetic CAP renderer changes (Stage 2.4).

Two pieces of new logic to validate without standing up a full DB:

1. ``_topo_sort_calculations`` — must order subtotals so that calcs depending
   on other calcs compute AFTER their dependencies.
2. ``_load_calculations`` — when called with ``element_ids`` (the cross-
   structure mode), must compose calc arcs from any structure whose
   subtotal target is in the supplied element set.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.roboledger.reports.fact_grid import (
  _load_calculations,
  _topo_sort_calculations,
)


def _capture_query_and_params(session: MagicMock) -> tuple[str, dict]:
  call_args = session.execute.call_args
  query, params = call_args.args
  return str(query.compile(compile_kwargs={"literal_binds": True})), params


def _empty_result() -> MagicMock:
  result = MagicMock()
  result.__iter__ = lambda self: iter([])
  return result


# ── _topo_sort_calculations ────────────────────────────────────────────────


def test_topo_sort_returns_independent_targets_first() -> None:
  """Targets with no calc-dependencies sort first."""
  calcs = {
    "elem_gross_profit": [("elem_revenues", 1.0), ("elem_cogs", -1.0)],
    "elem_op_income": [("elem_gross_profit", 1.0), ("elem_opex", -1.0)],
    "elem_net_income": [("elem_op_income", 1.0), ("elem_tax", -1.0)],
  }
  ordered = _topo_sort_calculations(calcs)
  assert ordered.index("elem_gross_profit") < ordered.index("elem_op_income")
  assert ordered.index("elem_op_income") < ordered.index("elem_net_income")


def test_topo_sort_preserves_independent_calcs() -> None:
  """Calcs with no shared targets all sort independently — order
  among them isn't significant, but each must appear once."""
  calcs = {
    "elem_revenues": [("elem_sales", 1.0), ("elem_service", 1.0)],
    "elem_cogs": [("elem_goods", 1.0), ("elem_services", 1.0)],
    "elem_opex": [("elem_sga", 1.0), ("elem_rd", 1.0)],
  }
  ordered = _topo_sort_calculations(calcs)
  assert set(ordered) == set(calcs.keys())
  assert len(ordered) == 3


def test_topo_sort_handles_empty() -> None:
  assert _topo_sort_calculations({}) == []


def test_topo_sort_handles_cycle_without_crashing() -> None:
  """Cycles in the calc DAG are pathological but shouldn't blow up the
  renderer. Cyclic targets land at the end."""
  calcs = {
    "a": [("b", 1.0)],
    "b": [("a", 1.0)],
    "independent": [("leaf", 1.0)],
  }
  ordered = _topo_sort_calculations(calcs)
  assert "independent" in ordered
  assert "a" in ordered
  assert "b" in ordered
  assert len(ordered) == 3


def test_topo_sort_chained_three_levels_deep() -> None:
  """IS-multistep canonical chain: GrossProfit → OpIncome → IncomeBefTax →
  IncomeAftTax → NetIncome — five levels, must come out in dependency order."""
  calcs = {
    "gross_profit": [("rev", 1.0), ("cogs", -1.0)],
    "op_income": [("gross_profit", 1.0), ("opex", -1.0)],
    "inc_bef_tax": [("op_income", 1.0), ("nonop", 1.0)],
    "inc_aft_tax": [("inc_bef_tax", 1.0), ("tax", -1.0)],
    "net_income": [("inc_aft_tax", 1.0), ("disc_ops", 1.0)],
  }
  ordered = _topo_sort_calculations(calcs)
  assert ordered.index("gross_profit") < ordered.index("op_income")
  assert ordered.index("op_income") < ordered.index("inc_bef_tax")
  assert ordered.index("inc_bef_tax") < ordered.index("inc_aft_tax")
  assert ordered.index("inc_aft_tax") < ordered.index("net_income")


# ── _load_calculations cross-structure mode ────────────────────────────────


def test_load_calculations_with_element_ids_filters_by_from_element() -> None:
  """When element_ids is passed, the SQL filter must target from_element_id
  (the subtotal target) — that's the disclosure-side anchor."""
  session = MagicMock()
  session.execute.return_value = _empty_result()

  _load_calculations(
    session,
    element_ids={"elem_revenues", "elem_gross_profit", "elem_net_income"},
  )

  sql, params = _capture_query_and_params(session)
  sql_lower = sql.lower().replace('"', "")
  assert "association_type" in sql_lower
  assert "calculation" in sql_lower
  assert "from_element_id" in sql_lower
  # All three elements appear as bound params (TextClause IN-list)
  assert set(params.values()) == {
    "elem_revenues",
    "elem_gross_profit",
    "elem_net_income",
  }


def test_load_calculations_with_structure_id_uses_legacy_path() -> None:
  """When structure_id is given (no element_ids), the query filters by
  structure_id — preserves backward-compat behavior for non-Disclosure
  callers."""
  session = MagicMock()
  session.execute.return_value = _empty_result()

  _load_calculations(session, structure_id="struct_legacy")

  sql, params = _capture_query_and_params(session)
  sql_lower = sql.lower().replace('"', "")
  assert "structure_id" in sql_lower
  assert "association_type = 'calculation'" in sql_lower
  assert params == {"structure_id": "struct_legacy"}


def test_load_calculations_with_neither_arg_returns_empty_no_query() -> None:
  """Defensive: no structure_id and no element_ids → empty result, no DB hit."""
  session = MagicMock()
  result = _load_calculations(session)
  assert result == {}
  session.execute.assert_not_called()


def test_load_calculations_groups_arcs_by_subtotal_target() -> None:
  """Each subtotal collects all its summand arcs into one list. Cross-
  structure mode requires summands to be present in ``element_ids``,
  otherwise the calc structure is rejected — caller's hierarchy doesn't
  carry the inputs."""
  session = MagicMock()
  row1 = MagicMock(
    structure_id="struct_a", from_element_id="t1", to_element_id="s1", weight=1.0
  )
  row2 = MagicMock(
    structure_id="struct_a", from_element_id="t1", to_element_id="s2", weight=-1.0
  )
  row3 = MagicMock(
    structure_id="struct_b", from_element_id="t2", to_element_id="s3", weight=1.0
  )
  result_mock = MagicMock()
  result_mock.__iter__ = lambda self: iter([row1, row2, row3])
  session.execute.return_value = result_mock

  result = _load_calculations(session, element_ids={"t1", "t2", "s1", "s2", "s3"})

  assert set(result.keys()) == {"t1", "t2"}
  assert result["t1"] == [("s1", 1.0), ("s2", -1.0)]
  assert result["t2"] == [("s3", 1.0)]


def test_load_calculations_picks_arrangement_whose_summands_are_in_hierarchy() -> None:
  """When two calc structures target the same element with different
  arrangements (FAC IS2 multistep vs IS11 single-step both compute
  fac:OperatingIncomeLoss), pick the one whose summands all live in the
  disclosure's element set. The arrangement matching the disclosure's
  shape is the one to use; the other belongs to a different variant."""
  session = MagicMock()
  # IS2 multistep: OpIncome = GrossProfit - OpEx (both in hierarchy)
  is2_row1 = MagicMock(
    structure_id="is2", from_element_id="opincome", to_element_id="gp", weight=1.0
  )
  is2_row2 = MagicMock(
    structure_id="is2", from_element_id="opincome", to_element_id="opex", weight=-1.0
  )
  # IS11 singlestep: OpIncome = Revenues - CostsAndExpenses (CostsAndExpenses NOT in hierarchy)
  is11_row1 = MagicMock(
    structure_id="is11", from_element_id="opincome", to_element_id="rev", weight=1.0
  )
  is11_row2 = MagicMock(
    structure_id="is11",
    from_element_id="opincome",
    to_element_id="costs_and_expenses",
    weight=-1.0,
  )
  result_mock = MagicMock()
  result_mock.__iter__ = lambda self: iter([is2_row1, is2_row2, is11_row1, is11_row2])
  session.execute.return_value = result_mock

  # Hierarchy contains GrossProfit and OpEx but not CostsAndExpenses
  result = _load_calculations(session, element_ids={"opincome", "gp", "opex", "rev"})

  assert "opincome" in result
  assert result["opincome"] == [("gp", 1.0), ("opex", -1.0)]


def test_load_calculations_prefers_decomposition_over_identity_check() -> None:
  """Tiebreaker when both arrangements have all summands in the
  hierarchy: prefer the DECOMPOSITION (most summands) over the IDENTITY
  (fewest summands). The 1-summand variant is almost always a
  validation tautology — fac-calculations BS2 says
  ``Assets = LiabilitiesAndEquity``, which is the cross-check, not the
  way Assets is computed from leaves. The decomposition (BS3:
  ``Assets = Current + Noncurrent``) is what actually walks to leaf
  data. Picking the identity strands the parent at zero because its
  summand is itself a rollup that has nothing flowing into it directly.
  """
  session = MagicMock()
  identity_summand = MagicMock(
    structure_id="bs2_identity",
    from_element_id="assets",
    to_element_id="liab_and_equity",
    weight=1.0,
  )
  decomp_a = MagicMock(
    structure_id="bs3_decomp",
    from_element_id="assets",
    to_element_id="current",
    weight=1.0,
  )
  decomp_b = MagicMock(
    structure_id="bs3_decomp",
    from_element_id="assets",
    to_element_id="noncurrent",
    weight=1.0,
  )
  result_mock = MagicMock()
  result_mock.__iter__ = lambda self: iter([identity_summand, decomp_a, decomp_b])
  session.execute.return_value = result_mock

  result = _load_calculations(
    session,
    element_ids={"assets", "liab_and_equity", "current", "noncurrent"},
  )

  assert result["assets"] == [("current", 1.0), ("noncurrent", 1.0)]


def test_load_calculations_defaults_missing_weight_to_one() -> None:
  """Calc arcs with NULL weight default to 1.0 (additive)."""
  session = MagicMock()
  row = MagicMock(
    structure_id="struct_x", from_element_id="t1", to_element_id="s1", weight=None
  )
  result_mock = MagicMock()
  result_mock.__iter__ = lambda self: iter([row])
  session.execute.return_value = result_mock

  result = _load_calculations(session, structure_id="struct_x")

  assert result["t1"] == [("s1", 1.0)]
