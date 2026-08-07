"""Forecast articulation (F-2) — BS roll, schedule movements, derived CF.

Pure-function coverage over a hand-built :class:`ArticulationContext`;
no session involved. The identities asserted here are the articulation
claims themselves: A = L + E by construction (balancing cash), RE roll
exactness, per-set schedule-movement freeze semantics, CF footing to
ΔCash via the reconciling plug.
"""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.operations.information_block.forecast_articulation import (
  ArticulationContext,
  ScheduleProjection,
  derive_cash_flow,
  roll_balance_sheet,
  schedule_instant_movement,
  schedule_is_delta,
)
from robosystems.operations.roboledger.reports.calc_dag import (
  resolve_calc_dag,
  topo_sort_calculations,
)

pytestmark = pytest.mark.unit

MAY = date(2026, 5, 31)
JUN = date(2026, 6, 30)
JUL = date(2026, 7, 31)

# A miniature but complete three-statement calc DAG.
CALC = {
  "Assets": [("AssetsCurrent", 1.0), ("AssetsNoncurrent", 1.0)],
  "AssetsCurrent": [("cash", 1.0), ("ar", 1.0), ("prepaid", 1.0)],
  "AssetsNoncurrent": [("ppe_net", 1.0)],
  "Liabilities": [("ap", 1.0)],
  "StockholdersEquity": [("apic", 1.0), ("re", 1.0)],
  "LE": [("Liabilities", 1.0), ("StockholdersEquity", 1.0)],
  "cf_op": [
    ("ni", 1.0),
    ("dda", 1.0),
    ("d_ar", 1.0),
    ("d_prepaid", 1.0),
    ("d_ap", 1.0),
    ("recon", 1.0),
  ],
  "cf_net": [("cf_op", 1.0), ("cf_inv", 1.0), ("cf_fin", 1.0)],
}
CALC_ORDER = topo_sort_calculations(CALC)

BS_PRIOR = {
  "cash": 50.0,
  "ar": 100.0,
  "prepaid": 12.0,
  "ppe_net": 60.0,
  "ap": 0.0,
  "apic": 100.0,
  "re": 122.0,
}


def _ctx(schedules: ScheduleProjection | None = None) -> ArticulationContext:
  return ArticulationContext(
    bs_structure_id="struct_bs",
    cf_structure_id="struct_cf",
    base_bs_element_ids=[
      *BS_PRIOR,
      "Assets",
      "AssetsCurrent",
      "AssetsNoncurrent",
      "Liabilities",
      "StockholdersEquity",
      "LE",
    ],
    bs_prior=dict(BS_PRIOR),
    cash_element_id="cash",
    re_element_id="re",
    ni_element_id="ni",
    assets_element_id="Assets",
    le_element_id="LE",
    net_change_element_id="cf_net",
    recon_element_id="recon",
    operating_element_id="cf_op",
    investing_element_id="cf_inv",
    financing_element_id="cf_fin",
    derivations={
      "d_ar": [("ar", -1.0)],
      "d_prepaid": [("prepaid", -1.0)],
      "d_ap": [("ap", 1.0)],
      "dda": [("accum_dep", 1.0)],
    },
    schedules=schedules or ScheduleProjection(),
  )


def _roll(ctx: ArticulationContext, **overrides) -> dict[str, float]:
  kwargs = {
    "month_end": JUN,
    "prev_end": MAY,
    "prior_bs": dict(BS_PRIOR),
    "rule_values": {"ar": 150.0, "ap": 62.0},
    "rule_instant_targets": {"ar", "ap"},
    "resolved_is": {"ni": 10.0},
    "calculations": CALC,
    "calc_order": CALC_ORDER,
  }
  kwargs.update(overrides)
  return roll_balance_sheet(ctx, **kwargs)


class TestBalanceSheetRoll:
  def test_articulated_roll_balances_by_construction(self):
    schedules = ScheduleProjection(
      instant_by_set={
        ("fs_dep", "ppe_net"): {MAY: -10.0, JUN: -12.0},
        ("fs_prepaid", "prepaid"): {MAY: 12.0, JUN: 11.0},
      }
    )
    bs = _roll(_ctx(schedules))

    # Carry, rules, schedules, RE roll, balancing cash — each line lands.
    assert bs["apic"] == 100.0  # carry
    assert bs["ar"] == 150.0  # DSO rule
    assert bs["ap"] == 62.0  # DPO rule
    assert bs["prepaid"] == 11.0  # schedule draw-down
    assert bs["ppe_net"] == 58.0  # accumulated-dep movement (-2)
    assert bs["re"] == 132.0  # RE[m] = RE[m-1] + NI

    # Balancing cash: LE = 62 + (100 + 132) = 294; other assets = 219.
    assert bs["cash"] == pytest.approx(75.0)

    # The articulation claim itself: A = L + E, exactly.
    final = resolve_calc_dag(bs, set(bs), CALC, CALC_ORDER)
    assert final["Assets"] == pytest.approx(final["LE"])

  def test_subtotals_never_carried(self):
    bs = _roll(_ctx())
    for subtotal in ("Assets", "AssetsCurrent", "Liabilities", "LE"):
      assert subtotal not in bs  # leaves only; subtotals derive at emission

  def test_skipped_rule_target_falls_back_to_carry(self):
    # DSO inactive this month — AR carries instead of jumping.
    bs = _roll(_ctx(), rule_values={"ap": 62.0})
    assert bs["ar"] == 100.0

  def test_re_rolls_from_zero_without_base_fact(self):
    ctx = _ctx()
    prior = {k: v for k, v in BS_PRIOR.items() if k != "re"}
    bs = _roll(ctx, prior_bs=prior)
    assert bs["re"] == 10.0


class TestScheduleSemantics:
  def test_instant_movement_freezes_when_a_schedule_ends(self):
    # Set A still running (+2); set B ended after MAY — it contributes 0,
    # never a reversal of its final balance.
    schedules = ScheduleProjection(
      instant_by_set={
        ("fs_a", "ppe_net"): {MAY: 10.0, JUN: 12.0},
        ("fs_b", "ppe_net"): {MAY: 8.0},
      }
    )
    ctx = _ctx(schedules)
    assert schedule_instant_movement(ctx, "ppe_net", JUN, MAY) == pytest.approx(2.0)

  def test_instant_movement_new_schedule_contributes_full_balance(self):
    schedules = ScheduleProjection(instant_by_set={("fs_new", "prepaid"): {JUN: 9.0}})
    ctx = _ctx(schedules)
    assert schedule_instant_movement(ctx, "prepaid", JUN, MAY) == pytest.approx(9.0)

  def test_is_delta_stops_an_ended_schedules_expense(self):
    schedules = ScheduleProjection(
      duration_total={"dda": {"2026-05": 5.0, "2026-06": 3.0}}
    )
    ctx = _ctx(schedules)
    # Reference month carried 5 inside the IS value; June's schedule says 3.
    assert schedule_is_delta(ctx, "dda", "2026-06", "2026-05") == pytest.approx(-2.0)
    # July vs a May reference: no schedule fact at all — the whole
    # scheduled expense stops.
    assert schedule_is_delta(ctx, "dda", "2026-07", "2026-05") == pytest.approx(-5.0)

  def test_is_delta_telescopes_through_successive_prev_months(self):
    # The walk passes the PREVIOUS month as the reference (the carried
    # value rolls), so successive deltas must telescope to the
    # base-anchored total — the property that keeps an ended schedule a
    # one-time step down instead of a cumulative march.
    schedules = ScheduleProjection(
      duration_total={"dda": {"2026-05": 5.0, "2026-06": 3.0}}
    )
    ctx = _ctx(schedules)
    jun = schedule_is_delta(ctx, "dda", "2026-06", "2026-05")
    jul = schedule_is_delta(ctx, "dda", "2026-07", "2026-06")
    aug = schedule_is_delta(ctx, "dda", "2026-08", "2026-07")
    assert jun + jul + aug == pytest.approx(
      schedule_is_delta(ctx, "dda", "2026-08", "2026-05")
    )


class TestRuleDeltaPushDown:
  """A Derive rule that targets a calc PARENT must scale the parent's
  carried children proportionally, or the statement's own RollUp
  verification fails (driven parent, stale children)."""

  def test_children_scale_proportionally(self):
    from robosystems.operations.information_block.forecast_compute import (
      _scale_rule_target_children,
    )

    calcs = {"revenues": [("stream_a", 1.0), ("stream_b", 1.0)]}
    current = {"revenues": 206.0, "stream_a": 150.0, "stream_b": 50.0}
    _scale_rule_target_children(current, {"revenues"}, calcs)
    assert current["stream_a"] == pytest.approx(154.5)
    assert current["stream_b"] == pytest.approx(51.5)
    # The rollup identity the verification checks:
    assert current["stream_a"] + current["stream_b"] == pytest.approx(
      current["revenues"]
    )

  def test_scaling_recurses_down_the_subtree(self):
    from robosystems.operations.information_block.forecast_compute import (
      _scale_rule_target_children,
    )

    calcs = {
      "revenues": [("product", 1.0)],
      "product": [("sku_1", 1.0), ("sku_2", 1.0)],
    }
    current = {"revenues": 220.0, "product": 200.0, "sku_1": 120.0, "sku_2": 80.0}
    _scale_rule_target_children(current, {"revenues"}, calcs)
    assert current["product"] == pytest.approx(220.0)
    assert current["sku_1"] == pytest.approx(132.0)
    assert current["sku_2"] == pytest.approx(88.0)

  def test_zero_children_sum_left_untouched(self):
    from robosystems.operations.information_block.forecast_compute import (
      _scale_rule_target_children,
    )

    calcs = {"revenues": [("stream_a", 1.0)]}
    current = {"revenues": 206.0, "stream_a": 0.0}
    _scale_rule_target_children(current, {"revenues"}, calcs)
    # No proportional basis — visible RollUp failure beats an invented split.
    assert current["stream_a"] == 0.0

  def test_carried_fallback_is_a_no_op(self):
    from robosystems.operations.information_block.forecast_compute import (
      _scale_rule_target_children,
    )

    calcs = {"revenues": [("stream_a", 1.0)]}
    current = {"revenues": 100.0, "stream_a": 100.0}
    _scale_rule_target_children(current, {"revenues"}, calcs)
    assert current["stream_a"] == 100.0


class TestDerivedCashFlow:
  def test_cf_derives_deltas_and_foots_without_plug(self):
    ctx = _ctx()
    bs = {**BS_PRIOR, "ar": 150.0, "ap": 62.0, "prepaid": 11.0, "re": 132.0}
    bs["cash"] = 75.0  # the balancing value for these lines
    cf, plug = derive_cash_flow(
      ctx,
      bs=bs,
      prior_bs=dict(BS_PRIOR),
      resolved_is={"ni": 10.0, "dda": 2.0},
      calculations=CALC,
      calc_order=CALC_ORDER,
    )
    assert cf["d_ar"] == pytest.approx(-50.0)  # asset up = cash use
    assert cf["d_prepaid"] == pytest.approx(1.0)  # asset down = cash source
    assert cf["d_ap"] == pytest.approx(62.0)  # liability up = cash source
    assert cf["dda"] == pytest.approx(2.0)  # direct IS value wins
    assert cf["ni"] == pytest.approx(10.0)
    # ΔCash = 25 = 10 + 2 - 50 + 1 + 62 → no residual, no plug.
    assert plug == 0.0
    assert "recon" not in cf
    assert cf["cf_op"] == pytest.approx(25.0)
    assert cf["cf_net"] == pytest.approx(25.0)
    # Zero investing/financing stay out of the emission (actuals mirror).
    assert "cf_inv" not in cf
    assert "cf_fin" not in cf

  def test_residual_books_to_the_reconciling_plug(self):
    ctx = _ctx()
    bs = {**BS_PRIOR, "ar": 150.0, "ap": 62.0, "prepaid": 11.0, "re": 132.0}
    bs["cash"] = 75.0
    # No direct DDA and no accum_dep in the BS dicts → derived net change
    # misses 2.0 vs ΔCash; the plug closes it.
    cf, plug = derive_cash_flow(
      ctx,
      bs=bs,
      prior_bs=dict(BS_PRIOR),
      resolved_is={"ni": 10.0},
      calculations=CALC,
      calc_order=CALC_ORDER,
    )
    delta_cash = 25.0
    assert plug == pytest.approx(delta_cash - (10.0 - 50.0 + 1.0 + 62.0))
    assert cf["recon"] == pytest.approx(plug)
    assert cf["cf_net"] == pytest.approx(delta_cash)  # foots exactly

  def test_direct_leaf_never_double_counts_with_derivation(self):
    # dda has BOTH a direct IS value and a derivation source that moved.
    ctx = _ctx()
    bs = {**BS_PRIOR, "accum_dep": -12.0, "re": 132.0}
    prior = {**BS_PRIOR, "accum_dep": -10.0}
    cf, _ = derive_cash_flow(
      ctx,
      bs=bs,
      prior_bs=prior,
      resolved_is={"ni": 10.0, "dda": 2.0},
      calculations=CALC,
      calc_order=CALC_ORDER,
    )
    assert cf["dda"] == pytest.approx(2.0)


class TestScheduleInstantRouting:
  """Contra polarity on the direct route (June-close F14).

  A Net-only presentation maps its contra CoA account straight onto the
  Net anchor; the schedule's running balance is stored positive, so a +1
  application marches the Net line UP by the monthly amortization. The
  sign flips whenever source and target disagree on contra-ness; the
  PP&E routes arm keeps its own encoded sign (no double flip).
  """

  @staticmethod
  def _row(
    *,
    source_id: str,
    target_id: str,
    target_qname: str,
    value: float,
    classification: str | None = "asset",
    source_classification: str | None = "asset",
    period_type: str = "instant",
    fact_set_id: str = "fs1",
    period_end: date = JUN,
  ):
    from types import SimpleNamespace

    return SimpleNamespace(
      fact_set_id=fact_set_id,
      value=value,
      period_end=period_end,
      period_type=period_type,
      source_id=source_id,
      target_id=target_id,
      target_qname=target_qname,
      classification=classification,
      source_classification=source_classification,
    )

  def _project(self, rows, *, base_bs, ppe_net_element_id=None):
    from unittest.mock import MagicMock

    from robosystems.operations.information_block.forecast_articulation import (
      _load_schedule_projection,
    )

    session = MagicMock()
    session.execute.return_value.fetchall.return_value = rows
    return _load_schedule_projection(
      session,
      mapping_id="struct_mapping",
      entity_id="ent_1",
      window_start=MAY,
      window_end=JUL,
      base_is_element_ids=set(),
      base_bs_element_ids=base_bs,
      ppe_net_element_id=ppe_net_element_id,
    )

  def test_contra_source_onto_net_anchor_flips_sign(self):
    """Accumulated Amortization (contraAsset) → IntangibleAssetsNet: the
    running balance must REDUCE the Net line."""
    rows = [
      self._row(
        source_id="coa_accum_amort",
        target_id="intangibles_net",
        target_qname="rs-gaap:IntangibleAssetsNetExcludingGoodwill",
        value=10.49,
        source_classification="contraAsset",
      )
    ]
    projection = self._project(rows, base_bs={"intangibles_net"})
    assert projection.instant_by_set[("fs1", "intangibles_net")][JUN] == pytest.approx(
      -10.49
    )

  def test_plain_source_direct_route_keeps_sign(self):
    rows = [
      self._row(
        source_id="coa_prepaid",
        target_id="prepaid",
        target_qname="rs-gaap:PrepaidExpenseCurrent",
        value=12.0,
      )
    ]
    projection = self._project(rows, base_bs={"prepaid"})
    assert projection.instant_by_set[("fs1", "prepaid")][JUN] == pytest.approx(12.0)

  def test_congruent_contra_pair_keeps_sign(self):
    """A contra source mapped onto a contra CONCEPT the base presents
    directly (classified gross/contra presentation) applies at +1 — the
    line itself is contra, its balance grows positive."""
    rows = [
      self._row(
        source_id="coa_accum_dep",
        target_id="accum_dep_concept",
        target_qname="rs-gaap:SomeContraConcept",
        value=194.83,
        classification="contraAsset",
        source_classification="contraAsset",
      )
    ]
    projection = self._project(rows, base_bs={"accum_dep_concept"})
    assert projection.instant_by_set[("fs1", "accum_dep_concept")][
      JUN
    ] == pytest.approx(194.83)

  def test_ppe_routes_arm_unchanged_no_double_flip(self):
    """The PP&E contra concept routes onto Net at the encoded -1; the
    source's contra trait must not flip it back to +1."""
    rows = [
      self._row(
        source_id="coa_accum_dep",
        target_id="accum_dep_concept",
        target_qname=(
          "rs-gaap:AccumulatedDepreciationDepletionAndAmortization"
          "PropertyPlantAndEquipment"
        ),
        value=194.83,
        classification="contraAsset",
        source_classification="contraAsset",
      )
    ]
    projection = self._project(rows, base_bs={"ppe_net"}, ppe_net_element_id="ppe_net")
    assert projection.instant_by_set[("fs1", "ppe_net")][JUN] == pytest.approx(-194.83)
