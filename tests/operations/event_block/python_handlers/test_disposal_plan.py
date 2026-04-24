"""Unit tests for compute_disposal_plan — the pure read/compute helper."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from robosystems.operations.event_block.python_handlers._disposal_plan import (
  DisposalPlan,
  ScheduleNotFoundError,
  compute_disposal_plan,
)


def _make_structure(
  original_amount: int = 20000,
  asset_element_id: str = "elem_asset",
  credit_element_id: str = "elem_accum_dep",
) -> MagicMock:
  structure = MagicMock()
  structure.id = "struct_schedule"
  structure.structure_type = "schedule"
  structure.artifact_mechanics = {
    "schedule_metadata": {
      "original_amount": original_amount,
      "asset_element_id": asset_element_id,
    },
    "entry_template": {"credit_element_id": credit_element_id},
  }
  return structure


def _session_with_structure(structure, accumulated_dollars: float = 100.0) -> MagicMock:
  """Build a session that returns the structure then the accumulated-dep fact."""
  session = MagicMock()
  structure_result = MagicMock()
  structure_result.scalar_one_or_none.return_value = structure

  acc_row = MagicMock()
  acc_row.value = accumulated_dollars
  fact_result = MagicMock()
  fact_result.fetchone.return_value = acc_row

  session.execute.side_effect = [structure_result, fact_result]
  return session


class TestComputeDisposalPlan:
  def test_sale_with_gain(self) -> None:
    """Asset sold for more than NBV → gain posted as credit."""
    structure = _make_structure(original_amount=20000)
    session = _session_with_structure(structure, accumulated_dollars=100.0)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_schedule",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=15000,  # $150
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_gain_loss",
    )

    assert isinstance(plan, DisposalPlan)
    assert plan.original_amount == 20000
    assert plan.accumulated_depreciation == 10000  # $100 → 10000 cents
    assert plan.nbv == 10000  # 20000 - 10000
    assert plan.gain_loss == 5000  # 15000 - 10000 = +$50 gain

    # 4 legs: DR accum dep, CR asset, DR cash, CR gain
    assert len(plan.line_items) == 4
    assert plan.line_items[0]["element_id"] == "elem_accum_dep"
    assert plan.line_items[0]["debit_amount"] == 10000
    assert plan.line_items[1]["element_id"] == "elem_asset"
    assert plan.line_items[1]["credit_amount"] == 20000
    assert plan.line_items[2]["element_id"] == "elem_cash"
    assert plan.line_items[2]["debit_amount"] == 15000
    assert plan.line_items[3]["element_id"] == "elem_gain_loss"
    assert plan.line_items[3]["credit_amount"] == 5000

    total_debit = sum(li["debit_amount"] for li in plan.line_items)
    total_credit = sum(li["credit_amount"] for li in plan.line_items)
    assert total_debit == total_credit  # balanced

  def test_sale_with_loss(self) -> None:
    """Asset sold for less than NBV → loss posted as debit."""
    structure = _make_structure(original_amount=20000)
    session = _session_with_structure(structure, accumulated_dollars=100.0)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_schedule",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=5000,  # $50
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_gain_loss",
    )

    assert plan.nbv == 10000
    assert plan.gain_loss == -5000  # 5000 - 10000 = -$50 loss
    # Loss is posted as debit on gain_loss element
    loss_leg = next(
      li for li in plan.line_items if li["element_id"] == "elem_gain_loss"
    )
    assert loss_leg["debit_amount"] == 5000
    assert loss_leg["credit_amount"] == 0

  def test_abandonment_no_proceeds(self) -> None:
    """Asset abandoned with NBV > 0 → full loss posted, no cash leg."""
    structure = _make_structure(original_amount=20000)
    session = _session_with_structure(structure, accumulated_dollars=100.0)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_schedule",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=0,
      proceeds_element_id=None,
      gain_loss_element_id="elem_gain_loss",
    )

    assert plan.nbv == 10000
    assert plan.gain_loss == -10000
    # 3 legs: DR accum dep, CR asset, DR loss (no cash leg because proceeds=0)
    assert len(plan.line_items) == 3
    elements = [li["element_id"] for li in plan.line_items]
    assert "elem_cash" not in elements

  def test_fully_depreciated_asset_abandonment(self) -> None:
    """NBV = 0 and no proceeds → just remove asset + accum dep, no gain/loss leg."""
    structure = _make_structure(original_amount=20000)
    session = _session_with_structure(structure, accumulated_dollars=200.0)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_schedule",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=0,
      proceeds_element_id=None,
      gain_loss_element_id=None,  # allowed — no gain_loss leg needed
    )

    assert plan.nbv == 0
    assert plan.gain_loss == 0
    # 2 legs: DR accum dep, CR asset
    assert len(plan.line_items) == 2

  def test_proceeds_without_element_id_raises(self) -> None:
    structure = _make_structure()
    session = _session_with_structure(structure)

    with pytest.raises(ValueError, match="proceeds_element_id is required"):
      compute_disposal_plan(
        session,
        structure_id="struct_schedule",
        disposal_date=date(2026, 3, 31),
        sale_proceeds=5000,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )

  def test_gain_loss_without_element_id_raises(self) -> None:
    """NBV > 0 and proceeds != NBV but no gain_loss_element_id → error."""
    structure = _make_structure(original_amount=20000)
    session = _session_with_structure(structure, accumulated_dollars=100.0)

    with pytest.raises(ValueError, match="gain_loss_element_id is required"):
      compute_disposal_plan(
        session,
        structure_id="struct_schedule",
        disposal_date=date(2026, 3, 31),
        sale_proceeds=0,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )

  def test_missing_asset_element_id_raises(self) -> None:
    structure = _make_structure(asset_element_id=None)  # type: ignore[arg-type]
    structure.artifact_mechanics["schedule_metadata"]["asset_element_id"] = None
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = structure

    with pytest.raises(ValueError, match="asset_element_id"):
      compute_disposal_plan(
        session,
        structure_id="struct_schedule",
        disposal_date=date(2026, 3, 31),
        sale_proceeds=0,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )

  def test_missing_credit_element_id_raises(self) -> None:
    structure = _make_structure()
    structure.artifact_mechanics["entry_template"]["credit_element_id"] = None
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = structure

    with pytest.raises(ValueError, match="credit_element_id"):
      compute_disposal_plan(
        session,
        structure_id="struct_schedule",
        disposal_date=date(2026, 3, 31),
        sale_proceeds=0,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )

  def test_schedule_not_found_raises(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with pytest.raises(ScheduleNotFoundError, match="Schedule not found"):
      compute_disposal_plan(
        session,
        structure_id="struct_missing",
        disposal_date=date(2026, 3, 31),
        sale_proceeds=0,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )

  def test_no_accumulated_dep_facts_treats_as_zero(self) -> None:
    """When there are no facts in the schedule window, accumulated dep = 0."""
    structure = _make_structure(original_amount=20000)
    session = MagicMock()
    structure_result = MagicMock()
    structure_result.scalar_one_or_none.return_value = structure
    fact_result = MagicMock()
    fact_result.fetchone.return_value = None  # no fact row
    session.execute.side_effect = [structure_result, fact_result]

    plan = compute_disposal_plan(
      session,
      structure_id="struct_schedule",
      disposal_date=date(2026, 3, 31),
      sale_proceeds=20000,
      proceeds_element_id="elem_cash",
      gain_loss_element_id=None,  # gain_loss = 0
    )

    assert plan.accumulated_depreciation == 0
    assert plan.nbv == 20000
    assert plan.gain_loss == 0
