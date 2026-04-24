"""Tests for the asset_disposed Python handler (dispatch + preview)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.roboledger.commands.event_block.python_handlers._disposal_plan import (
  DisposalPlan,
  ScheduleNotFoundError,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed import (
  AssetDisposedMetadata,
  dispatch,
  dispatch_preview,
)


def _make_event(occurred_at: datetime | None = None):
  event = MagicMock()
  event.id = "evt_test"
  event.occurred_at = occurred_at or datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _make_plan(**overrides) -> DisposalPlan:
  defaults = {
    "structure_id": "struct_schedule",
    "asset_element_id": "elem_asset",
    "credit_element_id": "elem_accum_dep",
    "original_amount": 20000,
    "accumulated_depreciation": 10000,
    "nbv": 10000,
    "sale_proceeds": 5000,
    "gain_loss": -5000,
    "line_items": [
      {"element_id": "elem_accum_dep", "debit_amount": 10000, "credit_amount": 0},
      {"element_id": "elem_asset", "debit_amount": 0, "credit_amount": 20000},
      {"element_id": "elem_cash", "debit_amount": 5000, "credit_amount": 0},
      {"element_id": "elem_gain_loss", "debit_amount": 5000, "credit_amount": 0},
    ],
  }
  defaults.update(overrides)
  return DisposalPlan(**defaults)


class TestDispatchPreview:
  def test_preview_happy_path_returns_plan(self) -> None:
    session = MagicMock()
    body = CreateEventBlockRequest(
      event_type="asset_disposed",
      event_category="adjustment",
      occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
      source="native",
      metadata={"schedule_id": "struct_schedule", "proceeds": 5000},
      apply_handlers=True,
    )
    metadata = AssetDisposedMetadata(
      schedule_id="struct_schedule",
      proceeds=5000,
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_gain_loss",
    )

    plan = _make_plan()
    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
      return_value=plan,
    ):
      result = dispatch_preview(session, body, metadata)

    assert result.would_succeed is True
    assert result.validation_errors == []
    assert result.computed_values["nbv_cents"] == 10000
    assert result.computed_values["gain_loss_cents"] == -5000
    assert result.computed_values["accumulated_depreciation_cents"] == 10000
    assert result.computed_values["sale_proceeds_cents"] == 5000
    assert len(result.planned_entries) == 1
    assert result.planned_entries[0]["entry_type"] == "closing"
    assert len(result.planned_entries[0]["line_items"]) == 4

  def test_preview_schedule_not_found(self) -> None:
    session = MagicMock()
    body = CreateEventBlockRequest(
      event_type="asset_disposed",
      event_category="adjustment",
      occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
      source="native",
      metadata={"schedule_id": "struct_missing"},
      apply_handlers=True,
    )
    metadata = AssetDisposedMetadata(schedule_id="struct_missing")

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
      side_effect=ScheduleNotFoundError("struct_missing"),
    ):
      result = dispatch_preview(session, body, metadata)

    assert result.would_succeed is False
    assert any("not found" in err.lower() for err in result.validation_errors)
    assert result.planned_entries == []

  def test_preview_missing_element_id(self) -> None:
    session = MagicMock()
    body = CreateEventBlockRequest(
      event_type="asset_disposed",
      event_category="adjustment",
      occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
      source="native",
      metadata={"schedule_id": "struct_schedule", "proceeds": 5000},
      apply_handlers=True,
    )
    metadata = AssetDisposedMetadata(
      schedule_id="struct_schedule",
      proceeds=5000,
      # proceeds_element_id missing → compute helper raises ValueError
    )

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
      side_effect=ValueError("proceeds_element_id is required when sale_proceeds > 0."),
    ):
      result = dispatch_preview(session, body, metadata)

    assert result.would_succeed is False
    assert len(result.validation_errors) == 1
    assert "proceeds_element_id" in result.validation_errors[0]


class TestDispatch:
  def test_dispatch_happy_path_invokes_all_steps(self) -> None:
    """Full atomic disposal fires truncate → delete rule → post entry → link event."""
    session = MagicMock()
    event = _make_event()
    metadata = AssetDisposedMetadata(
      schedule_id="struct_schedule",
      proceeds=5000,
      proceeds_element_id="elem_cash",
      gain_loss_element_id="elem_gain_loss",
    )

    plan = _make_plan()

    entry_result = MagicMock()
    entry_result.entry_id = "je_disposal"

    service_mock = MagicMock()
    service_mock.create_manual_closing_entry.return_value = entry_result

    with (
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ) as compute_mock,
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
      ) as delete_rule_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    # compute was called
    compute_mock.assert_called_once()
    # truncate_schedule was called
    service_mock.truncate_schedule.assert_called_once()
    tr_kwargs = service_mock.truncate_schedule.call_args.kwargs
    assert tr_kwargs["structure_id"] == "struct_schedule"
    assert tr_kwargs["new_end_date"] == event.occurred_at.date()
    # SumEquals rule was deleted
    delete_rule_mock.assert_called_once_with(session, "struct_schedule")
    # Manual closing entry was created with the plan's line items
    service_mock.create_manual_closing_entry.assert_called_once()
    ce_kwargs = service_mock.create_manual_closing_entry.call_args.kwargs
    assert ce_kwargs["line_items"] == plan.line_items
    assert ce_kwargs["entry_type"] == "closing"
    # Entry was linked to event via triggered_by_event_id (session.execute called)
    assert session.execute.called
    # Result carries the new entry id
    assert result.entry_ids == ["je_disposal"]

  def test_dispatch_uses_custom_memo_when_provided(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = AssetDisposedMetadata(
      schedule_id="struct_schedule",
      memo="Custom disposal memo",
    )

    plan = _make_plan(sale_proceeds=0, gain_loss=-10000)
    entry_result = MagicMock()
    entry_result.entry_id = "je_disposal"
    service_mock = MagicMock()
    service_mock.create_manual_closing_entry.return_value = entry_result

    with (
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ),
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
      ),
    ):
      dispatch(session, event, metadata, created_by="usr_test")

    assert service_mock.create_manual_closing_entry.call_args.kwargs["memo"] == (
      "Custom disposal memo"
    )

  def test_dispatch_default_memo_when_none(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = AssetDisposedMetadata(schedule_id="struct_schedule")

    plan = _make_plan(sale_proceeds=0, gain_loss=0)
    entry_result = MagicMock()
    entry_result.entry_id = "je_disposal"
    service_mock = MagicMock()
    service_mock.create_manual_closing_entry.return_value = entry_result

    with (
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ),
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
      ),
    ):
      dispatch(session, event, metadata, created_by="usr_test")

    memo = service_mock.create_manual_closing_entry.call_args.kwargs["memo"]
    assert "struct_schedule" in memo

  def test_dispatch_without_occurred_at_raises(self) -> None:
    session = MagicMock()
    event = _make_event()
    event.occurred_at = None
    metadata = AssetDisposedMetadata(schedule_id="struct_schedule")

    with pytest.raises(ValueError, match="requires occurred_at"):
      dispatch(session, event, metadata, created_by="usr_test")
