"""Tests for the asset_disposed Python handler (dispatch + preview)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.event_block.python_handlers._disposal_plan import (
  DisposalPlan,
  ScheduleNotFoundError,
)
from robosystems.operations.event_block.python_handlers.asset_disposed import (
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
      "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
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
      "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
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
      "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
      side_effect=ValueError("proceeds_element_id is required when sale_proceeds > 0."),
    ):
      result = dispatch_preview(session, body, metadata)

    assert result.would_succeed is False
    assert len(result.validation_errors) == 1
    assert "proceeds_element_id" in result.validation_errors[0]


class TestDispatch:
  def test_dispatch_happy_path_invokes_all_steps(self) -> None:
    """Full atomic disposal: void obligations → drop rule → post entry → link event."""
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
        "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ) as compute_mock,
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._void_pending_obligations_for_schedule",
        return_value=12,
      ) as void_mock,
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
      ) as delete_rule_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    # compute was called
    compute_mock.assert_called_once()
    # Pending obligations were voided
    void_mock.assert_called_once_with(
      session,
      structure_id="struct_schedule",
      disposal_event_id=event.id,
    )
    # truncate_schedule is no longer called — the void chain replaces it
    assert not service_mock.truncate_schedule.called
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
        "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._void_pending_obligations_for_schedule",
        return_value=0,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
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
        "robosystems.operations.event_block.python_handlers.asset_disposed.compute_disposal_plan",
        return_value=plan,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed.ScheduleService",
        return_value=service_mock,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._void_pending_obligations_for_schedule",
        return_value=0,
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.asset_disposed._delete_sum_equals_rule"
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


class TestVoidPendingObligationsForSchedule:
  """Stream 2.C — disposal voids the obligation chain instead of truncating facts."""

  @staticmethod
  def _structure_with_event(schedule_created_event_id: str | None) -> MagicMock:
    structure = MagicMock()
    structure.metadata_ = (
      {"schedule_created_event_id": schedule_created_event_id}
      if schedule_created_event_id is not None
      else {}
    )
    return structure

  def test_voids_pending_obligations_linked_via_obligated_by(self) -> None:
    """Update query targets pending events on the schedule's originator chain."""
    from robosystems.operations.event_block.python_handlers.asset_disposed import (
      _void_pending_obligations_for_schedule,
    )

    session = MagicMock()
    session.get.return_value = self._structure_with_event("evt_schedule_created")
    locked = MagicMock()
    locked.scalars.return_value = iter([f"evt_due_{i}" for i in range(11)])
    update_result = MagicMock(rowcount=11)
    session.execute.side_effect = [locked, update_result]

    voided = _void_pending_obligations_for_schedule(
      session,
      structure_id="struct_schedule",
      disposal_event_id="evt_disposal",
    )

    assert voided == 11
    # The locking select targets pending events on the schedule's originator
    # chain, in the shared lock order; the UPDATE then goes by id.
    lock_stmt = session.execute.call_args_list[0].args[0]
    lock_sql = str(lock_stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "events.obligated_by_event_id = 'evt_schedule_created'" in lock_sql
    assert "events.status IN ('pending')" in lock_sql
    assert "FOR UPDATE" in lock_sql and "ORDER BY events.id" in lock_sql
    update_stmt = session.execute.call_args_list[1].args[0]
    rendered = str(update_stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "events.id IN (" in rendered
    assert "events.status IN ('pending')" in rendered
    assert "status='voided'" in rendered.replace(" = ", "=")
    assert "replaced_by_event_id='evt_disposal'" in rendered.replace(" = ", "=")

  def test_returns_zero_when_structure_missing(self) -> None:
    """Defensive no-op when the structure can't be loaded."""
    from robosystems.operations.event_block.python_handlers.asset_disposed import (
      _void_pending_obligations_for_schedule,
    )

    session = MagicMock()
    session.get.return_value = None

    voided = _void_pending_obligations_for_schedule(
      session,
      structure_id="struct_missing",
      disposal_event_id="evt_disposal",
    )

    assert voided == 0
    session.execute.assert_not_called()

  def test_returns_zero_when_structure_predates_stream_2a(self) -> None:
    """Schedules created before the originator-id stamping land: with no stamp
    AND no recoverable obligations, the robust fallback finds nothing, so
    disposal is a clean no-op (returns 0)."""
    from robosystems.operations.event_block.python_handlers.asset_disposed import (
      _void_pending_obligations_for_schedule,
    )

    session = MagicMock()
    session.get.return_value = self._structure_with_event(None)
    # No stamp on the structure AND the fallback recovery finds no obligations.
    session.execute.return_value.scalar.return_value = None

    voided = _void_pending_obligations_for_schedule(
      session,
      structure_id="struct_old",
      disposal_event_id="evt_disposal",
    )

    assert voided == 0
    # The fallback recovery select ran; no UPDATE followed.
    assert session.execute.call_count == 1


class TestComputeDisposalPlanShapes:
  """Direct compute_disposal_plan coverage for the two schedule shapes.

  Depreciation-style schedules carry a RISING cumulative instant fact on
  the contra credit element; prepaid-style ("self-carried") schedules
  carry the DECLINING remaining balance on the credited asset itself.
  Reading the latter as "accumulated" inverts NBV — the July 2026 close
  regression this class pins down.
  """

  @staticmethod
  def _structure(
    asset_element_id: str, credit_element_id: str, original_amount: int
  ) -> MagicMock:
    structure = MagicMock()
    structure.artifact_mechanics = {
      "schedule_metadata": {
        "asset_element_id": asset_element_id,
        "original_amount": original_amount,
      },
      "entry_template": {"credit_element_id": credit_element_id},
    }
    return structure

  @staticmethod
  def _session(structure: MagicMock, instant_value: float | None) -> MagicMock:
    structure_result = MagicMock()
    structure_result.scalar_one_or_none.return_value = structure
    fact_result = MagicMock()
    if instant_value is None:
      fact_result.fetchone.return_value = None
    else:
      row = MagicMock()
      row.value = instant_value
      fact_result.fetchone.return_value = row
    session = MagicMock()
    session.execute.side_effect = [structure_result, fact_result]
    return session

  def test_depreciation_shape_reads_instant_as_accumulated(self) -> None:
    from datetime import date

    from robosystems.operations.event_block.python_handlers._disposal_plan import (
      compute_disposal_plan,
    )

    structure = self._structure("elem_ppe", "elem_accum", 120_000)
    session = self._session(structure, 300.00)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_depr",
      disposal_date=date(2026, 7, 1),
      sale_proceeds=90_000,
      proceeds_element_id="elem_cash",
      gain_loss_element_id=None,
    )

    assert plan.accumulated_depreciation == 30_000
    assert plan.nbv == 90_000
    assert plan.gain_loss == 0
    # DR accumulated / CR asset-at-cost / DR proceeds
    assert [
      (li["element_id"], li["debit_amount"], li["credit_amount"])
      for li in plan.line_items
    ] == [
      ("elem_accum", 30_000, 0),
      ("elem_ppe", 0, 120_000),
      ("elem_cash", 90_000, 0),
    ]

  def test_self_carried_prepaid_reads_instant_as_remaining_balance(self) -> None:
    from datetime import date

    from robosystems.operations.event_block.python_handlers._disposal_plan import (
      compute_disposal_plan,
    )

    # The July 2026 regression: AWS RI 2026-02, original 1,315.08, remaining
    # 1,132.43 at 6/30. The old code computed NBV = 182.65 (inverted).
    structure = self._structure("elem_prepaid", "elem_prepaid", 131_508)
    session = self._session(structure, 1132.43)

    plan = compute_disposal_plan(
      session,
      structure_id="struct_ri",
      disposal_date=date(2026, 7, 1),
      sale_proceeds=113_243,
      proceeds_element_id="elem_other_services",
      gain_loss_element_id=None,
    )

    assert plan.nbv == 113_243
    assert plan.accumulated_depreciation == 18_265
    assert plan.gain_loss == 0
    # Single derecognition credit on the self-carried asset + proceeds.
    assert [
      (li["element_id"], li["debit_amount"], li["credit_amount"])
      for li in plan.line_items
    ] == [
      ("elem_prepaid", 0, 113_243),
      ("elem_other_services", 113_243, 0),
    ]
    # Balanced.
    assert sum(li["debit_amount"] for li in plan.line_items) == sum(
      li["credit_amount"] for li in plan.line_items
    )

  def test_self_carried_with_nothing_left_raises(self) -> None:
    from datetime import date

    from robosystems.operations.event_block.python_handlers._disposal_plan import (
      compute_disposal_plan,
    )

    structure = self._structure("elem_prepaid", "elem_prepaid", 131_508)
    session = self._session(structure, 0.0)

    with pytest.raises(ValueError, match="Nothing to dispose"):
      compute_disposal_plan(
        session,
        structure_id="struct_ri",
        disposal_date=date(2026, 7, 1),
        sale_proceeds=0,
        proceeds_element_id=None,
        gain_loss_element_id=None,
      )
