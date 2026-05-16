"""Tests for the schedule_entry_due Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.event_block.python_handlers.schedule_entry_due import (
  ScheduleEntryDueMetadata,
  dispatch,
  dispatch_preview,
)
from robosystems.operations.roboledger.schedules.service import ClosingEntryResult


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _make_metadata(**overrides) -> ScheduleEntryDueMetadata:
  defaults = {
    "schedule_id": "struct_schedule",
    "posting_date": date(2026, 3, 31),
    "period_start": date(2026, 3, 1),
    "period_end": date(2026, 3, 31),
    "memo": None,
  }
  defaults.update(overrides)
  return ScheduleEntryDueMetadata(**defaults)


def _make_body(**overrides) -> CreateEventBlockRequest:
  defaults = {
    "event_type": "schedule_entry_due",
    "event_category": "adjustment",
    "source": "native",
    "occurred_at": datetime(2026, 3, 31, tzinfo=UTC),
    "metadata": {
      "schedule_id": "struct_schedule",
      "posting_date": "2026-03-31",
      "period_start": "2026-03-01",
      "period_end": "2026-03-31",
    },
    "apply_handlers": True,
  }
  defaults.update(overrides)
  return CreateEventBlockRequest(**defaults)


class TestDispatch:
  def test_dispatch_created_links_entry(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    service_mock = MagicMock()
    service_mock.create_closing_entry.return_value = ClosingEntryResult(
      outcome="created",
      entry_id="je_new",
      status="draft",
      posting_date=date(2026, 3, 31),
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.schedule_entry_due.ScheduleService",
      return_value=service_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    service_mock.create_closing_entry.assert_called_once()
    kwargs = service_mock.create_closing_entry.call_args.kwargs
    assert kwargs["structure_id"] == "struct_schedule"
    assert kwargs["period_start"] == date(2026, 3, 1)
    assert kwargs["period_end"] == date(2026, 3, 31)
    assert kwargs["created_by"] == "usr_test"

    # Entry linked to event via triggered_by_event_id
    assert session.execute.called
    assert result.entry_ids == ["je_new"]

  def test_dispatch_skipped_does_not_link(self) -> None:
    """When outcome is 'skipped' (no fact), no entry to link."""
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    service_mock = MagicMock()
    service_mock.create_closing_entry.return_value = ClosingEntryResult(
      outcome="skipped",
      entry_id=None,
      reason="No in-scope fact for element in this period.",
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.schedule_entry_due.ScheduleService",
      return_value=service_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    assert result.entry_ids == []
    # No Entry update issued when outcome has no entry_id
    session.execute.assert_not_called()

  def test_dispatch_removed_does_not_link(self) -> None:
    """Outcome 'removed' deletes the stale draft; event records the attempt."""
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    service_mock = MagicMock()
    service_mock.create_closing_entry.return_value = ClosingEntryResult(
      outcome="removed",
      entry_id=None,
      reason="Schedule no longer covers this period.",
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.schedule_entry_due.ScheduleService",
      return_value=service_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    assert result.entry_ids == []
    session.execute.assert_not_called()


class TestDispatchPreview:
  def test_preview_returns_plan_when_fact_exists(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    structure = MagicMock()
    structure.block_type = "schedule"
    structure.name = "Office Equipment"
    structure.metadata_ = {
      "entry_template": {
        "debit_element_id": "elem_dep_exp",
        "credit_element_id": "elem_accum_dep",
        "entry_type": "closing",
        "memo_template": "Depreciation - {structure_name}",
      }
    }
    session.get.return_value = structure

    fact_row = MagicMock()
    fact_row.value = 100.0  # $100 = 10000 cents
    fact_result = MagicMock()
    fact_result.fetchone.return_value = fact_row
    session.execute.return_value = fact_result

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert len(preview.planned_entries) == 1
    entry = preview.planned_entries[0]
    assert entry["entry_type"] == "closing"
    assert "Office Equipment" in entry["memo"]
    assert len(entry["line_items"]) == 2
    assert preview.computed_values["amount_cents"] == 10000

  def test_preview_skipped_when_no_fact(self) -> None:
    """No in-scope fact → preview returns empty plan, skipped outcome."""
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    structure = MagicMock()
    structure.block_type = "schedule"
    structure.name = "Office Equipment"
    structure.metadata_ = {
      "entry_template": {
        "debit_element_id": "elem_dep_exp",
        "credit_element_id": "elem_accum_dep",
      }
    }
    session.get.return_value = structure

    fact_result = MagicMock()
    fact_result.fetchone.return_value = None
    session.execute.return_value = fact_result

    preview = dispatch_preview(session, body, metadata)

    # Skipped is a valid outcome — not an error.
    assert preview.would_succeed is True
    assert preview.planned_entries == []
    assert preview.computed_values["outcome"] == "skipped"

  def test_preview_schedule_not_found(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()
    session.get.return_value = None

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("not found" in err.lower() for err in preview.validation_errors)

  def test_preview_schedule_without_template(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    structure = MagicMock()
    structure.block_type = "schedule"
    structure.metadata_ = {}
    session.get.return_value = structure

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("entry template" in err.lower() for err in preview.validation_errors)
