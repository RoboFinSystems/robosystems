"""Tests for the manual_adjustment Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.roboledger.commands.event_block.python_handlers.manual_adjustment import (
  ManualAdjustmentLineItem,
  ManualAdjustmentMetadata,
  dispatch,
  dispatch_preview,
)
from robosystems.operations.roboledger.schedules.service import ClosingEntryResult


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _balanced_lines() -> list[ManualAdjustmentLineItem]:
  return [
    ManualAdjustmentLineItem(element_id="elem_cash", debit_amount=5000),
    ManualAdjustmentLineItem(element_id="elem_revenue", credit_amount=5000),
  ]


def _make_metadata(**overrides) -> ManualAdjustmentMetadata:
  defaults = {
    "posting_date": date(2026, 3, 31),
    "memo": "March correction",
    "line_items": _balanced_lines(),
    "entry_type": "closing",
  }
  defaults.update(overrides)
  return ManualAdjustmentMetadata(**defaults)


def _make_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="manual_adjustment",
    event_category="adjustment",
    source="native",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    metadata={
      "posting_date": "2026-03-31",
      "memo": "March correction",
      "line_items": [
        {"element_id": "elem_cash", "debit_amount": 5000, "credit_amount": 0},
        {"element_id": "elem_revenue", "debit_amount": 0, "credit_amount": 5000},
      ],
      "entry_type": "closing",
    },
    apply_handlers=True,
  )


class TestDispatch:
  def test_dispatch_creates_entry_and_links(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    service_mock = MagicMock()
    service_mock.create_manual_closing_entry.return_value = ClosingEntryResult(
      outcome="created",
      entry_id="je_manual",
      status="draft",
      posting_date=date(2026, 3, 31),
      memo="March correction",
      amount=50.0,
    )

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.manual_adjustment.ScheduleService",
      return_value=service_mock,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    service_mock.create_manual_closing_entry.assert_called_once()
    kwargs = service_mock.create_manual_closing_entry.call_args.kwargs
    assert kwargs["posting_date"] == date(2026, 3, 31)
    assert kwargs["memo"] == "March correction"
    assert kwargs["entry_type"] == "closing"
    assert kwargs["line_items"] == [
      {
        "element_id": "elem_cash",
        "debit_amount": 5000,
        "credit_amount": 0,
        "description": None,
      },
      {
        "element_id": "elem_revenue",
        "debit_amount": 0,
        "credit_amount": 5000,
        "description": None,
      },
    ]
    assert session.execute.called  # Entry linked via triggered_by_event_id
    assert result.entry_ids == ["je_manual"]


class TestDispatchPreview:
  def test_preview_balanced_ok(self) -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = None  # no closed period
    body = _make_body()
    metadata = _make_metadata()

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert preview.computed_values["total_debit_cents"] == 5000
    assert preview.computed_values["total_credit_cents"] == 5000
    assert len(preview.planned_entries) == 1

  def test_preview_unbalanced_fails(self) -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = None
    body = _make_body()
    metadata = _make_metadata(
      line_items=[
        ManualAdjustmentLineItem(element_id="elem_cash", debit_amount=5000),
        ManualAdjustmentLineItem(element_id="elem_revenue", credit_amount=3000),
      ]
    )

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("does not balance" in err for err in preview.validation_errors)

  def test_preview_rejects_line_with_both_dr_and_cr(self) -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = None
    body = _make_body()
    metadata = _make_metadata(
      line_items=[
        ManualAdjustmentLineItem(
          element_id="elem_cash", debit_amount=5000, credit_amount=100
        ),
        ManualAdjustmentLineItem(element_id="elem_revenue", credit_amount=5000),
      ]
    )

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("cannot have both" in err for err in preview.validation_errors)

  def test_preview_rejects_closed_period(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    row = MagicMock()
    row.name = "2026-03"
    row.status = "closed"
    session.execute.return_value.fetchone.return_value = row

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("closed period" in err for err in preview.validation_errors)
