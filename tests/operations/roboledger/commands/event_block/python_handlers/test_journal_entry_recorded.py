"""Tests for the journal_entry_recorded Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  JournalEntryLineItemInput,
  JournalEntryResponse,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded import (
  JournalEntryRecordedMetadata,
  dispatch,
  dispatch_preview,
)


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.status = "classified"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _balanced_lines() -> list[JournalEntryLineItemInput]:
  return [
    JournalEntryLineItemInput(element_id="elem_cash", debit_amount=5000),
    JournalEntryLineItemInput(element_id="elem_revenue", credit_amount=5000),
  ]


def _make_metadata(**overrides) -> JournalEntryRecordedMetadata:
  defaults = {
    "posting_date": date(2026, 3, 31),
    "memo": "Sale",
    "line_items": _balanced_lines(),
    "type": "standard",
    "status": "draft",
    "transaction_id": None,
  }
  defaults.update(overrides)
  return JournalEntryRecordedMetadata(**defaults)


def _make_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    source="native",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    metadata={
      "posting_date": "2026-03-31",
      "memo": "Sale",
      "line_items": [
        {"element_id": "elem_cash", "debit_amount": 5000, "credit_amount": 0},
        {"element_id": "elem_revenue", "debit_amount": 0, "credit_amount": 5000},
      ],
      "type": "standard",
      "status": "draft",
    },
    apply_handlers=True,
  )


def _fake_response(entry_id="je_new", transaction_id="txn_new") -> JournalEntryResponse:
  return JournalEntryResponse(
    id=entry_id,
    transaction_id=transaction_id,
    type="standard",
    status="draft",
    posting_date=date(2026, 3, 31),
    memo="Sale",
    provenance="manual_entry",
    line_items=[],
    total_debit=5000,
    total_credit=5000,
  )


class TestDispatch:
  def test_dispatch_draft_keeps_classified_status(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(status="draft")

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=_fake_response(),
    ) as mock_create:
      result = dispatch(session, event, metadata, created_by="usr_test")

    mock_create.assert_called_once()
    # Both Entry and Transaction got audit-linked (2 updates)
    assert session.execute.call_count == 2
    # Status not mutated for draft entries
    assert event.status == "classified"
    assert result.entry_ids == ["je_new"]
    assert result.transaction_ids == ["txn_new"]

  def test_dispatch_posted_overrides_to_fulfilled(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(status="posted")

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=_fake_response(),
    ):
      dispatch(session, event, metadata, created_by="usr_test")

    # Dynamic status override
    assert event.status == "fulfilled"

  def test_dispatch_no_transaction_id_in_response_skips_link(self) -> None:
    """If caller supplied a transaction_id, response.transaction_id may be None."""
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    resp = _fake_response(transaction_id=None)
    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=resp,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    # Only Entry got linked (1 update)
    assert session.execute.call_count == 1
    assert result.transaction_ids == []

  def test_dispatch_propagates_closed_period_error(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
    ):
      with pytest.raises(ClosedPeriodError):
        dispatch(session, event, metadata, created_by="usr_test")


class TestDispatchPreview:
  def test_preview_balanced_ok(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert preview.computed_values["total_debit_cents"] == 5000
    assert preview.computed_values["total_credit_cents"] == 5000
    assert preview.computed_values["target_status"] == "classified"

  def test_preview_posted_reports_fulfilled_target_status(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata(status="posted")

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.computed_values["target_status"] == "fulfilled"

  def test_preview_unbalanced_fails(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata(
      line_items=[
        JournalEntryLineItemInput(element_id="elem_cash", debit_amount=5000),
        JournalEntryLineItemInput(element_id="elem_revenue", credit_amount=3000),
      ]
    )

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("balance" in err.lower() for err in preview.validation_errors)

  def test_preview_closed_period_fails(self) -> None:
    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed",
      side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("closed" in err.lower() for err in preview.validation_errors)
