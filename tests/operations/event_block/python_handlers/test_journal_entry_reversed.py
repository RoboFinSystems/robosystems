"""Tests for the journal_entry_reversed Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import JournalEntryResponse
from robosystems.operations.event_block.python_handlers.journal_entry_reversed import (
  JournalEntryReversedMetadata,
  dispatch,
  dispatch_preview,
)


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _make_metadata(**overrides) -> JournalEntryReversedMetadata:
  defaults = {
    "entry_id": "je_original",
    "posting_date": None,
    "memo": None,
    "reason": None,
  }
  defaults.update(overrides)
  return JournalEntryReversedMetadata(**defaults)


def _make_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="journal_entry_reversed",
    event_category="adjustment",
    source="native",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    metadata={"entry_id": "je_original"},
    apply_handlers=True,
  )


def _fake_response(id_="je_reversal") -> JournalEntryResponse:
  return JournalEntryResponse(
    id=id_,
    transaction_id="txn_orig",
    type="reversing",
    status="posted",
    posting_date=date(2026, 3, 31),
    memo="Reversal of journal entry je_original",
    provenance="manual_entry",
    line_items=[],
    total_debit=10000,
    total_credit=10000,
  )


class TestDispatch:
  def test_dispatch_creates_reversal_and_links_both_entries(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_reversed.reverse_journal_entry",
      return_value=_fake_response(),
    ) as mock_reverse:
      result = dispatch(session, event, metadata, created_by="usr_test")

    mock_reverse.assert_called_once()
    body_arg = mock_reverse.call_args.args[1]
    assert body_arg.entry_id == "je_original"

    # Two UPDATE statements: one for the reversing Entry, one for the
    # original — both linked to the event for full audit chain.
    assert session.execute.call_count == 2
    assert result.entry_ids == ["je_reversal"]
    assert result.transaction_ids == []

  def test_dispatch_passes_optional_overrides_through(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(
      posting_date=date(2026, 4, 1),
      memo="Custom reversal memo",
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_reversed.reverse_journal_entry",
      return_value=_fake_response(),
    ) as mock_reverse:
      dispatch(session, event, metadata, created_by="usr_test")

    body_arg = mock_reverse.call_args.args[1]
    assert body_arg.posting_date == date(2026, 4, 1)
    assert body_arg.memo == "Custom reversal memo"


class TestDispatchPreview:
  def test_preview_happy_path(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    original = MagicMock()
    original.status = "posted"
    session.get.return_value = original

    line_items = [
      _line(element_id="elem_cash", debit=10000, credit=0, order=1),
      _line(element_id="elem_revenue", debit=0, credit=10000, order=2),
    ]
    session.execute.return_value.scalars.return_value.all.return_value = line_items

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_reversed.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert len(preview.planned_entries) == 1
    plan = preview.planned_entries[0]
    assert plan["entry_type"] == "reversing"
    # Lines should be flipped: debit ↔ credit
    assert plan["line_items"] == [
      {"element_id": "elem_cash", "debit_amount": 0, "credit_amount": 10000},
      {"element_id": "elem_revenue", "debit_amount": 10000, "credit_amount": 0},
    ]

  def test_preview_entry_not_found(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    body = _make_body()
    metadata = _make_metadata(entry_id="je_missing")

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("not found" in e.lower() for e in preview.validation_errors)

  def test_preview_rejects_unposted_original(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    original = MagicMock()
    original.status = "draft"
    session.get.return_value = original

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_reversed.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("only posted entries" in e.lower() for e in preview.validation_errors)

  def test_preview_rejects_closed_period(self) -> None:
    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    original = MagicMock()
    original.status = "posted"
    session.get.return_value = original

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_reversed.assert_period_not_closed",
      side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("closed" in e.lower() for e in preview.validation_errors)


def _line(element_id: str, debit: int, credit: int, order: int):
  li = MagicMock()
  li.element_id = element_id
  li.debit_amount = debit
  li.credit_amount = credit
  li.line_order = order
  return li
