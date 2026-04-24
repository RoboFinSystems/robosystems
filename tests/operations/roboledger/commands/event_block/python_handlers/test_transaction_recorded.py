"""Tests for the transaction_recorded Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.transactions import TransactionResponse
from robosystems.operations.roboledger.commands.event_block.python_handlers.transaction_recorded import (
  TransactionRecordedMetadata,
  dispatch,
  dispatch_preview,
)


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _make_metadata(**overrides) -> TransactionRecordedMetadata:
  defaults = {
    "type": "invoice",
    "date": date(2026, 3, 31),
    "amount": 10000,
    "currency": "USD",
    "description": "Invoice 123",
    "status": "pending",
  }
  defaults.update(overrides)
  return TransactionRecordedMetadata(**defaults)


def _make_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="transaction_recorded",
    event_category="adjustment",
    source="native",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    metadata={
      "type": "invoice",
      "date": "2026-03-31",
      "amount": 10000,
      "currency": "USD",
      "description": "Invoice 123",
      "status": "pending",
    },
    apply_handlers=True,
  )


def _fake_response(id_="txn_new") -> TransactionResponse:
  return TransactionResponse(
    id=id_,
    type="invoice",
    date=date(2026, 3, 31),
    amount=10000,
    currency="USD",
    description="Invoice 123",
    status="pending",
    source="native",
  )


class TestDispatch:
  def test_dispatch_creates_and_links_transaction(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.transaction_recorded.create_transaction",
      return_value=_fake_response(),
    ) as mock_create:
      result = dispatch(session, event, metadata, created_by="usr_test")

    mock_create.assert_called_once()
    # Transaction got linked via triggered_by_event_id
    assert session.execute.called
    assert result.transaction_ids == ["txn_new"]
    assert result.entry_ids == []

  def test_dispatch_body_carries_all_metadata(self) -> None:
    """The handler should pass every field into CreateTransactionRequest."""
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(
      type="payment",
      merchant_name="Acme",
      reference_number="REF-42",
      category="Utilities",
      due_date=date(2026, 4, 15),
    )

    with patch(
      "robosystems.operations.roboledger.commands.event_block.python_handlers.transaction_recorded.create_transaction",
      return_value=_fake_response(id_="txn_x"),
    ) as mock_create:
      dispatch(session, event, metadata, created_by="usr_test")

    body_arg = mock_create.call_args.args[1]
    assert body_arg.type == "payment"
    assert body_arg.merchant_name == "Acme"
    assert body_arg.reference_number == "REF-42"
    assert body_arg.category == "Utilities"
    assert body_arg.due_date == date(2026, 4, 15)


class TestDispatchPreview:
  def test_preview_always_succeeds(self) -> None:
    """Standalone Transactions have no entries to balance — preview is an echo."""
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert preview.planned_entries == []
    assert preview.computed_values["type"] == "invoice"
    assert preview.computed_values["amount_cents"] == 10000
    assert preview.computed_values["currency"] == "USD"
