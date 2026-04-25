"""Tests for the DSL event-block engine."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.engine import apply_handler


def test_apply_handler_links_entry_and_transaction_to_originating_event() -> None:
  session = MagicMock()
  added: list[object] = []
  session.add.side_effect = lambda obj: added.append(obj)

  event = Event(
    id="evt_invoice",
    event_type="invoice_issued",
    event_category="sales",
    occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
    source="native",
    amount=12500,
    currency="USD",
    description="Customer invoice",
    metadata_={},
    created_by="usr_test",
  )
  handler = EventHandler(
    id="hdl_invoice",
    name="Invoice Handler",
    event_type="invoice_issued",
    transaction_template={
      "transactions": [
        {
          "entry_template": {
            "debit": {"element_id": "elem_ar", "amount": "{{ event.amount }}"},
            "credit": {"element_id": "elem_rev", "amount": "{{ event.amount }}"},
          }
        }
      ]
    },
    priority=10,
    is_active=True,
    origin="tenant",
    created_by="usr_test",
  )

  transactions = apply_handler(session, event, handler, created_by="usr_test")

  entry = next(obj for obj in added if isinstance(obj, Entry))
  transaction = next(obj for obj in added if isinstance(obj, Transaction))
  assert len(transactions) == 1
  assert transaction.triggered_by_event_id == "evt_invoice"
  assert entry.triggered_by_event_id == "evt_invoice"
