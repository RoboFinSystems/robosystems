"""Tests for the DSL event-block engine."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.extensions.roboledger.entry import (
  ENTRY_PROVENANCE_VALUES,
  Entry,
)
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.engine import (
  apply_handler,
  posting_date_for_event,
)
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError


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

  # Regression (ck_entries_provenance / migration 0019): the engine tags its
  # entries provenance='event_handler'. That value MUST be in the model's
  # permitted set — before 0019 the DB CHECK rejected it, so the first real
  # (non-mocked) GL insert would have CheckViolated. This session is a mock,
  # so it never hit the constraint; assert the invariant explicitly instead.
  assert entry.provenance == "event_handler"
  assert entry.provenance in ENTRY_PROVENANCE_VALUES


def test_posting_date_prefers_effective_at() -> None:
  effective = datetime(2026, 1, 31, tzinfo=UTC)
  occurred = datetime(2026, 2, 15, tzinfo=UTC)
  assert posting_date_for_event(effective_at=effective, occurred_at=occurred) == date(
    2026, 1, 31
  )


def test_apply_handler_refuses_a_closed_period() -> None:
  session = MagicMock()
  event = Event(
    id="evt_invoice",
    event_type="invoice_issued",
    event_category="sales",
    occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
    source="native",
    amount=12500,
    created_by="usr_test",
  )
  handler = EventHandler(
    id="hdl_invoice",
    name="Invoice Handler",
    event_type="invoice_issued",
    transaction_template={"transactions": []},
    priority=10,
    is_active=True,
    origin="tenant",
    created_by="usr_test",
  )
  with (
    patch(
      "robosystems.operations.event_block.engine.assert_period_not_closed",
      side_effect=ClosedPeriodError("2026-04", date(2026, 4, 1)),
    ),
    pytest.raises(ClosedPeriodError),
  ):
    apply_handler(session, event, handler, created_by="usr_test")
  session.add.assert_not_called()
