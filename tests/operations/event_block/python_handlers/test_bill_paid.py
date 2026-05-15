"""Tests for the bill_paid Python handler — AP-side duality resolution.

Bill-paid shares the linkage helper with payment_received; this file
only verifies (a) the AP candidate type set, (b) that the dispatcher
wires the link step after journal_dispatch. Detailed match-algorithm
coverage lives in ``test_payment_received.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.python_handlers.bill_paid import (
  _AP_ORIGINATING_TYPES,
  dispatch,
)


def _scalar_first_result(value):
  result = MagicMock()
  result.scalars.return_value.first.return_value = value
  return result


def test_ap_candidate_types_contain_only_bill_received() -> None:
  assert _AP_ORIGINATING_TYPES == ("bill_received",)


def test_dispatch_runs_journal_then_links_to_bill() -> None:
  bill = Event(
    id="evt_bill",
    event_type="bill_received",
    event_category="purchase",
    event_class="economic",
    agent_id="agt_vendor",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    source="quickbooks",
    external_id="Bill_7",
    status="committed",
    amount=4200,
    currency="USD",
    metadata_={"qb_doc_number": "BIL-7"},
    created_by="usr_test",
  )
  bill_payment = Event(
    id="evt_billpay",
    event_type="bill_paid",
    event_category="purchase",
    event_class="economic",
    agent_id="agt_vendor",
    occurred_at=datetime(2026, 4, 5, tzinfo=UTC),
    source="quickbooks",
    external_id="BillPayment_99",
    status="captured",
    amount=4200,
    currency="USD",
    metadata_={"qb_linked_txns": [{"txn_id": "7", "txn_type": "Bill"}]},
    created_by="usr_test",
  )
  session = MagicMock()
  session.execute.return_value = _scalar_first_result(bill)
  metadata = MagicMock()

  with patch(
    "robosystems.operations.event_block.python_handlers.bill_paid.journal_dispatch"
  ) as mock_journal:
    mock_journal.return_value = MagicMock(
      entry_ids=["je_bp"], transaction_ids=["txn_bp"]
    )
    result = dispatch(session, bill_payment, metadata, created_by="usr_test")

  mock_journal.assert_called_once_with(session, bill_payment, metadata, "usr_test")
  assert bill_payment.discharges_event_id == "evt_bill"
  assert result.entry_ids == ["je_bp"]
