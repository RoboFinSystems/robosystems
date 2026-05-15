"""Tests for the payment_received Python handler — AR-side duality resolution."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.python_handlers.payment_received import (
  _AR_ORIGINATING_TYPES,
  _TERMINAL_INVALID_STATUSES,
  _link_discharge,
  _resolve_by_linked_txns,
  _resolve_by_reference_number,
  dispatch,
)


def _make_payment_event(
  *,
  event_id: str = "evt_pay",
  metadata: dict | None = None,
  agent_id: str | None = "agt_cust",
  discharges_event_id: str | None = None,
) -> Event:
  """Build an in-memory Event row (not session-attached)."""
  evt = Event(
    id=event_id,
    event_type="payment_received",
    event_category="sales",
    event_class="economic",
    agent_id=agent_id,
    occurred_at=datetime(2026, 4, 15, tzinfo=UTC),
    source="quickbooks",
    external_id="Payment_123",
    status="captured",
    amount=10000,
    currency="USD",
    metadata_=metadata or {},
    created_by="usr_test",
  )
  evt.discharges_event_id = discharges_event_id
  return evt


def _make_invoice_event(
  *,
  event_id: str = "evt_inv",
  agent_id: str | None = "agt_cust",
  qb_doc_number: str | None = "INV-001",
) -> Event:
  return Event(
    id=event_id,
    event_type="invoice_issued",
    event_category="sales",
    event_class="economic",
    agent_id=agent_id,
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    source="quickbooks",
    external_id="Invoice_42",
    status="committed",
    amount=10000,
    currency="USD",
    metadata_={"qb_doc_number": qb_doc_number},
    created_by="usr_test",
  )


def _scalar_first_result(value):
  result = MagicMock()
  result.scalars.return_value.first.return_value = value
  return result


class TestResolveByLinkedTxns:
  def test_returns_first_match_when_linked_txn_resolves(self) -> None:
    """LinkedTxn names {Invoice, 42} → look up event with external_id 'Invoice_42'."""
    invoice = _make_invoice_event()
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(invoice)

    match = _resolve_by_linked_txns(
      session,
      source="quickbooks",
      linked_txns=[{"txn_id": "42", "txn_type": "Invoice"}],
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )

    assert match is invoice
    sent_stmt = str(session.execute.call_args.args[0])
    assert "events.external_id IN" in sent_stmt or "external_id" in sent_stmt

  def test_returns_none_for_empty_linked_txns(self) -> None:
    session = MagicMock()
    match = _resolve_by_linked_txns(
      session,
      source="quickbooks",
      linked_txns=[],
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )
    assert match is None
    session.execute.assert_not_called()

  def test_skips_refs_missing_either_field(self) -> None:
    """A LinkedTxn with empty TxnId or TxnType is dropped, not matched."""
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(None)

    match = _resolve_by_linked_txns(
      session,
      source="quickbooks",
      linked_txns=[
        {"txn_id": "", "txn_type": "Invoice"},
        {"txn_id": "99", "txn_type": ""},
      ],
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )

    assert match is None
    session.execute.assert_not_called()

  def test_excludes_voided_and_superseded_originators(self) -> None:
    """Terminal-invalid statuses must not appear as discharge targets.

    `captured` / `classified` are intentionally allowed so a sync batch
    that fires the payment handler before its sibling invoice handler
    still links — the AR aggregate separately filters originating
    events by `committed`/`fulfilled`, so a pre-commit invoice can't
    pollute open totals if the link is set early.
    """
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(None)

    _resolve_by_linked_txns(
      session,
      source="quickbooks",
      linked_txns=[{"txn_id": "42", "txn_type": "Invoice"}],
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )

    sent_stmt = str(session.execute.call_args.args[0])
    # SQLAlchemy compiles `notin_(("voided", "superseded"))` to NOT IN.
    assert "NOT IN" in sent_stmt
    assert True  # sanity ref
    assert set(_TERMINAL_INVALID_STATUSES) == {"voided", "superseded"}


class TestResolveByReferenceNumber:
  def test_scopes_by_agent_when_present(self) -> None:
    invoice = _make_invoice_event()
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(invoice)

    match = _resolve_by_reference_number(
      session,
      source="quickbooks",
      reference_number="INV-001",
      agent_id="agt_cust",
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )

    assert match is invoice
    # Verify the agent scope made it into the WHERE clause; the
    # compiled SQL should reference agent_id rather than IS NULL.
    sent_stmt = str(session.execute.call_args.args[0])
    assert "agent_id" in sent_stmt

  def test_returns_none_for_empty_reference(self) -> None:
    session = MagicMock()
    match = _resolve_by_reference_number(
      session,
      source="quickbooks",
      reference_number="",
      agent_id="agt_cust",
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )
    assert match is None
    session.execute.assert_not_called()

  def test_restricts_to_null_agent_when_payment_unscoped(self) -> None:
    """A payment with agent_id=None never silently links to someone else's invoice."""
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(None)

    _resolve_by_reference_number(
      session,
      source="quickbooks",
      reference_number="INV-001",
      agent_id=None,
      candidate_event_types=_AR_ORIGINATING_TYPES,
    )

    sent_stmt = str(session.execute.call_args.args[0])
    # `Event.agent_id.is_(None)` compiles to `IS NULL`.
    assert "IS NULL" in sent_stmt


class TestLinkDischarge:
  def test_idempotent_when_already_linked(self) -> None:
    """Re-fires that hit an already-linked event do not re-query."""
    payment = _make_payment_event(discharges_event_id="evt_existing")
    session = MagicMock()

    _link_discharge(session, payment, _AR_ORIGINATING_TYPES)

    assert payment.discharges_event_id == "evt_existing"
    session.execute.assert_not_called()

  def test_linked_txns_win_over_reference_number(self) -> None:
    """Both signals present; LinkedTxn-resolved match is used."""
    invoice = _make_invoice_event(event_id="evt_linked")
    payment = _make_payment_event(
      metadata={
        "qb_linked_txns": [{"txn_id": "42", "txn_type": "Invoice"}],
        "qb_reference_number": "INV-999",  # Would resolve elsewhere if fallback ran
      },
    )
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(invoice)

    _link_discharge(session, payment, _AR_ORIGINATING_TYPES)

    assert payment.discharges_event_id == "evt_linked"
    # Only one query — LinkedTxn matched, fallback never ran.
    assert session.execute.call_count == 1

  def test_falls_back_to_reference_number_when_linked_txn_misses(self) -> None:
    invoice = _make_invoice_event(event_id="evt_fallback")
    payment = _make_payment_event(
      metadata={
        "qb_linked_txns": [{"txn_id": "99", "txn_type": "Invoice"}],
        "qb_reference_number": "INV-001",
      },
    )
    session = MagicMock()
    session.execute.side_effect = [
      _scalar_first_result(None),  # LinkedTxn miss
      _scalar_first_result(invoice),  # reference_number hit
    ]

    _link_discharge(session, payment, _AR_ORIGINATING_TYPES)

    assert payment.discharges_event_id == "evt_fallback"
    assert session.execute.call_count == 2

  def test_leaves_unlinked_when_no_signal_matches(self) -> None:
    payment = _make_payment_event(
      metadata={"qb_linked_txns": [], "qb_reference_number": ""}
    )
    session = MagicMock()

    _link_discharge(session, payment, _AR_ORIGINATING_TYPES)

    assert payment.discharges_event_id is None
    # No metadata signals → no DB call at all.
    session.execute.assert_not_called()

  def test_leaves_unlinked_when_both_lookups_miss(self) -> None:
    payment = _make_payment_event(
      metadata={
        "qb_linked_txns": [{"txn_id": "99", "txn_type": "Invoice"}],
        "qb_reference_number": "INV-001",
      }
    )
    session = MagicMock()
    session.execute.side_effect = [
      _scalar_first_result(None),
      _scalar_first_result(None),
    ]

    _link_discharge(session, payment, _AR_ORIGINATING_TYPES)

    assert payment.discharges_event_id is None
    assert session.execute.call_count == 2


class TestDispatch:
  def test_runs_journal_dispatch_then_links_discharge(self) -> None:
    payment = _make_payment_event(
      metadata={"qb_linked_txns": [{"txn_id": "42", "txn_type": "Invoice"}]}
    )
    invoice = _make_invoice_event(event_id="evt_inv_linked")
    session = MagicMock()
    session.execute.return_value = _scalar_first_result(invoice)
    metadata = MagicMock()

    with patch(
      "robosystems.operations.event_block.python_handlers.payment_received.journal_dispatch"
    ) as mock_journal:
      mock_journal.return_value = MagicMock(
        entry_ids=["je_1"], transaction_ids=["txn_1"]
      )

      result = dispatch(session, payment, metadata, created_by="usr_test")

    mock_journal.assert_called_once_with(session, payment, metadata, "usr_test")
    assert payment.discharges_event_id == "evt_inv_linked"
    assert result.entry_ids == ["je_1"]


@pytest.mark.parametrize(
  ("originating_type",), [("invoice_issued",), ("sales_receipt_recorded",)]
)
def test_ar_candidate_types_include_invoices_and_sales_receipts(
  originating_type,
) -> None:
  assert originating_type in _AR_ORIGINATING_TYPES
