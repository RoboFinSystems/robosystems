"""Event Block Python handler registry, keyed by event_type. Consulted before
the DSL registry."""

from __future__ import annotations

from .asset_disposed import ASSET_DISPOSED_HANDLER
from .bank_feed import BANK_FEED_HANDLERS
from .bill_paid import BILL_PAID_HANDLER
from .journal_entry_recorded import JOURNAL_ENTRY_RECORDED_HANDLER
from .journal_entry_reversed import JOURNAL_ENTRY_REVERSED_HANDLER
from .payment_received import PAYMENT_RECEIVED_HANDLER
from .schedule_created import SCHEDULE_CREATED_HANDLER
from .schedule_entry_due import SCHEDULE_ENTRY_DUE_HANDLER
from .types import EventBlockPythonHandler

EVENT_BLOCK_PYTHON_REGISTRY: dict[str, EventBlockPythonHandler] = {
  ASSET_DISPOSED_HANDLER.event_type: ASSET_DISPOSED_HANDLER,
  SCHEDULE_CREATED_HANDLER.event_type: SCHEDULE_CREATED_HANDLER,
  SCHEDULE_ENTRY_DUE_HANDLER.event_type: SCHEDULE_ENTRY_DUE_HANDLER,
  JOURNAL_ENTRY_RECORDED_HANDLER.event_type: JOURNAL_ENTRY_RECORDED_HANDLER,
  JOURNAL_ENTRY_REVERSED_HANDLER.event_type: JOURNAL_ENTRY_REVERSED_HANDLER,
  # Journal GL shape plus the discharges_event_id link to the invoice/bill.
  PAYMENT_RECEIVED_HANDLER.event_type: PAYMENT_RECEIVED_HANDLER,
  BILL_PAID_HANDLER.event_type: BILL_PAID_HANDLER,
  # QB source-class event types: distinct labels, same journal GL shape.
  "invoice_issued": JOURNAL_ENTRY_RECORDED_HANDLER,
  "bill_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "sales_receipt_recorded": JOURNAL_ENTRY_RECORDED_HANDLER,
  "cash_expense_recorded": JOURNAL_ENTRY_RECORDED_HANDLER,
  "check_written": JOURNAL_ENTRY_RECORDED_HANDLER,
  "credit_card_charge": JOURNAL_ENTRY_RECORDED_HANDLER,
  "credit_card_refund": JOURNAL_ENTRY_RECORDED_HANDLER,
  "deposit_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "inventory_adjusted": JOURNAL_ENTRY_RECORDED_HANDLER,
  # Bank-feed events post only once classified.
  **BANK_FEED_HANDLERS,
}


def get_python_handler(event_type: str) -> EventBlockPythonHandler | None:
  """Look up a Python handler by event_type, or None if not registered."""
  return EVENT_BLOCK_PYTHON_REGISTRY.get(event_type)


__all__ = ["EVENT_BLOCK_PYTHON_REGISTRY", "get_python_handler"]
