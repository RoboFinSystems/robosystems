"""Event Block Python handler registry — discriminator-keyed dispatch table.

Hub-defined handlers for complex event workflows. Each registry value is an
EventBlockPythonHandler frozen dataclass. Resolution happens in
`create_event_block` (commands.py) before the DSL fallback.

Adding a new handler:
  1. Create a new module under python_handlers/ (e.g., revenue_recognition.py)
  2. Define its EventBlockPythonHandler constant
  3. Register it below
"""

from __future__ import annotations

from .asset_disposed import ASSET_DISPOSED_HANDLER
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
  # AR/AP duality handlers — same GL shape as journal_entry_recorded
  # plus a post-dispatch step that resolves discharges_event_id to the
  # originating invoice/bill via QB's LinkedTxn refs (with a
  # reference_number fallback).
  PAYMENT_RECEIVED_HANDLER.event_type: PAYMENT_RECEIVED_HANDLER,
  BILL_PAID_HANDLER.event_type: BILL_PAID_HANDLER,
  # Remaining QB source-class events still dispatch through the journal
  # handler — same GL shape, no class-specific side effect yet.
  "invoice_issued": JOURNAL_ENTRY_RECORDED_HANDLER,
  "bill_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "sales_receipt_recorded": JOURNAL_ENTRY_RECORDED_HANDLER,
  # Additional QB source-class events. The dbt model routes each QB
  # transaction type to its own event_type so the purchase / treasury
  # semantic QB carries survives ingest. They all dispatch through the
  # journal handler because the on-approve GL shape is identical — only the
  # inbox label, event_category, and downstream filtering differ.
  "cash_expense_recorded": JOURNAL_ENTRY_RECORDED_HANDLER,
  "check_written": JOURNAL_ENTRY_RECORDED_HANDLER,
  "credit_card_charge": JOURNAL_ENTRY_RECORDED_HANDLER,
  "credit_card_refund": JOURNAL_ENTRY_RECORDED_HANDLER,
  "deposit_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "inventory_adjusted": JOURNAL_ENTRY_RECORDED_HANDLER,
}


def get_python_handler(event_type: str) -> EventBlockPythonHandler | None:
  """Look up a Python handler by event_type, or None if not registered."""
  return EVENT_BLOCK_PYTHON_REGISTRY.get(event_type)


__all__ = ["EVENT_BLOCK_PYTHON_REGISTRY", "get_python_handler"]
