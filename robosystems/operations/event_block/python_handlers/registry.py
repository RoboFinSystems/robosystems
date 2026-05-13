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
from .journal_entry_recorded import JOURNAL_ENTRY_RECORDED_HANDLER
from .journal_entry_reversed import JOURNAL_ENTRY_REVERSED_HANDLER
from .schedule_created import SCHEDULE_CREATED_HANDLER
from .schedule_entry_due import SCHEDULE_ENTRY_DUE_HANDLER
from .types import EventBlockPythonHandler

EVENT_BLOCK_PYTHON_REGISTRY: dict[str, EventBlockPythonHandler] = {
  ASSET_DISPOSED_HANDLER.event_type: ASSET_DISPOSED_HANDLER,
  SCHEDULE_CREATED_HANDLER.event_type: SCHEDULE_CREATED_HANDLER,
  SCHEDULE_ENTRY_DUE_HANDLER.event_type: SCHEDULE_ENTRY_DUE_HANDLER,
  JOURNAL_ENTRY_RECORDED_HANDLER.event_type: JOURNAL_ENTRY_RECORDED_HANDLER,
  JOURNAL_ENTRY_REVERSED_HANDLER.event_type: JOURNAL_ENTRY_REVERSED_HANDLER,
  # QB source-class events all dispatch through the journal handler — they post
  # journal entries on approve and only differ in inbox display + filtering.
  # Class-specific handlers (revenue recognition, payment-discharges-invoice)
  # are post-Phase-2 work; see event-driven-ledger.md Phase 4b/4c.
  "invoice_issued": JOURNAL_ENTRY_RECORDED_HANDLER,
  "bill_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "payment_received": JOURNAL_ENTRY_RECORDED_HANDLER,
  "bill_paid": JOURNAL_ENTRY_RECORDED_HANDLER,
  "sales_receipt_recorded": JOURNAL_ENTRY_RECORDED_HANDLER,
  # Additional QB source-class events (Phase 2.5, §3.14 of roadmap): the
  # QB importer used to collapse 7 transaction types into
  # `journal_entry_recorded`, hiding the purchase / treasury semantic
  # that QB already carries. The dbt model now routes them to these
  # specific event_types; all still dispatch through the same journal
  # handler because the on-approve GL shape is identical — only the
  # inbox label, event_category, and downstream filtering change.
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
