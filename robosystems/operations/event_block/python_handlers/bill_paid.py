"""Bill paid event handler — AP-side duality resolution.

Symmetric counterpart to ``payment_received``. The QB BillPayment row
arrives with the same nested-entries metadata shape as
``journal_entry_recorded`` (DR AP / CR Cash); the journal handler
writes the GL rows unchanged, and this handler resolves the discharge
link so ``event.discharges_event_id`` points at the originating
``bill_received`` event.

See ``payment_received`` for the matching algorithm — same two-stage
lookup (LinkedTxn first, qb_reference_number ↔ qb_doc_number fallback)
with the candidate event-type set restricted to AP origins.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.event import Event

from .journal_entry_recorded import (
  JournalEntryRecordedMetadata,
)
from .journal_entry_recorded import (
  dispatch as journal_dispatch,
)
from .journal_entry_recorded import (
  dispatch_preview as journal_dispatch_preview,
)
from .payment_received import _link_discharge
from .types import EventBlockPythonHandler, HandlerPreview, HandlerResult

# Event types whose row a bill_paid event is allowed to discharge.
# Bill is the canonical AP scenario. Excluded: VendorCredit (negative
# AP, separate flow), JournalEntry (no AP semantic).
_AP_ORIGINATING_TYPES: tuple[str, ...] = ("bill_received",)


def dispatch(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """Run the journal-entry GL writes, then stamp the discharge link."""
  result = journal_dispatch(session, event, metadata, created_by)
  _link_discharge(session, event, _AP_ORIGINATING_TYPES)
  return result


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: JournalEntryRecordedMetadata,
) -> HandlerPreview:
  """Preview is identical to journal_entry_recorded — see payment_received."""
  return journal_dispatch_preview(session, body, metadata)


BILL_PAID_HANDLER = EventBlockPythonHandler(
  event_type="bill_paid",
  display_name="Bill Paid",
  metadata_schema=JournalEntryRecordedMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
