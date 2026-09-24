"""bill_paid handler: the AP counterpart of ``payment_received``, linking the
payment to its ``bill_received`` event."""

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

# Event types a bill payment may discharge (VendorCredit is a separate flow).
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
