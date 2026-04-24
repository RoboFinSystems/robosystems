"""Manual adjustment event handler.

Fires when create-event-block runs with event_type='manual_adjustment' and
apply_handlers=True. Creates a balanced free-form draft entry (not tied to
a schedule) and links it to the event.

Replaces the standalone `create-manual-closing-entry` operation. The
underlying logic (balance validation + closed-period gate) lives in
`ScheduleService.create_manual_closing_entry` and is reused verbatim.

Event status after success: 'classified' (the draft still has to go
through close-period to become posted).
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.schedules.service import ScheduleService

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class ManualAdjustmentLineItem(BaseModel):
  """One debit/credit line in a manual adjustment."""

  element_id: str = Field(..., description="Element ID (chart of accounts).")
  debit_amount: int = Field(0, ge=0, description="Debit in cents.")
  credit_amount: int = Field(0, ge=0, description="Credit in cents.")
  description: str | None = None


class ManualAdjustmentMetadata(BaseModel):
  """Metadata for a manual_adjustment event."""

  posting_date: date = Field(..., description="Posting date for the draft entry.")
  memo: str = Field(
    ...,
    min_length=1,
    description="Required memo — usually cites the business event.",
  )
  line_items: list[ManualAdjustmentLineItem] = Field(
    ...,
    min_length=1,
    description="Line items; total debits must equal total credits.",
  )
  entry_type: Literal["closing", "adjusting", "standard", "reversing"] = Field(
    "closing", description="Entry type for the draft."
  )


def _validate_balance(
  line_items: list[ManualAdjustmentLineItem],
) -> tuple[int, int, list[str]]:
  """Return (total_debit, total_credit, errors). Matches service validation."""
  total_debit = 0
  total_credit = 0
  errors: list[str] = []
  for i, li in enumerate(line_items):
    if not li.element_id:
      errors.append(f"Line item {i}: missing element_id")
      continue
    if li.debit_amount == 0 and li.credit_amount == 0:
      errors.append(f"Line item {i}: must have a non-zero debit or credit amount")
      continue
    if li.debit_amount > 0 and li.credit_amount > 0:
      errors.append(f"Line item {i}: cannot have both debit and credit amounts")
      continue
    total_debit += li.debit_amount
    total_credit += li.credit_amount
  if total_debit != total_credit:
    errors.append(
      f"Manual entry does not balance: total_debit={total_debit} "
      f"total_credit={total_credit}"
    )
  return total_debit, total_credit, errors


def dispatch(
  session: Session,
  event: Event,
  metadata: ManualAdjustmentMetadata,
  created_by: str,
) -> HandlerResult:
  """Create the manual draft entry; link it to the event."""
  service = ScheduleService()
  result = service.create_manual_closing_entry(
    session,
    posting_date=metadata.posting_date,
    line_items=[li.model_dump() for li in metadata.line_items],
    memo=metadata.memo,
    created_by=created_by,
    entry_type=metadata.entry_type,
  )

  assert result.entry_id is not None  # create outcome always has an id
  session.execute(
    update(Entry)
    .where(Entry.id == result.entry_id)
    .values(triggered_by_event_id=event.id)
  )

  logger.info(
    "manual_adjustment event %s fired: entry=%s amount=%s",
    event.id,
    result.entry_id,
    result.amount,
  )

  return HandlerResult(entry_ids=[result.entry_id])


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: ManualAdjustmentMetadata,
) -> HandlerPreview:
  """Validate balance + closed-period without persisting."""
  total_debit, total_credit, errors = _validate_balance(metadata.line_items)

  # Closed-period check mirrors _assert_period_not_closed in the service.
  if not errors:
    from sqlalchemy import text

    row = session.execute(
      text(
        """
        SELECT name, status
        FROM fiscal_periods
        WHERE start_date <= :posting_date AND end_date >= :posting_date
        LIMIT 1
        """
      ),
      {"posting_date": metadata.posting_date},
    ).fetchone()
    if row is not None and row.status == "closed":
      errors.append(
        f"Cannot draft manual entry in closed period {row.name!r} "
        f"(posting_date={metadata.posting_date})."
      )

  if errors:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={
        "total_debit_cents": total_debit,
        "total_credit_cents": total_credit,
      },
      validation_errors=errors,
    )

  return HandlerPreview(
    would_succeed=True,
    planned_entries=[
      {
        "posting_date": str(metadata.posting_date),
        "memo": metadata.memo,
        "entry_type": metadata.entry_type,
        "line_items": [li.model_dump() for li in metadata.line_items],
      }
    ],
    computed_values={
      "total_debit_cents": total_debit,
      "total_credit_cents": total_credit,
    },
    validation_errors=[],
  )


MANUAL_ADJUSTMENT_HANDLER = EventBlockPythonHandler(
  event_type="manual_adjustment",
  display_name="Manual Adjustment",
  metadata_schema=ManualAdjustmentMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
