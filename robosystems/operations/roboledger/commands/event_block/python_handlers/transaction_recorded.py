"""Transaction recorded event handler.

Fires when create-event-block runs with event_type='transaction_recorded'
and apply_handlers=True. Creates a standalone business-event Transaction
(no Entry / no LineItems) and links it to the event.

Replaces the standalone `create-transaction` operation. Standalone
Transactions are rare — callers usually attach journal entries in the same
request via `journal_entry_recorded`. This handler exists to preserve the
shape of the retired op without forking the data model.

Event status after success: 'fulfilled' (a standalone Transaction has no
draft lifecycle — it's either recorded or it isn't).
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.transactions import CreateTransactionRequest
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.commands.journal_entries import (
  create_transaction,
)

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class TransactionRecordedMetadata(BaseModel):
  """Metadata for a transaction_recorded event.

  Mirrors `CreateTransactionRequest`. `date` is the transaction's business
  date and is distinct from `event.occurred_at` (which records when the
  event was captured).
  """

  type: str = Field(..., description="Free-form business type (invoice, payment, ...)")
  date: date
  amount: int = Field(..., description="Amount in cents.")
  currency: str = "USD"
  description: str | None = None
  merchant_name: str | None = None
  reference_number: str | None = None
  number: str | None = None
  category: str | None = None
  due_date: date | None = None
  status: Literal["pending", "posted"] = "pending"


def dispatch(
  session: Session,
  event: Event,
  metadata: TransactionRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """Create the Transaction; link it to the event."""
  body = CreateTransactionRequest(**metadata.model_dump())
  response = create_transaction(session, body, created_by)

  session.execute(
    update(Transaction)
    .where(Transaction.id == response.id)
    .values(triggered_by_event_id=event.id)
  )

  logger.info(
    "transaction_recorded event %s fired: txn=%s type=%s amount=%s",
    event.id,
    response.id,
    metadata.type,
    metadata.amount,
  )

  return HandlerResult(transaction_ids=[response.id])


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: TransactionRecordedMetadata,
) -> HandlerPreview:
  """Read-only: echo back what would be created. No balance validation
  because a standalone Transaction has no entries to balance."""
  return HandlerPreview(
    would_succeed=True,
    planned_entries=[],
    computed_values={
      "type": metadata.type,
      "date": str(metadata.date),
      "amount_cents": metadata.amount,
      "currency": metadata.currency,
      "status": metadata.status,
    },
    validation_errors=[],
  )


TRANSACTION_RECORDED_HANDLER = EventBlockPythonHandler(
  event_type="transaction_recorded",
  display_name="Transaction Recorded",
  metadata_schema=TransactionRecordedMetadata,
  target_status="fulfilled",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
