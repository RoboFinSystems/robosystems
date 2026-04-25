"""Handler engine — evaluate a transaction_template and produce GL rows.

Phase 3: immediate postings only. One template entry → one Transaction +
one Entry + two LineItems (debit + credit). All rows are flushed but not
committed; the caller (create_event_block) commits the whole unit of work
atomically so failures roll back cleanly.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction

from .template import (
  TemplateInterpolationError,
  build_event_context,
  build_handler_context,
  interpolate,
)


class EngineValidationError(Exception):
  """Template produced invalid or unbalanced GL entries."""


def _resolve_amount(expr: str, context: dict) -> int:
  """Resolve a template amount expression to a non-negative int (cents)."""
  try:
    value = interpolate(expr, context)
  except TemplateInterpolationError:
    raise
  if value is None:
    raise EngineValidationError(
      f"Amount expression '{expr}' resolved to None — event field may be missing"
    )
  try:
    cents = int(value)
  except (TypeError, ValueError):
    raise EngineValidationError(
      f"Amount expression '{expr}' resolved to non-numeric value: {value!r}"
    )
  if cents < 0:
    raise EngineValidationError(
      f"Amount expression '{expr}' resolved to negative value: {cents}. "
      "Amounts must be non-negative cents. Use a separate event type for reversals."
    )
  return cents


def apply_handler(
  session: Session,
  event: Event,
  handler: EventHandler,
  *,
  created_by: str,
) -> list[Transaction]:
  """Evaluate handler.transaction_template against event and persist GL rows.

  All rows are flushed (not committed). The calling create_event_block
  commits after this returns so the event row + all GL rows land atomically.

  Args:
      session: Tenant-scoped extensions session (search_path already set).
      event: The newly-created Event row (must have been flushed to get its id).
      handler: The resolved EventHandler row.
      created_by: User/system identifier for audit columns.

  Returns:
      List of Transaction rows created (typically one per template entry).

  Raises:
      EngineValidationError: Unbalanced entry, missing element, negative amount.
      TemplateInterpolationError: Bad template expression.
  """
  template = handler.transaction_template
  if not template or "transactions" not in template:
    raise EngineValidationError(
      f"Handler '{handler.name}' has no 'transactions' key in transaction_template"
    )

  context = {
    "event": build_event_context(event),
    "handler": build_handler_context(handler),
  }

  posting_date = (
    event.effective_at.date()
    if event.effective_at
    else (event.occurred_at.date() if event.occurred_at else date.today())
  )

  # One timestamp per handler invocation keeps the audit trail coherent —
  # Transaction, Entry, and every LineItem share the same created_at/updated_at.
  now = datetime.now(UTC)

  created_transactions: list[Transaction] = []

  for i, entry_spec in enumerate(template["transactions"]):
    entry_template = entry_spec.get("entry_template")
    if not entry_template:
      raise EngineValidationError(
        f"Template entry {i} is missing 'entry_template' key in handler '{handler.name}'"
      )

    debit_spec = entry_template.get("debit")
    credit_spec = entry_template.get("credit")
    if not debit_spec or not credit_spec:
      raise EngineValidationError(
        f"Template entry {i} must have both 'debit' and 'credit' keys"
      )

    debit_element_id = debit_spec.get("element_id", "")
    credit_element_id = credit_spec.get("element_id", "")
    if not debit_element_id or not credit_element_id:
      raise EngineValidationError(
        f"Template entry {i} debit/credit must each have an 'element_id'"
      )

    # Resolve element_ids (may themselves be template expressions)
    if "{{" in debit_element_id:
      debit_element_id = str(interpolate(debit_element_id, context))
    if "{{" in credit_element_id:
      credit_element_id = str(interpolate(credit_element_id, context))

    debit_cents = _resolve_amount(debit_spec["amount"], context)
    credit_cents = _resolve_amount(credit_spec["amount"], context)

    if debit_cents != credit_cents:
      raise EngineValidationError(
        f"Template entry {i} is unbalanced: debit={debit_cents} != credit={credit_cents}. "
        "Every entry must have equal debit and credit amounts."
      )

    # Create Transaction shell (the real-world event wrapper)
    txn = Transaction(
      type=event.event_type,
      amount=debit_cents,
      currency=event.currency or "USD",
      date=posting_date,
      source=event.source,
      triggered_by_event_id=event.id,
      status="pending",
      description=event.description or handler.name,
      created_at=now,
      updated_at=now,
      created_by=created_by,
    )
    session.add(txn)
    session.flush()

    # Create Entry (accounting interpretation)
    entry = Entry(
      transaction_id=txn.id,
      type="standard",
      posting_date=posting_date,
      status="draft",
      memo=event.description or handler.name,
      provenance="event_handler",
      triggered_by_event_id=event.id,
      created_at=now,
      updated_at=now,
      created_by=created_by,
    )
    session.add(entry)
    session.flush()

    # Create debit LineItem
    debit_li = LineItem(
      entry_id=entry.id,
      element_id=debit_element_id,
      debit_amount=debit_cents,
      credit_amount=0,
      line_order=0,
      created_at=now,
      updated_at=now,
    )
    # Create credit LineItem
    credit_li = LineItem(
      entry_id=entry.id,
      element_id=credit_element_id,
      debit_amount=0,
      credit_amount=credit_cents,
      line_order=1,
      created_at=now,
      updated_at=now,
    )
    session.add(debit_li)
    session.add(credit_li)
    session.flush()

    created_transactions.append(txn)

  return created_transactions
