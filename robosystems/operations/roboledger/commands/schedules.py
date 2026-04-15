"""Write operations for schedules and closing entries.

Thin wrappers over `ScheduleService`. The service does the heavy lifting
(fact generation, entry creation, balance validation) — these functions
translate request bodies to service calls and assemble responses.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.schedules import (
  ClosingEntryResponse,
  CreateClosingEntryRequest,
  CreateManualClosingEntryRequest,
  CreateScheduleRequest,
  ScheduleCreatedResponse,
  TruncateScheduleRequest,
  TruncateScheduleResponse,
)
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.operations.roboledger.schedules.service import (
  EntryTemplate,
  ScheduleMetadata,
)


def _build_closing_entry_response(result) -> ClosingEntryResponse:
  """Map a ScheduleService ClosingEntryResult to the wire response."""
  reversal_resp = None
  if result.reversal:
    reversal_resp = ClosingEntryResponse(
      outcome=result.reversal.outcome,
      entry_id=result.reversal.entry_id,
      status=result.reversal.status,
      posting_date=result.reversal.posting_date,
      memo=result.reversal.memo,
      debit_element_id=result.reversal.debit_element_id,
      credit_element_id=result.reversal.credit_element_id,
      amount=result.reversal.amount,
      reason=result.reversal.reason,
    )
  return ClosingEntryResponse(
    outcome=result.outcome,
    entry_id=result.entry_id,
    status=result.status,
    posting_date=result.posting_date,
    memo=result.memo,
    debit_element_id=result.debit_element_id,
    credit_element_id=result.credit_element_id,
    amount=result.amount,
    reason=result.reason,
    reversal=reversal_resp,
  )


def create_schedule(
  session: Session,
  body: CreateScheduleRequest,
  created_by: str,
  service: ScheduleService,
) -> ScheduleCreatedResponse:
  """Create a schedule with pre-generated facts for each period.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  et = EntryTemplate(
    debit_element_id=body.entry_template.debit_element_id,
    credit_element_id=body.entry_template.credit_element_id,
    entry_type=body.entry_template.entry_type,
    memo_template=body.entry_template.memo_template,
    auto_reverse=body.entry_template.auto_reverse,
  )

  sm = None
  if body.schedule_metadata:
    sm = ScheduleMetadata(
      method=body.schedule_metadata.method,
      original_amount=body.schedule_metadata.original_amount,
      residual_value=body.schedule_metadata.residual_value,
      useful_life_months=body.schedule_metadata.useful_life_months,
      asset_element_id=body.schedule_metadata.asset_element_id,
    )

  structure = service.create_schedule(
    session,
    name=body.name,
    taxonomy_id=body.taxonomy_id,
    element_ids=body.element_ids,
    period_start=body.period_start,
    period_end=body.period_end,
    monthly_amount=body.monthly_amount,
    entry_template=et,
    schedule_metadata=sm,
    created_by=created_by,
    closed_through=body.closed_through,
  )

  # Count generated facts and distinct periods for the response.
  count_row = session.execute(
    text("SELECT COUNT(*) AS cnt FROM facts WHERE structure_id = :sid"),
    {"sid": structure.id},
  ).fetchone()
  period_row = session.execute(
    text(
      "SELECT COUNT(DISTINCT (period_start, period_end)) AS cnt "
      "FROM facts WHERE structure_id = :sid"
    ),
    {"sid": structure.id},
  ).fetchone()

  session.commit()

  return ScheduleCreatedResponse(
    structure_id=structure.id,
    name=structure.name,
    taxonomy_id=structure.taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
  )


def truncate_schedule(
  session: Session,
  structure_id: str,
  body: TruncateScheduleRequest,
  updated_by: str,
  service: ScheduleService,
) -> TruncateScheduleResponse:
  """End a schedule early, deleting forward facts + stale drafts.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  result = service.truncate_schedule(
    session,
    structure_id=structure_id,
    new_end_date=body.new_end_date,
    reason=body.reason,
    updated_by=updated_by,
  )
  session.commit()

  return TruncateScheduleResponse(
    structure_id=result["structure_id"],
    new_end_date=result["new_end_date"],
    facts_deleted=result["facts_deleted"],
    reason=result["reason"],
  )


def create_closing_entry(
  session: Session,
  structure_id: str,
  body: CreateClosingEntryRequest,
  created_by: str,
  service: ScheduleService,
) -> ClosingEntryResponse:
  """Create a draft closing entry from a schedule's facts for a period.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  result = service.create_closing_entry(
    session,
    structure_id=structure_id,
    posting_date=body.posting_date,
    period_start=body.period_start,
    period_end=body.period_end,
    created_by=created_by,
    memo=body.memo,
  )
  session.commit()
  return _build_closing_entry_response(result)


def create_manual_closing_entry(
  session: Session,
  body: CreateManualClosingEntryRequest,
  created_by: str,
  service: ScheduleService,
) -> ClosingEntryResponse:
  """Create a manual (non-schedule) draft closing entry.

  Used for one-off adjustments (asset disposals, impairments,
  reclassifications). Total debits must equal total credits.
  Raises `ValueError` for validation failures — caller maps to 422.
  """
  result = service.create_manual_closing_entry(
    session,
    posting_date=body.posting_date,
    line_items=[
      {
        "element_id": li.element_id,
        "debit_amount": li.debit_amount,
        "credit_amount": li.credit_amount,
        "description": li.description,
      }
      for li in body.line_items
    ],
    memo=body.memo,
    created_by=created_by,
    entry_type=body.entry_type,
  )
  session.commit()
  return _build_closing_entry_response(result)
