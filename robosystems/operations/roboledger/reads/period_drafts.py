"""Period drafts read operation.

Returns every draft entry whose `posting_date` falls within a period,
fully expanded with line items, element names/codes, source schedule
name, and per-entry balance check. Pure read — no side effects.

The entry projection and row→entry grouping live in `journal_entries`,
shared with the journal read. This module is the close-review *outbox*
on top of it: the QB write-back disposition and the period aggregates.
Amounts stay in cents here — that is this response's contract, and the
shared fetch is unit-neutral.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse,
  DraftLineItem,
  PeriodDraftsResponse,
)
from robosystems.operations.roboledger.fiscal_calendar import period_date_range
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  WritebackConnection,
  writeback_eligible_entry_ids,
)
from robosystems.operations.roboledger.reads.journal_entries import (
  EntryOrder,
  fetch_entry_rows,
)


def list_period_drafts(
  session: Session,
  period: str,
  writeback: WritebackConnection | None = None,
) -> PeriodDraftsResponse:
  """Return all draft entries for review within a given YYYY-MM period.

  This is the close-review *outbox*: every queued draft entry, plus —
  when ``writeback`` is supplied (the qb_authoritative/hybrid QB
  connection the caller resolved against the platform DB) — a
  ``will_publish_to_qb`` flag per draft and a publish summary on the
  response. The publish predicate is shared with the actual close write
  (``qb_writeback.py``), so the preview cannot drift from what
  ``close-period`` does. When ``writeback`` is None (no write-back
  connection), every draft is local-only.
  """
  period_start, period_end = period_date_range(period)

  # Drafts that close would publish to QB — but only actually publish if
  # a write-back connection exists. Skip the eligibility query entirely
  # when there's no connection (the common native/no-connection case),
  # since the result would be discarded.
  has_writeback = writeback is not None
  eligible_ids = (
    writeback_eligible_entry_ids(session, period_start, period_end)
    if has_writeback
    else set()
  )

  entry_rows = fetch_entry_rows(
    session,
    start_date=period_start,
    end_date=period_end,
    status="draft",
    order_by=EntryOrder.PERIOD_REVIEW,
  )

  drafts: list[DraftEntryResponse] = []
  total_debit = 0
  total_credit = 0
  all_balanced = True
  qb_publish_count = 0
  for entry in entry_rows:
    line_items = [
      DraftLineItem(
        line_item_id=li["line_item_id"],
        element_id=li["element_id"],
        element_code=li["element_code"],
        element_name=li["element_name"],
        debit_amount=li["debit_amount"],
        credit_amount=li["credit_amount"],
        description=li["description"],
      )
      for li in entry.line_items
    ]
    entry_debit = sum(li.debit_amount for li in line_items)
    entry_credit = sum(li.credit_amount for li in line_items)
    balanced = entry_debit == entry_credit
    if not balanced:
      all_balanced = False
    total_debit += entry_debit
    total_credit += entry_credit
    # Publishes on close only if both halves of the predicate hold:
    # a write-back connection exists AND this draft is eligible.
    will_publish = has_writeback and entry.entry_id in eligible_ids
    if will_publish:
      qb_publish_count += 1
    drafts.append(
      DraftEntryResponse(
        entry_id=entry.entry_id,
        posting_date=entry.posting_date,
        type=entry.type,
        memo=entry.memo,
        provenance=entry.provenance,
        source_structure_id=entry.source_structure_id,
        source_structure_name=entry.source_structure_name,
        line_items=line_items,
        total_debit=entry_debit,
        total_credit=entry_credit,
        balanced=balanced,
        will_publish_to_qb=will_publish,
      )
    )

  return PeriodDraftsResponse(
    period=period,
    period_start=period_start,
    period_end=period_end,
    draft_count=len(drafts),
    total_debit=total_debit,
    total_credit=total_credit,
    all_balanced=all_balanced,
    qb_writeback_connection_id=writeback.connection_id if writeback else None,
    qb_write_policy=writeback.write_policy if writeback else None,
    qb_publish_count=qb_publish_count,
    local_only_count=len(drafts) - qb_publish_count,
    drafts=drafts,
  )
