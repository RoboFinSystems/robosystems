"""Period drafts read operation.

Returns every draft entry whose `posting_date` falls within a period,
fully expanded with line items, element names/codes, source schedule
name, and per-entry balance check. Pure read — no side effects.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse,
  DraftLineItem,
  PeriodDraftsResponse,
)
from robosystems.operations.roboledger.fiscal_calendar import period_date_range

_DRAFT_ENTRIES_SQL = text("""
  SELECT
    e.id               AS entry_id,
    e.posting_date     AS posting_date,
    e.type             AS entry_type,
    e.memo             AS memo,
    e.provenance       AS provenance,
    e.source_structure_id AS source_structure_id,
    s.name             AS source_structure_name,
    li.id              AS line_item_id,
    li.element_id      AS element_id,
    el.code            AS element_code,
    el.name            AS element_name,
    li.debit_amount    AS debit_amount,
    li.credit_amount   AS credit_amount,
    li.description     AS line_description
  FROM entries e
  LEFT JOIN structures s ON s.id = e.source_structure_id
  JOIN line_items li ON li.entry_id = e.id
  JOIN elements el ON el.id = li.element_id
  WHERE e.posting_date >= :period_start
    AND e.posting_date <= :period_end
    AND e.status = 'draft'
  ORDER BY e.posting_date, s.name NULLS LAST, e.id, li.line_order, li.id
""")


def list_period_drafts(session: Session, period: str) -> PeriodDraftsResponse:
  """Return all draft entries for review within a given YYYY-MM period."""
  period_start, period_end = period_date_range(period)

  rows = session.execute(
    _DRAFT_ENTRIES_SQL,
    {"period_start": period_start, "period_end": period_end},
  ).fetchall()

  # Group line items under their parent entry
  by_entry: dict[str, dict[str, Any]] = {}
  for row in rows:
    entry_id = row.entry_id
    if entry_id not in by_entry:
      by_entry[entry_id] = {
        "entry_id": entry_id,
        "posting_date": row.posting_date,
        "type": row.entry_type,
        "memo": row.memo,
        "provenance": row.provenance,
        "source_structure_id": row.source_structure_id,
        "source_structure_name": row.source_structure_name,
        "line_items": [],
      }
    by_entry[entry_id]["line_items"].append(
      DraftLineItem(
        line_item_id=row.line_item_id,
        element_id=row.element_id,
        element_code=row.element_code,
        element_name=row.element_name,
        debit_amount=int(row.debit_amount or 0),
        credit_amount=int(row.credit_amount or 0),
        description=row.line_description,
      )
    )

  drafts: list[DraftEntryResponse] = []
  total_debit = 0
  total_credit = 0
  all_balanced = True
  for entry_data in by_entry.values():
    line_items = entry_data["line_items"]
    entry_debit = sum(li.debit_amount for li in line_items)
    entry_credit = sum(li.credit_amount for li in line_items)
    balanced = entry_debit == entry_credit
    if not balanced:
      all_balanced = False
    total_debit += entry_debit
    total_credit += entry_credit
    drafts.append(
      DraftEntryResponse(
        entry_id=entry_data["entry_id"],
        posting_date=entry_data["posting_date"],
        type=entry_data["type"],
        memo=entry_data["memo"],
        provenance=entry_data["provenance"],
        source_structure_id=entry_data["source_structure_id"],
        source_structure_name=entry_data["source_structure_name"],
        line_items=line_items,
        total_debit=entry_debit,
        total_credit=entry_credit,
        balanced=balanced,
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
    drafts=drafts,
  )
