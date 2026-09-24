"""Journal entry reads: the entry-centric view of the ledger.

Unlike `transactions.py`, this sees entries with no parent transaction, a
supported shape (closing entries never set ``transaction_id``). It owns the
entry projection for both the journal and `period_drafts.list_period_drafts`,
so the two cannot drift.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Any, NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.transactions import (
  LedgerJournalEntryListResponse,
  LedgerJournalEntryResponse,
  LedgerLineItemResponse,
)

# ORDER BY cannot be a bind parameter, so it is interpolated, only ever from
# the fixed `EntryOrder` values, never caller input.
_ENTRY_ROWS_TEMPLATE = """
  WITH matched AS (
    SELECT e.id
    FROM entries e
    LEFT JOIN structures s ON s.id = e.source_structure_id
    WHERE (:start_date IS NULL OR e.posting_date >= :start_date)
      AND (:end_date IS NULL OR e.posting_date <= :end_date)
      AND (:status IS NULL OR e.status = :status)
      AND (:type IS NULL OR e.type = :type)
      AND (:provenance IS NULL OR e.provenance = :provenance)
      AND (:transaction_id IS NULL OR e.transaction_id = :transaction_id)
    ORDER BY {order_by}
    LIMIT :limit OFFSET :offset
  )
  SELECT
    e.id                  AS entry_id,
    e.number              AS number,
    e.transaction_id      AS transaction_id,
    e.posting_date        AS posting_date,
    e.type                AS entry_type,
    e.status              AS status,
    e.memo                AS memo,
    e.provenance          AS provenance,
    e.source_structure_id AS source_structure_id,
    e.triggered_by_event_id AS triggered_by_event_id,
    e.reversal_of         AS reversal_of,
    e.posted_at           AS posted_at,
    s.name                AS source_structure_name,
    li.id                 AS line_item_id,
    li.element_id         AS element_id,
    el.code               AS element_code,
    el.name               AS element_name,
    li.debit_amount       AS debit_amount,
    li.credit_amount      AS credit_amount,
    li.description        AS line_description,
    li.line_order         AS line_order
  -- LEFT, not INNER, on line_items: `matched` already picked this page of
  -- entries, so an entry with no line items would be dropped here after
  -- being counted and after consuming a LIMIT slot — a page silently
  -- shorter than it claims, and a total that disagrees with the rows.
  -- Double-entry should make that impossible; if a bug elsewhere ever
  -- breaks the invariant, the entry shows up empty rather than vanishing.
  FROM entries e
  JOIN matched m ON m.id = e.id
  LEFT JOIN structures s ON s.id = e.source_structure_id
  LEFT JOIN line_items li ON li.entry_id = e.id
  LEFT JOIN elements el ON el.id = li.element_id
  ORDER BY {order_by}, li.line_order, li.id
"""

_COUNT_SQL = text("""
  SELECT count(*)
  FROM entries e
  WHERE (:start_date IS NULL OR e.posting_date >= :start_date)
    AND (:end_date IS NULL OR e.posting_date <= :end_date)
    AND (:status IS NULL OR e.status = :status)
    AND (:type IS NULL OR e.type = :type)
    AND (:provenance IS NULL OR e.provenance = :provenance)
    AND (:transaction_id IS NULL OR e.transaction_id = :transaction_id)
""")


class EntryOrder(StrEnum):
  """The only values that reach the interpolated ORDER BY."""

  PERIOD_REVIEW = "e.posting_date, s.name NULLS LAST, e.id"
  RECENT_FIRST = "e.posting_date DESC, e.id"


_SQL_BY_ORDER = {
  order: text(_ENTRY_ROWS_TEMPLATE.format(order_by=order.value)) for order in EntryOrder
}

# The CTE always binds a limit, so "no pagination" is a ceiling.
_NO_LIMIT = 1_000_000


class EntryRow(NamedTuple):
  """One entry with its line items, in cents; callers convert units."""

  entry_id: str
  number: str | None
  transaction_id: str | None
  posting_date: date
  type: str
  status: str
  memo: str | None
  provenance: str | None
  source_structure_id: str | None
  source_structure_name: str | None
  triggered_by_event_id: str | None
  reversal_of: str | None
  posted_at: Any
  line_items: list[dict[str, Any]]


def fetch_entry_rows(
  session: Session,
  *,
  start_date: date | None = None,
  end_date: date | None = None,
  status: str | None = None,
  type: str | None = None,
  provenance: str | None = None,
  transaction_id: str | None = None,
  limit: int | None = None,
  offset: int = 0,
  order_by: EntryOrder = EntryOrder.RECENT_FIRST,
) -> list[EntryRow]:
  """Fetch entries with their line items, in cents. Without a
  ``transaction_id`` filter, parentless entries are included."""
  rows = session.execute(
    _SQL_BY_ORDER[order_by],
    {
      "start_date": start_date,
      "end_date": end_date,
      "status": status,
      "type": type,
      "provenance": provenance,
      "transaction_id": transaction_id,
      "limit": limit if limit is not None else _NO_LIMIT,
      "offset": offset,
    },
  ).fetchall()

  by_entry: dict[str, dict[str, Any]] = {}
  order: list[str] = []
  for row in rows:
    entry_id = row.entry_id
    if entry_id not in by_entry:
      order.append(entry_id)
      by_entry[entry_id] = {
        "entry_id": entry_id,
        "number": row.number,
        "transaction_id": row.transaction_id,
        "posting_date": row.posting_date,
        "type": row.entry_type,
        "status": row.status,
        "memo": row.memo,
        "provenance": row.provenance,
        "source_structure_id": row.source_structure_id,
        "source_structure_name": row.source_structure_name,
        "triggered_by_event_id": row.triggered_by_event_id,
        "reversal_of": row.reversal_of,
        "posted_at": row.posted_at,
        "line_items": [],
      }
    if row.line_item_id is None:
      continue
    by_entry[entry_id]["line_items"].append(
      {
        "line_item_id": row.line_item_id,
        "element_id": row.element_id,
        "element_code": row.element_code,
        "element_name": row.element_name,
        "debit_amount": int(row.debit_amount or 0),
        "credit_amount": int(row.credit_amount or 0),
        "description": row.line_description,
        "line_order": row.line_order,
      }
    )

  return [EntryRow(**by_entry[entry_id]) for entry_id in order]


def list_journal_entries(
  session: Session,
  *,
  start_date: date | None = None,
  end_date: date | None = None,
  status: str | None = None,
  type: str | None = None,
  provenance: str | None = None,
  transaction_id: str | None = None,
  limit: int = 100,
  offset: int = 0,
) -> LedgerJournalEntryListResponse:
  """List journal entries with line items, newest first. Amounts are dollars;
  pagination counts entries."""
  params = {
    "start_date": start_date,
    "end_date": end_date,
    "status": status,
    "type": type,
    "provenance": provenance,
    "transaction_id": transaction_id,
  }
  total = session.execute(_COUNT_SQL, params).scalar() or 0

  entry_rows = fetch_entry_rows(
    session,
    limit=limit,
    offset=offset,
    order_by=EntryOrder.RECENT_FIRST,
    **params,
  )

  entries: list[LedgerJournalEntryResponse] = []
  for entry in entry_rows:
    total_debit = sum(li["debit_amount"] for li in entry.line_items)
    total_credit = sum(li["credit_amount"] for li in entry.line_items)
    entries.append(
      LedgerJournalEntryResponse(
        id=entry.entry_id,
        number=entry.number,
        transaction_id=entry.transaction_id,
        type=entry.type,
        status=entry.status,
        posting_date=entry.posting_date,
        memo=entry.memo,
        provenance=entry.provenance,
        source_structure_id=entry.source_structure_id,
        source_structure_name=entry.source_structure_name,
        triggered_by_event_id=entry.triggered_by_event_id,
        reversal_of=entry.reversal_of,
        posted_at=entry.posted_at,
        line_items=[
          LedgerLineItemResponse(
            id=li["line_item_id"],
            account_id=li["element_id"],
            account_name=li["element_name"],
            account_code=li["element_code"],
            debit_amount=cents_to_dollars(li["debit_amount"]),
            credit_amount=cents_to_dollars(li["credit_amount"]),
            description=li["description"],
            line_order=li["line_order"],
          )
          for li in entry.line_items
        ],
        total_debit=cents_to_dollars(total_debit),
        total_credit=cents_to_dollars(total_credit),
        balanced=total_debit == total_credit,
      )
    )

  return LedgerJournalEntryListResponse(
    entries=entries,
    pagination=create_pagination_info(total, limit, offset),
  )
