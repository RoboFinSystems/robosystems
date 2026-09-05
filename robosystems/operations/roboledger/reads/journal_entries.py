"""Journal entry read operations — the entry-centric view of the ledger.

`transactions.py` reads the ledger as transactions with entries hanging
off them. That shape cannot see an entry with no parent, and parentless
entries are a supported shape, not an anomaly: `Entry.transaction_id` is
nullable by design, `create_closing_entry` / `create_manual_closing_entry`
never set it, and `Entry.triggered_by_event_id` exists precisely to carry
the event chain for entries that have no transaction to carry it.

The practical consequence, before this module existed: every entry the
close posted was invisible to every list surface in the product. The
trial balance saw them (it joins line items directly), the statements
saw them, and nothing a user could browse did — an entry left the only
surface that showed it, the close-review outbox, at the moment it was
posted.

This module owns the entry projection and the row→entry grouping for
*both* readers. `period_drafts.list_period_drafts` is a caller, so the
close-review query and the journal query cannot drift apart.
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

# One projection, two orderings. The ORDER BY is the only part that varies
# between the close-review read and the journal read, and it cannot be a
# bind parameter — so it is interpolated from this fixed, internal map and
# never from anything a caller supplies.
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
  """The orderings this projection supports, and the only values that ever
  reach the interpolated ORDER BY.

  An enum rather than a bare string so a bad caller is a type error at
  check time instead of a KeyError at request time — and so nothing can
  wire a client-supplied ordering through by accident if this ever grows
  a public ``orderBy`` argument.
  """

  # Close review reads chronologically, grouped by schedule — the order a
  # reviewer walks the outbox in.
  PERIOD_REVIEW = "e.posting_date, s.name NULLS LAST, e.id"
  # The journal reads newest first, matching the transaction list beside it.
  RECENT_FIRST = "e.posting_date DESC, e.id"


_SQL_BY_ORDER = {
  order: text(_ENTRY_ROWS_TEMPLATE.format(order_by=order.value)) for order in EntryOrder
}

# A limit is always bound (the CTE needs one), so "no pagination" is a
# ceiling rather than an absent clause.
_NO_LIMIT = 1_000_000


class EntryRow(NamedTuple):
  """One entry with its line items, in raw DB units (cents).

  Unit conversion belongs to each caller's response contract — the close
  outbox reports cents, the journal reports dollars — so the shared
  fetch stays unit-neutral.
  """

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
  """Fetch entries with their line items, grouped, in raw cents.

  Every filter is optional and independent. No filter on
  `transaction_id` means parentless and parented entries come back
  together — which is the whole point; a caller that wants only
  standalone entries has to say so, and today none does.
  """
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
  """List journal entries with line items expanded, newest first.

  Entry-centric: an entry is returned on its own terms whether or not it
  has a parent transaction. Amounts are dollars, matching the
  transaction reads this sits beside; pagination counts entries.
  """
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
