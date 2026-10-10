"""Which of an account's ledger lines the bank has cleared.

On an account a bank feed keeps, every line the feed brought is the bank's
own record, so it has cleared by construction. The lines that did not come
from the feed (a payment recorded against a bill, a manual journal entry)
are what the bank may not have seen yet. Cleared is derived, never ticked.

Lines dated before the feed's first line are history the feed started from
(an opening balance, months kept elsewhere) and count as cleared.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from robosystems.operations.connection_service import BANK_FEED_PROVIDERS
from robosystems.operations.roboledger.entry_status import landed_entry_bindparam
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  WRITEBACK_EXCLUDED_EVENT_STATUSES,
)

# A reversal takes the origin of the entry it reverses, so reversing a feed
# line is the feed's business too.
_CASH_LINES_SQL = text("""
  SELECT li.id AS line_id,
         e.id AS entry_id,
         e.reversal_of,
         e.posting_date,
         e.status,
         COALESCE(li.debit_amount, 0) - COALESCE(li.credit_amount, 0) AS net,
         src.id AS event_id,
         COALESCE(src.source IN :feed_sources, FALSE) AS from_feed,
         COALESCE(NULLIF(li.description, ''), NULLIF(e.memo, ''),
                  src.description, 'Journal entry') AS description
  FROM line_items li
  JOIN entries e ON e.id = li.entry_id
  LEFT JOIN entries original ON original.id = e.reversal_of
  LEFT JOIN events src
    ON src.id = COALESCE(e.triggered_by_event_id, original.triggered_by_event_id)
  WHERE li.element_id = :element_id
    AND (:entity_id IS NULL OR e.entity_id = :entity_id)
    AND e.posting_date <= :through
    AND (
      e.status IN :landed_entry_statuses
      OR (
        NOT :landed_only
        AND e.status = 'draft'
        AND (src.id IS NULL OR src.status NOT IN :retracted)
      )
    )
  ORDER BY e.posting_date, e.id, li.id
""").bindparams(
  landed_entry_bindparam(),
  bindparam("feed_sources", value=sorted(BANK_FEED_PROVIDERS), expanding=True),
  bindparam("retracted", value=list(WRITEBACK_EXCLUDED_EVENT_STATUSES), expanding=True),
)


@dataclass(frozen=True)
class CashLine:
  """One ledger line on the account, debit-positive cents."""

  line_id: str
  entry_id: str
  reversal_of: str | None
  posting_date: date
  net_cents: int
  event_id: str | None
  from_feed: bool
  description: str


@dataclass(frozen=True)
class AccountLines:
  """An account's ledger lines through a date, as the close will leave them."""

  lines: list[CashLine]

  @property
  def feed_start(self) -> date | None:
    """The first day the feed booked a line on the account; None when it
    never has, and the account is not bank-fed."""
    return next((line.posting_date for line in self.lines if line.from_feed), None)

  def balance(self, through: date) -> int:
    return sum(line.net_cents for line in self.lines if line.posting_date <= through)

  def feed_activity(self, after: date, through: date) -> tuple[int, int]:
    """The booked feed lines dated after ``after``, to ``through``: how many,
    and their net."""
    lines = [
      line
      for line in self.lines
      if line.from_feed and after < line.posting_date <= through
    ]
    return len(lines), sum(line.net_cents for line in lines)

  def outstanding(self, through: date) -> list[CashLine]:
    """The lines the bank had not cleared by ``through``: not from the feed,
    dated from the feed's first day to ``through``. An entry and its
    reversal both inside that span cancel and are left out."""
    start = self.feed_start
    if start is None:
      return []
    candidates = [
      line
      for line in self.lines
      if not line.from_feed and start <= line.posting_date <= through
    ]
    entry_ids = {line.entry_id for line in candidates}
    cancelled = {
      entry_id
      for line in candidates
      if line.reversal_of in entry_ids
      for entry_id in (line.entry_id, line.reversal_of)
    }
    return [line for line in candidates if line.entry_id not in cancelled]


def account_lines(
  session: Session,
  element_id: str,
  through: date,
  *,
  entity_id: str | None,
  landed_only: bool = False,
) -> AccountLines:
  """Every landed line on the account to ``through``, and, unless
  ``landed_only``, the drafts the close will post."""
  rows = session.execute(
    _CASH_LINES_SQL,
    {
      "element_id": element_id,
      "entity_id": entity_id,
      "through": through,
      "landed_only": landed_only,
    },
  )
  return AccountLines(
    lines=[
      CashLine(
        line_id=str(row.line_id),
        entry_id=str(row.entry_id),
        reversal_of=str(row.reversal_of) if row.reversal_of else None,
        posting_date=row.posting_date,
        net_cents=int(row.net),
        event_id=str(row.event_id) if row.event_id else None,
        from_feed=bool(row.from_feed),
        description=str(row.description),
      )
      for row in rows
    ]
  )
