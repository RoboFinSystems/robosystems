"""The balance sheet a ledger starts from (vault spec ``opening-balances``).

Books whose history does not come with them begin from a balance sheet: one
``opening`` entry per entity, dated the day before the entity's first fiscal
period, so every period's opening balances include it and no period's activity
does. It is the only writer of that entry type. The statements read it as a
balance; a window that starts before the books begin has no flows to show.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import NamedTuple

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.journal_entries import (
  JournalEntryResponse,
  RecordOpeningBalanceRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.library.reads.elements import efs_trait_by_element
from robosystems.operations.locking import RowLockedError, acquire_shared_period_fence
from robosystems.operations.roboledger.commands._guards import assert_accounts_postable
from robosystems.operations.roboledger.commands.journal_entries import (
  _entry_to_response,
  _flow_qnames,
  _load_line_items,
  provenance_for_source,
)
from robosystems.operations.roboledger.entity_scope import ensure_entity_id
from robosystems.operations.roboledger.entry_status import SUPERSEDED_ENTRY_STATUS
from robosystems.operations.roboledger.supersede import (
  record_replacements,
  supersede_entries,
)

OPENING = "opening"

# Income and expense before the books begin is already in retained earnings.
_PROFIT_AND_LOSS = frozenset({"revenue", "expense", "gain", "loss"})


class _OpeningLine(NamedTuple):
  """A line of an opening entry. A moved opening carries its lines whole,
  metadata included: the consolidation engine tags each line there."""

  element_id: str
  debit: int
  credit: int
  flow_element_id: str | None = None
  description: str | None = None
  metadata: dict | None = None


class OpeningBalanceError(ValueError):
  """An opening balance that cannot be recorded as given."""


class OpeningBalanceLockedError(ValueError):
  """The entity has closed a period, so its opening balance is part of
  closed history."""


def opening_entry(session: Session, entity_id: str) -> Entry | None:
  return session.execute(
    select(Entry).where(
      Entry.entity_id == entity_id,
      Entry.type == OPENING,
      Entry.status != SUPERSEDED_ENTRY_STATUS,
    )
  ).scalar_one_or_none()


def books_start(session: Session, entity_ids: list[str]) -> date | None:
  """The first day the books of ``entity_ids`` cover: the day after the latest
  of their opening entries. None when none of them starts from one."""
  if not entity_ids:
    return None
  eve = session.execute(
    select(func.max(Entry.posting_date)).where(
      Entry.entity_id.in_(entity_ids),
      Entry.type == OPENING,
      Entry.status != SUPERSEDED_ENTRY_STATUS,
    )
  ).scalar()
  return eve + timedelta(days=1) if eve else None


def opening_date(session: Session, entity_id: str) -> date | None:
  """The eve of the entity's first fiscal period, or None without one."""
  first_start = session.execute(
    text("SELECT min(start_date) FROM fiscal_periods WHERE entity_id = :e"),
    {"e": entity_id},
  ).scalar()
  return first_start - timedelta(days=1) if first_start else None


def move_opening_entry(session: Session, entity_id: str, actor: str) -> None:
  """Keep the opening entry on the eve of the first period after the
  calendar's start moves (allowed only before the first close): the entry is
  superseded and posted again, unchanged, on the new eve."""
  entry = opening_entry(session, entity_id)
  eve = opening_date(session, entity_id)
  if entry is None or eve is None or entry.posting_date == eve:
    return
  old_lines = _load_line_items(session, entry.id)
  superseded = supersede_entries(
    session,
    [entry.id],
    op="opening_moved",
    actor=actor,
    reason=f"The calendar's start moved; the opening balance is now dated {eve}.",
  )
  moved = _post_opening(
    session,
    entity_id=entity_id,
    eve=eve,
    memo=entry.memo or "Opening balances",
    lines=[
      _OpeningLine(
        li.element_id,
        li.debit_amount,
        li.credit_amount,
        li.flow_element_id,
        li.description,
        dict(li.metadata_ or {}),
      )
      for li in old_lines
    ],
    provenance=entry.provenance,
    created_by=actor,
  )
  record_replacements(session, superseded, [moved.id])


def _assert_not_closed(session: Session, entity_id: str) -> str:
  """The graph id, once the entity has a calendar and no closed month."""
  calendar = session.execute(
    text(
      "SELECT graph_id, closed_through_period FROM fiscal_calendar WHERE entity_id = :e"
    ),
    {"e": entity_id},
  ).first()
  if calendar is None:
    raise OpeningBalanceError(
      "Set up the entity's fiscal calendar first (initialize-fiscal-calendar): "
      "the opening balance is dated the day before its first month."
    )
  closed = session.execute(
    text(
      "SELECT 1 FROM fiscal_periods WHERE entity_id = :e AND status <> 'open' LIMIT 1"
    ),
    {"e": entity_id},
  ).first()
  if calendar.closed_through_period or closed:
    raise OpeningBalanceLockedError(
      "The opening balance can change only before the entity's first close. "
      "Correct it with an adjusting entry in an open month, or reopen back to "
      "the first month."
    )
  return str(calendar.graph_id)


def _lines(
  session: Session, body: RecordOpeningBalanceRequest, entity_id: str
) -> list[tuple[str, int, int]]:
  """The entry's lines, the absorbing equity account's last when the
  balances do not balance."""
  seen: set[str] = set()
  lines: list[tuple[str, int, int]] = []
  for i, balance in enumerate(body.balances):
    if bool(balance.debit_amount) == bool(balance.credit_amount):
      raise OpeningBalanceError(
        f"Balance {i}: give a debit or a credit, not both and not neither."
      )
    if balance.element_id in seen:
      raise OpeningBalanceError(
        f"Balance {i}: account {balance.element_id} is listed twice."
      )
    seen.add(balance.element_id)
    lines.append((balance.element_id, balance.debit_amount, balance.credit_amount))

  difference = sum(d for _, d, _ in lines) - sum(c for _, _, c in lines)
  if difference:
    if not body.equity_element_id:
      raise OpeningBalanceError(
        f"The balances differ by {abs(difference) / 100:,.2f}; name the equity "
        "account that takes the difference (equity_element_id), usually "
        "Opening Balance Equity."
      )
    lines.append(
      (body.equity_element_id, 0, difference)
      if difference > 0
      else (body.equity_element_id, -difference, 0)
    )

  ids = sorted({eid for eid, _, _ in lines})
  assert_accounts_postable(session, ids, entity_id=entity_id)
  traits = efs_trait_by_element(session, ids)
  income = [eid for eid in ids if traits.get(eid) in _PROFIT_AND_LOSS]
  if income:
    raise OpeningBalanceError(
      f"An opening balance is a balance sheet; {', '.join(income)} "
      "is an income or expense account. Its total before the books begin "
      "belongs in retained earnings."
    )
  if body.equity_element_id and traits.get(body.equity_element_id) != "equity":
    raise OpeningBalanceError(
      f"equity_element_id {body.equity_element_id} is not an equity account."
    )
  return lines


def record_opening_balance(
  session: Session, body: RecordOpeningBalanceRequest, created_by: str
) -> JournalEntryResponse:
  """Record, or replace before the first close, the entity's opening balance:
  one posted ``opening`` entry dated the eve of its first fiscal period.

  Raises `OpeningBalanceError` for balances that cannot stand as given and
  `OpeningBalanceLockedError` once the entity has closed a period, besides
  the account guards' own errors.
  """
  entity_id = ensure_entity_id(session, body.entity_id)
  graph_id = _assert_not_closed(session, entity_id)
  eve = opening_date(session, entity_id)
  if eve is None:
    raise OpeningBalanceError(
      "The entity's calendar has no periods yet; seed them before recording "
      "the opening balance."
    )
  first_month = f"{eve + timedelta(days=1):%Y-%m}"
  # The first close takes the exclusive side of this month's fence.
  acquire_shared_period_fence(
    session,
    graph_id,
    first_month,
    detail=f"Period {first_month} is being closed by another process. Retry in a moment.",
  )
  _assert_not_closed(session, entity_id)
  lines = _lines(session, body, entity_id)

  replaced = opening_entry(session, entity_id)
  if replaced is not None and replaced.status != "posted":
    raise OpeningBalanceError(
      f"The opening balance entry is {replaced.status}, so it cannot be "
      "replaced; record the correction as a journal entry."
    )
  superseded = (
    supersede_entries(
      session,
      [replaced.id],
      op="opening_replaced",
      actor=created_by,
      reason="A new opening balance was recorded for the entity.",
    )
    if replaced is not None
    else []
  )
  entry = _post_opening(
    session,
    entity_id=entity_id,
    eve=eve,
    memo=body.memo or "Opening balances",
    lines=[
      _OpeningLine(element_id, debit, credit) for element_id, debit, credit in lines
    ],
    provenance=provenance_for_source(None),
    created_by=created_by,
  )
  record_replacements(session, superseded, [entry.id])
  line_items = _load_line_items(session, entry.id)
  return _entry_to_response(entry, line_items, _flow_qnames(session, line_items))


def _post_opening(
  session: Session,
  *,
  entity_id: str,
  eve: date,
  memo: str,
  lines: list[_OpeningLine],
  provenance: str | None,
  created_by: str,
) -> Entry:
  """Write the entity's opening entry, its transaction and its lines,
  posted on ``eve``."""
  now = datetime.now(UTC)
  txn = Transaction(
    entity_id=entity_id,
    type="opening_balance",
    amount=sum(line.debit for line in lines),
    date=eve,
    description=memo,
    source="native",
    status="posted",
    posted_at=now,
    created_by=created_by,
  )
  session.add(txn)
  session.flush()
  entry = Entry(
    entity_id=entity_id,
    transaction_id=txn.id,
    type=OPENING,
    status="posted",
    posting_date=eve,
    memo=memo,
    provenance=provenance,
    posted_at=now,
    created_by=created_by,
  )
  session.add(entry)
  try:
    session.flush()
  except IntegrityError as exc:
    # Two records raced past the read above; the index lets one through.
    raise RowLockedError(
      "Another opening balance was recorded for this entity at the same time. "
      "Retry to replace it."
    ) from exc
  for order, line in enumerate(lines, start=1):
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=line.element_id,
        flow_element_id=line.flow_element_id,
        debit_amount=line.debit,
        credit_amount=line.credit,
        description=line.description,
        metadata_=line.metadata or {},
        line_order=order,
      )
    )
  session.flush()
  return entry
