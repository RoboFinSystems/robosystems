"""Bank lines a sync held out of a closed month.

A closed month takes no new bank lines, but dropping one would lose it: the
sync only counted it, and the next run overwrote the count. So the line is
written and held — ``voided`` with reason ``feed_in_closed_month`` — and
every later close of its entity's books is blocked until someone decides:
reopen the month (which gives its held lines back as ``captured``), or close
over them with ``allow_held_bank_lines``, which records the decision on each
line so it no longer blocks.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from robosystems.models.extensions import Event

FEED_IN_CLOSED_MONTH = "feed_in_closed_month"

_SAMPLE = 5


def hold(event: Event) -> None:
  """Retire a just-captured bank line its closed month cannot take."""
  event.status = "voided"
  event.metadata_ = {
    **dict(event.metadata_ or {}),
    "void_reason": FEED_IN_CLOSED_MONTH,
  }


def _posting_date():
  return func.date(func.coalesce(Event.effective_at, Event.occurred_at))


def _held(session: Session, entity_id: str):
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BANK_EVENT_TYPES,
  )

  return session.query(Event).filter(
    Event.entity_id == entity_id,
    Event.event_type.in_(BANK_EVENT_TYPES),
    Event.status == "voided",
    Event.metadata_["void_reason"].astext == FEED_IN_CLOSED_MONTH,
  )


def _undecided(session: Session, entity_id: str):
  return _held(session, entity_id).filter(
    Event.metadata_["held_acknowledged_in"].astext.is_(None)
  )


def undecided_held_lines(session: Session, entity_id: str) -> tuple[int, list[str]]:
  """How many held lines no one has decided on, and up to five of them as
  ``date description amount [event id]``, oldest first."""
  query = _undecided(session, entity_id)
  count = query.count()
  if not count:
    return 0, []
  sample = []
  for event in (
    query.order_by(_posting_date().asc(), Event.id.asc()).limit(_SAMPLE).all()
  ):
    posted = event.effective_at or event.occurred_at
    sample.append(
      f"{posted:%Y-%m-%d}: {event.description or event.external_id or ''} "
      f"{int(event.amount or 0) / 100:.2f} [{event.id}]"
    )
  return count, sample


def acknowledge_held_lines(
  session: Session, entity_id: str, *, period: str, actor_id: str
) -> int:
  """Record on each undecided held line that the close of ``period`` went
  over it. Returns how many."""
  from robosystems.operations.locking import ordered_lock_column

  lines = (
    _undecided(session, entity_id)
    .order_by(ordered_lock_column())
    .populate_existing()
    .with_for_update()
    .all()
  )
  now = datetime.now(UTC).isoformat()
  for line in lines:
    line.metadata_ = {
      **dict(line.metadata_ or {}),
      "held_acknowledged_in": period,
      "held_acknowledged_by": actor_id,
      "held_acknowledged_at": now,
    }
  session.flush()
  return len(lines)


def restore_held_lines(session: Session, entity_id: str, start: date, end: date) -> int:
  """Give back as ``captured`` the held lines dated ``start``..``end``, the
  month a reopen opened. Returns how many."""
  from robosystems.operations.locking import ordered_lock_column

  lines = (
    _held(session, entity_id)
    .filter(_posting_date() >= start, _posting_date() <= end)
    .order_by(ordered_lock_column())
    .with_for_update()
    .all()
  )
  for line in lines:
    metadata = {
      key: value
      for key, value in dict(line.metadata_ or {}).items()
      if key != "void_reason" and not key.startswith("held_acknowledged_")
    }
    line.metadata_ = metadata
    line.status = "captured"
  session.flush()
  return len(lines)


__all__ = [
  "FEED_IN_CLOSED_MONTH",
  "acknowledge_held_lines",
  "hold",
  "restore_held_lines",
  "undecided_held_lines",
]
