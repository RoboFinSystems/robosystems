"""Taking posted entries out of an open month's books without deleting them.

A correction that rebuilds what was posted (a reconciling item's restate, a
replaced opening balance) supersedes the old entries: they move to
``superseded`` with a record of why, keep their lines and ids, and every
reader of the books drops them. A transaction left with no live entry is
voided. Callers hold the closed-period fence; this writes no ledger row.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import exists, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, aliased

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.entry_status import SUPERSEDED_ENTRY_STATUS


class EntryNotSupersedableError(ValueError):
  """Entries asked to be superseded that are not posted: a reversed one is
  corrected by its reversal, and a draft has not landed."""

  def __init__(self, entry_ids: list[str]) -> None:
    super().__init__(
      f"Only posted entries can be superseded; {', '.join(entry_ids)} is not posted."
    )
    self.entry_ids = entry_ids


def supersede_entries(
  session: Session,
  entry_ids: list[str],
  *,
  op: str,
  actor: str,
  reason: str | None = None,
  event_id: str | None = None,
) -> list[str]:
  """Supersede ``entry_ids``, all of them posted, and void each transaction
  none of whose entries is still live. Returns the ids superseded; raises
  :class:`EntryNotSupersedableError` before writing if any is not posted.
  Record what replaced them with :func:`record_replacements`."""
  if not entry_ids:
    return []
  not_posted = sorted(
    str(entry_id)
    for entry_id in session.scalars(
      select(Entry.id).where(Entry.id.in_(entry_ids), Entry.status != "posted")
    )
  )
  if not_posted:
    raise EntryNotSupersedableError(not_posted)
  now = datetime.now(UTC)
  record: dict[str, Any] = {"op": op, "actor": actor, "reason": reason}
  if event_id is not None:
    record["event_id"] = event_id
  superseded = [
    str(entry_id)
    for (entry_id,) in session.execute(
      update(Entry)
      .where(Entry.id.in_(entry_ids), Entry.status == "posted")
      .values(
        status=SUPERSEDED_ENTRY_STATUS,
        superseded_at=now,
        supersession=record,
        updated_at=now,
      )
      .returning(Entry.id)
      .execution_options(synchronize_session="fetch")
    )
  ]
  if superseded:
    sibling = aliased(Entry)
    session.execute(
      update(Transaction)
      .where(
        Transaction.id.in_(
          select(Entry.transaction_id).where(
            Entry.id.in_(superseded), Entry.transaction_id.is_not(None)
          )
        ),
        Transaction.status == "posted",
        ~exists().where(
          sibling.transaction_id == Transaction.id,
          sibling.status != SUPERSEDED_ENTRY_STATUS,
        ),
      )
      .values(status="void", updated_at=now)
      .execution_options(synchronize_session="fetch")
    )
  session.flush()
  return superseded


def record_replacements(
  session: Session, superseded_ids: list[str], replacement_ids: list[str]
) -> None:
  """Name the entries that replaced ``superseded_ids`` on their record."""
  if not superseded_ids:
    return
  session.execute(
    update(Entry)
    .where(Entry.id.in_(superseded_ids))
    .values(
      supersession=Entry.supersession.op("||")(
        literal({"replacement_entry_ids": sorted(replacement_ids)}, JSONB)
      )
    )
    .execution_options(synchronize_session="fetch")
  )
  session.flush()
