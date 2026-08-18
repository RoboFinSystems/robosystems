"""The close's QuickBooks pre-publish must not lose dedupe markers.

`_publish_drafts_to_qb` commits deliberately: the `qb_external_id` markers
record journal entries that now exist in QuickBooks, so they have to survive
whatever happens to the rest of the close. Otherwise a retried close republishes
them once QB's RequestId dedup window expires, and the customer's books get
duplicate entries.

`execute_event_block` bounds its wait for the event's row lock, so a conflicting
writer surfaces as an error over an **aborted** PostgreSQL transaction rather
than as a block. Each publish therefore needs its own SAVEPOINT — without one
the first such failure poisons the transaction and takes the markers of every
entry that already reached QuickBooks down with it.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

_MODULE = "robosystems.operations.roboledger.fiscal_calendar.close_service"
_QB = "robosystems.operations.roboledger.fiscal_calendar.qb_writeback"


class _AbortedTransaction(Exception):
  """Stands in for the database error a bounded lock wait raises."""


def _draft(event_id: str):
  entry = MagicMock(id=f"entry_{event_id}", memo="m", posting_date=date(2026, 1, 31))
  event = MagicMock(id=event_id)
  return entry, event


def _session():
  """Session whose `begin_nested` behaves like a PostgreSQL SAVEPOINT.

  The savepoint absorbs the failure and the outer transaction stays usable —
  which is the whole property under test, so it is modelled rather than mocked
  away.
  """
  session = MagicMock()
  session.journal = []

  class _Savepoint:
    def __enter__(self):
      session.journal.append("begin")
      return self

    def __exit__(self, exc_type, exc, tb):
      session.journal.append("rollback" if exc_type else "release")
      return False

  session.begin_nested.side_effect = lambda: _Savepoint()
  session.commit.side_effect = lambda: session.journal.append("commit")
  return session


def test_a_failed_publish_does_not_cost_the_markers_of_earlier_ones():
  """Entry 2 fails on a database error; 1 and 3 reached QB and still commit."""
  from robosystems.operations.roboledger.fiscal_calendar.close_service import (
    PeriodCloseService,
    WritebackFailed,
  )

  published: list[str] = []

  def _fake_execute(session, body, created_by, **_kwargs):
    if body.event_id == "evt_2":
      raise _AbortedTransaction("current transaction is aborted")
    published.append(body.event_id)
    return MagicMock(status="fulfilled", qb_error=None)

  session = _session()
  drafts = [_draft("evt_1"), _draft("evt_2"), _draft("evt_3")]

  with (
    patch(
      "robosystems.operations.event_block.commands.execute_event_block", _fake_execute
    ),
    patch("robosystems.database.SessionFactory"),
    patch(
      f"{_QB}.resolve_writeback_connection",
      return_value=MagicMock(connection_id="conn_1"),
    ),
    patch(f"{_QB}.select_writeback_eligible_entries", return_value=drafts),
  ):
    with pytest.raises(WritebackFailed) as exc:
      PeriodCloseService(MagicMock())._publish_drafts_to_qb(
        session,
        "kg_test",
        date(2026, 1, 1),
        date(2026, 1, 31),
        actor_id="usr_1",
      )

  # The two that reached QuickBooks did so, and the batch error names only the
  # one that failed.
  assert published == ["evt_1", "evt_3"]
  assert [f["event_id"] for f in exc.value.failed_events] == ["evt_2"]

  # The decisive part: the failure was contained in its own savepoint and the
  # markers were committed before WritebackFailed was raised.
  assert session.journal == [
    "begin",
    "release",
    "begin",
    "rollback",
    "begin",
    "release",
    "commit",
  ]
