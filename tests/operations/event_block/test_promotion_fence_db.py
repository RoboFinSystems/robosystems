"""Autopilot promotion's period fence — real Postgres.

Autopilot writes GL, so the sweep takes the shared period fence before it
locks the obligation rows (the same order as close, which holds the
exclusive side then locks events). The MagicMock tests in
``test_promotion.py`` cannot see the fence: ``_period_covering`` returns a
mock whose ``status`` is never ``"closed"``, so it is a silent no-op there.

What the fence must and must not do is a question of which periods are
closed, so these run against a throwaway tenant schema in the test
Postgres. The specific trap: dispatched obligations stay ``classified``
for good, so a fence over the whole candidate set would fence every
period that ever held a schedule and fail the sweep on the first one that
has since closed — on a tenant's *second* month-end.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Structure, Taxonomy
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.promotion import promote_pending_obligations

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef01"
TAXONOMY_ID = "tax_sched_fence"
SCHEDULE_ID = "sch_fence_0001"
SWEEP_AS_OF = datetime(2026, 7, 5, tzinfo=UTC)


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_promo_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _seed_calendar(session, *, may_status: str = "closed") -> None:
  session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      name="2026-05",
      start_date=date(2026, 5, 1),
      end_date=date(2026, 5, 31),
      period_type="month",
      status=may_status,
    )
  )
  session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      name="2026-06",
      start_date=date(2026, 6, 1),
      end_date=date(2026, 6, 30),
      period_type="month",
      status="open",
    )
  )
  # The schedule must exist, or the orphan guard voids the obligations.
  session.add(
    Taxonomy(
      id=TAXONOMY_ID, name="Schedules", taxonomy_type="schedule", created_by="usr"
    )
  )
  session.flush()
  session.add(
    Structure(
      id=SCHEDULE_ID,
      taxonomy_id=TAXONOMY_ID,
      name="Depreciation",
      block_type="schedule",
      created_by="usr",
    )
  )
  session.flush()


def _obligation(session, *, status: str, period_end: date) -> Event:
  event = Event(
    event_type="schedule_entry_due",
    event_category="adjustment",
    occurred_at=datetime.combine(period_end, datetime.min.time(), tzinfo=UTC),
    source="schedule",
    status=status,
    created_by="usr",
    metadata_={
      "schedule_id": SCHEDULE_ID,
      "period_start": period_end.replace(day=1).isoformat(),
      "period_end": period_end.isoformat(),
    },
  )
  session.add(event)
  session.flush()
  return event


def _draft_for(session, event: Event) -> Entry:
  """The closing entry a past autopilot dispatch left behind."""
  entry = Entry(
    posting_date=event.occurred_at.date(),
    status="draft",
    memo="depreciation",
    created_by="usr",
    source_structure_id=SCHEDULE_ID,
    triggered_by_event_id=event.id,
  )
  session.add(entry)
  session.flush()
  return entry


def _sweep(session):
  """Autopilot sweep with the handler faked — the fence, not the GL, is under test."""
  handler = MagicMock()
  handler.metadata_schema.model_validate.side_effect = lambda m: m
  with patch(
    "robosystems.operations.event_block.promotion.get_python_handler",
    return_value=handler,
  ):
    result = promote_pending_obligations(
      session, GRAPH_ID, as_of=SWEEP_AS_OF, dispatch_handlers=True
    )
  return result, handler


class TestAutopilotFence:
  def test_closed_history_does_not_block_the_open_period(self, ext_session):
    """May was promoted, drafted and closed months ago; its obligation is
    still `classified` because that is where dispatched obligations rest.
    The June sweep must not fence May at all."""
    _seed_calendar(ext_session, may_status="closed")
    may = _obligation(ext_session, status="classified", period_end=date(2026, 5, 31))
    _draft_for(ext_session, may)
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    result, handler = _sweep(ext_session)

    assert result.classified_event_ids == [june.id]
    assert result.dispatched_event_ids == [june.id]
    assert result.errors == []
    handler.dispatch.assert_called_once()
    assert handler.dispatch.call_args.args[1].id == june.id
    assert ext_session.get(Event, may.id).status == "classified"

  def test_stranded_obligation_in_closed_period_is_reported_not_fatal(
    self, ext_session
  ):
    """A close with allow_stranded_obligations leaves an undrafted
    `classified` obligation behind in a closed period. It cannot be
    dispatched (that would write GL into the closed period), but it must
    not take the open period's obligation down with it."""
    _seed_calendar(ext_session, may_status="closed")
    may = _obligation(ext_session, status="classified", period_end=date(2026, 5, 31))
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    result, handler = _sweep(ext_session)

    assert result.dispatched_event_ids == [june.id]
    assert [eid for eid, _ in result.errors] == [may.id]
    assert "closed period" in result.errors[0][1]
    assert "2026-05" in result.errors[0][1]
    handler.dispatch.assert_called_once()
    assert ext_session.get(Event, may.id).status == "classified"

  def test_pending_obligation_in_closed_period_stays_pending(self, ext_session):
    """Left out of the sweep entirely: not flipped to `classified` (which
    would only strand it), not dispatched, and named in the errors."""
    _seed_calendar(ext_session, may_status="closed")
    may = _obligation(ext_session, status="pending", period_end=date(2026, 5, 31))
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    result, _handler = _sweep(ext_session)

    assert result.classified_event_ids == [june.id]
    assert result.dispatched_event_ids == [june.id]
    assert [eid for eid, _ in result.errors] == [may.id]
    assert ext_session.get(Event, may.id).status == "pending"

  def test_open_periods_fence_and_dispatch_normally(self, ext_session):
    """Sanity: with nothing closed the fence is transparent and both
    matured obligations dispatch."""
    _seed_calendar(ext_session, may_status="open")
    may = _obligation(ext_session, status="pending", period_end=date(2026, 5, 31))
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    result, handler = _sweep(ext_session)

    assert set(result.dispatched_event_ids) == {may.id, june.id}
    assert result.errors == []
    assert handler.dispatch.call_count == 2


class TestSweepLocksOnlyTheWriteSet:
  """The sweep locks what it writes — pending + stranded — not the classified
  history, and it decides status from the locked read, not the preview."""

  def test_does_not_lock_the_classified_history(self, ext_session):
    """Close's publish holds schedule-sourced events FOR UPDATE while it posts
    to QuickBooks. A dispatched, drafted May obligation is exactly such a row;
    the June sweep must not wait behind it."""
    from robosystems.operations.locking import bounded_lock_wait

    _seed_calendar(ext_session, may_status="open")
    may = _obligation(ext_session, status="classified", period_end=date(2026, 5, 31))
    _draft_for(ext_session, may)
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    schema = ext_session.execute(text("select current_schema()")).scalar()
    holder = ext_session.get_bind().connect()
    try:
      holder.execute(text(f'SET search_path TO "{schema}"'))
      holder.execute(
        text("SELECT id FROM events WHERE id = :id FOR UPDATE"), {"id": may.id}
      )
      # Bounded so a sweep that does try the May row fails in 3s instead of
      # hanging the test behind the holder.
      with bounded_lock_wait(ext_session, "sweep blocked on the classified history"):
        result, handler = _sweep(ext_session)
    finally:
      holder.rollback()
      holder.close()

    assert result.dispatched_event_ids == [june.id]
    assert result.errors == []
    handler.dispatch.assert_called_once()

  def test_locked_read_sees_a_void_that_landed_after_the_preview(self, ext_session):
    """The unlocked preview puts the rows in the identity map. Without
    ``populate_existing`` the locked read hands back the preview's status,
    and a void that committed while the sweep waited on the lock is acted on
    as if it never happened — the lost update the lock exists to prevent."""
    import threading

    _seed_calendar(ext_session, may_status="open")
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()
    schema = ext_session.execute(text("select current_schema()")).scalar()

    holder = ext_session.get_bind().connect()
    holder.execute(text(f'SET search_path TO "{schema}"'))
    holder.execute(
      text("SELECT id FROM events WHERE id = :id FOR UPDATE"), {"id": june.id}
    )
    swept = threading.Event()

    def _void_then_release():
      # Let the sweep preview `pending` and block on the row lock, then void.
      swept.wait(timeout=5)
      time.sleep(0.5)
      holder.execute(
        text("UPDATE events SET status = 'voided' WHERE id = :id"), {"id": june.id}
      )
      holder.commit()
      holder.close()

    voider = threading.Thread(target=_void_then_release)
    voider.start()
    swept.set()
    try:
      result, handler = _sweep(ext_session)
    finally:
      voider.join(timeout=10)

    assert result.classified_event_ids == []
    assert result.dispatched_event_ids == []
    handler.dispatch.assert_not_called()
    ext_session.expire_all()
    assert ext_session.get(Event, june.id).status == "voided"


class TestDispatchRunsUnderASavepoint:
  """A database-level failure inside one handler must not abort the sweep's
  transaction: the other obligations still dispatch and the caller's commit
  succeeds. A lock wait is not a per-event error and propagates."""

  def test_a_poison_obligation_does_not_abort_the_others(self, ext_session):
    from sqlalchemy.exc import IntegrityError

    _seed_calendar(ext_session, may_status="open")
    may = _obligation(ext_session, status="pending", period_end=date(2026, 5, 31))
    june = _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    handler = MagicMock()
    handler.metadata_schema.model_validate.side_effect = lambda m: m

    def _dispatch(session, event, _typed, _created_by):
      if event.id == may.id:
        # A constraint violation from inside the handler — the class of
        # failure that aborts a Postgres transaction until rollback.
        session.execute(
          text("INSERT INTO events (id, event_type) VALUES (:id, NULL)"),
          {"id": "evt_poison"},
        )
        raise AssertionError("unreachable: the INSERT raises")

    handler.dispatch.side_effect = _dispatch
    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler",
      return_value=handler,
    ):
      result = promote_pending_obligations(
        ext_session, GRAPH_ID, as_of=SWEEP_AS_OF, dispatch_handlers=True
      )

    assert result.dispatched_event_ids == [june.id]
    assert [eid for eid, _ in result.errors] == [may.id]
    assert IntegrityError.__name__ in result.errors[0][1]
    # The transaction is still live: the caller's commit lands.
    ext_session.commit()
    ext_session.expire_all()
    assert ext_session.get(Event, june.id).status == "classified"
    assert ext_session.get(Event, may.id).status == "classified"

  def test_a_lock_wait_propagates_instead_of_becoming_an_error_line(self, ext_session):
    from robosystems.operations.locking import RowLockedError

    _seed_calendar(ext_session, may_status="open")
    _obligation(ext_session, status="pending", period_end=date(2026, 6, 30))
    ext_session.commit()

    handler = MagicMock()
    handler.metadata_schema.model_validate.side_effect = lambda m: m
    handler.dispatch.side_effect = RowLockedError("a closer holds the fence")
    with (
      patch(
        "robosystems.operations.event_block.promotion.get_python_handler",
        return_value=handler,
      ),
      pytest.raises(RowLockedError),
    ):
      promote_pending_obligations(
        ext_session, GRAPH_ID, as_of=SWEEP_AS_OF, dispatch_handlers=True
      )
