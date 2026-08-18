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
