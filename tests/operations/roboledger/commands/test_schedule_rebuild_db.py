"""rebuild_schedule against real Postgres: one live obligation per period."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  RebuildScheduleRequest,
  ScheduleMetadataRequest,
  TerminateScheduleRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  rebuild_schedule,
  terminate_schedule,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef01"
AS_OF = datetime(2026, 2, 15, tzinfo=UTC)


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_rebuild_{uuid.uuid4().hex[:12]}"
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


def _element(session, name: str) -> str:
  element = Element(name=name, code=name[:8], created_by="test")
  session.add(element)
  session.flush()
  return str(element.id)


def _live_obligations_for(session, period_start: date) -> list[Event]:
  return (
    session.query(Event)
    .filter(
      Event.event_type == "schedule_entry_due",
      Event.status != "voided",
      Event.metadata_["period_start"].astext == period_start.isoformat(),
    )
    .all()
  )


def test_rebuild_leaves_one_live_obligation_for_a_drafted_period(ext_session):
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  debit = _element(session, "Depreciation Expense")
  credit = _element(session, "Accumulated Depreciation")

  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Depreciation",
      element_ids=[debit, credit],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 3, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit
      ),
    ),
    created_by="usr",
  )

  # January matures and autopilot drafts its closing entry.
  promote_pending_obligations(session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True)
  session.commit()
  assert len(_live_obligations_for(session, date(2026, 1, 1))) == 1

  rebuild_schedule(session, RebuildScheduleRequest(structure_id=created.structure_id))
  promote_pending_obligations(session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True)
  session.commit()

  assert len(_live_obligations_for(session, date(2026, 1, 1))) == 1
  january_drafts = (
    session.query(Entry)
    .filter(
      Entry.source_structure_id == created.structure_id,
      Entry.status == "draft",
      Entry.posting_date <= date(2026, 1, 31),
    )
    .count()
  )
  assert january_drafts == 1


def test_a_terminated_schedule_rebuilds_to_its_new_end(ext_session):
  """Terminate writes the new end; a rebuild must regenerate to it, not to the
  end the schedule was created with."""
  from robosystems.models.extensions.roboledger.fact import Fact

  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  debit = _element(session, "Insurance Expense")
  credit = _element(session, "Prepaid Insurance")
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Prepaid insurance",
      element_ids=[debit, credit],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 6, 30),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit
      ),
    ),
    created_by="usr",
  )
  terminate_schedule(
    session,
    TerminateScheduleRequest(
      structure_id=created.structure_id,
      new_end_date=date(2026, 3, 31),
      reason="policy cancelled",
    ),
    created_by="usr",
  )
  session.commit()

  rebuild_schedule(session, RebuildScheduleRequest(structure_id=created.structure_id))
  session.commit()

  last_fact = (
    session.query(Fact.period_end)
    .filter(Fact.structure_id == created.structure_id)
    .order_by(Fact.period_end.desc())
    .first()
  )
  assert last_fact is not None and last_fact.period_end == date(2026, 3, 31)
  live_after_cutoff = (
    session.query(Event)
    .filter(
      Event.event_type == "schedule_entry_due",
      Event.status != "voided",
      Event.metadata_["period_start"].astext > "2026-03-31",
    )
    .count()
  )
  assert live_after_cutoff == 0


def _facts(session, structure_id: str) -> list[tuple]:
  from robosystems.models.extensions.roboledger.fact import Fact

  return sorted(
    (
      (str(f.element_id), f.period_start or f.period_end, f.period_end, float(f.value))
      for f in session.query(Fact).filter(Fact.structure_id == structure_id)
    ),
    key=lambda row: (row[0], row[2], row[1]),
  )


def _depreciation(session) -> str:
  session.add(Entity(name="Fictional Co", created_by="usr"))
  debit = _element(session, "Depreciation Expense")
  credit = _element(session, "Accumulated Depreciation")
  asset = _element(session, "Equipment")
  return create_schedule(
    session,
    CreateScheduleRequest(
      name="Truck",
      element_ids=[debit, credit, asset],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      monthly_amount=100_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit
      ),
      schedule_metadata=ScheduleMetadataRequest(
        method="straight_line",
        original_amount=1_200_000,
        useful_life_months=12,
        asset_element_id=asset,
      ),
    ),
    created_by="usr",
  ).structure_id


def test_a_terminated_depreciation_rebuilds_to_the_same_books(ext_session):
  """Terminate re-anchors the basis to what was recognized; the rebuild must
  still carry the asset at cost, not plug the rest into the cutoff month."""
  session = ext_session
  structure_id = _depreciation(session)
  terminate_schedule(
    session,
    TerminateScheduleRequest(
      structure_id=structure_id,
      new_end_date=date(2026, 4, 30),
      reason="sold the truck",
    ),
    created_by="usr",
  )
  session.commit()
  terminated = _facts(session, structure_id)

  rebuild_schedule(session, RebuildScheduleRequest(structure_id=structure_id))
  session.commit()

  assert _facts(session, structure_id) == terminated
  assert max(end for _e, _s, end, _v in terminated) == date(2026, 4, 30)


def test_a_disposed_schedule_is_not_rebuilt(ext_session):
  session = ext_session
  structure_id = _depreciation(session)
  session.add(
    Event(
      id="evt_disposal",
      entity_id=str(session.query(Entity.id).scalar()),
      event_type="asset_disposed",
      event_category="adjustment",
      source="manual",
      status="classified",
      occurred_at=datetime(2026, 3, 15),
      metadata_={"schedule_id": structure_id},
      created_by="usr",
    )
  )
  session.commit()

  with pytest.raises(ValueError, match="Void the disposal first"):
    rebuild_schedule(session, RebuildScheduleRequest(structure_id=structure_id))
