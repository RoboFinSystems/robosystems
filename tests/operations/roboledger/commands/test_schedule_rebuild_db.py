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
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  rebuild_schedule,
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
