"""A native ledger closes consecutive months without a sync seeding period rows."""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampResult,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_seed_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _no_stamp(session, **kwargs):
  return StatementStampResult(stamped=False, note="no_coa_mapping")


def test_native_ledger_closes_two_consecutive_months(session):
  fcs = FiscalCalendarService()
  fcs.initialize(session, GRAPH_ID, closed_through="2026-04", actor_id="usr_1")
  # Seeded as initialize-ledger does on its setup month; nothing extends it.
  fcs.ensure_fiscal_periods(
    session,
    GRAPH_ID,
    start_period="2026-04",
    end_period="2026-05",
    closed_through="2026-04",
  )
  session.commit()

  closer = PeriodCloseService(fcs, statement_stamper=_no_stamp)
  for period in ("2026-05", "2026-06"):
    closer.close(
      session,
      GRAPH_ID,
      period,
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )
    session.commit()

  statuses = dict(
    session.query(FiscalPeriod.name, FiscalPeriod.status)
    .filter(FiscalPeriod.name.in_(["2026-05", "2026-06"]))
    .all()
  )
  assert statuses == {"2026-05": "closed", "2026-06": "closed"}
  assert fcs.get(session, GRAPH_ID).closed_through_period == "2026-06"
