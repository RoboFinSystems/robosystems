"""Reopen racing the next month's close, against a real tenant schema."""

from __future__ import annotations

import threading
import time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  _reopen_under_fence,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  FiscalCalendarService,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd08"
P, P1 = "2026-01", "2026-02"


@pytest.fixture
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def test_reopen_cannot_land_under_a_month_closing_after_it(tenant):
  svc = FiscalCalendarService()
  with extensions_session(GRAPH) as s:
    svc.initialize(s, GRAPH, closed_through=P, actor_id="seed")
    svc.ensure_fiscal_periods(s, GRAPH, start_period=P, end_period=P1, closed_through=P)

  def closer():
    # The tail of close(P+1): advance under the calendar lock, then hold the
    # transaction open while the reopen arrives.
    with extensions_session(GRAPH) as s:
      svc.advance_closed_through(s, GRAPH, P1, actor_id="closer")
      s.query(FiscalPeriod).filter_by(graph_id=GRAPH, name=P1).one().status = "closed"
      s.flush()
      time.sleep(1.0)

  def reopener():
    time.sleep(0.3)
    try:
      with extensions_session(GRAPH) as s:
        _reopen_under_fence(
          s,
          GRAPH,
          P,
          actor_id="reopener",
          reason="adjustment",
          note=None,
          service=svc,
          actor_type="user",
        )
    except Exception:
      pass

  threads = [threading.Thread(target=closer), threading.Thread(target=reopener)]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()

  with extensions_session(GRAPH) as s:
    statuses = {r.name: r.status for r in s.query(FiscalPeriod)}
    closed_through = svc.get(s, GRAPH).closed_through_period
  assert statuses[P1] == "closed"
  assert statuses[P] == "closed"
  assert closed_through == P1
