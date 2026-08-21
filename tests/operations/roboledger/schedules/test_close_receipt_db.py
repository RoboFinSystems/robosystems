"""The close receipt survives the round trip through Postgres — real DB,
because the read is raw SQL.

``get_period_close_status`` selects from ``fiscal_periods`` with hand-written
SQL, and every mocked-session test in this suite drives ``session.execute``
through a canned result list, so none of them executes it. That gap has
already cost two defects on live books (PR #1228: a rule rewrite that missed
the key the evaluator reads, and a close summary that counted phantom
schedules) — both shipped green against tests that asserted what the author
wrote rather than what the database returns.

So these tests run the real statement against real tables.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.reads.schedules import get_period_close_status
from robosystems.operations.roboledger.schedules.service import ScheduleService

pytestmark = pytest.mark.unit

PERIOD_START = date(2026, 7, 1)
PERIOD_END = date(2026, 7, 31)

RECEIPT = {
  "version": 1,
  "period": "2026-07",
  "closed_at": "2026-08-20T03:35:00+00:00",
  "closed_by": "user_abc",
  "actor_type": "agent",
  "was_reclose": False,
  "entries_posted": 34,
  "entries_published_to_qb": 31,
  "entries_posted_locally": 3,
  "target_auto_advanced": True,
  "rule_summary": {"pass": 7, "fail": 0, "error": 0, "skipped": 0},
  "evaluated_structure_ids": ["struct_1", "struct_2"],
  "statements_stamped": True,
  "statement_stamp_note": None,
  "stamped_statement_sets": {"struct_bs": "fs_123"},
  "statement_rule_summary": {"pass": 20, "fail": 0},
}


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_receipt_{uuid.uuid4().hex[:12]}"
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


def _period(session, *, status, receipt):
  session.add(
    FiscalPeriod(
      graph_id="kg_test",
      name="2026-07",
      start_date=PERIOD_START,
      end_date=PERIOD_END,
      period_type="month",
      status=status,
      closed_at=datetime(2026, 8, 20, 3, 35) if status == "closed" else None,
      closed_by="user_abc" if status == "closed" else None,
      close_receipt=receipt,
    )
  )
  session.flush()


def _read(session):
  return get_period_close_status(session, ScheduleService(), PERIOD_START, PERIOD_END)


def test_receipt_round_trips_through_postgres(ext_session):
  """A stamped receipt comes back off the row with its numbers intact."""
  _period(ext_session, status="closed", receipt=RECEIPT)

  result = _read(ext_session)

  assert result.period_status == "closed"
  assert result.close_receipt is not None
  # The split must survive, not just the total — reconstructing which
  # entries reached QuickBooks is the whole reason the receipt exists.
  assert result.close_receipt.entries_posted == 34
  assert result.close_receipt.entries_published_to_qb == 31
  assert result.close_receipt.entries_posted_locally == 3
  assert result.close_receipt.actor_type == "agent"
  assert result.close_receipt.rule_summary == {
    "pass": 7,
    "fail": 0,
    "error": 0,
    "skipped": 0,
  }
  assert result.close_receipt.stamped_statement_sets == {"struct_bs": "fs_123"}


def test_open_period_has_no_receipt(ext_session):
  _period(ext_session, status="open", receipt=None)

  result = _read(ext_session)

  assert result.period_status == "open"
  assert result.close_receipt is None


def test_period_closed_before_receipts_shipped_reads_as_no_receipt(ext_session):
  """A pre-existing closed period is not a failed close.

  There is no backfill, so every period closed before this shipped has
  `status='closed'` and a NULL receipt. That combination must read cleanly
  rather than looking like a close that went wrong.
  """
  _period(ext_session, status="closed", receipt=None)

  result = _read(ext_session)

  assert result.period_status == "closed"
  assert result.close_receipt is None


def test_unreadable_receipt_degrades_to_none(ext_session):
  """A receipt that fails validation must not break the status read.

  `get-period-close-status` is exactly what an operator calls when a close's
  response was lost in transport. If a malformed receipt made that read
  raise, the failure mode the receipt exists to fix would take the recovery
  path down with it.
  """
  _period(ext_session, status="closed", receipt={"version": 1, "nonsense": True})

  result = _read(ext_session)

  assert result.period_status == "closed"
  assert result.close_receipt is None


def test_writer_and_reader_agree_on_every_key():
  """What `close()` stamps is exactly what the read model parses.

  The #1228 defects were both a writer and a reader disagreeing about a key
  while each side's own test passed. This asserts the seam itself: build a
  receipt from a real `PeriodCloseResult`, then validate it with the model
  the read path uses. A field renamed on one side and not the other fails
  here rather than on a tenant's books.

  No database needed — the contract is between two pure projections.
  """
  from datetime import UTC

  from robosystems.models.api.extensions.schedules import CloseReceiptResponse
  from robosystems.operations.roboledger.fiscal_calendar.close_service import (
    PeriodCloseResult,
    _build_close_receipt,
  )

  result = PeriodCloseResult(
    period="2026-07",
    entries_posted=34,
    target_auto_advanced=True,
    calendar=object(),  # never serialized — a live ORM object by design
    was_reclose=False,
    entries_published_to_qb=31,
    entries_posted_locally=3,
    rule_summary={"pass": 7, "fail": 0},
    evaluated_structure_ids=("struct_1",),
    statements_stamped=True,
    statement_stamp_note=None,
    stamped_statement_sets={"struct_bs": "fs_123"},
    statement_rule_summary={"pass": 20, "fail": 0},
  )

  raw = _build_close_receipt(
    result,
    actor_id="user_abc",
    actor_type="agent",
    closed_at=datetime(2026, 8, 20, 3, 35, tzinfo=UTC),
  )

  # Must be JSON-serializable — it goes into a JSONB column, and a stray
  # datetime or tuple would only fail at commit time, mid-close.
  import json

  json.dumps(raw)

  parsed = CloseReceiptResponse.model_validate(raw)

  assert parsed.entries_posted == 34
  assert parsed.entries_published_to_qb == 31
  assert parsed.entries_posted_locally == 3
  assert parsed.period == "2026-07"
  assert parsed.actor_type == "agent"
  assert parsed.closed_by == "user_abc"
  assert parsed.evaluated_structure_ids == ["struct_1"]
  assert parsed.stamped_statement_sets == {"struct_bs": "fs_123"}
  # The calendar object must not have leaked into the receipt.
  assert "calendar" not in raw
