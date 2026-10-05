"""A filing that keeps ending the process run is set aside; the rest of its
batch is not.

Against a real Postgres session. A run flushes once, at the end, so a run
that dies mid-batch leaves every filing it had finished ``processing`` beside
the one it died on, each with another attempt counted.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from robosystems.adapters.sec.pipeline.configs import ERROR_RETRY_MAX_ATTEMPTS
from robosystems.adapters.sec.pipeline.process import recover_stale_processing
from robosystems.models.core import Graph, SourceFile

pytestmark = pytest.mark.unit

QUARTER = "2031-Q1"


def _stale_file(
  session, *, attempts: int, started_ago: timedelta, quarter: str = QUARTER
) -> SourceFile:
  if Graph.get_by_id("sec", session) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=session,
    )
  row = SourceFile.create(
    graph_id="sec",
    storage_key=f"sec/raw/{uuid4().hex}.zip",
    file_type="xbrl_filing",
    session=session,
    partition_key=f"{quarter}_{uuid4().hex[:10]}_0000000000-31-000001",
    status="processing",
  )
  row.attempts = attempts
  row.last_attempt_at = datetime.now(UTC) - started_ago
  session.commit()
  return row


def _never_finished(_: SourceFile) -> bool:
  return False


def _statuses(session, *rows: SourceFile) -> list[str]:
  return [session.get(SourceFile, row.id).status for row in rows]


def test_files_a_dead_run_left_processing_go_back_to_pending(test_db):
  first = _stale_file(test_db, attempts=1, started_ago=timedelta(minutes=9))
  last = _stale_file(test_db, attempts=1, started_ago=timedelta(minutes=1))

  reset, set_aside = recover_stale_processing(test_db, f"{QUARTER}_", _never_finished)

  assert (reset, set_aside) == (2, None)
  assert _statuses(test_db, first, last) == ["pending", "pending"]


def test_the_file_every_run_died_on_is_set_aside_and_its_batch_is_not(test_db):
  """The batchmates carry as many attempts as the filing that killed the run:
  each rerun restores them from cache and counts another. Only the one in
  flight is spent."""
  batchmates = [
    _stale_file(
      test_db, attempts=ERROR_RETRY_MAX_ATTEMPTS, started_ago=timedelta(minutes=m)
    )
    for m in (9, 6)
  ]
  in_flight = _stale_file(
    test_db, attempts=ERROR_RETRY_MAX_ATTEMPTS, started_ago=timedelta(minutes=1)
  )

  reset, set_aside = recover_stale_processing(test_db, f"{QUARTER}_", _never_finished)

  assert reset == 2
  assert set_aside is not None and set_aside.id == in_flight.id
  assert _statuses(test_db, *batchmates, in_flight) == ["pending", "pending", "error"]
  assert "ended without finishing" in test_db.get(SourceFile, in_flight.id).error_reason


def test_a_file_under_the_cap_gets_another_run(test_db):
  in_flight = _stale_file(
    test_db, attempts=ERROR_RETRY_MAX_ATTEMPTS - 1, started_ago=timedelta(minutes=1)
  )

  reset, set_aside = recover_stale_processing(test_db, f"{QUARTER}_", _never_finished)

  assert (reset, set_aside) == (1, None)
  assert _statuses(test_db, in_flight) == ["pending"]


def test_a_file_that_finished_is_not_blamed_for_a_run_that_died_later(test_db):
  """A run that dies in its flush leaves only finished filings behind; the
  last of them did nothing wrong."""
  last = _stale_file(
    test_db, attempts=ERROR_RETRY_MAX_ATTEMPTS, started_ago=timedelta(minutes=1)
  )

  reset, set_aside = recover_stale_processing(test_db, f"{QUARTER}_", lambda _: True)

  assert (reset, set_aside) == (1, None)
  assert _statuses(test_db, last) == ["pending"]


def test_another_quarter_is_left_alone(test_db):
  other = _stale_file(
    test_db,
    attempts=ERROR_RETRY_MAX_ATTEMPTS,
    started_ago=timedelta(minutes=1),
    quarter="2031-Q2",
  )

  reset, set_aside = recover_stale_processing(test_db, f"{QUARTER}_", _never_finished)

  assert (reset, set_aside) == (0, None)
  assert _statuses(test_db, other) == ["processing"]
