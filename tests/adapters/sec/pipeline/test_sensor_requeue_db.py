"""A failed SEC filing is retried, a bounded number of times, after a backoff.

Against a real Postgres session. The process step only ever selects
``pending`` files, so an ``error`` row that nothing moves back is as final as
a skip.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from dagster import build_sensor_context

from robosystems.adapters.sec.pipeline.sensors import (
  ERROR_RETRY_MAX_ATTEMPTS,
  sec_processing_sensor,
)
from robosystems.models.core import Graph, SourceFile

pytestmark = pytest.mark.unit


def _failed_file(session, *, attempts: int, last_attempt_ago: timedelta) -> SourceFile:
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
    partition_key=f"2031-Q1_{uuid4().hex[:10]}_0000000000-31-000001",
    status="error",
  )
  row.attempts = attempts
  row.last_attempt_at = datetime.now(UTC) - last_attempt_ago
  session.commit()
  return row


def _tick(session):
  from dagster import DagsterInstance

  with (
    DagsterInstance.ephemeral() as instance,
    patch.object(instance, "get_runs", return_value=[]),
    patch("robosystems.adapters.sec.pipeline.sensors.env") as env,
    patch("robosystems.database.session", return_value=session),
  ):
    env.ENVIRONMENT = "prod"
    list(sec_processing_sensor(build_sensor_context(instance=instance)))


def test_a_failed_filing_is_requeued_after_its_backoff(test_db):
  due = _failed_file(test_db, attempts=1, last_attempt_ago=timedelta(hours=2))
  too_soon = _failed_file(test_db, attempts=1, last_attempt_ago=timedelta(minutes=5))
  spent = _failed_file(
    test_db, attempts=ERROR_RETRY_MAX_ATTEMPTS, last_attempt_ago=timedelta(hours=2)
  )

  ids = (due.id, too_soon.id, spent.id)

  _tick(test_db)

  statuses = [test_db.get(SourceFile, row_id).status for row_id in ids]
  assert statuses == ["pending", "error", "error"]
