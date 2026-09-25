"""A stray pending row in another quarter does not stall the nightly chain.

A failed process run on a quarter's last night leaves rows no later nightly
revisits. With no process run in flight to drain them, the chain wakes the
master instead of waiting forever. Runs against the real platform test
database.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance

from robosystems.adapters.sec.pipeline.sensors import sec_incremental_pipeline_sensor
from robosystems.models.core.graph.source_file import SourceFile
from tests.adapters.sec.pipeline.test_sensors import _build_run_status_context

pytestmark = pytest.mark.integration


def _sec_graph(test_db) -> None:
  from robosystems.models.core.graph.graph import Graph

  if test_db.query(Graph).filter(Graph.graph_id == "sec").first() is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=test_db,
    )


def test_the_master_wakes_past_a_stranded_quarter(test_db):
  _sec_graph(test_db)
  test_db.add(
    SourceFile(
      graph_id="sec",
      partition_key=f"2026-Q1_0000000001_{uuid.uuid4().hex[:10]}",
      storage_key=f"raw/stray/{uuid.uuid4().hex}.zip",
      file_type="xbrl_filing",
      status="pending",
    )
  )
  test_db.commit()

  factory = MagicMock(side_effect=lambda: test_db)
  context = _build_run_status_context(
    sensor_name="sec_incremental_pipeline_sensor",
    job_name="sec_process",
    run_id="run-proc-q2-done",
    tags={
      "mode": "incremental",
      "dagster/partition": "2026-Q2",
      "batch_id": "20260630-21",
    },
    instance=DagsterInstance.ephemeral(),
  )
  with (
    patch("robosystems.database.session", factory),
    patch("robosystems.adapters.sec.pipeline.sensors.env") as mock_env,
    patch.object(test_db, "close"),
  ):
    mock_env.ENVIRONMENT = "prod"
    requests = list(sec_incremental_pipeline_sensor(context))

  assert [r.job_name for r in requests] == ["shared_master_wake"]
