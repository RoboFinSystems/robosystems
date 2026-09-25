"""A per-file ingest into the graph holds the graph's materialize lock.

Against real Valkey. The per-file copy used to run with no lock and no
concurrency tag, alongside a full materialize of the same database.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
from robosystems.operations.graph.commands.ingest_file import ingest_file_cmd

pytestmark = pytest.mark.integration

MB = 1024**2


@pytest.fixture
def graph_id():
  graph_id = f"kgtest{uuid.uuid4().hex[:12]}"
  client = create_redis_client(ValkeyDatabase.LOCKS)
  yield graph_id
  client.delete(f"lock:graph_materialize:{graph_id}")
  client.close()


def _lock_holder(graph_id: str) -> str | None:
  client = create_redis_client(ValkeyDatabase.LOCKS)
  try:
    return client.get(f"lock:graph_materialize:{graph_id}")
  finally:
    client.close()


async def _ingest(
  graph_id: str, *, staged: bool, enqueue: AsyncMock, size: int = 1 * MB
):
  graph_file = MagicMock()
  graph_file.graph_id = graph_id
  graph_file.upload_status = "pending"
  graph_file.duckdb_status = None
  graph_file.operation_id = None
  graph_file.file_size_bytes = 1 * MB
  graph_file.row_count = 10

  s3 = MagicMock()
  s3.s3_client.head_object.return_value = {"ContentLength": size}
  graph = MagicMock(parent_graph_id=None, graph_tier="ladybug-standard")
  storage = {
    "allowed": True,
    "retryable": False,
    "limit_gb": 20,
    "enforced_storage_gb": 1,
  }
  db = MagicMock()

  with (
    patch("robosystems.models.core.GraphFile.get_by_id", return_value=graph_file),
    patch(
      "robosystems.models.core.GraphFile.get_all_for_table",
      return_value=[
        MagicMock(upload_status="uploaded", file_size_bytes=1 * MB, row_count=10)
      ],
    ),
    patch("robosystems.models.core.Graph.get_by_id", return_value=graph),
    patch(
      "robosystems.operations.graph.commands.ingest_file.S3Client", return_value=s3
    ),
    patch(
      "robosystems.operations.graph.commands.ingest_file._measure_row_count",
      return_value=(10, True),
    ),
    patch.object(
      IngestionLimitChecker,
      "check_instance_storage",
      new_callable=AsyncMock,
      return_value=storage,
    ),
    patch(
      "robosystems.operations.graph.engine.direct_staging.stage_file_directly",
      new_callable=AsyncMock,
      return_value={"status": "success" if staged else "error"},
    ),
    patch("robosystems.worker.client.enqueue_task", enqueue),
  ):
    table = db.query.return_value.filter.return_value.with_for_update.return_value
    table.first.return_value = MagicMock(id="tbl_1", table_name="Entity")
    return await ingest_file_cmd(
      graph_id=graph_id,
      file_id="file_1",
      ingest_to_graph=True,
      current_user=MagicMock(id="usr_test"),
      db=db,
      background_tasks=MagicMock(),
    )


@pytest.mark.asyncio
async def test_ingest_to_graph_refuses_while_a_materialize_holds_the_graph(graph_id):
  client = create_redis_client(ValkeyDatabase.LOCKS)
  client.set(f"lock:graph_materialize:{graph_id}", "materialize-run", ex=300)
  client.close()
  enqueue = AsyncMock(return_value={"operation_id": "op_x"})

  with pytest.raises(HTTPException) as exc:
    await _ingest(graph_id, staged=True, enqueue=enqueue)

  assert exc.value.status_code == 409
  enqueue.assert_not_awaited()
  assert _lock_holder(graph_id) == "materialize-run"


@pytest.mark.asyncio
async def test_the_graph_write_carries_the_lock_and_the_tag(graph_id):
  enqueue = AsyncMock(return_value={"operation_id": "op_x"})

  result = await _ingest(graph_id, staged=True, enqueue=enqueue)

  assert result["operation_id"] == "op_x"
  params = enqueue.await_args.kwargs["params"]
  assert enqueue.await_args.kwargs["task_type"] == "dagster_job_monitor"
  assert params["job_name"] == "materialize_file_job"
  assert params["tags"] == {"materialize_db": graph_id}
  assert params["lock_key"] == f"graph_materialize:{graph_id}"
  # Handed to the worker, which frees it once the run stops.
  assert _lock_holder(graph_id) == params["lock_id"]


@pytest.mark.asyncio
async def test_a_failed_staging_frees_the_lock(graph_id):
  enqueue = AsyncMock(return_value={"operation_id": "op_x"})

  await _ingest(graph_id, staged=False, enqueue=enqueue)

  enqueue.assert_not_awaited()
  assert _lock_holder(graph_id) is None


@pytest.mark.asyncio
async def test_a_large_file_stages_and_writes_under_the_lock(graph_id):
  """Above the direct-staging threshold, staging and the graph write run as one
  Dagster job, and that job carries the lock too."""
  enqueue = AsyncMock(return_value={"operation_id": "op_x"})

  result = await _ingest(graph_id, staged=True, enqueue=enqueue, size=60 * MB)

  assert result["operation_id"] == "op_x"
  params = enqueue.await_args.kwargs["params"]
  assert params["job_name"] == "stage_file_job"
  assert params["tags"] == {"materialize_db": graph_id}
  assert _lock_holder(graph_id) == params["lock_id"]
