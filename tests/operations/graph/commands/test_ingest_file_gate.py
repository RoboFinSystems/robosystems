"""Tests for the ingest-file storage gate.

The gate must measure the instance's physical footprint (not the logical
staging sum), refuse when the graph cannot be resolved, and surface an
unmeasurable instance as retryable rather than allowed.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
from robosystems.operations.graph.commands.ingest_file import ingest_file_cmd

GB = 1024**3
MB = 1024**2


def _storage_check(
  *,
  allowed: bool = True,
  retryable: bool = False,
  total_storage_gb: float | None = 1.0,
  limit_gb: float = 20.0,
  enforced_storage_gb: float | None = None,
) -> dict:
  return {
    "allowed": allowed,
    "retryable": retryable,
    "errors": [] if allowed else ["Instance storage exceeds limit"],
    "total_storage_gb": total_storage_gb,
    # Enforced == total unless a test says otherwise (no transient artifacts).
    "enforced_storage_gb": (
      enforced_storage_gb if enforced_storage_gb is not None else total_storage_gb
    ),
    "limit_gb": limit_gb,
    "usage_percentage": None,
    "status": "healthy" if allowed else "over_limit",
    "databases": [],
    "items": [],
  }


def _gate_mocks(file_size_bytes: int, graph, storage_check: dict | None):
  """Patch everything between the route and the storage gate."""
  graph_file = MagicMock()
  graph_file.graph_id = "kg123"
  graph_file.s3_key = "uploads/kg123/file.csv"

  s3 = MagicMock()
  s3.s3_client.head_object.return_value = {"ContentLength": file_size_bytes}

  return (
    patch(
      "robosystems.models.core.GraphFile.get_by_id",
      return_value=graph_file,
    ),
    patch(
      "robosystems.operations.graph.commands.ingest_file.S3Client",
      return_value=s3,
    ),
    patch(
      "robosystems.models.core.Graph.get_by_id",
      return_value=graph,
    ),
    patch.object(
      IngestionLimitChecker,
      "check_instance_storage",
      new_callable=AsyncMock,
      return_value=storage_check,
    ),
  )


async def _run() -> dict:
  return await ingest_file_cmd(
    graph_id="kg123",
    file_id="file_1",
    ingest_to_graph=False,
    current_user=MagicMock(),
    db=MagicMock(),
    background_tasks=MagicMock(),
  )


@pytest.mark.asyncio
async def test_unresolved_graph_refuses_instead_of_skipping():
  """A Graph lookup miss must refuse, not silently bypass the cap."""
  files, s3, graph, storage = _gate_mocks(10 * MB, graph=None, storage_check=None)
  with files, s3, graph, storage as mock_storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 404
  mock_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmeasurable_instance_is_retryable_not_allowed():
  """Graph API unavailable -> 503 with Retry-After, never a pass."""
  mock_graph = MagicMock()
  mock_graph.parent_graph_id = None
  mock_graph.graph_tier = "ladybug-standard"

  files, s3, graph, storage = _gate_mocks(
    10 * MB,
    graph=mock_graph,
    storage_check=_storage_check(allowed=False, retryable=True, total_storage_gb=None),
  )
  with files, s3, graph, storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 503
  assert exc.value.headers["Retry-After"] == "30"


@pytest.mark.asyncio
async def test_over_cap_instance_blocks_upload():
  mock_graph = MagicMock()
  mock_graph.parent_graph_id = None
  mock_graph.graph_tier = "ladybug-standard"

  files, s3, graph, storage = _gate_mocks(
    10 * MB,
    graph=mock_graph,
    storage_check=_storage_check(allowed=False, total_storage_gb=21.0),
  )
  with files, s3, graph, storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 413


@pytest.mark.asyncio
async def test_upload_without_headroom_blocks():
  """Under cap now, but the incoming file would cross it."""
  mock_graph = MagicMock()
  mock_graph.parent_graph_id = None
  mock_graph.graph_tier = "ladybug-standard"

  files, s3, graph, storage = _gate_mocks(
    50 * MB,
    graph=mock_graph,
    storage_check=_storage_check(total_storage_gb=19.99, limit_gb=20.0),
  )
  with files, s3, graph, storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 413


@pytest.mark.asyncio
async def test_headroom_reads_enforced_not_total():
  """An in-flight blue-green rebuild must not reject uploads.

  Total includes the `-wip` copy and sits at the cap; the durable (enforced)
  figure has room. Headroom is judged on the latter, same as the cap itself.
  """
  mock_graph = MagicMock()
  mock_graph.parent_graph_id = None
  mock_graph.graph_tier = "ladybug-standard"

  files, s3, graph, storage = _gate_mocks(
    50 * MB,
    graph=mock_graph,
    storage_check=_storage_check(
      total_storage_gb=19.99, enforced_storage_gb=10.0, limit_gb=20.0
    ),
  )
  with files, s3, graph, storage:
    result = await _run()

  assert result is not None


@pytest.mark.asyncio
async def test_subgraph_upload_measures_parent_scope():
  """A subgraph's upload counts against its parent's whole instance."""
  mock_graph = MagicMock()
  mock_graph.parent_graph_id = "kg_parent"
  mock_graph.graph_tier = "ladybug-standard"

  files, s3, graph, storage = _gate_mocks(
    10 * MB,
    graph=mock_graph,
    storage_check=_storage_check(allowed=False, total_storage_gb=21.0),
  )
  with files, s3, graph, storage as mock_storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 413
  assert mock_storage.await_args.args[1] == "kg_parent"
