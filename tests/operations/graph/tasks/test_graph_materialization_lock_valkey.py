"""The direct materialize task keeps its lock while the copy may still run.

Against real Valkey. The Graph API's COPY is synchronous and ignores the
client going away, so a timed-out chunk (or a budget cancel) must not free
the lock that keeps a second copy out of the same database.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.graph_api.client.exceptions import GraphTimeoutError
from robosystems.operations.graph.tasks.graph_materialization import (
  GraphMaterializationTask,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def held_lock():
  client = create_redis_client(ValkeyDatabase.LOCKS)
  lock_key = f"graph_materialize:kgtest{uuid.uuid4().hex[:10]}"
  lock_id = uuid.uuid4().hex
  client.set(f"lock:{lock_key}", lock_id, ex=300)
  yield client, lock_key, lock_id
  client.delete(f"lock:{lock_key}")
  client.close()


async def _run(lock_key, lock_id, outcome):
  task = GraphMaterializationTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={"lock_key": lock_key, "lock_id": lock_id},
    manager=MagicMock(),
  )

  def db_gen():
    yield MagicMock()

  materialize = (
    AsyncMock(side_effect=outcome)
    if isinstance(outcome, Exception)
    else AsyncMock(return_value=outcome)
  )
  with (
    patch("robosystems.database.get_db_session", side_effect=lambda: db_gen()),
    patch(
      "robosystems.operations.graph.engine.direct_materialization.materialize_graph_directly",
      materialize,
    ),
  ):
    return await task.execute()


@pytest.mark.asyncio
async def test_a_timed_out_chunk_keeps_the_lock(held_lock):
  """Through the real materialize_graph_directly, which turns every error into
  a result: the timeout must still reach the task as "may still be copying"."""
  from robosystems.operations.graph.engine.direct_materialization import (
    materialize_graph_directly,
  )

  client, lock_key, lock_id = held_lock
  task = GraphMaterializationTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={"lock_key": lock_key, "lock_id": lock_id},
    manager=MagicMock(),
  )

  def db_gen():
    yield MagicMock()

  with (
    patch("robosystems.database.get_db_session", side_effect=lambda: db_gen()),
    patch(
      "robosystems.operations.graph.engine.direct_materialization.materialize_graph_directly",
      materialize_graph_directly,
    ),
    patch(
      "robosystems.models.core.Graph.get_by_id",
      side_effect=GraphTimeoutError("chunk timed out"),
    ),
    patch(
      "robosystems.middleware.sse.operation_manager.get_operation_manager",
      return_value=MagicMock(fail_operation=AsyncMock(), emit_progress=AsyncMock()),
    ),
  ):
    result = await task.execute()

  assert result["copy_may_still_run"] is True
  assert client.get(f"lock:{lock_key}") == lock_id


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [{"status": "success"}, ValueError("no schema")])
async def test_a_finished_or_refused_copy_releases_the_lock(held_lock, outcome):
  client, lock_key, lock_id = held_lock
  try:
    await _run(lock_key, lock_id, outcome)
  except ValueError:
    pass
  assert client.get(f"lock:{lock_key}") is None
