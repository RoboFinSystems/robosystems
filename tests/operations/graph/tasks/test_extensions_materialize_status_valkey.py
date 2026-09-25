"""A failed extensions materialization is recorded as a failed operation.

Through the real worker consumer and real Valkey operation storage: the
SDK's materialize() reads this status, so COMPLETED here meant success=True
for a graph that was not rebuilt.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.middleware.sse.operation_manager import OperationManager

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_a_failed_materialization_fails_the_operation():
  import robosystems.operations.graph.tasks.extensions_materialize  # noqa: F401  (register)
  from robosystems.worker.consumer import _process_task

  manager = OperationManager()
  graph_id = "kg" + uuid.uuid4().hex[:18]
  op_id = await manager.event_storage.create_operation(
    operation_type="extensions_materialize", user_id="usr_test", graph_id=graph_id
  )
  task = {
    "task_id": op_id,
    "task_type": "extensions_materialize",
    "graph_id": graph_id,
    "user_id": "usr_test",
    "params": {"rebuild": True},
  }
  materializer = MagicMock()
  materializer.materialize = AsyncMock(
    return_value=SimpleNamespace(
      status="error", errors=["materialization lock held"], duration_ms=1.0
    )
  )
  protection = MagicMock(protect=AsyncMock(), unprotect=AsyncMock())
  with patch(
    "robosystems.operations.extensions.materialize.ExtensionsMaterializer",
    return_value=materializer,
  ):
    await _process_task(
      task, json.dumps(task), AsyncMock(), "inflight", manager, "w1", protection
    )

  assert await manager.get_operation_status(op_id) == OperationStatus.FAILED
