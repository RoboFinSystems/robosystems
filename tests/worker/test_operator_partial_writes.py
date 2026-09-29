"""An author run that stops after a write still reports it.

Through the real worker consumer and the real Valkey operation store; only
the model, the tool surface and the credit meter are stubbed. A model error,
the task budget and a cancel each end the run after `create-agent` landed,
and the stored operation must carry that write.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.middleware.sse.operation_manager import OperationManager
from robosystems.operations.operators.ai_client import AIResponse

pytestmark = pytest.mark.integration

WORKER = "robosystems.operations.operators.adapters.worker"


@pytest.fixture(autouse=True)
def fresh_event_storage(monkeypatch):
  # The shared store caches a Valkey client bound to the first test's loop.
  monkeypatch.setattr("robosystems.middleware.sse.event_storage._event_storage", None)


def _write_turn() -> AIResponse:
  return AIResponse(
    content="",
    model="us.anthropic.claude-opus-5-5",
    input_tokens=1,
    output_tokens=1,
    stop_reason="tool_use",
    content_blocks=[
      {
        "toolUse": {
          "toolUseId": "t0",
          "name": "create-agent",
          "input": {"name": "Notion Labs", "agent_type": "vendor"},
        }
      }
    ],
  )


def _tools() -> MagicMock:
  tools = MagicMock()
  tools.get_tool_schemas = AsyncMock(
    return_value=[{"name": "create-agent", "description": "", "inputSchema": {}}]
  )
  tools.call_tool = AsyncMock(
    return_value={"id": "agt_partial1", "name": "Notion Labs"}
  )
  tools.close = AsyncMock()
  return tools


async def _run(second_call, *, timeout: float | None = None):
  import robosystems.operations.operators.adapters.worker_task  # noqa: F401  (register)
  from robosystems.worker import consumer

  manager = OperationManager()
  graph_id = "kg" + uuid.uuid4().hex[:18]
  op_id = await manager.event_storage.create_operation(
    operation_type="operator", user_id="usr_test", graph_id=graph_id
  )
  calls = {"n": 0}

  async def create_message(**_kwargs):
    calls["n"] += 1
    if calls["n"] == 1:
      return _write_turn()
    return await second_call(manager, op_id)

  ai = MagicMock()
  ai.create_message = AsyncMock(side_effect=create_message)
  consumer_stub = MagicMock()
  consumer_stub.consume = AsyncMock(return_value=1.0)
  task = {
    "task_id": op_id,
    "task_type": "operator",
    "graph_id": graph_id,
    "user_id": "usr_test",
    "params": {"operator_type": "author", "query": "add Notion Labs as a vendor"},
  }
  protection = MagicMock(protect=AsyncMock(), unprotect=AsyncMock())
  with ExitStack() as stack:
    stack.enter_context(patch(f"{WORKER}.enforce_operator_write_role"))
    stack.enter_context(patch(f"{WORKER}.enforce_operator_graph_scope"))
    stack.enter_context(patch(f"{WORKER}.enforce_operator_credits"))
    stack.enter_context(patch(f"{WORKER}.SessionFactory"))
    stack.enter_context(patch(f"{WORKER}.HttpToolAccess", return_value=_tools()))
    stack.enter_context(patch(f"{WORKER}.get_ai_client", return_value=ai))
    stack.enter_context(
      patch(f"{WORKER}.FactoryCreditConsumer", return_value=consumer_stub)
    )
    if timeout is not None:
      stack.enter_context(patch.dict(consumer.TASK_TIMEOUTS, {"operator": timeout}))
    await consumer._process_task(
      task, json.dumps(task), AsyncMock(), "inflight", manager, "w1", protection
    )
  metadata = await manager.event_storage.get_operation_metadata(op_id)
  return metadata


def _writes(payload) -> list[str]:
  return [w.get("id") for w in (payload or {}).get("writes") or []]


@pytest.mark.asyncio
async def test_a_model_error_after_a_write_reports_it():
  async def fail(_manager, _op_id):
    raise RuntimeError("ThrottlingException")

  metadata = await _run(fail)

  assert metadata.status == OperationStatus.FAILED
  assert _writes(metadata.error_details) == ["agt_partial1"]


@pytest.mark.asyncio
async def test_the_task_budget_after_a_write_reports_it():
  async def hang(_manager, _op_id):
    await asyncio.sleep(30)

  metadata = await _run(hang, timeout=1)

  assert metadata.status == OperationStatus.FAILED
  assert _writes(metadata.error_details) == ["agt_partial1"]


@pytest.mark.asyncio
async def test_a_cancel_after_a_write_keeps_its_receipt():
  async def cancel_then_answer(manager, op_id):
    await manager.cancel_operation(op_id)
    return AIResponse(
      content="done",
      model="us.anthropic.claude-opus-5-5",
      input_tokens=1,
      output_tokens=1,
      stop_reason="end_turn",
      content_blocks=[{"text": "done"}],
    )

  # The user cancels while the second call is in flight; the run's result
  # arrives after the cancel landed.
  metadata = await _run(cancel_then_answer)

  assert metadata.status == OperationStatus.CANCELLED
  assert _writes(metadata.result_data) == ["agt_partial1"]
