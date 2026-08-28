"""The worker adapter carries the request's history and context and returns
the response envelope the operator endpoint and the SSE completion event hand
to callers."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.operators.adapters import worker as adapter
from robosystems.operations.operators.base import (
  OperatorCapability,
  OperatorResult,
  OperatorSpec,
)

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"


def _operator(result: OperatorResult) -> MagicMock:
  operator = MagicMock()
  operator.spec = OperatorSpec(
    name="Cypher Operator",
    description="test",
    capabilities=[OperatorCapability.CUSTOM],
    read_only=True,
  )
  operator.run = AsyncMock(return_value=result)
  return operator


def _tracked_ai() -> MagicMock:
  tracked = MagicMock()
  tracked.total_credits = 12.5
  tracked.total_tokens = {"input": 4, "output": 10, "cache_read": 100, "cache_write": 0}
  tracked.call_count = 2
  return tracked


def _patched_adapter(stack: ExitStack, tracked: MagicMock, tools: MagicMock) -> None:
  for name in (
    "enforce_operator_write_role",
    "enforce_operator_graph_scope",
    "enforce_operator_credits",
    "get_ai_client",
    "FactoryCreditConsumer",
  ):
    stack.enter_context(patch.object(adapter, name))
  stack.enter_context(patch.object(adapter, "SessionFactory", return_value=MagicMock()))
  stack.enter_context(patch.object(adapter, "DirectToolAccess", return_value=tools))
  stack.enter_context(patch.object(adapter, "TrackedAIClient", return_value=tracked))


@pytest.mark.asyncio
async def test_run_operator_worker_returns_the_response_envelope():
  operator = _operator(
    OperatorResult(
      content="42",
      metadata={"cypher": "MATCH (n) RETURN n", "rows": [{"n": 1}]},
      tools_called=["read-graph-cypher"],
      confidence_score=0.9,
    )
  )
  tracked = _tracked_ai()
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, tracked, tools)
    result = await adapter.run_operator_worker(
      operator=operator,
      task_id="op_01TEST",
      graph_id=GRAPH_ID,
      user_id=USER_ID,
      params={
        "operator_type": "cypher",
        "query": "How many nodes?",
        "mode": "standard",
        "history": [{"role": "user", "content": "hi"}],
        "context": {"max_credits": 50},
      },
      manager=AsyncMock(),
    )

  # The envelope the endpoint and the SSE completion event hand to callers
  assert result["content"] == "42"
  assert result["operator_used"] == "Cypher Operator"
  assert result["mode_used"] == "standard"
  assert result["tokens_used"] == tracked.total_tokens
  assert result["confidence_score"] == 0.9
  assert result["execution_time"] >= 0
  assert result["tools_called"] == ["read-graph-cypher"]
  assert result["metadata"]["cypher"] == "MATCH (n) RETURN n"
  assert result["metadata"]["credits_consumed"] == 12.5
  assert result["metadata"]["call_count"] == 2
  assert result["metadata"]["has_credit_tracking"] is True
  # The operator's own keys still merge flat — the mapping consumers read them
  assert result["cypher"] == "MATCH (n) RETURN n"
  assert result["total_credits_consumed"] == 12.5
  tools.close.assert_awaited_once()

  # History and context reached the operator context
  ctx = operator.run.call_args[0][0]
  assert ctx.query == "How many nodes?"
  assert ctx.history == [{"role": "user", "content": "hi"}]
  assert ctx.extra["max_credits"] == 50
  assert ctx.extra["operator_type"] == "cypher"


@pytest.mark.asyncio
async def test_run_operator_worker_tolerates_missing_history_and_context():
  """The mapping enqueue sends neither; a malformed context is ignored."""
  operator = _operator(OperatorResult(content="ok"))
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    result = await adapter.run_operator_worker(
      operator=operator,
      task_id="op_01TEST",
      graph_id=GRAPH_ID,
      user_id=USER_ID,
      params={"operator_type": "mapping", "mapping_id": "map_1", "context": "junk"},
      manager=AsyncMock(),
    )

  ctx = operator.run.call_args[0][0]
  assert ctx.history == []
  assert ctx.extra["mapping_id"] == "map_1"
  assert ctx.mode.value == "standard"
  assert result["mode_used"] == "standard"


@pytest.mark.asyncio
async def test_worker_task_without_operator_type_fails_rather_than_completing():
  """A returned error dict would be stored as a COMPLETED result; the consumer
  only records FAILED on an exception."""
  from robosystems.operations.operators.adapters.worker_task import OperatorWorkerTask

  task = OperatorWorkerTask("op_1", GRAPH_ID, USER_ID, {"query": "?"}, AsyncMock())

  with pytest.raises(ValueError, match="operator_type"):
    await task.execute()
