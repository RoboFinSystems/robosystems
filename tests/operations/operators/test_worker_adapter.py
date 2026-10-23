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
from robosystems.security.operation_audit import AuditCaller

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"


def _operator(result: OperatorResult) -> MagicMock:
  operator = MagicMock()
  operator.spec = OperatorSpec(
    name="Analyst Operator",
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
  tracked.transcript = [{"call": 1, "input": [], "output": [{"text": "42"}]}]
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
  stack.enter_context(patch.object(adapter, "HttpToolAccess", return_value=tools))
  stack.enter_context(patch.object(adapter, "TrackedAIClient", return_value=tracked))
  stack.enter_context(patch.object(adapter, "record_operator_run"))


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
        "operator_type": "analyst",
        "query": "How many nodes?",
        "mode": "standard",
        "history": [{"role": "user", "content": "hi"}],
        "context": {"max_credits": 50},
      },
      manager=AsyncMock(),
    )

  # The envelope the endpoint and the SSE completion event hand to callers
  assert result["content"] == "42"
  assert result["operator_used"] == "Analyst Operator"
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
  assert ctx.extra["operator_type"] == "analyst"


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


@pytest.mark.asyncio
async def test_worker_uses_the_full_tool_surface_gated_by_read_only():
  """The model-driven loop needs the tools a graph actually exposes.

  DirectToolAccess only reports tool classes registered by hand, so a loop
  operator on it saw no tools at all in prod (it narrated a tool call as
  text). The worker builds HttpToolAccess with the operator's read_only flag,
  exactly as the API path did.
  """
  operator = _operator(OperatorResult(content="ok"))
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    http_access = stack.enter_context(
      patch.object(adapter, "HttpToolAccess", return_value=tools)
    )
    await adapter.run_operator_worker(
      operator=operator,
      task_id="op_01TEST",
      graph_id=GRAPH_ID,
      user_id=USER_ID,
      params={"operator_type": "analyst", "query": "q"},
      manager=AsyncMock(),
    )

  # The run is named on the tool access so the mutation audit attributes
  # each write to it.
  http_access.assert_called_once_with(
    GRAPH_ID,
    read_only=True,
    user_id=USER_ID,
    caller=AuditCaller(operator_type="analyst", operation_id="op_01TEST"),
  )
  ctx = operator.run.call_args[0][0]
  assert ctx.tools is tools


def _recorded(stack_records: MagicMock):
  stack_records.assert_called_once()
  return stack_records.call_args[0][0]


@pytest.mark.asyncio
async def test_a_completed_run_is_recorded_with_its_transcript():
  operator = _operator(
    OperatorResult(
      content="42",
      metadata={"cypher": "MATCH (n) RETURN n", "rows": [{"n": 1}], "writes": []},
      tools_called=["read-graph-cypher"],
      confidence_score=0.9,
    )
  )
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    recorder = stack.enter_context(patch.object(adapter, "record_operator_run"))
    await adapter.run_operator_worker(
      operator=operator,
      task_id="op_01TEST",
      graph_id=GRAPH_ID,
      user_id=USER_ID,
      params={
        "operator_type": "analyst",
        "query": "How many nodes?",
        "mode": "standard",
        "thread_id": "thr_1",
        "history": [{"role": "user", "content": "hi"}],
      },
      manager=AsyncMock(),
    )

  record = _recorded(recorder)
  assert record.operation_id == "op_01TEST"
  assert record.thread_id == "thr_1"
  assert (record.graph_id, record.user_id) == (GRAPH_ID, USER_ID)
  assert (record.operator_type, record.mode, record.status) == (
    "analyst",
    "standard",
    "completed",
  )
  assert record.question == "How many nodes?"
  assert record.history == [{"role": "user", "content": "hi"}]
  assert record.transcript == [{"call": 1, "input": [], "output": [{"text": "42"}]}]
  assert record.answer == "42"
  assert record.error is None
  # Rows stay out; the Cypher, cost and tools stay in.
  assert "rows" not in record.result
  assert record.result["cypher"] == "MATCH (n) RETURN n"
  assert record.result["credits_consumed"] == 12.5
  assert record.result["tools_called"] == ["read-graph-cypher"]
  assert record.result["confidence_score"] == 0.9
  assert record.finished_at >= record.started_at
  assert record.duration_ms >= 0


@pytest.mark.asyncio
async def test_a_cancelled_run_is_recorded_as_cancelled():
  operator = _operator(
    OperatorResult(content="Cancelled before an answer.", metadata={"cancelled": True})
  )
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    recorder = stack.enter_context(patch.object(adapter, "record_operator_run"))
    await adapter.run_operator_worker(
      operator=operator,
      task_id="op_01TEST",
      graph_id=GRAPH_ID,
      user_id=USER_ID,
      params={"operator_type": "analyst", "query": "q"},
      manager=AsyncMock(),
    )

  assert _recorded(recorder).status == "cancelled"


@pytest.mark.asyncio
async def test_a_failed_run_is_recorded_and_the_failure_still_raises():
  operator = _operator(OperatorResult(content=""))
  operator.run = AsyncMock(side_effect=RuntimeError("bedrock refused"))
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    recorder = stack.enter_context(patch.object(adapter, "record_operator_run"))
    with pytest.raises(RuntimeError, match="bedrock refused"):
      await adapter.run_operator_worker(
        operator=operator,
        task_id="op_01TEST",
        graph_id=GRAPH_ID,
        user_id=USER_ID,
        params={"operator_type": "analyst", "query": "q"},
        manager=AsyncMock(),
      )

  record = _recorded(recorder)
  assert record.status == "failed"
  assert record.error == "bedrock refused"
  assert record.answer is None
  # What the calls so far cost is still on the record.
  assert record.result["credits_consumed"] == 12.5
  assert record.result["call_count"] == 2
  tools.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_interrupted_run_is_recorded_before_the_cancel_propagates():
  import asyncio

  operator = _operator(OperatorResult(content=""))
  operator.run = AsyncMock(side_effect=asyncio.CancelledError())
  tools = MagicMock()
  tools.close = AsyncMock()

  with ExitStack() as stack:
    _patched_adapter(stack, _tracked_ai(), tools)
    recorder = stack.enter_context(patch.object(adapter, "record_operator_run"))
    with pytest.raises(asyncio.CancelledError):
      await adapter.run_operator_worker(
        operator=operator,
        task_id="op_01TEST",
        graph_id=GRAPH_ID,
        user_id=USER_ID,
        params={"operator_type": "analyst", "query": "q"},
        manager=AsyncMock(),
      )

  record = _recorded(recorder)
  assert record.status == "failed"
  assert "Interrupted" in record.error
