"""Tests for the unified agent system (Operator, OperatorContext, TrackedAIClient, adapters)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.operations.operators.ai_client import AIMessage, AIResponse
from robosystems.operations.operators.base import (
  Operator,
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.credit_consumer import NoOpCreditConsumer
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import (
  clear_registry,
  get_operator,
  list_operators,
  register_operator,
)
from robosystems.operations.operators.progress import NoOpProgress
from robosystems.operations.operators.tracked_ai import TrackedAIClient

# ── Test Operator implementation ────────────────────────────────────────────────


class EchoAgent(Operator):
  """Simple agent that echoes the query for testing."""

  spec = OperatorSpec(
    name="Echo Operator",
    description="Echoes the query back",
    capabilities=[OperatorCapability.CUSTOM],
    requires_credits=True,
  )

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    response = await ctx.ai.create_message(
      messages=[AIMessage(role="user", content=ctx.query)],
      system="Echo back the input.",
      max_tokens=100,
      operation_description="Echo test",
    )
    return OperatorResult(
      content=response.content,
      metadata={"echoed": True},
      tools_called=[],
    )

  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    return 0.8


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_ai_client():
  """Mock AIClient that returns predictable responses."""
  client = MagicMock()
  client.create_message = AsyncMock(
    return_value=AIResponse(
      content="Test response",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=100,
      output_tokens=50,
      stop_reason="end_turn",
    )
  )
  return client


@pytest.fixture
def tracked_ai(mock_ai_client):
  """TrackedAIClient with NoOp credit consumer."""
  return TrackedAIClient(
    ai_client=mock_ai_client,
    graph_id="kg_test",
    user_id="user_test",
    credit_consumer=NoOpCreditConsumer(),
  )


@pytest.fixture
def operator_context(tracked_ai):
  """Minimal OperatorContext for testing."""
  return OperatorContext(
    graph_id="kg_test",
    user_id="user_test",
    query="test query",
    mode=OperatorMode.STANDARD,
    ai=tracked_ai,
    tools=None,
    progress=NoOpProgress(),
  )


# ── OperatorSpec tests ──────────────────────────────────────────────────────────


class TestOperatorSpec:
  def test_spec_readable_without_instantiation(self):
    assert EchoAgent.spec.name == "Echo Operator"
    assert EchoAgent.spec.requires_credits is True

  def test_spec_defaults(self):
    spec = OperatorSpec(
      name="Test",
      description="Test agent",
      capabilities=[],
    )
    assert spec.version == "1.0.0"
    assert spec.requires_credits is True
    assert OperatorMode.STANDARD in spec.supported_modes


# ── TrackedAIClient tests ────────────────────────────────────────────────────


class TestTrackedAIClient:
  @pytest.mark.asyncio
  async def test_tracks_tokens(self, tracked_ai, mock_ai_client):
    response = await tracked_ai.create_message(
      messages=[AIMessage(role="user", content="hello")],
      operation_description="test",
    )
    assert response.content == "Test response"
    assert tracked_ai.total_tokens == {
      "input": 100,
      "output": 50,
      "cache_read": 0,
      "cache_write": 0,
    }
    assert tracked_ai.call_count == 1

  @pytest.mark.asyncio
  async def test_accumulates_across_calls(self, tracked_ai, mock_ai_client):
    await tracked_ai.create_message(
      messages=[AIMessage(role="user", content="first")],
      operation_description="test1",
    )
    await tracked_ai.create_message(
      messages=[AIMessage(role="user", content="second")],
      operation_description="test2",
    )
    assert tracked_ai.total_tokens == {
      "input": 200,
      "output": 100,
      "cache_read": 0,
      "cache_write": 0,
    }
    assert tracked_ai.call_count == 2

  @pytest.mark.asyncio
  async def test_credit_consumption_failure_does_not_break(self, mock_ai_client):
    """Credit failure is logged but doesn't prevent AI calls."""

    class FailingConsumer:
      async def consume(self, **kwargs) -> float:
        raise RuntimeError("DB down")

    tracked = TrackedAIClient(
      ai_client=mock_ai_client,
      graph_id="kg_test",
      user_id="user_test",
      credit_consumer=FailingConsumer(),
    )
    response = await tracked.create_message(
      messages=[AIMessage(role="user", content="hello")],
      operation_description="test",
    )
    assert response.content == "Test response"
    assert tracked.total_credits == 0.0
    assert tracked.total_tokens == {
      "input": 100,
      "output": 50,
      "cache_read": 0,
      "cache_write": 0,
    }

  @pytest.mark.asyncio
  async def test_with_real_credit_consumer(self, mock_ai_client):
    """SessionCreditConsumer is called with correct args."""
    consumer = MagicMock()
    consumer.consume = AsyncMock(return_value=5.0)

    tracked = TrackedAIClient(
      ai_client=mock_ai_client,
      graph_id="kg_test",
      user_id="user_test",
      credit_consumer=consumer,
    )
    await tracked.create_message(
      messages=[AIMessage(role="user", content="hello")],
      operation_description="my op",
    )

    consumer.consume.assert_called_once_with(
      graph_id="kg_test",
      user_id="user_test",
      input_tokens=100,
      output_tokens=50,
      model="us.anthropic.claude-sonnet-4-6",
      operation_description="my op",
      cache_read_input_tokens=0,
      cache_creation_input_tokens=0,
    )
    assert tracked.total_credits == 5.0

  @pytest.mark.asyncio
  async def test_credit_summary(self, tracked_ai):
    await tracked_ai.create_message(
      messages=[AIMessage(role="user", content="hello")],
      operation_description="test",
    )
    summary = tracked_ai.credit_summary
    assert summary["call_count"] == 1
    assert summary["total_tokens"] == {
      "input": 100,
      "output": 50,
      "cache_read": 0,
      "cache_write": 0,
    }


# ── Operator.run() tests ────────────────────────────────────────────────────────


class TestOperatorRun:
  @pytest.mark.asyncio
  async def test_echo_agent(self, operator_context):
    agent = EchoAgent()
    result = await agent.run(operator_context)

    assert result.content == "Test response"
    assert result.metadata["echoed"] is True
    assert operator_context.ai.total_tokens["input"] == 100

  @pytest.mark.asyncio
  async def test_can_handle(self):
    agent = EchoAgent()
    assert agent.can_handle("anything") == 0.8

  @pytest.mark.asyncio
  async def test_default_can_handle(self):
    """Operator base class returns 0.5 by default."""

    class MinimalAgent(Operator):
      spec = OperatorSpec(name="Min", description="Minimal", capabilities=[])

      async def run(self, ctx):
        return OperatorResult(content="ok")

    agent = MinimalAgent()
    assert agent.can_handle("test") == 0.5


# ── Operator Registry tests ─────────────────────────────────────────────────────


class TestOperatorRegistry:
  def setup_method(self):
    clear_registry()

  def teardown_method(self):
    clear_registry()

  def test_register_and_get(self):
    @register_operator("test_echo")
    class TestEcho(Operator):
      spec = OperatorSpec(name="Test", description="Test", capabilities=[])

      async def run(self, ctx):
        return OperatorResult(content="ok")

    agent = get_operator("test_echo")
    assert isinstance(agent, TestEcho)

  def test_get_unknown_raises(self):
    with pytest.raises(KeyError, match="not registered"):
      get_operator("nonexistent")

  def test_former_name_resolves_to_the_canonical_operator(self):
    """`cypher` is the name the analyst shipped under; stored operator_type
    values (queued tasks, saved operations, client code) keep resolving, and
    the listing shows canonical names only."""
    from robosystems.operations.operators.operator_registry import (
      is_registered,
      resolve_operator_type,
    )

    @register_operator("analyst")
    class Analyst(Operator):
      spec = OperatorSpec(name="Analyst", description="a", capabilities=[])

      async def run(self, ctx):
        return OperatorResult(content="a")

    assert resolve_operator_type("cypher") == "analyst"
    assert isinstance(get_operator("cypher"), Analyst)
    assert is_registered("cypher")
    assert "cypher" not in list_operators()
    assert resolve_operator_type("nonexistent") == "nonexistent"

  def test_direct_registration_beats_the_alias(self):
    from robosystems.operations.operators.operator_registry import (
      resolve_operator_type,
    )

    @register_operator("cypher")
    class Legacy(Operator):
      spec = OperatorSpec(name="Legacy", description="l", capabilities=[])

      async def run(self, ctx):
        return OperatorResult(content="l")

    assert resolve_operator_type("cypher") == "cypher"
    assert isinstance(get_operator("cypher"), Legacy)

  def test_list_agents(self):
    @register_operator("test_a")
    class A(Operator):
      spec = OperatorSpec(
        name="A", description="Operator A", capabilities=[OperatorCapability.CUSTOM]
      )

      async def run(self, ctx):
        return OperatorResult(content="a")

    agents = list_operators()
    assert "test_a" in agents
    assert agents["test_a"]["name"] == "A"
    assert agents["test_a"]["requires_credits"] is True


# ── Progress tests ───────────────────────────────────────────────────────────


class TestProgress:
  @pytest.mark.asyncio
  async def test_noop_progress(self):
    progress = NoOpProgress()
    await progress.report("test", percent=50)
    assert not await progress.is_cancelled()

  @pytest.mark.asyncio
  async def test_callback_progress(self):
    from robosystems.operations.operators.progress import CallbackProgress

    calls = []

    def callback(msg, pct, detail):
      return calls.append((msg, pct))

    progress = CallbackProgress(callback)
    await progress.report("step 1", percent=25)
    assert len(calls) == 1
    assert calls[0] == ("step 1", 25)
    assert not await progress.is_cancelled()


# ── Tool Access tests ────────────────────────────────────────────────────────


class TestToolAccess:
  def test_direct_tool_access_graph_id(self):
    from robosystems.operations.operators.tool_access import DirectToolAccess

    tools = DirectToolAccess("kg_test")
    assert tools.graph_id == "kg_test"

  def test_direct_tool_access_get_tool_instance(self):
    from robosystems.operations.operators.tool_access import DirectToolAccess

    tools = DirectToolAccess("kg_test")

    class FakeTool:
      def __init__(self, client):
        self.graph_id = client.graph_id

    tool = tools.get_tool_instance(FakeTool)
    assert tool.graph_id == "kg_test"

    # Same instance returned on second call
    tool2 = tools.get_tool_instance(FakeTool)
    assert tool is tool2

  @pytest.mark.asyncio
  async def test_direct_tool_access_call_unknown_raises(self):
    from robosystems.operations.operators.tool_access import DirectToolAccess

    tools = DirectToolAccess("kg_test")
    with pytest.raises(ValueError, match="not registered"):
      await tools.call_tool("unknown-tool", {})


# ── Credit Consumer tests ────────────────────────────────────────────────────


class TestCreditConsumer:
  @pytest.mark.asyncio
  async def test_noop_consumer(self):
    consumer = NoOpCreditConsumer()
    result = await consumer.consume(
      graph_id="kg",
      user_id="u",
      input_tokens=100,
      output_tokens=50,
      model="m",
      operation_description="test",
    )
    assert result == 0.0
