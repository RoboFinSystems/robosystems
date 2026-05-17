"""Full base agent tests — dataclasses and utility methods."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.operations.operators.base import (
  BaseOperator,
  ExecutionProfile,
  OperatorCapability,
  OperatorMetadata,
  OperatorMode,
  OperatorResponse,
)


class TestOperatorCapability:
  def test_values(self):
    assert OperatorCapability.FINANCIAL_ANALYSIS.value == "financial_analysis"
    assert OperatorCapability.DEEP_RESEARCH.value == "deep_research"
    assert OperatorCapability.RAG_SEARCH.value == "rag_search"
    assert OperatorCapability.ENTITY_ANALYSIS.value == "entity_analysis"


class TestOperatorMode:
  def test_values(self):
    assert OperatorMode.QUICK.value == "quick"
    assert OperatorMode.STANDARD.value == "standard"
    assert OperatorMode.EXTENDED.value == "extended"
    assert OperatorMode.STREAMING.value == "streaming"


class TestExecutionProfile:
  def test_defaults(self):
    profile = ExecutionProfile(min_time=1, max_time=5, avg_time=3)
    assert profile.tool_calls == 0

  def test_with_tool_calls(self):
    profile = ExecutionProfile(min_time=5, max_time=15, avg_time=10, tool_calls=5)
    assert profile.tool_calls == 5


class TestOperatorMetadata:
  def test_defaults(self):
    meta = OperatorMetadata(
      name="test",
      description="A test agent",
      capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
    )
    assert meta.version == "1.0.0"
    assert len(meta.supported_modes) == 3
    assert meta.requires_credits is True
    assert meta.tags == []

  def test_execution_profiles(self):
    meta = OperatorMetadata(
      name="test",
      description="test",
      capabilities=[],
    )
    assert OperatorMode.QUICK in meta.execution_profile
    assert OperatorMode.STANDARD in meta.execution_profile
    assert OperatorMode.EXTENDED in meta.execution_profile


class TestOperatorResponse:
  def test_basic(self):
    resp = OperatorResponse(
      content="Analysis complete",
      operator_name="financial",
      mode_used=OperatorMode.STANDARD,
    )
    assert resp.content == "Analysis complete"
    assert resp.tools_called == []
    assert resp.requires_followup is False
    assert resp.confidence_score is None
    assert isinstance(resp.timestamp, datetime)

  def test_with_metadata(self):
    resp = OperatorResponse(
      content="result",
      operator_name="test",
      mode_used=OperatorMode.QUICK,
      tokens_used={"input": 1000, "output": 500},
      tools_called=["cypher_query", "get_schema"],
      confidence_score=0.95,
    )
    assert resp.tokens_used["input"] == 1000
    assert len(resp.tools_called) == 2
    assert resp.confidence_score == 0.95


# Concrete implementation for testing
class _TestOperator(BaseOperator):
  @property
  def metadata(self) -> OperatorMetadata:
    return OperatorMetadata(
      name="test_agent",
      description="Test agent",
      capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
      supported_modes=[OperatorMode.QUICK, OperatorMode.STANDARD],
    )

  async def analyze(self, query, mode=OperatorMode.STANDARD, **kwargs):
    return OperatorResponse(content="test", operator_name="test_agent", mode_used=mode)

  def can_handle(self, query, context=None):
    return 0.5


class TestBaseAgent:
  @pytest.fixture
  def agent(self):
    user = MagicMock()
    user.id = "user-123"
    return _TestOperator(graph_id="kg0123456789abcdef", user=user)

  def test_init(self, agent):
    assert agent.graph_id == "kg0123456789abcdef"
    assert agent.total_tokens_used == {"input": 0, "output": 0}
    assert agent.graph_client is None

  def test_supports_mode(self, agent):
    assert agent.supports_mode(OperatorMode.QUICK) is True
    assert agent.supports_mode(OperatorMode.STANDARD) is True
    assert agent.supports_mode(OperatorMode.EXTENDED) is False

  def test_has_capability(self, agent):
    assert agent.has_capability(OperatorCapability.FINANCIAL_ANALYSIS) is True
    assert agent.has_capability(OperatorCapability.RAG_SEARCH) is False

  def test_track_tokens(self, agent):
    agent.track_tokens(1000, 500)
    assert agent.total_tokens_used["input"] == 1000
    assert agent.total_tokens_used["output"] == 500
    agent.track_tokens(200, 100)
    assert agent.total_tokens_used["input"] == 1200
    assert agent.total_tokens_used["output"] == 600

  def test_validate_mode_supported(self, agent):
    agent.validate_mode(OperatorMode.QUICK)  # Should not raise

  def test_validate_mode_unsupported(self, agent):
    with pytest.raises(ValueError, match="does not support"):
      agent.validate_mode(OperatorMode.EXTENDED)

  def test_get_mode_limits(self, agent):
    limits = agent.get_mode_limits(OperatorMode.STANDARD)
    assert "max_tools" in limits
    assert "timeout" in limits

  @pytest.mark.asyncio
  async def test_prepare_context(self, agent):
    ctx = await agent.prepare_context("test query")
    assert ctx["graph_id"] == "kg0123456789abcdef"
    assert ctx["user_id"] == "user-123"
    assert ctx["operator_name"] == "test_agent"

  @pytest.mark.asyncio
  async def test_prepare_context_with_existing(self, agent):
    ctx = await agent.prepare_context("query", context={"custom": "value"})
    assert ctx["custom"] == "value"
    assert ctx["graph_id"] == "kg0123456789abcdef"

  def test_repr(self, agent):
    r = repr(agent)
    assert "test_agent" in r
    assert "kg0123456789abcdef" in r

  @pytest.mark.asyncio
  async def test_close_no_client(self, agent):
    await agent.close()  # Should not raise

  @pytest.mark.asyncio
  async def test_close_with_client(self, agent):
    agent.graph_client = AsyncMock()
    await agent.close()
    agent.graph_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_consume_credits_no_session(self, agent):
    result = await agent.consume_credits(1000, 500)
    assert result is None

  @pytest.mark.asyncio
  async def test_can_handle(self, agent):
    score = agent.can_handle("analyze revenue")
    assert score == 0.5
