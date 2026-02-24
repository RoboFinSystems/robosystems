"""Full base agent tests — dataclasses and utility methods."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.operations.agents.base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  BaseAgent,
  ExecutionProfile,
)


class TestAgentCapability:
  def test_values(self):
    assert AgentCapability.FINANCIAL_ANALYSIS.value == "financial_analysis"
    assert AgentCapability.DEEP_RESEARCH.value == "deep_research"
    assert AgentCapability.RAG_SEARCH.value == "rag_search"
    assert AgentCapability.ENTITY_ANALYSIS.value == "entity_analysis"


class TestAgentMode:
  def test_values(self):
    assert AgentMode.QUICK.value == "quick"
    assert AgentMode.STANDARD.value == "standard"
    assert AgentMode.EXTENDED.value == "extended"
    assert AgentMode.STREAMING.value == "streaming"


class TestExecutionProfile:
  def test_defaults(self):
    profile = ExecutionProfile(min_time=1, max_time=5, avg_time=3)
    assert profile.tool_calls == 0

  def test_with_tool_calls(self):
    profile = ExecutionProfile(min_time=5, max_time=15, avg_time=10, tool_calls=5)
    assert profile.tool_calls == 5


class TestAgentMetadata:
  def test_defaults(self):
    meta = AgentMetadata(
      name="test",
      description="A test agent",
      capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
    )
    assert meta.version == "1.0.0"
    assert len(meta.supported_modes) == 3
    assert meta.requires_credits is True
    assert meta.tags == []

  def test_execution_profiles(self):
    meta = AgentMetadata(
      name="test",
      description="test",
      capabilities=[],
    )
    assert AgentMode.QUICK in meta.execution_profile
    assert AgentMode.STANDARD in meta.execution_profile
    assert AgentMode.EXTENDED in meta.execution_profile


class TestAgentResponse:
  def test_basic(self):
    resp = AgentResponse(
      content="Analysis complete",
      agent_name="financial",
      mode_used=AgentMode.STANDARD,
    )
    assert resp.content == "Analysis complete"
    assert resp.tools_called == []
    assert resp.requires_followup is False
    assert resp.confidence_score is None
    assert isinstance(resp.timestamp, datetime)

  def test_with_metadata(self):
    resp = AgentResponse(
      content="result",
      agent_name="test",
      mode_used=AgentMode.QUICK,
      tokens_used={"input": 1000, "output": 500},
      tools_called=["cypher_query", "get_schema"],
      confidence_score=0.95,
    )
    assert resp.tokens_used["input"] == 1000
    assert len(resp.tools_called) == 2
    assert resp.confidence_score == 0.95


# Concrete implementation for testing
class _TestAgent(BaseAgent):
  @property
  def metadata(self) -> AgentMetadata:
    return AgentMetadata(
      name="test_agent",
      description="Test agent",
      capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
      supported_modes=[AgentMode.QUICK, AgentMode.STANDARD],
    )

  async def analyze(self, query, mode=AgentMode.STANDARD, **kwargs):
    return AgentResponse(content="test", agent_name="test_agent", mode_used=mode)

  def can_handle(self, query, context=None):
    return 0.5


class TestBaseAgent:
  @pytest.fixture
  def agent(self):
    user = MagicMock()
    user.id = "user-123"
    return _TestAgent(graph_id="kg0123456789abcdef", user=user)

  def test_init(self, agent):
    assert agent.graph_id == "kg0123456789abcdef"
    assert agent.total_tokens_used == {"input": 0, "output": 0}
    assert agent.graph_client is None

  def test_supports_mode(self, agent):
    assert agent.supports_mode(AgentMode.QUICK) is True
    assert agent.supports_mode(AgentMode.STANDARD) is True
    assert agent.supports_mode(AgentMode.EXTENDED) is False

  def test_has_capability(self, agent):
    assert agent.has_capability(AgentCapability.FINANCIAL_ANALYSIS) is True
    assert agent.has_capability(AgentCapability.RAG_SEARCH) is False

  def test_track_tokens(self, agent):
    agent.track_tokens(1000, 500)
    assert agent.total_tokens_used["input"] == 1000
    assert agent.total_tokens_used["output"] == 500
    agent.track_tokens(200, 100)
    assert agent.total_tokens_used["input"] == 1200
    assert agent.total_tokens_used["output"] == 600

  def test_validate_mode_supported(self, agent):
    agent.validate_mode(AgentMode.QUICK)  # Should not raise

  def test_validate_mode_unsupported(self, agent):
    with pytest.raises(ValueError, match="does not support"):
      agent.validate_mode(AgentMode.EXTENDED)

  def test_get_mode_limits(self, agent):
    limits = agent.get_mode_limits(AgentMode.STANDARD)
    assert "max_tools" in limits
    assert "timeout" in limits

  @pytest.mark.asyncio
  async def test_prepare_context(self, agent):
    ctx = await agent.prepare_context("test query")
    assert ctx["graph_id"] == "kg0123456789abcdef"
    assert ctx["user_id"] == "user-123"
    assert ctx["agent_name"] == "test_agent"

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
