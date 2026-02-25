"""Unit tests for agent API models."""

from datetime import UTC

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.agent import (
  AgentHealthResponse,
  AgentHealthStatus,
  AgentListResponse,
  AgentMessage,
  AgentMetadataResponse,
  AgentMode,
  AgentRecommendation,
  AgentRecommendationRequest,
  AgentRecommendationResponse,
  AgentRequest,
  AgentResponse,
  BatchAgentRequest,
  BatchAgentResponse,
  SelectionCriteria,
)


@pytest.mark.unit
class TestAgentMode:
  def test_enum_values(self):
    assert AgentMode.QUICK == "quick"
    assert AgentMode.STANDARD == "standard"
    assert AgentMode.EXTENDED == "extended"
    assert AgentMode.STREAMING == "streaming"


@pytest.mark.unit
class TestAgentMessage:
  def test_valid_message(self):
    model = AgentMessage(role="user", content="What is Apple's revenue?")
    assert model.role == "user"
    assert model.content == "What is Apple's revenue?"
    assert model.timestamp is None

  def test_message_with_timestamp(self):
    from datetime import datetime

    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = AgentMessage(role="assistant", content="Answer", timestamp=ts)
    assert model.timestamp == ts


@pytest.mark.unit
class TestSelectionCriteria:
  def test_defaults(self):
    model = SelectionCriteria()
    assert model.min_confidence == 0.3
    assert model.required_capabilities == []
    assert model.preferred_mode is None
    assert model.max_response_time == 60.0
    assert model.excluded_agents == []

  def test_custom_criteria(self):
    model = SelectionCriteria(
      min_confidence=0.7,
      required_capabilities=["financial_analysis"],
      preferred_mode=AgentMode.EXTENDED,
      max_response_time=45.0,
      excluded_agents=["basic"],
    )
    assert model.min_confidence == 0.7
    assert model.preferred_mode == AgentMode.EXTENDED


@pytest.mark.unit
class TestAgentRequest:
  def test_minimal_request(self):
    model = AgentRequest(message="What is Apple's revenue?")
    assert model.message == "What is Apple's revenue?"
    assert model.history == []
    assert model.context is None
    assert model.mode == AgentMode.STANDARD
    assert model.enable_rag is True
    assert model.stream is False
    assert model.force_extended_analysis is False

  def test_with_history(self):
    history = [
      AgentMessage(role="user", content="Previous question"),
      AgentMessage(role="assistant", content="Previous answer"),
    ]
    model = AgentRequest(
      message="Follow up",
      history=history,
    )
    assert len(model.history) == 2

  def test_with_context(self):
    model = AgentRequest(
      message="Analyze",
      context={"industry": "tech", "focus": "revenue"},
    )
    assert model.context["industry"] == "tech"

  def test_with_selection_criteria(self):
    criteria = SelectionCriteria(
      min_confidence=0.8,
      required_capabilities=["financial_analysis"],
    )
    model = AgentRequest(
      message="Complex analysis",
      selection_criteria=criteria,
    )
    assert model.selection_criteria.min_confidence == 0.8

  def test_message_required(self):
    with pytest.raises(ValidationError):
      AgentRequest()  # type: ignore[call-arg]

  def test_force_extended_analysis(self):
    model = AgentRequest(
      message="Deep dive",
      force_extended_analysis=True,
      mode=AgentMode.EXTENDED,
    )
    assert model.force_extended_analysis is True

  def test_streaming_mode(self):
    model = AgentRequest(
      message="Stream this",
      stream=True,
    )
    assert model.stream is True


@pytest.mark.unit
class TestBatchAgentRequest:
  def test_valid_batch(self):
    queries = [
      AgentRequest(message="Query 1"),
      AgentRequest(message="Query 2"),
    ]
    model = BatchAgentRequest(queries=queries)
    assert len(model.queries) == 2
    assert model.parallel is False

  def test_parallel_batch(self):
    model = BatchAgentRequest(
      queries=[AgentRequest(message="Q1")],
      parallel=True,
    )
    assert model.parallel is True

  def test_queries_required(self):
    with pytest.raises(ValidationError):
      BatchAgentRequest()  # type: ignore[call-arg]


@pytest.mark.unit
class TestAgentResponse:
  def test_minimal_response(self):
    model = AgentResponse(
      content="Apple's revenue was $89.5B",
      agent_used="financial",
      mode_used=AgentMode.STANDARD,
    )
    assert model.content == "Apple's revenue was $89.5B"
    assert model.metadata is None
    assert model.tokens_used is None
    assert model.confidence_score is None
    assert model.is_partial is False
    assert model.timestamp is not None

  def test_full_response(self):
    model = AgentResponse(
      content="Analysis complete",
      agent_used="research",
      mode_used=AgentMode.EXTENDED,
      metadata={"routing_info": {"reason": "Market research query"}},
      tokens_used={"prompt_tokens": 450, "completion_tokens": 320, "total_tokens": 770},
      confidence_score=0.92,
      operation_id="op_123",
      execution_time=3.45,
    )
    assert model.confidence_score == 0.92
    assert model.tokens_used["total_tokens"] == 770

  def test_error_response(self):
    model = AgentResponse(
      content="",
      agent_used="financial",
      mode_used=AgentMode.STANDARD,
      error_details={
        "code": "insufficient_credits",
        "message": "Not enough credits",
      },
    )
    assert model.error_details["code"] == "insufficient_credits"


@pytest.mark.unit
class TestBatchAgentResponse:
  def test_valid_response(self):
    result = AgentResponse(
      content="Answer",
      agent_used="financial",
      mode_used=AgentMode.STANDARD,
    )
    model = BatchAgentResponse(
      results=[result],
      total_execution_time=3.5,
      parallel_processed=False,
    )
    assert len(model.results) == 1
    assert model.parallel_processed is False


@pytest.mark.unit
class TestAgentListResponse:
  def test_valid_response(self):
    model = AgentListResponse(
      agents={
        "financial": {"name": "Financial Agent", "version": "1.0"},
        "research": {"name": "Research Agent", "version": "1.0"},
      },
      total=2,
    )
    assert model.total == 2


@pytest.mark.unit
class TestAgentMetadataResponse:
  def test_valid_response(self):
    model = AgentMetadataResponse(
      name="Financial Agent",
      description="Analyzes financial data",
      version="1.0.0",
      capabilities=["financial_analysis", "ratio_calculation"],
      supported_modes=["standard", "extended"],
      requires_credits=True,
    )
    assert model.requires_credits is True
    assert model.author is None
    assert model.tags == []


@pytest.mark.unit
class TestAgentRecommendationRequest:
  def test_valid_request(self):
    model = AgentRecommendationRequest(query="Revenue analysis for Apple")
    assert model.context is None

  def test_with_context(self):
    model = AgentRecommendationRequest(
      query="Revenue analysis",
      context={"ticker": "AAPL"},
    )
    assert model.context["ticker"] == "AAPL"


@pytest.mark.unit
class TestAgentRecommendation:
  def test_valid_recommendation(self):
    model = AgentRecommendation(
      agent_type="financial",
      agent_name="Financial Agent",
      confidence=0.92,
      capabilities=["financial_analysis"],
    )
    assert model.confidence == 0.92
    assert model.reason is None


@pytest.mark.unit
class TestAgentRecommendationResponse:
  def test_valid_response(self):
    rec = AgentRecommendation(
      agent_type="financial",
      agent_name="Financial Agent",
      confidence=0.92,
      capabilities=["financial_analysis"],
    )
    model = AgentRecommendationResponse(
      recommendations=[rec],
      query="Revenue analysis",
    )
    assert len(model.recommendations) == 1


@pytest.mark.unit
class TestAgentHealthStatus:
  def test_healthy_status(self):
    model = AgentHealthStatus(
      agent_type="financial",
      status="healthy",
      name="Financial Agent",
      version="1.0.0",
    )
    assert model.error is None

  def test_unhealthy_status(self):
    model = AgentHealthStatus(
      agent_type="financial",
      status="unhealthy",
      error="Connection timeout",
    )
    assert model.error == "Connection timeout"


@pytest.mark.unit
class TestAgentHealthResponse:
  def test_valid_response(self):
    agent_status = AgentHealthStatus(
      agent_type="financial",
      status="healthy",
    )
    model = AgentHealthResponse(
      status="healthy",
      agents={"financial": agent_status},
    )
    assert model.status == "healthy"
    assert model.timestamp is not None
