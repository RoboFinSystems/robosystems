"""Unit tests for agent API models."""

from datetime import UTC

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.operator import (
  BatchOperatorRequest,
  BatchOperatorResponse,
  OperatorHealthResponse,
  OperatorHealthStatus,
  OperatorListResponse,
  OperatorMessage,
  OperatorMetadataResponse,
  OperatorMode,
  OperatorRecommendation,
  OperatorRecommendationRequest,
  OperatorRecommendationResponse,
  OperatorRequest,
  OperatorResponse,
  SelectionCriteria,
)


@pytest.mark.unit
class TestOperatorMode:
  def test_enum_values(self):
    assert OperatorMode.QUICK == "quick"
    assert OperatorMode.STANDARD == "standard"
    assert OperatorMode.EXTENDED == "extended"
    assert OperatorMode.STREAMING == "streaming"


@pytest.mark.unit
class TestOperatorMessage:
  def test_valid_message(self):
    model = OperatorMessage(role="user", content="What is Apple's revenue?")
    assert model.role == "user"
    assert model.content == "What is Apple's revenue?"
    assert model.timestamp is None

  def test_message_with_timestamp(self):
    from datetime import datetime

    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = OperatorMessage(role="assistant", content="Answer", timestamp=ts)
    assert model.timestamp == ts


@pytest.mark.unit
class TestSelectionCriteria:
  def test_defaults(self):
    model = SelectionCriteria()
    assert model.min_confidence == 0.3
    assert model.required_capabilities == []
    assert model.preferred_mode is None
    assert model.max_response_time == 60.0
    assert model.excluded_operators == []

  def test_custom_criteria(self):
    model = SelectionCriteria(
      min_confidence=0.7,
      required_capabilities=["financial_analysis"],
      preferred_mode=OperatorMode.EXTENDED,
      max_response_time=45.0,
      excluded_operators=["basic"],
    )
    assert model.min_confidence == 0.7
    assert model.preferred_mode == OperatorMode.EXTENDED


@pytest.mark.unit
class TestOperatorRequest:
  def test_minimal_request(self):
    model = OperatorRequest(message="What is Apple's revenue?")
    assert model.message == "What is Apple's revenue?"
    assert model.history == []
    assert model.context is None
    assert model.mode == OperatorMode.STANDARD
    assert model.enable_rag is True
    assert model.stream is False
    assert model.force_extended_analysis is False

  def test_with_history(self):
    history = [
      OperatorMessage(role="user", content="Previous question"),
      OperatorMessage(role="assistant", content="Previous answer"),
    ]
    model = OperatorRequest(
      message="Follow up",
      history=history,
    )
    assert len(model.history) == 2

  def test_with_context(self):
    model = OperatorRequest(
      message="Analyze",
      context={"industry": "tech", "focus": "revenue"},
    )
    assert model.context["industry"] == "tech"

  def test_with_selection_criteria(self):
    criteria = SelectionCriteria(
      min_confidence=0.8,
      required_capabilities=["financial_analysis"],
    )
    model = OperatorRequest(
      message="Complex analysis",
      selection_criteria=criteria,
    )
    assert model.selection_criteria.min_confidence == 0.8

  def test_message_required(self):
    with pytest.raises(ValidationError):
      OperatorRequest()  # type: ignore[call-arg]

  def test_force_extended_analysis(self):
    model = OperatorRequest(
      message="Deep dive",
      force_extended_analysis=True,
      mode=OperatorMode.EXTENDED,
    )
    assert model.force_extended_analysis is True

  def test_streaming_mode(self):
    model = OperatorRequest(
      message="Stream this",
      stream=True,
    )
    assert model.stream is True


@pytest.mark.unit
class TestBatchAgentRequest:
  def test_valid_batch(self):
    queries = [
      OperatorRequest(message="Query 1"),
      OperatorRequest(message="Query 2"),
    ]
    model = BatchOperatorRequest(queries=queries)
    assert len(model.queries) == 2
    assert model.parallel is False

  def test_parallel_batch(self):
    model = BatchOperatorRequest(
      queries=[OperatorRequest(message="Q1")],
      parallel=True,
    )
    assert model.parallel is True

  def test_queries_required(self):
    with pytest.raises(ValidationError):
      BatchOperatorRequest()  # type: ignore[call-arg]


@pytest.mark.unit
class TestOperatorResponse:
  def test_minimal_response(self):
    model = OperatorResponse(
      content="Apple's revenue was $89.5B",
      operator_used="financial",
      mode_used=OperatorMode.STANDARD,
    )
    assert model.content == "Apple's revenue was $89.5B"
    assert model.metadata is None
    assert model.tokens_used is None
    assert model.confidence_score is None
    assert model.is_partial is False
    assert model.timestamp is not None

  def test_full_response(self):
    model = OperatorResponse(
      content="Analysis complete",
      operator_used="research",
      mode_used=OperatorMode.EXTENDED,
      metadata={"routing_info": {"reason": "Market research query"}},
      tokens_used={"prompt_tokens": 450, "completion_tokens": 320, "total_tokens": 770},
      confidence_score=0.92,
      operation_id="op_123",
      execution_time=3.45,
    )
    assert model.confidence_score == 0.92
    assert model.tokens_used["total_tokens"] == 770

  def test_error_response(self):
    model = OperatorResponse(
      content="",
      operator_used="financial",
      mode_used=OperatorMode.STANDARD,
      error_details={
        "code": "insufficient_credits",
        "message": "Not enough credits",
      },
    )
    assert model.error_details["code"] == "insufficient_credits"


@pytest.mark.unit
class TestBatchAgentResponse:
  def test_valid_response(self):
    result = OperatorResponse(
      content="Answer",
      operator_used="financial",
      mode_used=OperatorMode.STANDARD,
    )
    model = BatchOperatorResponse(
      results=[result],
      total_execution_time=3.5,
      parallel_processed=False,
    )
    assert len(model.results) == 1
    assert model.parallel_processed is False


@pytest.mark.unit
class TestOperatorListResponse:
  def test_valid_response(self):
    model = OperatorListResponse(
      operators={
        "financial": {"name": "Financial Operator", "version": "1.0"},
        "research": {"name": "Research Operator", "version": "1.0"},
      },
      total=2,
    )
    assert model.total == 2


@pytest.mark.unit
class TestOperatorMetadataResponse:
  def test_valid_response(self):
    model = OperatorMetadataResponse(
      name="Financial Operator",
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
class TestOperatorRecommendationRequest:
  def test_valid_request(self):
    model = OperatorRecommendationRequest(query="Revenue analysis for Apple")
    assert model.context is None

  def test_with_context(self):
    model = OperatorRecommendationRequest(
      query="Revenue analysis",
      context={"ticker": "AAPL"},
    )
    assert model.context["ticker"] == "AAPL"


@pytest.mark.unit
class TestOperatorRecommendation:
  def test_valid_recommendation(self):
    model = OperatorRecommendation(
      operator_type="financial",
      operator_name="Financial Operator",
      confidence=0.92,
      capabilities=["financial_analysis"],
    )
    assert model.confidence == 0.92
    assert model.reason is None


@pytest.mark.unit
class TestOperatorRecommendationResponse:
  def test_valid_response(self):
    rec = OperatorRecommendation(
      operator_type="financial",
      operator_name="Financial Operator",
      confidence=0.92,
      capabilities=["financial_analysis"],
    )
    model = OperatorRecommendationResponse(
      recommendations=[rec],
      query="Revenue analysis",
    )
    assert len(model.recommendations) == 1


@pytest.mark.unit
class TestOperatorHealthStatus:
  def test_healthy_status(self):
    model = OperatorHealthStatus(
      operator_type="financial",
      status="healthy",
      name="Financial Operator",
      version="1.0.0",
    )
    assert model.error is None

  def test_unhealthy_status(self):
    model = OperatorHealthStatus(
      operator_type="financial",
      status="unhealthy",
      error="Connection timeout",
    )
    assert model.error == "Connection timeout"


@pytest.mark.unit
class TestOperatorHealthResponse:
  def test_valid_response(self):
    agent_status = OperatorHealthStatus(
      operator_type="financial",
      status="healthy",
    )
    model = OperatorHealthResponse(
      status="healthy",
      operators={"financial": agent_status},
    )
    assert model.status == "healthy"
    assert model.timestamp is not None
