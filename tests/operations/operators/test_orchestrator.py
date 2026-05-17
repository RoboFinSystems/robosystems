"""Test suite for agent orchestrator and routing logic.

Tests dynamic agent selection, routing, and multi-agent coordination.
Uses the new unified Operator protocol.
"""

import asyncio
from typing import Any
from unittest.mock import Mock, patch

import pytest

from robosystems.models.core import User
from robosystems.operations.operators.base import (
  Operator,
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.orchestrator import (
  OperatorOrchestrator,
  OperatorSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)


class TestRoutingStrategy:
  def test_strategy_values(self):
    assert RoutingStrategy.BEST_MATCH.value == "best_match"
    assert RoutingStrategy.ROUND_ROBIN.value == "round_robin"
    assert RoutingStrategy.CAPABILITY_BASED.value == "capability_based"
    assert RoutingStrategy.LOAD_BALANCED.value == "load_balanced"
    assert RoutingStrategy.ENSEMBLE.value == "ensemble"


class TestOperatorSelectionCriteria:
  def test_criteria_creation(self):
    criteria = OperatorSelectionCriteria(
      min_confidence=0.7,
      required_capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
      preferred_mode=OperatorMode.STANDARD,
      max_response_time=30.0,
    )
    assert criteria.min_confidence == 0.7
    assert OperatorCapability.FINANCIAL_ANALYSIS in criteria.required_capabilities
    assert criteria.preferred_mode == OperatorMode.STANDARD

  def test_criteria_defaults(self):
    criteria = OperatorSelectionCriteria()
    assert criteria.min_confidence == 0.3
    assert criteria.required_capabilities == []
    assert criteria.preferred_mode is None
    assert criteria.max_response_time == 60.0


class TestOrchestratorConfig:
  def test_config_creation(self):
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.CAPABILITY_BASED,
      enable_rag=True,
      enable_caching=True,
      enable_fallback=True,
      fallback_operator="financial",
      max_retries=3,
      timeout=45.0,
    )
    assert config.routing_strategy == RoutingStrategy.CAPABILITY_BASED
    assert config.enable_rag is True
    assert config.fallback_operator == "financial"

  def test_config_defaults(self):
    config = OrchestratorConfig()
    assert config.routing_strategy == RoutingStrategy.BEST_MATCH
    assert config.enable_rag is False
    assert config.enable_caching is False
    assert config.enable_fallback is True
    assert config.fallback_operator == "cypher"


# ── Mock agents for testing ──────────────────────────────────────────────────


class FinancialAgent(Operator):
  spec = OperatorSpec(
    name="financial",
    description="Mock financial agent",
    capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
  )

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    return OperatorResult(
      content=f"financial response: {ctx.query}",
      metadata={"agent": "financial"},
    )

  def can_handle(self, query: str, context: dict | None = None) -> float:
    return 0.8


class ResearchAgent(Operator):
  spec = OperatorSpec(
    name="research",
    description="Mock research agent",
    capabilities=[OperatorCapability.DEEP_RESEARCH],
  )

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    return OperatorResult(
      content=f"research response: {ctx.query}",
      metadata={"agent": "research"},
    )

  def can_handle(self, query: str, context: dict | None = None) -> float:
    return 0.6


class RagAgent(Operator):
  spec = OperatorSpec(
    name="rag",
    description="Mock RAG agent",
    capabilities=[OperatorCapability.RAG_SEARCH],
    requires_credits=False,
  )

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    return OperatorResult(
      content=f"rag response: {ctx.query}",
      metadata={"agent": "rag"},
    )

  def can_handle(self, query: str, context: dict | None = None) -> float:
    return 0.5


# Maps for mock registry
_MOCK_AGENTS = {
  "financial": FinancialAgent,
  "research": ResearchAgent,
  "rag": RagAgent,
}


def _mock_get_agent(operator_type: str) -> Operator:
  cls = _MOCK_AGENTS.get(operator_type)
  if cls is None:
    raise KeyError(f"Operator '{operator_type}' not registered")
  return cls()


def _mock_list_agents() -> dict[str, dict[str, Any]]:
  return {
    t: {
      "name": c.spec.name,
      "description": c.spec.description,
      "capabilities": [cap.value for cap in c.spec.capabilities],
      "supported_modes": [m.value for m in c.spec.supported_modes],
      "requires_credits": c.spec.requires_credits,
      "version": c.spec.version,
    }
    for t, c in _MOCK_AGENTS.items()
  }


# ── Orchestrator tests ───────────────────────────────────────────────────────


class TestOperatorOrchestrator:
  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  @pytest.fixture(autouse=True)
  def mock_agent_registry(self):
    """Patch agent registry functions for all tests."""
    with (
      patch(
        "robosystems.operations.operators.orchestrator.get_operator",
        side_effect=_mock_get_agent,
      ),
      patch(
        "robosystems.operations.operators.orchestrator.list_operators",
        side_effect=_mock_list_agents,
      ),
    ):
      yield

  @pytest.fixture(autouse=True)
  def mock_run_operator_api(self):
    """Patch run_operator_api to run agent directly without real tools/credits."""

    async def _fake_run_operator_api(operator, graph_id, user, query, mode, **kwargs):
      from robosystems.operations.operators.operator_context import OperatorContext
      from robosystems.operations.operators.progress import NoOpProgress

      ctx = OperatorContext(
        graph_id=graph_id,
        user_id=str(user.id),
        query=query,
        mode=mode,
        history=kwargs.get("history") or [],
        extra=kwargs.get("context") or {},
        progress=NoOpProgress(),
      )
      return await operator.run(ctx)

    with patch(
      "robosystems.operations.operators.orchestrator.run_operator_api",
      side_effect=_fake_run_operator_api,
    ):
      yield

  @pytest.fixture
  def orchestrator(self, mock_user):
    config = OrchestratorConfig(fallback_operator="rag")
    return OperatorOrchestrator("test_graph", mock_user, config=config)

  def test_orchestrator_initialization(self, mock_user):
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.CAPABILITY_BASED,
      enable_rag=False,
    )
    orchestrator = OperatorOrchestrator("test_graph", mock_user, config=config)
    assert orchestrator.graph_id == "test_graph"
    assert orchestrator.user == mock_user
    assert orchestrator.config.routing_strategy == RoutingStrategy.CAPABILITY_BASED

  @pytest.mark.asyncio
  async def test_route_query_explicit_agent(self, orchestrator):
    response = await orchestrator.route_query(
      query="Test query", operator_type="financial", mode=OperatorMode.STANDARD
    )
    assert response.operator_name == "financial"
    assert response.content == "financial response: Test query"
    assert response.metadata["routing_strategy"] == "explicit"

  @pytest.mark.asyncio
  async def test_route_query_best_match(self, orchestrator):
    response = await orchestrator.route_query(
      query="Financial analysis needed", mode=OperatorMode.STANDARD
    )
    assert response.operator_name == "financial"
    assert response.metadata["routing_strategy"] == "best_match"
    assert "confidence_scores" in response.metadata

  @pytest.mark.asyncio
  async def test_route_query_with_fallback(self, orchestrator):
    """Test fallback when no agent meets confidence threshold."""
    criteria = OperatorSelectionCriteria(min_confidence=0.99)
    response = await orchestrator.route_query(
      query="Unknown query type",
      mode=OperatorMode.STANDARD,
      selection_criteria=criteria,
    )
    assert response.operator_name == "rag"
    assert response.metadata["used_fallback"] is True

  @pytest.mark.asyncio
  async def test_route_query_capability_based(self, orchestrator):
    orchestrator.config.routing_strategy = RoutingStrategy.CAPABILITY_BASED
    criteria = OperatorSelectionCriteria(
      required_capabilities=[OperatorCapability.DEEP_RESEARCH]
    )
    response = await orchestrator.route_query(
      query="Research this topic",
      mode=OperatorMode.EXTENDED,
      selection_criteria=criteria,
    )
    assert response.operator_name == "research"
    assert response.metadata["routing_strategy"] == "capability_based"

  @pytest.mark.asyncio
  async def test_route_query_with_history(self, orchestrator):
    history = [
      {"role": "user", "content": "Previous question"},
      {"role": "assistant", "content": "Previous answer"},
    ]
    response = await orchestrator.route_query(
      query="Follow-up question", history=history, mode=OperatorMode.STANDARD
    )
    assert response is not None
    assert response.metadata.get("has_history") is True

  @pytest.mark.asyncio
  async def test_route_query_error_handling(self, orchestrator):
    """Test error handling when agent raises an exception."""
    with patch(
      "robosystems.operations.operators.orchestrator.run_operator_api",
      side_effect=Exception("Operator failed"),
    ):
      response = await orchestrator.route_query(
        query="Test query", operator_type="financial", mode=OperatorMode.STANDARD
      )
      assert response.error_details is not None
      assert "Operator failed" in response.error_details["message"]

  @pytest.mark.asyncio
  async def test_route_query_timeout(self, orchestrator):
    orchestrator.config.timeout = 0.1

    async def slow_run(*args, **kwargs):
      await asyncio.sleep(1)
      return OperatorResult(content="Too late")

    with patch(
      "robosystems.operations.operators.orchestrator.run_operator_api",
      side_effect=slow_run,
    ):
      response = await orchestrator.route_query(
        query="Test query", operator_type="financial", mode=OperatorMode.STANDARD
      )
      assert response.error_details is not None
      assert "timeout" in response.error_details["message"].lower()

  @pytest.mark.asyncio
  async def test_ensemble_routing(self, orchestrator):
    orchestrator.config.routing_strategy = RoutingStrategy.ENSEMBLE
    response = await orchestrator.route_query(
      query="Complex analysis needed", mode=OperatorMode.EXTENDED, ensemble_size=2
    )
    assert response.metadata["routing_strategy"] == "ensemble"
    assert "ensemble_agents" in response.metadata
    assert len(response.metadata["ensemble_agents"]) >= 2

  @pytest.mark.asyncio
  async def test_multi_agent_coordination(self, orchestrator):
    response = await orchestrator.coordinate_agents(
      query="Complex multi-part question",
      agent_sequence=["rag", "financial", "research"],
      mode=OperatorMode.EXTENDED,
    )
    assert response.metadata["coordination_type"] == "sequential"
    assert len(response.metadata["agent_sequence"]) == 3
    assert response.content is not None

  @pytest.mark.asyncio
  async def test_parallel_agent_execution(self, orchestrator):
    response = await orchestrator.coordinate_agents(
      query="Analyze from multiple perspectives",
      agent_sequence=["financial", "research"],
      mode=OperatorMode.STANDARD,
      coordination_type="parallel",
    )
    assert response.metadata["coordination_type"] == "parallel"
    assert "execution_time" in response.metadata

  def test_get_agent_recommendations(self, orchestrator):
    recommendations = orchestrator.get_operator_recommendations(
      query="Financial analysis of SEC filings"
    )
    assert len(recommendations) > 0
    assert recommendations[0]["operator_type"] == "financial"
    assert 0.0 <= recommendations[0]["confidence"] <= 1.0

  @pytest.mark.asyncio
  async def test_route_with_streaming(self, orchestrator):
    response = await orchestrator.route_query(
      query="Stream this response", mode=OperatorMode.STREAMING
    )
    assert response.mode_used == OperatorMode.STREAMING

  @pytest.mark.asyncio
  async def test_caching_enabled(self, orchestrator):
    orchestrator.config.enable_caching = True
    orchestrator._cache = {}

    response1 = await orchestrator.route_query(
      query="Cached query", mode=OperatorMode.QUICK
    )
    response2 = await orchestrator.route_query(
      query="Cached query", mode=OperatorMode.QUICK
    )
    assert response1.content == response2.content
    assert response2.metadata.get("from_cache") is True

  @pytest.mark.asyncio
  async def test_load_balanced_routing(self, orchestrator):
    orchestrator.config.routing_strategy = RoutingStrategy.LOAD_BALANCED
    responses = []
    for _ in range(3):
      response = await orchestrator.route_query(
        query="Test query", mode=OperatorMode.QUICK
      )
      responses.append(response.operator_name)
    assert len(set(responses)) > 1

  @pytest.mark.asyncio
  async def test_agent_metrics_collection(self, orchestrator):
    await orchestrator.route_query(
      query="Test query", operator_type="financial", mode=OperatorMode.STANDARD
    )
    metrics = orchestrator.get_metrics()
    assert "financial" in metrics["agent_usage"]
    assert metrics["agent_usage"]["financial"]["calls"] >= 1
    assert metrics["total_queries"] >= 1

  @pytest.mark.asyncio
  async def test_credit_check_insufficient(self, mock_user):
    mock_db = Mock()
    orchestrator = OperatorOrchestrator("test-graph", mock_user, mock_db)

    with patch(
      "robosystems.operations.graph.credit_service.CreditService"
    ) as mock_credit_service:
      mock_instance = Mock()
      mock_credit_service.return_value = mock_instance
      mock_instance.check_credit_balance.return_value = {
        "has_sufficient_credits": False,
        "estimated_credits": 10.0,
        "available_credits": 5.0,
      }

      agent = FinancialAgent()
      response = await orchestrator._execute_agent(
        agent, "test query", OperatorMode.STANDARD, None, {}
      )

      assert "Insufficient credits" in response.content
      assert response.error_details["code"] == "INSUFFICIENT_CREDITS"

  @pytest.mark.asyncio
  async def test_credit_check_passes(self, mock_user):
    mock_db = Mock()
    orchestrator = OperatorOrchestrator("test-graph", mock_user, mock_db)

    with patch(
      "robosystems.operations.graph.credit_service.CreditService"
    ) as mock_credit_service:
      mock_instance = Mock()
      mock_credit_service.return_value = mock_instance
      mock_instance.check_credit_balance.return_value = {
        "has_sufficient_credits": True,
        "estimated_credits": 10.0,
        "available_credits": 100.0,
      }

      agent = FinancialAgent()
      response = await orchestrator._execute_agent(
        agent, "test query", OperatorMode.STANDARD, None, {}
      )

      assert "financial response" in response.content

  @pytest.mark.asyncio
  async def test_credit_check_skipped_for_non_credit_agents(self, mock_user):
    orchestrator = OperatorOrchestrator("test-graph", mock_user, None)
    agent = RagAgent()
    response = await orchestrator._execute_agent(
      agent, "test query", OperatorMode.QUICK, None, {}
    )
    assert "rag response" in response.content
