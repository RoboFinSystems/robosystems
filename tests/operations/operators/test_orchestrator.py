"""Tests for operator selection: config, and confidence-ranked recommendations."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from robosystems.models.core import User
from robosystems.operations.operators.base import (
  Operator,
  OperatorCapability,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.orchestrator import (
  OperatorOrchestrator,
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


class TestOrchestratorConfig:
  def test_config_creation(self):
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.CAPABILITY_BASED,
      enable_rag=True,
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
    assert config.enable_fallback is True
    assert config.fallback_operator == "analyst"


# ── Mock agents for testing ──────────────────────────────────────────────────


class FinancialOperator(Operator):
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


class ResearchOperator(Operator):
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


class RagOperator(Operator):
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
  "financial": FinancialOperator,
  "research": ResearchOperator,
  "rag": RagOperator,
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

  def test_get_agent_recommendations(self, orchestrator):
    recommendations = orchestrator.get_operator_recommendations(
      query="Financial analysis of SEC filings"
    )
    assert len(recommendations) > 0
    assert recommendations[0]["operator_type"] == "financial"
    assert 0.0 <= recommendations[0]["confidence"] <= 1.0
