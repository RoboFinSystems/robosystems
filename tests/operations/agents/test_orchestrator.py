"""
Test suite for agent orchestrator and routing logic.

Tests dynamic agent selection, routing, and multi-agent coordination.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, List, Optional, Any
import asyncio

from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  RoutingStrategy,
  AgentSelectionCriteria,
  OrchestratorConfig,
)
from robosystems.operations.agents.base import (
  BaseAgent,
  AgentCapability,
  AgentMode,
  AgentResponse,
  AgentMetadata,
)
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.models.iam import User


class TestRoutingStrategy:
  """Test routing strategy enumeration."""

  def test_strategy_values(self):
    """Test routing strategy values."""
    assert RoutingStrategy.BEST_MATCH.value == "best_match"
    assert RoutingStrategy.ROUND_ROBIN.value == "round_robin"
    assert RoutingStrategy.CAPABILITY_BASED.value == "capability_based"
    assert RoutingStrategy.LOAD_BALANCED.value == "load_balanced"
    assert RoutingStrategy.ENSEMBLE.value == "ensemble"


class TestAgentSelectionCriteria:
  """Test agent selection criteria."""

  def test_criteria_creation(self):
    """Test creating selection criteria."""
    criteria = AgentSelectionCriteria(
      min_confidence=0.7,
      required_capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
      preferred_mode=AgentMode.STANDARD,
      max_response_time=30.0,
    )

    assert criteria.min_confidence == 0.7
    assert AgentCapability.FINANCIAL_ANALYSIS in criteria.required_capabilities
    assert criteria.preferred_mode == AgentMode.STANDARD

  def test_criteria_defaults(self):
    """Test default selection criteria."""
    criteria = AgentSelectionCriteria()

    assert criteria.min_confidence == 0.3
    assert criteria.required_capabilities == []
    assert criteria.preferred_mode is None
    assert criteria.max_response_time == 60.0


class TestOrchestratorConfig:
  """Test orchestrator configuration."""

  def test_config_creation(self):
    """Test creating orchestrator config."""
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.CAPABILITY_BASED,
      enable_rag=True,
      enable_caching=True,
      enable_fallback=True,
      fallback_agent="financial",
      max_retries=3,
      timeout=45.0,
    )

    assert config.routing_strategy == RoutingStrategy.CAPABILITY_BASED
    assert config.enable_rag is True
    assert config.fallback_agent == "financial"

  def test_config_defaults(self):
    """Test default config values."""
    config = OrchestratorConfig()

    assert config.routing_strategy == RoutingStrategy.BEST_MATCH
    assert config.enable_rag is False
    assert config.enable_caching is False
    assert config.enable_fallback is True
    assert config.fallback_agent == "cypher"


class MockAgent(BaseAgent):
  """Mock agent for testing."""

  def __init__(
    self,
    name: str,
    capabilities: List[AgentCapability],
    confidence_score: float = 0.5,
  ):
    self.name = name
    self.capabilities = capabilities
    self.confidence_score = confidence_score
    self._metadata = AgentMetadata(
      name=name,
      description=f"Mock {name} agent",
      capabilities=capabilities,
    )

  @property
  def metadata(self) -> AgentMetadata:
    return self._metadata

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: Optional[List[Dict]] = None,
    context: Optional[Dict] = None,
    callback: Optional[Any] = None,
  ) -> AgentResponse:
    return AgentResponse(
      content=f"{self.name} response: {query}",
      metadata={"agent": self.name},
      agent_name=self.name,
      mode_used=mode,
      confidence_score=self.confidence_score,
      tokens_used={"input": 0, "output": 0},
    )

  def can_handle(self, query: str, context: Optional[Dict] = None) -> float:
    return self.confidence_score


class TestAgentOrchestrator:
  """Test the agent orchestrator."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_registry(self):
    """Create a mock agent registry."""
    with patch("robosystems.operations.agents.orchestrator.AgentRegistry") as mock:
      registry = Mock(spec=AgentRegistry)

      # Create mock agents
      financial_agent = MockAgent(
        "financial",
        [AgentCapability.FINANCIAL_ANALYSIS],
        confidence_score=0.8,
      )
      research_agent = MockAgent(
        "research", [AgentCapability.DEEP_RESEARCH], confidence_score=0.6
      )
      rag_agent = MockAgent("rag", [AgentCapability.RAG_SEARCH], confidence_score=0.5)

      registry.get_agent.side_effect = lambda agent_type, *args: {
        "financial": financial_agent,
        "research": research_agent,
        "rag": rag_agent,
      }.get(agent_type)

      registry.get_all_agents.return_value = {
        "financial": financial_agent,
        "research": research_agent,
        "rag": rag_agent,
      }

      registry.list_agents.return_value = {
        "financial": {"name": "Financial Agent"},
        "research": {"name": "Research Agent"},
        "rag": {"name": "RAG Agent"},
      }

      mock.return_value = registry
      yield registry

  @pytest.fixture
  def mock_context_enricher(self):
    """Create a mock context enricher."""
    with patch("robosystems.operations.agents.orchestrator.ContextEnricher") as mock:
      enricher = AsyncMock()
      enricher.enrich = AsyncMock(
        return_value={
          "original": "context",
          "enriched": True,
          "relevant_documents": ["doc1", "doc2"],
        }
      )
      mock.return_value = enricher
      yield enricher

  @pytest.fixture
  def orchestrator(self, mock_user, mock_registry, mock_context_enricher):
    """Create an orchestrator instance."""
    config = OrchestratorConfig(fallback_agent="rag", enable_rag=True)
    return AgentOrchestrator("test_graph", mock_user, config=config)

  def test_orchestrator_initialization(self, mock_user):
    """Test orchestrator initialization."""
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.CAPABILITY_BASED,
      enable_rag=False,
    )

    orchestrator = AgentOrchestrator("test_graph", mock_user, config=config)

    assert orchestrator.graph_id == "test_graph"
    assert orchestrator.user == mock_user
    assert orchestrator.config.routing_strategy == RoutingStrategy.CAPABILITY_BASED
    assert orchestrator.config.enable_rag is False

  @pytest.mark.asyncio
  async def test_route_query_explicit_agent(self, orchestrator):
    """Test routing to explicitly specified agent."""
    response = await orchestrator.route_query(
      query="Test query",
      agent_type="financial",
      mode=AgentMode.STANDARD,
    )

    assert response.agent_name == "financial"
    assert response.content == "financial response: Test query"
    assert response.metadata["routing_strategy"] == "explicit"

  @pytest.mark.asyncio
  async def test_route_query_best_match(self, orchestrator):
    """Test best match routing strategy."""
    response = await orchestrator.route_query(
      query="Financial analysis needed",
      mode=AgentMode.STANDARD,
    )

    # Financial agent has highest confidence (0.8)
    assert response.agent_name == "financial"
    assert response.metadata["routing_strategy"] == "best_match"
    assert "confidence_scores" in response.metadata

  @pytest.mark.asyncio
  async def test_route_query_with_fallback(self, orchestrator):
    """Test fallback when no agent meets confidence threshold."""
    # Set all agents to low confidence
    for agent in orchestrator.registry.get_all_agents().values():
      agent.confidence_score = 0.1

    response = await orchestrator.route_query(
      query="Unknown query type", mode=AgentMode.STANDARD
    )

    # Should fall back to configured fallback agent
    assert response.agent_name == "rag"
    assert response.metadata["used_fallback"] is True

  @pytest.mark.asyncio
  async def test_route_query_capability_based(self, orchestrator):
    """Test capability-based routing."""
    orchestrator.config.routing_strategy = RoutingStrategy.CAPABILITY_BASED

    criteria = AgentSelectionCriteria(
      required_capabilities=[AgentCapability.DEEP_RESEARCH]
    )

    response = await orchestrator.route_query(
      query="Research this topic",
      mode=AgentMode.EXTENDED,
      selection_criteria=criteria,
    )

    assert response.agent_name == "research"
    assert response.metadata["routing_strategy"] == "capability_based"

  @pytest.mark.asyncio
  async def test_route_query_with_context_enrichment(
    self, orchestrator, mock_context_enricher
  ):
    """Test query routing with context enrichment."""
    response = await orchestrator.route_query(
      query="Analyze financial data",
      context={"original": "context"},
      mode=AgentMode.STANDARD,
    )

    # Context enricher should be called
    mock_context_enricher.enrich.assert_called_once()
    assert response.metadata["context_enriched"] is True

  @pytest.mark.asyncio
  async def test_route_query_no_rag(self, orchestrator, mock_context_enricher):
    """Test disabling RAG enrichment."""
    orchestrator.config.enable_rag = False

    response = await orchestrator.route_query(
      query="Test query",
      context={"original": "context"},
      mode=AgentMode.QUICK,
    )

    # Context enricher should not be called
    mock_context_enricher.enrich.assert_not_called()
    assert response.metadata.get("context_enriched", False) is False

  @pytest.mark.asyncio
  async def test_ensemble_routing(self, orchestrator):
    """Test ensemble routing strategy."""
    orchestrator.config.routing_strategy = RoutingStrategy.ENSEMBLE

    response = await orchestrator.route_query(
      query="Complex analysis needed",
      mode=AgentMode.EXTENDED,
      ensemble_size=2,
    )

    # Should aggregate responses from multiple agents
    assert response.metadata["routing_strategy"] == "ensemble"
    assert "ensemble_agents" in response.metadata
    assert len(response.metadata["ensemble_agents"]) >= 2

  @pytest.mark.asyncio
  async def test_route_query_with_history(self, orchestrator):
    """Test routing with conversation history."""
    history = [
      {"role": "user", "content": "Previous question"},
      {"role": "assistant", "content": "Previous answer"},
    ]

    response = await orchestrator.route_query(
      query="Follow-up question", history=history, mode=AgentMode.STANDARD
    )

    assert response is not None
    assert response.metadata.get("has_history") is True

  @pytest.mark.asyncio
  async def test_route_query_error_handling(self, orchestrator):
    """Test error handling in routing."""
    # Make agent raise an error
    with patch.object(
      orchestrator.registry.get_agent("financial"),
      "analyze",
      side_effect=Exception("Agent failed"),
    ):
      response = await orchestrator.route_query(
        query="Test query", agent_type="financial", mode=AgentMode.STANDARD
      )

      assert response.error_details is not None
      assert "Agent failed" in response.error_details["message"]

  @pytest.mark.asyncio
  async def test_route_query_timeout(self, orchestrator):
    """Test query timeout handling."""
    orchestrator.config.timeout = 0.1  # 100ms timeout

    async def slow_analyze(*args, **kwargs):
      await asyncio.sleep(1)  # Simulate slow response
      return AgentResponse(
        content="Too late",
        agent_name="slow",
        mode_used=AgentMode.STANDARD,
        tokens_used={"input": 0, "output": 0},
      )

    with patch.object(
      orchestrator.registry.get_agent("financial"),
      "analyze",
      side_effect=slow_analyze,
    ):
      response = await orchestrator.route_query(
        query="Test query", agent_type="financial", mode=AgentMode.STANDARD
      )

      assert response.error_details is not None
      assert "timeout" in response.error_details["message"].lower()

  @pytest.mark.asyncio
  async def test_multi_agent_coordination(self, orchestrator):
    """Test coordination between multiple agents."""
    response = await orchestrator.coordinate_agents(
      query="Complex multi-part question",
      agent_sequence=["rag", "financial", "research"],
      mode=AgentMode.EXTENDED,
    )

    assert response.metadata["coordination_type"] == "sequential"
    assert len(response.metadata["agent_sequence"]) == 3
    assert response.content is not None

  @pytest.mark.asyncio
  async def test_parallel_agent_execution(self, orchestrator):
    """Test parallel execution of multiple agents."""
    response = await orchestrator.coordinate_agents(
      query="Analyze from multiple perspectives",
      agent_sequence=["financial", "research"],
      mode=AgentMode.STANDARD,
      coordination_type="parallel",
    )

    assert response.metadata["coordination_type"] == "parallel"
    assert "execution_time" in response.metadata

  def test_get_agent_recommendations(self, orchestrator):
    """Test getting agent recommendations for a query."""
    recommendations = orchestrator.get_agent_recommendations(
      query="Financial analysis of SEC filings"
    )

    assert len(recommendations) > 0
    assert recommendations[0]["agent_type"] == "financial"
    assert recommendations[0]["confidence"] >= 0.0
    assert recommendations[0]["confidence"] <= 1.0

  @pytest.mark.asyncio
  async def test_route_with_streaming(self, orchestrator):
    """Test routing with streaming mode."""
    chunks = []

    async def stream_callback(chunk):
      chunks.append(chunk)

    response = await orchestrator.route_query(
      query="Stream this response",
      mode=AgentMode.STREAMING,
      stream_callback=stream_callback,
    )

    assert response.mode_used == AgentMode.STREAMING
    # Chunks would be collected if streaming was implemented

  @pytest.mark.asyncio
  async def test_caching_enabled(self, orchestrator):
    """Test response caching when enabled."""
    orchestrator.config.enable_caching = True
    orchestrator._cache = {}  # Initialize cache since we enabled it after init

    # First call
    response1 = await orchestrator.route_query(
      query="Cached query", mode=AgentMode.QUICK
    )

    # Second call with same query
    response2 = await orchestrator.route_query(
      query="Cached query", mode=AgentMode.QUICK
    )

    assert response1.content == response2.content
    assert response2.metadata.get("from_cache") is True

  @pytest.mark.asyncio
  async def test_load_balanced_routing(self, orchestrator):
    """Test load-balanced routing strategy."""
    orchestrator.config.routing_strategy = RoutingStrategy.LOAD_BALANCED

    responses = []
    for _ in range(3):
      response = await orchestrator.route_query(
        query="Test query", mode=AgentMode.QUICK
      )
      responses.append(response.agent_name)

    # Should distribute across agents
    assert len(set(responses)) > 1

  @pytest.mark.asyncio
  async def test_agent_metrics_collection(self, orchestrator):
    """Test that orchestrator collects agent metrics."""
    await orchestrator.route_query(
      query="Test query", agent_type="financial", mode=AgentMode.STANDARD
    )

    metrics = orchestrator.get_metrics()

    assert "financial" in metrics["agent_usage"]
    assert metrics["agent_usage"]["financial"]["calls"] >= 1
    assert metrics["total_queries"] >= 1

  @pytest.mark.asyncio
  async def test_credit_check_before_agent_execution(self, mock_user):
    """Test that credits are checked before executing an agent."""
    # Create a mock database session
    mock_db = Mock()

    orchestrator = AgentOrchestrator("test-graph", mock_user, mock_db)

    # Create a mock agent that requires credits
    mock_agent = Mock(spec=BaseAgent)
    mock_agent.metadata = AgentMetadata(
      name="Test Agent",
      description="Test",
      capabilities=[],
      requires_credits=True,
    )
    mock_agent.analyze = AsyncMock(
      return_value=AgentResponse(
        content="Test response",
        agent_name="Test Agent",
        mode_used=AgentMode.STANDARD,
      )
    )

    # Mock insufficient credits
    with patch(
      "robosystems.operations.graph.credit_service.CreditService"
    ) as mock_credit_service:
      mock_service_instance = Mock()
      mock_credit_service.return_value = mock_service_instance
      mock_service_instance.check_credit_balance.return_value = {
        "has_sufficient_credits": False,
        "estimated_credits": 10.0,
        "available_credits": 5.0,
      }

      response = await orchestrator._execute_agent(
        mock_agent, "test query", AgentMode.STANDARD, None, {}
      )

      # Should return an error response about insufficient credits
      assert response is not None
      assert "Insufficient credits" in response.content
      assert response.error_details is not None
      assert response.error_details["code"] == "INSUFFICIENT_CREDITS"
      assert mock_agent.analyze.called is False  # Agent should not be executed

  @pytest.mark.asyncio
  async def test_credit_check_passes_execution_proceeds(self, mock_user):
    """Test that agent executes when sufficient credits are available."""
    mock_db = Mock()
    orchestrator = AgentOrchestrator("test-graph", mock_user, mock_db)

    # Create a mock agent that requires credits
    mock_agent = Mock(spec=BaseAgent)
    mock_agent.metadata = AgentMetadata(
      name="Test Agent",
      description="Test",
      capabilities=[],
      requires_credits=True,
    )
    expected_response = AgentResponse(
      content="Success response",
      agent_name="Test Agent",
      mode_used=AgentMode.STANDARD,
    )
    mock_agent.analyze = AsyncMock(return_value=expected_response)

    # Mock sufficient credits
    with patch(
      "robosystems.operations.graph.credit_service.CreditService"
    ) as mock_credit_service:
      mock_service_instance = Mock()
      mock_credit_service.return_value = mock_service_instance
      mock_service_instance.check_credit_balance.return_value = {
        "has_sufficient_credits": True,
        "estimated_credits": 10.0,
        "available_credits": 100.0,
      }

      response = await orchestrator._execute_agent(
        mock_agent, "test query", AgentMode.STANDARD, None, {}
      )

      # Should execute normally
      assert response.content == "Success response"
      assert mock_agent.analyze.called is True

  @pytest.mark.asyncio
  async def test_credit_check_skipped_for_non_credit_agents(self, mock_user):
    """Test that agents not requiring credits skip credit check."""
    orchestrator = AgentOrchestrator("test-graph", mock_user, None)  # No DB session

    # Create a mock agent that doesn't require credits (like RAG)
    mock_agent = Mock(spec=BaseAgent)
    mock_agent.metadata = AgentMetadata(
      name="RAG Agent",
      description="Test",
      capabilities=[],
      requires_credits=False,  # No credits required
    )
    expected_response = AgentResponse(
      content="RAG response",
      agent_name="RAG Agent",
      mode_used=AgentMode.QUICK,
    )
    mock_agent.analyze = AsyncMock(return_value=expected_response)

    response = await orchestrator._execute_agent(
      mock_agent, "test query", AgentMode.QUICK, None, {}
    )

    # Should execute without credit check
    assert response.content == "RAG response"
    assert mock_agent.analyze.called is True
