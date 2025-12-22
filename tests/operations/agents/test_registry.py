"""
Test suite for agent registry and discovery system.

Tests agent registration, discovery, and lifecycle management.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from robosystems.models.iam import User
from robosystems.operations.agents.base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  BaseAgent,
)
from robosystems.operations.agents.registry import (
  AgentNotFoundError,
  AgentRegistrationError,
  AgentRegistry,
  DuplicateAgentError,
)


class MockFinancialAgent(BaseAgent):
  """Mock financial agent for testing."""

  @property
  def metadata(self) -> AgentMetadata:
    return AgentMetadata(
      name="Financial Agent",
      description="Financial analysis agent",
      capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
      version="1.0.0",
    )

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict] | None = None,
    context: dict | None = None,
    callback: Any | None = None,
  ) -> AgentResponse:
    return AgentResponse(
      content=f"Financial analysis: {query}",
      agent_name=self.metadata.name,
      mode_used=mode,
      tokens_used={"input": 0, "output": 0},
    )

  def can_handle(self, query: str, context=None) -> float:
    if "financial" in query.lower() or "money" in query.lower():
      return 0.9
    return 0.3


class MockResearchAgent(BaseAgent):
  """Mock research agent for testing."""

  @property
  def metadata(self) -> AgentMetadata:
    return AgentMetadata(
      name="Research Agent",
      description="Deep research agent",
      capabilities=[AgentCapability.DEEP_RESEARCH],
      version="2.0.0",
    )

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict] | None = None,
    context: dict | None = None,
    callback: Any | None = None,
  ) -> AgentResponse:
    return AgentResponse(
      content=f"Research results: {query}",
      agent_name=self.metadata.name,
      mode_used=mode,
      tokens_used={"input": 0, "output": 0},
    )

  def can_handle(self, query: str, context=None) -> float:
    if "research" in query.lower() or "investigate" in query.lower():
      return 0.85
    return 0.2


class TestAgentRegistry:
  """Test the agent registry system."""

  @pytest.fixture
  def registry(self):
    """Create a fresh registry instance."""
    # Clear any existing registrations
    AgentRegistry._agents.clear()
    return AgentRegistry()

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  def test_registry_singleton(self):
    """Test that registry is a singleton."""
    registry1 = AgentRegistry()
    registry2 = AgentRegistry()
    assert registry1 is registry2

  def test_register_agent_decorator(self, registry):
    """Test registering agent with decorator."""

    @registry.register("test_agent")
    class TestAgent(BaseAgent):
      @property
      def metadata(self):
        return AgentMetadata(
          name="Test Agent",
          description="Test",
          capabilities=[],
        )

      async def analyze(
        self,
        query: str,
        mode: AgentMode = AgentMode.STANDARD,
        history: list[dict] | None = None,
        context: dict | None = None,
        callback: Any | None = None,
      ):
        return AgentResponse(
          content="test",
          agent_name="test",
          mode_used=mode,
          tokens_used={"input": 0, "output": 0},
        )

      def can_handle(self, query, context=None):
        return 0.5

    assert "test_agent" in registry._agents
    assert registry._agents["test_agent"] == TestAgent

  def test_register_agent_method(self, registry):
    """Test registering agent with method."""
    registry.register_agent("financial", MockFinancialAgent)

    assert "financial" in registry._agents
    assert registry._agents["financial"] == MockFinancialAgent

  def test_register_duplicate_agent(self, registry):
    """Test that duplicate registration raises error."""
    registry.register_agent("financial", MockFinancialAgent)

    with pytest.raises(DuplicateAgentError):
      registry.register_agent("financial", MockResearchAgent)

  def test_register_with_override(self, registry):
    """Test overriding existing agent registration."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("financial", MockResearchAgent, override=True)

    assert registry._agents["financial"] == MockResearchAgent

  def test_unregister_agent(self, registry):
    """Test unregistering an agent."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.unregister_agent("financial")

    assert "financial" not in registry._agents

  def test_unregister_nonexistent_agent(self, registry):
    """Test unregistering non-existent agent."""
    with pytest.raises(AgentNotFoundError):
      registry.unregister_agent("nonexistent")

  def test_get_agent(self, registry, mock_user):
    """Test getting agent instance."""
    registry.register_agent("financial", MockFinancialAgent)

    agent = registry.get_agent("financial", "test_graph", mock_user)

    assert agent is not None
    assert isinstance(agent, MockFinancialAgent)
    assert agent.graph_id == "test_graph"
    assert agent.user == mock_user

  def test_get_nonexistent_agent(self, registry, mock_user):
    """Test getting non-existent agent returns None."""
    agent = registry.get_agent("nonexistent", "test_graph", mock_user)
    assert agent is None

  def test_get_agent_with_db_session(self, registry, mock_user):
    """Test getting agent with database session."""
    registry.register_agent("financial", MockFinancialAgent)
    db_session = Mock()

    agent = registry.get_agent(
      "financial", "test_graph", mock_user, db_session=db_session
    )

    assert agent.db_session == db_session

  def test_get_all_agents(self, registry, mock_user):
    """Test getting all registered agents."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    agents = registry.get_all_agents("test_graph", mock_user)

    assert len(agents) == 2
    assert "financial" in agents
    assert "research" in agents
    assert isinstance(agents["financial"], MockFinancialAgent)
    assert isinstance(agents["research"], MockResearchAgent)

  def test_list_agents(self, registry):
    """Test listing agent metadata."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    agent_list = registry.list_agents()

    assert len(agent_list) == 2
    assert agent_list["financial"]["name"] == "Financial Agent"
    assert agent_list["financial"]["version"] == "1.0.0"
    assert agent_list["research"]["name"] == "Research Agent"
    assert agent_list["research"]["version"] == "2.0.0"

  def test_get_agents_by_capability(self, registry):
    """Test getting agents by capability."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    financial_agents = registry.get_agents_by_capability(
      AgentCapability.FINANCIAL_ANALYSIS
    )
    research_agents = registry.get_agents_by_capability(AgentCapability.DEEP_RESEARCH)

    assert len(financial_agents) == 1
    assert "financial" in financial_agents
    assert len(research_agents) == 1
    assert "research" in research_agents

  def test_get_agents_by_mode(self, registry):
    """Test getting agents that support specific mode."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    # Both agents support standard mode by default
    standard_agents = registry.get_agents_by_mode(AgentMode.STANDARD)

    assert len(standard_agents) >= 2

  def test_validate_agent_class(self, registry):
    """Test agent class validation."""

    class InvalidAgent:
      """Not a BaseAgent subclass."""

      pass

    with pytest.raises(AgentRegistrationError):
      registry.register_agent("invalid", InvalidAgent)

  def test_agent_discovery(self, registry):
    """Test agent discovery based on query."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    # Discover best agent for financial query
    best_agent = registry.discover_agent(
      query="Need financial analysis", graph_id="test", user=Mock()
    )

    assert best_agent is not None
    assert isinstance(best_agent, MockFinancialAgent)

    # Discover best agent for research query
    best_agent = registry.discover_agent(
      query="Research this topic", graph_id="test", user=Mock()
    )

    assert best_agent is not None
    assert isinstance(best_agent, MockResearchAgent)

  def test_bulk_registration(self, registry):
    """Test bulk agent registration."""
    agents = {
      "financial": MockFinancialAgent,
      "research": MockResearchAgent,
    }

    registry.register_bulk(agents)

    assert "financial" in registry._agents
    assert "research" in registry._agents

  def test_clear_registry(self, registry):
    """Test clearing all registrations."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.register_agent("research", MockResearchAgent)

    registry.clear()

    assert len(registry._agents) == 0

  def test_agent_aliases(self, registry):
    """Test agent aliases for backward compatibility."""
    registry.register_agent("financial", MockFinancialAgent)
    registry.add_alias("finance", "financial")
    registry.add_alias("money", "financial")

    agent1 = registry.get_agent("finance", "test", Mock())
    agent2 = registry.get_agent("money", "test", Mock())

    assert isinstance(agent1, MockFinancialAgent)
    assert isinstance(agent2, MockFinancialAgent)

  def test_get_agent_metadata(self, registry):
    """Test getting agent metadata without instantiation."""
    registry.register_agent("financial", MockFinancialAgent)

    metadata = registry.get_agent_metadata("financial")

    assert metadata is not None
    assert metadata["name"] == "Financial Agent"
    assert metadata["capabilities"] == ["financial_analysis"]

  def test_is_registered(self, registry):
    """Test checking if agent is registered."""
    registry.register_agent("financial", MockFinancialAgent)

    assert registry.is_registered("financial") is True
    assert registry.is_registered("nonexistent") is False

  def test_agent_initialization_error(self, registry, mock_user):
    """Test handling agent initialization errors."""

    class FailingAgent(BaseAgent):
      def __init__(self, *args, **kwargs):
        raise ValueError("Initialization failed")

      @property
      def metadata(self):
        return AgentMetadata(name="Fail", description="Fails", capabilities=[])

      async def analyze(
        self,
        query: str,
        mode: AgentMode = AgentMode.STANDARD,
        history: list[dict] | None = None,
        context: dict | None = None,
        callback: Any | None = None,
      ) -> AgentResponse:
        raise NotImplementedError("Failing agent analyze")

      def can_handle(self, query, context=None):
        return 0.0

    registry.register_agent("failing", FailingAgent)

    with pytest.raises(AgentRegistrationError):
      registry.get_agent("failing", "test", mock_user)

  def test_agent_health_check(self, registry, mock_user):
    """Test agent health checking."""
    registry.register_agent("financial", MockFinancialAgent)

    health = registry.check_agent_health("financial", "test", mock_user)

    assert health["status"] == "healthy"
    assert health["agent_type"] == "financial"
    assert "metadata" in health

  def test_lazy_loading(self, registry):
    """Test lazy loading of agents."""
    # Register agent class path instead of class itself
    registry.register_lazy(
      "financial", "robosystems.operations.agents.financial.FinancialAgent"
    )

    # Agent should be registered but not loaded
    assert "financial" in registry._lazy_imports
    # When getting the agent, it should be imported and instantiated
    # (This would require the actual implementation)

  def test_agent_dependencies(self, registry):
    """Test agent dependency management."""
    registry.register_agent("financial", MockFinancialAgent)

    # Register agent with dependencies
    @registry.register("composite", depends_on=["financial", "research"])
    class CompositeAgent(BaseAgent):
      @property
      def metadata(self):
        return AgentMetadata(
          name="Composite", description="Composite agent", capabilities=[]
        )

      async def analyze(
        self,
        query: str,
        mode: AgentMode = AgentMode.STANDARD,
        history: list[dict] | None = None,
        context: dict | None = None,
        callback: Any | None = None,
      ):
        return AgentResponse(
          content="composite",
          agent_name="composite",
          mode_used=mode,
          tokens_used={"input": 0, "output": 0},
        )

      def can_handle(self, query, context=None):
        return 0.5

    # Should not be able to instantiate without dependencies
    with pytest.raises(AgentRegistrationError):
      registry.get_agent("composite", "test", Mock())

    # After registering dependency, should work
    registry.register_agent("research", MockResearchAgent)
    agent = registry.get_agent("composite", "test", Mock())
    assert agent is not None

  def test_agent_versioning(self, registry):
    """Test agent version management."""
    registry.register_agent("test_financial", MockFinancialAgent, version="1.0.0")

    # Register newer version
    class MockFinancialAgentV2(MockFinancialAgent):
      @property
      def metadata(self):
        return AgentMetadata(
          name="Financial Agent",
          description="Financial analysis agent v2",
          capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
          version="2.0.0",
        )

    registry.register_agent(
      "test_financial", MockFinancialAgentV2, override=True, version="2.0.0"
    )

    # Get specific version
    agent_v1 = registry.get_agent("test_financial", "test", Mock(), version="1.0.0")
    agent_v2 = registry.get_agent("test_financial", "test", Mock(), version="2.0.0")

    assert agent_v1.metadata.version == "1.0.0"
    assert agent_v2.metadata.version == "2.0.0"

    # Get latest by default
    agent_latest = registry.get_agent("test_financial", "test", Mock())
    assert agent_latest.metadata.version == "2.0.0"
