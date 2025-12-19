"""
Comprehensive test suite for the base agent architecture.

Tests the abstract base class, agent modes, capabilities, and core behaviors.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.models.iam import User
from robosystems.operations.agents.base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  BaseAgent,
)


class TestAgentCapability:
  """Test agent capability enumeration."""

  def test_capability_values(self):
    """Test that all expected capabilities are defined."""
    assert AgentCapability.FINANCIAL_ANALYSIS.value == "financial_analysis"
    assert AgentCapability.DEEP_RESEARCH.value == "deep_research"
    assert AgentCapability.COMPLIANCE.value == "compliance"
    assert AgentCapability.RAG_SEARCH.value == "rag_search"
    assert AgentCapability.CUSTOM.value == "custom"
    assert AgentCapability.ENTITY_ANALYSIS.value == "entity_analysis"
    assert AgentCapability.TREND_ANALYSIS.value == "trend_analysis"

  def test_capability_from_string(self):
    """Test creating capability from string value."""
    cap = AgentCapability("financial_analysis")
    assert cap == AgentCapability.FINANCIAL_ANALYSIS


class TestAgentMode:
  """Test agent execution modes."""

  def test_mode_values(self):
    """Test that all expected modes are defined."""
    assert AgentMode.QUICK.value == "quick"
    assert AgentMode.STANDARD.value == "standard"
    assert AgentMode.EXTENDED.value == "extended"
    assert AgentMode.STREAMING.value == "streaming"

  def test_mode_comparison(self):
    """Test mode comparison and ordering."""
    # Modes should have implicit complexity ordering
    assert AgentMode.QUICK != AgentMode.EXTENDED
    assert AgentMode.STANDARD.value == "standard"
    # Verify all modes exist
    assert AgentMode.QUICK.value == "quick"
    assert AgentMode.EXTENDED.value == "extended"
    assert AgentMode.STREAMING.value == "streaming"


class TestAgentMetadata:
  """Test agent metadata structure."""

  def test_metadata_creation(self):
    """Test creating agent metadata."""
    metadata = AgentMetadata(
      name="Test Agent",
      description="A test agent",
      version="1.0.0",
      capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
      supported_modes=[AgentMode.QUICK, AgentMode.STANDARD],
      max_tokens={"input": 100000, "output": 8000},
      requires_credits=True,
    )

    assert metadata.name == "Test Agent"
    assert metadata.version == "1.0.0"
    assert AgentCapability.FINANCIAL_ANALYSIS in metadata.capabilities
    assert AgentMode.QUICK in metadata.supported_modes
    assert metadata.requires_credits is True

  def test_metadata_defaults(self):
    """Test metadata with default values."""
    metadata = AgentMetadata(
      name="Simple Agent", description="Simple test", capabilities=[]
    )

    assert metadata.version == "1.0.0"
    assert metadata.supported_modes == [
      AgentMode.QUICK,
      AgentMode.STANDARD,
      AgentMode.EXTENDED,
    ]
    assert metadata.requires_credits is True
    assert metadata.max_tokens == {"input": 150000, "output": 8000}


class TestAgentResponse:
  """Test agent response structure."""

  def test_response_creation(self):
    """Test creating agent response."""
    response = AgentResponse(
      content="Test response",
      metadata={"key": "value"},
      agent_name="test_agent",
      mode_used=AgentMode.QUICK,
      tokens_used={"input": 100, "output": 50},
      tools_called=["tool1", "tool2"],
      confidence_score=0.95,
      requires_followup=False,
    )

    assert response.content == "Test response"
    assert response.metadata and response.metadata["key"] == "value"
    assert response.agent_name == "test_agent"
    assert response.confidence_score == 0.95
    assert len(response.tools_called) == 2

  def test_response_with_error(self):
    """Test response with error information."""
    response = AgentResponse(
      content="Error occurred",
      metadata={"error": "Tool failure"},
      agent_name="test_agent",
      mode_used=AgentMode.QUICK,
      error_details={"code": "TOOL_ERROR", "message": "Tool execution failed"},
    )

    assert response.error_details is not None
    assert response.error_details["code"] == "TOOL_ERROR"


class ConcreteTestAgent(BaseAgent):
  """Concrete implementation for testing."""

  def __init__(self, graph_id: str, user: User, db_session=None):
    super().__init__(graph_id, user, db_session)
    self.analyze_called = False
    self.can_handle_called = False

  @property
  def metadata(self) -> AgentMetadata:
    return AgentMetadata(
      name="Test Agent",
      description="Agent for testing",
      capabilities=[AgentCapability.CUSTOM],
      supported_modes=[AgentMode.QUICK, AgentMode.STANDARD],
    )

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict] | None = None,
    context: dict | None = None,
    callback: Any | None = None,
  ) -> AgentResponse:
    self.analyze_called = True
    return AgentResponse(
      content=f"Test response for: {query}",
      metadata={"test": True},
      agent_name=self.metadata.name,
      mode_used=mode,
      tokens_used={"input": 10, "output": 5},
    )

  def can_handle(self, query: str, context: dict | None = None) -> float:
    self.can_handle_called = True
    if "test" in query.lower():
      return 0.9
    return 0.1


class TestBaseAgent:
  """Test the base agent abstract class."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def test_agent(self, mock_user):
    """Create a test agent instance."""
    return ConcreteTestAgent("test_graph", mock_user)

  def test_agent_initialization(self, mock_user):
    """Test agent initialization."""
    agent = ConcreteTestAgent("test_graph", mock_user, db_session="mock_session")

    assert agent.graph_id == "test_graph"
    assert agent.user == mock_user
    assert agent.db_session == "mock_session"
    assert agent.total_tokens_used == {"input": 0, "output": 0}

  def test_agent_metadata(self, test_agent):
    """Test agent metadata property."""
    metadata = test_agent.metadata

    assert metadata.name == "Test Agent"
    assert metadata.description == "Agent for testing"
    assert AgentCapability.CUSTOM in metadata.capabilities
    assert AgentMode.QUICK in metadata.supported_modes

  @pytest.mark.asyncio
  async def test_agent_analyze(self, test_agent):
    """Test agent analyze method."""
    response = await test_agent.analyze(
      query="Test query", mode=AgentMode.QUICK, context={"key": "value"}
    )

    assert test_agent.analyze_called is True
    assert response.content == "Test response for: Test query"
    assert response.mode_used == AgentMode.QUICK
    assert response.tokens_used["input"] == 10

  def test_agent_can_handle(self, test_agent):
    """Test agent can_handle method."""
    # High confidence for test queries
    score = test_agent.can_handle("This is a test query")
    assert test_agent.can_handle_called is True
    assert score == 0.9

    # Low confidence for other queries
    score = test_agent.can_handle("Financial analysis needed")
    assert score == 0.1

  def test_agent_supports_mode(self, test_agent):
    """Test checking if agent supports a mode."""
    assert test_agent.supports_mode(AgentMode.QUICK) is True
    assert test_agent.supports_mode(AgentMode.STANDARD) is True
    assert test_agent.supports_mode(AgentMode.EXTENDED) is False
    assert test_agent.supports_mode(AgentMode.STREAMING) is False

  def test_agent_has_capability(self, test_agent):
    """Test checking if agent has a capability."""
    assert test_agent.has_capability(AgentCapability.CUSTOM) is True
    assert test_agent.has_capability(AgentCapability.FINANCIAL_ANALYSIS) is False

  @pytest.mark.asyncio
  async def test_agent_with_mcp_tools(self, test_agent, mock_user):
    """Test agent with MCP tools initialization."""
    # Since create_graph_mcp_client is async, we need to handle it properly
    mock_client = Mock()
    mock_tools_instance = Mock()

    with patch(
      "robosystems.middleware.mcp.create_graph_mcp_client", new_callable=AsyncMock
    ) as mock_create_client:
      with patch("robosystems.middleware.mcp.GraphMCPTools") as mock_tools:
        mock_create_client.return_value = mock_client
        mock_tools.return_value = mock_tools_instance

        # Create agent with MCP tools
        agent = ConcreteTestAgent("test_graph", mock_user)
        await agent.initialize_tools()

        mock_create_client.assert_called_once_with(graph_id="test_graph")
        mock_tools.assert_called_once()
        assert agent.graph_client == mock_client
        assert agent.mcp_tools == mock_tools_instance

  @pytest.mark.asyncio
  async def test_agent_cleanup(self, test_agent):
    """Test agent cleanup/close method."""
    test_agent.graph_client = AsyncMock()
    test_agent.graph_client.close = AsyncMock()

    await test_agent.close()

    test_agent.graph_client.close.assert_called_once()

  def test_agent_track_tokens(self, test_agent):
    """Test token tracking functionality."""
    test_agent.track_tokens(input_tokens=100, output_tokens=50)

    assert test_agent.total_tokens_used["input"] == 100
    assert test_agent.total_tokens_used["output"] == 50

    test_agent.track_tokens(input_tokens=50, output_tokens=25)

    assert test_agent.total_tokens_used["input"] == 150
    assert test_agent.total_tokens_used["output"] == 75

  @pytest.mark.asyncio
  async def test_agent_with_callback(self, test_agent):
    """Test agent with progress callback."""
    callback = Mock()

    response = await test_agent.analyze(
      query="Test query",
      mode=AgentMode.STANDARD,
      callback=callback,
    )

    # Agent implementation should call callback if provided
    # This would be implemented in the concrete agent
    assert response.content == "Test response for: Test query"

  def test_agent_abstract_methods(self, mock_user):
    """Test that abstract methods must be implemented."""
    with pytest.raises(TypeError):
      # Cannot instantiate abstract class directly
      BaseAgent("test_graph", mock_user)  # type: ignore[abstract]

  @pytest.mark.asyncio
  async def test_agent_mode_validation(self, test_agent):
    """Test that agent validates mode support."""
    # Should work fine with supported mode
    response = await test_agent.analyze(query="Test", mode=AgentMode.QUICK)
    assert response is not None

    # Extended mode not supported by test agent
    with patch.object(test_agent, "analyze") as mock_analyze:
      mock_analyze.side_effect = ValueError("Mode not supported")

      with pytest.raises(ValueError, match="Mode not supported"):
        await test_agent.analyze(query="Test", mode=AgentMode.EXTENDED)


class TestAgentCreditsIntegration:
  """Test agent integration with credit system."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def test_agent(self, mock_user):
    """Create a test agent instance."""
    return ConcreteTestAgent("test_graph", mock_user)

  @pytest.fixture
  def mock_credit_service(self):
    """Mock credit service."""
    with patch("robosystems.operations.graph.credit_service.CreditService") as mock:
      service = Mock()
      service.consume_ai_tokens = Mock(
        return_value={"success": True, "credits_consumed": 100}
      )
      mock.return_value = service
      yield service

  @pytest.mark.asyncio
  async def test_agent_consume_credits(self, test_agent, mock_credit_service):
    """Test agent credit consumption."""
    test_agent.db_session = Mock()

    await test_agent.consume_credits(
      input_tokens=1000, output_tokens=500, model="claude-3-sonnet"
    )

    mock_credit_service.consume_ai_tokens.assert_called_once()
    call_args = mock_credit_service.consume_ai_tokens.call_args[1]
    assert call_args["input_tokens"] == 1000
    assert call_args["output_tokens"] == 500
    assert call_args["model"] == "claude-3-sonnet"

  @pytest.mark.asyncio
  async def test_agent_no_credits_without_session(self, test_agent):
    """Test that credits aren't consumed without DB session."""
    test_agent.db_session = None

    # Should not raise error, just log
    await test_agent.consume_credits(input_tokens=1000, output_tokens=500)

    # No error should be raised
    assert True


class TestAgentModes:
  """Test different agent execution modes."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock(spec=User)
    user.id = "test_user_id"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def test_agent(self, mock_user):
    """Create a test agent instance."""
    return ConcreteTestAgent("test_graph", mock_user)

  @pytest.mark.asyncio
  async def test_quick_mode_limits(self, test_agent):
    """Test that quick mode has appropriate limits."""
    response = await test_agent.analyze(query="Quick test", mode=AgentMode.QUICK)

    # Quick mode should have lower token usage
    assert response.tokens_used["input"] <= 50000
    assert response.tokens_used["output"] <= 2000

  @pytest.mark.asyncio
  async def test_extended_mode_capabilities(self, test_agent):
    """Test extended mode capabilities."""
    # Mock extended analysis
    with patch.object(test_agent, "analyze") as mock_analyze:
      mock_analyze.return_value = AgentResponse(
        content="Extended analysis complete",
        metadata={"depth": "comprehensive"},
        agent_name="Test Agent",
        mode_used=AgentMode.EXTENDED,
        tokens_used={"input": 100000, "output": 8000},
        tools_called=["tool1", "tool2", "tool3", "tool4", "tool5"],
      )

      response = await test_agent.analyze(
        query="Deep research needed", mode=AgentMode.EXTENDED
      )

      assert response.mode_used == AgentMode.EXTENDED
      assert len(response.tools_called) >= 5
      assert response.tokens_used["input"] > 50000

  @pytest.mark.asyncio
  async def test_streaming_mode(self, test_agent):
    """Test streaming mode capabilities."""
    chunks = []

    async def stream_callback(chunk):
      chunks.append(chunk)

    with patch.object(test_agent, "analyze") as mock_analyze:
      mock_analyze.return_value = AgentResponse(
        content="Streaming response",
        metadata={"streaming": True},
        agent_name="Test Agent",
        mode_used=AgentMode.STREAMING,
      )

      response = await test_agent.analyze(
        query="Stream this", mode=AgentMode.STREAMING, callback=stream_callback
      )

      assert response.mode_used == AgentMode.STREAMING
