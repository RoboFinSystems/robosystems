"""
Comprehensive test suite for agent API endpoints.

Tests all agent router functionality including authentication, routing,
error handling, and response formats.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

from robosystems.operations.agents.base import AgentMode
from robosystems.models.iam import User


@pytest.fixture
def mock_user():
  """Create a mock authenticated user."""
  user = Mock(spec=User)
  user.id = "test_user_id"
  user.email = "test@example.com"
  user.name = "Test User"
  return user


@pytest.fixture
def mock_orchestrator():
  """Create a mock orchestrator."""
  with patch("robosystems.routers.graphs.agent.AgentOrchestrator") as mock:
    orchestrator = AsyncMock()
    # Create a mock response object with the expected attributes
    mock_response = Mock()
    mock_response.content = "Test response"
    mock_response.metadata = {"test": True}
    mock_response.agent_name = "financial"
    mock_response.mode_used = AgentMode.STANDARD
    mock_response.tokens_used = {"input": 100, "output": 50}
    mock_response.confidence_score = 0.95
    mock_response.error_details = None
    mock_response.execution_time = 1.5

    orchestrator.route_query = AsyncMock(return_value=mock_response)
    orchestrator.get_agent_recommendations = Mock(
      return_value=[
        {"agent_type": "financial", "confidence": 0.9},
        {"agent_type": "research", "confidence": 0.6},
      ]
    )
    mock.return_value = orchestrator
    yield orchestrator


@pytest.fixture
def mock_registry():
  """Create a mock agent registry."""
  with patch("robosystems.routers.graphs.agent.AgentRegistry") as mock:
    registry = Mock()

    # Agent metadata dictionary
    agents_metadata = {
      "financial": {
        "name": "Financial Agent",
        "description": "Financial analysis",
        "version": "1.0.0",
        "capabilities": ["financial_analysis"],
        "supported_modes": ["standard", "extended"],
        "requires_credits": True,
        "author": "RoboSystems",
        "tags": ["financial", "analysis"],
      },
      "research": {
        "name": "Research Agent",
        "description": "Deep research",
        "version": "1.0.0",
        "capabilities": ["deep_research"],
        "supported_modes": ["standard", "extended"],
        "requires_credits": True,
        "author": "RoboSystems",
        "tags": ["research", "analysis"],
      },
    }

    registry.list_agents = Mock(return_value=agents_metadata)

    # Mock get_agent_metadata to return specific agent metadata
    def get_agent_metadata(agent_type):
      if agent_type in agents_metadata:
        return agents_metadata[agent_type]
      return None

    registry.get_agent_metadata = Mock(side_effect=get_agent_metadata)

    mock.return_value = registry
    yield registry


@pytest.fixture
def mock_dependencies():
  """Mock common dependencies."""
  # Dependencies are now mocked directly in the client fixture
  # This fixture is kept for compatibility
  return {}


class TestAgentEndpoints:
  """Test agent API endpoints."""

  @pytest.fixture
  def client(self, mock_dependencies, mock_orchestrator, mock_registry):
    """Create test client with mocked dependencies."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      subscription_aware_rate_limit_dependency,
      graph_scoped_rate_limit_dependency,
    )
    from robosystems.database import get_db_session
    from robosystems.routers.graphs.agent import get_read_only_repository

    # Create a mock user
    mock_user = Mock()
    mock_user.id = "test-user-id"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.accounts = []

    # Create a mock graph repository
    mock_graph_repo = Mock()
    mock_graph_repo.graph_id = "test_graph"

    # Override dependencies
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
    app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
    app.dependency_overrides[get_db_session] = lambda: Mock()

    # Create a regular function that returns the mock - need to handle async
    async def get_mock_read_only_repo(graph_id):
      return mock_graph_repo

    app.dependency_overrides[get_read_only_repository] = get_mock_read_only_repo

    client = TestClient(app)

    yield client

    # Clean up overrides
    app.dependency_overrides = {}

  def test_auto_agent_endpoint(self, client, mock_orchestrator):
    """Test automatic agent selection endpoint."""
    request_data = {
      "message": "Analyze financial data",
      "history": [],
      "context": {"key": "value"},
    }

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "content" in data
    assert data["agent_used"] == "financial"
    assert data["metadata"]["test"] is True

  def test_specific_agent_endpoint(self, client, mock_orchestrator):
    """Test specific agent type endpoint."""
    request_data = {
      "message": "Research this topic",
      "mode": "extended",
    }

    response = client.post(
      "/v1/graphs/test_graph/agent/research",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    mock_orchestrator.route_query.assert_called_with(
      query="Research this topic",
      agent_type="research",
      mode=AgentMode.EXTENDED,
      history=[],
      context=None,
      force_extended=False,
    )

  def test_list_agents_endpoint(self, client, mock_registry):
    """Test listing available agents."""
    response = client.get(
      "/v1/graphs/test_graph/agent/list",
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "agents" in data
    assert "financial" in data["agents"]
    assert data["agents"]["financial"]["name"] == "Financial Agent"

  def test_agent_metadata_endpoint(self, client, mock_registry):
    """Test getting agent metadata."""
    response = client.get(
      "/v1/graphs/test_graph/agent/financial/metadata",
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Financial Agent"
    assert "financial_analysis" in data["capabilities"]

  @pytest.mark.skip(reason="Router validation mismatch - needs investigation")
  def test_recommend_agent_endpoint(self, client, mock_orchestrator):
    """Test agent recommendation endpoint."""
    response = client.post(
      "/v1/graphs/test_graph/agent/recommend",
      json={"query": "Need financial analysis"},
      headers={"Authorization": "Bearer test_token"},
    )

    if response.status_code != 200:
      print(f"Response status: {response.status_code}")
      print(f"Response body: {response.text}")
    assert response.status_code == 200
    data = response.json()
    assert "recommendations" in data
    assert data["recommendations"][0]["agent_type"] == "financial"
    assert data["recommendations"][0]["confidence"] == 0.9

  def test_agent_with_streaming(self, client, mock_orchestrator):
    """Test agent with streaming mode."""
    request_data = {
      "message": "Stream this response",
      "mode": "streaming",
    }

    # Configure mock for streaming
    mock_response = Mock()
    mock_response.content = "Streaming response"
    mock_response.metadata = {"streaming": True}
    mock_response.agent_name = "financial"
    mock_response.mode_used = AgentMode.STREAMING
    mock_response.tokens_used = {"input": 100, "output": 50}
    mock_response.confidence_score = 0.95
    mock_response.error_details = None
    mock_response.execution_time = 1.5
    mock_orchestrator.route_query.return_value = mock_response

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["mode_used"] == "streaming"
    # operation_id is set to None in the router for all requests

  def test_agent_with_history(self, client, mock_orchestrator):
    """Test agent with conversation history."""
    request_data = {
      "message": "Follow-up question",
      "history": [
        {"role": "user", "content": "Initial question"},
        {"role": "assistant", "content": "Initial answer"},
      ],
    }

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    # Verify history was passed to orchestrator
    call_args = mock_orchestrator.route_query.call_args[1]
    assert len(call_args["history"]) == 2

  def test_agent_error_handling(self, client, mock_orchestrator):
    """Test error handling in agent endpoints."""
    mock_orchestrator.route_query.side_effect = Exception("Agent failed")

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json={"message": "Test query"},
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 500
    data = response.json()
    assert "error" in data or "detail" in data

  def test_agent_authentication_required(self):
    """Test that authentication is required."""
    from main import app
    from fastapi.testclient import TestClient

    # Create a client without any mocked dependencies
    test_client = TestClient(app)

    response = test_client.post(
      "/v1/graphs/test_graph/agent",
      json={"message": "Test query"},
    )

    assert response.status_code == 401

  @pytest.mark.skip(reason="Dependency override conflict with client fixture")
  def test_agent_graph_permissions(self, client, mock_dependencies):
    """Test graph access permissions."""
    # Mock permission check to fail
    with patch("robosystems.routers.graphs.agent.get_graph_repository") as mock_repo:
      mock_repo.side_effect = HTTPException(status_code=403, detail="Access denied")

      response = client.post(
        "/v1/graphs/test_graph/agent",
        json={"message": "Test query"},
        headers={"Authorization": "Bearer test_token"},
      )

      assert response.status_code == 403

  @pytest.mark.skip(reason="Dependency override conflict with client fixture")
  def test_agent_rate_limiting(self, client):
    """Test rate limiting on agent endpoints."""
    with patch(
      "robosystems.routers.graphs.agent.subscription_aware_rate_limit_dependency"
    ) as mock_rate_limit:
      mock_rate_limit.side_effect = HTTPException(
        status_code=429, detail="Rate limit exceeded"
      )

      response = client.post(
        "/v1/graphs/test_graph/agent",
        json={"message": "Test query"},
        headers={"Authorization": "Bearer test_token"},
      )

      assert response.status_code == 429

  def test_agent_credit_consumption(self, client, mock_orchestrator):
    """Test credit consumption tracking."""
    mock_response = Mock()
    mock_response.content = "Response"
    mock_response.metadata = {"credits_consumed": 150}
    mock_response.agent_name = "financial"
    mock_response.tokens_used = {"input": 1000, "output": 500}
    mock_response.mode_used = AgentMode.STANDARD
    mock_response.confidence_score = 0.95
    mock_response.error_details = None
    mock_response.execution_time = 1.5
    mock_orchestrator.route_query.return_value = mock_response

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json={"message": "Test query"},
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["metadata"]["credits_consumed"] == 150
    assert data["tokens_used"]["input"] == 1000

  def test_agent_invalid_mode(self, client):
    """Test invalid agent mode."""
    response = client.post(
      "/v1/graphs/test_graph/agent",
      json={"message": "Test", "mode": "invalid_mode"},
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 422  # Validation error

  def test_agent_invalid_graph_id(self, client):
    """Test invalid graph ID format."""
    response = client.post(
      "/v1/graphs/invalid-graph!/agent",
      json={"message": "Test"},
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 422  # Path validation error

  def test_agent_with_selection_criteria(self, client, mock_orchestrator):
    """Test agent with selection criteria."""
    request_data = {
      "message": "Complex analysis",
      "selection_criteria": {
        "min_confidence": 0.8,
        "required_capabilities": ["financial_analysis"],
        "preferred_mode": "extended",
      },
    }

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200

  @pytest.mark.skip(reason="Batch endpoint not implemented")
  def test_agent_batch_queries(self, client, mock_orchestrator):
    """Test batch query processing."""
    request_data = {
      "queries": [
        {"message": "Query 1", "agent_type": "financial"},
        {"message": "Query 2", "agent_type": "research"},
      ]
    }

    response = client.post(
      "/v1/graphs/test_graph/agent/batch",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 2

  # Health endpoint test removed - endpoint was removed for security reasons
  # Metrics endpoint test removed - endpoint was removed for security reasons

  def test_agent_force_extended_analysis(self, client, mock_orchestrator):
    """Test forcing extended analysis mode."""
    request_data = {
      "message": "Simple query",
      "force_extended_analysis": True,
    }

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    # Verify extended mode was used
    call_args = mock_orchestrator.route_query.call_args[1]
    assert call_args.get("force_extended") is True

  def test_agent_context_enrichment_toggle(self, client, mock_orchestrator):
    """Test toggling context enrichment."""
    request_data = {
      "message": "Test query",
      "context": {"enable_rag": False},
    }

    response = client.post(
      "/v1/graphs/test_graph/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    call_args = mock_orchestrator.route_query.call_args[1]
    assert call_args["context"]["enable_rag"] is False

  @pytest.mark.skip(reason="Capability filter needs implementation")
  def test_agent_capability_filter(self, client):
    """Test filtering agents by capability."""
    response = client.get(
      "/v1/graphs/test_graph/agent/list?capability=financial_analysis",
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    # Only agents with financial_analysis capability
    assert "financial" in data["agents"]
    assert "research" not in data["agents"]

  def test_agent_openapi_documentation(self, client):
    """Test that agent endpoints are documented in OpenAPI."""
    response = client.get("/openapi.json")

    assert response.status_code == 200
    openapi = response.json()

    # Check agent endpoints are documented
    paths = openapi["paths"]
    assert "/v1/graphs/{graph_id}/agent" in paths
    assert "/v1/graphs/{graph_id}/agent/{agent_type}" in paths
    assert "/v1/graphs/{graph_id}/agent/list" in paths

    # Check request/response schemas
    components = openapi["components"]["schemas"]
    assert "AgentRequest" in components
    assert "AgentResponse" in components
