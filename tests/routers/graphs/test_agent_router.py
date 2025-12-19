"""
Comprehensive test suite for agent API endpoints.

Tests all agent router functionality including authentication, routing,
error handling, and response formats.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from robosystems.models.iam import User
from robosystems.operations.agents.base import AgentMode
from tests.conftest import VALID_TEST_GRAPH_ID


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
  with patch("robosystems.routers.graphs.agent.handlers.AgentOrchestrator") as mock:
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
  with patch("robosystems.routers.graphs.agent.execute.AgentRegistry") as mock:
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
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph
    from robosystems.middleware.rate_limits import (
      graph_scoped_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Create a mock user
    mock_user = Mock()
    mock_user.id = "test-user-id"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.accounts = []

    # Override dependencies
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
    app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
    app.dependency_overrides[get_db_session] = lambda: Mock()

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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert "content" in data
    assert data["agent_used"] == "financial"
    assert data["metadata"]["test"] is True

  def test_specific_agent_endpoint(self, client, mock_orchestrator, mock_registry):
    """Test specific agent type endpoint."""
    # Mock registry to return a valid agent with metadata
    mock_agent = Mock()
    mock_metadata = Mock()
    mock_metadata.execution_profile = {
      AgentMode.STANDARD: Mock(min_time=1, max_time=5, avg_time=3, tool_calls=5),
      AgentMode.EXTENDED: Mock(min_time=1, max_time=5, avg_time=3, tool_calls=12),
    }
    mock_agent.metadata = mock_metadata
    mock_registry.get_agent.return_value = mock_agent

    request_data = {
      "message": "Research this topic",
      "mode": "extended",
    }

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent/research",
      json=request_data,
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200

  def test_list_agents_endpoint(self, client, mock_registry):
    """Test listing available agents."""
    response = client.get(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent/financial",
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Financial Agent"
    assert "financial_analysis" in data["capabilities"]

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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
      json={"message": "Test query"},
      headers={"Authorization": "Bearer test_token"},
    )

    assert response.status_code == 500
    data = response.json()
    assert "error" in data or "detail" in data

  def test_agent_authentication_required(self):
    """Test that authentication is required."""
    from fastapi.testclient import TestClient

    from main import app

    # Create a client without any mocked dependencies
    test_client = TestClient(app)

    response = test_client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
      json={"message": "Test query"},
    )

    assert response.status_code == 401

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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent/batch",
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
    with (
      patch(
        "robosystems.routers.graphs.agent.handlers._run_agent_analysis_background"
      ) as mock_background_task,
      patch(
        "robosystems.routers.graphs.agent.handlers.create_operation_response",
        new_callable=AsyncMock,
        return_value={
          "operation_id": "test-op-123",
          "status": "pending",
          "_links": {
            "stream": "/v1/operations/test-op-123/stream",
            "status": "/v1/operations/test-op-123/status",
          },
        },
      ),
    ):
      # Mock the background task
      mock_background_task.return_value = None

      request_data = {
        "message": "Simple query",
        "force_extended_analysis": True,
      }

      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
        json=request_data,
        headers={"Authorization": "Bearer test_token"},
      )

      # Extended analysis always uses async execution (202)
      assert response.status_code == 202
      data = response.json()
      assert "operation_id" in data
      assert data["status"] == "pending"

  def test_agent_context_enrichment_toggle(self, client, mock_orchestrator):
    """Test toggling context enrichment."""
    request_data = {
      "message": "Test query",
      "context": {"enable_rag": False},
    }

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent",
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
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/agent?capability=financial_analysis",
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

    # Check request/response schemas
    components = openapi["components"]["schemas"]
    assert "AgentRequest" in components
    assert "AgentResponse" in components
