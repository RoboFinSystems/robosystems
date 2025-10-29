import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from robosystems.models.api.agent import AgentMessage


@pytest.fixture
def mock_anthropic_response():
  """Mock response from Anthropic API"""
  mock_content = [MagicMock(type="text", text="This is a test response from Claude")]
  mock_response = MagicMock()
  mock_response.content = mock_content
  return mock_response


@pytest.mark.asyncio
@pytest.mark.unit
async def test_agent_router_basic(
  client_with_mocked_auth: TestClient,
):
  """Test basic agent endpoint."""
  with (
    patch(
      "robosystems.routers.graphs.agent.AgentOrchestrator"
    ) as mock_orchestrator_class,
    patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id"
    ) as mock_get_credits,
  ):
    # Mock GraphCredits to return a credit pool
    mock_credits = MagicMock()
    mock_credits.graph_id = "default"
    mock_credits.current_balance = 1000.0
    mock_credits.credit_multiplier = 1.0
    mock_get_credits.return_value = mock_credits

    # Mock the AgentOrchestrator class instance
    mock_orchestrator_instance = MagicMock()
    mock_agent_response = MagicMock()
    mock_agent_response.content = "Test response for: simple query using graph default"
    mock_agent_response.agent_name = "financial"
    mock_agent_response.mode_used.value = "standard"
    mock_agent_response.metadata = {"analysis_type": "test", "graph_id": "default"}
    mock_agent_response.tokens_used = {"input": 50, "output": 50}
    mock_agent_response.confidence_score = 0.95
    mock_agent_response.error_details = None
    mock_agent_response.execution_time = 1.5

    mock_orchestrator_instance.route_query = AsyncMock(return_value=mock_agent_response)
    mock_orchestrator_class.return_value = mock_orchestrator_instance

    response = client_with_mocked_auth.post(
      "/v1/graphs/default/agent", json={"message": "simple query"}
    )

    assert response.status_code == 200
    data = response.json()
    assert "content" in data
    assert "Test response" in data["content"]


@pytest.mark.asyncio
@pytest.mark.unit
async def test_agent_endpoint_with_history(
  client_with_mocked_auth: TestClient,
):
  """Test the agent endpoint with conversation history."""
  history = [
    AgentMessage(role="user", content="Previous question", timestamp=None),
    AgentMessage(role="assistant", content="Previous answer", timestamp=None),
  ]

  with (
    patch(
      "robosystems.routers.graphs.agent.AgentOrchestrator"
    ) as mock_orchestrator_class,
    patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id"
    ) as mock_get_credits,
  ):
    # Mock GraphCredits to return a credit pool
    mock_credits = MagicMock()
    mock_credits.graph_id = "default"
    mock_credits.current_balance = 1000.0
    mock_credits.credit_multiplier = 1.0
    mock_get_credits.return_value = mock_credits

    # Mock the AgentOrchestrator class instance
    mock_orchestrator_instance = MagicMock()
    mock_agent_response = MagicMock()
    mock_agent_response.content = "Response with history"
    mock_agent_response.agent_name = "financial"
    mock_agent_response.mode_used.value = "standard"
    mock_agent_response.metadata = {"has_history": True, "graph_id": "default"}
    mock_agent_response.tokens_used = {"input": 50, "output": 50}
    mock_agent_response.confidence_score = 0.95
    mock_agent_response.error_details = None
    mock_agent_response.execution_time = 1.5

    mock_orchestrator_instance.route_query = AsyncMock(return_value=mock_agent_response)
    mock_orchestrator_class.return_value = mock_orchestrator_instance

    response = client_with_mocked_auth.post(
      "/v1/graphs/default/agent",
      json={
        "message": "simple query",
        "history": [msg.model_dump() for msg in history],
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert "content" in data
    assert "metadata" in data
    assert data["metadata"]["has_history"] is True


@pytest.mark.asyncio
@pytest.mark.unit
async def test_agent_endpoint_with_context(
  client_with_mocked_auth: TestClient,
):
  """Test the agent endpoint with additional context."""
  with (
    patch(
      "robosystems.routers.graphs.agent.AgentOrchestrator"
    ) as mock_orchestrator_class,
    patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id"
    ) as mock_get_credits,
  ):
    # Mock GraphCredits to return a credit pool
    mock_credits = MagicMock()
    mock_credits.graph_id = "default"
    mock_credits.current_balance = 1000.0
    mock_credits.credit_multiplier = 1.0
    mock_get_credits.return_value = mock_credits

    # Mock the AgentOrchestrator class instance
    mock_orchestrator_instance = MagicMock()
    mock_agent_response = MagicMock()
    mock_agent_response.content = "Response with context"
    mock_agent_response.agent_name = "financial"
    mock_agent_response.mode_used.value = "standard"
    mock_agent_response.metadata = {"has_context": True, "graph_id": "default"}
    mock_agent_response.tokens_used = {"input": 50, "output": 50}
    mock_agent_response.confidence_score = 0.95
    mock_agent_response.error_details = None
    mock_agent_response.execution_time = 1.5

    mock_orchestrator_instance.route_query = AsyncMock(return_value=mock_agent_response)
    mock_orchestrator_class.return_value = mock_orchestrator_instance

    response = client_with_mocked_auth.post(
      "/v1/graphs/default/agent",
      json={
        "message": "simple query",
        "context": {"key": "value"},
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert "content" in data
    assert "metadata" in data
    assert data["metadata"]["has_context"] is True


@pytest.mark.asyncio
@pytest.mark.unit
async def test_agent_error_handling(
  client_with_mocked_auth: TestClient,
):
  """Test agent error handling."""
  with (
    patch(
      "robosystems.routers.graphs.agent.AgentOrchestrator"
    ) as mock_orchestrator_class,
    patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id"
    ) as mock_get_credits,
  ):
    # Mock GraphCredits to return a credit pool
    mock_credits = MagicMock()
    mock_credits.graph_id = "default"
    mock_credits.current_balance = 1000.0
    mock_credits.credit_multiplier = 1.0
    mock_get_credits.return_value = mock_credits

    # Make orchestrator class raise an exception
    mock_orchestrator_class.side_effect = Exception("Test error")

    response = client_with_mocked_auth.post(
      "/v1/graphs/default/agent", json={"message": "simple query"}
    )

    assert response.status_code == 500
    data = response.json()
    assert "detail" in data
    assert "Test error" in data["detail"]


# Note: MCP Client integration tests are in tests/adapters/test_mcp.py


class TestAgentUnauthorizedAccess:
  """Test unauthorized access to agent endpoints."""

  def test_unauthorized_access(self):
    """Test access without authentication."""
    from main import app
    from fastapi.testclient import TestClient

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        # Test POST /v1/default/agent endpoint
        response = test_client.post(
          "/v1/graphs/default/agent", json={"message": "test"}
        )
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides

  def test_invalid_api_key(self):
    """Test access with invalid API key."""
    from main import app
    from fastapi.testclient import TestClient

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        headers = {"Authorization": "Bearer invalid-api-key-12345"}
        response = test_client.post(
          "/v1/graphs/default/agent",
          json={"message": "test"},
          headers=headers,
        )
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides


class TestGraphIdValidation:
  """Test graph_id parameter validation."""

  def test_graph_id_validation_through_path(self, client_with_mocked_auth: TestClient):
    """Test that path parameter validation works."""
    with (
      patch(
        "robosystems.routers.graphs.agent.AgentOrchestrator"
      ) as mock_orchestrator_class,
      patch(
        "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id"
      ) as mock_get_credits,
    ):
      # Mock GraphCredits to return a credit pool
      mock_credits = MagicMock()
      mock_credits.graph_id = "test_graph"
      mock_credits.current_balance = 1000.0
      mock_credits.credit_multiplier = 1.0
      mock_get_credits.return_value = mock_credits

      # Mock the AgentOrchestrator class instance
      mock_orchestrator_instance = MagicMock()
      mock_agent_response = MagicMock()
      mock_agent_response.content = "Test response"
      mock_agent_response.agent_name = "financial"
      mock_agent_response.mode_used.value = "standard"
      mock_agent_response.metadata = {"graph_id": "test_graph"}
      mock_agent_response.tokens_used = {"input": 50, "output": 50}
      mock_agent_response.confidence_score = 0.95
      mock_agent_response.error_details = None
      mock_agent_response.execution_time = 1.5

      mock_orchestrator_instance.route_query = AsyncMock(
        return_value=mock_agent_response
      )
      mock_orchestrator_class.return_value = mock_orchestrator_instance

      # Test with valid graph_id
      response = client_with_mocked_auth.post(
        "/v1/graphs/test_graph/agent", json={"message": "test"}
      )
      assert response.status_code == 200

      # Test with invalid graph_id (too short)
      response = client_with_mocked_auth.post(
        "/v1/graphs/ab/agent", json={"message": "test"}
      )
      assert response.status_code == 422  # FastAPI validation error

      # Test with invalid graph_id (contains invalid character)
      # Note: The @ character gets URL-encoded and the validation happens at dependency level
      try:
        response = client_with_mocked_auth.post(
          "/v1/graphs/test@graph/agent", json={"message": "test"}
        )
        # Should either get 422 from FastAPI or 500 from dependency validation
        assert response.status_code in [422, 500]
      except ValueError:
        # If validation error is raised during test execution, that's also valid
        pass
