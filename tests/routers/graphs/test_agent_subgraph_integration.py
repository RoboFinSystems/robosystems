from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTier
from robosystems.models.api.graphs.agent import AgentMessage
from robosystems.models.iam import Graph, GraphCredits, User


@pytest.fixture
def parent_graph_with_credits(db_session: Session) -> tuple[Graph, GraphCredits, User]:
  import uuid

  from robosystems.utils.ulid import generate_prefixed_ulid

  user = User(
    id=generate_prefixed_ulid("user"),
    email=f"test_{uuid.uuid4().hex[:8]}@example.com",
    name="Test User",
    password_hash="hashed_password",
    is_active=True,
    email_verified=True,
  )
  db_session.add(user)

  graph_id = f"kg{uuid.uuid4().hex[:16]}"
  graph = Graph(
    graph_id=graph_id,
    graph_name="Test Parent Graph",
    graph_type="generic",
    graph_tier=GraphTier.LADYBUG_STANDARD.value,
  )
  db_session.add(graph)

  credits = GraphCredits(
    graph_id=graph.graph_id,
    user_id=user.id,
    billing_admin_id=user.id,
    monthly_allocation=Decimal("10000"),
    current_balance=Decimal("10000"),
  )
  db_session.add(credits)
  db_session.commit()

  return graph, credits, user


class TestAgentSubgraphIntegration:
  """Integration tests for agent execution with subgraph IDs"""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_agent_execute_with_subgraph_accepts_subgraph_id(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
    db_session: Session,
  ):
    """Agent endpoint accepts subgraph IDs"""
    parent_graph, parent_credits, user = parent_graph_with_credits
    subgraph_id = f"{parent_graph.graph_id}_dev"

    with patch(
      "robosystems.routers.graphs.agent.handlers.AgentOrchestrator"
    ) as mock_orchestrator_class:
      mock_orchestrator_instance = MagicMock()
      mock_agent_response = MagicMock()
      mock_agent_response.content = "Test response from agent on subgraph"
      mock_agent_response.agent_name = "financial"
      mock_agent_response.mode_used.value = "standard"
      mock_agent_response.metadata = {"graph_id": subgraph_id}
      mock_agent_response.tokens_used = {"input": 100, "output": 50}
      mock_agent_response.confidence_score = 0.95
      mock_agent_response.error_details = None
      mock_agent_response.execution_time = 1.5

      mock_orchestrator_instance.route_query = AsyncMock(
        return_value=mock_agent_response
      )
      mock_orchestrator_class.return_value = mock_orchestrator_instance

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{subgraph_id}/agent",
        json={"message": "Test query on subgraph"},
      )

      assert response.status_code == 200
      data = response.json()
      assert "content" in data

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_multiple_subgraphs_both_accepted(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
    db_session: Session,
  ):
    """Multiple subgraphs can all execute agent queries"""
    parent_graph, parent_credits, user = parent_graph_with_credits
    subgraph_dev = f"{parent_graph.graph_id}_dev"
    subgraph_prod = f"{parent_graph.graph_id}_prod"

    with patch(
      "robosystems.routers.graphs.agent.handlers.AgentOrchestrator"
    ) as mock_orchestrator_class:
      mock_orchestrator_instance = MagicMock()
      mock_agent_response = MagicMock()
      mock_agent_response.content = "Response"
      mock_agent_response.agent_name = "financial"
      mock_agent_response.mode_used.value = "standard"
      mock_agent_response.metadata = {}
      mock_agent_response.tokens_used = {"input": 50, "output": 25}
      mock_agent_response.confidence_score = 0.95
      mock_agent_response.error_details = None
      mock_agent_response.execution_time = 1.0

      mock_orchestrator_instance.route_query = AsyncMock(
        return_value=mock_agent_response
      )
      mock_orchestrator_class.return_value = mock_orchestrator_instance

      response1 = client_with_mocked_auth.post(
        f"/v1/graphs/{subgraph_dev}/agent",
        json={"message": "Query on dev subgraph"},
      )

      response2 = client_with_mocked_auth.post(
        f"/v1/graphs/{subgraph_prod}/agent",
        json={"message": "Query on prod subgraph"},
      )

      assert response1.status_code == 200
      assert response2.status_code == 200

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_subgraph_agent_with_conversation_history(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
  ):
    """Agent should handle conversation history on subgraphs"""
    parent_graph, parent_credits, user = parent_graph_with_credits
    subgraph_id = f"{parent_graph.graph_id}_test"

    history = [
      AgentMessage(role="user", content="Previous question", timestamp=None),
      AgentMessage(role="assistant", content="Previous answer", timestamp=None),
    ]

    with patch(
      "robosystems.routers.graphs.agent.handlers.AgentOrchestrator"
    ) as mock_orchestrator_class:
      mock_orchestrator_instance = MagicMock()
      mock_agent_response = MagicMock()
      mock_agent_response.content = "Response with context from history"
      mock_agent_response.agent_name = "financial"
      mock_agent_response.mode_used.value = "standard"
      mock_agent_response.metadata = {"has_history": True}
      mock_agent_response.tokens_used = {"input": 150, "output": 75}
      mock_agent_response.confidence_score = 0.9
      mock_agent_response.error_details = None
      mock_agent_response.execution_time = 2.0

      mock_orchestrator_instance.route_query = AsyncMock(
        return_value=mock_agent_response
      )
      mock_orchestrator_class.return_value = mock_orchestrator_instance

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{subgraph_id}/agent",
        json={
          "message": "Follow-up question",
          "history": [h.model_dump() for h in history],
        },
      )

      assert response.status_code == 200
      data = response.json()
      assert "content" in data

      orchestrator_call = mock_orchestrator_instance.route_query.call_args
      assert orchestrator_call is not None
