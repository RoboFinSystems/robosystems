from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTier
from robosystems.models.api.graphs.operator import OperatorMessage
from robosystems.models.core import Graph, GraphCredits, User

EXECUTE = "robosystems.routers.graphs.operator.execute"


@pytest.fixture(autouse=True)
def _bypass_graph_lifecycle_gate():
  """The operator router runs `require_graph_access` (lifecycle/subscription)
  before dispatch; these tests use synthetic graph ids with no Graph row, so
  the gate would 404 before reaching the handler logic under test. Its own
  behavior is covered in tests/middleware/billing/test_enforcement.py and the
  wiring in test_operator_router.py::TestOperatorLifecycleGate."""
  with patch(
    "robosystems.middleware.billing.enforcement.require_graph_access",
    return_value=None,
  ):
    yield


@pytest.fixture
def queued_worker():
  """The worker seam: gates pass and the enqueue returns a 202 envelope."""

  def _envelope(task_type, graph_id, user_id, params):
    return {
      "operation_id": "op_01SUBGRAPHTEST0000000000000",
      "status": "pending",
      "operation_type": task_type,
      "graph_id": graph_id,
      "_links": {},
    }

  with (
    patch(f"{EXECUTE}.enqueue_task", new=AsyncMock(side_effect=_envelope)) as enqueue,
    patch(f"{EXECUTE}.enforce_operator_write_role"),
    patch(f"{EXECUTE}.enforce_operator_graph_scope"),
    patch(f"{EXECUTE}.enforce_operator_credits"),
  ):
    yield enqueue


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


class TestOperatorSubgraphIntegration:
  """Operator runs on subgraph ids are accepted and queued against that id."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_agent_execute_with_subgraph_accepts_subgraph_id(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
    queued_worker: AsyncMock,
  ):
    parent_graph, _, _ = parent_graph_with_credits
    subgraph_id = f"{parent_graph.graph_id}_dev"

    response = client_with_mocked_auth.post(
      f"/v1/graphs/{subgraph_id}/operator",
      json={"message": "Test query on subgraph"},
    )

    assert response.status_code == 202
    assert response.json()["graph_id"] == subgraph_id
    assert queued_worker.call_args[0][1] == subgraph_id

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_multiple_subgraphs_both_accepted(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
    queued_worker: AsyncMock,
  ):
    parent_graph, _, _ = parent_graph_with_credits
    subgraph_dev = f"{parent_graph.graph_id}_dev"
    subgraph_prod = f"{parent_graph.graph_id}_prod"

    response1 = client_with_mocked_auth.post(
      f"/v1/graphs/{subgraph_dev}/operator",
      json={"message": "Query on dev subgraph"},
    )
    response2 = client_with_mocked_auth.post(
      f"/v1/graphs/{subgraph_prod}/operator",
      json={"message": "Query on prod subgraph"},
    )

    assert response1.status_code == 202
    assert response2.status_code == 202
    queued_ids = [call[0][1] for call in queued_worker.call_args_list]
    assert queued_ids == [subgraph_dev, subgraph_prod]

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_subgraph_agent_with_conversation_history(
    self,
    client_with_mocked_auth: TestClient,
    parent_graph_with_credits: tuple[Graph, GraphCredits, User],
    queued_worker: AsyncMock,
  ):
    parent_graph, _, _ = parent_graph_with_credits
    subgraph_id = f"{parent_graph.graph_id}_test"

    history = [
      OperatorMessage(role="user", content="Previous question", timestamp=None),
      OperatorMessage(role="assistant", content="Previous answer", timestamp=None),
    ]

    response = client_with_mocked_auth.post(
      f"/v1/graphs/{subgraph_id}/operator",
      json={
        "message": "Follow-up question",
        "history": [h.model_dump() for h in history],
      },
    )

    assert response.status_code == 202
    params = queued_worker.call_args[0][3]
    assert len(params["history"]) == 2
    assert params["history"][0] == {"role": "user", "content": "Previous question"}
