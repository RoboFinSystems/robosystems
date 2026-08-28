"""Operator router tests.

Every operator run executes on the background worker: the endpoints gate the
request, enqueue it, and answer 202 with the operation links — or 200 with the
result under a bounded `?mode=sync` wait. These tests pin that contract with
the queue, the gates and the operation store mocked at the router's seam.
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from robosystems.middleware.sse.event_storage import OperationMetadata, OperationStatus
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.credit_preflight import (
  InsufficientOperatorCreditsError,
)
from tests.conftest import VALID_TEST_GRAPH_ID

EXECUTE = "robosystems.routers.graphs.operator.execute"
OPERATION_ID = "op_01TESTQUEUED00000000000000"


def _queued_envelope(operation_id: str = OPERATION_ID) -> dict:
  return {
    "operation_id": operation_id,
    "status": "pending",
    "operation_type": "operator",
    "graph_id": VALID_TEST_GRAPH_ID,
    "_links": {
      "stream": f"/v1/operations/{operation_id}/stream",
      "status": f"/v1/operations/{operation_id}/status",
      "cancel": f"/v1/operations/{operation_id}",
    },
    "message": "Operation operator queued.",
  }


def _metadata(
  status: OperationStatus,
  result_data: dict | None = None,
  error_message: str | None = None,
) -> OperationMetadata:
  return OperationMetadata(
    operation_id=OPERATION_ID,
    operation_type="operator",
    user_id="test-user-id",
    graph_id=VALID_TEST_GRAPH_ID,
    status=status,
    created_at="2026-01-01T00:00:00Z",
    updated_at="2026-01-01T00:00:10Z",
    error_message=error_message,
    result_data=result_data,
  )


COMPLETED_RESULT = {
  "content": "Revenue was 1.2M",
  "operator_used": "Cypher Operator",
  "mode_used": "standard",
  "metadata": {"credits_consumed": 116.7, "rows": [{"total": 1200000}]},
  "tokens_used": {"input": 4, "output": 600, "cache_read": 22881, "cache_write": 0},
  "confidence_score": 0.9,
  "execution_time": 14.1,
}


@pytest.fixture(autouse=True)
def _bypass_graph_lifecycle_gate():
  """The operator router runs `require_graph_access` (lifecycle/subscription)
  before dispatch; these tests use synthetic graph ids with no Graph row, so
  the gate would 404 before reaching the handler logic under test. Its own
  behavior is covered in tests/middleware/billing/test_enforcement.py and the
  wiring in TestOperatorLifecycleGate."""
  with patch(
    "robosystems.middleware.billing.enforcement.require_graph_access",
    return_value=None,
  ):
    yield


@pytest.fixture
def mock_registry():
  """Registry metadata plus a lightweight operator for `get_operator`."""
  operators_metadata = {
    "financial": {
      "name": "Financial Operator",
      "description": "Financial analysis",
      "version": "1.0.0",
      "capabilities": ["financial_analysis"],
      "supported_modes": ["standard", "extended"],
      "requires_credits": True,
    },
    "research": {
      "name": "Research Operator",
      "description": "Deep research",
      "version": "1.0.0",
      "capabilities": ["deep_research"],
      "supported_modes": ["standard", "extended"],
      "requires_credits": True,
    },
  }

  mock_operator = Mock()
  mock_operator.spec = Mock()
  mock_operator.spec.name = "Research Operator"

  with (
    patch(f"{EXECUTE}.list_operators", return_value=operators_metadata),
    patch(f"{EXECUTE}.get_operator", return_value=mock_operator) as get_op,
  ):
    yield get_op


@pytest.fixture
def mock_recommendations():
  """The auto endpoint's operator choice comes from the registry ranking."""
  with patch(f"{EXECUTE}.OperatorOrchestrator") as orchestrator_cls:
    orchestrator = Mock()
    orchestrator.get_operator_recommendations = Mock(
      return_value=[
        {"operator_type": "financial", "confidence": 0.9},
        {"operator_type": "research", "confidence": 0.6},
      ]
    )
    orchestrator_cls.return_value = orchestrator
    yield orchestrator


@pytest.fixture
def mock_gates():
  """Write-role, graph-scope and credit gates pass unless a test says otherwise."""
  with (
    patch(f"{EXECUTE}.enforce_operator_write_role") as write_role,
    patch(f"{EXECUTE}.enforce_operator_graph_scope") as scope,
    patch(f"{EXECUTE}.enforce_operator_credits") as credits,
  ):
    yield {"write_role": write_role, "scope": scope, "credits": credits}


@pytest.fixture
def mock_enqueue():
  with patch(
    f"{EXECUTE}.enqueue_task", new=AsyncMock(return_value=_queued_envelope())
  ) as enqueue:
    yield enqueue


@pytest.fixture
def client(mock_registry, mock_recommendations, mock_gates, mock_enqueue):
  """Test client with auth, rate limits and the DB session overridden."""
  from main import app
  from robosystems.database import get_db_session
  from robosystems.middleware.auth.dependencies import get_current_user_with_graph
  from robosystems.middleware.rate_limits import (
    graph_scoped_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
  )

  mock_user = Mock()
  mock_user.id = "test-user-id"
  mock_user.name = "Test User"
  mock_user.email = "test@example.com"
  mock_user.accounts = []

  app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
  app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
  app.dependency_overrides[get_db_session] = lambda: Mock()

  yield TestClient(app)

  app.dependency_overrides = {}


class TestQueuedExecution:
  """The default answer is 202 with the operation's links."""

  def test_auto_endpoint_queues_the_ranked_operator(self, client, mock_enqueue):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={"message": "Analyze financial data", "context": {"key": "value"}},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["operation_id"] == OPERATION_ID
    assert data["status"] == "pending"
    assert data["_links"]["stream"] == f"/v1/operations/{OPERATION_ID}/stream"

    mock_enqueue.assert_awaited_once()
    task_type, graph_id, user_id, params = mock_enqueue.call_args[0]
    assert (task_type, graph_id, user_id) == (
      "operator",
      VALID_TEST_GRAPH_ID,
      "test-user-id",
    )
    assert params["operator_type"] == "financial"
    assert params["query"] == "Analyze financial data"
    assert params["mode"] == "standard"
    assert params["history"] == []
    assert params["context"] == {"key": "value"}

  def test_specific_endpoint_queues_the_named_operator(self, client, mock_enqueue):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research",
      json={"message": "Research this topic", "mode": "extended"},
    )

    assert response.status_code == 202
    params = mock_enqueue.call_args[0][3]
    assert params["operator_type"] == "research"
    assert params["mode"] == "extended"

  def test_history_and_credit_ceiling_travel_with_the_task(self, client, mock_enqueue):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={
        "message": "Follow-up question",
        "history": [
          {"role": "user", "content": "Initial question"},
          {"role": "assistant", "content": "Initial answer"},
        ],
        "context": {"enable_rag": False},
        "max_credits": 150,
      },
    )

    assert response.status_code == 202
    params = mock_enqueue.call_args[0][3]
    assert params["history"] == [
      {"role": "user", "content": "Initial question"},
      {"role": "assistant", "content": "Initial answer"},
    ]
    assert params["context"] == {"enable_rag": False, "max_credits": 150}

  def test_force_extended_analysis_queues_extended_mode(self, client, mock_enqueue):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={"message": "Simple query", "force_extended_analysis": True},
    )

    assert response.status_code == 202
    assert mock_enqueue.call_args[0][3]["mode"] == "extended"

  def test_async_and_stream_modes_also_queue(self, client, mock_enqueue):
    for mode in ("async", "stream", "auto"):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator?mode={mode}",
        json={"message": "Test query"},
      )
      assert response.status_code == 202, mode

  def test_gates_run_before_the_enqueue(self, client, mock_enqueue, mock_gates):
    client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research",
      json={"message": "Test query"},
    )

    mock_gates["write_role"].assert_called_once()
    mock_gates["scope"].assert_called_once()
    mock_gates["credits"].assert_called_once()
    assert mock_gates["credits"].call_args[0][4] == OperatorMode.STANDARD

  def test_insufficient_credits_is_402_and_nothing_is_queued(
    self, client, mock_enqueue, mock_gates
  ):
    mock_gates["credits"].side_effect = InsufficientOperatorCreditsError(
      operator_name="Research Operator",
      estimated_credits=120.0,
      available_credits=12.0,
    )

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research",
      json={"message": "Test query"},
    )

    assert response.status_code == 402
    detail = response.json()["detail"]
    assert detail["code"] == "INSUFFICIENT_CREDITS"
    assert detail["required_credits"] == 120.0
    assert detail["available_credits"] == 12.0
    mock_enqueue.assert_not_awaited()

  def test_unknown_operator_type_is_404(self, client, mock_registry, mock_enqueue):
    mock_registry.side_effect = KeyError("nope")

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/nope",
      json={"message": "Test query"},
    )

    assert response.status_code == 404
    mock_enqueue.assert_not_awaited()

  def test_no_recommendation_is_404(self, client, mock_recommendations, mock_enqueue):
    mock_recommendations.get_operator_recommendations.return_value = []

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={"message": "Test query"},
    )

    assert response.status_code == 404
    mock_enqueue.assert_not_awaited()

  def test_selection_criteria_exclude_and_threshold(self, client, mock_enqueue):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={
        "message": "Complex analysis",
        "selection_criteria": {
          "min_confidence": 0.5,
          "excluded_operators": ["financial"],
        },
      },
    )

    assert response.status_code == 202
    assert mock_enqueue.call_args[0][3]["operator_type"] == "research"

  def test_queue_failure_is_a_500(self, client, mock_enqueue):
    mock_enqueue.side_effect = RuntimeError("valkey down")

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={"message": "Test query"},
    )

    assert response.status_code == 500
    assert "internal error" in response.json()["detail"].lower()


class TestSyncWait:
  """`?mode=sync` waits for the worker and answers 200 when it lands in time."""

  def test_completed_run_answers_200_with_the_result(self, client):
    with patch(
      f"{EXECUTE}._wait_for_operation",
      new=AsyncMock(
        return_value=_metadata(OperationStatus.COMPLETED, COMPLETED_RESULT)
      ),
    ) as wait:
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    wait.assert_awaited_once_with(OPERATION_ID)
    assert response.status_code == 200
    data = response.json()
    assert data["content"] == "Revenue was 1.2M"
    assert data["operator_used"] == "Cypher Operator"
    assert data["mode_used"] == "standard"
    assert data["metadata"]["credits_consumed"] == 116.7
    assert data["tokens_used"]["cache_read"] == 22881
    assert data["confidence_score"] == 0.9
    assert data["operation_id"] == OPERATION_ID
    assert data["error_details"] is None

  def test_still_running_at_the_deadline_answers_202(self, client):
    with patch(
      f"{EXECUTE}._wait_for_operation",
      new=AsyncMock(return_value=_metadata(OperationStatus.RUNNING)),
    ):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    assert response.status_code == 202
    assert response.json()["operation_id"] == OPERATION_ID

  def test_paused_run_answers_202_so_the_caller_can_resume(self, client):
    with patch(
      f"{EXECUTE}._wait_for_operation",
      new=AsyncMock(return_value=_metadata(OperationStatus.AWAITING_INPUT)),
    ):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    assert response.status_code == 202

  def test_failed_run_answers_200_with_error_details(self, client):
    with patch(
      f"{EXECUTE}._wait_for_operation",
      new=AsyncMock(
        return_value=_metadata(
          OperationStatus.FAILED, error_message="Operation failed — reference op_x"
        )
      ),
    ):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    assert response.status_code == 200
    data = response.json()
    assert data["error_details"]["code"] == "OPERATOR_FAILED"
    assert data["content"] == "Operation failed — reference op_x"
    assert data["operator_used"] == "research"

  def test_cancelled_run_answers_200_with_error_details(self, client):
    with patch(
      f"{EXECUTE}._wait_for_operation",
      new=AsyncMock(return_value=_metadata(OperationStatus.CANCELLED)),
    ):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    assert response.status_code == 200
    assert response.json()["error_details"]["code"] == "OPERATOR_CANCELLED"

  def test_expired_operation_answers_202(self, client):
    with patch(f"{EXECUTE}._wait_for_operation", new=AsyncMock(return_value=None)):
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/research?mode=sync",
        json={"message": "Test query"},
      )

    assert response.status_code == 202


class TestWaitForOperation:
  @pytest.mark.asyncio
  async def test_returns_as_soon_as_the_run_settles(self):
    from robosystems.routers.graphs.operator.execute import _wait_for_operation

    storage = AsyncMock()
    storage.get_operation_metadata.side_effect = [
      _metadata(OperationStatus.PENDING),
      _metadata(OperationStatus.RUNNING),
      _metadata(OperationStatus.COMPLETED, COMPLETED_RESULT),
    ]

    with (
      patch(f"{EXECUTE}.get_event_storage", return_value=storage),
      patch(f"{EXECUTE}.SYNC_POLL_INTERVAL_SECONDS", 0),
      patch(f"{EXECUTE}.SYNC_WAIT_SECONDS", 5),
    ):
      metadata = await _wait_for_operation(OPERATION_ID)

    assert metadata is not None
    assert metadata.status == OperationStatus.COMPLETED
    assert storage.get_operation_metadata.await_count == 3

  @pytest.mark.asyncio
  async def test_stops_at_the_deadline_with_the_last_status(self):
    from robosystems.routers.graphs.operator.execute import _wait_for_operation

    storage = AsyncMock()
    storage.get_operation_metadata.return_value = _metadata(OperationStatus.RUNNING)

    with (
      patch(f"{EXECUTE}.get_event_storage", return_value=storage),
      patch(f"{EXECUTE}.SYNC_POLL_INTERVAL_SECONDS", 0),
      patch(f"{EXECUTE}.SYNC_WAIT_SECONDS", 0),
    ):
      metadata = await _wait_for_operation(OPERATION_ID)

    assert metadata is not None
    assert metadata.status == OperationStatus.RUNNING

  def test_sync_wait_stays_under_the_load_balancer_idle_timeout(self):
    from robosystems.routers.graphs.operator.execute import SYNC_WAIT_SECONDS

    assert SYNC_WAIT_SECONDS < 60


class TestReadEndpoints:
  def test_list_operators(self, client):
    response = client.get(f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator")

    assert response.status_code == 200
    data = response.json()
    assert "financial" in data["operators"]
    assert data["operators"]["financial"]["name"] == "Financial Operator"

  def test_list_operators_filters_by_capability(self, client):
    response = client.get(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator?capability=financial_analysis"
    )

    assert response.status_code == 200
    data = response.json()
    assert "financial" in data["operators"]
    assert "research" not in data["operators"]

  def test_operator_metadata(self, client):
    response = client.get(f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator/financial")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Financial Operator"
    assert "financial_analysis" in data["capabilities"]

  def test_openapi_documents_the_operator_endpoints(self, client):
    response = client.get("/openapi.json")

    assert response.status_code == 200
    openapi = response.json()
    paths = openapi["paths"]
    assert "/v1/graphs/{graph_id}/operator" in paths
    assert "/v1/graphs/{graph_id}/operator/{operator_type}" in paths
    assert "202" in paths["/v1/graphs/{graph_id}/operator"]["post"]["responses"]
    components = openapi["components"]["schemas"]
    assert "OperatorRequest" in components
    assert "OperatorResponse" in components


class TestValidation:
  def test_invalid_mode_is_422(self, client):
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
      json={"message": "Test", "mode": "invalid_mode"},
    )

    assert response.status_code == 422

  def test_invalid_graph_id_is_422(self, client):
    response = client.post(
      "/v1/graphs/invalid-graph!/operator",
      json={"message": "Test"},
    )

    assert response.status_code == 422

  def test_short_graph_id_is_422(self, client):
    response = client.post("/v1/graphs/ab/operator", json={"message": "test"})

    assert response.status_code == 422


class TestAuthentication:
  def test_authentication_required(self):
    from main import app

    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()
    try:
      with TestClient(app) as test_client:
        response = test_client.post(
          f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
          json={"message": "Test query"},
        )
      assert response.status_code == 401
    finally:
      app.dependency_overrides = original_overrides

  def test_invalid_api_key_is_401(self):
    from main import app

    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()
    try:
      with TestClient(app) as test_client:
        response = test_client.post(
          f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operator",
          json={"message": "test"},
          headers={"Authorization": "Bearer invalid-api-key-12345"},
        )
      assert response.status_code == 401
    finally:
      app.dependency_overrides = original_overrides


class TestOperatorLifecycleGate:
  """The operator surface runs the graph lifecycle/subscription gate.

  A suspended, mid-teardown or expired graph accepted operator runs on no
  other surface's say-so — the gate lives in the shared pre-dispatch helper
  so both operator entry points inherit it.
  """

  @pytest.mark.asyncio
  async def test_lifecycle_gate_runs_before_repository_limits(self):
    from fastapi import HTTPException

    from robosystems.routers.graphs.operator.execute import (
      _enforce_shared_repository_agent_limits,
    )

    user = MagicMock()
    db = MagicMock()

    with (
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access",
        side_effect=HTTPException(status_code=403, detail="Subscription has ended."),
      ) as gate,
      patch(
        "robosystems.routers.graphs.query.execute._check_shared_repository_limits",
        new=AsyncMock(),
      ) as mock_check,
    ):
      with pytest.raises(HTTPException) as exc:
        await _enforce_shared_repository_agent_limits("kg0123456789abcdef", user, db)

    assert exc.value.status_code == 403
    gate.assert_called_once_with("kg0123456789abcdef", db, require_write=False)
    mock_check.assert_not_awaited()


class TestOperatorSharedRepositoryLimits:
  """The operator surface must enforce the manifest's agent_calls_* limits.

  Repository plans have always advertised agent volume limits, but only
  query/mcp/search called the volume limiter — nothing ever passed
  operation="agent", so the advertised numbers were unenforced.
  """

  @pytest.mark.asyncio
  async def test_agent_limits_delegate_with_agent_operation(self):
    from robosystems.routers.graphs.operator.execute import (
      _enforce_shared_repository_agent_limits,
    )

    user = MagicMock()
    db = MagicMock()

    with patch(
      "robosystems.routers.graphs.query.execute._check_shared_repository_limits",
      new=AsyncMock(),
    ) as mock_check:
      await _enforce_shared_repository_agent_limits("sec", user, db)

    mock_check.assert_awaited_once_with(
      "sec", user, db, endpoint="agent", operation="agent"
    )
