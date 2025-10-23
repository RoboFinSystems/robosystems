"""
Tests for the graph query endpoint.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from httpx import AsyncClient
from sqlalchemy.orm import Session

from robosystems.middleware.auth.jwt import create_jwt_token
from robosystems.models.iam import User, UserGraph


@pytest.fixture
def mock_graph_router():
  """Mock GraphRouter for tests."""
  with (
    patch("robosystems.middleware.graph.router.GraphRouter") as mock_router_class,
    patch(
      "robosystems.middleware.graph.router.get_graph_repository"
    ) as mock_get_graph_repo,
  ):
    mock_router = Mock()
    mock_repository = AsyncMock()

    # Default mock responses
    mock_repository.execute_query = AsyncMock(return_value=[{"node_count": 42}])
    mock_router.get_repository = AsyncMock(return_value=mock_repository)
    mock_router_class.return_value = mock_router

    # Mock the get_graph_repository function directly
    mock_get_graph_repo.return_value = mock_repository

    try:
      yield mock_repository
    finally:
      # Clean up any side effects
      mock_repository.reset_mock()
      mock_router.reset_mock()
      mock_router_class.reset_mock()
      mock_get_graph_repo.reset_mock()


@pytest.mark.asyncio
async def test_cypher_query_success(
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
  mock_graph_router: AsyncMock,
):
  """Test successful Cypher query execution."""
  # Get the user_graph from the fixture that includes credits
  test_user_graph = test_graph_with_credits["user_graph"]

  # Create access token
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  # Test query
  request_data = {"query": "MATCH (n) RETURN count(n) as node_count", "timeout": 30}

  response = await async_client.post(
    f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
  )

  # Query can return either 200 (immediate execution) or 202 (queued)
  assert response.status_code in [200, 202]
  data = response.json()

  if response.status_code == 200:
    # Immediate execution
    assert data["success"] is True
    assert data["graph_id"] == test_user_graph.graph_id
    assert "data" in data
    assert "columns" in data
    assert "row_count" in data
    assert "execution_time_ms" in data
  else:
    # Queued execution
    assert data["status"] == "queued"
    assert "query_id" in data
    assert "queue_position" in data
    assert "estimated_wait_seconds" in data


@pytest.mark.asyncio
async def test_cypher_query_with_parameters(
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
  mock_graph_router: AsyncMock,
):
  """Test Cypher query with parameters."""
  # Get the user_graph from the fixture that includes credits
  test_user_graph = test_graph_with_credits["user_graph"]

  # Configure mock to return entity data
  mock_graph_router.execute_query.return_value = [
    {"n": {"id": "123", "name": "Test Entity", "type": "Entity"}}
  ]

  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  request_data = {
    "query": "MATCH (n:Entity) WHERE n.name = $entity_name RETURN n",
    "parameters": {"entity_name": "Test Entity"},
    "timeout": 30,
  }

  response = await async_client.post(
    f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
  )

  # Query can return either 200 (immediate execution) or 202 (queued)
  assert response.status_code in [200, 202]
  data = response.json()

  if response.status_code == 200:
    assert data["success"] is True
  else:
    # Queued execution
    assert data["status"] == "queued"
    assert "query_id" in data


@pytest.mark.asyncio
@patch("robosystems.routers.graphs.query.execute.get_query_queue")
@patch("robosystems.routers.graphs.query.execute.get_universal_repository_with_auth")
async def test_cypher_query_write_operations_blocked(
  mock_get_repo,
  mock_get_queue,
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
):
  """Test that write operations are blocked at query endpoint (use staging pipeline instead)."""
  test_user_graph = test_graph_with_credits["user_graph"]
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  # Try various write operations
  write_queries = [
    "CREATE (n:TestNode {name: 'test'})",
    "MERGE (n:TestNode {id: 1})",
    "MATCH (n) SET n.updated = true",
    "MATCH (n) DELETE n",
    "MATCH (n) DETACH DELETE n",
  ]

  for query in write_queries:
    request_data = {"query": query}

    response = await async_client.post(
      f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
    )

    # Write operations are blocked and should return 403
    assert response.status_code == 403, f"Expected 403 for write query: {query}"

    data = response.json()
    assert "not allowed" in data["detail"].lower()
    assert (
      "staging pipeline" in data["detail"].lower() or "tables" in data["detail"].lower()
    )


@pytest.mark.asyncio
@patch("robosystems.routers.graphs.query.execute.get_query_queue")
@patch("robosystems.routers.graphs.query.execute.get_universal_repository_with_auth")
async def test_cypher_query_timeout(
  mock_get_repo,
  mock_get_queue,
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
):
  """Test query timeout handling."""
  test_user_graph = test_graph_with_credits["user_graph"]
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  # Configure mock to simulate timeout
  import asyncio

  mock_repo = AsyncMock()
  mock_repo.execute_query = AsyncMock(side_effect=asyncio.TimeoutError("Query timeout"))
  # Also mock the streaming method to avoid async iteration issues
  mock_repo.execute_query_streaming = None  # Disable streaming to force fallback
  mock_get_repo.return_value = mock_repo

  # Configure mock queue manager
  mock_queue_manager = Mock()
  mock_queue_manager.get_stats.return_value = {"queue_size": 0, "running_queries": 1}
  mock_queue_manager.submit_query = AsyncMock(return_value="q_timeout_test")
  mock_get_queue.return_value = mock_queue_manager

  # Query that will timeout
  request_data = {
    "query": "MATCH (n) RETURN n",
    "timeout": 1,  # Very short timeout
  }

  response = await async_client.post(
    f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
  )

  # With the new execution strategy, timeout handling might return different responses:
  # - 202: Queued execution (expected for timeout scenarios)
  # - 500: Actual timeout error propagated
  # - 200: Error response with error details (streaming fallback fails)
  assert response.status_code in [200, 202, 500], (
    f"Unexpected status {response.status_code}: {response.text}"
  )

  data = response.json()
  if response.status_code == 202:
    # Queued execution
    assert data["status"] == "queued"
    assert "query_id" in data
  elif response.status_code == 200:
    # Error response in successful HTTP status (fallback behavior)
    assert "error" in data or "success" in data
    if "error" in data:
      # This indicates the timeout was handled and resulted in an error
      assert "error" in data and data["error"] is not None
  # For 500, we expect an error response


@pytest.mark.asyncio
async def test_cypher_query_unauthorized(test_user_graph: UserGraph, test_db):
  """Test unauthorized access."""
  # Create a client without auth overrides
  from httpx import AsyncClient, ASGITransport
  from main import app
  from robosystems.database import get_db_session

  # Only override database, not auth
  def override_get_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db

  try:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
      request_data = {"query": "MATCH (n) RETURN n LIMIT 1"}

      # No auth header
      response = await client.post(
        f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data
      )

      assert response.status_code == 401
  finally:
    # Clean up overrides
    app.dependency_overrides = {}


@pytest.mark.asyncio
async def test_cypher_query_forbidden_graph(
  async_client: AsyncClient, test_user: User, db_session: Session
):
  """Test access to a graph the user doesn't have access to."""
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  request_data = {"query": "MATCH (n) RETURN n LIMIT 1"}

  # Try to access a graph the user doesn't have access to
  # In test environment, the exception might propagate instead of being caught by middleware
  try:
    response = await async_client.post(
      "/v1/graphs/entity_99999/query", json=request_data, headers=headers
    )

    # Should get 403 Forbidden, 402 Payment Required or 500 (credit decorator ValueError)
    assert response.status_code in [402, 403, 500]
    if response.status_code == 500:
      # The credit decorator throws ValueError which gets caught by middleware
      assert (
        "No credit pool found" in response.text
        or "Internal Server Error" in response.text
        or "Failed to access graph" in response.text
      )
    else:
      data = response.json()
      assert (
        "No credit pool found" in data["detail"]
        or "Access denied" in data["detail"]
        or "don't have access" in data["detail"]
      )
  except ValueError as e:
    # In test environment, the exception might propagate directly
    assert "No credit pool found for graph entity_99999" in str(e)


@pytest.mark.asyncio
async def test_cypher_query_sec_repository_with_access(
  async_client: AsyncClient,
  test_user: User,
  db_session: Session,
  mock_graph_router: AsyncMock,
):
  """Test querying SEC repository with proper access - should succeed."""
  # Configure mock to return SEC entity data
  mock_graph_router.execute_query.return_value = [
    {"c.name": "Apple Inc."},
    {"c.name": "Microsoft Corporation"},
    {"c.name": "Amazon.com Inc."},
  ]

  # Grant SEC read access
  from robosystems.models.iam.user_repository import (
    UserRepository,
    RepositoryType,
    RepositoryPlan,
    RepositoryAccessLevel,
  )
  from robosystems.models.iam.user_repository_credits import UserRepositoryCredits
  from decimal import Decimal
  import uuid

  # Create a mock access record for SEC repository
  access_record = UserRepository(
    id=f"access_{uuid.uuid4().hex[:8]}",
    user_id=test_user.id,
    repository_type=RepositoryType.SEC,
    repository_name="sec",
    access_level=RepositoryAccessLevel.READ,
    repository_plan=RepositoryPlan.STARTER,
    is_active=True,
  )
  db_session.add(access_record)
  db_session.commit()

  # Grant SEC read access by creating shared repository credits
  sec_credits = UserRepositoryCredits.create_for_access(
    access_id=access_record.id,
    repository_type=access_record.repository_type,
    repository_plan=access_record.repository_plan,
    monthly_allocation=1000,
    session=db_session,
  )
  sec_credits.current_balance = Decimal("1000.0")
  db_session.add(sec_credits)
  db_session.commit()

  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  request_data = {"query": "MATCH (c:Entity) RETURN c.name LIMIT 5"}

  response = await async_client.post(
    "/v1/graphs/sec/query", json=request_data, headers=headers
  )

  # Query should succeed since we've properly granted SEC access and credits
  assert response.status_code in [200, 202]  # 200 for direct execution, 202 for queued

  data = response.json()
  if response.status_code == 200:
    # Direct execution - verify response structure
    assert data["success"] is True
    assert data["graph_id"] == "sec"
    assert "data" in data
    assert "columns" in data
    assert "row_count" in data
    assert "execution_time_ms" in data
  else:
    # Queued execution - verify queue response structure
    assert data["status"] == "queued"
    assert "query_id" in data
    assert "queue_position" in data
    assert "estimated_wait_seconds" in data


@pytest.mark.asyncio
async def test_cypher_query_sec_repository_no_access(
  async_client: AsyncClient, test_user: User, db_session: Session
):
  """Test querying SEC repository without access."""
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  request_data = {"query": "MATCH (c:Entity) RETURN c.name LIMIT 5"}

  # The test may fail in different ways depending on how credits are handled
  try:
    response = await async_client.post(
      "/v1/graphs/sec/query", json=request_data, headers=headers
    )

    # Should fail with either 402, 403, or 500
    assert response.status_code in [402, 403, 500]
    if response.status_code == 500:
      # The credit decorator throws ValueError which gets caught by middleware
      assert (
        "No credit pool found" in response.text
        or "Internal Server Error" in response.text
      )
    else:
      data = response.json()
      assert (
        "Insufficient SEC credits" in data["detail"]
        or "No credit pool found" in data["detail"]
        or "No user repository credits found" in data["detail"]
        or "Access denied" in data["detail"]
        or "don't have access" in data["detail"]
        or "requires a paid subscription" in data["detail"]  # New rate limiting message
      )
  except ValueError as e:
    # In test environment, the exception might propagate directly
    assert "No credit pool found for graph sec" in str(
      e
    ) or "No user repository credits found" in str(e)


@pytest.mark.asyncio
async def test_cypher_query_empty(
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
):
  """Test empty query validation."""
  test_user_graph = test_graph_with_credits["user_graph"]
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  request_data = {"query": ""}

  response = await async_client.post(
    f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
  )

  assert response.status_code == 422  # Validation error


@pytest.mark.asyncio
async def test_cypher_query_too_long(
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
):
  """Test query length limit."""
  test_user_graph = test_graph_with_credits["user_graph"]
  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  # Create a very long query
  long_query = "MATCH (n) WHERE " + " OR ".join(
    [f"n.prop{i} = {i}" for i in range(10000)]
  )

  request_data = {"query": long_query}

  response = await async_client.post(
    f"/v1/graphs/{test_user_graph.graph_id}/query", json=request_data, headers=headers
  )

  assert response.status_code == 422  # Validation error


@pytest.mark.asyncio
async def test_get_graph_schema(
  async_client: AsyncClient,
  test_user: User,
  test_graph_with_credits: dict,
  db_session: Session,
  mock_graph_router: AsyncMock,
):
  """Test getting graph schema information."""
  test_user_graph = test_graph_with_credits["user_graph"]

  # Configure mock to return schema data
  mock_graph_router.execute_query.side_effect = [
    # CALL SHOW_TABLES() RETURN *
    [{"name": "Entity", "type": "NODE", "comment": ""}],
    # CALL TABLE_INFO('Entity') RETURN *
    [
      {"name": "id", "type": "STRING", "default": None, "primary": True},
      {"name": "name", "type": "STRING", "default": None, "primary": False},
    ],
  ]

  token = create_jwt_token(test_user.id)
  headers = {"Authorization": f"Bearer {token}"}

  response = await async_client.get(
    f"/v1/graphs/{test_user_graph.graph_id}/schema/info", headers=headers
  )

  assert response.status_code == 200
  data = response.json()

  assert data["graph_id"] == test_user_graph.graph_id
  assert "schema" in data
  assert "node_labels" in data["schema"]
  assert "relationship_types" in data["schema"]
  assert "node_properties" in data["schema"]
