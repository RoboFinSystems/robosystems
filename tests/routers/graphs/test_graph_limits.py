"""Integration tests for graph creation limits through API endpoints."""

from unittest.mock import Mock, patch

import pytest
from fastapi import status
from httpx import AsyncClient

from robosystems.config import env  # Import to enable patching
from robosystems.models.core import OrgLimits


@pytest.mark.asyncio
class TestGraphCreationLimits:
  """Test graph creation with various limit scenarios."""

  @pytest.fixture
  def auth_headers(self):
    """Mock authentication headers."""
    return {"Authorization": "Bearer test_token"}

  @pytest.fixture
  def graph_request_data(self):
    """Standard graph creation request."""
    return {
      "metadata": {"graph_name": "Test Graph", "description": "Testing graph limits"},
      "instance_tier": "ladybug-standard",
      "initial_entity": {"name": "Test Corp", "uri": "https://testcorp.com"},
    }

  async def test_create_graph_with_zero_limit_blocked(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that orgs with limit=0 get 403 error when creating graphs."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(OrgLimits, "get_or_create_for_org") as mock_get_limits:
          # Create mock limits with zero max
          mock_limits = Mock()
          mock_limits.max_graph_users = 0
          mock_limits.can_create_graph.return_value = (
            False,
            "Safety limit reached (0 graphs). Please contact support if you need more.",
          )
          mock_get_limits.return_value = mock_limits

          mock_db = Mock()
          mock_get_db.return_value = iter([mock_db])

          # Mock OrgUser.get_user_orgs to return a list with an org
          mock_org_user = Mock()
          mock_org_user.org_id = "test-org-123"
          mock_get_user_orgs.return_value = [mock_org_user]

          # Attempt to create graph
          response = await async_client.post("/v1/graphs", json=graph_request_data)

          # Assert 403 Forbidden
          assert response.status_code == status.HTTP_403_FORBIDDEN
          response_data = response.json()
          assert response_data["detail"]["error"]["code"] == "graph_limit_reached"
          assert (
            "Safety limit reached (0 graphs)"
            in response_data["detail"]["error"]["message"]
          )
          assert (
            "contact support" in response_data["detail"]["error"]["message"].lower()
          )

  async def test_create_graph_at_limit_blocked(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that orgs at their limit cannot create more graphs."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(OrgLimits, "get_or_create_for_org") as mock_get_limits:
          # Create mock limits at maximum
          mock_limits = Mock()
          mock_limits.max_graph_users = 5
          mock_limits.can_create_graph.return_value = (
            False,
            "Safety limit reached (5 graphs). Please contact support if you need more.",
          )
          mock_get_limits.return_value = mock_limits

          mock_db = Mock()
          mock_get_db.return_value = iter([mock_db])

          # Mock OrgUser.get_user_orgs to return a list with an org
          mock_org_user = Mock()
          mock_org_user.org_id = "test-org-123"
          mock_get_user_orgs.return_value = [mock_org_user]

          # Attempt to create graph
          response = await async_client.post("/v1/graphs", json=graph_request_data)

          # Assert 403 Forbidden
          assert response.status_code == status.HTTP_403_FORBIDDEN
          response_data = response.json()
          assert response_data["detail"]["error"]["code"] == "graph_limit_reached"
          assert (
            "Safety limit reached (5 graphs)"
            in response_data["detail"]["error"]["message"]
          )

  # TODO: This test needs to be updated to handle SSE responses
  # async def test_create_graph_below_limit_allowed(
  #   self, async_client: AsyncClient, graph_request_data
  # ):
  #   """Test that users below their limit can create graphs successfully."""
  #   # This test needs to be rewritten to handle the new SSE response format
  #   pass

  async def test_create_graph_with_custom_limit_from_env(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that ORG_GRAPHS_DEFAULT_LIMIT environment variable is respected."""
    # Test with environment variable set to 0
    with patch.object(env, "ORG_GRAPHS_DEFAULT_LIMIT", 0):
      with patch("robosystems.database.get_db_session") as mock_get_db:
        with patch(
          "robosystems.models.core.OrgUser.get_user_orgs"
        ) as mock_get_user_orgs:
          with patch.object(OrgLimits, "get_or_create_for_org") as mock_get_limits:
            # Mock creating new org with env default
            mock_limits = Mock()
            mock_limits.max_graph_users = 0  # From env
            mock_limits.can_create_graph.return_value = (
              False,
              "Safety limit reached (0 graphs). Please contact support if you need more.",
            )
            mock_get_limits.return_value = mock_limits

            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])

            # Mock OrgUser.get_user_orgs to return a list with an org
            mock_org_user = Mock()
            mock_org_user.org_id = "test-org-123"
            mock_get_user_orgs.return_value = [mock_org_user]

            response = await async_client.post("/v1/graphs", json=graph_request_data)

            assert response.status_code == status.HTTP_403_FORBIDDEN
            response_data = response.json()
            assert (
              "Safety limit reached (0 graphs)"
              in response_data["detail"]["error"]["message"]
            )

  async def test_graph_limit_message_formatting(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that error messages are properly formatted for different limits."""
    test_cases = [
      (0, "Safety limit reached (0 graphs)"),
      (1, "Safety limit reached (1 graphs)"),
      (10, "Safety limit reached (10 graphs)"),
      (100, "Safety limit reached (100 graphs)"),
    ]

    for limit, expected_message_part in test_cases:
      with patch("robosystems.database.get_db_session") as mock_get_db:
        with patch(
          "robosystems.models.core.OrgUser.get_user_orgs"
        ) as mock_get_user_orgs:
          with patch.object(OrgLimits, "get_or_create_for_org") as mock_get_limits:
            mock_limits = Mock()
            mock_limits.max_graph_users = limit
            mock_limits.can_create_graph.return_value = (
              False,
              f"Safety limit reached ({limit} graphs). Please contact support if you need more.",
            )
            mock_get_limits.return_value = mock_limits

            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])

            # Mock OrgUser.get_user_orgs to return a list with an org
            mock_org_user = Mock()
            mock_org_user.org_id = "test-org-123"
            mock_get_user_orgs.return_value = [mock_org_user]

            response = await async_client.post("/v1/graphs", json=graph_request_data)

            assert response.status_code == status.HTTP_403_FORBIDDEN
            response_data = response.json()
            assert expected_message_part in response_data["detail"]["error"]["message"]

  # TODO: This test needs to be updated to handle SSE responses
  # async def test_user_without_limits_creates_defaults(
  #   self, async_client: AsyncClient, graph_request_data
  # ):
  #   """Test that users without limits get defaults from environment."""
  #   # This test needs to be rewritten to handle the new SSE response format
  #   pass


@pytest.mark.asyncio
class TestGraphLimitsEndpoint:
  """GET /v1/graphs/{graph_id}/limits must report what is enforced.

  Three bugs lived in this one handler: `credits` was permanently null (it
  read a column that does not exist, behind a bare except), `subscription_tier`
  came from a User attribute that does not exist, and shared repositories
  reported the anonymous base table while enforcement grants the user-keyed
  standard tier. These pin the endpoint end to end.
  """

  def _use_test_db(self, test_db):
    """Route the endpoint's async session dependency at the test database.

    async_client does not override get_async_db_session; its teardown clears
    all overrides, so adding one here is safe.
    """
    from main import app
    from robosystems.database import get_async_db_session

    async def override():
      yield test_db

    app.dependency_overrides[get_async_db_session] = override

  def _make_graph(self, test_db, test_org, test_user, tier, with_credits=True):
    import uuid
    from datetime import UTC, datetime
    from decimal import Decimal

    from robosystems.models.core import Graph, GraphCredits, GraphUser

    graph = Graph.create(
      graph_id=f"kg{uuid.uuid4().hex[:16]}",
      graph_name="Limits Endpoint Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      base_schema="base",
      schema_extensions=[],
      graph_tier=tier,
      graph_instance_id="test-instance",
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role="admin",
      is_selected=False,
      session=test_db,
    )
    if with_credits:
      test_db.add(
        GraphCredits(
          id=f"gc_{graph.graph_id}",
          graph_id=graph.graph_id,
          user_id=test_user.id,
          billing_admin_id=test_user.id,
          current_balance=Decimal("31999.50"),
          monthly_allocation=Decimal("32000"),
          last_allocation_date=datetime.now(UTC),
        )
      )
    test_db.commit()
    return graph

  def _no_graph_api(self):
    """The handler tolerates an unreachable Graph API; keep tests hermetic."""
    from unittest.mock import AsyncMock

    return (
      patch(
        "robosystems.routers.graphs.limits.get_universal_repository",
        new=AsyncMock(),
      ),
      patch(
        "robosystems.routers.graphs.limits._get_graph_client",
        new=AsyncMock(side_effect=RuntimeError("no graph api in tests")),
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage",
        new=AsyncMock(side_effect=RuntimeError("no graph api in tests")),
      ),
    )

  async def test_reports_enforced_tier_rate_and_credits(
    self, async_client: AsyncClient, test_db, test_org, test_user
  ):
    from robosystems.config.graph_tier import GraphTier

    graph = self._make_graph(test_db, test_org, test_user, GraphTier.LADYBUG_LARGE)
    self._use_test_db(test_db)

    repo, client, storage = self._no_graph_api()
    with repo, client, storage:
      response = await async_client.get(f"/v1/graphs/{graph.graph_id}/limits")

    assert response.status_code == 200
    data = response.json()
    assert data["graph_tier"] == "ladybug-large"
    assert data["subscription_tier"] == "ladybug-large"
    # The enforced GRAPH_QUERY limit for ladybug-large, not 60 x multiplier
    assert data["rate_limits"]["requests_per_minute"] == 120
    # Was permanently null: the handler read monthly_credit_limit, a column
    # that does not exist, and a bare except swallowed the AttributeError.
    assert data["credits"] == {
      "monthly_ai_credits": 32000,
      "current_balance": 31999,
    }

  async def test_reports_document_usage_against_tier_cap(
    self, async_client: AsyncClient, test_db, test_org, test_user
  ):
    """The meter must mirror upload enforcement: only uploaded documents
    count toward the tier cap, not connection-synced ones."""
    from robosystems.config.graph_tier import GraphTier
    from robosystems.models.core import Document

    graph = self._make_graph(test_db, test_org, test_user, GraphTier.LADYBUG_STANDARD)
    for i in range(2):
      Document.create(
        graph_id=graph.graph_id,
        user_id=test_user.id,
        title=f"Uploaded {i}",
        content="# doc",
        session=test_db,
      )
    Document.create(
      graph_id=graph.graph_id,
      user_id=test_user.id,
      title="Synced",
      content="# synced",
      session=test_db,
      external_id="qb-123",
      source_type="connection_sync",
    )
    self._use_test_db(test_db)

    repo, client, storage = self._no_graph_api()
    with repo, client, storage:
      response = await async_client.get(f"/v1/graphs/{graph.graph_id}/limits")

    assert response.status_code == 200
    data = response.json()
    assert data["documents"] == {
      "current_count": 2,
      "max_documents": 100,
      "approaching_limit": False,
    }

  async def test_shared_repository_reports_the_enforced_fallback_tier(
    self, async_client: AsyncClient, test_db
  ):
    """ladybug-shared has no limits table; enforcement grants the user-keyed
    standard tier, and the report must not fall to the base table (20/min)."""
    self._use_test_db(test_db)

    repo, client, storage = self._no_graph_api()
    with repo, client, storage:
      response = await async_client.get("/v1/graphs/sec/limits")

    assert response.status_code == 200
    data = response.json()
    assert data["is_shared_repository"] is True
    assert data["graph_tier"] == "ladybug-shared"
    assert data["subscription_tier"] == "ladybug-standard"
    assert data["rate_limits"]["requests_per_minute"] == 60
    assert data["credits"] is None

  async def test_legacy_tier_string_floors_at_standard_not_base(
    self, async_client: AsyncClient, test_db, test_org, test_user
  ):
    """A graphs.graph_tier value predating a tier rename must report the
    fallback tier's limits, mirroring what the limiter enforces."""
    graph = self._make_graph(
      test_db, test_org, test_user, "kuzu-standard", with_credits=False
    )
    self._use_test_db(test_db)

    repo, client, storage = self._no_graph_api()
    with repo, client, storage:
      response = await async_client.get(f"/v1/graphs/{graph.graph_id}/limits")

    assert response.status_code == 200
    data = response.json()
    assert data["graph_tier"] == "kuzu-standard"
    assert data["subscription_tier"] == "ladybug-standard"
    assert data["rate_limits"]["requests_per_minute"] == 60
    # No billing plan for the legacy string: the count reports, the cap is
    # open — matching enforcement, which only gates known tiers.
    assert data["documents"] == {
      "current_count": 0,
      "max_documents": None,
      "approaching_limit": False,
    }

  async def test_shared_repository_subgraph_takes_the_shared_path(
    self, async_client: AsyncClient, test_db
  ):
    """sec_historical must be treated exactly like sec: shared tier report,
    no storage walk against the shared master, no credits block. The
    exact-only check ran it through the user-graph sections."""
    self._use_test_db(test_db)

    repo, client, storage = self._no_graph_api()
    with repo, client, storage:
      response = await async_client.get("/v1/graphs/sec_historical/limits")

    assert response.status_code == 200
    data = response.json()
    assert data["is_shared_repository"] is True
    assert data["graph_tier"] == "ladybug-shared"
    assert data["subscription_tier"] == "ladybug-standard"
    assert data["rate_limits"]["requests_per_minute"] == 60
    assert data["credits"] is None
    assert data["documents"] is None
    assert data["content"] is None
    assert data["instance"] is None
