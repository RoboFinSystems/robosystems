"""Tests for the upgrade-tier graph operation endpoint.

Covers the POST /v1/graphs/{graph_id}/operations/upgrade-tier handler
introduced when tier upgrades moved from the subscriptions router to
the CQRS operations surface.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from tests.conftest import VALID_TEST_GRAPH_ID

CMD_MODULE = "robosystems.operations.graph.commands.tier"


class TestUpgradeTierEndpoint:
  """Tests for POST /v1/graphs/{graph_id}/operations/upgrade-tier."""

  @pytest.fixture
  def client(self):
    """Test client with rate limiting disabled."""
    from robosystems.middleware.rate_limits import (
      analytics_rate_limit_dependency,
      auth_rate_limit_dependency,
      auth_status_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      graph_scoped_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      sso_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      user_management_rate_limit_dependency,
    )

    original = app.dependency_overrides.copy()

    app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
    app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
    app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
    app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None

    yield TestClient(app)
    app.dependency_overrides = original

  @pytest.fixture
  def mock_user(self):
    user = MagicMock()
    user.id = "test-user-123"
    user.email = "test@example.com"
    return user

  @pytest.mark.unit
  def test_requires_authentication(self, client):
    """Unauthenticated requests are rejected."""
    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operations/upgrade-tier",
      json={"new_tier": "ladybug-large"},
    )
    assert response.status_code in (401, 403, 422)

  @pytest.mark.unit
  def test_returns_202_with_operation_envelope(self, client, mock_user):
    """Authenticated request dispatches the tier change and returns 202."""
    from robosystems.database import get_async_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph
    from robosystems.middleware.extensions import get_idempotency_cache

    mock_cache = AsyncMock()
    mock_cache.get = AsyncMock(return_value=None)
    mock_cache.put = AsyncMock(return_value=None)

    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
    app.dependency_overrides[get_async_db_session] = lambda: MagicMock()
    app.dependency_overrides[get_idempotency_cache] = lambda: mock_cache

    try:
      with patch(
        f"{CMD_MODULE}.change_graph_tier_cmd",
        new_callable=AsyncMock,
        return_value="op_test_abc123",
      ):
        response = client.post(
          f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operations/upgrade-tier",
          json={"new_tier": "ladybug-large"},
        )

      assert response.status_code == 202
      data = response.json()
      assert data["operation"] == "upgrade-tier"
      assert data["status"] == "pending"
      assert "operationId" in data
    finally:
      del app.dependency_overrides[get_current_user_with_graph]
      del app.dependency_overrides[get_async_db_session]
      del app.dependency_overrides[get_idempotency_cache]

  @pytest.mark.unit
  def test_rejects_invalid_tier(self, client, mock_user):
    """Invalid tier values are rejected with 422."""
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
    try:
      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/operations/upgrade-tier",
        json={"new_tier": "ladybug-premium"},
      )
      assert response.status_code == 422
    finally:
      del app.dependency_overrides[get_current_user_with_graph]
