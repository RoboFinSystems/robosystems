"""
Comprehensive security tests for graph access control.

Tests that users cannot access graphs they don't own, validating the
graph-scoped authorization system across all endpoint patterns.
"""

import pytest
from fastapi import status
from unittest.mock import patch, MagicMock

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.iam import UserGraph
from robosystems.database import session


@pytest.fixture
def mock_request():
  """Create a mock request object for testing."""
  request = MagicMock()
  request.client = MagicMock()
  request.client.host = "127.0.0.1"
  request.headers = {}
  request.url = MagicMock()
  request.url.path = "/test"
  return request


@pytest.fixture
def test_api_key():
  """Create a mock API key for testing."""
  api_key = MagicMock()
  api_key.key = "test_api_key_123"
  return api_key


@pytest.mark.unit
class TestGraphAccessControlDependency:
  """Test the get_current_user_with_graph dependency."""

  async def test_valid_jwt_with_graph_access(
    self, mock_request, test_user, sample_graph
  ):
    """Test that users with valid graph access are authenticated."""
    # Create user-graph relationship
    UserGraph.create(
      user_id=test_user.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=session,
    )

    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {"authorization": "Bearer valid_token"}
    mock_request.url.path = f"/v1/graphs/{sample_graph.graph_id}/info"

    with patch(
      "robosystems.middleware.auth.dependencies.verify_jwt_token"
    ) as mock_verify:
      with patch(
        "robosystems.middleware.auth.dependencies.User.get_by_id"
      ) as mock_get_user:
        mock_verify.return_value = test_user.id
        mock_get_user.return_value = test_user

        # Should succeed - user has access
        user = await get_current_user_with_graph(
          mock_request, sample_graph.graph_id, None
        )
        assert user.id == test_user.id

  async def test_valid_jwt_without_graph_access_raises_403(
    self, mock_request, test_user, sample_graph
  ):
    """Test that users without graph access get HTTP 403."""
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {"authorization": "Bearer valid_token"}
    mock_request.url.path = f"/v1/graphs/{sample_graph.graph_id}/info"

    with patch(
      "robosystems.middleware.auth.dependencies.verify_jwt_token"
    ) as mock_verify:
      with patch(
        "robosystems.middleware.auth.dependencies.User.get_by_id"
      ) as mock_get_user:
        mock_verify.return_value = test_user.id
        mock_get_user.return_value = test_user

        # Should raise 403 - user lacks access
        with pytest.raises(Exception) as exc_info:
          await get_current_user_with_graph(mock_request, sample_graph.graph_id, None)
        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

  async def test_api_key_with_graph_access(
    self, mock_request, test_user, sample_graph, test_api_key
  ):
    """Test that API keys with graph access work."""
    # Create user-graph relationship
    UserGraph.create(
      user_id=test_user.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=session,
    )

    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {}
    mock_request.url.path = f"/v1/graphs/{sample_graph.graph_id}/info"

    with patch(
      "robosystems.middleware.auth.dependencies.validate_api_key_with_graph"
    ) as mock_validate:
      mock_validate.return_value = test_user

      # Should succeed - API key has graph access
      user = await get_current_user_with_graph(
        mock_request, sample_graph.graph_id, test_api_key.key
      )
      assert user.id == test_user.id
      mock_validate.assert_called_once_with(test_api_key.key, sample_graph.graph_id)

  async def test_api_key_without_graph_access_raises_403(
    self, mock_request, test_user, sample_graph, test_api_key
  ):
    """Test that API keys without graph access get HTTP 403."""
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {}
    mock_request.url.path = f"/v1/graphs/{sample_graph.graph_id}/info"

    with patch(
      "robosystems.middleware.auth.dependencies.validate_api_key_with_graph"
    ) as mock_validate:
      mock_validate.return_value = None  # No access

      # Should raise 403
      with pytest.raises(Exception) as exc_info:
        await get_current_user_with_graph(
          mock_request, sample_graph.graph_id, test_api_key.key
        )
      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.integration
class TestGraphEndpointAccessControl:
  """Integration tests for graph endpoint access control."""

  async def test_graph_info_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/info denies unauthorized users."""
    response = await auth_integration_client.get(
      f"/v1/graphs/{sample_graph.graph_id}/info",
      headers={"Authorization": f"Bearer {other_user_token}"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert "Access denied" in response.json()["detail"]

  async def test_graph_query_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/query denies unauthorized users."""
    response = await auth_integration_client.post(
      f"/v1/graphs/{sample_graph.graph_id}/query",
      headers={"Authorization": f"Bearer {other_user_token}"},
      json={"cypher": "MATCH (n) RETURN n LIMIT 1"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_graph_backups_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/backups denies unauthorized users."""
    response = await auth_integration_client.post(
      f"/v1/graphs/{sample_graph.graph_id}/backups",
      headers={"Authorization": f"Bearer {other_user_token}"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_graph_agent_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/agent denies unauthorized users."""
    response = await auth_integration_client.post(
      f"/v1/graphs/{sample_graph.graph_id}/agent",
      headers={"Authorization": f"Bearer {other_user_token}"},
      json={"message": "Test query"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_shared_repository_access_requires_permission(
    self, auth_integration_client, test_user_token, test_user
  ):
    """Test shared repository (SEC) requires explicit permission.

    User should be denied access to 'sec' shared repository when they don't
    have explicit UserRepository access granted.
    """
    response = await auth_integration_client.get(
      "/v1/graphs/sec/info",
      headers={"Authorization": f"Bearer {test_user_token}"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_authorized_user_can_access_their_graph(
    self, auth_integration_client, test_user_token, sample_graph
  ):
    """Test that users CAN access graphs they own."""
    response = await auth_integration_client.get(
      f"/v1/graphs/{sample_graph.graph_id}/info",
      headers={"Authorization": f"Bearer {test_user_token}"},
    )

    assert response.status_code == status.HTTP_200_OK


@pytest.mark.unit
class TestUserGraphAccessValidation:
  """Test UserGraph.user_has_access validation."""

  def test_user_has_access_returns_true_for_member(self, test_user, sample_graph):
    """Test user_has_access returns True when user is a member."""
    UserGraph.create(
      user_id=test_user.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=session,
    )

    has_access = UserGraph.user_has_access(test_user.id, sample_graph.graph_id, session)
    assert has_access is True

  def test_user_has_access_returns_false_for_non_member(self, test_user, sample_graph):
    """Test user_has_access returns False when user is not a member."""
    has_access = UserGraph.user_has_access(test_user.id, sample_graph.graph_id, session)
    assert has_access is False

  def test_user_has_admin_access_validates_role(self, test_user, sample_graph):
    """Test user_has_admin_access validates admin role."""
    # Create as member
    UserGraph.create(
      user_id=test_user.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=session,
    )

    # Should not have admin access
    has_admin = UserGraph.user_has_admin_access(
      test_user.id, sample_graph.graph_id, session
    )
    assert has_admin is False

    # Update to admin
    user_graph = UserGraph.get_by_user_and_graph(
      test_user.id, sample_graph.graph_id, session
    )
    user_graph.update_role("admin", session)

    # Now should have admin access
    has_admin = UserGraph.user_has_admin_access(
      test_user.id, sample_graph.graph_id, session
    )
    assert has_admin is True
