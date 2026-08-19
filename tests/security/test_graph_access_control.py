"""
Comprehensive security tests for graph access control.

Tests that users cannot access graphs they don't own, validating the
graph-scoped authorization system across all endpoint patterns.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status

from robosystems.database import session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.core import GraphUser


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
    mock_request.client.host = "127.0.0.1"
    mock_request.headers = {"authorization": "Bearer valid_token"}
    mock_request.url.path = f"/v1/graphs/{sample_graph.graph_id}/info"

    with (
      patch(
        "robosystems.middleware.auth.dependencies.verify_jwt_claims"
      ) as mock_verify,
      patch(
        "robosystems.middleware.auth.dependencies._db_get_user_by_id"
      ) as mock_get_user,
      patch(
        "robosystems.middleware.auth.dependencies._db_check_graph_access"
      ) as mock_access,
    ):
      mock_verify.return_value = (test_user.id, 0)
      mock_get_user.return_value = test_user
      mock_access.return_value = True

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

    with (
      patch(
        "robosystems.middleware.auth.dependencies.verify_jwt_claims"
      ) as mock_verify,
      patch(
        "robosystems.middleware.auth.dependencies._db_get_user_by_id"
      ) as mock_get_user,
      patch(
        "robosystems.middleware.auth.dependencies._db_check_graph_access"
      ) as mock_access,
    ):
      mock_verify.return_value = (test_user.id, 0)
      mock_get_user.return_value = test_user
      mock_access.return_value = False

      # Should raise 403 - user lacks access
      with pytest.raises(Exception) as exc_info:
        await get_current_user_with_graph(mock_request, sample_graph.graph_id, None)
      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

  async def test_api_key_with_graph_access(
    self, mock_request, test_user, sample_graph, test_api_key
  ):
    """Test that API keys with graph access work."""
    # Create user-graph relationship
    GraphUser.create(
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
      mock_validate.assert_called_once_with(
        test_api_key.key, sample_graph.graph_id, allow_deprovisioned=False
      )

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
      f"/v1/graphs/{sample_graph.graph_id}/query/cypher",
      headers={"Authorization": f"Bearer {other_user_token}"},
      json={"cypher": "MATCH (n) RETURN n LIMIT 1"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_graph_backups_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/operations/create-backup denies unauthorized users."""
    response = await auth_integration_client.post(
      f"/v1/graphs/{sample_graph.graph_id}/operations/create-backup",
      headers={"Authorization": f"Bearer {other_user_token}"},
      json={"backup_format": "full_dump"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN

  async def test_graph_operator_endpoint_denies_unauthorized_user(
    self, auth_integration_client, test_user_token, other_user_token, sample_graph
  ):
    """Test /v1/graphs/{graph_id}/operator denies unauthorized users."""
    response = await auth_integration_client.post(
      f"/v1/graphs/{sample_graph.graph_id}/operator",
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
class TestGraphUserAccessValidation:
  """Test GraphUser.user_has_access validation.

  Note: test_user is OWNER of the graph's org and therefore holds implicit
  graph admin, so negative cases need a user without an org role.
  """

  @staticmethod
  def _create_org_member(test_db, test_org, test_user):
    """Create a plain org MEMBER — no implicit graph access."""
    import uuid

    from robosystems.models.core import OrgRole, OrgUser, User

    member = User(
      email=f"plainmember+{uuid.uuid4().hex[:8]}@example.com",
      name="Plain Member",
      password_hash=test_user.password_hash,
    )
    test_db.add(member)
    test_db.commit()
    OrgUser.create(
      org_id=test_org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )
    return member

  def test_user_has_access_returns_true_for_member(
    self, test_db, test_org, test_user, sample_graph
  ):
    """Test user_has_access returns True when user is a member."""
    member = self._create_org_member(test_db, test_org, test_user)
    GraphUser.create(
      user_id=member.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=test_db,
    )

    assert GraphUser.user_has_access(member.id, sample_graph.graph_id, test_db)

  def test_user_has_access_returns_false_for_non_member(
    self, test_db, test_org, test_user, sample_graph
  ):
    """No explicit grant and no org owner/admin role → no access."""
    member = self._create_org_member(test_db, test_org, test_user)

    assert not GraphUser.user_has_access(member.id, sample_graph.graph_id, test_db)

  def test_org_owner_has_implicit_access(self, test_db, test_user, sample_graph):
    """Org OWNER holds implicit graph admin without a GraphUser row."""
    assert GraphUser.user_has_access(test_user.id, sample_graph.graph_id, test_db)
    assert GraphUser.user_has_admin_access(test_user.id, sample_graph.graph_id, test_db)

  def test_user_has_admin_access_validates_role(
    self, test_db, test_org, test_user, sample_graph
  ):
    """Test user_has_admin_access validates admin role."""
    member = self._create_org_member(test_db, test_org, test_user)
    GraphUser.create(
      user_id=member.id,
      graph_id=sample_graph.graph_id,
      role="member",
      session=test_db,
    )

    # Member role should not have admin access
    assert not GraphUser.user_has_admin_access(
      member.id, sample_graph.graph_id, test_db
    )

    # Update to admin
    user_graph = GraphUser.get_by_user_and_graph(
      member.id, sample_graph.graph_id, test_db
    )
    user_graph.update_role("admin", test_db)

    assert GraphUser.user_has_admin_access(member.id, sample_graph.graph_id, test_db)
