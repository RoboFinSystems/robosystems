"""Tests for admin graph deprovision endpoint."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app
from robosystems.config.graph_tier import GraphTier
from robosystems.models.iam import Graph, Org, OrgType, OrgUser, User
from robosystems.models.iam.graph import GraphStatus


@pytest.fixture
def mock_admin_auth(monkeypatch):
  """Mock admin authentication to bypass AWS Secrets Manager."""

  def mock_verify_admin_key(self, api_key):
    return {
      "key_id": "admin",
      "name": "Test Admin",
      "permissions": ["*"],
      "created_by": "system",
    }

  monkeypatch.setattr(
    "robosystems.middleware.auth.admin.AdminAuthMiddleware.verify_admin_key",
    mock_verify_admin_key,
  )
  yield


@pytest.fixture
def client():
  return TestClient(app)


@pytest.fixture
def test_user(db_session: Session):
  unique_id = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.commit()
  db_session.refresh(user)
  return user


@pytest.fixture
def test_org(db_session: Session, test_user):
  org = Org.create(
    name="Test Organization",
    org_type=OrgType.PERSONAL,
    session=db_session,
  )
  OrgUser.create(
    org_id=org.id,
    user_id=test_user.id,
    role="OWNER",
    session=db_session,
  )
  return org


@pytest.fixture
def test_graph(db_session: Session, test_org):
  unique_id = str(uuid.uuid4())[:8]
  graph = Graph.create(
    graph_id=f"kg_{unique_id}",
    graph_name="Test Graph",
    graph_type="entity",
    org_id=test_org.id,
    session=db_session,
    graph_tier=GraphTier.LADYBUG_STANDARD,
  )
  return graph


class TestDeprovisionGraph:
  """Tests for POST /admin/v1/graphs/{graph_id}/deprovision."""

  def test_deprovision_success(self, client, db_session, test_graph, mock_admin_auth):
    """Test successfully deprovisioning an active graph."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      response = client.post(
        f"/admin/v1/graphs/{test_graph.graph_id}/deprovision?skip_backup=true",
        headers={"Authorization": "Bearer test-admin-key"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == test_graph.graph_id
      assert data["previous_status"] == "active"
      assert data["database_deleted"] is True
      assert data["status"] == "deprovisioned"
      assert data["records_cleaned"] is True
      assert data["backup_created"] is False

      # Verify graph status was updated in DB
      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  def test_deprovision_graph_not_found(self, client, mock_admin_auth):
    """Test 404 for nonexistent graph_id."""
    response = client.post(
      "/admin/v1/graphs/kg_nonexistent/deprovision?skip_backup=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()

  def test_deprovision_already_deprovisioned(
    self, client, db_session, test_graph, mock_admin_auth
  ):
    """Test 409 when graph is already deprovisioned."""
    # First deprovision it
    test_graph.transition_status(GraphStatus.DEPROVISIONED, db_session)

    response = client.post(
      f"/admin/v1/graphs/{test_graph.graph_id}/deprovision?skip_backup=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 409
    assert "already deprovisioned" in response.json()["detail"].lower()

  def test_deprovision_rejects_shared_repository(
    self, client, db_session, test_graph, mock_admin_auth
  ):
    """Test 403 when trying to deprovision a shared repository."""
    test_graph.is_repository = True
    test_graph.repository_type = "sec"
    db_session.commit()

    response = client.post(
      f"/admin/v1/graphs/{test_graph.graph_id}/deprovision?skip_backup=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 403
    assert "shared repository" in response.json()["detail"].lower()

  def test_deprovision_database_deletion_fails_gracefully(
    self, client, db_session, test_graph, mock_admin_auth
  ):
    """Test that DB deletion failure still succeeds with database_deleted=False."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_client.delete_database.side_effect = Exception("Connection refused")
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      response = client.post(
        f"/admin/v1/graphs/{test_graph.graph_id}/deprovision?skip_backup=true",
        headers={"Authorization": "Bearer test-admin-key"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["database_deleted"] is False
      assert data["status"] == "deprovisioned"
      assert data["warnings"] is not None
      assert any("Database deletion failed" in w for w in data["warnings"])
      assert "not found or already removed" in data["message"]

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  def test_deprovision_with_skip_backup(
    self, client, db_session, test_graph, mock_admin_auth
  ):
    """Test that skip_backup=true skips backup creation."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      response = client.post(
        f"/admin/v1/graphs/{test_graph.graph_id}/deprovision?skip_backup=true",
        headers={"Authorization": "Bearer test-admin-key"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["backup_created"] is False
