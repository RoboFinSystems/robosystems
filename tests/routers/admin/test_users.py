"""Tests for the admin users router — deletion surface."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app
from robosystems.models.core import Graph, Org, OrgType, OrgUser, User


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


ADMIN_HEADERS = {"Authorization": "Bearer test-admin-key"}


@pytest.fixture
def deletable_user(db_session: Session):
  """A user whose org holds nothing — the common support case."""
  unique_id = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{unique_id}",
    email=f"squatter+{unique_id}@example.com",
    name="Squatter",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.commit()

  org = Org.create(
    name=f"Personal {unique_id}", org_type=OrgType.PERSONAL, session=db_session
  )
  OrgUser.create(org_id=org.id, user_id=user.id, role="OWNER", session=db_session)
  return user


class TestDeleteUser:
  def test_dry_run_reports_without_deleting(
    self, client, db_session, deletable_user, mock_admin_auth
  ):
    response = client.delete(
      f"/admin/v1/users/{deletable_user.id}?dry_run=true", headers=ADMIN_HEADERS
    )

    assert response.status_code == 200
    data = response.json()
    assert data["dry_run"] is True
    assert data["deleted"] is False
    assert data["blockers"] == []
    assert User.get_by_id(deletable_user.id, db_session) is not None

  def test_delete_frees_the_email(
    self, client, db_session, deletable_user, mock_admin_auth
  ):
    email = deletable_user.email

    response = client.delete(
      f"/admin/v1/users/{deletable_user.id}", headers=ADMIN_HEADERS
    )

    assert response.status_code == 200
    assert response.json()["deleted"] is True
    assert User.get_by_email(email, db_session) is None

  def test_live_graph_returns_conflict(
    self, client, db_session, deletable_user, mock_admin_auth
  ):
    org_id = OrgUser.get_user_orgs(deletable_user.id, db_session)[0].org_id
    Graph.create(
      graph_id=f"kg{uuid.uuid4().hex[:16]}",
      org_id=org_id,
      graph_name="Live Graph",
      graph_type="generic",
      session=db_session,
    )

    response = client.delete(
      f"/admin/v1/users/{deletable_user.id}", headers=ADMIN_HEADERS
    )

    assert response.status_code == 409
    blockers = response.json()["detail"]["blockers"]
    assert [b["code"] for b in blockers] == ["org_has_live_graphs"]
    assert User.get_by_id(deletable_user.id, db_session) is not None

  def test_unknown_user_returns_404(self, client, mock_admin_auth):
    response = client.delete("/admin/v1/users/user_missing", headers=ADMIN_HEADERS)

    assert response.status_code == 404
