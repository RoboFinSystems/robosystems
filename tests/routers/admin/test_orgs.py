"""Tests for admin orgs router."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app
from robosystems.models.core import Org, OrgLimits, OrgType, OrgUser, User


@pytest.fixture
def mock_admin_auth(monkeypatch):
  """Mock admin authentication to bypass AWS Secrets Manager."""

  def mock_verify_admin_key(self, api_key):
    """Mock verify_admin_key to always return valid admin metadata."""
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
def test_user(db_session: Session):
  """Create a test user."""
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
  """Create a test organization."""
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
def client():
  """Create a test client."""
  return TestClient(app)


ADMIN_HEADERS = {"Authorization": "Bearer test-admin-key"}


class TestGetOrg:
  """Tests for the get org endpoint."""

  def test_get_org_includes_max_graphs(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """Org details expose the effective graph limit."""
    limits = OrgLimits.get_or_create_for_org(test_org.id, db_session)
    limits.update_limit(3, db_session)

    response = client.get(f"/admin/v1/orgs/{test_org.id}", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == test_org.id
    assert data["max_graphs"] == 3

  def test_get_org_without_limits_row_uses_default(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """Orgs with no OrgLimits row report the environment default without creating one."""
    response = client.get(f"/admin/v1/orgs/{test_org.id}", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["max_graphs"], int)
    assert OrgLimits.get_by_org_id(test_org.id, db_session) is None

  def test_get_org_not_found(self, client, mock_admin_auth):
    """Unknown org returns 404."""
    response = client.get("/admin/v1/orgs/org_missing", headers=ADMIN_HEADERS)

    assert response.status_code == 404


class TestListOrgs:
  """Tests for the list orgs endpoint."""

  def test_list_orgs_includes_max_graphs(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """Org list rows expose the effective graph limit."""
    limits = OrgLimits.get_or_create_for_org(test_org.id, db_session)
    limits.update_limit(1, db_session)

    response = client.get("/admin/v1/orgs", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    rows = {org["org_id"]: org for org in response.json()}
    assert rows[test_org.id]["max_graphs"] == 1


class TestUpdateOrg:
  """Tests for the update org endpoint."""

  def test_update_max_graphs(self, client, db_session, test_org, mock_admin_auth):
    """max_graphs updates the OrgLimits row and is reflected in the response."""
    limits = OrgLimits.get_or_create_for_org(test_org.id, db_session)
    limits.update_limit(1, db_session)

    response = client.patch(
      f"/admin/v1/orgs/{test_org.id}",
      json={"max_graphs": 2},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    assert response.json()["max_graphs"] == 2

    db_session.expire_all()
    updated = OrgLimits.get_by_org_id(test_org.id, db_session)
    assert updated is not None
    assert updated.max_graphs == 2

  def test_update_max_graphs_creates_limits_row(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """Setting max_graphs on an org without a limits row creates one."""
    assert OrgLimits.get_by_org_id(test_org.id, db_session) is None

    response = client.patch(
      f"/admin/v1/orgs/{test_org.id}",
      json={"max_graphs": 5},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    assert response.json()["max_graphs"] == 5

    db_session.expire_all()
    created = OrgLimits.get_by_org_id(test_org.id, db_session)
    assert created is not None
    assert created.max_graphs == 5

  def test_update_max_graphs_unlimited(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """-1 is accepted and means unlimited."""
    response = client.patch(
      f"/admin/v1/orgs/{test_org.id}",
      json={"max_graphs": -1},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    assert response.json()["max_graphs"] == -1

  def test_update_max_graphs_below_minimum_rejected(
    self, client, test_org, mock_admin_auth
  ):
    """Values below -1 are rejected by validation."""
    response = client.patch(
      f"/admin/v1/orgs/{test_org.id}",
      json={"max_graphs": -2},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422

  def test_update_billing_fields_via_body(
    self, client, db_session, test_org, mock_admin_auth
  ):
    """Billing settings are read from the JSON body (the admin CLI's contract)."""
    response = client.patch(
      f"/admin/v1/orgs/{test_org.id}",
      json={"billing_email": "billing@example.com", "invoice_billing_enabled": True},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    data = response.json()
    assert data["billing_email"] == "billing@example.com"
    assert data["invoice_billing_enabled"] is True

  def test_update_org_not_found(self, client, mock_admin_auth):
    """Unknown org returns 404."""
    response = client.patch(
      "/admin/v1/orgs/org_missing",
      json={"max_graphs": 2},
      headers=ADMIN_HEADERS,
    )

    assert response.status_code == 404
