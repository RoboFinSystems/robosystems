"""Comprehensive tests for admin subscription router."""

import pytest
import uuid
from fastapi.testclient import TestClient
from unittest.mock import Mock
from sqlalchemy.orm import Session

from main import app
from robosystems.models.billing import (
  BillingCustomer,
  BillingSubscription,
  BillingAuditLog,
  SubscriptionStatus,
)
from robosystems.models.iam import User, Graph


@pytest.fixture
def admin_mock_request():
  """Create a mock request with admin authentication."""
  mock_request = Mock()
  mock_request.state.admin = {
    "key_id": "admin",
    "name": "Test Admin",
    "permissions": ["*"],
  }
  mock_request.state.admin_key_id = "admin"
  return mock_request


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
def test_graph(db_session: Session, test_user):
  """Create a test graph."""
  unique_id = str(uuid.uuid4())[:8]
  graph = Graph(
    graph_id=f"kg_{unique_id}",
    graph_name="Test Graph",
    graph_type="entity",
  )
  db_session.add(graph)
  db_session.commit()
  db_session.refresh(graph)
  return graph


@pytest.fixture
def test_customer(db_session: Session, test_user):
  """Create a test billing customer."""
  customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
  return customer


@pytest.fixture
def test_subscription(db_session: Session, test_user, test_graph):
  """Create a test subscription."""
  subscription = BillingSubscription.create_subscription(
    user_id=test_user.id,
    resource_type="graph",
    resource_id=test_graph.graph_id,
    plan_name="kuzu-standard",
    base_price_cents=4999,
    session=db_session,
  )
  db_session.refresh(subscription)
  return subscription


@pytest.fixture
def client():
  """Create a test client."""
  return TestClient(app)


class TestListSubscriptions:
  """Tests for listing subscriptions endpoint."""

  def test_list_subscriptions_success(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test successfully listing subscriptions."""
    response = client.get(
      "/admin/v1/subscriptions",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1

    subscription_ids = [sub["id"] for sub in data]
    assert test_subscription.id in subscription_ids

  def test_list_subscriptions_with_resource_type_filter(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test listing subscriptions filtered by resource type."""
    response = client.get(
      "/admin/v1/subscriptions?resource_type=graph",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert all(sub["resource_type"] == "graph" for sub in data)

  def test_list_subscriptions_with_status_filter(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test listing subscriptions filtered by status."""
    test_subscription.activate(db_session)

    response = client.get(
      "/admin/v1/subscriptions?status_filter=active",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    subscription_ids = [sub["id"] for sub in data]
    assert test_subscription.id in subscription_ids

  def test_list_subscriptions_with_email_filter(self, client, mock_admin_auth):
    """Test that email filter parameter works without errors."""
    response = client.get(
      "/admin/v1/subscriptions?user_email=test",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

  def test_list_subscriptions_exclude_canceled(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test that canceled subscriptions are excluded by default."""
    test_subscription.activate(db_session)
    test_subscription.cancel(db_session, immediate=True)

    response = client.get(
      "/admin/v1/subscriptions?include_canceled=false",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    subscription_ids = [sub["id"] for sub in data]
    assert test_subscription.id not in subscription_ids

  def test_list_subscriptions_include_canceled(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test including canceled subscriptions."""
    test_subscription.activate(db_session)
    test_subscription.cancel(db_session, immediate=True)

    response = client.get(
      "/admin/v1/subscriptions?include_canceled=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    subscription_ids = [sub["id"] for sub in data]
    assert test_subscription.id in subscription_ids

  def test_list_subscriptions_pagination(
    self, client, db_session, test_user, test_graph, mock_admin_auth
  ):
    """Test pagination parameters."""
    for i in range(5):
      unique_id = str(uuid.uuid4())[:8]
      graph = Graph(
        graph_id=f"kg_{unique_id}",
        graph_name=f"Test Graph {i}",
        graph_type="entity",
      )
      db_session.add(graph)
      db_session.commit()

      BillingSubscription.create_subscription(
        user_id=test_user.id,
        resource_type="graph",
        resource_id=graph.graph_id,
        plan_name="kuzu-standard",
        base_price_cents=2999,
        session=db_session,
      )

    response = client.get(
      "/admin/v1/subscriptions?limit=2&offset=0",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

  def test_list_subscriptions_unauthorized(self, client):
    """Test listing subscriptions without authentication."""
    response = client.get("/admin/v1/subscriptions")
    assert response.status_code == 401


class TestGetSubscription:
  """Tests for getting a specific subscription endpoint."""

  def test_get_subscription_success(self, client, test_subscription, mock_admin_auth):
    """Test successfully getting a subscription."""
    response = client.get(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == test_subscription.id
    assert data["resource_type"] == test_subscription.resource_type
    assert data["resource_id"] == test_subscription.resource_id
    assert data["plan_name"] == test_subscription.plan_name

  def test_get_subscription_not_found(self, client, mock_admin_auth):
    """Test getting a non-existent subscription."""
    response = client.get(
      "/admin/v1/subscriptions/bsub_nonexistent",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()

  def test_get_subscription_unauthorized(self, client, test_subscription):
    """Test getting subscription without authentication."""
    response = client.get(f"/admin/v1/subscriptions/{test_subscription.id}")
    assert response.status_code == 401


class TestCreateSubscription:
  """Tests for creating subscriptions endpoint."""

  def test_create_subscription_success(
    self, client, db_session, test_user, test_graph, mock_admin_auth
  ):
    """Test successfully creating a subscription."""
    unique_graph_id = f"kg_{str(uuid.uuid4())[:8]}"
    graph = Graph(
      graph_id=unique_graph_id,
      graph_name="New Graph",
      graph_type="entity",
    )
    db_session.add(graph)
    db_session.commit()

    payload = {
      "resource_type": "graph",
      "resource_id": unique_graph_id,
      "user_id": test_user.id,
      "plan_name": "kuzu-standard",
      "billing_interval": "monthly",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    if response.status_code != 201:
      print(f"Error response: {response.json()}")

    assert response.status_code == 201
    data = response.json()
    assert data["id"].startswith("bsub_")
    assert data["resource_type"] == "graph"
    assert data["resource_id"] == unique_graph_id
    assert data["plan_name"] == "kuzu-standard"
    assert data["status"] == SubscriptionStatus.ACTIVE.value

  def test_create_subscription_user_not_found(
    self, client, test_graph, mock_admin_auth
  ):
    """Test creating subscription for non-existent user."""
    payload = {
      "resource_type": "graph",
      "resource_id": test_graph.graph_id,
      "user_id": "nonexistent_user",
      "plan_name": "kuzu-standard",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404
    assert "User" in response.json()["detail"]

  def test_create_subscription_graph_not_found(
    self, client, test_user, mock_admin_auth
  ):
    """Test creating subscription for non-existent graph."""
    payload = {
      "resource_type": "graph",
      "resource_id": "kg_nonexistent",
      "user_id": test_user.id,
      "plan_name": "kuzu-standard",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404
    assert "Graph" in response.json()["detail"]

  def test_create_subscription_already_exists(
    self, client, test_subscription, test_user, test_graph, mock_admin_auth
  ):
    """Test creating subscription when one already exists."""
    payload = {
      "resource_type": "graph",
      "resource_id": test_graph.graph_id,
      "user_id": test_user.id,
      "plan_name": "kuzu-standard",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 400
    assert "already exists" in response.json()["detail"].lower()

  def test_create_subscription_invalid_plan(
    self, client, db_session, test_user, mock_admin_auth
  ):
    """Test creating subscription with invalid plan name."""
    unique_graph_id = f"kg_{str(uuid.uuid4())[:8]}"
    graph = Graph(
      graph_id=unique_graph_id,
      graph_name="New Graph",
      graph_type="entity",
    )
    db_session.add(graph)
    db_session.commit()

    payload = {
      "resource_type": "graph",
      "resource_id": unique_graph_id,
      "user_id": test_user.id,
      "plan_name": "invalid_plan",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 400
    assert "Invalid plan name" in response.json()["detail"]

  def test_create_subscription_creates_audit_log(
    self, client, db_session, test_user, mock_admin_auth
  ):
    """Test that creating subscription creates an audit log entry."""
    unique_graph_id = f"kg_{str(uuid.uuid4())[:8]}"
    graph = Graph(
      graph_id=unique_graph_id,
      graph_name="New Graph",
      graph_type="entity",
    )
    db_session.add(graph)
    db_session.commit()

    payload = {
      "resource_type": "graph",
      "resource_id": unique_graph_id,
      "user_id": test_user.id,
      "plan_name": "kuzu-standard",
    }

    response = client.post(
      "/admin/v1/subscriptions",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 201
    subscription_id = response.json()["id"]

    audit_logs = (
      db_session.query(BillingAuditLog)
      .filter(BillingAuditLog.subscription_id == subscription_id)
      .all()
    )

    assert len(audit_logs) > 0
    assert any(log.event_type == "subscription.created" for log in audit_logs)

  def test_create_subscription_unauthorized(self, client, test_user, test_graph):
    """Test creating subscription without authentication."""
    payload = {
      "resource_type": "graph",
      "resource_id": test_graph.graph_id,
      "user_id": test_user.id,
      "plan_name": "kuzu-standard",
    }

    response = client.post("/admin/v1/subscriptions", json=payload)
    assert response.status_code == 401


class TestUpdateSubscription:
  """Tests for updating subscriptions endpoint."""

  def test_update_subscription_status_to_active(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test updating subscription status to active."""
    payload = {"status": "active"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == SubscriptionStatus.ACTIVE.value

  def test_update_subscription_status_to_paused(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test updating subscription status to paused."""
    test_subscription.activate(db_session)

    payload = {"status": "paused"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == SubscriptionStatus.PAUSED.value

  def test_update_subscription_status_to_canceled(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test updating subscription status to canceled."""
    test_subscription.activate(db_session)

    payload = {"status": "canceled"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == SubscriptionStatus.CANCELED.value

  def test_update_subscription_plan_name(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test updating subscription plan name."""
    payload = {"plan_name": "kuzu-large"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["plan_name"] == "kuzu-large"

  def test_update_subscription_base_price(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test updating subscription base price."""
    payload = {"base_price_cents": 4999}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["base_price_cents"] == 4999

  def test_update_subscription_invalid_plan(
    self, client, test_subscription, mock_admin_auth
  ):
    """Test updating subscription with invalid plan name."""
    payload = {"plan_name": "invalid_plan"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 400
    assert "Invalid plan name" in response.json()["detail"]

  def test_update_subscription_not_found(self, client, mock_admin_auth):
    """Test updating non-existent subscription."""
    payload = {"plan_name": "kuzu-large"}

    response = client.patch(
      "/admin/v1/subscriptions/bsub_nonexistent",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404

  def test_update_subscription_creates_audit_log(
    self, client, db_session, test_subscription, mock_admin_auth
  ):
    """Test that updating subscription creates audit log."""
    payload = {"plan_name": "kuzu-large"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}",
      json=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200

    audit_logs = (
      db_session.query(BillingAuditLog)
      .filter(
        BillingAuditLog.subscription_id == test_subscription.id,
        BillingAuditLog.event_type == "subscription.updated",
      )
      .all()
    )

    assert len(audit_logs) > 0

  def test_update_subscription_unauthorized(self, client, test_subscription):
    """Test updating subscription without authentication."""
    payload = {"plan_name": "kuzu-large"}

    response = client.patch(
      f"/admin/v1/subscriptions/{test_subscription.id}", json=payload
    )
    assert response.status_code == 401


class TestGetSubscriptionAuditLog:
  """Tests for getting subscription audit log endpoint."""

  def test_get_audit_log_success(
    self, client, db_session, test_subscription, test_user, mock_admin_auth
  ):
    """Test successfully getting audit log."""
    BillingAuditLog.log_event(
      session=db_session,
      event_type="subscription.created",
      billing_customer_user_id=test_user.id,
      subscription_id=test_subscription.id,
      description="Test audit log",
    )

    response = client.get(
      f"/admin/v1/subscriptions/{test_subscription.id}/audit",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert data[0]["event_type"] == "subscription.created"

  def test_get_audit_log_with_event_type_filter(
    self, client, db_session, test_subscription, test_user, mock_admin_auth
  ):
    """Test getting audit log filtered by event type."""
    BillingAuditLog.log_event(
      session=db_session,
      event_type="subscription.created",
      billing_customer_user_id=test_user.id,
      subscription_id=test_subscription.id,
      description="Created",
    )
    BillingAuditLog.log_event(
      session=db_session,
      event_type="subscription.updated",
      billing_customer_user_id=test_user.id,
      subscription_id=test_subscription.id,
      description="Updated",
    )

    response = client.get(
      f"/admin/v1/subscriptions/{test_subscription.id}/audit?event_type=subscription.created",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert all(log["event_type"] == "subscription.created" for log in data)

  def test_get_audit_log_subscription_not_found(self, client, mock_admin_auth):
    """Test getting audit log for non-existent subscription."""
    response = client.get(
      "/admin/v1/subscriptions/bsub_nonexistent/audit",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 404

  def test_get_audit_log_unauthorized(self, client, test_subscription):
    """Test getting audit log without authentication."""
    response = client.get(f"/admin/v1/subscriptions/{test_subscription.id}/audit")
    assert response.status_code == 401


class TestListCustomers:
  """Tests for listing customers endpoint."""

  def test_list_customers_success(
    self, client, db_session, test_customer, test_user, mock_admin_auth
  ):
    """Test successfully listing customers."""
    response = client.get(
      "/admin/v1/subscriptions/customers/all",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1

    user_ids = [customer["user_id"] for customer in data]
    assert test_user.id in user_ids

  def test_list_customers_with_payment_method_filter(
    self, client, db_session, test_customer, mock_admin_auth
  ):
    """Test listing customers filtered by payment method."""
    test_customer.update_payment_method(
      stripe_payment_method_id="pm_test", session=db_session
    )

    response = client.get(
      "/admin/v1/subscriptions/customers/all?has_payment_method=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    user_ids = [customer["user_id"] for customer in data]
    assert test_customer.user_id in user_ids

  def test_list_customers_with_invoice_billing_filter(
    self, client, db_session, test_customer, mock_admin_auth
  ):
    """Test listing customers filtered by invoice billing."""
    test_customer.enable_invoice_billing(
      billing_email="billing@example.com",
      billing_contact_name="Finance",
      payment_terms="net_30",
      session=db_session,
    )

    response = client.get(
      "/admin/v1/subscriptions/customers/all?invoice_billing_enabled=true",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    user_ids = [customer["user_id"] for customer in data]
    assert test_customer.user_id in user_ids

  def test_list_customers_pagination(self, client, db_session, mock_admin_auth):
    """Test customer list pagination."""
    for i in range(5):
      unique_id = str(uuid.uuid4())[:8]
      user = User(
        id=f"user_{unique_id}",
        email=f"user{i}+{unique_id}@example.com",
        name=f"User {i}",
        password_hash="test_hash",
      )
      db_session.add(user)
      db_session.commit()
      BillingCustomer.get_or_create(user_id=user.id, session=db_session)

    response = client.get(
      "/admin/v1/subscriptions/customers/all?limit=2&offset=0",
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

  def test_list_customers_unauthorized(self, client):
    """Test listing customers without authentication."""
    response = client.get("/admin/v1/subscriptions/customers/all")
    assert response.status_code == 401


class TestUpdateCustomer:
  """Tests for updating customer endpoint."""

  def test_update_customer_enable_invoice_billing(
    self, client, db_session, test_customer, mock_admin_auth
  ):
    """Test enabling invoice billing for customer."""
    payload = {
      "invoice_billing_enabled": True,
      "billing_email": "billing@enterprise.com",
      "billing_contact_name": "Finance Team",
      "payment_terms": "net_60",
    }

    response = client.patch(
      f"/admin/v1/subscriptions/customers/{test_customer.user_id}",
      params=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["invoice_billing_enabled"] is True
    assert data["billing_email"] == "billing@enterprise.com"
    assert data["payment_terms"] == "net_60"

  def test_update_customer_change_payment_terms(
    self, client, db_session, test_customer, mock_admin_auth
  ):
    """Test changing customer payment terms."""
    payload = {"payment_terms": "net_90"}

    response = client.patch(
      f"/admin/v1/subscriptions/customers/{test_customer.user_id}",
      params=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["payment_terms"] == "net_90"

  def test_update_customer_creates_if_not_exists(
    self, client, db_session, test_user, mock_admin_auth
  ):
    """Test that updating creates customer if doesn't exist."""
    db_session.query(BillingCustomer).filter(
      BillingCustomer.user_id == test_user.id
    ).delete()
    db_session.commit()

    payload = {"payment_terms": "net_30"}

    response = client.patch(
      f"/admin/v1/subscriptions/customers/{test_user.id}",
      params=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == test_user.id

  def test_update_customer_creates_audit_log(
    self, client, db_session, test_customer, mock_admin_auth
  ):
    """Test that updating customer creates audit log."""
    payload = {"payment_terms": "net_60"}

    response = client.patch(
      f"/admin/v1/subscriptions/customers/{test_customer.user_id}",
      params=payload,
      headers={"Authorization": "Bearer test-admin-key"},
    )

    assert response.status_code == 200

    audit_logs = (
      db_session.query(BillingAuditLog)
      .filter(
        BillingAuditLog.billing_customer_user_id == test_customer.user_id,
        BillingAuditLog.event_type == "customer.updated",
      )
      .all()
    )

    assert len(audit_logs) > 0

  def test_update_customer_unauthorized(self, client, test_customer):
    """Test updating customer without authentication."""
    payload = {"payment_terms": "net_60"}

    response = client.patch(
      f"/admin/v1/subscriptions/customers/{test_customer.user_id}",
      params=payload,
    )
    assert response.status_code == 401
