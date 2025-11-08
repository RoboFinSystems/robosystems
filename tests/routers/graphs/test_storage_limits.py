"""
Tests for storage limits API endpoints.

Tests the FastAPI endpoints for storage limit functionality.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock

from fastapi.testclient import TestClient
from robosystems.config.graph_tier import GraphTier
from robosystems.models.iam.graph_credits import GraphCredits
from robosystems.models.iam.graph_usage import (
  GraphUsage,
  UsageEventType,
)


@pytest.fixture
def client_with_test_user(test_db, test_user):
  """Create a test client with the actual test user."""
  from main import app
  from robosystems.middleware.auth.dependencies import (
    get_current_user,
    get_current_user_with_graph,
  )
  from robosystems.middleware.rate_limits import (
    subscription_aware_rate_limit_dependency,
  )
  from robosystems.database import get_db_session

  # Use the test_user from the fixture
  mock_user = Mock()
  mock_user.id = test_user.id
  mock_user.name = test_user.name
  mock_user.email = test_user.email
  mock_user.accounts = []

  # Override the dependencies
  app.dependency_overrides[get_current_user] = lambda: mock_user
  app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

  # Override the database session dependency
  def override_get_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db

  client = TestClient(app)
  yield client

  # Reset the dependency overrides
  app.dependency_overrides = {}


class TestStorageLimitsAPI:
  """Test storage limits API endpoints."""

  def test_get_storage_limits_success(
    self, client_with_test_user: TestClient, sample_graph_credits, db_session, test_user
  ):
    """Test successful storage limits retrieval."""
    # Create usage record (30GB = 30% of 100GB, below 80% threshold)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="api_usage_1",
      graph_id=sample_graph_credits.graph_id,
      user_id=sample_graph_credits.user_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=sample_graph_credits.graph_tier.value
      if hasattr(sample_graph_credits.graph_tier, "value")
      else sample_graph_credits.graph_tier,
      storage_gb=Decimal("30"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
    )

    assert response.status_code == 200
    data = response.json()

    assert data["graph_id"] == sample_graph_credits.graph_id
    assert data["current_storage_gb"] == 30.0
    assert data["effective_limit_gb"] == 100.0
    assert data["usage_percentage"] == 30.0
    assert data["within_limit"] is True
    assert data["approaching_limit"] is False
    assert data["has_override"] is False

  def test_get_storage_limits_approaching_threshold(
    self, client_with_test_user: TestClient, sample_graph_credits, db_session
  ):
    """Test storage limits when approaching threshold."""
    # Create usage record at 90% (90GB of 100GB)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="api_usage_2",
      graph_id=sample_graph_credits.graph_id,
      user_id=sample_graph_credits.user_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=sample_graph_credits.graph_tier.value
      if hasattr(sample_graph_credits.graph_tier, "value")
      else sample_graph_credits.graph_tier,
      storage_gb=Decimal("90"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
    )

    assert response.status_code == 200
    data = response.json()

    assert data["usage_percentage"] == 90.0
    assert data["within_limit"] is True
    assert data["approaching_limit"] is True
    assert data["needs_warning"] is True
    assert "recommendations" in data
    assert len(data["recommendations"]) > 0

  def test_get_storage_limits_exceeds_limit(
    self, client_with_test_user: TestClient, sample_graph_credits, db_session
  ):
    """Test storage limits when exceeding limit."""
    # Create usage record that exceeds limit (120GB > 100GB)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="api_usage_3",
      graph_id=sample_graph_credits.graph_id,
      user_id=sample_graph_credits.user_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=sample_graph_credits.graph_tier.value
      if hasattr(sample_graph_credits.graph_tier, "value")
      else sample_graph_credits.graph_tier,
      storage_gb=Decimal("120"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
    )

    assert response.status_code == 200
    data = response.json()

    assert data["usage_percentage"] == 120.0
    assert data["within_limit"] is False
    assert data["approaching_limit"] is True
    assert "recommendations" in data
    assert any("Contact support" in rec for rec in data["recommendations"])

  def test_get_storage_limits_with_override(
    self, client_with_test_user: TestClient, sample_graph_credits, db_session
  ):
    """Test storage limits with admin override."""
    # Set override limit to 200GB
    sample_graph_credits.storage_override_gb = Decimal("200")
    db_session.commit()

    # Create usage record (120GB = 60% of 200GB override)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="api_usage_4",
      graph_id=sample_graph_credits.graph_id,
      user_id=sample_graph_credits.user_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=sample_graph_credits.graph_tier.value
      if hasattr(sample_graph_credits.graph_tier, "value")
      else sample_graph_credits.graph_tier,
      storage_gb=Decimal("120"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
    )

    assert response.status_code == 200
    data = response.json()

    assert data["effective_limit_gb"] == 200.0
    assert data["usage_percentage"] == 60.0
    assert data["within_limit"] is True
    assert data["has_override"] is True

  def test_get_storage_limits_no_usage_data(
    self, client_with_test_user: TestClient, sample_graph_credits
  ):
    """Test storage limits with no usage data."""
    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
    )

    assert response.status_code == 200
    data = response.json()

    assert data["current_storage_gb"] == 0.0
    assert data["usage_percentage"] == 0.0
    assert data["within_limit"] is True

  def test_get_storage_limits_no_credit_pool(self, client_with_test_user: TestClient):
    """Test storage limits for non-existent graph."""
    response = client_with_test_user.get(
      "/v1/graphs/nonexistent_graph/credits/storage/limits"
    )

    # Access check happens before credit pool check, so we get 403 instead of 404
    assert response.status_code == 403

  def test_get_storage_limits_access_denied(
    self, client: TestClient, sample_graph_credits
  ):
    """Test storage limits without authentication."""
    response = client.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits"
    )

    assert response.status_code == 401

  def test_get_storage_limits_server_error(
    self, client_with_test_user: TestClient, sample_graph_credits
  ):
    """Test storage limits with server error."""
    with patch(
      "robosystems.operations.graph.credit_service.CreditService.check_storage_limit"
    ) as mock_check:
      mock_check.side_effect = Exception("Database error")

      response = client_with_test_user.get(
        f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/limits",
      )

      assert response.status_code == 500
      response_data = response.json()
      # Check if it's a nested structure like the error above
      if isinstance(response_data.get("detail"), dict):
        assert response_data["detail"]["detail"] == "Failed to check storage limits"
      else:
        assert response_data["detail"] == "Failed to check storage limits"


class TestStorageUsageAPI:
  """Test storage usage API endpoints."""

  def test_get_storage_usage_success(
    self, client_with_test_user: TestClient, sample_graph_credits, db_session
  ):
    """Test successful storage usage retrieval."""
    # Create some usage records
    base_date = datetime.now(timezone.utc)
    for i in range(5):
      record_time = base_date - timedelta(days=i)
      usage_record = GraphUsage(
        id=f"usage_api_{i}",
        graph_id=sample_graph_credits.graph_id,
        user_id=sample_graph_credits.user_id,
        event_type=UsageEventType.STORAGE_SNAPSHOT.value,
        graph_tier=sample_graph_credits.graph_tier.value
        if hasattr(sample_graph_credits.graph_tier, "value")
        else sample_graph_credits.graph_tier,
        storage_gb=Decimal(str(100 + i * 10)),
        recorded_at=record_time,
        billing_year=record_time.year,
        billing_month=record_time.month,
        billing_day=record_time.day,
        billing_hour=record_time.hour,
      )
      db_session.add(usage_record)
    db_session.commit()

    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/usage",
      params={"days": 7},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["graph_id"] == sample_graph_credits.graph_id
    assert data["graph_tier"] == "kuzu-standard"
    assert data["storage_multiplier"] == 1.0
    assert data["base_storage_cost_per_gb"] == 10.0
    assert len(data["recent_usage"]) >= 4  # May have 4-5 records due to date filtering
    assert "summary" in data
    assert data["summary"]["total_storage_credits"] > 0

  def test_get_storage_usage_different_days(
    self, client_with_test_user: TestClient, sample_graph_credits
  ):
    """Test storage usage with different day parameters."""
    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/usage",
      params={"days": 90},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["period"]["days_requested"] == 90

  def test_get_storage_usage_no_data(
    self, client_with_test_user: TestClient, sample_graph_credits
  ):
    """Test storage usage with no usage data."""
    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/usage"
    )

    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["total_storage_credits"] == 0.0
    assert data["summary"]["average_daily_storage_gb"] == 0.0
    assert len(data["recent_usage"]) == 0

  def test_get_storage_usage_access_denied(
    self, client: TestClient, sample_graph_credits
  ):
    """Test storage usage without authentication."""
    response = client.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/usage"
    )

    assert response.status_code == 401

  def test_get_storage_usage_normal_case(
    self, client_with_test_user: TestClient, sample_graph_credits
  ):
    """Test storage usage endpoint works normally."""
    response = client_with_test_user.get(
      f"/v1/graphs/{sample_graph_credits.graph_id}/credits/storage/usage",
    )

    # Should succeed even with no data
    assert response.status_code == 200
    data = response.json()
    assert "summary" in data
    assert "recent_usage" in data


@pytest.fixture
def sample_graph_credits(db_session, test_user, test_org):
  """Create sample graph credits for API testing."""
  import uuid

  # First create the graph
  from robosystems.models.iam import Graph, GraphUser

  graph_id = f"api_test_graph_{uuid.uuid4().hex[:8]}"
  Graph.create(
    graph_id=graph_id,
    org_id=test_org.id,
    graph_name="Test Graph",
    graph_type="generic",
    session=db_session,
    graph_tier=GraphTier.KUZU_STANDARD,
  )

  # Create GraphUser to give the test user access
  GraphUser.create(
    user_id=test_user.id,
    graph_id=graph_id,
    role="admin",
    session=db_session,
  )

  credits = GraphCredits.create_for_graph(
    graph_id=graph_id,
    user_id=test_user.id,
    billing_admin_id=test_user.id,
    monthly_allocation=Decimal("1000"),
    session=db_session,
  )
  return credits


@pytest.fixture
def sample_user(db_session):
  """Create sample user for API testing."""
  import uuid
  from robosystems.models.iam import User

  user = User(
    id=f"api_test_user_{uuid.uuid4().hex[:8]}",
    email=f"apitest{uuid.uuid4().hex[:8]}@example.com",
    name="API Test User",
    password_hash="hashed_password",
    is_active=True,
  )
  db_session.add(user)
  db_session.commit()
  return user
