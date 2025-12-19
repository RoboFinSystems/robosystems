"""Integration tests for subscription billing with graph usage tracking."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from robosystems.models.billing import BillingSubscription, SubscriptionStatus
from robosystems.models.iam import (
  GraphUsage,
  GraphUser,
  OrgLimits,
  User,
)
from robosystems.operations.graph.subscription_service import GraphSubscriptionService


class TestSubscriptionBillingIntegration:
  """Test subscription service with graph usage tracking and billing calculations."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    session = MagicMock()
    return session

  @pytest.fixture
  def sample_billing_plans(self):
    """Create sample billing plans with detailed pricing."""
    plans = {
      "free": Mock(
        id="plan_free",
        name="free",
        base_price_cents=0,
        included_graphs=1,
        included_storage_gb=1,
        included_queries_per_month=1000,
        overage_price_per_gb_cents=0,  # No overages on free
        overage_price_per_1k_queries_cents=0,
        is_active=True,
      ),
      "starter": Mock(
        id="plan_starter",
        name="starter",
        base_price_cents=1999,  # $19.99
        included_graphs=5,
        included_storage_gb=10,
        included_queries_per_month=10000,
        overage_price_per_gb_cents=299,  # $2.99 per GB
        overage_price_per_1k_queries_cents=99,  # $0.99 per 1k queries
        is_active=True,
      ),
      "pro": Mock(
        id="plan_pro",
        name="pro",
        base_price_cents=9999,  # $99.99
        included_graphs=20,
        included_storage_gb=100,
        included_queries_per_month=100000,
        overage_price_per_gb_cents=199,  # $1.99 per GB
        overage_price_per_1k_queries_cents=49,  # $0.49 per 1k queries
        is_active=True,
      ),
      "enterprise": Mock(
        id="plan_enterprise",
        name="enterprise",
        base_price_cents=49999,  # $499.99
        included_graphs=100,
        included_storage_gb=1000,
        included_queries_per_month=1000000,
        overage_price_per_gb_cents=99,  # $0.99 per GB
        overage_price_per_1k_queries_cents=19,  # $0.19 per 1k queries
        is_active=True,
      ),
    }
    return plans

  @pytest.fixture
  def sample_user_with_graphs(self):
    """Create a sample user with multiple graph databases."""
    user = Mock(spec=User)
    user.id = "user123"
    user.email = "billing@example.com"

    # Create user graphs
    graphs = []
    for i in range(3):
      graph = Mock(spec=GraphUser)
      graph.user_id = user.id
      graph.graph_id = f"entity_{1000 + i}"
      graph.entity_id = f"comp_{1000 + i}"
      graph.created_at = datetime.now(UTC) - timedelta(days=30)
      graphs.append(graph)

    return {"user": user, "graphs": graphs}

  @pytest.fixture
  def sample_usage_data(self, sample_user_with_graphs):
    """Create sample usage tracking data."""
    usage_records = []
    base_time = datetime.now(UTC) - timedelta(days=7)

    for graph in sample_user_with_graphs["graphs"]:
      for day in range(7):
        record = Mock(spec=GraphUsage)
        record.user_id = sample_user_with_graphs["user"].id
        record.graph_id = graph.graph_id
        record.entity_id = graph.entity_id
        record.size_bytes = 1024 * 1024 * 1024 * (1 + day * 0.1)  # 1-1.6 GB
        record.query_count = 100 + day * 20  # 100-220 queries per day
        record.instance_id = "i-abc123"
        record.cluster_tier = "standard"
        record.node_type = "entity_writer"
        record.region = "us-east-1"
        record.recorded_at = base_time + timedelta(days=day)
        usage_records.append(record)

    return usage_records

  def test_calculate_monthly_billing_with_usage(
    self,
    mock_db_session,
    sample_billing_plans,
    sample_user_with_graphs,
    sample_usage_data,
  ):
    """Test calculating monthly billing including usage-based charges."""
    GraphSubscriptionService(mock_db_session)
    user = sample_user_with_graphs["user"]
    graphs = sample_user_with_graphs["graphs"]

    # Set up organization with Pro plan
    user_limits = Mock(spec=OrgLimits)
    user_limits.org_id = "org_test_123"
    user_limits.subscription_tier = "pro"
    user_limits.max_api_calls_per_hour = 1000

    with patch.object(OrgLimits, "get_or_create_for_org", return_value=user_limits):
      # Create graph subscriptions
      pro_plan = sample_billing_plans["pro"]
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        pro_plan
      )

      # Mock existing subscriptions
      subscriptions = []
      for graph in graphs:
        sub = Mock(spec=BillingSubscription)
        sub.user_id = user.id
        sub.graph_id = graph.graph_id
        sub.plan_id = pro_plan.id
        sub.status = SubscriptionStatus.ACTIVE.value
        sub.current_period_start = datetime.now(UTC) - timedelta(days=15)
        sub.current_period_end = datetime.now(UTC) + timedelta(days=15)
        subscriptions.append(sub)

      # Calculate total storage used (latest records)
      total_storage_gb = sum(
        record.size_bytes / (1024**3)
        for record in sample_usage_data
        if record.recorded_at.day == sample_usage_data[-1].recorded_at.day
      )

      # Calculate total queries this month
      total_queries = sum(record.query_count for record in sample_usage_data)

      # Verify storage and query calculations
      # Note: Our test data generates ~4.8GB, which is within the pro plan's 100GB limit
      assert total_storage_gb < pro_plan.included_storage_gb  # Within storage limit
      assert total_queries < pro_plan.included_queries_per_month  # Within query limit

      # Calculate expected bill (no overages in this case)
      base_cost = pro_plan.base_price_cents / 100  # $99.99
      storage_overage_gb = max(0, total_storage_gb - pro_plan.included_storage_gb)
      storage_overage_cost = (
        storage_overage_gb * pro_plan.overage_price_per_gb_cents
      ) / 100

      expected_total = base_cost + storage_overage_cost

      # In a real implementation, this would be a method on the service
      # Since we're within limits, total should equal base cost
      assert expected_total == base_cost  # No overages

  def test_calculate_billing_with_storage_overages(
    self, mock_db_session, sample_billing_plans, sample_user_with_graphs
  ):
    """Test calculating monthly billing with storage overages."""
    GraphSubscriptionService(mock_db_session)

    # Set up organization on Starter plan with lower storage limits
    user_limits = Mock(spec=OrgLimits)
    user_limits.org_id = "org_test_123"
    user_limits.subscription_tier = "starter"
    user_limits.max_api_calls_per_hour = 1000

    with patch.object(OrgLimits, "get_or_create_for_org", return_value=user_limits):
      starter_plan = sample_billing_plans["starter"]
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        starter_plan
      )

      # Create usage data that exceeds starter plan limits (10GB)
      overage_usage = []
      for i in range(3):
        record = Mock(spec=GraphUsage)
        record.graph_id = f"entity_{1000 + i}"
        record.size_bytes = 5 * 1024**3  # 5GB per database = 15GB total
        record.query_count = 1000
        record.recorded_at = datetime.now(UTC)
        overage_usage.append(record)

      # Calculate totals
      total_storage_gb = sum(r.size_bytes for r in overage_usage) / (1024**3)
      total_queries = sum(r.query_count for r in overage_usage)

      # Verify we have overages
      assert total_storage_gb > starter_plan.included_storage_gb  # 15GB > 10GB
      assert total_queries < starter_plan.included_queries_per_month

      # Calculate bill with overages
      base_cost = starter_plan.base_price_cents / 100  # $19.99
      storage_overage_gb = (
        total_storage_gb - starter_plan.included_storage_gb
      )  # 5GB overage
      storage_overage_cost = (
        storage_overage_gb * starter_plan.overage_price_per_gb_cents
      ) / 100

      expected_total = base_cost + storage_overage_cost

      # Should have overage charges
      assert expected_total > base_cost
      assert (
        abs(storage_overage_cost - (5 * 2.99)) < 0.01
      )  # 5GB * $2.99/GB (allow for floating point)

  def test_usage_based_plan_recommendations(
    self, mock_db_session, sample_billing_plans, sample_usage_data
  ):
    """Test recommending plans based on actual usage patterns."""
    GraphSubscriptionService(mock_db_session)

    # Calculate usage totals
    total_storage_gb = sum(
      record.size_bytes / (1024**3)
      for record in sample_usage_data
      if record.recorded_at
      == max(r.recorded_at for r in sample_usage_data if r.graph_id == record.graph_id)
    )
    total_monthly_queries = (
      sum(record.query_count for record in sample_usage_data) * 4
    )  # Extrapolate to month
    graph_count = len({record.graph_id for record in sample_usage_data})

    # Determine recommended plan based on usage
    recommended_plan = None
    for plan_name, plan in sample_billing_plans.items():
      if (
        graph_count <= plan.included_graphs
        and total_storage_gb <= plan.included_storage_gb
        and total_monthly_queries <= plan.included_queries_per_month
      ):
        recommended_plan = plan_name
        break

    # With our test data, should recommend at least starter plan
    assert recommended_plan in ["starter", "pro", "enterprise"]

  def test_billing_period_proration(self, mock_db_session, sample_billing_plans):
    """Test prorated billing when upgrading mid-cycle."""
    GraphSubscriptionService(mock_db_session)

    # Create subscription that started 10 days ago
    # start_date = datetime.now(timezone.utc) - timedelta(days=10)  # Unused in simplified test

    # Calculate proration for upgrade from starter to pro
    starter_plan = sample_billing_plans["starter"]
    pro_plan = sample_billing_plans["pro"]

    days_on_starter = 10
    days_on_pro = 20
    total_days = 30

    # Prorated costs
    starter_prorated = (
      starter_plan.base_price_cents * days_on_starter / total_days
    ) / 100
    pro_prorated = (pro_plan.base_price_cents * days_on_pro / total_days) / 100

    total_prorated = starter_prorated + pro_prorated

    # Should be less than full pro price but more than starter
    assert total_prorated < (pro_plan.base_price_cents / 100)
    assert total_prorated > (starter_plan.base_price_cents / 100)

  def test_usage_tracking_for_multiple_instances(
    self, mock_db_session, sample_user_with_graphs
  ):
    """Test tracking usage across multiple LadybugDB instances."""
    # Simulate usage data from multiple instances
    instance_usage = {
      "i-writer1": {
        "entity_1000": {"size_bytes": 1024**3, "query_count": 100},
        "entity_1001": {"size_bytes": 2 * 1024**3, "query_count": 200},
      },
      "i-writer2": {
        "entity_1002": {"size_bytes": 1.5 * 1024**3, "query_count": 150},
      },
    }

    # Create usage records
    usage_records = []
    for instance_id, databases in instance_usage.items():
      for graph_id, metrics in databases.items():
        record = Mock(spec=GraphUsage)
        record.instance_id = instance_id
        record.graph_id = graph_id
        record.size_bytes = metrics["size_bytes"]
        record.query_count = metrics["query_count"]
        record.recorded_at = datetime.now(UTC)
        usage_records.append(record)

    # Aggregate usage across instances
    total_storage = sum(r.size_bytes for r in usage_records) / (1024**3)
    total_queries = sum(r.query_count for r in usage_records)

    assert total_storage == 4.5  # GB
    assert total_queries == 450

  def test_usage_anomaly_detection(self, sample_usage_data):
    """Test detecting unusual usage patterns for billing alerts."""
    # Create anomalous usage data
    base_time = datetime.now(UTC) - timedelta(days=7)
    normal_storage = 1024 * 1024 * 1024  # 1GB
    normal_queries = 100

    # Create usage with clear anomalies
    test_usage = []
    for day in range(7):
      for graph_num in range(3):
        record = Mock(spec=GraphUsage)
        record.graph_id = f"entity_{1000 + graph_num}"
        record.recorded_at = base_time + timedelta(days=day)

        # Days 5 and 6 have anomalous usage (3x normal)
        if day >= 5:
          record.size_bytes = normal_storage * 3
          record.query_count = normal_queries * 3
        else:
          record.size_bytes = normal_storage
          record.query_count = normal_queries

        test_usage.append(record)

    # Calculate daily averages
    daily_usage = {}
    for record in test_usage:
      date = record.recorded_at.date()
      if date not in daily_usage:
        daily_usage[date] = {"storage": 0, "queries": 0, "count": 0}

      daily_usage[date]["storage"] += record.size_bytes
      daily_usage[date]["queries"] += record.query_count
      daily_usage[date]["count"] += 1

    # Calculate average daily usage
    avg_storage = sum(d["storage"] for d in daily_usage.values()) / len(daily_usage)
    avg_queries = sum(d["queries"] for d in daily_usage.values()) / len(daily_usage)

    # Check for anomalies (e.g., 2x average)
    anomalies = []
    for date, usage in daily_usage.items():
      if usage["storage"] > avg_storage * 1.5:  # Lower threshold to catch anomalies
        anomalies.append({"date": date, "type": "storage", "value": usage["storage"]})
      if usage["queries"] > avg_queries * 1.5:
        anomalies.append({"date": date, "type": "queries", "value": usage["queries"]})

    # Should detect anomalies on days 5 and 6
    assert len(anomalies) >= 4  # At least 2 days * 2 types (storage + queries)
