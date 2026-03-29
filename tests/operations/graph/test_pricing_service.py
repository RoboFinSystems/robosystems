from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.graph.pricing_service import GraphPricingService

MOCK_USER_ID = "usr_test123"
MOCK_GRAPH_ID = "kg_test456"

SAMPLE_PLAN = {
  "name": "ladybug-standard",
  "display_name": "LadybugDB Standard",
  "base_price_cents": 4900,
}

SAMPLE_LARGE_PLAN = {
  "name": "ladybug-large",
  "display_name": "LadybugDB Large",
  "base_price_cents": 19900,
}


@pytest.fixture
def mock_session():
  return MagicMock()


@pytest.fixture
def pricing_service(mock_session):
  return GraphPricingService(mock_session)


class TestGetSubscriptionPlan:
  @pytest.mark.unit
  def test_no_customer_returns_default_plan(self, pricing_service):
    with (
      patch(
        "robosystems.models.core.billing.customer.BillingCustomer.get_by_user_id",
        return_value=None,
      ),
      patch(
        "robosystems.config.billing.BillingConfig.get_subscription_plan",
        return_value=SAMPLE_PLAN,
      ) as mock_get_plan,
    ):
      result = pricing_service.get_subscription_plan(MOCK_USER_ID, MOCK_GRAPH_ID)

      mock_get_plan.assert_called_with("ladybug-standard")
      assert result == SAMPLE_PLAN

  @pytest.mark.unit
  def test_with_active_subscription(self, pricing_service, mock_session):
    mock_customer = MagicMock()
    mock_customer.org_id = "org_123"

    mock_subscription = MagicMock()
    mock_subscription.plan_name = "ladybug-large"

    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    with (
      patch(
        "robosystems.models.core.billing.customer.BillingCustomer.get_by_user_id",
        return_value=mock_customer,
      ),
      patch(
        "robosystems.config.billing.BillingConfig.get_subscription_plan",
        return_value=SAMPLE_LARGE_PLAN,
      ) as mock_get_plan,
    ):
      result = pricing_service.get_subscription_plan(MOCK_USER_ID, MOCK_GRAPH_ID)

      mock_get_plan.assert_called_with("ladybug-large")
      assert result == SAMPLE_LARGE_PLAN

  @pytest.mark.unit
  def test_no_active_subscription_returns_default(self, pricing_service, mock_session):
    mock_customer = MagicMock()
    mock_customer.org_id = "org_123"

    mock_session.query.return_value.filter.return_value.first.return_value = None

    with (
      patch(
        "robosystems.models.core.billing.customer.BillingCustomer.get_by_user_id",
        return_value=mock_customer,
      ),
      patch(
        "robosystems.config.billing.BillingConfig.get_subscription_plan",
        return_value=SAMPLE_PLAN,
      ) as mock_get_plan,
    ):
      result = pricing_service.get_subscription_plan(MOCK_USER_ID, MOCK_GRAPH_ID)

      mock_get_plan.assert_called_with("ladybug-standard")
      assert result == SAMPLE_PLAN

  @pytest.mark.unit
  def test_subscription_with_no_plan_name_returns_default(
    self, pricing_service, mock_session
  ):
    mock_customer = MagicMock()
    mock_customer.org_id = "org_123"

    mock_subscription = MagicMock()
    mock_subscription.plan_name = None

    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    with (
      patch(
        "robosystems.models.core.billing.customer.BillingCustomer.get_by_user_id",
        return_value=mock_customer,
      ),
      patch(
        "robosystems.config.billing.BillingConfig.get_subscription_plan",
        return_value=SAMPLE_PLAN,
      ) as mock_get_plan,
    ):
      result = pricing_service.get_subscription_plan(MOCK_USER_ID, MOCK_GRAPH_ID)

      mock_get_plan.assert_called_with("ladybug-standard")
      assert result == SAMPLE_PLAN


class TestCalculateGraphMonthlyBill:
  @pytest.mark.unit
  def test_no_usage_returns_base_cost(self, pricing_service, mock_session):
    mock_session.query.return_value.filter.return_value.all.return_value = []

    with patch.object(
      pricing_service, "get_subscription_plan", return_value=SAMPLE_PLAN
    ):
      result = pricing_service.calculate_graph_monthly_bill(
        MOCK_USER_ID, MOCK_GRAPH_ID, 2024, 6
      )

    assert result["graph_id"] == MOCK_GRAPH_ID
    assert result["billing_period"] == {"year": 2024, "month": 6}
    assert result["plan"]["name"] == "ladybug-standard"
    assert result["plan"]["display_name"] == "LadybugDB Standard"
    assert result["usage"]["total_gb_hours"] == 0
    assert result["usage"]["total_queries"] == 0
    assert result["charges"]["base_monthly"] == 49.0
    assert result["charges"]["total"] == 49.0
    assert "generated_at" in result

  @pytest.mark.unit
  def test_with_usage_records(self, pricing_service, mock_session):
    record1 = MagicMock()
    record1.size_gb = 1.5
    record1.query_count = 100

    record2 = MagicMock()
    record2.size_gb = 2.0
    record2.query_count = 200

    mock_session.query.return_value.filter.return_value.all.return_value = [
      record1,
      record2,
    ]

    with patch.object(
      pricing_service, "get_subscription_plan", return_value=SAMPLE_PLAN
    ):
      result = pricing_service.calculate_graph_monthly_bill(
        MOCK_USER_ID, MOCK_GRAPH_ID, 2024, 6
      )

    assert result["usage"]["total_gb_hours"] == 3.5
    assert result["usage"]["avg_size_gb"] == 1.75
    assert result["usage"]["max_size_gb"] == 2.0
    assert result["usage"]["total_queries"] == 300
    assert result["usage"]["measurement_count"] == 2
    assert result["charges"]["base_monthly"] == 49.0

  @pytest.mark.unit
  def test_no_plan_raises_value_error(self, pricing_service):
    with patch.object(pricing_service, "get_subscription_plan", return_value=None):
      with pytest.raises(ValueError, match="No billing plan found"):
        pricing_service.calculate_graph_monthly_bill(
          MOCK_USER_ID, MOCK_GRAPH_ID, 2024, 6
        )

  @pytest.mark.unit
  def test_large_plan_pricing(self, pricing_service, mock_session):
    mock_session.query.return_value.filter.return_value.all.return_value = []

    with patch.object(
      pricing_service, "get_subscription_plan", return_value=SAMPLE_LARGE_PLAN
    ):
      result = pricing_service.calculate_graph_monthly_bill(
        MOCK_USER_ID, MOCK_GRAPH_ID, 2024, 6
      )

    assert result["charges"]["base_monthly"] == 199.0
    assert result["charges"]["total"] == 199.0


class TestCalculateUsageMetrics:
  @pytest.mark.unit
  def test_aggregation(self, pricing_service):
    records = []
    for size, queries in [(1.0, 50), (2.0, 100), (3.0, 150)]:
      r = MagicMock()
      r.size_gb = size
      r.query_count = queries
      records.append(r)

    result = pricing_service._calculate_usage_metrics(records)

    assert result["total_gb_hours"] == 6.0
    assert result["avg_size_gb"] == 2.0
    assert result["max_size_gb"] == 3.0
    assert result["total_queries"] == 300
    assert result["measurement_count"] == 3

  @pytest.mark.unit
  def test_single_record(self, pricing_service):
    r = MagicMock()
    r.size_gb = 1.5
    r.query_count = 42

    result = pricing_service._calculate_usage_metrics([r])

    assert result["total_gb_hours"] == 1.5
    assert result["avg_size_gb"] == 1.5
    assert result["max_size_gb"] == 1.5
    assert result["total_queries"] == 42
    assert result["measurement_count"] == 1
