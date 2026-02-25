"""Unit tests for billing API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.billing.checkout import (
  CheckoutResponse,
  CheckoutStatusResponse,
  CreateCheckoutRequest,
)
from robosystems.models.api.billing.credits import (
  CreditCheckRequest,
  CreditErrorCode,
  CreditSummaryResponse,
  CreditTransactionResponse,
  DetailedTransactionsResponse,
  EnhancedCreditTransactionResponse,
  StandardizedCreditError,
  StorageLimitResponse,
  create_access_denied_error,
  create_insufficient_credits_error,
)
from robosystems.models.api.billing.offering import (
  GraphSubscriptionTier,
  OperationCosts,
  ServiceOfferingSummary,
  TokenPricing,
)
from robosystems.models.api.billing.subscription import (
  AddOnCreditInfo,
  AllocationResponse,
  AllocationResult,
  AvailableRepository,
  CreditsSummaryResponse,
  CreditSummary,
  RepositoryCreditsResponse,
  RepositoryPlanInfo,
  SubscriptionInfo,
  TierUpgradeRequest,
  UserSubscriptionsResponse,
)
from robosystems.models.api.billing.upgrade import UpgradeSubscriptionRequest


@pytest.mark.unit
class TestCreditCheckRequest:
  def test_valid_request(self):
    model = CreditCheckRequest(operation_type="agent_query")
    assert model.operation_type == "agent_query"
    assert model.base_cost is None

  def test_with_base_cost(self):
    model = CreditCheckRequest(operation_type="agent_query", base_cost=5.0)
    assert model.base_cost == 5.0

  def test_operation_type_required(self):
    with pytest.raises(ValidationError):
      CreditCheckRequest()  # type: ignore[call-arg]


@pytest.mark.unit
class TestCreditSummaryResponse:
  def test_valid_response(self):
    model = CreditSummaryResponse(
      graph_id="kg123",
      graph_tier="ladybug-standard",
      current_balance=7500.0,
      monthly_allocation=8000.0,
      consumed_this_month=500.0,
      transaction_count=25,
      usage_percentage=6.25,
    )
    assert model.current_balance == 7500.0
    assert model.last_allocation_date is None


@pytest.mark.unit
class TestCreditTransactionResponse:
  def test_valid_response(self):
    model = CreditTransactionResponse(
      id="tx_123",
      type="consumption",
      amount=-5.0,
      description="Agent query",
      metadata={"agent": "financial"},
      created_at="2024-01-15T10:00:00Z",
    )
    assert model.amount == -5.0


@pytest.mark.unit
class TestStorageLimitResponse:
  def test_valid_response(self):
    model = StorageLimitResponse(
      graph_id="kg123",
      current_storage_gb=2.5,
      effective_limit_gb=10.0,
      usage_percentage=25.0,
      within_limit=True,
      approaching_limit=False,
      needs_warning=False,
      has_override=False,
    )
    assert model.within_limit is True
    assert model.recommendations is None

  def test_with_recommendations(self):
    model = StorageLimitResponse(
      graph_id="kg123",
      current_storage_gb=9.0,
      effective_limit_gb=10.0,
      usage_percentage=90.0,
      within_limit=True,
      approaching_limit=True,
      needs_warning=True,
      has_override=False,
      recommendations=["Consider upgrading", "Archive old data"],
    )
    assert len(model.recommendations) == 2


@pytest.mark.unit
class TestEnhancedCreditTransactionResponse:
  def test_minimal_response(self):
    model = EnhancedCreditTransactionResponse(
      id="tx_123",
      type="consumption",
      amount=-5.0,
      description="Query",
      metadata={},
      created_at="2024-01-01T00:00:00Z",
    )
    assert model.operation_id is None
    assert model.idempotency_key is None

  def test_full_response(self):
    model = EnhancedCreditTransactionResponse(
      id="tx_123",
      type="consumption",
      amount=-5.0,
      description="Query",
      metadata={"agent": "financial"},
      created_at="2024-01-01T00:00:00Z",
      operation_id="op_abc",
      idempotency_key="idem_123",
      request_id="req_456",
      user_id="user_789",
    )
    assert model.operation_id == "op_abc"


@pytest.mark.unit
class TestDetailedTransactionsResponse:
  def test_valid_response(self):
    model = DetailedTransactionsResponse(
      transactions=[],
      summary={},
      total_count=0,
      filtered_count=0,
      date_range={"start": "2024-01-01", "end": "2024-01-31"},
    )
    assert model.total_count == 0


@pytest.mark.unit
class TestCreditErrorCode:
  def test_enum_values(self):
    assert CreditErrorCode.INSUFFICIENT_CREDITS == "insufficient_credits"
    assert CreditErrorCode.ACCESS_DENIED == "access_denied"
    assert CreditErrorCode.CREDIT_SYSTEM_ERROR == "credit_system_error"
    assert CreditErrorCode.INVALID_OPERATION == "invalid_operation"


@pytest.mark.unit
class TestStandardizedCreditError:
  def test_creation(self):
    error = StandardizedCreditError(
      message="Not enough credits",
      error_code=CreditErrorCode.INSUFFICIENT_CREDITS,
    )
    assert error.message == "Not enough credits"
    assert error.error_code == CreditErrorCode.INSUFFICIENT_CREDITS
    assert error.metadata == {}

  def test_with_metadata(self):
    error = StandardizedCreditError(
      message="Error",
      error_code=CreditErrorCode.ACCESS_DENIED,
      metadata={"resource": "graph_123"},
    )
    assert error.metadata == {"resource": "graph_123"}

  def test_to_http_detail(self):
    error = StandardizedCreditError(
      message="Insufficient",
      error_code=CreditErrorCode.INSUFFICIENT_CREDITS,
      metadata={"required": 100, "available": 50},
    )
    detail = error.to_http_detail()
    assert detail["message"] == "Insufficient"
    assert detail["error_code"] == "insufficient_credits"
    assert detail["metadata"]["required"] == 100

  def test_is_exception(self):
    error = StandardizedCreditError(
      message="Test",
      error_code=CreditErrorCode.CREDIT_SYSTEM_ERROR,
    )
    assert isinstance(error, Exception)
    assert str(error) == "Test"


@pytest.mark.unit
class TestCreateInsufficientCreditsError:
  def test_error_message(self):
    error = create_insufficient_credits_error(100.0, 50.0, "agent query")
    assert "100.0" in error.message
    assert "50.0" in error.message
    assert "agent query" in error.message
    assert error.error_code == CreditErrorCode.INSUFFICIENT_CREDITS

  def test_error_metadata(self):
    error = create_insufficient_credits_error(75.0, 25.0, "research")
    assert error.metadata["required_credits"] == 75.0
    assert error.metadata["available_credits"] == 25.0
    assert error.metadata["operation"] == "research"


@pytest.mark.unit
class TestCreateAccessDeniedError:
  def test_without_reason(self):
    error = create_access_denied_error("query", "graph_123")
    assert "query" in error.message
    assert "graph_123" in error.message
    assert error.error_code == CreditErrorCode.ACCESS_DENIED

  def test_with_reason(self):
    error = create_access_denied_error("query", "graph_123", reason="No subscription")
    assert "No subscription" in error.message
    assert error.metadata["reason"] == "No subscription"


@pytest.mark.unit
class TestCreateCheckoutRequest:
  def test_valid_request(self):
    model = CreateCheckoutRequest(
      plan_name="ladybug-standard",
      resource_type="graph",
      resource_config={"graph_name": "My Graph"},
    )
    assert model.plan_name == "ladybug-standard"
    assert model.resource_type == "graph"

  def test_repository_resource(self):
    model = CreateCheckoutRequest(
      plan_name="sec-starter",
      resource_type="repository",
      resource_config={"repository_name": "sec"},
    )
    assert model.resource_type == "repository"


@pytest.mark.unit
class TestCheckoutResponse:
  def test_defaults(self):
    model = CheckoutResponse()
    assert model.checkout_url is None
    assert model.session_id is None
    assert model.subscription_id is None
    assert model.requires_checkout is True
    assert model.billing_disabled is False

  def test_billing_disabled(self):
    model = CheckoutResponse(
      billing_disabled=True,
      requires_checkout=False,
      subscription_id="sub_123",
    )
    assert model.billing_disabled is True


@pytest.mark.unit
class TestCheckoutStatusResponse:
  def test_valid_response(self):
    model = CheckoutStatusResponse(
      status="active",
      subscription_id="sub_123",
      resource_id="kg123",
    )
    assert model.status == "active"
    assert model.error is None


@pytest.mark.unit
class TestUpgradeSubscriptionRequest:
  def test_valid_request(self):
    model = UpgradeSubscriptionRequest(plan_name="ladybug-large")
    assert model.plan_name == "ladybug-large"


@pytest.mark.unit
class TestSubscriptionModels:
  def test_repository_plan_info(self):
    model = RepositoryPlanInfo(
      plan="sec-starter",
      name="SEC Starter",
      monthly_price=9.99,
      monthly_credits=1000,
      features=["Read access", "Basic queries"],
    )
    assert model.plan == "sec-starter"

  def test_available_repository(self):
    plan = RepositoryPlanInfo(
      plan="sec-starter",
      name="Starter",
      monthly_price=9.99,
      monthly_credits=1000,
      features=["Basic"],
    )
    model = AvailableRepository(
      type="sec",
      name="SEC EDGAR",
      description="SEC filings data",
      enabled=True,
      plans=[plan],
    )
    assert model.enabled is True
    assert model.coming_soon is False

  def test_available_repository_coming_soon(self):
    model = AvailableRepository(
      type="industry",
      name="Industry Data",
      description="Industry benchmarks",
      enabled=False,
      coming_soon=True,
      plans=[],
    )
    assert model.coming_soon is True

  def test_subscription_info(self):
    model = SubscriptionInfo(
      id="sub_123",
      user_id="user_456",
      addon_type="sec_data",
      addon_tier="starter",
      is_active=True,
      activated_at="2024-01-01T00:00:00Z",
      monthly_price_cents=999,
      features=["Read access"],
      metadata={},
    )
    assert model.is_active is True
    assert model.expires_at is None

  def test_user_subscriptions_response(self):
    model = UserSubscriptionsResponse(
      subscriptions=[],
      total_count=0,
      active_count=0,
    )
    assert model.total_count == 0

  def test_tier_upgrade_request(self):
    model = TierUpgradeRequest(new_plan="professional")
    assert model.new_plan == "professional"

  def test_credit_summary(self):
    model = CreditSummary(
      current_balance=7500.0,
      monthly_allocation=8000.0,
      consumed_this_month=500.0,
      usage_percentage=6.25,
      rollover_credits=0.0,
      allows_rollover=False,
      is_active=True,
    )
    assert model.current_balance == 7500.0
    assert model.next_allocation_date is None

  def test_addon_credit_info(self):
    model = AddOnCreditInfo(
      subscription_id="sub_123",
      addon_type="sec_data",
      name="SEC Data",
      tier="starter",
      credits_remaining=800.0,
      credits_allocated=1000.0,
      credits_consumed=200.0,
    )
    assert model.rollover_amount == 0

  def test_credits_summary_response(self):
    model = CreditsSummaryResponse(
      add_ons=[],
      total_credits=0.0,
      addon_count=0,
    )
    assert model.credits_by_addon is None

  def test_repository_credits_response_no_access(self):
    model = RepositoryCreditsResponse(
      repository="sec",
      has_access=False,
      message="No active subscription",
    )
    assert model.credits is None

  def test_allocation_result(self):
    model = AllocationResult(
      addon_type="sec_data",
      addon_tier="starter",
      was_allocated=True,
      current_balance=1000.0,
    )
    assert model.was_allocated is True

  def test_allocation_response(self):
    model = AllocationResponse(
      message="Allocation complete",
      allocations=[],
      total_subscriptions=0,
    )
    assert model.total_subscriptions == 0


@pytest.mark.unit
class TestOfferingModels:
  def test_graph_subscription_tier(self):
    model = GraphSubscriptionTier(
      name="ladybug-standard",
      display_name="LadybugDB Standard",
      description="Entry tier",
      monthly_price_per_graph=0.0,
      monthly_credits_per_graph=8000,
      infrastructure="Shared r7g.large",
      features=["Shared infrastructure"],
      backup_retention_days=7,
      priority_support=False,
      api_rate_multiplier=1.0,
      backend="ladybug",
    )
    assert model.max_queries_per_hour is None
    assert model.max_subgraphs == 0

  def test_token_pricing(self):
    model = TokenPricing(
      input_per_1k_tokens=0.003,
      output_per_1k_tokens=0.015,
    )
    assert model.input_per_1k_tokens == 0.003

  def test_operation_costs(self):
    pricing = TokenPricing(input_per_1k_tokens=0.003, output_per_1k_tokens=0.015)
    model = OperationCosts(
      description="AI operation costs",
      ai_operations={"agent_query": 5.0},
      token_pricing={"claude-3-5-sonnet": pricing},
      included_operations=["cypher_query", "backup_create"],
      notes=["Only AI operations consume credits"],
    )
    assert "cypher_query" in model.included_operations

  def test_service_offering_summary(self):
    model = ServiceOfferingSummary(
      total_graph_tiers=3,
      total_repositories=3,
      enabled_repositories=1,
      coming_soon_repositories=2,
    )
    assert model.enabled_repositories == 1
