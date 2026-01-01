from decimal import Decimal

from robosystems.config.billing import core
from robosystems.config.billing.core import (
  DEFAULT_GRAPH_BILLING_PLANS,
  TIER_CREDIT_ALLOCATIONS,
  BillingConfig,
)
from robosystems.config.credits import CreditConfig


def test_get_subscription_plan_returns_plan_with_credit_allocation():
  """Test that subscription plans include monthly_credit_allocation."""
  plan = BillingConfig.get_subscription_plan("ladybug-standard")

  assert plan is not None
  assert plan["name"] == "ladybug-standard"
  assert plan["monthly_credit_allocation"] == 25
  assert plan["included_gb"] == 10


def test_get_subscription_plan_returns_none_for_unknown_tier():
  assert BillingConfig.get_subscription_plan("does-not-exist") is None


def test_tier_credit_allocations_matches_plans():
  """Test that TIER_CREDIT_ALLOCATIONS is built from DEFAULT_GRAPH_BILLING_PLANS."""
  for plan in DEFAULT_GRAPH_BILLING_PLANS:
    tier_name = plan["name"]
    assert tier_name in TIER_CREDIT_ALLOCATIONS
    assert TIER_CREDIT_ALLOCATIONS[tier_name] == plan["monthly_credit_allocation"]


def test_get_monthly_credits_returns_from_plans():
  """Test that get_monthly_credits returns values from plans."""
  assert BillingConfig.get_monthly_credits("ladybug-standard") == 25
  assert BillingConfig.get_monthly_credits("ladybug-large") == 100
  assert BillingConfig.get_monthly_credits("ladybug-xlarge") == 300
  assert BillingConfig.get_monthly_credits("unknown") == 0


def test_get_operation_cost_uses_credit_config(monkeypatch):
  monkeypatch.setattr(
    CreditConfig,
    "get_operation_cost",
    classmethod(lambda cls, op: Decimal("42") if op == "agent_call" else Decimal("0")),
  )

  assert BillingConfig.get_operation_cost("agent_call") == Decimal("42")
  assert BillingConfig.get_operation_cost("unknown") == Decimal("0")


def test_validate_configuration_reports_missing_plan(monkeypatch):
  monkeypatch.setattr(
    CreditConfig,
    "MONTHLY_ALLOCATIONS",
    {"ladybug-standard": 500, "custom-tier": 50},
  )
  monkeypatch.setattr(CreditConfig, "OPERATION_COSTS", {"agent_call": Decimal("1")})
  warnings: list[str] = []
  monkeypatch.setattr(
    core.logger,
    "warning",
    lambda message, *args, **kwargs: warnings.append(message),
  )

  result = BillingConfig.validate_configuration()

  assert not result["valid"]
  assert any("custom-tier" in issue for issue in result["issues"])
  assert any("validation found" in message for message in warnings)


def test_validate_configuration_passes_when_allocations_match(monkeypatch):
  monkeypatch.setattr(CreditConfig, "MONTHLY_ALLOCATIONS", {"ladybug-standard": 25})
  monkeypatch.setattr(
    CreditConfig,
    "OPERATION_COSTS",
    {"agent_call": Decimal("1"), "mcp_call": Decimal("0")},
  )
  infos: list[str] = []
  monkeypatch.setattr(
    core.logger,
    "info",
    lambda message, *args, **kwargs: infos.append(message),
  )

  result = BillingConfig.validate_configuration()

  assert result["valid"]
  assert result["summary"]["subscription_tiers"] == 1
  assert any("validation passed" in message for message in infos)


def test_get_all_pricing_info_returns_expected_structure():
  pricing = BillingConfig.get_all_pricing_info()

  assert set(pricing["subscription_tiers"]) == {
    "ladybug-standard",
    "ladybug-large",
    "ladybug-xlarge",
  }
  assert (
    pricing["subscription_tiers"]["ladybug-large"]["monthly_credit_allocation"] == 100
  )
  # AI operations use token-based pricing now, no fixed costs
  assert pricing["ai_operation_costs"] == {}
  assert "query" in pricing["no_credit_operations"]
  assert pricing["storage_pricing"]["included_per_tier"]["ladybug-standard"] == 10
