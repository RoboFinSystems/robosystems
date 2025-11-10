from decimal import Decimal

from robosystems.config.billing import core
from robosystems.config.billing.core import BillingConfig, DEFAULT_GRAPH_BILLING_PLANS
from robosystems.config import credits as credits_module
from robosystems.config.credits import CreditConfig


def test_get_subscription_plan_injects_latest_credit_allocation(monkeypatch):
  credits = {
    "kuzu-standard": 1234,
    "kuzu-large": 4321,
    "kuzu-xlarge": 9999,
  }
  # Patch in both modules since core imports it at module load time
  monkeypatch.setattr(credits_module, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(core, "TIER_CREDIT_ALLOCATIONS", credits)

  plan = BillingConfig.get_subscription_plan("kuzu-standard")

  assert plan is not None
  assert plan["name"] == "kuzu-standard"
  assert plan["monthly_credit_allocation"] == 1234
  assert plan is not DEFAULT_GRAPH_BILLING_PLANS[0]


def test_get_subscription_plan_returns_none_for_unknown_tier():
  assert BillingConfig.get_subscription_plan("does-not-exist") is None


def test_get_monthly_credits_returns_from_tier_allocations(monkeypatch):
  credits = {
    "kuzu-standard": 100,
    "kuzu-large": 777,
    "kuzu-xlarge": 800,
  }
  # Patch in both modules since core imports it at module load time
  monkeypatch.setattr(credits_module, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(core, "TIER_CREDIT_ALLOCATIONS", credits)

  assert BillingConfig.get_monthly_credits("kuzu-large") == 777


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
    {"kuzu-standard": 500, "custom-tier": 50},
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
  credits = {"kuzu-standard": 111}
  monkeypatch.setattr(credits_module, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(core, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(CreditConfig, "MONTHLY_ALLOCATIONS", {"kuzu-standard": 111})
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


def test_get_all_pricing_info_returns_expected_structure(monkeypatch):
  credits = {
    "kuzu-standard": 100,
    "kuzu-large": 200,
    "kuzu-xlarge": 300,
  }
  monkeypatch.setattr(credits_module, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(core, "TIER_CREDIT_ALLOCATIONS", credits)
  monkeypatch.setattr(
    CreditConfig,
    "OPERATION_COSTS",
    {
      "agent_call": Decimal("10"),
      "mcp_call": Decimal("5"),
      "ai_analysis": Decimal("20"),
    },
  )

  pricing = BillingConfig.get_all_pricing_info()

  assert set(pricing["subscription_tiers"]) == {
    "kuzu-standard",
    "kuzu-large",
    "kuzu-xlarge",
  }
  assert pricing["subscription_tiers"]["kuzu-large"]["monthly_credit_allocation"] == 200
  assert pricing["ai_operation_costs"] == {
    "agent_call": 10.0,
    "mcp_call": 5.0,
    "ai_analysis": 20.0,
  }
  assert "query" in pricing["no_credit_operations"]
  assert pricing["storage_pricing"]["included_per_tier"]["kuzu-standard"] == 100
