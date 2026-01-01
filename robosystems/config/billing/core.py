"""
Core billing configuration - Graph subscriptions and main billing plans.

This module defines the primary subscription tiers for graph databases
and core billing functionality. This is the SINGLE SOURCE OF TRUTH for
all tier-related configuration.
"""

import logging
from decimal import Decimal
from typing import Any

from ..credits import CreditConfig

logger = logging.getLogger(__name__)


# SINGLE SOURCE OF TRUTH: Subscription tier configuration
# All tier-related settings are defined here in one place.
# NOTE: Stripe prices are auto-created from this config on first checkout
#
# Credit allocations with token-based pricing (~0.125 credits per agent call):
# - 25 credits = ~200 agent calls/month (~7/day)
# - 100 credits = ~800 agent calls/month (~27/day)
# - 300 credits = ~2,400 agent calls/month (~80/day)
# NOTE: MCP tool access is unlimited - credits only apply to in-house AI agents
DEFAULT_GRAPH_BILLING_PLANS: list[dict[str, Any]] = [
  {
    "name": "ladybug-standard",
    "display_name": "LadybugDB Standard",
    "description": "Multi-tenant LadybugDB infrastructure - perfect for most applications",
    "base_price_cents": 5000,  # $50/month
    "monthly_credit_allocation": 25,  # ~200 agent calls/month
    "included_gb": 10,  # 10 GB storage included (overage via credits)
    "max_queries_per_hour": 10000,
    "infrastructure": "Multi-tenant (shared r7g.large/xlarge)",
    "backup_retention_days": 7,
    "priority_support": True,
  },
  {
    "name": "ladybug-large",
    "display_name": "LadybugDB Large",
    "description": "Dedicated r7g.large instance - enhanced performance with subgraph support",
    "base_price_cents": 30000,  # $300/month
    "monthly_credit_allocation": 100,  # ~800 agent calls/month
    "included_gb": 50,  # 50 GB storage included (overage via credits)
    "max_queries_per_hour": 50000,
    "infrastructure": "Dedicated r7g.large (2 vCPU, 16 GB RAM)",
    "backup_retention_days": 30,
    "priority_support": True,
  },
  {
    "name": "ladybug-xlarge",
    "display_name": "LadybugDB XLarge",
    "description": "Dedicated r7g.xlarge instance - maximum performance and scale",
    "base_price_cents": 70000,  # $700/month
    "monthly_credit_allocation": 300,  # ~2,400 agent calls/month
    "included_gb": 100,  # 100 GB storage included (overage via credits)
    "max_queries_per_hour": None,  # Unlimited
    "infrastructure": "Dedicated r7g.xlarge (4 vCPU, 32 GB RAM)",
    "backup_retention_days": 90,
    "priority_support": True,
  },
]


# Helper to get credit allocations by tier name (for backward compatibility)
def get_tier_credit_allocation(tier: str) -> int:
  """Get monthly credit allocation for a tier from the billing plans."""
  for plan in DEFAULT_GRAPH_BILLING_PLANS:
    if plan["name"] == tier:
      return plan.get("monthly_credit_allocation", 0)
  return 0


# Build TIER_CREDIT_ALLOCATIONS from plans for backward compatibility
TIER_CREDIT_ALLOCATIONS = {
  plan["name"]: plan["monthly_credit_allocation"] for plan in DEFAULT_GRAPH_BILLING_PLANS
}


# Helper to get included storage by tier name
def get_included_storage(tier: str) -> int:
  """Get included storage in GB for a tier from the billing plans."""
  for plan in DEFAULT_GRAPH_BILLING_PLANS:
    if plan["name"] == tier:
      return plan.get("included_gb", 100)
  return 100  # Default for unknown tiers


# Build STORAGE_INCLUDED from plans for backward compatibility
STORAGE_INCLUDED = {
  plan["name"]: plan["included_gb"] for plan in DEFAULT_GRAPH_BILLING_PLANS
}


class StorageBillingConfig:
  """Storage limits by subscription tier (derived from billing plans)."""

  STORAGE_INCLUDED = STORAGE_INCLUDED

  @classmethod
  def get_included_storage(cls, tier: str) -> int:
    """Get included storage in GB for a tier."""
    return get_included_storage(tier)


class BillingConfig:
  """
  Single source of truth for all billing-related configuration.

  This class provides a unified interface to access:
  - Subscription tier information
  - Credit allocations
  - Pricing details
  - Operation costs
  - Repository pricing
  """

  @classmethod
  def get_subscription_plan(cls, tier: str) -> dict[str, Any] | None:
    """
    Get complete subscription plan information for a tier.

    Args:
        tier: Subscription tier name (e.g., ladybug-standard, ladybug-large, ladybug-xlarge)

    Returns:
        Dict with plan details or None if not found
    """
    for plan in DEFAULT_GRAPH_BILLING_PLANS:
      if plan["name"] == tier:
        return plan

    return None

  @classmethod
  def get_monthly_credits(cls, tier: str) -> int:
    """
    Get monthly credit allocation for a subscription tier.

    Args:
        tier: Subscription tier name

    Returns:
        Monthly credit allocation
    """
    return TIER_CREDIT_ALLOCATIONS.get(tier, 0)

  @classmethod
  def get_operation_cost(
    cls, operation_type: str, context: dict[str, Any] | None = None
  ) -> Decimal:
    """
    Get the cost for an operation.

    Only AI operations consume credits. Database operations don't consume credits.

    Args:
        operation_type: Type of operation
        context: Optional context (unused in simplified model)

    Returns:
        Cost in credits (0 for non-AI operations)
    """
    # Simply return the operation cost from CreditConfig
    # No multipliers in the simplified model
    return CreditConfig.get_operation_cost(operation_type)

  @classmethod
  def get_repository_pricing(cls, repository_id: str) -> dict[str, Any] | None:
    """
    Get complete pricing information for a shared repository.

    NOTE: Repository pricing is now handled by UserRepository model.
    This method is deprecated but kept for API compatibility.

    Args:
        repository_id: Repository identifier (e.g., 'sec', 'industry')

    Returns:
        None (repository pricing moved to UserRepository model)
    """
    # Repository pricing is now handled by UserRepository model
    return None

  @classmethod
  def get_repository_plan(
    cls, repository_id: str, plan_name: str
  ) -> dict[str, Any] | None:
    """
    Get plan details for a specific repository subscription.

    Args:
        repository_id: Repository identifier (e.g., 'sec', 'industry')
        plan_name: Plan name (e.g., 'sec-starter', 'starter')

    Returns:
        Dict with plan details including price_cents, monthly_credits, features
    """
    from .repositories import RepositoryBillingConfig, RepositoryPlan

    # Extract the plan tier from the plan name (e.g., 'sec-starter' -> 'starter')
    plan_tier = plan_name.split("-")[-1] if "-" in plan_name else plan_name

    # Validate the plan tier
    try:
      repo_plan = RepositoryPlan(plan_tier)
    except ValueError:
      logger.warning(f"Invalid repository plan: {plan_name}")
      return None

    # Get plan details
    plan_details = RepositoryBillingConfig.get_plan_details(repo_plan)
    if not plan_details:
      return None

    # Return in a consistent format with subscription plans
    return {
      "name": plan_name,
      "price_cents": plan_details["price_cents"],
      "monthly_credits": plan_details["monthly_credits"],
      "features": plan_details["features"],
      "description": plan_details["description"],
    }

  @classmethod
  def validate_configuration(cls) -> dict[str, Any]:
    """
    Validate that all billing configuration is consistent.

    Returns:
        Dict with validation results and any inconsistencies found
    """
    issues = []

    # Check that all tiers in CreditConfig have billing plans
    for tier in CreditConfig.MONTHLY_ALLOCATIONS:
      plan = cls.get_subscription_plan(tier)
      if not plan:
        issues.append(f"No billing plan found for tier '{tier}'")
      elif plan["monthly_credit_allocation"] != CreditConfig.MONTHLY_ALLOCATIONS[tier]:
        issues.append(
          f"Credit allocation mismatch for '{tier}': "
          f"billing={plan['monthly_credit_allocation']}, "
          f"credits={CreditConfig.MONTHLY_ALLOCATIONS[tier]}"
        )

    # Repository configurations are now handled by UserRepository model
    # Skip repository validation as it's moved to a different system

    # Log validation results
    if issues:
      logger.warning(f"Billing configuration validation found {len(issues)} issues")
      for issue in issues:
        logger.warning(f"  - {issue}")
    else:
      logger.info("Billing configuration validation passed")

    return {
      "valid": len(issues) == 0,
      "issues": issues,
      "summary": {
        "subscription_tiers": len(CreditConfig.MONTHLY_ALLOCATIONS),
        "billing_plans": len(DEFAULT_GRAPH_BILLING_PLANS),
        "repositories": 0,  # Repository configs moved to UserRepository model
        "operation_types": len(CreditConfig.OPERATION_COSTS),
      },
    }

  @classmethod
  def get_all_pricing_info(cls) -> dict[str, Any]:
    """
    Get complete pricing information for all offerings.

    This is what should be used by the offerings API endpoint.

    Returns:
        Complete pricing structure
    """
    return {
      "subscription_tiers": {
        tier: cls.get_subscription_plan(tier)
        for tier in ["ladybug-standard", "ladybug-large", "ladybug-xlarge"]
        if cls.get_subscription_plan(tier)
      },
      # AI operations use token-based pricing (see AIBillingConfig.TOKEN_PRICING)
      # No fixed-cost AI operations - all are billed per token
      "ai_operation_costs": {},
      "no_credit_operations": [
        "query",
        "analytics",
        "import",
        "backup",
        "sync",
        "api_call",
        "schema_query",
        "connection_create",
        "database_query",
      ],
      "storage_pricing": {
        "included_per_tier": {
          "ladybug-standard": 10,  # GB
          "ladybug-large": 50,  # GB
          "ladybug-xlarge": 100,  # GB
        },
        "overage_credits_per_gb_per_day": 1,  # 1 credit/GB/day for storage overage
      },
    }
