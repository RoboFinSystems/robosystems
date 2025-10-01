"""
Core billing configuration - Graph subscriptions and main billing plans.

This module defines the primary subscription tiers for graph databases
and core billing functionality.
"""

from typing import Dict, Any, Optional, List
from decimal import Decimal
from ..credits import CreditConfig
from ..tier_config import get_tier_monthly_credits
import logging

logger = logging.getLogger(__name__)


# Unified billing plans configuration
DEFAULT_GRAPH_BILLING_PLANS: List[Dict[str, Any]] = [
  {
    "name": "standard",
    "display_name": "Standard",
    "description": "Perfect for most applications with AI credit allocation",
    "base_price_cents": 4999,  # $49.99
    "included_gb": 100,  # 100 GB storage included
    "overage_price_cents_per_gb": 100,  # $1.00 per GB overage
    "monthly_credit_allocation": 10000,  # 10k AI credits per month (100 agent calls)
    "max_queries_per_hour": 10000,
    "backup_retention_days": 30,
    "priority_support": True,
  },
  {
    "name": "enterprise",
    "display_name": "Enterprise",
    "description": "For high-performance applications with extensive AI capabilities",
    "base_price_cents": 19999,  # $199.99
    "included_gb": 500,  # 500 GB storage included
    "overage_price_cents_per_gb": 50,  # $0.50 per GB overage
    "monthly_credit_allocation": 50000,  # 50k AI credits per month (500 agent calls)
    "max_queries_per_hour": 50000,
    "backup_retention_days": 90,
    "priority_support": True,
  },
  {
    "name": "premium",
    "display_name": "Premium",
    "description": "Maximum performance and scale with abundant AI credits",
    "base_price_cents": 49999,  # $499.99
    "included_gb": 2000,  # 2 TB storage included
    "overage_price_cents_per_gb": 25,  # $0.25 per GB overage
    "monthly_credit_allocation": 200000,  # 200k AI credits per month (2000 agent calls)
    "max_queries_per_hour": None,  # Unlimited
    "backup_retention_days": 365,
    "priority_support": True,
  },
]


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
  def get_subscription_plan(cls, tier: str) -> Optional[Dict[str, Any]]:
    """
    Get complete subscription plan information for a tier.

    Args:
        tier: Subscription tier name (standard, enterprise, premium)

    Returns:
        Dict with plan details or None if not found
    """
    # First check billing plans
    for plan in DEFAULT_GRAPH_BILLING_PLANS:
      if plan["name"] == tier:
        return {
          **plan,
          # Use centralized tier configuration for credit allocations
          "monthly_credit_allocation": get_tier_monthly_credits(tier),
        }

    return None

  @classmethod
  def get_monthly_credits(cls, tier: str) -> int:
    """
    Get monthly credit allocation for a subscription tier.

    Uses centralized tier configuration as the authoritative source.

    Args:
        tier: Subscription tier name

    Returns:
        Monthly credit allocation
    """
    return get_tier_monthly_credits(tier)

  @classmethod
  def get_operation_cost(
    cls, operation_type: str, context: Optional[Dict[str, Any]] = None
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
  def get_repository_pricing(cls, repository_id: str) -> Optional[Dict[str, Any]]:
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
  def validate_configuration(cls) -> Dict[str, Any]:
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
  def get_all_pricing_info(cls) -> Dict[str, Any]:
    """
    Get complete pricing information for all offerings.

    This is what should be used by the offerings API endpoint.

    Returns:
        Complete pricing structure
    """
    return {
      "subscription_tiers": {
        tier: cls.get_subscription_plan(tier)
        for tier in ["standard", "enterprise", "premium"]
        if cls.get_subscription_plan(tier)
      },
      "ai_operation_costs": {
        "agent_call": float(CreditConfig.OPERATION_COSTS.get("agent_call", 0)),
        "mcp_call": float(CreditConfig.OPERATION_COSTS.get("mcp_call", 0)),
        "ai_analysis": float(CreditConfig.OPERATION_COSTS.get("ai_analysis", 0)),
      },
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
          "standard": 100,  # GB
          "enterprise": 500,  # GB
          "premium": 2000,  # GB
        },
        "overage_per_gb_per_month": {
          "standard": 1.00,  # $1.00/GB
          "enterprise": 0.50,  # $0.50/GB
          "premium": 0.25,  # $0.25/GB
        },
      },
    }
