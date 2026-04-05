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
# Typical agent call (~5K input, ~1.5K output): ~38 credits
#
# Credit allocations (included with subscription):
# - 8,000 credits = ~200 agent calls/month (~7/day)
# - 32,000 credits = ~800 agent calls/month (~27/day)
# - 100,000 credits = ~2,600 agent calls/month (~87/day)
#
# Only AI agent operations consume credits. MCP tools, database queries,
# and all other operations are unlimited.
DEFAULT_GRAPH_BILLING_PLANS: list[dict[str, Any]] = [
  {
    "name": "ladybug-standard",
    "display_name": "LadybugDB Standard",
    "description": "Dedicated m7g.large LadybugDB infrastructure with subgraph support",
    "base_price_cents": 9900,  # $99/month
    "monthly_credit_allocation": 8000,  # ~200 agent calls/month
    "max_queries_per_hour": 10000,
    "infrastructure": "Dedicated m7g.large (2 vCPU, 8 GB RAM)",
    "backup_retention_days": 7,
    "backup_downloads_per_month": 10,  # R2 zero-egress
    "max_documents": 100,
    "priority_support": True,
  },
  {
    "name": "ladybug-large",
    "display_name": "LadybugDB Large",
    "description": "Dedicated r7g.large instance - enhanced performance with subgraph support",
    "base_price_cents": 29900,  # $299/month
    "monthly_credit_allocation": 32000,  # ~800 agent calls/month
    "max_queries_per_hour": 50000,
    "infrastructure": "Dedicated r7g.large (2 vCPU, 16 GB RAM)",
    "backup_retention_days": 30,
    "backup_downloads_per_month": 20,  # R2 zero-egress
    "max_documents": 1000,
    "priority_support": True,
  },
  {
    "name": "ladybug-xlarge",
    "display_name": "LadybugDB XLarge",
    "description": "Dedicated r7g.xlarge instance - maximum performance and scale",
    "base_price_cents": 69900,  # $699/month
    "monthly_credit_allocation": 100000,  # ~2,600 agent calls/month
    "max_queries_per_hour": None,  # Unlimited
    "infrastructure": "Dedicated r7g.xlarge (4 vCPU, 32 GB RAM)",
    "backup_retention_days": 90,
    "backup_downloads_per_month": 999,  # Effectively unlimited (R2 zero-egress)
    "max_documents": 10000,
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
  plan["name"]: plan["monthly_credit_allocation"]
  for plan in DEFAULT_GRAPH_BILLING_PLANS
}


def get_tier_backup_downloads_per_month(tier: str) -> int | None:
  """Get monthly backup download limit for a tier. Returns None if tier not found."""
  for plan in DEFAULT_GRAPH_BILLING_PLANS:
    if plan["name"] == tier:
      return plan.get("backup_downloads_per_month", 0)
  return None


def get_tier_max_documents(tier: str) -> int | None:
  """Get max documents (uploaded docs) for a tier."""
  for plan in DEFAULT_GRAPH_BILLING_PLANS:
    if plan["name"] == tier:
      return plan.get("max_documents")
  return None


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
    from robosystems.config.shared_repositories import get_manifest, get_plan_details

    # Extract the plan tier from the plan name (e.g., 'sec-starter' -> 'starter')
    plan_tier = plan_name.split("-")[-1] if "-" in plan_name else plan_name

    # Get plan details (pass repo_id for per-repo plan lookup)
    plan_details = get_plan_details(plan_tier, repo_id=repository_id)
    if not plan_details:
      return None

    # Build display_name from repository name + plan name
    # e.g., "SEC EDGAR Filings - Pro" instead of raw "advanced"
    manifest = get_manifest(repository_id)
    repo_display = manifest.name if manifest else repository_id.upper()
    plan_display = plan_details.get("name", plan_tier.title())
    display_name = f"{repo_display} - {plan_display}"

    # Return in a consistent format with subscription plans
    return {
      "name": plan_name,
      "display_name": display_name,
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

    # Validate all billing plans have required fields
    required_fields = ["name", "monthly_credit_allocation", "base_price_cents"]
    for plan in DEFAULT_GRAPH_BILLING_PLANS:
      for field in required_fields:
        if field not in plan:
          issues.append(
            f"Billing plan '{plan.get('name', 'unknown')}' missing '{field}'"
          )

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
        "billing_plans": len(DEFAULT_GRAPH_BILLING_PLANS),
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
    }
