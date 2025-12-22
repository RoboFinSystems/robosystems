"""
Service offering API endpoint.

Provides comprehensive information about all subscription offerings:
- Per-graph infrastructure subscriptions (ladybug-standard, ladybug-large, ladybug-xlarge)
- Shared repository subscriptions (SEC, industry, economic data)
- Operation costs and credit information
- Features and capabilities for each infrastructure tier
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends

from ..config import BillingConfig, env
from ..config.billing import RepositoryBillingConfig
from ..middleware.rate_limits import public_api_rate_limit_dependency
from ..models.api import ServiceOfferingsResponse
from ..models.api.common import ErrorCode, ErrorResponse, create_error_response
from ..models.iam.user_repository import UserRepository

logger = logging.getLogger(__name__)


class OfferingFeatureGenerator:
  """Generate feature lists for different repository types and plan levels."""

  @staticmethod
  def get_features_for_repository(
    repo_type: str, plan_type: str, plan_config: dict[str, Any]
  ) -> list[str]:
    """Get marketing-friendly features for a specific repository and plan.

    Args:
      repo_type: Repository type (e.g., 'sec', 'industry', 'economic')
      plan_type: Plan type (e.g., 'basic', 'advanced', 'unlimited')
      plan_config: Plan configuration containing monthly_credits, price_monthly, etc.

    Returns:
      List of feature strings
    """
    # Get repository-specific features
    if repo_type == "sec":
      return OfferingFeatureGenerator._get_sec_features(plan_type, plan_config)
    elif repo_type == "industry":
      return OfferingFeatureGenerator._get_industry_features(plan_type, plan_config)
    elif repo_type == "economic":
      return OfferingFeatureGenerator._get_economic_features(plan_type, plan_config)
    else:
      return OfferingFeatureGenerator._get_default_features(plan_config)

  @staticmethod
  def _get_sec_features(plan_type: str, plan_config: dict[str, Any]) -> list[str]:
    """Get features for SEC repository plans."""
    base_features = [
      f"{plan_config['monthly_credits']:,} credits per month",
      "Read-only access to SEC filings",
      "Query company financial data",
      "Access via API and MCP tools",
    ]

    if plan_type == "basic":
      return base_features
    elif plan_type == "advanced":
      return [
        *base_features,
        "Priority query processing",
        "Advanced analytics endpoints",
      ]
    elif plan_type == "unlimited":
      return [
        *base_features,
        "Priority query processing",
        "Advanced analytics endpoints",
        "Bulk export capabilities",
        "Dedicated support",
      ]
    else:
      return base_features

  @staticmethod
  def _get_industry_features(_plan_type: str, plan_config: dict[str, Any]) -> list[str]:
    """Get features for industry repository plans (placeholder for future implementation)."""
    # Currently uses default features, but can be customized in the future
    return OfferingFeatureGenerator._get_default_features(plan_config)

  @staticmethod
  def _get_economic_features(_plan_type: str, plan_config: dict[str, Any]) -> list[str]:
    """Get features for economic repository plans (placeholder for future implementation)."""
    # Currently uses default features, but can be customized in the future
    return OfferingFeatureGenerator._get_default_features(plan_config)

  @staticmethod
  def _get_default_features(plan_config: dict[str, Any]) -> list[str]:
    """Get default features for any repository."""
    return [
      f"{plan_config['monthly_credits']:,} credits per month",
      f"${plan_config['price_monthly']}/month",
      "Read-only access",
      "Query via Cypher",
      "API access",
    ]


# Public offering router - comprehensive service menu
offering_router = APIRouter(
  prefix="/offering",
  tags=["Service Offerings"],
)


@offering_router.get(
  "",
  response_model=ServiceOfferingsResponse,
  summary="Get Service Offerings",
  description="""Get comprehensive information about all subscription offerings.

This endpoint provides complete information about both graph database subscriptions
and shared repository subscriptions. This is the primary endpoint for frontend
applications to display subscription options.

**Pricing Model:**
- Graph subscriptions are **per-graph** with infrastructure-based pricing
- Each graph you create has its own monthly subscription
- Organizations can have multiple graphs with different infrastructure tiers
- Credits are allocated per-graph, not shared across organization

Includes:
- Graph infrastructure tiers (ladybug-standard, ladybug-large, ladybug-xlarge) - per-graph pricing
- Shared repository subscriptions (SEC, industry, economic data) - org-level
- Operation costs and credit information
- Features and capabilities for each tier
- Enabled/disabled status for repositories

All data comes from the config-based systems to ensure accuracy with backend behavior.

No authentication required - this is public service information.""",
  operation_id="getServiceOfferings",
  responses={
    200: {
      "description": "Complete service offerings retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_subscriptions": {
              "tiers": [
                {
                  "name": "ladybug-standard",
                  "display_name": "LadybugDB Standard",
                  "monthly_price": 49.99,
                  "monthly_credits": 10000,
                  "infrastructure": "Multi-tenant (shared r7g.large/xlarge)",
                  "features": [
                    "10k AI credits/month per graph",
                    "100 GB storage included",
                    "Multi-tenant infrastructure",
                    "30-day backup retention",
                  ],
                }
              ]
            },
            "repository_subscriptions": {
              "repositories": [
                {
                  "type": "sec",
                  "name": "SEC Data",
                  "enabled": True,
                  "plans": [
                    {"plan": "starter", "monthly_price": 29.99, "monthly_credits": 5000}
                  ],
                }
              ]
            },
            "operation_costs": {
              "base_costs": {"agent_call": 100.0, "ai_analysis": 100.0},
            },
          }
        }
      },
    },
    500: {
      "description": "Failed to retrieve service offerings",
      "model": ErrorResponse,
    },
  },
)
async def get_service_offerings(
  _rate_limit: None = Depends(public_api_rate_limit_dependency),
) -> ServiceOfferingsResponse:
  """Get comprehensive information about all subscription offerings."""
  try:
    # Get graph subscription information from billing config
    graph_pricing = BillingConfig.get_all_pricing_info()

    # Get tier configurations from graph.yml for technical specs
    from ..config.graph_tier import GraphTierConfig

    tier_configs = GraphTierConfig.get_available_tiers(include_disabled=False)

    # Filter to only customer-facing tiers (exclude internal/shared infrastructure)
    customer_tiers = ["ladybug-standard", "ladybug-large", "ladybug-xlarge"]
    tier_configs = [tier for tier in tier_configs if tier.get("tier") in customer_tiers]

    # Build graph subscription tiers from billing plans
    graph_tiers = []
    for tier_name, plan_data in graph_pricing["subscription_tiers"].items():
      if not plan_data or tier_name not in customer_tiers:
        continue

      # Find the corresponding tier config for technical specs
      tier_config = next((t for t in tier_configs if t.get("tier") == tier_name), None)

      # Get storage information
      storage_included = (
        graph_pricing.get("storage_pricing", {})
        .get("included_per_tier", {})
        .get(tier_name, 100)
      )
      storage_overage = (
        graph_pricing.get("storage_pricing", {})
        .get("overage_per_gb_per_month", {})
        .get(tier_name, 1.0)
      )

      # Build features list
      features = [
        f"{plan_data.get('monthly_credit_allocation', 0):,} AI credits per graph",
        f"{storage_included:,} GB storage included",
        f"${storage_overage:.2f}/GB storage overage",
        plan_data.get("infrastructure", "Managed infrastructure"),
        f"{plan_data.get('backup_retention_days', 0)}-day backup retention",
        "Priority support"
        if plan_data.get("priority_support", False)
        else "Standard support",
      ]

      # Add subgraph support if available
      if tier_config and tier_config.get("max_subgraphs", 0) > 0:
        features.append(f"Up to {tier_config.get('max_subgraphs')} subgraphs")

      tier_info = {
        "name": tier_name,
        "display_name": plan_data.get("display_name", tier_name.title()),
        "description": plan_data.get("description", ""),
        "monthly_price_per_graph": plan_data.get("base_price_cents", 0) / 100.0,
        "monthly_credits_per_graph": plan_data.get("monthly_credit_allocation", 0),
        "storage_included_gb": storage_included,
        "storage_overage_per_gb": storage_overage,
        "infrastructure": plan_data.get("infrastructure", "Managed"),
        "features": features,
        "backup_retention_days": plan_data.get("backup_retention_days", 0),
        "priority_support": plan_data.get("priority_support", False),
        "max_queries_per_hour": plan_data.get("max_queries_per_hour"),
        "max_subgraphs": tier_config.get("max_subgraphs", 0) if tier_config else 0,
        "api_rate_multiplier": tier_config.get("api_rate_multiplier", 1.0)
        if tier_config
        else 1.0,
        "backend": tier_config.get("backend", "ladybug") if tier_config else "ladybug",
        "instance_type": tier_config.get("instance", {}).get("type")
        if tier_config
        else None,
      }
      graph_tiers.append(tier_info)

    # Sort graph tiers by price
    graph_tiers.sort(key=lambda x: x["monthly_price_per_graph"])

    # Get repository subscription information from both sources
    all_repo_configs = UserRepository.get_all_repository_configs()

    repositories = []
    for repo_type, repo_config in all_repo_configs.items():
      # Convert repository plans
      plans = []
      for plan_type, plan_config in repo_config.get("plans", {}).items():
        # Get rate limits from RepositoryBillingConfig if available
        from ..config.billing.repositories import RepositoryPlan, SharedRepository

        try:
          repo_enum = SharedRepository(repo_type.value)
          plan_enum = RepositoryPlan(plan_type.value)
          rate_limits = RepositoryBillingConfig.get_rate_limits(repo_enum, plan_enum)
        except (ValueError, KeyError):
          rate_limits = None

        # Get plan details from RepositoryBillingConfig
        try:
          plan_enum = RepositoryPlan(plan_type.value)
          plan_details = RepositoryBillingConfig.get_plan_details(plan_enum)
        except (ValueError, KeyError):
          plan_details = None

        # Use the feature generator or get from plan_details
        if plan_details and "features" in plan_details:
          features = plan_details["features"]
        else:
          features = OfferingFeatureGenerator.get_features_for_repository(
            repo_type.value, plan_type.value, plan_config
          )

        plan_info = {
          "plan": plan_type.value,
          "name": plan_config["name"],
          "monthly_price": plan_config["price_monthly"],
          "monthly_credits": plan_config["monthly_credits"],
          "access_level": plan_config["access_level"].value,
          "features": features,
        }

        # Add rate limits if available
        if rate_limits:
          plan_info["rate_limits"] = {
            "queries_per_hour": rate_limits.get("queries_per_hour"),
            "mcp_queries_per_hour": rate_limits.get("mcp_queries_per_hour"),
            "agent_calls_per_hour": rate_limits.get("agent_calls_per_hour"),
          }

        plans.append(plan_info)

      # Sort plans by price
      plans.sort(key=lambda x: x["monthly_price"])

      # Repository descriptions
      descriptions = {
        "sec": "SEC public company filings and financial data",
        "industry": "Industry benchmarking and comparative analysis data",
        "economic": "Economic indicators and macroeconomic metrics",
      }

      repo_info = {
        "type": repo_type.value,
        "name": f"{repo_type.value.upper()} Data",
        "description": descriptions.get(
          repo_type.value, f"{repo_type.value.title()} data repository"
        ),
        "enabled": repo_config.get("enabled", False),
        "coming_soon": repo_config.get("coming_soon", False),
        "plans": plans,
      }
      repositories.append(repo_info)

    # Get operation costs
    base_costs = {
      k: float(v) for k, v in graph_pricing.get("ai_operation_costs", {}).items()
    }

    # Get no-credit operations list
    no_credit_ops = graph_pricing.get("no_credit_operations", [])

    # Get AI token pricing information (sample of key models)
    token_pricing = {
      "claude_4_opus": {
        "input_per_1k_tokens": 15,  # credits
        "output_per_1k_tokens": 75,  # credits
      },
      "claude_4_sonnet": {
        "input_per_1k_tokens": 3,  # credits
        "output_per_1k_tokens": 15,  # credits
      },
      "gpt4": {
        "input_per_1k_tokens": 30,  # credits
        "output_per_1k_tokens": 60,  # credits
      },
    }

    return ServiceOfferingsResponse(
      billing_enabled=env.BILLING_ENABLED,
      graph_subscriptions={
        "description": "Per-graph infrastructure subscriptions - each graph has its own subscription",
        "pricing_model": "per_graph",
        "tiers": graph_tiers,
        "storage": {
          "included_per_tier": graph_pricing.get("storage_pricing", {}).get(
            "included_per_tier", {}
          ),
          "overage_pricing": graph_pricing.get("storage_pricing", {}).get(
            "overage_per_gb_per_month", {}
          ),
        },
        "notes": [
          "Each graph database has its own subscription and monthly cost",
          "Organizations can create multiple graphs with different infrastructure tiers",
          "Credits are allocated per graph, not shared across the organization",
          "Higher tiers provide dedicated infrastructure with better performance",
          "Storage is included in each graph's subscription price",
          "Additional storage is billed per GB per month per graph",
        ],
      },
      repository_subscriptions={
        "description": "Organization-level shared repository access subscriptions",
        "pricing_model": "per_organization",
        "repositories": repositories,
        "notes": [
          "Repository subscriptions are purchased at the organization level",
          "All organization members share access to subscribed repositories",
          "Repository subscriptions are separate from graph subscriptions",
          "Can be combined with any graph infrastructure tier",
          "Repository queries do not consume AI credits",
          "Rate limits apply based on subscription plan",
        ],
      },
      operation_costs={
        "description": "Credit costs for AI operations (per-graph credit allocation)",
        "ai_operations": base_costs,
        "token_pricing": token_pricing,
        "included_operations": no_credit_ops,
        "notes": [
          "Credits are allocated per graph based on its infrastructure tier",
          "Only AI operations (agent calls, MCP AI tools, AI analysis) consume credits",
          "All database operations are included (queries, imports, backups, exports, etc.)",
          "Token-based pricing applies for actual AI API usage",
          "Credits do not roll over between billing periods",
          "1 credit ≈ $0.001 USD",
        ],
      },
      summary={
        "total_graph_tiers": len(graph_tiers),
        "total_repositories": len(repositories),
        "enabled_repositories": len([r for r in repositories if r["enabled"]]),
        "coming_soon_repositories": len(
          [r for r in repositories if r.get("coming_soon", False)]
        ),
      },
    )

  except Exception as e:
    logger.error(f"Failed to get service offerings: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve service offerings",
      code=ErrorCode.INTERNAL_ERROR,
    )
