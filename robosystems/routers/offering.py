"""
Service offering API endpoint.

Provides comprehensive information about all subscription offerings:
- Graph database subscription tiers (standard, enterprise, premium)
- Shared repository subscriptions (SEC, industry, economic data)
- Operation costs and credit information
- Features and capabilities for each tier
"""

import logging
from typing import Dict, List, Any
from fastapi import APIRouter, Depends

from ..middleware.rate_limits import public_api_rate_limit_dependency
from ..models.api.common import ErrorResponse, ErrorCode, create_error_response
from ..config import BillingConfig
from ..config.billing import RepositoryBillingConfig
from ..models.iam.user_repository import UserRepository

logger = logging.getLogger(__name__)


class OfferingFeatureGenerator:
  """Generate feature lists for different repository types and plan levels."""

  @staticmethod
  def get_features_for_repository(
    repo_type: str, plan_type: str, plan_config: Dict[str, Any]
  ) -> List[str]:
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
  def _get_sec_features(plan_type: str, plan_config: Dict[str, Any]) -> List[str]:
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
      return base_features + [
        "Priority query processing",
        "Advanced analytics endpoints",
      ]
    elif plan_type == "unlimited":
      return base_features + [
        "Priority query processing",
        "Advanced analytics endpoints",
        "Bulk export capabilities",
        "Dedicated support",
      ]
    else:
      return base_features

  @staticmethod
  def _get_industry_features(plan_type: str, plan_config: Dict[str, Any]) -> List[str]:
    """Get features for industry repository plans (placeholder for future implementation)."""
    # Currently uses default features, but can be customized in the future
    return OfferingFeatureGenerator._get_default_features(plan_config)

  @staticmethod
  def _get_economic_features(plan_type: str, plan_config: Dict[str, Any]) -> List[str]:
    """Get features for economic repository plans (placeholder for future implementation)."""
    # Currently uses default features, but can be customized in the future
    return OfferingFeatureGenerator._get_default_features(plan_config)

  @staticmethod
  def _get_default_features(plan_config: Dict[str, Any]) -> List[str]:
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
  summary="Get Service Offerings",
  description="""Get comprehensive information about all subscription offerings.

This endpoint provides complete information about both graph database subscriptions
and shared repository subscriptions. This is the primary endpoint for frontend
applications to display subscription options.

Includes:
- Graph subscription tiers (standard, enterprise, premium)
- Shared repository subscriptions (SEC, industry, economic data)
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
                  "name": "standard",
                  "display_name": "Standard",
                  "monthly_price": 49.99,
                  "monthly_credits": 100000,
                  "features": [
                    "100k credits/month",
                    "Standard graphs",
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
):
  """Get comprehensive information about all subscription offerings."""
  try:
    # Get graph subscription information from billing config
    graph_pricing = BillingConfig.get_all_pricing_info()

    # Get tier configurations from graph.yml for technical specs
    from ..config.tier_config import TierConfig

    tier_configs = TierConfig.get_available_tiers(include_disabled=False)

    # Filter out internal-only and not-yet-available tiers
    excluded_tiers = ["kuzu-shared", "neo4j-community-large", "neo4j-enterprise-xlarge"]
    tier_configs = [
      tier for tier in tier_configs if tier.get("tier") not in excluded_tiers
    ]

    # Create a mapping from old tier names to new tier names
    tier_name_mapping = {
      "standard": "kuzu-standard",
      "enterprise": "kuzu-large",
      "premium": "kuzu-xlarge",
    }

    # Convert graph subscription tiers
    graph_tiers = []
    for tier_name, plan_data in graph_pricing["subscription_tiers"].items():
      if not plan_data:
        continue

      # Map old tier name to new tier name
      new_tier_name = tier_name_mapping.get(tier_name, tier_name)

      # Find the corresponding tier config
      tier_config = next(
        (t for t in tier_configs if t.get("tier") == new_tier_name), None
      )

      # Graph tier restrictions (using new tier names)
      allowed_graph_tiers = {
        "standard": ["kuzu-standard"],
        "enterprise": ["kuzu-standard", "kuzu-large"],
        "premium": ["kuzu-standard", "kuzu-large", "kuzu-xlarge"],
      }

      # Get storage information for this tier
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

      # Build features list - merge tier config features with billing features if available
      if tier_config:
        # Use features from tier config as the primary source
        features = tier_config.get("features", [])
        # Add pricing and support info from billing config
        features.extend(
          [
            f"${plan_data.get('base_price_cents', 0) / 100:.2f}/month",
            f"${storage_overage}/GB storage overage",
            "Priority support"
            if plan_data.get("priority_support", False)
            else "Standard support",
          ]
        )
      else:
        # Fallback to original features if no tier config
        features = [
          f"{plan_data.get('monthly_credit_allocation', 0):,} AI credits per month",
          f"${plan_data.get('base_price_cents', 0) / 100:.2f}/month",
          f"{storage_included:,} GB storage included",
          f"${storage_overage}/GB overage",
          f"{plan_data.get('backup_retention_days', 0)} day backup retention",
          "Priority support"
          if plan_data.get("priority_support", False)
          else "Standard support",
        ]

      tier_info = {
        "name": tier_name,
        "display_name": tier_config.get(
          "display_name", plan_data.get("display_name", tier_name.title())
        )
        if tier_config
        else plan_data.get("display_name", tier_name.title()),
        "description": tier_config.get("description", plan_data.get("description", ""))
        if tier_config
        else plan_data.get("description", ""),
        "monthly_price": plan_data.get("base_price_cents", 0) / 100.0,
        "monthly_credits": tier_config.get(
          "monthly_credits", plan_data.get("monthly_credit_allocation", 0)
        )
        if tier_config
        else plan_data.get("monthly_credit_allocation", 0),
        "storage_included_gb": tier_config.get("storage_limit_gb", storage_included)
        if tier_config
        else storage_included,
        "storage_overage_per_gb": storage_overage,
        "allowed_graph_tiers": allowed_graph_tiers.get(tier_name, ["kuzu-standard"]),
        "features": features,
        "backup_retention_days": plan_data.get("backup_retention_days", 0),
        "priority_support": plan_data.get("priority_support", False),
        "max_queries_per_hour": plan_data.get("max_queries_per_hour"),
        # Add technical specs from tier config if available
        "max_subgraphs": tier_config.get("max_subgraphs") if tier_config else None,
        "api_rate_multiplier": tier_config.get("api_rate_multiplier")
        if tier_config
        else 1.0,
        "backend": tier_config.get("backend") if tier_config else "kuzu",
        "instance_type": tier_config.get("instance", {}).get("type")
        if tier_config
        else None,
      }
      graph_tiers.append(tier_info)

    # Sort graph tiers by price
    graph_tiers.sort(key=lambda x: x["monthly_price"])

    # Get repository subscription information from both sources
    all_repo_configs = UserRepository.get_all_repository_configs()

    repositories = []
    for repo_type, repo_config in all_repo_configs.items():
      # Convert repository plans
      plans = []
      for plan_type, plan_config in repo_config.get("plans", {}).items():
        # Get rate limits from RepositoryBillingConfig if available
        from ..config.billing.repositories import SharedRepository, RepositoryPlan

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

    return {
      "graph_subscriptions": {
        "description": "Entity-specific graph database subscriptions",
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
          "Each company gets its own isolated graph database",
          "Higher tiers provide better performance and more resources",
          "Storage included in monthly subscription price",
          "Additional storage billed per GB per month",
        ],
      },
      "repository_subscriptions": {
        "description": "Shared data repository access subscriptions",
        "repositories": repositories,
        "notes": [
          "Repository subscriptions provide access to shared public data",
          "Can be combined with any graph subscription tier",
          "Repository queries do not consume credits",
          "Rate limits apply based on subscription plan",
        ],
      },
      "operation_costs": {
        "description": "Credit costs for operations",
        "ai_operations": base_costs,
        "token_pricing": token_pricing,
        "included_operations": no_credit_ops,
        "notes": [
          "Only AI operations (agent calls, MCP AI tools, AI analysis) consume credits",
          "All database operations are included (queries, imports, backups, etc.)",
          "Token-based pricing applies for actual AI API usage",
          "1 credit = approximately $0.001 USD",
        ],
      },
      "summary": {
        "total_graph_tiers": len(graph_tiers),
        "total_repositories": len(repositories),
        "enabled_repositories": len([r for r in repositories if r["enabled"]]),
        "coming_soon_repositories": len(
          [r for r in repositories if r.get("coming_soon", False)]
        ),
      },
    }

  except Exception as e:
    logger.error(f"Failed to get service offerings: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve service offerings",
      code=ErrorCode.INTERNAL_ERROR,
    )
