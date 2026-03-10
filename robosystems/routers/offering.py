"""
Service offering API endpoint.

Provides comprehensive information about all subscription offerings:
- Per-graph infrastructure subscriptions (ladybug-standard, ladybug-large, ladybug-xlarge)
- Shared repository subscriptions
- Operation costs and credit information
- Features and capabilities for each infrastructure tier
"""

import logging

from fastapi import APIRouter, Depends

from ..config import BillingConfig, env
from ..config.shared_repositories import (
  get_all_manifests as _get_all_manifests,
)
from ..config.shared_repositories import (
  get_rate_limits as _get_rate_limits,
)
from ..middleware.rate_limits import public_api_rate_limit_dependency
from ..models.api import ServiceOfferingsResponse
from ..models.api.common import ErrorCode, ErrorResponse, create_error_response

logger = logging.getLogger(__name__)


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
- Shared repository subscriptions - org-level
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
                  "monthly_price": 50.00,
                  "monthly_credits": 8000,
                  "infrastructure": "Multi-tenant (shared r7g.large/xlarge)",
                  "features": [
                    "8,000 AI credits per graph",
                    "5M node limit",
                    "7-day backup retention",
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
                    {"plan": "starter", "monthly_price": 29, "monthly_credits": 5000}
                  ],
                }
              ]
            },
            "operation_costs": {
              "token_pricing": {
                "claude_4_sonnet": {
                  "input_per_1k_tokens": 3,
                  "output_per_1k_tokens": 15,
                }
              },
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

      # Build features list
      features = [
        f"{plan_data.get('monthly_credit_allocation', 0):,} AI credits per graph",
        plan_data.get("infrastructure", "Managed infrastructure"),
        f"{plan_data.get('backup_retention_days', 0)}-day backup retention",
        "Priority support"
        if plan_data.get("priority_support", False)
        else "Standard support",
      ]

      # Add subgraph support if available
      if tier_config and tier_config.get("max_subgraphs", 0) > 0:
        features.append(f"Up to {tier_config.get('max_subgraphs')} subgraphs")

      # Add content limits if available
      if tier_config:
        graph_limits = tier_config.get("limits", {}).get("graph_limits", {})
        if not graph_limits:
          # Try from the writer config directly
          from ..config.graph_tier import GraphTierConfig

          graph_limits = GraphTierConfig.get_graph_limits(tier_name)
        if graph_limits:
          max_nodes = graph_limits.get("max_nodes", 0)
          if max_nodes >= 1_000_000:
            features.append(f"{max_nodes // 1_000_000}M node limit")

      tier_info = {
        "name": tier_name,
        "display_name": plan_data.get("display_name", tier_name.title()),
        "description": plan_data.get("description", ""),
        "monthly_price_per_graph": plan_data.get("base_price_cents", 0) / 100.0,
        "monthly_credits_per_graph": plan_data.get("monthly_credit_allocation", 0),
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

    # Get repository subscription information directly from manifests
    repositories = []
    for repo_id, manifest in _get_all_manifests().items():
      plans = []
      if manifest.plans:
        for plan_key, plan_details in manifest.plans.items():
          rate_limits = _get_rate_limits(repo_id, plan_key)

          plan_info = {
            "plan": plan_key,
            "name": plan_details.get("name", plan_key.title()),
            "monthly_price": plan_details.get("price_monthly", 0),
            "monthly_credits": plan_details.get("monthly_credits", 0),
            "access_level": plan_details.get("access_level", "READ").lower(),
            "features": plan_details.get("features", []),
          }

          if rate_limits:
            plan_info["rate_limits"] = {
              "queries_per_hour": rate_limits.get("queries_per_hour"),
              "mcp_queries_per_hour": rate_limits.get("mcp_queries_per_hour"),
              "agent_calls_per_hour": rate_limits.get("agent_calls_per_hour"),
            }

          plans.append(plan_info)

      # Sort plans by price
      plans.sort(key=lambda x: x["monthly_price"])

      repo_info = {
        "type": repo_id,
        "name": manifest.name,
        "description": manifest.description,
        "enabled": manifest.status == "available",
        "coming_soon": manifest.status == "coming_soon",
        "plans": plans,
      }
      repositories.append(repo_info)

    # Get operation costs
    base_costs = {
      k: float(v) for k, v in graph_pricing.get("ai_operation_costs", {}).items()
    }

    # Get no-credit operations list
    no_credit_ops = graph_pricing.get("no_credit_operations", [])

    # Get AI token pricing from authoritative source
    from ..config.billing.ai import AIBillingConfig

    token_pricing = {}
    for model_name, prices in AIBillingConfig.TOKEN_PRICING.items():
      token_pricing[model_name] = {
        "input_per_1k_tokens": float(prices["input"]),
        "output_per_1k_tokens": float(prices["output"]),
      }

    return ServiceOfferingsResponse(
      billing_enabled=env.BILLING_ENABLED,
      graph_subscriptions={
        "description": "Per-graph infrastructure subscriptions - each graph has its own subscription",
        "pricing_model": "per_graph",
        "tiers": graph_tiers,
        "notes": [
          "Each graph database has its own subscription and monthly cost",
          "Organizations can create multiple graphs with different infrastructure tiers",
          "Credits are allocated per graph, not shared across the organization",
          "Higher tiers provide dedicated infrastructure with better performance",
          "Graph content limits (nodes, relationships, rows) vary by tier",
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
