"""Service offerings endpoint — public subscription and pricing information."""

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
from ..models.api.common import (
  COMMON_ERROR_RESPONSES,
  ErrorCode,
  create_error_response,
)

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
  description="Returns all subscription tiers, shared repository plans, and AI credit costs. No authentication required.",
  operation_id="getServiceOfferings",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_service_offerings(
  _rate_limit: None = Depends(public_api_rate_limit_dependency),
) -> ServiceOfferingsResponse:
  try:
    # Get graph subscription information from billing config
    graph_pricing = BillingConfig.get_all_pricing_info()

    # Get tier configurations from graph.yml for technical specs
    from ..config.graph_tier import GraphTierConfig

    # include_disabled=True is deliberate. get_available_tiers() gates on
    # deployment.always_enabled / enabled_default, which answers "is the
    # CloudFormation stack deployed by default" — not "can a customer buy
    # this". Large and XLarge carry enabled_default: false plus an enable_var
    # (LBUG_LARGE_ENABLED_PROD) that only the deploy workflow reads, so the
    # running API cannot see that they are in fact enabled. Filtering here
    # dropped their tier_config and silently zeroed max_subgraphs and
    # api_rate_multiplier. The customer_tiers list below plus the billing
    # plans are the real gate on what gets listed.
    tier_configs = GraphTierConfig.get_available_tiers(include_disabled=True)

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

      # Get backup retention from graph.yml (single source of truth for infra limits)
      backup_limits = GraphTierConfig.get_backup_limits(tier_name)
      backup_retention_days = backup_limits.get("backup_retention_days", 0)

      # Get instance type from graph.yml. vcpus and instance_ram_gb are the
      # physical instance specs; duckdb_max_threads and max_memory_mb are
      # tuning knobs (threads can oversubscribe the CPU, and max_memory_mb is
      # the LadybugDB budget after OS overhead) and must not be reported here.
      instance_config = GraphTierConfig.get_instance_config(tier_name)
      instance_type = instance_config.get("type", "")
      vcpus = instance_config.get("vcpus", 0)
      instance_ram_gb = instance_config.get("instance_ram_gb", 0)
      infrastructure = (
        f"Dedicated {instance_type} ({vcpus} vCPU, {instance_ram_gb} GB RAM)"
        if instance_type and vcpus and instance_ram_gb
        else "Dedicated instance"
        if instance_type
        else "Managed infrastructure"
      )

      # Build features list
      features = [
        f"{plan_data.get('monthly_credit_allocation', 0):,} AI credits per graph",
        infrastructure,
        f"{backup_retention_days}-day backup retention",
        "Priority support"
        if plan_data.get("priority_support", False)
        else "Standard support",
      ]

      # Add subgraph support if available
      if tier_config and tier_config.get("max_subgraphs", 0) > 0:
        features.append(f"Up to {tier_config.get('max_subgraphs')} subgraphs")

      # Add content limits if available
      graph_limits: dict = {}
      if tier_config:
        graph_limits = tier_config.get("limits", {}).get("graph_limits", {})
        if not graph_limits:
          # Try from the writer config directly
          from ..config.graph_tier import GraphTierConfig

          graph_limits = GraphTierConfig.get_graph_limits(tier_name)
        if graph_limits:
          storage_limit = graph_limits.get("instance_storage_limit_gb", 0)
          if storage_limit > 0:
            features.append(f"{int(storage_limit)} GB instance storage")

      tier_info = {
        "name": tier_name,
        "display_name": plan_data.get("display_name", tier_name.title()),
        "description": plan_data.get("description", ""),
        "monthly_price_per_graph": plan_data.get("base_price_cents", 0) / 100.0,
        "monthly_credits_per_graph": plan_data.get("monthly_credit_allocation", 0),
        "infrastructure": infrastructure,
        "features": features,
        "backup_retention_days": backup_retention_days,
        "priority_support": plan_data.get("priority_support", False),
        "max_subgraphs": tier_config.get("max_subgraphs", 0) if tier_config else 0,
        "instance_storage_limit_gb": GraphTierConfig.get_instance_storage_limit_gb(
          tier_name
        ),
        "api_rate_multiplier": tier_config.get("api_rate_multiplier", 1.0)
        if tier_config
        else 1.0,
        "backend": tier_config.get("backend", "ladybug") if tier_config else "ladybug",
        "instance_type": instance_type or None,
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
              "searches_per_hour": rate_limits.get("searches_per_hour"),
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
          "Only AI agent operations consume credits",
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
