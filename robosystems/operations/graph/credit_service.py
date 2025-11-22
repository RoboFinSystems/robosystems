"""
Credit management service for the graph-based credit system.

This service handles all credit-related operations including:
- Credit allocation and consumption
- Credit balance management
- Subscription tier enforcement
- Credit transaction tracking
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any

from sqlalchemy.orm import Session

from ...models.iam import (
  GraphCredits,
  GraphCreditTransaction,
  GraphUser,
)
from ...config.graph_tier import GraphTier
from ...models.iam.graph_credits import CreditTransactionType
from ...models.iam.user_repository_credits import UserRepositoryCredits
from ...models.iam.user_repository import UserRepository, RepositoryType
from ...config.credits import CreditConfig
from ...config import BillingConfig
from ...middleware.graph.types import parse_graph_id

logger = logging.getLogger(__name__)


class CreditService:
  """Service for managing graph credits and billing."""

  def __init__(self, session: Session):
    """Initialize credit service with database session."""
    self.session = session

    # Warm up operation cost cache on initialization
    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.warmup_operation_costs(CREDIT_COSTS)
    except Exception as e:
      logger.warning(f"Failed to warm up credit cache: {e}")
      # Fallback: Ensure cache can be populated on-demand if warmup fails
      # The cache will be populated lazily on first access, which is acceptable
      # as long as the application continues to function

  def create_graph_credits(
    self,
    graph_id: str,
    user_id: str,
    billing_admin_id: str,
    subscription_tier: str,
    graph_tier: GraphTier = GraphTier.LADYBUG_STANDARD,
  ) -> GraphCredits:
    """
    Create credit pool for a new graph.

    Args:
        graph_id: Unique graph identifier
        user_id: Graph owner user ID
        billing_admin_id: User responsible for billing
        subscription_tier: User's subscription tier (ladybug-standard/ladybug-large/ladybug-xlarge)
        graph_tier: Database tier (ladybug-standard/ladybug-large/ladybug-xlarge)

    Returns:
        GraphCredits instance
    """
    # Get the billing plan for the subscription tier
    plan_config = BillingConfig.get_subscription_plan(subscription_tier)
    if not plan_config:
      raise ValueError(
        f"No billing plan found for subscription tier: {subscription_tier}"
      )

    # Validate that subscription tier allows this graph tier
    if not self._can_create_graph_tier(subscription_tier, graph_tier):
      raise ValueError(
        f"Subscription tier '{subscription_tier}' does not allow '{graph_tier.value}' graph tier"
      )

    # Create the credit record
    credits = GraphCredits.create_for_graph(
      graph_id=graph_id,
      user_id=user_id,
      billing_admin_id=billing_admin_id,
      monthly_allocation=Decimal(str(plan_config["monthly_credit_allocation"])),
      session=self.session,
    )

    logger.info(
      f"Created credit pool for graph {graph_id}: {plan_config['monthly_credit_allocation']} credits"
    )
    return credits

  def _get_parent_graph_id(self, graph_id: str) -> str:
    """
    Get the parent graph ID from any graph ID.

    For subgraphs (e.g., 'kg0123_dev'), returns the parent ID ('kg0123').
    For parent graphs, returns the graph ID unchanged.
    For shared repositories, returns the graph ID unchanged.

    This ensures subgraphs share the same credit pool as their parent.
    """
    parent_id, _ = parse_graph_id(graph_id)
    return parent_id

  def _is_shared_repository(self, graph_id: str) -> bool:
    """Check if the graph_id represents a shared repository."""
    # Check if graph_id matches any known repository type
    known_repositories = ["sec", "industry", "economic", "market", "esg", "regulatory"]
    return graph_id.lower() in known_repositories

  def consume_credits(
    self,
    graph_id: str,
    operation_type: str,
    base_cost: Decimal,
    metadata: Optional[Dict[str, Any]] = None,
    cached: bool = False,
    user_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    request_id: Optional[str] = None,
    operation_id: Optional[str] = None,
  ) -> Dict[str, Any]:
    """
    Consume credits for a graph operation.

    Args:
        graph_id: Graph identifier
        operation_type: Type of operation (e.g., 'api_call', 'query', 'mcp_call')
        base_cost: Base credit cost (before multiplier)
        metadata: Optional metadata for the transaction
        cached: Whether this is a cached operation (no credit consumption)
        user_id: User ID (required for shared repository operations)

    Returns:
        Dict with consumption results
    """
    # Cached operations don't consume credits
    if cached:
      return {
        "success": True,
        "credits_consumed": 0,
        "cached": True,
        "message": "Cached operation - no credits consumed",
      }

    # Check if this is a shared repository
    if self._is_shared_repository(graph_id):
      if not user_id:
        return {
          "success": False,
          "error": "User ID required for shared repository operations",
          "credits_consumed": 0,
        }

      # Route to shared repository credit system
      return self.consume_shared_repository_credits(
        user_id=user_id,
        repository_name=graph_id,
        operation_type=operation_type,
        metadata=metadata,
        cached=cached,
        base_cost=base_cost,  # Pass the base_cost through for AI operations
      )

    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    # Try to get cached balance first
    from ...middleware.billing.cache import credit_cache

    cached_data = credit_cache.get_cached_graph_credit_balance(parent_graph_id)

    if cached_data:
      # Use cached balance for quick check
      balance, graph_tier = cached_data

      # Quick insufficient check from cache
      if balance < base_cost:
        return {
          "success": False,
          "error": "Insufficient credits",
          "credits_consumed": 0,
          "required_credits": float(base_cost),
          "available_credits": float(balance),
        }

    # Get credit record from database using parent graph ID
    # Subgraphs share the same credit pool as their parent
    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {
        "success": False,
        "error": f"No credit pool found for graph {parent_graph_id}"
        + (f" (parent of subgraph {graph_id})" if graph_id != parent_graph_id else ""),
        "credits_consumed": 0,
      }

    # Consume credits atomically
    consumption_result = credits.consume_credits_atomic(
      amount=base_cost,
      operation_type=operation_type,
      operation_description=f"{operation_type} operation on graph {graph_id}",
      session=self.session,
      request_id=request_id,
      user_id=user_id,
    )

    if consumption_result["success"]:
      # Update cache with new balance from atomic operation
      # Use parent_graph_id for cache to ensure subgraphs share cache with parent
      try:
        from ...middleware.billing.cache import credit_cache

        # Invalidate old cache and set new one with updated balance
        credit_cache.invalidate_graph_credit_balance(parent_graph_id)

        # Cache the new balance from atomic operation
        graph_tier_value = (
          credits.graph_tier.value
          if hasattr(credits.graph_tier, "value")
          else str(credits.graph_tier)
        )
        credit_cache.cache_graph_credit_balance(
          graph_id=parent_graph_id,
          balance=Decimal(str(consumption_result["new_balance"])),
          graph_tier=graph_tier_value,
        )
      except Exception as e:
        logger.warning(f"Failed to update credit cache after consumption: {e}")

      return {
        "success": True,
        "credits_consumed": consumption_result["credits_consumed"],
        "base_cost": consumption_result["base_cost"],
        "remaining_balance": consumption_result["new_balance"],
        "cached": False,
        "transaction_id": consumption_result["transaction_id"],
      }
    else:
      # Invalidate cache on failure to ensure consistency
      # Use parent_graph_id to ensure subgraphs' shared cache is invalidated
      try:
        from ...middleware.billing.cache import credit_cache

        credit_cache.invalidate_graph_credit_balance(parent_graph_id)
      except Exception as e:
        logger.warning(f"Failed to invalidate credit cache: {e}")

      return {
        "success": False,
        "error": consumption_result.get("error", "Credit consumption failed"),
        "credits_consumed": 0,
        "required_credits": consumption_result.get(
          "required_credits",
          float(base_cost),
        ),
        "available_credits": consumption_result.get("available_credits", 0),
      }

  def get_credit_summary(
    self, graph_id: str, user_id: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get comprehensive credit summary for a graph or shared repository.

    For user graphs: Returns credit summary from GraphCredits table (graph-specific).
    For shared repositories: Returns credit summary from UserRepositoryCredits table (user-specific).

    Args:
        graph_id: Graph or repository identifier
        user_id: User ID (required for shared repositories)

    Returns:
        Dict with credit summary information
    """
    # Check if this is a shared repository
    if self._is_shared_repository(graph_id):
      # For shared repositories, we need user_id to fetch user-specific credits
      if not user_id:
        return {"error": "User ID required for shared repository credit summary"}

      # Get user's repository credits
      user_repo_credits = UserRepositoryCredits.get_user_repository_credits(
        user_id=user_id,
        repository_type=graph_id,  # repository_type is the graph_id for shared repos
        session=self.session,
      )

      if not user_repo_credits:
        return {
          "error": f"No repository credit pool found for user {user_id} and repository {graph_id}"
        }

      # Get the repository credit summary
      repo_summary = user_repo_credits.get_summary()

      # Transform to match CreditSummaryResponse format
      # Repository credits use a different tier system, but we map to graph_tier for consistency
      return {
        "graph_id": graph_id,
        "graph_tier": f"{graph_id}-repository",  # e.g., "sec-repository"
        "current_balance": repo_summary["current_balance"],
        "monthly_allocation": repo_summary["monthly_allocation"],
        "consumed_this_month": repo_summary["consumed_this_month"],
        "transaction_count": 0,  # Repository transactions tracked separately
        "usage_percentage": repo_summary["usage_percentage"],
        "last_allocation_date": repo_summary["last_allocation_date"],
      }

    # For user graphs (and subgraphs), use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    # Try cache first
    from ...middleware.billing.cache import credit_cache

    cached_summary = credit_cache.get_cached_credit_summary(parent_graph_id)
    if cached_summary:
      return cached_summary

    # Fallback to database using parent graph ID (subgraphs share parent's pool)
    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": f"No credit pool found for graph {parent_graph_id}"}

    summary = credits.get_usage_summary(self.session)

    # Cache the summary using parent_graph_id (subgraphs share parent's cache)
    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.cache_credit_summary(parent_graph_id, summary)
    except Exception as e:
      logger.warning(f"Failed to cache credit summary: {e}")

    return summary

  def allocate_monthly_credits(self, graph_id: str) -> Dict[str, Any]:
    """Allocate monthly credits if due."""
    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    allocated = credits.allocate_monthly_credits(self.session)

    if allocated:
      self.session.commit()

      # Invalidate cache after allocation using parent_graph_id
      try:
        from ...middleware.billing.cache import credit_cache

        credit_cache.invalidate_graph_credit_balance(parent_graph_id)
      except Exception as e:
        logger.warning(f"Failed to invalidate credit cache after allocation: {e}")

      return {
        "success": True,
        "allocated_credits": float(credits.monthly_allocation),
        "new_balance": float(credits.current_balance),
        "allocation_date": credits.last_allocation_date.isoformat(),
      }
    else:
      return {"success": False, "message": "Monthly allocation not due yet"}

  def add_bonus_credits(
    self,
    graph_id: str,
    amount: Decimal,
    description: str,
    metadata: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """Add bonus credits to a graph."""
    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    # Add credits
    credits.current_balance += amount
    credits.updated_at = datetime.now(timezone.utc)

    # Record transaction with idempotency using parent_graph_id
    import uuid

    idempotency_key = f"bonus_{parent_graph_id}_{uuid.uuid4()}"

    GraphCreditTransaction.create_transaction(
      graph_credits_id=credits.id,
      transaction_type=CreditTransactionType.BONUS,
      amount=amount,
      description=description,
      metadata=metadata,
      session=self.session,
      idempotency_key=idempotency_key,
      graph_id=parent_graph_id,
      user_id=credits.user_id,
    )

    self.session.commit()

    # Invalidate cache after bonus credits using parent_graph_id
    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.invalidate_graph_credit_balance(parent_graph_id)
    except Exception as e:
      logger.warning(f"Failed to invalidate credit cache after bonus: {e}")

    return {
      "success": True,
      "credits_added": float(amount),
      "new_balance": float(credits.current_balance),
      "description": description,
    }

  def get_credit_transactions(
    self,
    graph_id: str,
    transaction_type: Optional[CreditTransactionType] = None,
    limit: int = 100,
  ) -> List[Dict[str, Any]]:
    """Get credit transactions for a graph."""
    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return []

    transactions = GraphCreditTransaction.get_transactions_for_graph(
      graph_credits_id=credits.id,
      transaction_type=transaction_type,
      limit=limit,
      session=self.session,
    )

    return [
      {
        "id": t.id,
        "type": t.transaction_type,
        "amount": float(t.amount),
        "description": t.description,
        "metadata": t.get_metadata(),
        "created_at": t.created_at.isoformat(),
      }
      for t in transactions
    ]

  def check_credit_balance(
    self,
    graph_id: str,
    required_credits: Decimal,
    user_id: Optional[str] = None,
    operation_type: str = "query",
  ) -> Dict[str, Any]:
    """Check if graph has sufficient credits for an operation."""
    # Check if this is a shared repository
    if self._is_shared_repository(graph_id):
      if not user_id:
        return {
          "has_sufficient_credits": False,
          "error": "User ID required for shared repository access",
        }

      # Route to shared repository credit system
      shared_check = self.check_shared_repository_access(
        user_id=user_id,
        repository_name=graph_id,
        operation_type=operation_type,
        required_credits=required_credits,  # Pass the required credits for AI operations
      )

      # Convert shared repository response to standard format
      if shared_check.get("has_access", False):
        return {
          "has_sufficient_credits": shared_check.get("has_sufficient_credits", False),
          "required_credits": shared_check.get(
            "required_credits", float(required_credits)
          ),
          "available_credits": shared_check.get("available_credits", 0),
          "base_cost": float(required_credits),
          "cached": False,
          "repository_type": "shared",
        }
      else:
        return {
          "has_sufficient_credits": False,
          "error": shared_check.get("error", "No access to shared repository"),
          "requires_addon": shared_check.get("requires_addon", False),
        }

    # Original graph credit logic for regular graphs
    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    # Try cache first for quick balance check
    from ...middleware.billing.cache import credit_cache

    cached_data = credit_cache.get_cached_graph_credit_balance(parent_graph_id)

    if cached_data:
      balance, graph_tier = cached_data
      has_sufficient = balance >= required_credits

      return {
        "has_sufficient_credits": has_sufficient,
        "required_credits": float(required_credits),
        "available_credits": float(balance),
        "base_cost": float(required_credits),
        "cached": True,
        "repository_type": "graph",
      }

    # Fallback to database using parent graph ID (subgraphs share parent's pool)
    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {
        "has_sufficient_credits": False,
        "error": "No credit pool found for graph",
      }

    # Calculate actual remaining balance (allocation - consumed)
    consumed_this_month = self._get_consumed_this_month(parent_graph_id)
    actual_balance = float(credits.monthly_allocation) - float(consumed_this_month)

    has_sufficient = Decimal(str(actual_balance)) >= required_credits

    # Cache the balance for future checks using parent_graph_id
    try:
      from ...middleware.billing.cache import credit_cache

      # graph_tier is always a string from the property
      graph_tier_value = credits.graph_tier

      credit_cache.cache_graph_credit_balance(
        graph_id=parent_graph_id,
        balance=Decimal(str(actual_balance)),
        graph_tier=graph_tier_value,
      )
    except Exception as e:
      logger.warning(f"Failed to cache credit balance: {e}")

    return {
      "has_sufficient_credits": has_sufficient,
      "required_credits": float(required_credits),
      "available_credits": actual_balance,
      "base_cost": float(required_credits),
      "remaining_balance": float(credits.current_balance),
      "cached": False,
      "repository_type": "graph",
    }

  def get_subscription_tier_limits(self, subscription_tier: str) -> Dict[str, Any]:
    """Get limits and features for a subscription tier."""
    allowed_tiers = {
      "ladybug-standard": [GraphTier.LADYBUG_STANDARD],
      "ladybug-large": [GraphTier.LADYBUG_STANDARD, GraphTier.LADYBUG_LARGE],
      "ladybug-xlarge": [
        GraphTier.LADYBUG_STANDARD,
        GraphTier.LADYBUG_LARGE,
        GraphTier.LADYBUG_XLARGE,
        GraphTier.NEO4J_COMMUNITY_LARGE,
        GraphTier.NEO4J_ENTERPRISE_XLARGE,
      ],
    }

    plan_config = BillingConfig.get_subscription_plan(subscription_tier)

    return {
      "subscription_tier": subscription_tier,
      "monthly_price": plan_config["base_price_cents"] / 100 if plan_config else 0,
      "monthly_credits": plan_config["monthly_credit_allocation"] if plan_config else 0,
      "allowed_graph_tiers": [
        tier.value for tier in allowed_tiers.get(subscription_tier, [])
      ],
      "features": {
        "backup_retention_days": plan_config["backup_retention_days"]
        if plan_config
        else 0,
        "priority_support": plan_config["priority_support"] if plan_config else False,
        "storage_billing": "Variable based on usage - charged daily at 0.05 credits per GB",
        "credit_based_storage": True,
      },
    }

  def upgrade_graph_tier(
    self, graph_id: str, new_tier: GraphTier, user_subscription_tier: str
  ) -> Dict[str, Any]:
    """
    Upgrade a graph to a new tier.

    Note: This is not allowed in the current system design as it would
    require complex migration logic. This method is for future use.
    """
    # For now, we don't allow tier upgrades
    return {
      "success": False,
      "error": "Graph tier upgrades are not supported",
      "message": "Each graph tier is architecturally optimized and cannot be changed after creation",
    }

  def _get_consumed_this_month(self, graph_id: str) -> Decimal:
    """Get total credits consumed this month for a graph."""
    from sqlalchemy import func
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # For subgraphs, use parent graph ID to access shared credit pool
    parent_graph_id = self._get_parent_graph_id(graph_id)

    # Get the graph credits record using parent_graph_id
    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return Decimal("0")

    # Sum all consumption transactions for this month
    result = (
      self.session.query(func.sum(GraphCreditTransaction.amount))
      .filter(
        GraphCreditTransaction.graph_credits_id == credits.id,
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
        GraphCreditTransaction.created_at >= month_start,
      )
      .scalar()
    )

    # Return absolute value since consumption transactions are negative
    return abs(result) if result else Decimal("0")

  def _can_create_graph_tier(
    self, subscription_tier: str, graph_tier: GraphTier
  ) -> bool:
    """Check if subscription tier allows creating a graph of the specified tier."""
    tier_restrictions = {
      "ladybug-standard": [GraphTier.LADYBUG_STANDARD],
      "ladybug-large": [GraphTier.LADYBUG_STANDARD, GraphTier.LADYBUG_LARGE],
      "ladybug-xlarge": [
        GraphTier.LADYBUG_STANDARD,
        GraphTier.LADYBUG_LARGE,
        GraphTier.LADYBUG_XLARGE,
        GraphTier.NEO4J_COMMUNITY_LARGE,
        GraphTier.NEO4J_ENTERPRISE_XLARGE,
      ],
    }

    allowed_tiers = tier_restrictions.get(subscription_tier, [])
    return graph_tier in allowed_tiers

  def bulk_allocate_monthly_credits(self) -> Dict[str, Any]:
    """Allocate monthly credits for all graphs that are due."""
    # Get all credit records that need allocation

    # Find graphs that haven't been allocated in the past 30 days
    now = datetime.now(timezone.utc)
    cutoff_date = now.replace(day=1)  # First of current month

    due_allocations = (
      self.session.query(GraphCredits)
      .filter(
        (GraphCredits.last_allocation_date.is_(None))
        | (GraphCredits.last_allocation_date < cutoff_date)
      )
      .all()
    )

    allocated_count = 0
    total_credits = Decimal("0")

    for credits in due_allocations:
      if credits.allocate_monthly_credits(self.session):
        allocated_count += 1
        total_credits += credits.monthly_allocation

    self.session.commit()

    # Invalidate all credit caches after bulk allocation
    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.invalidate_all_graph_credits()
    except Exception as e:
      logger.warning(f"Failed to invalidate all credit caches: {e}")

    return {
      "allocated_graphs": allocated_count,
      "total_credits_allocated": float(total_credits),
      "allocation_date": now.isoformat(),
    }

  def get_all_credit_summaries(self, user_id: str) -> List[Dict[str, Any]]:
    """Get credit summaries for all graphs owned by a user."""
    # Get all graphs for the user
    user_graphs = (
      self.session.query(GraphUser).filter(GraphUser.user_id == user_id).all()
    )

    summaries = []
    for user_graph in user_graphs:
      summary = self.get_credit_summary(user_graph.graph_id)
      if "error" not in summary:
        summary["graph_name"] = user_graph.graph.graph_name
        summary["role"] = user_graph.role
        summaries.append(summary)

    return summaries

  def consume_shared_repository_credits(
    self,
    user_id: str,
    repository_name: str,
    operation_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    cached: bool = False,
    base_cost: Optional[Decimal] = None,
  ) -> Dict[str, Any]:
    """
    Consume credits for a shared repository operation.

    Args:
        user_id: User identifier
        repository_name: Name of shared repository (e.g., 'sec', 'industry')
        operation_type: Type of operation (e.g., 'query', 'analytics')
        metadata: Optional metadata for the transaction
        cached: Whether this is a cached operation (no credit consumption)

    Returns:
        Dict with consumption results
    """
    # Cached operations don't consume credits
    if cached:
      return {
        "success": True,
        "credits_consumed": 0,
        "cached": True,
        "message": "Cached operation - no credits consumed",
      }

    # Get shared credits for this repository
    shared_credits = UserRepositoryCredits.get_user_repository_credits(
      user_id=user_id, repository_type=repository_name, session=self.session
    )

    if not shared_credits:
      return {
        "success": False,
        "error": f"No active add-on for {repository_name} repository",
        "credits_consumed": 0,
        "requires_addon": True,
      }

    # Get operation cost - use passed base_cost if provided (e.g., for AI tokens)
    # Otherwise use predefined costs or default
    if base_cost is None:
      repo_costs = SHARED_REPO_CREDIT_COSTS.get(repository_name, {})
      base_cost = repo_costs.get(operation_type, Decimal("1.0"))

    # Consume credits
    success = shared_credits.consume_credits(
      amount=base_cost,
      repository_name=repository_name,
      operation_type=operation_type,
      session=self.session,
      metadata=metadata,
    )

    if success:
      self.session.commit()

      return {
        "success": True,
        "credits_consumed": float(base_cost),
        "remaining_balance": float(shared_credits.current_balance),
        "cached": False,
        "addon_type": shared_credits.user_repository.repository_type.value,
        "addon_tier": shared_credits.user_repository.repository_plan.value,
      }
    else:
      return {
        "success": False,
        "error": "Insufficient shared repository credits",
        "credits_consumed": 0,
        "required_credits": float(base_cost),
        "available_credits": float(shared_credits.current_balance),
        "addon_type": shared_credits.user_repository.repository_type.value,
        "addon_tier": shared_credits.user_repository.repository_plan.value,
      }

  def get_shared_repository_summary(self, user_id: str) -> Dict[str, Any]:
    """Get summary of all shared repository credits for a user."""
    access_records = UserRepository.get_user_repositories(user_id, self.session)

    summaries = {}
    for access_record in access_records:
      if access_record.user_credits:
        repo_type = access_record.repository_type.value
        summaries[repo_type] = {
          "access_id": access_record.id,
          "repository_type": access_record.repository_type.value,
          "subscription_tier": access_record.repository_plan.value,
          "access_level": access_record.access_level.value,
          "credits": access_record.user_credits.get_summary(),
        }

    return summaries

  def check_shared_repository_access(
    self,
    user_id: str,
    repository_name: str,
    operation_type: str,
    required_credits: Optional[Decimal] = None,
  ) -> Dict[str, Any]:
    """Check if user has access and sufficient credits for a shared repository operation."""
    # Get shared credits
    shared_credits = UserRepositoryCredits.get_user_repository_credits(
      user_id=user_id, repository_type=repository_name, session=self.session
    )

    if not shared_credits:
      return {
        "has_access": False,
        "error": f"No active subscription for {repository_name} repository",
        "requires_subscription": True,
      }

    # Check if subscription is active
    if not shared_credits.user_repository.is_active:
      return {
        "has_access": False,
        "error": "Subscription is not active",
        "addon_type": shared_credits.user_repository.repository_type.value,
        "addon_tier": shared_credits.user_repository.repository_plan.value,
      }

    # Get operation cost - use passed required_credits if provided (e.g., for AI tokens)
    # Otherwise use predefined costs or default
    if required_credits is None:
      repo_costs = SHARED_REPO_CREDIT_COSTS.get(repository_name, {})
      required_credits = repo_costs.get(operation_type, Decimal("1.0"))
      # If operation is included (0.0), no credit check needed
      if required_credits == Decimal("0.0"):
        return {
          "has_access": True,
          "has_sufficient_credits": True,
          "required_credits": 0.0,
          "available_credits": float(shared_credits.current_balance),
          "addon_type": shared_credits.user_repository.repository_type.value,
          "addon_tier": shared_credits.user_repository.repository_plan.value,
          "operation_included": True,
        }

    has_sufficient = shared_credits.current_balance >= required_credits  # type: ignore[operator]

    return {
      "has_access": True,
      "has_sufficient_credits": has_sufficient,
      "required_credits": float(required_credits),
      "available_credits": float(shared_credits.current_balance),
      "addon_type": shared_credits.user_repository.repository_type.value,
      "addon_tier": shared_credits.user_repository.repository_plan.value,
    }

  def _addon_type_to_repo_name(self, addon_type: str) -> str:
    """Convert add-on type to repository name."""
    mapping = {
      RepositoryType.SEC.value: "sec",
      RepositoryType.INDUSTRY.value: "industry",
      RepositoryType.ECONOMIC.value: "economic",
    }
    return mapping.get(addon_type, addon_type)

  def _repo_name_to_addon_type(self, repo_name: str) -> Optional[RepositoryType]:
    """Convert repository name to repository type."""
    mapping = {
      "sec": RepositoryType.SEC,
      "industry": RepositoryType.INDUSTRY,
      "economic": RepositoryType.ECONOMIC,
    }
    return mapping.get(repo_name)

  def check_storage_limit(
    self, graph_id: str, current_storage_gb: Optional[Decimal] = None
  ) -> Dict[str, Any]:
    """
    Check storage limits for a graph and provide recommendations.

    Note: Storage limits only apply to user graphs, not shared repositories.
    Shared repositories are managed internally and don't expose storage to users.

    Args:
        graph_id: Graph identifier
        current_storage_gb: Current storage usage (fetched if not provided)

    Returns:
        Dict with storage limit information and status
    """
    # Shared repositories don't have user-facing storage limits
    if self._is_shared_repository(graph_id):
      return {"error": "Storage limits not applicable for shared repositories"}

    # Get credit record
    credits = GraphCredits.get_by_graph_id(graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    # Get current storage if not provided
    if current_storage_gb is None:
      # Try to get latest storage usage from tracking
      from ...models.iam.graph_usage import GraphUsage, UsageEventType
      from sqlalchemy import desc

      latest_usage = (
        self.session.query(GraphUsage)
        .filter(
          GraphUsage.graph_id == graph_id,
          GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        )
        .order_by(desc(GraphUsage.recorded_at))
        .first()
      )

      current_storage_gb = (
        Decimal(str(latest_usage.storage_gb))
        if latest_usage and latest_usage.storage_gb
        else Decimal("0")
      )

    # Check limits
    limit_check = credits.check_storage_limit(current_storage_gb)

    # Add graph_id to the response
    limit_check["graph_id"] = graph_id

    # Add recommendations
    if not limit_check["within_limit"]:
      limit_check["recommendations"] = [
        "Contact support to request storage limit increase",
        "Consider archiving or cleaning up unused data",
        "Review data import processes for efficiency",
      ]
    elif limit_check["approaching_limit"]:
      limit_check["recommendations"] = [
        "Monitor storage usage closely",
        "Plan for potential storage limit increase",
        "Review current data retention policies",
      ]

    return limit_check

  def set_storage_override(
    self,
    graph_id: str,
    new_limit_gb: Decimal,
    admin_user_id: str,
    reason: str,
  ) -> Dict[str, Any]:
    """
    Set storage override limit (admin only).

    Args:
        graph_id: Graph identifier
        new_limit_gb: New storage limit in GB
        admin_user_id: Admin user setting the override
        reason: Reason for the override

    Returns:
        Dict with override results
    """
    # Get credit record
    credits = GraphCredits.get_by_graph_id(graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    old_limit = credits.get_effective_storage_limit()

    # Set override
    credits.set_storage_override(
      new_limit_gb=new_limit_gb,
      admin_user_id=admin_user_id,
      reason=reason,
      session=self.session,
    )

    self.session.commit()

    return {
      "success": True,
      "graph_id": graph_id,
      "old_limit_gb": float(old_limit),
      "new_limit_gb": float(new_limit_gb),
      "admin_user_id": admin_user_id,
      "reason": reason,
      "override_set_at": datetime.now(timezone.utc).isoformat(),
    }

  def get_storage_limit_violations(self) -> List[Dict[str, Any]]:
    """Get all graphs that are exceeding their storage limits."""
    from ...models.iam.graph_usage import GraphUsage, UsageEventType
    from sqlalchemy import func

    # Get latest storage usage for each graph
    latest_usage_subquery = (
      self.session.query(
        GraphUsage.graph_id,
        func.max(GraphUsage.recorded_at).label("latest_time"),
      )
      .filter(GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value)
      .group_by(GraphUsage.graph_id)
      .subquery()
    )

    # Get graphs with current storage usage
    current_usage = (
      self.session.query(
        GraphUsage.graph_id,
        GraphUsage.storage_gb,
        GraphUsage.user_id,
      )
      .join(
        latest_usage_subquery,
        (GraphUsage.graph_id == latest_usage_subquery.c.graph_id)
        & (GraphUsage.recorded_at == latest_usage_subquery.c.latest_time),
      )
      .all()
    )

    violations = []

    for usage in current_usage:
      credits = GraphCredits.get_by_graph_id(usage.graph_id, self.session)
      if credits and usage.storage_gb:
        limit_check = credits.check_storage_limit(Decimal(str(usage.storage_gb)))

        if not limit_check["within_limit"] or limit_check["approaching_limit"]:
          violations.append(
            {
              "graph_id": usage.graph_id,
              "user_id": usage.user_id,
              "graph_tier": credits.graph_tier,
              "current_storage_gb": limit_check["current_storage_gb"],
              "effective_limit_gb": limit_check["effective_limit_gb"],
              "usage_percentage": limit_check["usage_percentage"],
              "exceeds_limit": not limit_check["within_limit"],
              "approaching_limit": limit_check["approaching_limit"],
              "has_override": limit_check["has_override"],
              "needs_warning": limit_check["needs_warning"],
            }
          )

    return violations

  def consume_ai_tokens(
    self,
    graph_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
    metadata: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
  ) -> Dict[str, Any]:
    """
    Consume credits based on actual AI token usage.

    This method is called AFTER the AI operation completes with actual token counts.
    It uses the same rock-solid atomic credit consumption but with precise costs.

    Args:
        graph_id: Graph identifier
        input_tokens: Actual input tokens used
        output_tokens: Actual output tokens generated
        model: AI model used (e.g., 'claude-3-opus', 'gpt-4')
        operation_description: Description of the AI operation
        metadata: Optional metadata for the transaction
        user_id: User ID for tracking

    Returns:
        Dict with consumption results
    """
    from ...config import AIBillingConfig

    # Map model names to pricing
    model_pricing_map = {
      # Claude 4/4.1 models (current)
      "claude-4-opus": "anthropic_claude_4_opus",
      "claude-4.1-opus": "anthropic_claude_4.1_opus",
      "claude-opus-4.1-20250805": "anthropic_claude_4.1_opus",  # Full model ID
      "claude-4-sonnet": "anthropic_claude_4_sonnet",
      "claude-4.1-sonnet": "anthropic_claude_4_sonnet",
      # Legacy Claude 3 models
      "claude-3-opus": "anthropic_claude_3_opus",
      "claude-3-sonnet": "anthropic_claude_3_sonnet",
      "claude-3.5-sonnet": "anthropic_claude_3_sonnet",
      # OpenAI models
      "gpt-4": "openai_gpt4",
      "gpt-3.5-turbo": "openai_gpt35",
    }

    # Get pricing for the model
    pricing_key = model_pricing_map.get(model.lower(), "anthropic_claude_4_sonnet")
    pricing = AIBillingConfig.TOKEN_PRICING.get(pricing_key)

    if not pricing:
      logger.warning(
        f"No pricing found for model {model}, using default Sonnet pricing"
      )
      pricing = {
        "input": Decimal("0.01"),  # Default to Sonnet 4 with 3.33x markup
        "output": Decimal("0.05"),
      }

    # Calculate actual cost based on tokens
    input_cost = (Decimal(input_tokens) / 1000) * pricing["input"]
    output_cost = (Decimal(output_tokens) / 1000) * pricing["output"]
    raw_cost = input_cost + output_cost

    # Apply minimum charge (rounds up to at least 0.01)
    total_cost = AIBillingConfig.apply_minimum_charge(raw_cost)

    # Build metadata
    token_metadata = {
      "input_tokens": input_tokens,
      "output_tokens": output_tokens,
      "total_tokens": input_tokens + output_tokens,
      "model": model,
      "input_cost": str(input_cost),
      "output_cost": str(output_cost),
      "raw_cost": str(raw_cost),
      "total_cost": str(total_cost),
      "minimum_charge_applied": total_cost > raw_cost,
    }
    if metadata:
      token_metadata.update(metadata)

    # Use existing consume_credits with minimum-applied cost
    return self.consume_credits(
      graph_id=graph_id,
      operation_type="ai_tokens",
      base_cost=total_cost,
      metadata=token_metadata,
      user_id=user_id,
    )

  def consume_storage_credits(
    self,
    graph_id: str,
    storage_gb: Decimal,
    metadata: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """
    Consume credits for daily storage overage.

    Only charges for storage ABOVE the included limit in the subscription.
    Storage charges can result in negative balances - users cannot "turn off" storage.

    Args:
        graph_id: Graph identifier
        storage_gb: Average storage usage in GB for the day
        metadata: Optional metadata for the transaction

    Returns:
        Dict with consumption results
    """
    # Get credit record from database
    credits = GraphCredits.get_by_graph_id(graph_id, self.session)
    if not credits:
      return {
        "success": False,
        "error": "No credit pool found for graph",
        "credits_consumed": 0,
      }

    # Get subscription plan to find included storage
    graph_tier = (
      credits.graph_tier
      if hasattr(credits.graph_tier, "value")
      else str(credits.graph_tier)
    )
    plan_config = BillingConfig.get_subscription_plan(graph_tier)

    if not plan_config:
      logger.warning(
        f"No billing plan found for tier {graph_tier}, treating all storage as overage"
      )
      included_gb = Decimal("0")
    else:
      included_gb = Decimal(str(plan_config.get("included_gb", 0)))

    # Calculate overage (only charge for storage above included limit)
    overage_gb = max(Decimal("0"), storage_gb - included_gb)

    # If no overage, no charges
    if overage_gb <= 0:
      return {
        "success": True,
        "credits_consumed": 0,
        "overage_gb": 0,
        "included_gb": float(included_gb),
        "total_storage_gb": float(storage_gb),
        "remaining_balance": float(credits.current_balance),
        "went_negative": False,
        "storage_charge": True,
        "message": "Storage within included limit - no charges applied",
      }

    # Calculate overage cost (10 credits per GB per day)
    overage_cost = get_operation_cost("storage_daily") * overage_gb

    # Storage charges are always applied, even if it results in negative balance
    old_balance = credits.current_balance
    credits.current_balance -= overage_cost
    credits.updated_at = datetime.now(timezone.utc)

    # Record transaction
    transaction_metadata = {
      "total_storage_gb": str(storage_gb),
      "included_gb": str(included_gb),
      "overage_gb": str(overage_gb),
      "overage_cost": str(overage_cost),
      "credits_per_gb_day": "10",
      "old_balance": str(old_balance),
      "new_balance": str(credits.current_balance),
      "allows_negative": True,
      "graph_tier": graph_tier,
    }
    if metadata:
      transaction_metadata.update(metadata)

    GraphCreditTransaction.create_transaction(
      graph_credits_id=credits.id,
      transaction_type=CreditTransactionType.CONSUMPTION,
      amount=-overage_cost,
      description=f"Daily storage overage: {overage_gb} GB (total: {storage_gb} GB, included: {included_gb} GB)",
      metadata=transaction_metadata,
      session=self.session,
    )

    self.session.commit()

    # Update cache with new balance
    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.update_cached_balance_after_consumption(graph_id, overage_cost)
    except Exception as e:
      logger.warning(f"Failed to update credit cache after storage consumption: {e}")

    # Determine if balance went negative
    went_negative = old_balance >= 0 and credits.current_balance < 0

    return {
      "success": True,
      "credits_consumed": float(overage_cost),
      "overage_gb": float(overage_gb),
      "included_gb": float(included_gb),
      "total_storage_gb": float(storage_gb),
      "credits_per_gb_day": 10,
      "remaining_balance": float(credits.current_balance),
      "went_negative": went_negative,
      "old_balance": float(old_balance),
      "storage_charge": True,
    }


# Map operation costs to use centralized configuration
CREDIT_COSTS = {
  "api_call": CreditConfig.OPERATION_COSTS["api_call"],
  "query": CreditConfig.OPERATION_COSTS["query"],
  "mcp_call": CreditConfig.OPERATION_COSTS["mcp_call"],
  "agent_call": CreditConfig.OPERATION_COSTS["agent_call"],
  "ai_analysis": CreditConfig.OPERATION_COSTS.get("ai_analysis", Decimal("100")),
  "import": CreditConfig.OPERATION_COSTS["import"],
  "backup": CreditConfig.OPERATION_COSTS["backup"],
  "analytics": CreditConfig.OPERATION_COSTS["analytics"],
  "sync": CreditConfig.OPERATION_COSTS["sync"],
  "storage_daily": CreditConfig.OPERATION_COSTS["storage_per_gb_day"],
}


def get_operation_cost(operation_type: str) -> Decimal:
  """Get the base credit cost for an operation type."""
  # Try cache first
  try:
    from ...middleware.billing.cache import credit_cache

    cached_cost = credit_cache.get_cached_operation_cost(operation_type)
    if cached_cost is not None:
      return cached_cost
  except Exception:
    pass  # Fallback if cache not available

  # Fallback to constant
  # Default to 0 for unknown operations (database operations don't consume credits)
  cost = CREDIT_COSTS.get(operation_type, Decimal("0"))

  # Cache the cost
  try:
    from ...middleware.billing.cache import credit_cache

    credit_cache.cache_operation_cost(operation_type, cost)
  except Exception as e:
    logger.warning(f"Failed to cache operation cost: {e}")

  return cost


# Shared repository credit costs (usually higher than graph operations)
SHARED_REPO_CREDIT_COSTS = {
  "sec": {
    "query": Decimal("0.0"),  # Included - SEC data query (rate-limited only)
    "mcp": Decimal("0.0"),  # Included - MCP query (rate-limited only)
    "entity_lookup": Decimal(
      "0.0"
    ),  # Included - Basic entity lookup (rate-limited only)
    "filing_fetch": Decimal("0.0"),  # Included - Fetch filing data (rate-limited only)
    "analytics": Decimal("0.0"),  # Included - Complex analytics (rate-limited only)
    "ai_tokens": None,  # Dynamic - calculated based on actual token usage
    "bulk_export": Decimal("50.0"),  # Bulk data export
  },
  "industry": {
    "query": Decimal("3.0"),  # Industry benchmark query
    "comparison": Decimal("10.0"),  # Multi-entity comparison
    "analytics": Decimal("25.0"),  # Industry analytics
  },
  "economic": {
    "query": Decimal("1.0"),  # Economic indicator query
    "time_series": Decimal("5.0"),  # Time series data
    "analytics": Decimal("15.0"),  # Economic analytics
  },
  "market": {
    "quote": Decimal("0.5"),  # Single stock quote
    "history": Decimal("5.0"),  # Price history
    "analytics": Decimal("10.0"),  # Market analytics
  },
}
