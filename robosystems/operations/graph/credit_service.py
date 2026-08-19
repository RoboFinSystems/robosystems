"""Allocation, consumption, and reporting of graph credits.

Two credit pools live behind one interface. A user graph draws on
``GraphCredits``, keyed by graph; a shared repository draws on
``UserRepositoryCredits``, keyed by (user, repository) — which is why every
shared-repository path needs a ``user_id`` and a user-graph path does not.

Subgraphs never have their own pool: ``kg0123_dev`` and ``sec_historical``
resolve to ``kg0123`` and ``sec`` before any lookup, so a subgraph spends its
parent's credits and shares its cache entry.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from ...config import BillingConfig
from ...config.credits import CreditConfig
from ...config.graph_tier import GraphTier
from ...config.shared_repositories import (
  get_credit_costs as _get_credit_costs,
)
from ...config.shared_repositories import (
  is_shared_repository_or_subgraph as _is_shared_repository_or_subgraph,
)
from ...middleware.graph.types import parse_graph_id
from ...models.core import (
  Graph,
  GraphCredits,
  GraphCreditTransaction,
  GraphUsage,
  GraphUser,
)
from ...models.core.graph.graph_credits import CreditTransactionType
from ...models.core.user.user_repository import UserRepository
from ...models.core.user.user_repository_credits import (
  UserRepositoryCredits,
  UserRepositoryCreditTransaction,
  UserRepositoryCreditTransactionType,
)

logger = logging.getLogger(__name__)


class CreditService:
  """Service for managing graph credits and billing."""

  def __init__(self, session: Session):
    self.session = session

    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.warmup_operation_costs(CREDIT_COSTS)
    except Exception as e:
      # Non-fatal: the cache populates lazily on first access instead.
      logger.warning(f"Failed to warm up credit cache: {e}")

  def create_graph_credits(
    self,
    graph_id: str,
    user_id: str,
    billing_admin_id: str,
    subscription_tier: str,
    graph_tier: GraphTier = GraphTier.LADYBUG_STANDARD,
  ) -> GraphCredits:
    """Create the graph's credit pool from its subscription plan.

    Raises ValueError when the subscription tier is unknown or does not permit
    ``graph_tier``.
    """
    plan_config = BillingConfig.get_subscription_plan(subscription_tier)
    if not plan_config:
      raise ValueError(
        f"No billing plan found for subscription tier: {subscription_tier}"
      )

    if not self._can_create_graph_tier(subscription_tier, graph_tier):
      raise ValueError(
        f"Subscription tier '{subscription_tier}' does not allow '{graph_tier.value}' graph tier"
      )

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
    """Resolve a graph id to the id its credit pool is keyed by.

    ``kg0123_dev`` -> ``kg0123``; parents and repositories pass through.
    """
    parent_id, _ = parse_graph_id(graph_id)
    return parent_id

  def _is_shared_repository(self, graph_id: str) -> bool:
    """True for a shared repository *or* one of its subgraphs.

    Subgraphs must match: `sec_historical` bills against the user's `sec`
    pool. An exact-only check sends them down the user-graph path, where the
    `GraphCredits` lookup finds nothing and the operation runs unbilled.
    """
    return _is_shared_repository_or_subgraph(graph_id)

  def consume_credits(
    self,
    graph_id: str,
    operation_type: str,
    base_cost: Decimal,
    metadata: dict[str, Any] | None = None,
    cached: bool = False,
    user_id: str | None = None,
    idempotency_key: str | None = None,
    request_id: str | None = None,
    operation_id: str | None = None,
    drain_on_shortfall: bool = False,
  ) -> dict[str, Any]:
    """Debit ``base_cost`` for an operation, returning the outcome as a dict.

    Never raises on an insufficient balance — check ``success``. A cached
    operation is free. Shared repositories route to
    :meth:`consume_shared_repository_credits` and therefore require
    ``user_id``.

    The debit itself is atomic; everything after it (cache refresh, OTel
    metric, usage-ledger row) is best-effort and must never fail a debit that
    already committed.
    """
    if cached:
      return {
        "success": True,
        "credits_consumed": 0,
        "cached": True,
        "message": "Cached operation - no credits consumed",
      }

    if self._is_shared_repository(graph_id):
      if not user_id:
        return {
          "success": False,
          "error": "User ID required for shared repository operations",
          "credits_consumed": 0,
        }

      # The pool is keyed by repository id, so resolve a subgraph first.
      return self.consume_shared_repository_credits(
        user_id=user_id,
        repository_name=self._get_parent_graph_id(graph_id),
        operation_type=operation_type,
        metadata=metadata,
        cached=cached,
        base_cost=base_cost,
        drain_on_shortfall=drain_on_shortfall,
      )

    parent_graph_id = self._get_parent_graph_id(graph_id)

    # No cached pre-reject here, deliberately. When called with
    # drain_on_shortfall (the post-hoc AI path), an early return on a stale
    # cached balance would skip the drain the atomic consume below performs.
    # The cheap balance check belongs in the pre-flight (`check_credit_balance`),
    # not on this post-charge path.
    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {
        "success": False,
        "error": f"No credit pool found for graph {parent_graph_id}"
        + (f" (parent of subgraph {graph_id})" if graph_id != parent_graph_id else ""),
        "credits_consumed": 0,
      }

    consumption_result = credits.consume_credits_atomic(
      amount=base_cost,
      operation_type=operation_type,
      operation_description=f"{operation_type} operation on graph {graph_id}",
      session=self.session,
      request_id=request_id,
      user_id=user_id,
      metadata=metadata,
      drain_on_shortfall=drain_on_shortfall,
    )

    if consumption_result["success"]:
      try:
        from ...middleware.billing.cache import credit_cache

        credit_cache.invalidate_graph_credit_balance(parent_graph_id)

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

      # Resolved out here because both best-effort blocks below read it; a
      # binding inside the first would be missing whenever that block raised.
      graph_tier_val = (
        credits.graph_tier.value
        if hasattr(credits.graph_tier, "value")
        else str(credits.graph_tier)
      )

      try:
        from ...middleware.otel.metrics import get_endpoint_metrics

        get_endpoint_metrics().record_credit_consumption(
          operation_type=operation_type,
          credits_consumed=float(consumption_result["credits_consumed"]),
          graph_tier=graph_tier_val,
        )
      except Exception:
        pass  # Metrics are best-effort, never break credit consumption

      # Usage-ledger row, a *different* table from the credit ledger the
      # atomic consume just wrote: `graph_credit_transactions` is
      # authoritative for billing, while `graph_usage` is what
      # `/orgs/{org_id}/usage` and the per-graph usage views read. Both have
      # to be written or the dashboards under-report real spend.
      try:
        if user_id:
          GraphUsage.record_credit_consumption(
            user_id=user_id,
            graph_id=graph_id,
            graph_tier=graph_tier_val,
            operation_type=operation_type,
            credits_consumed=Decimal(str(consumption_result["credits_consumed"])),
            base_credit_cost=Decimal(str(consumption_result["base_cost"])),
            session=self.session,
            cached_operation=cached,
            metadata=metadata,
          )
        else:
          # `GraphUsage.user_id` is NOT NULL and consumption is attributed per
          # user, so an unattributed spend is dropped rather than forged.
          logger.warning(
            f"Credit consumption on {graph_id} has no user_id; "
            f"usage row skipped (operation_type={operation_type})"
          )
      except Exception as e:
        logger.warning(f"Failed to record credit usage for {graph_id}: {e}")

      return {
        "success": True,
        "credits_consumed": consumption_result["credits_consumed"],
        "base_cost": consumption_result["base_cost"],
        "remaining_balance": consumption_result["new_balance"],
        "cached": False,
        "transaction_id": consumption_result["transaction_id"],
      }
    else:
      drained = Decimal(str(consumption_result.get("credits_consumed", 0) or 0))

      # Drop the cached balance so the next check re-reads the database. On a
      # drain-to-zero, pin the cache at zero as well: the pre-flight prefers
      # the cached figure, and a stale positive balance there would re-admit
      # the very request the drain exists to stop.
      try:
        from ...middleware.billing.cache import credit_cache

        credit_cache.invalidate_graph_credit_balance(parent_graph_id)
        if consumption_result.get("drained_to_zero"):
          credit_cache.cache_graph_credit_balance(
            graph_id=parent_graph_id,
            balance=Decimal("0"),
            graph_tier=(
              credits.graph_tier.value
              if hasattr(credits.graph_tier, "value")
              else str(credits.graph_tier)
            ),
          )
      except Exception as e:
        logger.warning(f"Failed to update credit cache after shortfall: {e}")

      # A drain is real spend, so the usage ledger records it like any other
      # debit — otherwise the dashboards under-report exactly the calls that
      # exhausted the pool.
      if drained > 0 and user_id:
        try:
          GraphUsage.record_credit_consumption(
            user_id=user_id,
            graph_id=graph_id,
            graph_tier=(
              credits.graph_tier.value
              if hasattr(credits.graph_tier, "value")
              else str(credits.graph_tier)
            ),
            operation_type=operation_type,
            credits_consumed=drained,
            base_credit_cost=base_cost,
            session=self.session,
            cached_operation=cached,
            metadata=metadata,
          )
        except Exception as e:
          logger.warning(f"Failed to record drained credit usage for {graph_id}: {e}")

      return {
        "success": False,
        "error": consumption_result.get("error", "Credit consumption failed"),
        "credits_consumed": float(drained),
        "shortfall": consumption_result.get("shortfall"),
        "drained_to_zero": bool(consumption_result.get("drained_to_zero", False)),
        "required_credits": consumption_result.get(
          "required_credits",
          float(base_cost),
        ),
        "available_credits": consumption_result.get("available_credits", 0),
      }

  def get_credit_summary(
    self, graph_id: str, user_id: str | None = None
  ) -> dict[str, Any]:
    """Balance and usage for a graph, or for a user's repository pool.

    Errors are returned as ``{"error": ...}`` rather than raised.
    ``user_id`` is required for shared repositories.
    """
    if self._is_shared_repository(graph_id):
      if not user_id:
        return {"error": "User ID required for shared repository credit summary"}

      # The pool is keyed by repository id, so resolve a subgraph first.
      user_repo_credits = UserRepositoryCredits.get_user_repository_credits(
        user_id=user_id,
        repository_type=self._get_parent_graph_id(graph_id),
        session=self.session,
      )

      if not user_repo_credits:
        return {
          "error": f"No repository credit pool found for user {user_id} and repository {graph_id}"
        }

      repo_summary = user_repo_credits.get_summary()

      # Transform to match CreditSummaryResponse format
      # Repository credits use a different tier system, but we map to graph_tier for consistency
      return {
        "graph_id": graph_id,
        "graph_tier": f"{graph_id}-repository",  # e.g., "sec-repository"
        "current_balance": repo_summary["current_balance"],
        "monthly_allocation": repo_summary["monthly_allocation"],
        "consumed_this_month": repo_summary["consumed_this_month"],
        "transaction_count": self.session.query(UserRepositoryCreditTransaction.id)
        .filter(
          UserRepositoryCreditTransaction.credit_pool_id == user_repo_credits.id,
          UserRepositoryCreditTransaction.transaction_type
          == UserRepositoryCreditTransactionType.CONSUMPTION.value,
        )
        .count(),
        "usage_percentage": repo_summary["usage_percentage"],
        "last_allocation_date": repo_summary["last_allocation_date"],
      }

    parent_graph_id = self._get_parent_graph_id(graph_id)

    from ...middleware.billing.cache import credit_cache

    cached_summary = credit_cache.get_cached_credit_summary(parent_graph_id)
    if cached_summary:
      return cached_summary

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": f"No credit pool found for graph {parent_graph_id}"}

    summary = credits.get_usage_summary(self.session)

    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.cache_credit_summary(parent_graph_id, summary)
    except Exception as e:
      logger.warning(f"Failed to cache credit summary: {e}")

    return summary

  def allocate_monthly_credits(self, graph_id: str) -> dict[str, Any]:
    """Grant the monthly allocation if it is due; a no-op otherwise."""
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    allocated = credits.allocate_monthly_credits(self.session)

    if allocated:
      self.session.commit()

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
    metadata: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Credit the pool outside the monthly allocation, ledgered as BONUS."""
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    credits.current_balance += amount
    credits.updated_at = datetime.now(UTC)

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

  def reset_graph_pool(
    self,
    graph_id: str,
    initiated_by: str,
    reason: str | None = None,
  ) -> dict[str, Any]:
    """Reset a graph's pool to its monthly allocation, forfeiting the rest.

    Both movements land in the ledger (EXPIRATION for the remainder, then a
    manual ALLOCATION), and the scheduled monthly reset still fires normally
    when the month turns — `last_allocation_date` is left alone.
    """
    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    forfeited = credits.reset_pool(
      self.session, initiated_by=initiated_by, reason=reason
    )

    self.session.commit()

    try:
      from ...middleware.billing.cache import credit_cache

      credit_cache.invalidate_graph_credit_balance(parent_graph_id)
    except Exception as e:
      logger.warning(f"Failed to invalidate credit cache after pool reset: {e}")

    return {
      "success": True,
      "credits_forfeited": float(forfeited),
      "new_balance": float(credits.current_balance),
    }

  def get_credit_transactions(
    self,
    graph_id: str,
    transaction_type: CreditTransactionType | None = None,
    limit: int = 100,
  ) -> list[dict[str, Any]]:
    """Ledger entries for the graph's pool, newest first."""
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
    user_id: str | None = None,
    operation_type: str = "query",
  ) -> dict[str, Any]:
    """Preflight a spend without debiting anything.

    Advisory only: the balance can move between this check and the atomic
    consume, which is the real gate.
    """
    if self._is_shared_repository(graph_id):
      if not user_id:
        return {
          "has_sufficient_credits": False,
          "error": "User ID required for shared repository access",
        }

      # The pool is keyed by repository id, so resolve a subgraph first.
      shared_check = self.check_shared_repository_access(
        user_id=user_id,
        repository_name=self._get_parent_graph_id(graph_id),
        operation_type=operation_type,
        required_credits=required_credits,
      )

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

    parent_graph_id = self._get_parent_graph_id(graph_id)

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

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return {
        "has_sufficient_credits": False,
        "error": "No credit pool found for graph",
      }

    # Must be `current_balance`: that is the column `consume_credits_atomic`
    # decrements and gates its UPDATE on, so it is the only figure that decides
    # whether a spend succeeds. Any derived alternative would also collide with
    # the consume path in the shared cache key below.
    actual_balance = float(credits.current_balance)

    has_sufficient = Decimal(str(actual_balance)) >= required_credits

    try:
      from ...middleware.billing.cache import credit_cache

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

  def get_subscription_tier_limits(self, subscription_tier: str) -> dict[str, Any]:
    """Get limits and features for a subscription tier."""
    allowed_tiers = {
      "ladybug-standard": [GraphTier.LADYBUG_STANDARD],
      "ladybug-large": [GraphTier.LADYBUG_STANDARD, GraphTier.LADYBUG_LARGE],
      "ladybug-xlarge": [
        GraphTier.LADYBUG_STANDARD,
        GraphTier.LADYBUG_LARGE,
        GraphTier.LADYBUG_XLARGE,
      ],
    }

    plan_config = BillingConfig.get_subscription_plan(subscription_tier)

    from robosystems.config.graph_tier import GraphTierConfig

    backup_limits = GraphTierConfig.get_backup_limits(subscription_tier)

    return {
      "subscription_tier": subscription_tier,
      "monthly_price": plan_config["base_price_cents"] / 100 if plan_config else 0,
      "monthly_credits": plan_config["monthly_credit_allocation"] if plan_config else 0,
      "allowed_graph_tiers": [
        tier.value for tier in allowed_tiers.get(subscription_tier, [])
      ],
      "features": {
        "backup_retention_days": backup_limits.get("backup_retention_days", 0),
        "priority_support": plan_config["priority_support"] if plan_config else False,
        "storage_included": True,
      },
    }

  def upgrade_graph_tier(
    self, graph_id: str, new_tier: GraphTier, user_subscription_tier: str
  ) -> dict[str, Any]:
    """Always refuses. A graph's tier is fixed at creation.

    Kept as the explicit answer to "can I change a tier in place?"; use the
    change-tier operation, which provisions a new graph.
    """
    return {
      "success": False,
      "error": "Graph tier upgrades are not supported",
      "message": "Each graph tier is architecturally optimized and cannot be changed after creation",
    }

  def _get_consumed_this_month(self, graph_id: str) -> Decimal:
    """Sum of this calendar month's consumption, as a positive number."""
    from datetime import datetime

    from sqlalchemy import func

    now = datetime.now(UTC)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    parent_graph_id = self._get_parent_graph_id(graph_id)

    credits = GraphCredits.get_by_graph_id(parent_graph_id, self.session)
    if not credits:
      return Decimal("0")

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

    # Consumption rows are stored negative.
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
      ],
    }

    allowed_tiers = tier_restrictions.get(subscription_tier, [])
    return graph_tier in allowed_tiers

  def bulk_allocate_monthly_credits(self) -> dict[str, Any]:
    """Run the monthly allocation across every graph that is due.

    Skips inactive graphs (suspended, canceled) so a dormant graph does not
    keep accruing an allocation.
    """
    now = datetime.now(UTC)
    cutoff_date = now.replace(day=1)  # First of the current month

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
      graph = Graph.get_by_id(credits.graph_id, self.session)
      if not graph or not graph.is_active:
        continue

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

  def get_all_credit_summaries(self, user_id: str) -> list[dict[str, Any]]:
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
    metadata: dict[str, Any] | None = None,
    cached: bool = False,
    base_cost: Decimal | None = None,
    drain_on_shortfall: bool = False,
  ) -> dict[str, Any]:
    """Debit a user's repository pool. Missing add-on returns
    ``requires_addon``.

    ``base_cost`` overrides the repository's per-operation cost table; AI
    operations pass their token-derived cost that way.
    """
    if cached:
      return {
        "success": True,
        "credits_consumed": 0,
        "cached": True,
        "message": "Cached operation - no credits consumed",
      }

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

    if base_cost is None:
      repo_costs = _get_credit_costs(repository_name) or {}
      base_cost = repo_costs.get(operation_type, Decimal("1.0"))

    success = shared_credits.consume_credits(
      amount=base_cost,
      repository_name=repository_name,
      operation_type=operation_type,
      session=self.session,
      metadata=metadata,
      drain_on_shortfall=drain_on_shortfall,
    )

    if success:
      self.session.commit()

      repo_plan = shared_credits.user_repository.repository_plan
      addon_tier = repo_plan.value if hasattr(repo_plan, "value") else str(repo_plan)

      return {
        "success": True,
        "credits_consumed": float(base_cost),
        "remaining_balance": float(shared_credits.current_balance),
        "cached": False,
        "addon_type": shared_credits.user_repository.repository_type,
        "addon_tier": addon_tier,
      }
    else:
      repo_plan = shared_credits.user_repository.repository_plan
      addon_tier = repo_plan.value if hasattr(repo_plan, "value") else str(repo_plan)

      return {
        "success": False,
        "error": "Insufficient shared repository credits",
        "credits_consumed": 0,
        "required_credits": float(base_cost),
        "available_credits": float(shared_credits.current_balance),
        "addon_type": shared_credits.user_repository.repository_type,
        "addon_tier": addon_tier,
      }

  def get_shared_repository_summary(self, user_id: str) -> dict[str, Any]:
    """Every repository pool the user holds, keyed by repository type."""
    access_records = UserRepository.get_user_repositories(user_id, self.session)

    summaries = {}
    for access_record in access_records:
      if access_record.user_credits:
        repo_type = access_record.repository_type
        summaries[repo_type] = {
          "access_id": access_record.id,
          "repository_type": access_record.repository_type,
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
    required_credits: Decimal | None = None,
  ) -> dict[str, Any]:
    """Preflight a repository spend: subscription, then balance.

    A zero-cost operation short-circuits as included in the plan.
    """
    shared_credits = UserRepositoryCredits.get_user_repository_credits(
      user_id=user_id, repository_type=repository_name, session=self.session
    )

    if not shared_credits:
      return {
        "has_access": False,
        "error": f"No active subscription for {repository_name} repository",
        "requires_subscription": True,
      }

    if not shared_credits.user_repository.is_active:
      return {
        "has_access": False,
        "error": "Subscription is not active",
        "addon_type": shared_credits.user_repository.repository_type,
        "addon_tier": shared_credits.user_repository.repository_plan.value,
      }

    if required_credits is None:
      repo_costs = _get_credit_costs(repository_name) or {}
      required_credits = repo_costs.get(operation_type, Decimal("1.0"))
      if required_credits == Decimal("0.0"):
        return {
          "has_access": True,
          "has_sufficient_credits": True,
          "required_credits": 0.0,
          "available_credits": float(shared_credits.current_balance),
          "addon_type": shared_credits.user_repository.repository_type,
          "addon_tier": shared_credits.user_repository.repository_plan.value,
          "operation_included": True,
        }

    has_sufficient = shared_credits.current_balance >= required_credits  # type: ignore[operator]

    return {
      "has_access": True,
      "has_sufficient_credits": has_sufficient,
      "required_credits": float(required_credits),
      "available_credits": float(shared_credits.current_balance),
      "addon_type": shared_credits.user_repository.repository_type,
      "addon_tier": shared_credits.user_repository.repository_plan.value,
    }

  def set_storage_override(
    self,
    graph_id: str,
    new_limit_gb: Decimal,
    admin_user_id: str,
    reason: str,
  ) -> dict[str, Any]:
    """Raise or lower a graph's storage limit above its tier default.

    Admin-only; this method does not itself check the caller's role.
    """
    credits = GraphCredits.get_by_graph_id(graph_id, self.session)
    if not credits:
      return {"error": "No credit pool found for graph"}

    old_limit = credits.get_effective_storage_limit()

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
      "override_set_at": datetime.now(UTC).isoformat(),
    }

  def consume_ai_tokens(
    self,
    graph_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
    metadata: dict[str, Any] | None = None,
    user_id: str | None = None,
  ) -> dict[str, Any]:
    """Bill an AI call from its measured token counts.

    Called *after* the model responds, so the charge reflects real usage rather
    than an estimate. Rates are per 1,000 tokens and a floor
    (``AIBillingConfig.apply_minimum_charge``) applies, so a tiny call still
    costs the minimum. An unrecognised model falls back to Sonnet pricing
    rather than billing zero.
    """
    from ...config import AIBillingConfig

    model_pricing_map = {
      # Bedrock model IDs as they come back from AIClient
      "us.anthropic.claude-sonnet-4-6": "anthropic_claude_4_sonnet",
      "us.anthropic.claude-sonnet-4-5-20250929-v1:0": "anthropic_claude_4_sonnet",
      "us.anthropic.claude-sonnet-4-20250514-v1:0": "anthropic_claude_4_sonnet",
      # Short-form names
      "claude-4-sonnet": "anthropic_claude_4_sonnet",
      "claude-4.1-sonnet": "anthropic_claude_4_sonnet",
    }

    pricing_key = model_pricing_map.get(model.lower(), "anthropic_claude_4_sonnet")
    pricing = AIBillingConfig.TOKEN_PRICING.get(pricing_key)

    if not pricing:
      logger.warning(f"No pricing found for model {model}, using Sonnet pricing")
      pricing = {"input": Decimal("3"), "output": Decimal("15")}

    input_cost = (Decimal(input_tokens) / 1000) * pricing["input"]
    output_cost = (Decimal(output_tokens) / 1000) * pricing["output"]
    raw_cost = input_cost + output_cost

    total_cost = AIBillingConfig.apply_minimum_charge(raw_cost)

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

    return self.consume_credits(
      graph_id=graph_id,
      operation_type="ai_tokens",
      base_cost=total_cost,
      metadata=token_metadata,
      user_id=user_id,
      # Post-hoc: the model call already happened, so a short pool drains to
      # zero and records the shortfall rather than leaving a re-enterable band.
      drain_on_shortfall=True,
    )


# Flat per-operation costs. AI operations are not here: they are priced per
# token by `consume_ai_tokens`. Storage is included in each tier, unmetered.
CREDIT_COSTS = {
  "api_call": CreditConfig.OPERATION_COSTS["api_call"],
  "query": CreditConfig.OPERATION_COSTS["query"],
  "mcp_call": CreditConfig.OPERATION_COSTS["mcp_call"],
  "mcp_tool_call": CreditConfig.OPERATION_COSTS["mcp_tool_call"],
  "import": CreditConfig.OPERATION_COSTS["import"],
  "backup": CreditConfig.OPERATION_COSTS["backup"],
  "analytics": CreditConfig.OPERATION_COSTS["analytics"],
  "sync": CreditConfig.OPERATION_COSTS["sync"],
}


def get_operation_cost(operation_type: str) -> Decimal:
  """Flat credit cost for an operation; 0 for anything unlisted.

  Zero is the correct default — database operations are free, and only the
  operations named in ``CREDIT_COSTS`` and AI token usage are billed.
  """
  try:
    from ...middleware.billing.cache import credit_cache

    cached_cost = credit_cache.get_cached_operation_cost(operation_type)
    if cached_cost is not None:
      return cached_cost
  except Exception:
    pass

  cost = CREDIT_COSTS.get(operation_type, Decimal("0"))

  try:
    from ...middleware.billing.cache import credit_cache

    credit_cache.cache_operation_cost(operation_type, cost)
  except Exception as e:
    logger.warning(f"Failed to cache operation cost: {e}")

  return cost
