"""Credit system caching using Valkey/Redis."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, Tuple, cast

import redis

from ...config import env
from ...config.valkey_registry import ValkeyDatabase
from ...config.valkey_registry import create_redis_client
from ...logger import logger


class CreditCache:
  """Manages credit balance and transaction caching in Valkey/Redis."""

  # Cache configuration
  CACHE_KEY_PREFIX = "credits:"
  GRAPH_CREDIT_PREFIX = "graph_credit:"
  SHARED_CREDIT_PREFIX = "shared_credit:"
  CREDIT_SUMMARY_PREFIX = "credit_summary:"
  OPERATION_COST_PREFIX = "op_cost:"

  # Default TTLs
  BALANCE_TTL = 300  # 5 minutes for balance cache
  SUMMARY_TTL = 600  # 10 minutes for summary cache
  OPERATION_COST_TTL = 3600  # 1 hour for operation costs

  def __init__(self):
    """Initialize Redis connection for credit caching."""
    self._redis = None

    # TTL configuration
    self.balance_ttl = env.CREDIT_BALANCE_CACHE_TTL
    self.summary_ttl = env.CREDIT_SUMMARY_CACHE_TTL
    self.operation_cost_ttl = env.CREDIT_OPERATION_COST_CACHE_TTL

  @property
  def redis(self) -> redis.Redis:
    """Get Redis connection, creating if needed."""
    if self._redis is None:
      try:
        # Use the new connection factory with proper ElastiCache support
        self._redis = create_redis_client(ValkeyDatabase.CREDITS_CACHE)
        # Test connection
        self._redis.ping()
        logger.info("Connected to Valkey/Redis for credit caching")
      except Exception as e:
        logger.error(f"Failed to connect to Valkey/Redis for credit caching: {e}")
        raise
    return self._redis

  def _get_graph_credit_key(self, graph_id: str) -> str:
    """Get cache key for graph credit balance."""
    return f"{self.GRAPH_CREDIT_PREFIX}{graph_id}"

  def _get_shared_credit_key(self, user_id: str, repository: str) -> str:
    """Get cache key for shared repository credit balance."""
    return f"{self.SHARED_CREDIT_PREFIX}{user_id}:{repository}"

  def _get_credit_summary_key(self, graph_id: str) -> str:
    """Get cache key for credit summary."""
    return f"{self.CREDIT_SUMMARY_PREFIX}{graph_id}"

  def _get_operation_cost_key(self, operation_type: str) -> str:
    """Get cache key for operation cost."""
    return f"{self.OPERATION_COST_PREFIX}{operation_type}"

  def cache_graph_credit_balance(
    self, graph_id: str, balance: Decimal, graph_tier: str
  ) -> None:
    """
    Cache graph credit balance with metadata.

    Args:
        graph_id: Graph identifier
        balance: Current credit balance
        graph_tier: Graph tier (standard/enterprise/premium)
    """
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      cache_data = {
        "balance": str(balance),
        "graph_tier": graph_tier,
        "cached_at": datetime.now(timezone.utc).isoformat(),
      }

      self.redis.setex(cache_key, self.balance_ttl, json.dumps(cache_data))
      logger.debug(f"Cached graph credit balance for {graph_id}: {balance}")

    except Exception as e:
      logger.error(f"Failed to cache graph credit balance: {e}")

  def get_cached_graph_credit_balance(
    self, graph_id: str
  ) -> Optional[Tuple[Decimal, str]]:
    """
    Get cached graph credit balance.

    Returns:
        Tuple of (balance, graph_tier) or None if not cached
    """
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      cached_data = self.redis.get(cache_key)

      if cached_data:
        data = json.loads(str(cached_data))
        balance = Decimal(data["balance"])
        graph_tier = data["graph_tier"]
        logger.debug(f"Credit balance cache hit for graph {graph_id}")
        return balance, graph_tier

      logger.debug(f"Credit balance cache miss for graph {graph_id}")
      return None

    except Exception as e:
      logger.error(f"Failed to get cached credit balance: {e}")
      return None

  def update_cached_balance_after_consumption(
    self, graph_id: str, credits_consumed: Decimal
  ) -> None:
    """
    Update cached balance after credit consumption.

    This is an optimistic update - if the cache exists, we decrement it.
    If not, we let it refresh from DB on next read.
    """
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      cached_data = self.redis.get(cache_key)

      if cached_data:
        data = json.loads(str(cached_data))
        current_balance = Decimal(data["balance"])
        new_balance = current_balance - credits_consumed

        # Update the cache with new balance
        data["balance"] = str(new_balance)
        data["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Get remaining TTL and preserve it
        ttl = cast(int, self.redis.ttl(cache_key))
        if ttl > 0:
          self.redis.setex(cache_key, ttl, json.dumps(data))
          logger.debug(f"Updated cached balance for {graph_id}: {new_balance}")

    except Exception as e:
      # If update fails, invalidate the cache to force DB read
      logger.error(f"Failed to update cached balance, invalidating: {e}")
      self.invalidate_graph_credit_balance(graph_id)

  def cache_credit_summary(self, graph_id: str, summary: Dict[str, Any]) -> None:
    """Cache comprehensive credit summary for a graph."""
    try:
      cache_key = self._get_credit_summary_key(graph_id)
      # Convert Decimal values to strings for JSON serialization
      summary_copy = summary.copy()
      for key in [
        "current_balance",
        "monthly_allocation",
        "consumed_this_month",
      ]:
        if key in summary_copy:
          summary_copy[key] = str(summary_copy[key])

      self.redis.setex(cache_key, self.summary_ttl, json.dumps(summary_copy))
      logger.debug(f"Cached credit summary for graph {graph_id}")

    except Exception as e:
      logger.error(f"Failed to cache credit summary: {e}")

  def get_cached_credit_summary(self, graph_id: str) -> Optional[Dict[str, Any]]:
    """Get cached credit summary for a graph."""
    try:
      cache_key = self._get_credit_summary_key(graph_id)
      cached_data = self.redis.get(cache_key)

      if cached_data:
        summary = json.loads(str(cached_data))
        # Convert string values back to floats for API response
        for key in [
          "current_balance",
          "monthly_allocation",
          "consumed_this_month",
        ]:
          if key in summary:
            summary[key] = float(summary[key])

        logger.debug(f"Credit summary cache hit for graph {graph_id}")
        return summary

      logger.debug(f"Credit summary cache miss for graph {graph_id}")
      return None

    except Exception as e:
      logger.error(f"Failed to get cached credit summary: {e}")
      return None

  def cache_operation_cost(self, operation_type: str, cost: Decimal) -> None:
    """Cache operation cost for quick lookup."""
    try:
      cache_key = self._get_operation_cost_key(operation_type)
      self.redis.setex(cache_key, self.operation_cost_ttl, str(cost))
      logger.debug(f"Cached operation cost for {operation_type}: {cost}")

    except Exception as e:
      logger.error(f"Failed to cache operation cost: {e}")

  def get_cached_operation_cost(self, operation_type: str) -> Optional[Decimal]:
    """Get cached operation cost."""
    try:
      cache_key = self._get_operation_cost_key(operation_type)
      cached_cost = self.redis.get(cache_key)

      if cached_cost:
        logger.debug(f"Operation cost cache hit for {operation_type}")
        return Decimal(str(cached_cost))

      logger.debug(f"Operation cost cache miss for {operation_type}")
      return None

    except Exception as e:
      logger.error(f"Failed to get cached operation cost: {e}")
      return None

  def invalidate_graph_credit_balance(self, graph_id: str) -> None:
    """Invalidate cached credit balance for a graph."""
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      deleted = cast(int, self.redis.delete(cache_key))

      # Also invalidate summary since it contains balance
      summary_key = self._get_credit_summary_key(graph_id)
      deleted += cast(int, self.redis.delete(summary_key))

      if deleted:
        logger.info(f"Invalidated credit cache for graph {graph_id}")

    except Exception as e:
      logger.error(f"Failed to invalidate credit cache: {e}")

  def invalidate_all_graph_credits(self) -> None:
    """Invalidate all cached graph credit data (for monthly allocation)."""
    try:
      # Delete all graph credit balance keys
      balance_pattern = f"{self.GRAPH_CREDIT_PREFIX}*"
      balance_keys = cast(list, self.redis.keys(balance_pattern)) or []

      # Delete all credit summary keys
      summary_pattern = f"{self.CREDIT_SUMMARY_PREFIX}*"
      summary_keys = cast(list, self.redis.keys(summary_pattern)) or []

      all_keys = balance_keys + summary_keys
      if all_keys:
        deleted = self.redis.delete(*all_keys)
        logger.info(f"Invalidated {deleted} credit cache entries")

    except Exception as e:
      logger.error(f"Failed to invalidate all credit caches: {e}")

  def warmup_operation_costs(self, costs: Dict[str, Decimal]) -> None:
    """Pre-populate operation cost cache."""
    try:
      for operation_type, cost in costs.items():
        self.cache_operation_cost(operation_type, cost)
      logger.info(f"Warmed up {len(costs)} operation cost cache entries")

    except Exception as e:
      logger.error(f"Failed to warmup operation costs: {e}")

  def get_cache_stats(self) -> Dict[str, Any]:
    """Get credit cache statistics."""
    try:
      info = self.redis.info()

      # Count different types of cached entries
      graph_balance_keys = (
        cast(list, self.redis.keys(f"{self.GRAPH_CREDIT_PREFIX}*")) or []
      )
      shared_balance_keys = (
        cast(list, self.redis.keys(f"{self.SHARED_CREDIT_PREFIX}*")) or []
      )
      summary_keys = cast(list, self.redis.keys(f"{self.CREDIT_SUMMARY_PREFIX}*")) or []
      operation_cost_keys = (
        cast(list, self.redis.keys(f"{self.OPERATION_COST_PREFIX}*")) or []
      )
      graph_balance_count = len(graph_balance_keys)
      shared_balance_count = len(shared_balance_keys)
      summary_count = len(summary_keys)
      operation_cost_count = len(operation_cost_keys)

      return {
        "connected": True,
        "redis_info": {
          "used_memory_human": cast(dict, info or {}).get("used_memory_human"),
          "connected_clients": cast(dict, info or {}).get("connected_clients"),
          "keyspace_hits": cast(dict, info or {}).get("keyspace_hits"),
          "keyspace_misses": cast(dict, info or {}).get("keyspace_misses"),
        },
        "cache_counts": {
          "graph_balances": graph_balance_count,
          "shared_balances": shared_balance_count,
          "summaries": summary_count,
          "operation_costs": operation_cost_count,
          "total": graph_balance_count
          + shared_balance_count
          + summary_count
          + operation_cost_count,
        },
        "ttl_config": {
          "balance_ttl": self.balance_ttl,
          "summary_ttl": self.summary_ttl,
          "operation_cost_ttl": self.operation_cost_ttl,
        },
      }
    except Exception as e:
      logger.error(f"Failed to get credit cache stats: {e}")
      return {"connected": False, "error": str(e)}


# Global credit cache instance
credit_cache = CreditCache()
