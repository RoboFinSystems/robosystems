"""Credit system caching using in-memory TTL cache."""

import fnmatch
import json
import threading
import time
from decimal import Decimal
from typing import Any, cast

from ...config.defaults import CacheDefaults
from ...config.tuning import TuningConfig
from ...logger import logger


class CreditCache:
  """Manages credit balance and transaction caching in-memory with TTL."""

  # Cache configuration
  CACHE_KEY_PREFIX = "credits:"
  GRAPH_CREDIT_PREFIX = "graph_credit:"
  SHARED_CREDIT_PREFIX = "shared_credit:"
  CREDIT_SUMMARY_PREFIX = "credit_summary:"
  OPERATION_COST_PREFIX = "op_cost:"

  def __init__(self):
    """Initialize in-memory cache for credit caching."""
    # Internal store: key -> (json_value, expires_at)
    self._store: dict[str, tuple[str, float]] = {}
    self._lock = threading.Lock()

    # TTL configuration - using TuningConfig for runtime tunability via SSM
    self.balance_ttl = TuningConfig.get_cache_balance_ttl()
    self.summary_ttl = TuningConfig.get_cache_summary_ttl()
    self.operation_cost_ttl = (
      CacheDefaults.OPERATION_COST_TTL
    )  # Not tunable (rarely changes)

  def _get(self, key: str) -> str | None:
    """Get a value from cache, returning None if expired or missing."""
    with self._lock:
      entry = self._store.get(key)
      if entry is None:
        return None
      value, expires_at = entry
      if time.time() > expires_at:
        del self._store[key]
        return None
      return value

  def _setex(self, key: str, ttl: int, value: str) -> None:
    """Set a value with TTL (seconds)."""
    with self._lock:
      self._store[key] = (value, time.time() + ttl)

  def _delete(self, *keys: str) -> int:
    """Delete keys, returning count of keys actually removed."""
    deleted = 0
    with self._lock:
      for key in keys:
        if key in self._store:
          del self._store[key]
          deleted += 1
    return deleted

  def _keys(self, pattern: str) -> list[str]:
    """Return keys matching a glob pattern."""
    now = time.time()
    with self._lock:
      return [
        k
        for k, (_, expires_at) in self._store.items()
        if expires_at > now and fnmatch.fnmatch(k, pattern)
      ]

  def _ttl(self, key: str) -> int:
    """Return remaining TTL in seconds, or -2 if key doesn't exist."""
    with self._lock:
      entry = self._store.get(key)
      if entry is None:
        return -2
      _, expires_at = entry
      remaining = int(expires_at - time.time())
      if remaining <= 0:
        del self._store[key]
        return -2
      return remaining

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
        graph_tier: Graph tier (ladybug-standard/ladybug-large/ladybug-xlarge)
    """
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      from datetime import UTC, datetime

      cache_data = {
        "balance": str(balance),
        "graph_tier": graph_tier,
        "cached_at": datetime.now(UTC).isoformat(),
      }

      self._setex(cache_key, self.balance_ttl, json.dumps(cache_data))
      logger.debug(f"Cached graph credit balance for {graph_id}: {balance}")

    except Exception as e:
      logger.error(f"Failed to cache graph credit balance: {e}")

  def get_cached_graph_credit_balance(
    self, graph_id: str
  ) -> tuple[Decimal, str] | None:
    """
    Get cached graph credit balance.

    Returns:
        Tuple of (balance, graph_tier) or None if not cached
    """
    try:
      cache_key = self._get_graph_credit_key(graph_id)
      cached_data = self._get(cache_key)

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
      cached_data = self._get(cache_key)

      if cached_data:
        data = json.loads(str(cached_data))
        current_balance = Decimal(data["balance"])
        new_balance = current_balance - credits_consumed

        # Update the cache with new balance
        from datetime import UTC, datetime

        data["balance"] = str(new_balance)
        data["updated_at"] = datetime.now(UTC).isoformat()

        # Get remaining TTL and preserve it
        ttl = cast(int, self._ttl(cache_key))
        if ttl > 0:
          self._setex(cache_key, ttl, json.dumps(data))
          logger.debug(f"Updated cached balance for {graph_id}: {new_balance}")

    except Exception as e:
      # If update fails, invalidate the cache to force DB read
      logger.error(f"Failed to update cached balance, invalidating: {e}")
      self.invalidate_graph_credit_balance(graph_id)

  def cache_credit_summary(self, graph_id: str, summary: dict[str, Any]) -> None:
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

      self._setex(cache_key, self.summary_ttl, json.dumps(summary_copy))
      logger.debug(f"Cached credit summary for graph {graph_id}")

    except Exception as e:
      logger.error(f"Failed to cache credit summary: {e}")

  def get_cached_credit_summary(self, graph_id: str) -> dict[str, Any] | None:
    """Get cached credit summary for a graph."""
    try:
      cache_key = self._get_credit_summary_key(graph_id)
      cached_data = self._get(cache_key)

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
      self._setex(cache_key, self.operation_cost_ttl, str(cost))
      logger.debug(f"Cached operation cost for {operation_type}: {cost}")

    except Exception as e:
      logger.error(f"Failed to cache operation cost: {e}")

  def get_cached_operation_cost(self, operation_type: str) -> Decimal | None:
    """Get cached operation cost."""
    try:
      cache_key = self._get_operation_cost_key(operation_type)
      cached_cost = self._get(cache_key)

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
      deleted = self._delete(cache_key)

      # Also invalidate summary since it contains balance
      summary_key = self._get_credit_summary_key(graph_id)
      deleted += self._delete(summary_key)

      if deleted:
        logger.info(f"Invalidated credit cache for graph {graph_id}")

    except Exception as e:
      logger.error(f"Failed to invalidate credit cache: {e}")

  def invalidate_all_graph_credits(self) -> None:
    """Invalidate all cached graph credit data (for monthly allocation)."""
    try:
      # Delete all graph credit balance keys
      balance_keys = self._keys(f"{self.GRAPH_CREDIT_PREFIX}*")

      # Delete all credit summary keys
      summary_keys = self._keys(f"{self.CREDIT_SUMMARY_PREFIX}*")

      all_keys = balance_keys + summary_keys
      if all_keys:
        deleted = self._delete(*all_keys)
        logger.info(f"Invalidated {deleted} credit cache entries")

    except Exception as e:
      logger.error(f"Failed to invalidate all credit caches: {e}")

  def warmup_operation_costs(self, costs: dict[str, Decimal]) -> None:
    """Pre-populate operation cost cache."""
    try:
      for operation_type, cost in costs.items():
        self.cache_operation_cost(operation_type, cost)
      logger.info(f"Warmed up {len(costs)} operation cost cache entries")

    except Exception as e:
      logger.error(f"Failed to warmup operation costs: {e}")

  def get_cache_stats(self) -> dict[str, Any]:
    """Get credit cache statistics."""
    try:
      # Count different types of cached entries
      graph_balance_keys = self._keys(f"{self.GRAPH_CREDIT_PREFIX}*")
      shared_balance_keys = self._keys(f"{self.SHARED_CREDIT_PREFIX}*")
      summary_keys = self._keys(f"{self.CREDIT_SUMMARY_PREFIX}*")
      operation_cost_keys = self._keys(f"{self.OPERATION_COST_PREFIX}*")
      graph_balance_count = len(graph_balance_keys)
      shared_balance_count = len(shared_balance_keys)
      summary_count = len(summary_keys)
      operation_cost_count = len(operation_cost_keys)

      return {
        "connected": True,
        "cache_type": "in-memory",
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
