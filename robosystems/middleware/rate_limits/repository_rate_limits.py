"""
Repository-specific rate limiting for shared repositories like SEC.

This module implements the second layer of rate limiting specifically for
shared repositories, working in conjunction with the existing burst protection.

IMPORTANT: Both direct API queries and MCP queries are FREE.
Rate limits are applied to prevent abuse and ensure fair usage across tiers.
No credits are consumed for any query operations.
"""

from enum import Enum
from typing import Dict, Optional
from datetime import datetime, timezone
import redis.asyncio as redis

from robosystems.config.rate_limits import RateLimitConfig, EndpointCategory
from robosystems.config.billing.repositories import RepositoryPlan
from robosystems.config import RepositoryBillingConfig, SharedRepository


class AllowedSharedEndpoints(str, Enum):
  """Endpoints allowed for shared repositories."""

  QUERY = "query"  # Direct Cypher queries
  MCP = "mcp"  # MCP tool access
  AGENT = "agent"  # AI agent operations
  SCHEMA = "schema"  # Schema inspection
  STATUS = "status"  # Status checks


# Endpoints that are BLOCKED for shared repositories
BLOCKED_SHARED_ENDPOINTS = [
  "backup",  # No backups of shared data
  "restore",  # No restore operations
  "delete",  # No deletion
  "admin",  # No admin operations
  "sync",  # No sync operations
  "import",  # No imports to shared repos
  "connections",  # No connection management
  "settings",  # No settings changes
]


class SharedRepositoryRateLimits:
  """
  Rate limits specific to shared repositories by subscription tier.

  Uses RepositoryBillingConfig as the single source of truth for rate limits.
  NO FREE TIER - all access requires a paid subscription.
  """

  @classmethod
  def get_repository_limits(cls) -> Dict:
    """Get rate limits from the centralized billing config."""
    return RepositoryBillingConfig.RATE_LIMITS

  @classmethod
  def get_limits(cls, repository: str, plan: RepositoryPlan) -> Dict:
    """Get rate limits for a repository and plan."""
    # Convert string repository to SharedRepository enum if needed
    if isinstance(repository, str):
      try:
        repository = SharedRepository(repository)
      except ValueError:
        return {}  # Unknown repository

    # Get limits from centralized config
    return RepositoryBillingConfig.get_rate_limits(repository, plan) or {}

  @classmethod
  def is_endpoint_allowed(cls, repository: str, endpoint: str) -> bool:
    """Check if an endpoint is allowed for a shared repository."""
    # Use centralized config for endpoint validation
    return RepositoryBillingConfig.is_endpoint_allowed(endpoint)


class DualLayerRateLimiter:
  """
  Implements two-layer rate limiting:
  1. Burst protection (existing) - prevents API abuse
  2. Repository limits (new) - subscription-based volume control
  """

  def __init__(self, redis_client: redis.Redis):
    self.redis = redis_client

  async def check_limits(
    self,
    user_id: str,
    graph_id: str,
    operation: str,
    endpoint: str,
    user_tier: str,
    repository_plan: Optional[RepositoryPlan] = None,
  ) -> Dict:
    """
    Check both burst and repository-specific limits.

    Args:
        user_id: User making the request
        graph_id: Graph ID (could be "sec" for shared repo)
        operation: Operation type (query, mcp, agent, export)
        endpoint: The actual endpoint being called
        user_tier: User's subscription tier (for burst limits)
        repository_plan: Repository subscription plan (for volume limits)

    Returns:
        Dict with allowed status and details
    """

    # Check if this is a shared repository
    if self._is_shared_repository(graph_id):
      # First check if the endpoint is even allowed
      if not SharedRepositoryRateLimits.is_endpoint_allowed(graph_id, endpoint):
        return {
          "allowed": False,
          "reason": "endpoint_not_allowed",
          "message": f"Endpoint '{endpoint}' is not allowed for shared repository '{graph_id}'",
          "allowed_endpoints": list(AllowedSharedEndpoints),
        }

      # Check if user has access (subscription required - must have valid plan)
      if not repository_plan:
        return {
          "allowed": False,
          "reason": "no_access",
          "message": f"Access to {graph_id} repository requires a paid subscription",
          "upgrade_url": "/upgrade",
        }

    # Layer 1: Check burst protection (existing system)
    burst_check = await self._check_burst_limit(user_id, operation, user_tier)
    if not burst_check["allowed"]:
      return {
        "allowed": False,
        "reason": "burst_limit",
        "detail": burst_check,
        "message": "Rate limit exceeded (burst protection)",
      }

    # Layer 2: Check repository-specific limits (if applicable)
    repo_check = None
    if self._is_shared_repository(graph_id) and repository_plan:
      repo_check = await self._check_repository_limit(
        user_id, graph_id, operation, repository_plan
      )
      if not repo_check["allowed"]:
        return {
          "allowed": False,
          "reason": "repository_limit",
          "detail": repo_check,
          "message": f"Repository {operation} limit exceeded for {repository_plan.value} plan",
        }

    return {
      "allowed": True,
      "burst": burst_check,
      "repo": repo_check if self._is_shared_repository(graph_id) else None,
    }

  async def _check_burst_limit(self, user_id: str, operation: str, tier: str) -> Dict:
    """Check existing burst protection limits."""
    category = self._operation_to_category(operation)
    limit_config = RateLimitConfig.get_rate_limit(tier, category)

    if not limit_config:
      return {"allowed": True}

    limit, window = limit_config

    # Use sliding window counter
    now = int(datetime.now(timezone.utc).timestamp())
    window_start = now - window
    key = f"burst:{user_id}:{operation}"

    # Remove old entries and count current window
    pipe = self.redis.pipeline()
    pipe.zremrangebyscore(key, 0, window_start)
    pipe.zadd(key, {str(now): now})
    pipe.zcount(key, window_start, now)
    pipe.expire(key, window)
    results = await pipe.execute()

    count = results[2]

    return {
      "allowed": count <= limit,
      "limit": limit,
      "current": count,
      "window": window,
      "reset_at": now + window,
    }

  async def _check_repository_limit(
    self, user_id: str, repository: str, operation: str, plan: RepositoryPlan
  ) -> Dict:
    """Check repository-specific volume limits."""
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)

    if not limits:
      return {"allowed": False, "message": "No access to repository"}

    # Map operation to limit keys
    operation_keys = {"query": "queries", "mcp": "mcp_queries", "agent": "agent_calls"}

    base_key = operation_keys.get(operation, "queries")

    # Check different time windows
    checks = []
    now = datetime.now(timezone.utc)

    # Check minute limit
    minute_limit_key = f"{base_key}_per_minute"
    if minute_limit_key in limits:
      limit = limits[minute_limit_key]
      if limit != -1:  # -1 means unlimited
        key = (
          f"repo:{repository}:{user_id}:{operation}:min:{now.strftime('%Y%m%d%H%M')}"
        )
        count = await self.redis.incr(key)
        if count == 1:
          await self.redis.expire(key, 60)

        if count > limit:
          return {
            "allowed": False,
            "window": "minute",
            "limit": limit,
            "current": count,
            "reset_in": 60,
          }
        checks.append({"window": "minute", "limit": limit, "current": count})

    # Check hour limit
    hour_limit_key = f"{base_key}_per_hour"
    if hour_limit_key in limits:
      limit = limits[hour_limit_key]
      if limit != -1:
        key = f"repo:{repository}:{user_id}:{operation}:hour:{now.strftime('%Y%m%d%H')}"
        count = await self.redis.incr(key)
        if count == 1:
          await self.redis.expire(key, 3600)

        if count > limit:
          return {
            "allowed": False,
            "window": "hour",
            "limit": limit,
            "current": count,
            "reset_in": 3600,
          }
        checks.append({"window": "hour", "limit": limit, "current": count})

    # Check day limit
    day_limit_key = f"{base_key}_per_day"
    if day_limit_key in limits:
      limit = limits[day_limit_key]
      if limit != -1:
        key = f"repo:{repository}:{user_id}:{operation}:day:{now.strftime('%Y%m%d')}"
        count = await self.redis.incr(key)
        if count == 1:
          await self.redis.expire(key, 86400)

        if count > limit:
          return {
            "allowed": False,
            "window": "day",
            "limit": limit,
            "current": count,
            "reset_in": 86400,
          }
        checks.append({"window": "day", "limit": limit, "current": count})

    return {"allowed": True, "checks": checks}

  def _is_shared_repository(self, graph_id: str) -> bool:
    """Check if this is a shared repository."""
    return graph_id in [repo.value for repo in SharedRepository]

  def _operation_to_category(self, operation: str) -> EndpointCategory:
    """Map operation to endpoint category for burst limits."""
    mapping = {
      "query": EndpointCategory.GRAPH_QUERY,
      "mcp": EndpointCategory.GRAPH_MCP,
      "agent": EndpointCategory.GRAPH_AGENT,
    }
    return mapping.get(operation, EndpointCategory.GRAPH_READ)

  async def get_usage_stats(
    self, user_id: str, repository: str, plan: RepositoryPlan
  ) -> Dict:
    """Get current usage statistics for a user."""
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)
    if not limits:
      return {}

    now = datetime.now(timezone.utc)
    stats = {}

    # Get current usage for each operation type
    for operation in ["query", "mcp", "agent"]:
      operation_stats = {}

      # Check each time window
      for window, fmt in [
        ("minute", "%Y%m%d%H%M"),
        ("hour", "%Y%m%d%H"),
        ("day", "%Y%m%d"),
      ]:
        key = (
          f"repo:{repository}:{user_id}:{operation}:{window[:3]}:{now.strftime(fmt)}"
        )
        count = await self.redis.get(key)
        operation_stats[window] = int(count) if count else 0

      stats[operation] = operation_stats

    return {"usage": stats, "limits": limits, "plan": plan.value}
