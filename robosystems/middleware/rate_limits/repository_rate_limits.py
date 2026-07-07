"""
Repository-specific rate limiting for shared repositories like SEC.

This module implements the subscription-plan *volume* limits for shared
repositories. Per-request *burst* protection is handled upstream by the
per-tier FastAPI dependency (``subscription_aware_rate_limit_dependency``),
so this layer only enforces the manifest's per-plan volume caps.

IMPORTANT: Both direct API queries and MCP queries are included.
Rate limits are applied to prevent abuse and ensure fair usage across tiers.
No credits are consumed for any query operations.
"""

from datetime import UTC, datetime
from enum import Enum

import redis.asyncio as redis

from robosystems.config.shared_repositories import (
  get_rate_limits as _get_rate_limits,
)
from robosystems.config.shared_repositories import (
  is_endpoint_allowed as _is_endpoint_allowed,
)
from robosystems.config.shared_repositories import (
  is_shared_repository_or_subgraph,
  resolve_shared_repository_parent,
)


class AllowedSharedEndpoints(str, Enum):
  """Endpoints allowed for shared repositories."""

  QUERY = "query"  # Direct Cypher queries
  MCP = "mcp"  # MCP tool access
  AGENT = "agent"  # AI agent operations
  SEARCH = "search"  # Full-text search (OpenSearch)
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

  Uses the shared repository registry as the accessor for rate limits from manifests.
  NO FREE TIER - all access requires a paid subscription.
  """

  @classmethod
  def get_limits(cls, repository: str, plan: str) -> dict:
    """Get rate limits for a repository and plan."""
    return _get_rate_limits(repository, plan) or {}

  @classmethod
  def is_endpoint_allowed(cls, repository: str, endpoint: str) -> bool:
    """Check if an endpoint is allowed for a shared repository."""
    return _is_endpoint_allowed(endpoint)


class DualLayerRateLimiter:
  """
  Enforce shared-repository subscription-plan *volume* limits.

  Per-request burst protection is applied upstream by the per-tier FastAPI
  dependency (``subscription_aware_rate_limit_dependency``); this class only
  layers the manifest's per-plan volume caps on top for shared repos. (The
  name is historical — there is now a single layer here.)
  """

  def __init__(self, redis_client: redis.Redis):
    self.redis = redis_client

  async def check_limits(
    self,
    user_id: str,
    graph_id: str,
    operation: str,
    endpoint: str,
    repository_plan: str | None = None,
  ) -> dict:
    """
    Check shared-repository per-plan volume limits.

    Args:
        user_id: User making the request
        graph_id: Graph ID (could be "sec"/"sec_historical" for a shared repo)
        operation: Operation type (query, mcp, search)
        endpoint: The actual endpoint being called
        repository_plan: Repository subscription plan (for volume limits)

    Returns:
        Dict with allowed status and details
    """
    from robosystems.config import env

    # Only shared repositories (including subgraphs like sec_historical) are
    # gated here; other graphs rely on the upstream burst dependency alone.
    if not is_shared_repository_or_subgraph(graph_id):
      return {"allowed": True, "repo": None}

    # Resolve subgraph to parent for policy + subscription lookups
    parent_repo_id = resolve_shared_repository_parent(graph_id)

    # The endpoint must be allowed for shared repositories
    if not SharedRepositoryRateLimits.is_endpoint_allowed(parent_repo_id, endpoint):
      return {
        "allowed": False,
        "reason": "endpoint_not_allowed",
        "message": f"Endpoint '{endpoint}' is not allowed for shared repository '{graph_id}'",
        "allowed_endpoints": list(AllowedSharedEndpoints),
      }

    # Access requires a valid (paid) subscription plan
    if not repository_plan:
      return {
        "allowed": False,
        "reason": "no_access",
        "message": f"Access to {graph_id} repository requires a paid subscription",
        "upgrade_url": f"{env.ROBOSYSTEMS_URL}/billing",
      }

    repo_check = await self._check_repository_limit(
      user_id, parent_repo_id, operation, repository_plan
    )
    if not repo_check["allowed"]:
      return {
        "allowed": False,
        "reason": "repository_limit",
        "detail": repo_check,
        "message": f"Repository {operation} limit exceeded for {repository_plan} plan",
      }

    return {"allowed": True, "repo": repo_check}

  async def _check_repository_limit(
    self, user_id: str, repository: str, operation: str, plan: str
  ) -> dict:
    """Check repository-specific volume limits."""
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)

    if not limits:
      return {"allowed": False, "message": "No access to repository"}

    # Map operation to limit keys
    operation_keys = {
      "query": "queries",
      "mcp": "mcp_queries",
      "agent": "agent_calls",
      "search": "searches",
    }

    base_key = operation_keys.get(operation, "queries")

    # Check different time windows
    checks = []
    now = datetime.now(UTC)

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

  async def get_usage_stats(self, user_id: str, repository: str, plan: str) -> dict:
    """Get current usage statistics for a user."""
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)
    if not limits:
      return {}

    now = datetime.now(UTC)
    stats = {}

    # Get current usage for each operation type
    for operation in ["query", "mcp", "agent", "search"]:
      operation_stats = {}

      # Check each time window. The token must match exactly what
      # _check_repository_limit writes ("min"/"hour"/"day") — using
      # window[:3] produced "hou" for the hour bucket and silently read 0.
      for window, token, fmt in [
        ("minute", "min", "%Y%m%d%H%M"),
        ("hour", "hour", "%Y%m%d%H"),
        ("day", "day", "%Y%m%d"),
      ]:
        key = f"repo:{repository}:{user_id}:{operation}:{token}:{now.strftime(fmt)}"
        count = await self.redis.get(key)
        operation_stats[window] = int(count) if count else 0

      stats[operation] = operation_stats

    return {"usage": stats, "limits": limits, "plan": plan}
