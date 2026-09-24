"""Volume limits for shared repositories such as SEC.

Enforces the subscription plan's per-plan volume caps from the adapter
manifest. Per-request *burst* protection is a separate, upstream layer
(``subscription_aware_rate_limit_dependency``).

Both direct API queries and MCP queries count against these caps. Queries
consume no credits — this is the only thing bounding their use.
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
  QUERY = "query"
  MCP = "mcp"
  AGENT = "agent"
  SEARCH = "search"
  SCHEMA = "schema"
  STATUS = "status"


BLOCKED_SHARED_ENDPOINTS = [
  "backup",
  "restore",
  "delete",
  "admin",
  "sync",
  "import",
  "connections",
  "settings",
]


class SharedRepositoryRateLimits:
  """Per-plan limits from the repository manifests. There is no free tier."""

  @classmethod
  def get_limits(cls, repository: str, plan: str) -> dict:
    return _get_rate_limits(repository, plan) or {}

  @classmethod
  def is_endpoint_allowed(cls, repository: str, endpoint: str) -> bool:
    """Check an endpoint against the repository's own allowed/blocked lists."""
    return _is_endpoint_allowed(endpoint, repo_id=repository)


class DualLayerRateLimiter:
  """Enforce shared-repository subscription-plan *volume* limits.

  Burst protection is upstream (``subscription_aware_rate_limit_dependency``);
  despite the name, this is a single layer.
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
    from robosystems.config import env

    if not is_shared_repository_or_subgraph(graph_id):
      return {"allowed": True, "repo": None}

    parent_repo_id = resolve_shared_repository_parent(graph_id)

    if not SharedRepositoryRateLimits.is_endpoint_allowed(parent_repo_id, endpoint):
      return {
        "allowed": False,
        "reason": "endpoint_not_allowed",
        "message": f"Endpoint '{endpoint}' is not allowed for shared repository '{graph_id}'",
        "allowed_endpoints": list(AllowedSharedEndpoints),
      }

    if not repository_plan:
      return {
        "allowed": False,
        "reason": "no_access",
        "message": f"Access to {graph_id} repository requires a paid subscription",
        "upgrade_url": f"{env.ROBOSYSTEMS_URL}/repositories/browse",
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
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)

    if not limits:
      return {"allowed": False, "message": "No access to repository"}

    operation_keys = {
      "query": "queries",
      "mcp": "mcp_queries",
      "agent": "agent_calls",
      "search": "searches",
    }

    base_key = operation_keys.get(operation, "queries")

    checks = []
    now = datetime.now(UTC)

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
    limits = SharedRepositoryRateLimits.get_limits(repository, plan)
    if not limits:
      return {}

    now = datetime.now(UTC)
    stats = {}

    for operation in ["query", "mcp", "agent", "search"]:
      operation_stats = {}

      # Tokens must match the keys _check_repository_limit writes.
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
