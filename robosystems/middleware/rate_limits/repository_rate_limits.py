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


def start_plan_url() -> str:
  """Where a trial subscriber starts the plan without waiting out the trial."""
  from robosystems.config import env

  return f"{env.ROBOSYSTEMS_URL}/organization?tab=subscriptions"


def subscribe_url() -> str:
  """Where someone with no subscription picks a plan."""
  from robosystems.config import env

  return f"{env.ROBOSYSTEMS_URL}/repositories/browse"


def add_card_url() -> str:
  """Where an organization owner adds a payment method."""
  from robosystems.config import env

  return f"{env.ROBOSYSTEMS_URL}/organization?tab=billing"


def trial_end_date(ends_at: datetime) -> str:
  return f"{ends_at:%B} {ends_at.day}, {ends_at.year}"


def no_card_way_on(ends_at: datetime) -> str:
  """A trial taken without a card does not become the plan by itself."""
  return (
    f"No card is on file: add one at {add_card_url()} to keep the plan when the "
    f"trial ends, or to start it now. Without a card the trial ends on "
    f"{trial_end_date(ends_at)} and nothing is charged."
  )


def _read(reader, repository: str, user_id: str):
  """Run a billing read on the refusal path; None when it fails."""
  from robosystems.database import SessionFactory

  session = SessionFactory()
  try:
    return reader(repository, user_id, session)
  except Exception:
    return None
  finally:
    session.close()


def _trial_without_card_ends_at(repository: str, user_id: str) -> datetime | None:
  from robosystems.operations.billing.repository_subscriptions import (
    trial_without_card_ends_at,
  )

  return _read(trial_without_card_ends_at, repository, user_id)


def trial_refusal(
  repository: str, operation: str = "query", *, user_id: str | None = None
) -> str:
  """What a trial subscriber is told when they reach past the trial: raw
  queries, or the AI analyst (``operation="agent"``), which runs on credits."""
  what = "The AI analyst is" if operation == "agent" else "Raw queries are"
  opens = "It opens" if operation == "agent" else "They open"
  lead = (
    f"{what} not part of the {repository.upper()} trial, which covers the "
    f"analysis tools. {opens} with your first payment"
  )
  ends_at = _trial_without_card_ends_at(repository, user_id) if user_id else None
  if ends_at is not None:
    return f"{lead}. {no_card_way_on(ends_at)}"
  return (
    f"{lead}, and you do not have to wait for the trial to end: start your plan "
    f"now at {start_plan_url()}"
  )


def trial_ended_refusal(graph_id: str, user_id: str) -> str | None:
  """The refusal for a trial that ended without becoming the plan, or None
  when that is not why the caller has no access. Read on the refusal path."""
  from robosystems.operations.billing.repository_subscriptions import trial_ended_at

  try:
    repository = resolve_shared_repository_parent(graph_id)
  except ValueError:
    return None
  ended = _read(trial_ended_at, repository, user_id)
  if ended is None:
    return None
  return (
    f"Your {repository.upper()} trial ended on {trial_end_date(ended)}. "
    f"Subscribe at {subscribe_url()} to keep using it."
  )


_LIMIT_NOUNS = {
  "mcp": "tool calls",
  "query": "queries",
  "search": "searches",
  "agent": "AI analyst calls",
}
_LIMIT_KEYS = {
  "mcp": "mcp_queries",
  "query": "queries",
  "search": "searches",
  "agent": "agent_calls",
}
_RESETS = {"minute": "within a minute", "hour": "within the hour", "day": "tomorrow"}


def limit_message(
  repository: str,
  operation: str,
  detail: dict,
  plan: str | None,
  *,
  trial: bool = False,
  user_id: str | None = None,
) -> str:
  """What a subscriber is told at a volume limit. On a trial it says the limit
  is the trial's, what the plan's own is, and how to start the plan now.
  ``user_id`` lets a trial with no card be told to add one, and a cancelled
  trial that it runs out."""
  from robosystems.config import env

  window = detail.get("window", "period")
  noun = _LIMIT_NOUNS.get(operation, "requests")
  reached = f"limit of {detail.get('limit', 0)} {noun} per {window}"
  resets = f"It resets {_RESETS.get(window, 'shortly')}."
  plan_key = (plan or "").split("-")[-1]
  plan_name = plan_key.capitalize() or "your"

  if not trial:
    return (
      f"You have reached the {plan_name} plan's {reached}. {resets} "
      f"Plans and their limits: {env.ROBOSYSTEMS_URL}/repositories/browse"
    )

  plan_limit = (_get_rate_limits(repository, plan_key) or {}).get(
    f"{_LIMIT_KEYS.get(operation, 'queries')}_per_{window}"
  )
  if plan_limit == -1:
    plans_own = f" The {plan_name} plan has no {window} limit."
  elif plan_limit:
    plans_own = f" The {plan_name} plan allows {plan_limit} per {window}."
  else:
    plans_own = ""
  lead = (
    f"You have reached the {repository.upper()} trial's {reached}. {resets} "
    f"A trial runs at lower limits than the plan.{plans_own}"
  )
  ends_at = _trial_without_card_ends_at(repository, user_id) if user_id else None
  if ends_at is not None:
    return f"{lead} {no_card_way_on(ends_at)}"
  if user_id and _trial_is_canceled(repository, user_id):
    return f"{lead} Your trial is cancelled and keeps these limits to its last day."
  return (
    f"{lead} You do not have to wait for the trial to end: start your plan "
    f"now at {start_plan_url()}"
  )


def _trial_is_canceled(repository: str, user_id: str) -> bool:
  """Read on the refusal path only, so the cost is one query per 429."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.billing import BillingSubscription

  session = SessionFactory()
  try:
    subscription = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id=repository,
      user_id=user_id,
      session=session,
    )
    return bool(subscription and subscription.status == "canceled")
  except Exception:
    return False
  finally:
    session.close()


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
