"""Subscription-based rate limiting configuration and utilities."""

from ...config.rate_limits import EndpointCategory, RateLimitConfig

SUBSCRIPTION_RATE_LIMITS = RateLimitConfig.SUBSCRIPTION_RATE_LIMITS


def get_subscription_rate_limit(
  tier: str, category: EndpointCategory
) -> tuple[int, int] | None:
  return RateLimitConfig.get_rate_limit(tier, category)


def get_endpoint_category(path: str, method: str = "GET") -> EndpointCategory | None:
  return RateLimitConfig.get_endpoint_category(path, method)


def should_use_subscription_limits(path: str) -> bool:
  """Whether an endpoint uses subscription-tier rate limits."""
  # Extensions reads and writes are tenant-scoped.
  if path.startswith("/extensions/"):
    return True

  # The graph-agnostic MCP transports carry no graph in their path; they are
  # tenant-scoped through the OAuth grant and take the MCP buckets.
  if path in ("/v1/mcp", "/v1/mcp/roboledger"):
    return True

  if path.startswith("/v1/") and len(path.split("/")) >= 4:
    path_parts = path[4:].split("/")
    if path_parts[0] and path_parts[0] not in [
      "auth",
      "user",
      "status",
      "health",
      "create",
    ]:
      return True

  return bool(
    any(
      path.startswith(prefix)
      for prefix in ["/v1/user/subscription", "/v1/user/limits", "/v1/operations"]
    )
  )
