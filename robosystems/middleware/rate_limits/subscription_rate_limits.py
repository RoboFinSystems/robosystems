"""Subscription-based rate limiting configuration and utilities."""

from ...config.rate_limits import EndpointCategory, RateLimitConfig

# Re-export subscription rate limits from centralized config
SUBSCRIPTION_RATE_LIMITS = RateLimitConfig.SUBSCRIPTION_RATE_LIMITS


def get_subscription_rate_limit(
  tier: str, category: EndpointCategory
) -> tuple[int, int] | None:
  """Get rate limit for a subscription tier and endpoint category."""
  return RateLimitConfig.get_rate_limit(tier, category)


def get_endpoint_category(path: str, method: str = "GET") -> EndpointCategory | None:
  """Determine the category of an endpoint based on its path and method."""
  return RateLimitConfig.get_endpoint_category(path, method)


def should_use_subscription_limits(path: str) -> bool:
  """Determine if an endpoint should use subscription-based rate limits."""
  # Extensions surface — both graph-scoped reads (GraphQL) and writes
  # (REST operation endpoints) are tenant-scoped and must use the
  # per-tier buckets. Without this branch the new primary read surface
  # would silently fall back to the generic API limiter and lose its
  # subscription-tier observability.
  if path.startswith("/extensions/"):
    return True

  # The graph-agnostic MCP transport carries no graph in its path; it is
  # tenant-scoped through the OAuth grant and takes the MCP buckets.
  if path == "/v1/mcp":
    return True

  # Always use subscription limits for graph-scoped endpoints
  if path.startswith("/v1/") and len(path.split("/")) >= 4:
    # Check if it's a graph-scoped endpoint (has graph_id)
    path_parts = path[4:].split("/")
    if path_parts[0] and path_parts[0] not in [
      "auth",
      "user",
      "status",
      "health",
      "create",
    ]:
      return True

  # Also use subscription limits for certain non-graph endpoints
  return bool(
    any(
      path.startswith(prefix)
      for prefix in ["/v1/user/subscription", "/v1/user/limits", "/v1/operations"]
    )
  )
