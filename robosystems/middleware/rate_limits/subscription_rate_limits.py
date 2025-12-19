"""Subscription-based rate limiting configuration and utilities."""


from ...config.rate_limits import EndpointCategory, RateLimitConfig

# Re-export subscription rate limits from centralized config
SUBSCRIPTION_RATE_LIMITS = RateLimitConfig.SUBSCRIPTION_RATE_LIMITS


def get_subscription_rate_limit(
  tier: str, category: EndpointCategory
) -> tuple[int, int] | None:
  """
  Get rate limit for a subscription tier and endpoint category.

  Args:
      tier: Subscription tier (ladybug-standard, ladybug-large, ladybug-xlarge)
      category: Endpoint category

  Returns:
      Tuple of (limit, window_seconds) or None if not configured
  """
  return RateLimitConfig.get_rate_limit(tier, category)


def get_endpoint_category(path: str, method: str = "GET") -> EndpointCategory | None:
  """
  Determine the category of an endpoint based on its path and method.

  Args:
      path: The API endpoint path
      method: HTTP method

  Returns:
      The endpoint category or None if not categorized
  """
  return RateLimitConfig.get_endpoint_category(path, method)


def should_use_subscription_limits(path: str) -> bool:
  """
  Determine if an endpoint should use subscription-based rate limits.

  Args:
      path: The API endpoint path

  Returns:
      True if subscription limits should be applied
  """
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
  return bool(any(path.startswith(prefix) for prefix in ["/v1/user/subscription", "/v1/user/limits", "/v1/operations"]))
