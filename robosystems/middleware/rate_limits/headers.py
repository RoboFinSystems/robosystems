"""Middleware to add rate limit headers to responses."""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class RateLimitHeaderMiddleware(BaseHTTPMiddleware):
  """Middleware that adds rate limit headers to responses."""

  async def dispatch(self, request: Request, call_next):
    """Add rate limit headers to the response if they exist in request state."""
    response = await call_next(request)

    # Add general rate limit headers
    if hasattr(request.state, "rate_limit_remaining"):
      response.headers["X-RateLimit-Remaining"] = str(
        request.state.rate_limit_remaining
      )

    if hasattr(request.state, "rate_limit_limit"):
      response.headers["X-RateLimit-Limit"] = str(request.state.rate_limit_limit)

    # Add subscription-specific headers
    if hasattr(request.state, "rate_limit_tier"):
      response.headers["X-RateLimit-Tier"] = request.state.rate_limit_tier

    if hasattr(request.state, "rate_limit_category"):
      response.headers["X-RateLimit-Category"] = request.state.rate_limit_category

    # Add auth rate limit headers
    if hasattr(request.state, "auth_rate_limit_remaining"):
      response.headers["X-Auth-RateLimit-Remaining"] = str(
        request.state.auth_rate_limit_remaining
      )

    if hasattr(request.state, "auth_rate_limit_limit"):
      response.headers["X-Auth-RateLimit-Limit"] = str(
        request.state.auth_rate_limit_limit
      )

    # Add MCP/Agent specific headers
    if hasattr(request.state, "mcp_rate_limit_remaining"):
      response.headers["X-MCP-RateLimit-Remaining"] = str(
        request.state.mcp_rate_limit_remaining
      )

    if hasattr(request.state, "mcp_rate_limit_limit"):
      response.headers["X-MCP-RateLimit-Limit"] = str(
        request.state.mcp_rate_limit_limit
      )

    if hasattr(request.state, "agent_rate_limit_remaining"):
      response.headers["X-Agent-RateLimit-Remaining"] = str(
        request.state.agent_rate_limit_remaining
      )

    if hasattr(request.state, "agent_rate_limit_limit"):
      response.headers["X-Agent-RateLimit-Limit"] = str(
        request.state.agent_rate_limit_limit
      )

    return response
