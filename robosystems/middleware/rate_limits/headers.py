"""Copies rate-limit state from ``request.state`` onto response headers."""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class RateLimitHeaderMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next):
    response = await call_next(request)

    if hasattr(request.state, "rate_limit_remaining"):
      response.headers["X-RateLimit-Remaining"] = str(
        request.state.rate_limit_remaining
      )

    if hasattr(request.state, "rate_limit_limit"):
      response.headers["X-RateLimit-Limit"] = str(request.state.rate_limit_limit)

    if hasattr(request.state, "rate_limit_tier"):
      response.headers["X-RateLimit-Tier"] = request.state.rate_limit_tier

    if hasattr(request.state, "rate_limit_category"):
      response.headers["X-RateLimit-Category"] = request.state.rate_limit_category

    if hasattr(request.state, "auth_rate_limit_remaining"):
      response.headers["X-Auth-RateLimit-Remaining"] = str(
        request.state.auth_rate_limit_remaining
      )

    if hasattr(request.state, "auth_rate_limit_limit"):
      response.headers["X-Auth-RateLimit-Limit"] = str(
        request.state.auth_rate_limit_limit
      )

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
