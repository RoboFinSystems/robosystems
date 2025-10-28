"""Database session cleanup middleware."""

from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from ..database import session, activate_request_scope, deactivate_request_scope
from ..logger import logger


class DatabaseSessionMiddleware(BaseHTTPMiddleware):
  """Middleware to ensure database sessions are properly cleaned up after requests."""

  async def dispatch(self, request: Request, call_next: Callable) -> Response:
    """
    Process request and ensure session cleanup.

    Args:
        request: The incoming request
        call_next: The next middleware/endpoint to call

    Returns:
        Response: The response from the endpoint
    """
    scope_token = None
    try:
      scope_token = activate_request_scope()
      # Process the request
      response = await call_next(request)
      return response
    finally:
      # Always clean up the session, regardless of success or failure
      try:
        session.remove()
      except Exception as e:
        # Log the exception but don't fail the request
        # The session cleanup failed, but we don't want to crash the response
        logger.warning(f"Database session cleanup failed: {e}")
      finally:
        deactivate_request_scope(scope_token)
