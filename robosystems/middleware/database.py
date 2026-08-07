"""Database session cleanup middleware."""

from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from ..database import activate_request_scope, deactivate_request_scope, session
from ..logger import logger


class DatabaseSessionMiddleware(BaseHTTPMiddleware):
  """Middleware to ensure database sessions are properly cleaned up after requests."""

  async def dispatch(self, request: Request, call_next: Callable) -> Response:
    """Scope the shared session to this request and release it on the way out.

    Cleanup runs whether the endpoint succeeded or raised; a cleanup failure
    is logged rather than raised, so it can't turn a good response into a 500.
    """
    scope_token = None
    try:
      scope_token = activate_request_scope()
      response = await call_next(request)
      return response
    finally:
      try:
        session.remove()
      except Exception as e:
        logger.warning(f"Database session cleanup failed: {e}")
      finally:
        deactivate_request_scope(scope_token)
