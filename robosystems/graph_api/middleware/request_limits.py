"""
Request size limiting middleware for Graph API.

Prevents DoS attacks by limiting request body sizes.
"""

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.config import env
from robosystems.logger import logger


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
  """
  Middleware to limit request body sizes.

  Prevents memory exhaustion attacks by rejecting oversized requests.
  """

  def __init__(
    self,
    app,
    max_body_size: int | None = None,
    max_query_size: int | None = None,
    max_schema_size: int | None = None,
  ):
    super().__init__(app)
    # Default limits (in bytes)
    self.max_body_size = max_body_size or env.GRAPH_MAX_REQUEST_SIZE
    self.max_query_size = (
      max_query_size
      or env.GRAPH_MAX_QUERY_LENGTH * 10  # Convert characters to approximate bytes
    )  # Allow for multi-byte characters
    self.max_schema_size = max_schema_size or 1 * 1024 * 1024  # 1MB for schema DDL

    logger.info(
      f"Request Size Limit Middleware initialized - "
      f"Max body: {self.max_body_size:,} bytes, "
      f"Max query: {self.max_query_size:,} bytes, "
      f"Max schema: {self.max_schema_size:,} bytes"
    )

  async def dispatch(self, request: Request, call_next):
    """Check request size before processing."""
    # Check Content-Length header
    content_length_str = request.headers.get("content-length")
    if content_length_str:
      content_length = int(content_length_str)

      # Determine appropriate limit based on endpoint
      path = request.url.path

      if "/query" in path:
        max_size = self.max_query_size
        limit_type = "query"
      elif "/schema" in path:
        max_size = self.max_schema_size
        limit_type = "schema"
      else:
        max_size = self.max_body_size
        limit_type = "body"

      if content_length > max_size:
        logger.warning(
          f"Request body too large: {content_length:,} bytes "
          f"(max {limit_type}: {max_size:,} bytes) from {request.client.host if request.client else 'unknown'}"
        )
        return JSONResponse(
          status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
          content={
            "detail": f"Request {limit_type} too large. "
            f"Size: {content_length:,} bytes, "
            f"Max allowed: {max_size:,} bytes"
          },
        )

    # For chunked encoding or missing Content-Length, we'd need to
    # implement streaming validation, but for now we'll proceed
    return await call_next(request)
