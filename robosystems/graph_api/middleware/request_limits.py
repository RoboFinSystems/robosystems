"""
Request size limiting middleware for Graph API.

Prevents DoS attacks by limiting request body sizes.
"""

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.config.constants import GRAPH_MAX_REQUEST_SIZE, MAX_QUERY_LENGTH
from robosystems.logger import logger


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
  """Reject oversized request bodies before they are read into memory.

  The limit is chosen per endpoint family — queries, schema DDL, everything
  else — since a Cypher query and a bulk body have very different reasonable
  sizes.
  """

  def __init__(
    self,
    app,
    max_body_size: int | None = None,
    max_query_size: int | None = None,
    max_schema_size: int | None = None,
  ):
    super().__init__(app)
    # All limits in bytes.
    self.max_body_size = max_body_size or GRAPH_MAX_REQUEST_SIZE
    # MAX_QUERY_LENGTH counts characters; 10x leaves room for multi-byte ones.
    self.max_query_size = max_query_size or MAX_QUERY_LENGTH * 10
    self.max_schema_size = max_schema_size or 1 * 1024 * 1024

    logger.info(
      f"Request Size Limit Middleware initialized - "
      f"Max body: {self.max_body_size:,} bytes, "
      f"Max query: {self.max_query_size:,} bytes, "
      f"Max schema: {self.max_schema_size:,} bytes"
    )

  async def dispatch(self, request: Request, call_next):
    """Reject the request with 413 if its Content-Length exceeds the limit."""
    content_length_str = request.headers.get("content-length")
    if content_length_str:
      content_length = int(content_length_str)

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

    # A chunked request carries no Content-Length and so passes unchecked;
    # enforcing a limit there requires validating the body as it streams.
    return await call_next(request)
