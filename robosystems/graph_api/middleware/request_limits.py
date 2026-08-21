"""Request size limiting for the Graph API.

The body handling — header check, streamed byte count, buffer-and-replay — is
:class:`~robosystems.middleware.request_size.BodySizeLimitMiddleware`. This
module supplies only the limit, which varies by endpoint family.
"""

from __future__ import annotations

from starlette.types import ASGIApp

from robosystems.config.constants import GRAPH_MAX_REQUEST_SIZE, MAX_QUERY_LENGTH
from robosystems.logger import logger
from robosystems.middleware.request_size import BodySizeLimitMiddleware


class RequestSizeLimitMiddleware(BodySizeLimitMiddleware):
  """Reject oversized request bodies before they are read into memory.

  The limit is chosen per endpoint family — queries, schema DDL, everything
  else — since a Cypher query and a bulk body have very different reasonable
  sizes.
  """

  def __init__(
    self,
    app: ASGIApp,
    max_body_size: int | None = None,
    max_query_size: int | None = None,
    max_schema_size: int | None = None,
  ) -> None:
    # All limits in bytes.
    super().__init__(app, max_body_size=max_body_size or GRAPH_MAX_REQUEST_SIZE)
    # MAX_QUERY_LENGTH counts characters; 10x leaves room for multi-byte ones.
    self.max_query_size = max_query_size or MAX_QUERY_LENGTH * 10
    self.max_schema_size = max_schema_size or 1 * 1024 * 1024

    logger.info(
      f"Request Size Limit Middleware initialized - "
      f"Max body: {self.max_body_size:,} bytes, "
      f"Max query: {self.max_query_size:,} bytes, "
      f"Max schema: {self.max_schema_size:,} bytes"
    )

  def _limit_for(self, path: str) -> tuple[int, str]:
    if "/query" in path:
      return self.max_query_size, "query"
    if "/schema" in path:
      return self.max_schema_size, "schema"
    return self.max_body_size, "body"
