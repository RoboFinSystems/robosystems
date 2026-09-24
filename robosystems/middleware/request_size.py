"""Request-body size limiting, enforced before auth or rate limiting read
the body.

The cap applies to ``Content-Length`` and to the streamed body itself (so a
chunked request is bounded too). The body is buffered and replayed, so an
oversized request gets a 413 without the app ever being invoked; nothing
stream-consumes a request body, so buffering costs nothing extra.

Subclasses vary only the limit, via :meth:`BodySizeLimitMiddleware._limit_for`
(here per path prefix; the Graph API per endpoint family).
"""

from __future__ import annotations

from collections.abc import Sequence

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from robosystems.config.constants import PUBLIC_MAX_REQUEST_SIZE
from robosystems.logger import logger


class BodySizeLimitMiddleware:
  """Reject request bodies larger than a per-request byte limit with a 413."""

  def __init__(self, app: ASGIApp, *, max_body_size: int) -> None:
    self.app = app
    self.max_body_size = max_body_size

  def _limit_for(self, path: str) -> tuple[int, str]:
    """The byte limit for ``path``, and the noun to describe it in the 413."""
    return self.max_body_size, "body"

  async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
      await self.app(scope, receive, send)
      return

    path = scope.get("path", "")
    limit, label = self._limit_for(path)

    content_length = _content_length(scope)
    if content_length is not None and content_length > limit:
      await self._reject(
        send, path, content_length, limit, label, source="content-length"
      )
      return

    buffered: list[Message] = []
    received = 0
    while True:
      message = await receive()
      if message["type"] != "http.request":
        buffered.append(message)
        break
      received += len(message.get("body", b""))
      if received > limit:
        await self._reject(send, path, received, limit, label, source="stream")
        return
      buffered.append(message)
      if not message.get("more_body", False):
        break

    replay = _iter_messages(buffered, receive)
    await self.app(scope, replay, send)

  async def _reject(
    self,
    send: Send,
    path: str,
    size: int,
    limit: int,
    label: str,
    *,
    source: str,
  ) -> None:
    logger.warning(
      f"Request {label} too large on {path}: {size:,} bytes "
      f"exceeds {limit:,} ({source})"
    )
    body = (
      b'{"detail":"Request '
      + label.encode()
      + b" too large. Max allowed: "
      + str(limit).encode()
      + b' bytes"}'
    )
    await send(
      {
        "type": "http.response.start",
        "status": 413,
        "headers": [
          (b"content-type", b"application/json"),
          (b"content-length", str(len(body)).encode()),
          # The unread body would desync a keep-alive connection.
          (b"connection", b"close"),
        ],
      }
    )
    await send({"type": "http.response.body", "body": body})


class RequestSizeLimitMiddleware(BodySizeLimitMiddleware):
  """The public app's limiter: a default cap plus per-path overrides."""

  def __init__(
    self,
    app: ASGIApp,
    *,
    max_body_size: int = PUBLIC_MAX_REQUEST_SIZE,
    path_limits: Sequence[tuple[str, int]] = (),
  ) -> None:
    super().__init__(app, max_body_size=max_body_size)
    # Longest prefix first so a specific path wins over a broader one.
    self.path_limits = tuple(
      sorted(path_limits, key=lambda item: len(item[0]), reverse=True)
    )
    logger.info(
      "Request Size Limit Middleware initialized - "
      f"default {self.max_body_size:,} bytes"
      + (f", {len(self.path_limits)} path override(s)" if self.path_limits else "")
    )

  def _limit_for(self, path: str) -> tuple[int, str]:
    for prefix, limit in self.path_limits:
      if path.startswith(prefix):
        return limit, "body"
    return self.max_body_size, "body"


def _iter_messages(buffered: list[Message], receive: Receive) -> Receive:
  """Replay the buffered request messages, then fall through to live receive.

  Falling through matters for a client disconnect that arrives after the body:
  the app awaits ``receive`` again and gets the real ``http.disconnect``.
  """
  index = 0

  async def replay() -> Message:
    nonlocal index
    if index < len(buffered):
      message = buffered[index]
      index += 1
      return message
    return await receive()

  return replay


def _content_length(scope: Scope) -> int | None:
  for name, value in scope.get("headers", ()):
    if name == b"content-length":
      try:
        return int(value)
      except ValueError:
        return None
  return None
