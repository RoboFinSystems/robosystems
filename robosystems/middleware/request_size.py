"""Request-body size limiting.

An app that reads a request body into memory before authentication or rate
limiting runs has an unbounded body as a pre-auth memory-allocation DoS that
nothing downstream can bound. FastAPI/Starlette resolve the body during
dependency and model validation, and some routes (the Stripe webhook) call
``await request.body()`` explicitly before any check.

:class:`BodySizeLimitMiddleware` enforces the cap in two places:

1. the ``Content-Length`` header (every conforming client sets it), rejected
   before the app is invoked; and
2. the streamed body itself, byte-counted as the middleware buffers it, so a
   chunked request with no ``Content-Length`` is bounded by the same cap.

The body is buffered in the middleware and replayed to the app, so the cap is
enforced without depending on how a downstream route or middleware handles a
mid-read failure — an oversized request is answered with a 413 and the app is
never invoked for it. Neither app stream-consumes a request body (public
uploads go straight to S3 via presigned PUT), so buffering up to the limit
costs nothing a body read would not have cost anyway.

Subclasses choose the limit per request by overriding
:meth:`BodySizeLimitMiddleware._limit_for`. Two do:

* :class:`RequestSizeLimitMiddleware` (here) resolves a longest-prefix path
  override, letting one family — the webhook — carry a tighter limit than the
  default.
* The Graph API's variant resolves a limit per endpoint family, since a Cypher
  query and a bulk body have very different reasonable sizes.

Both inherit the body handling itself, so the limit is the only thing a
subclass varies and the two services cannot enforce it differently.
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

    # Buffer the body, enforcing the cap as it arrives. Reject before invoking
    # the app if it overflows — a chunked request with no Content-Length is
    # bounded here, not merely at the header.
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
          # We answer without consuming the (over-limit) request body. On an
          # HTTP/1.1 keep-alive connection the unread bytes would otherwise be
          # read as the start of the next request and desync the stream, so
          # signal the server to close the connection after this response.
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
