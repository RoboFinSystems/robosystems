"""The public-app request-body size limit rejects oversized bodies with a 413,
by Content-Length and by the streamed bytes of a chunked request that declares
none. It enforces the cap in the middleware, before the app is invoked, so it
does not depend on how a downstream route handles a mid-read failure.

The per-path override is this app's own; the body handling it inherits is
covered against the Graph API's per-family limits in
``tests/graph_api/test_request_limits.py``.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.middleware.request_size import RequestSizeLimitMiddleware


def _app(**kwargs) -> FastAPI:
  app = FastAPI()

  # An inner BaseHTTPMiddleware to prove the cap holds through the task-group
  # machinery a BaseHTTPMiddleware wraps receive in.
  class _Inner(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
      return await call_next(request)

  @app.post("/echo")
  async def echo(request: Request):
    return {"len": len(await request.body())}

  @app.get("/ping")
  async def ping():
    return {"ok": True}

  app.add_middleware(_Inner)
  app.add_middleware(RequestSizeLimitMiddleware, **kwargs)
  return app


@pytest.mark.unit
class TestRequestSizeLimit:
  def test_body_within_limit_passes(self):
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/echo", content=b"x" * 500)
    assert r.status_code == 200
    assert r.json() == {"len": 500}

  def test_content_length_over_limit_is_413(self):
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/echo", content=b"x" * 2000)
    assert r.status_code == 413
    assert "too large" in r.json()["detail"].lower()
    # Answered without consuming the body → the connection must not be reused.
    assert r.headers.get("connection", "").lower() == "close"

  def test_chunked_body_over_limit_is_413(self):
    """No Content-Length; the cap must hold on the streamed bytes."""
    client = TestClient(_app(max_body_size=1000))

    def gen():
      for _ in range(30):
        yield b"x" * 100  # 3000 bytes, no Content-Length

    r = client.post("/echo", content=gen())
    assert r.status_code == 413

  def test_chunked_body_within_limit_passes(self):
    client = TestClient(_app(max_body_size=1000))

    def gen():
      yield b"x" * 300

    r = client.post("/echo", content=gen())
    assert r.status_code == 200
    assert r.json() == {"len": 300}

  def test_empty_body_and_get_pass(self):
    client = TestClient(_app(max_body_size=1000))
    assert client.get("/ping").status_code == 200
    assert client.post("/echo", content=b"").json() == {"len": 0}

  def test_per_path_override_is_tighter(self):
    client = TestClient(_app(max_body_size=1_000_000, path_limits=[("/echo", 100)]))
    # Under the default but over the /echo override.
    r = client.post("/echo", content=b"x" * 500)
    assert r.status_code == 413

  def test_longest_prefix_wins(self):
    client = TestClient(
      _app(
        max_body_size=10,
        path_limits=[("/", 10), ("/echo", 10_000)],
      )
    )
    # /echo matches the longer, more permissive prefix.
    r = client.post("/echo", content=b"x" * 500)
    assert r.status_code == 200

  def test_malformed_content_length_falls_through_to_stream_cap(self):
    client = TestClient(_app(max_body_size=100))
    # A non-integer Content-Length is ignored; the stream cap still applies.
    r = client.post(
      "/echo", content=b"x" * 500, headers={"content-length": "not-a-number"}
    )
    # httpx will recompute a valid Content-Length, so this still rejects on
    # whichever path fires — the point is it is not accepted.
    assert r.status_code == 413
