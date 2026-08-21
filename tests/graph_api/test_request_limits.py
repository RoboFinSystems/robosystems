"""The Graph API's body cap, and the per-family limit it layers on top.

The interesting property is that the limit depends on the path while the
enforcement does not: a query body and a bulk body get different ceilings, but
both are bounded by the declared ``Content-Length`` *and* by the bytes actually
streamed, so a chunked request with no ``Content-Length`` is not a way around
either one.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from robosystems.config.constants import GRAPH_MAX_REQUEST_SIZE, MAX_QUERY_LENGTH
from robosystems.graph_api.middleware.request_limits import RequestSizeLimitMiddleware


def _app(**kwargs) -> FastAPI:
  app = FastAPI()

  @app.post("/databases/{graph_id}/query")
  async def query(graph_id: str, request: Request):
    return {"len": len(await request.body())}

  @app.post("/databases/{graph_id}/schema")
  async def schema(graph_id: str, request: Request):
    return {"len": len(await request.body())}

  @app.post("/databases/{graph_id}/tables/rows")
  async def rows(graph_id: str, request: Request):
    return {"len": len(await request.body())}

  @app.get("/health")
  async def health():
    return {"ok": True}

  @app.post("/databases/{graph_id}/query/stream")
  async def query_stream(graph_id: str, request: Request):
    size = len(await request.body())

    async def gen():
      for _ in range(3):
        yield b"chunk;"
      yield str(size).encode()

    return StreamingResponse(gen(), media_type="text/plain")

  app.add_middleware(RequestSizeLimitMiddleware, **kwargs)
  return app


def _chunks(total: int, each: int = 100):
  """A generator body, which makes httpx send it chunked with no length."""

  def gen():
    sent = 0
    while sent < total:
      n = min(each, total - sent)
      sent += n
      yield b"x" * n

  return gen()


@pytest.mark.unit
class TestGraphApiRequestSizeLimit:
  def test_body_within_limit_passes(self):
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/databases/kg1/tables/rows", content=b"x" * 500)
    assert r.status_code == 200
    assert r.json() == {"len": 500}

  def test_content_length_over_limit_is_413(self):
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/databases/kg1/tables/rows", content=b"x" * 2000)
    assert r.status_code == 413
    assert "too large" in r.json()["detail"].lower()
    # Answered without consuming the body → the connection must not be reused.
    assert r.headers.get("connection", "").lower() == "close"

  def test_chunked_body_over_limit_is_413(self):
    """No Content-Length; the cap must hold on the streamed bytes."""
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/databases/kg1/tables/rows", content=_chunks(3000))
    assert r.status_code == 413

  def test_chunked_body_within_limit_passes(self):
    client = TestClient(_app(max_body_size=1000))
    r = client.post("/databases/kg1/tables/rows", content=_chunks(300))
    assert r.status_code == 200
    assert r.json() == {"len": 300}

  def test_empty_body_and_get_pass(self):
    client = TestClient(_app(max_body_size=1000))
    assert client.get("/health").status_code == 200
    assert client.post("/databases/kg1/tables/rows", content=b"").json() == {"len": 0}


@pytest.mark.unit
class TestPerFamilyLimits:
  """A path over its own family's limit is rejected even when the generous
  default would have let it through — and the 413 names the family."""

  def test_query_limit_is_independent_of_the_body_limit(self):
    client = TestClient(_app(max_body_size=100_000, max_query_size=1000))
    over = client.post("/databases/kg1/query", content=b"x" * 5000)
    assert over.status_code == 413
    assert "query" in over.json()["detail"].lower()
    # The same body on a non-query path is under the default and passes.
    assert (
      client.post("/databases/kg1/tables/rows", content=b"x" * 5000).status_code == 200
    )

  def test_schema_limit_is_independent_of_the_body_limit(self):
    client = TestClient(_app(max_body_size=100_000, max_schema_size=1000))
    over = client.post("/databases/kg1/schema", content=b"x" * 5000)
    assert over.status_code == 413
    assert "schema" in over.json()["detail"].lower()

  def test_chunked_query_body_is_bounded_by_the_query_limit(self):
    """The family limit and the stream cap compose; neither is a way past the
    other."""
    client = TestClient(_app(max_body_size=100_000, max_query_size=1000))
    r = client.post("/databases/kg1/query", content=_chunks(5000))
    assert r.status_code == 413
    assert "query" in r.json()["detail"].lower()

  def test_defaults_come_from_the_shared_constants(self):
    limiter = RequestSizeLimitMiddleware(FastAPI())
    assert limiter.max_body_size == GRAPH_MAX_REQUEST_SIZE
    assert limiter.max_query_size == MAX_QUERY_LENGTH * 10
    assert limiter._limit_for("/databases/kg1/query")[0] == MAX_QUERY_LENGTH * 10
    assert limiter._limit_for("/databases/kg1/tables/rows")[0] == GRAPH_MAX_REQUEST_SIZE


@pytest.mark.unit
def test_streaming_responses_pass_through():
  """The Graph API streams query results, so the limiter must not buffer or
  break a streamed response on the way back out."""
  client = TestClient(_app(max_body_size=100_000, max_query_size=100_000))
  with client.stream(
    "POST", "/databases/kg1/query/stream", content=b"x" * 50
  ) as response:
    assert response.status_code == 200
    body = b"".join(response.iter_bytes())
  assert body == b"chunk;chunk;chunk;50"


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
  "messages,expected",
  [
    ([{"type": "http.disconnect"}], ["http.disconnect"]),
    (
      [
        {"type": "http.request", "body": b"x" * 10, "more_body": True},
        {"type": "http.disconnect"},
      ],
      ["http.request", "http.disconnect"],
    ),
  ],
)
async def test_a_disconnect_reaches_the_app_intact(messages, expected):
  """Buffering must not swallow a disconnect or reorder it behind the body.

  The app decides what a mid-body disconnect means — Starlette raises
  ``ClientDisconnect`` — so the limiter's job is to replay what arrived, in
  order, and not to hang waiting for a body that is never coming.
  """
  seen: list[str] = []

  async def app(scope, receive, send):
    while True:
      message = await receive()
      seen.append(message["type"])
      if message["type"] != "http.request" or not message.get("more_body"):
        break
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b""})

  limiter = RequestSizeLimitMiddleware(app, max_body_size=1000)
  pending = iter(messages)

  async def receive():
    return next(pending, {"type": "http.disconnect"})

  async def send(message):
    return None

  await asyncio.wait_for(
    limiter({"type": "http", "path": "/x", "headers": []}, receive, send),
    timeout=5,
  )
  assert seen == expected
