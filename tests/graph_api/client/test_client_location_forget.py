"""A writer that stops answering is left quickly, and its cached location dropped.

A replaced writer comes back at a new address that only the graph registry
knows. The client caps each connect at a few seconds and, on a connection
error, drops the graph's cached location once so the next client resolves it
afresh. The connection error here is real: nothing listens on the port.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.graph_api.client.client import GraphClient
from robosystems.graph_api.client.exceptions import GraphTransientError

pytestmark = pytest.mark.unit

DEAD_WRITER = "http://127.0.0.1:1"


def test_a_connect_gives_up_after_a_few_seconds():
  client = GraphClient(base_url=DEAD_WRITER)
  assert client.client.timeout.connect == 5.0
  assert client.client.timeout.read == 30.0


KEY = "graph:prod:location:kg0123456789abcdef01"


def _forget():
  return patch(
    "robosystems.graph_api.client.factory.GraphClientFactory.forget_location",
    new=AsyncMock(),
  )


async def test_a_refused_connect_drops_the_cached_location_once():
  client = GraphClient(base_url=DEAD_WRITER, max_retries=1, retry_delay=0.01)
  client._location_cache_key = KEY
  with _forget() as forget:
    with pytest.raises(GraphTransientError):
      await client._request("GET", "/health")
    with pytest.raises(GraphTransientError):
      await client._request("GET", "/health")
  forget.assert_awaited_once_with(KEY)
  await client.close()


def _transport(handler):
  import httpx

  return httpx.AsyncClient(
    base_url="http://10.0.0.9:8001", transport=httpx.MockTransport(handler)
  )


async def test_a_silent_writer_drops_the_cached_location_on_a_non_retried_write():
  """A terminated instance drops SYNs: a ConnectTimeout, on a retries=0 write."""
  import httpx

  def handler(request):
    raise httpx.ConnectTimeout("timed out", request=request)

  client = GraphClient(base_url="http://10.0.0.9:8001")
  await client.close()
  client.client = _transport(handler)
  client._location_cache_key = KEY
  with _forget() as forget:
    with pytest.raises(httpx.ConnectTimeout):
      await client._request("POST", "/databases/kg1/tables", retries=0)
  forget.assert_awaited_once_with(KEY)


async def test_a_long_request_timeout_keeps_the_short_connect():
  import httpx

  seen = {}

  def handler(request):
    seen.update(request.extensions["timeout"])
    return httpx.Response(200, json={})

  client = GraphClient(base_url="http://10.0.0.9:8001")
  await client.close()
  client.client = _transport(handler)
  await client._request("POST", "/databases/kg1/materialize", timeout=600.0, retries=0)
  assert seen["connect"] == 5.0
  assert seen["read"] == 600.0
