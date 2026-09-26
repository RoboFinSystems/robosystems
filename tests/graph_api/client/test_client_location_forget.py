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


async def test_a_dead_writer_drops_the_cached_location_once():
  client = GraphClient(base_url=DEAD_WRITER, max_retries=1, retry_delay=0.01)
  client._location_cache_key = "graph:prod:location:kg0123456789abcdef01"

  with patch(
    "robosystems.graph_api.client.factory.GraphClientFactory.forget_location",
    new=AsyncMock(),
  ) as forget:
    with pytest.raises(GraphTransientError):
      await client._execute_with_retry(client.client.get, "/health")
    with pytest.raises(GraphTransientError):
      await client._execute_with_retry(client.client.get, "/health")

  forget.assert_awaited_once_with("graph:prod:location:kg0123456789abcdef01")
  await client.close()
