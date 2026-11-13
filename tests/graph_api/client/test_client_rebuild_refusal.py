"""A query to a rebuilding graph is refused by the client, before any request."""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.client.exceptions import GraphTransientError

REBUILDING = "robosystems.middleware.graph.rebuild_status.is_graph_rebuilding"
GRAPH = "kg0123456789abcdef01"


@pytest.fixture
async def client():
  c = GraphClient(base_url="http://localhost:8001")
  c._request = AsyncMock()
  c._request.return_value.content = b'{"data": [], "columns": [], "row_count": 0}'
  c._request.return_value.json.return_value = {"data": [], "row_count": 0}
  yield c
  await c.close()


@pytest.mark.parametrize("streaming", [False, True], ids=["json", "streaming"])
async def test_rebuilding_graph_is_refused_without_a_request(client, streaming):
  with patch(REBUILDING, AsyncMock(return_value=True)):
    with pytest.raises(GraphTransientError) as refused:
      await client.query("MATCH (n) RETURN n", GRAPH, streaming=streaming)

  assert refused.value.status_code == 503
  assert refused.value.response_data["detail"]["status"] == "rebuilding"
  client._request.assert_not_awaited()


async def test_idle_graph_is_queried(client):
  with patch(REBUILDING, AsyncMock(return_value=False)) as check:
    await client.query("MATCH (n) RETURN n", GRAPH)

  check.assert_awaited_once_with(GRAPH)
  client._request.assert_awaited_once()


async def test_replica_reads_skip_the_check(client):
  # A replica serves its own copy; a rebuild of the master does not touch it.
  client._route_target = "shared_replica"
  with patch(REBUILDING, AsyncMock(return_value=True)) as check:
    await client.query("MATCH (n) RETURN n", "sec")

  check.assert_not_awaited()
  client._request.assert_awaited_once()
