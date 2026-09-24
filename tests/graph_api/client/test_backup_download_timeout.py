"""A full-dump download must not inherit the client's 30s default timeout."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.graph_api.client.client import GraphClient


@pytest.mark.unit
@pytest.mark.asyncio
async def test_backup_download_waits_for_a_large_dump():
  client = GraphClient(base_url="http://graph.local")
  response = MagicMock(content=b"x", headers={})
  client.client.post = AsyncMock(return_value=response)

  await client.download_backup("kg0000000000000001")

  timeout = client.client.post.call_args.kwargs["timeout"]
  assert timeout.read >= 1800
  assert client.config.timeout == 30  # the rest of the client is unchanged
  await client.close()
