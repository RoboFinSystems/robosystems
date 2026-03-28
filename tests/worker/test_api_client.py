"""Tests for the WorkerAPIClient."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from robosystems.worker.api_client import WorkerAPIClient

_DUMMY_REQUEST = httpx.Request("GET", "http://test")


@pytest.fixture
def client():
  with patch("robosystems.worker.api_client.env") as mock_env:
    mock_env.WORKER_API_BASE_URL = "http://test-api:8000"
    mock_env.WORKER_API_KEY = "test-key"
    return WorkerAPIClient(graph_id="kg0123456789abcdef")


def test_client_init(client):
  assert client.graph_id == "kg0123456789abcdef"
  assert client.base_url == "http://test-api:8000"
  assert client._headers["X-API-Key"] == "test-key"


@pytest.mark.asyncio
async def test_get(client):
  mock_response = httpx.Response(200, json={"elements": []}, request=_DUMMY_REQUEST)
  with patch(
    "httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response
  ):
    result = await client.get("/v1/ledger/kg123/elements")
    assert result == {"elements": []}


@pytest.mark.asyncio
async def test_post(client):
  mock_response = httpx.Response(200, json={"created": True}, request=_DUMMY_REQUEST)
  with patch(
    "httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response
  ):
    result = await client.post(
      "/v1/ledger/kg123/mappings/m1/associations", json={"a": 1}
    )
    assert result == {"created": True}


@pytest.mark.asyncio
async def test_get_raises_on_error(client):
  mock_response = httpx.Response(
    404, json={"detail": "not found"}, request=_DUMMY_REQUEST
  )
  with patch(
    "httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response
  ):
    with pytest.raises(httpx.HTTPStatusError):
      await client.get("/v1/missing")
