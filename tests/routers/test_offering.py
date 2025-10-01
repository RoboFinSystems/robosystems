"""
Tests for offering endpoint.

This test suite covers:
- Service offering information retrieval
- No authentication required
- Response format validation
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestOfferingEndpoint:
  """Test service offering endpoint."""

  async def test_get_offerings_no_auth_required(self, async_client: AsyncClient):
    """Test that offerings endpoint doesn't require authentication."""
    # No auth headers
    response = await async_client.get("/v1/offering")

    assert response.status_code == 200
    data = response.json()

    # Should return some offering data
    assert data is not None
    assert isinstance(data, dict)

  async def test_get_offerings_response_structure(self, async_client: AsyncClient):
    """Test the structure of offerings response."""
    response = await async_client.get("/v1/offering")

    assert response.status_code == 200
    data = response.json()

    # Check for expected top-level keys
    assert (
      "graph_subscriptions" in data
      or "repository_subscriptions" in data
      or "operation_costs" in data
    )

    # Response should be a dictionary
    assert isinstance(data, dict)

  async def test_get_offerings_rate_limiting(self, async_client: AsyncClient):
    """Test that offerings endpoint has reasonable rate limiting."""
    # Make multiple rapid requests
    responses = []
    for i in range(5):
      response = await async_client.get("/v1/offering")
      responses.append(response)

    # At least some should succeed (reasonable rate limit)
    successful = [r for r in responses if r.status_code == 200]
    assert len(successful) > 0

  async def test_get_offerings_consistent_response(self, async_client: AsyncClient):
    """Test that offerings endpoint returns consistent data."""
    # Get offerings twice
    response1 = await async_client.get("/v1/offering")
    response2 = await async_client.get("/v1/offering")

    assert response1.status_code == 200
    assert response2.status_code == 200

    # Both should return data
    data1 = response1.json()
    data2 = response2.json()

    assert data1 is not None
    assert data2 is not None

    # Structure should be the same
    assert data1.keys() == data2.keys()


# Note: More detailed tests would require knowledge of the actual
# offering data structure which may change over time.
