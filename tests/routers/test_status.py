"""
Tests for status endpoint.

This test suite covers:
- Health check endpoint
- Service status
- Version information
- Metrics collection
"""

from datetime import datetime
from unittest.mock import patch

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestStatusEndpoint:
  """Test service status endpoint."""

  async def test_status_endpoint_healthy(self, async_client: AsyncClient):
    """Test that status endpoint returns healthy."""
    response = await async_client.get("/v1/status")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "details" in data
    assert "version" in data["details"]
    assert "service" in data["details"]

  async def test_status_endpoint_no_auth_required(self, async_client: AsyncClient):
    """Test that status endpoint doesn't require authentication."""
    # No auth headers provided
    response = await async_client.get("/v1/status")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"

  async def test_status_endpoint_version(self, async_client: AsyncClient):
    """Test version information in status response."""
    with patch("robosystems.routers.status.version", return_value="1.2.3"):
      response = await async_client.get("/v1/status")

      assert response.status_code == 200
      data = response.json()

      assert data["details"]["version"] == "1.2.3"

  async def test_status_endpoint_version_unknown(self, async_client: AsyncClient):
    """Test status when version cannot be determined."""
    from importlib.metadata import PackageNotFoundError

    with patch(
      "robosystems.routers.status.version",
      side_effect=PackageNotFoundError("robosystems"),
    ):
      response = await async_client.get("/v1/status")

      assert response.status_code == 200
      data = response.json()

      assert data["details"]["version"] == "unknown"

  async def test_status_endpoint_version_error(self, async_client: AsyncClient):
    """Test status when reading version throws error."""
    from importlib.metadata import PackageNotFoundError

    with patch(
      "robosystems.routers.status.version",
      side_effect=PackageNotFoundError("robosystems"),
    ):
      response = await async_client.get("/v1/status")

      assert response.status_code == 200
      data = response.json()

      assert data["details"]["version"] == "unknown"
      assert data["status"] == "healthy"  # Service still healthy despite version error

  async def test_status_endpoint_response_format(self, async_client: AsyncClient):
    """Test the format of status response."""
    response = await async_client.get("/v1/status")

    assert response.status_code == 200
    data = response.json()

    # Check all required fields
    assert "status" in data
    assert "timestamp" in data
    assert "details" in data
    assert "version" in data["details"]
    assert "service" in data["details"]

    # Check field types
    assert isinstance(data["status"], str)
    assert isinstance(data["timestamp"], str)
    assert isinstance(data["details"]["version"], str)
    assert isinstance(data["details"]["service"], str)

    # Check timestamp is valid ISO format
    try:
      datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
    except ValueError:
      pytest.fail("Timestamp is not in valid ISO format")

  async def test_status_endpoint_service_name(self, async_client: AsyncClient):
    """Test service name in status response."""
    response = await async_client.get("/v1/status")

    assert response.status_code == 200
    data = response.json()

    assert data["details"]["service"] == "robosystems-api"

  async def test_status_endpoint_metrics_recorded(self, async_client: AsyncClient):
    """Test that status endpoint records metrics."""
    with patch("robosystems.middleware.otel.metrics.endpoint_metrics_decorator"):
      response = await async_client.get("/v1/status")

      assert response.status_code == 200
      # Metrics decorator should be applied to the endpoint

  async def test_status_endpoint_load_balancer_compatible(
    self, async_client: AsyncClient
  ):
    """Test status endpoint is compatible with AWS load balancer checks."""
    response = await async_client.get("/v1/status")

    assert response.status_code == 200

    # AWS ALB expects 200 status for healthy
    # Response time should be fast (not testing actual time here)
    # Content-Type should be JSON
    assert response.headers.get("content-type") == "application/json"

  async def test_status_endpoint_repeated_calls(self, async_client: AsyncClient):
    """Test multiple rapid calls to status endpoint."""
    # Simulate load balancer making frequent health checks
    for _ in range(10):
      response = await async_client.get("/v1/status")
      assert response.status_code == 200
      data = response.json()
      assert data["status"] == "healthy"

  async def test_status_endpoint_concurrent_calls(self, async_client: AsyncClient):
    """Test concurrent calls to status endpoint."""
    import asyncio

    async def make_request():
      response = await async_client.get("/v1/status")
      return response

    # Make 5 concurrent requests
    tasks = [make_request() for _ in range(5)]
    responses = await asyncio.gather(*tasks)

    for response in responses:
      assert response.status_code == 200
      data = response.json()
      assert data["status"] == "healthy"
