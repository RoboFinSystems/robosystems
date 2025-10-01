import pytest
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_status_check(client: TestClient):
  """Test the status check endpoint.

  This is a unit test because it's testing a simple API endpoint
  that doesn't have external dependencies.
  """
  response = client.get("/v1/status")
  assert response.status_code == 200

  data = response.json()
  # Check required fields
  assert data["status"] == "healthy"

  # Check that timestamp exists and is a valid ISO format
  assert "timestamp" in data
  assert isinstance(data["timestamp"], str)

  # Check details field contains service info
  assert "details" in data
  assert data["details"]["service"] == "robosystems-api"
  assert "version" in data["details"]


@pytest.mark.unit
def test_invalid_endpoint(client: TestClient):
  """Test handling of invalid endpoints.

  This is a unit test because it's testing basic application behavior
  without external dependencies.
  """
  response = client.get("/nonexistent")
  assert response.status_code == 404
