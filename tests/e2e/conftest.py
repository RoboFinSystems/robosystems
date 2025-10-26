import pytest
import time
import secrets
from unittest.mock import patch
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class HTTPClientWrapper:
  """
  Wrapper around httpx.Client that provides a TestClient-like interface.

  Makes REAL HTTP requests to the Docker API container (localhost:8000),
  just like the e2e_workflow_demo.py script does.
  """

  def __init__(self, base_url: str):
    import httpx

    self.client = httpx.Client(base_url=base_url, timeout=30.0)
    self.base_url = base_url

  def post(self, url: str, **kwargs):
    """Make a POST request."""
    response = self.client.post(url, **kwargs)
    response.status_code = response.status_code
    return response

  def get(self, url: str, **kwargs):
    """Make a GET request."""
    response = self.client.get(url, **kwargs)
    response.status_code = response.status_code
    return response

  def patch(self, url: str, **kwargs):
    """Make a PATCH request."""
    response = self.client.patch(url, **kwargs)
    response.status_code = response.status_code
    return response

  def delete(self, url: str, **kwargs):
    """Make a DELETE request."""
    response = self.client.delete(url, **kwargs)
    response.status_code = response.status_code
    return response

  def close(self):
    """Close the HTTP client."""
    self.client.close()


@pytest.fixture
def integration_client():
  """
  Create an HTTP client for E2E tests.

  CRITICAL: This makes REAL HTTP requests to localhost:8000 (the Docker API container),
  exactly like the e2e_workflow_demo.py script does. This ensures the test uses the same
  API instance that Celery workers connect to.
  """
  client = HTTPClientWrapper("http://localhost:8000")
  yield client
  client.close()


@pytest.fixture
def sample_parquet_file(tmp_path):
  """Create a sample Entity parquet file with proper schema."""
  num_rows = 50

  df = pd.DataFrame(
    {
      "identifier": [f"test_entity_{i:03d}" for i in range(1, num_rows + 1)],
      "uri": [None] * num_rows,
      "scheme": [None] * num_rows,
      "cik": [None] * num_rows,
      "ticker": [f"TEST{i}" if i % 10 == 0 else None for i in range(1, num_rows + 1)],
      "exchange": [None] * num_rows,
      "name": [f"Test Entity {i}" for i in range(1, num_rows + 1)],
      "legal_name": [None] * num_rows,
      "industry": ["Technology"] * num_rows,
      "entity_type": ["corporation"] * num_rows,
      "sic": [None] * num_rows,
      "sic_description": [None] * num_rows,
      "category": [f"Category_{i % 5}" for i in range(1, num_rows + 1)],
      "state_of_incorporation": [None] * num_rows,
      "fiscal_year_end": [None] * num_rows,
      "ein": [None] * num_rows,
      "tax_id": [None] * num_rows,
      "lei": [None] * num_rows,
      "phone": [None] * num_rows,
      "website": [None] * num_rows,
      "status": ["active"] * num_rows,
      "is_parent": pd.Series(
        [True if i % 5 == 0 else None for i in range(num_rows)], dtype="boolean"
      ),
      "parent_entity_id": [None] * num_rows,
      "created_at": pd.date_range("2025-01-01", periods=num_rows, freq="h").strftime(
        "%Y-%m-%d %H:%M:%S"
      ),
      "updated_at": [None] * num_rows,
    }
  )

  file_path = tmp_path / "test_entities.parquet"
  table = pa.Table.from_pandas(df)
  pq.write_table(table, file_path)

  return file_path, num_rows


@pytest.fixture
def test_user_with_api_key(integration_client):
  """
  Create a test user with API key - ALL via API calls (like production).

  This matches the e2e_workflow_demo.py script which works perfectly.
  """
  registration_data = {
    "name": "E2E Test User",
    "email": f"e2e_{int(time.time())}_{secrets.token_hex(4)}@example.com",
    "password": "S3cur3P@ssw0rd!E2E",
  }

  with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
    register_response = integration_client.post(
      "/v1/auth/register", json=registration_data
    )
    assert register_response.status_code == 201
    user_data = register_response.json()["user"]
    user_id = user_data["id"]

  login_response = integration_client.post(
    "/v1/auth/login",
    json={
      "email": registration_data["email"],
      "password": registration_data["password"],
    },
  )
  assert login_response.status_code == 200
  jwt_token = login_response.json()["token"]

  api_key_response = integration_client.post(
    "/v1/user/api-keys",
    headers={"Authorization": f"Bearer {jwt_token}"},
    json={"name": "E2E Test API Key"},
  )
  assert api_key_response.status_code == 201
  api_key = api_key_response.json()["key"]

  user_info = {
    "user_id": user_id,
    "email": registration_data["email"],
    "api_key": api_key,
    "headers": {"X-API-Key": api_key},
    "jwt_token": jwt_token,
  }

  yield user_info

  try:
    keys_response = integration_client.get(
      "/v1/user/api-keys",
      headers={"Authorization": f"Bearer {jwt_token}"},
    )
    if keys_response.status_code == 200:
      for key in keys_response.json().get("keys", []):
        integration_client.delete(
          f"/v1/user/api-keys/{key['id']}",
          headers={"Authorization": f"Bearer {jwt_token}"},
        )
  except Exception:
    pass


@pytest.fixture
def cleanup_graphs():
  """Track graph IDs created during test (for manual cleanup if needed)."""
  graph_ids = []
  yield graph_ids
