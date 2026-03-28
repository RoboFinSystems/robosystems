"""HTTP client for worker → API calls through internal ALB.

The worker calls the same REST API endpoints as the frontend and SDK,
authenticated via internal API key. In Docker Compose this goes to
robosystems-api:8000; in staging/prod it goes through the internal ALB
or service discovery endpoint.
"""

import logging
from typing import Any

import httpx

from robosystems.config import env

logger = logging.getLogger(__name__)


class WorkerAPIClient:
  """Async HTTP client for worker → API calls."""

  def __init__(self, graph_id: str, api_key: str | None = None) -> None:
    self.graph_id = graph_id
    self.base_url = env.WORKER_API_BASE_URL.rstrip("/")
    self.api_key = api_key or env.WORKER_API_KEY
    self._headers = {
      "X-API-Key": self.api_key,
      "Content-Type": "application/json",
    }

  async def get(
    self, path: str, params: dict[str, Any] | None = None
  ) -> dict[str, Any]:
    """GET request to API."""
    async with httpx.AsyncClient(
      base_url=self.base_url, headers=self._headers
    ) as client:
      response = await client.get(path, params=params, timeout=30.0)
      response.raise_for_status()
      return response.json()

  async def post(self, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
    """POST request to API."""
    async with httpx.AsyncClient(
      base_url=self.base_url, headers=self._headers
    ) as client:
      response = await client.post(path, json=json, timeout=30.0)
      response.raise_for_status()
      return response.json()

  async def delete(self, path: str) -> dict[str, Any]:
    """DELETE request to API."""
    async with httpx.AsyncClient(
      base_url=self.base_url, headers=self._headers
    ) as client:
      response = await client.delete(path, timeout=30.0)
      response.raise_for_status()
      return response.json()
