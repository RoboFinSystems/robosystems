"""Tests for graph API table materialization endpoint."""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.routers.databases.tables import materialize
from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.database import get_db_session


@pytest.fixture
def app_client(monkeypatch):
  app = FastAPI()
  app.include_router(materialize.router)

  session = SimpleNamespace(commit=lambda: None)

  app.dependency_overrides[get_db_session] = lambda: session
  return app


def test_materialize_rejects_read_only(monkeypatch, app_client):
  cluster_service = SimpleNamespace(read_only=True)
  app_client.dependency_overrides[get_cluster_service] = lambda: cluster_service

  client = TestClient(app_client)

  response = client.post(
    "/databases/graph-123/tables/Entity/materialize",
    json={"ignore_errors": True},
  )

  assert response.status_code == 403
  assert "not allowed" in response.json()["detail"]
