"""Tests for graph API table management endpoints."""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.core.duckdb.manager import TableCreateResponse, TableInfo
from robosystems.graph_api.routers.databases.tables import management


@pytest.fixture
def client(monkeypatch):
  app = FastAPI()
  app.include_router(management.router)

  fake_manager = SimpleNamespace(
    create_table=None,
    list_tables=None,
    delete_table=None,
  )
  monkeypatch.setattr(management, "table_manager", fake_manager)

  test_client = TestClient(app)
  test_client.fake_manager = fake_manager
  return test_client


def test_create_table_success(client):
  client.fake_manager.create_table = lambda req: TableCreateResponse(
    status="created",
    graph_id=req.graph_id,
    table_name=req.table_name,
    execution_time_ms=5.0,
  )

  response = client.post(
    "/databases/graph-123/tables",
    json={
      "graph_id": "graph-ignored",
      "table_name": "Entity",
      "s3_pattern": "s3://bucket/user/foo/graph-123/Entity/*.parquet",
    },
  )

  assert response.status_code == 200
  payload = response.json()
  assert payload["table_name"] == "Entity"
  assert payload["status"] == "created"


def test_create_table_error(client):
  def _raise(req):
    raise RuntimeError("boom")

  client.fake_manager.create_table = _raise

  response = client.post(
    "/databases/graph-123/tables",
    json={
      "graph_id": "graph-ignored",
      "table_name": "Entity",
      "s3_pattern": "s3://bucket/user/foo/graph-123/Entity/*.parquet",
    },
  )

  assert response.status_code == 500
  assert "Failed to create table" in response.json()["detail"]


def test_list_tables(client):
  client.fake_manager.list_tables = lambda graph_id: [
    TableInfo(
      graph_id=graph_id,
      table_name="Entity",
      size_bytes=1024,
      row_count=123,
      s3_location="s3://bucket/user/graph-123/Entity/**",
    )
  ]

  response = client.get("/databases/graph-123/tables")

  assert response.status_code == 200
  payload = response.json()
  assert payload[0]["table_name"] == "Entity"


def test_delete_table(client):
  client.fake_manager.delete_table = lambda graph_id, table_name: {
    "status": "deleted",
    "table_name": table_name,
  }

  response = client.delete("/databases/graph-123/tables/Entity")

  assert response.status_code == 200
  payload = response.json()
  assert payload["status"] == "deleted"
