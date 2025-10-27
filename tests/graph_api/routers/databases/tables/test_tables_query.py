"""Tests for graph API DuckDB query endpoint."""

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.routers.databases.tables import query
from robosystems.graph_api.core.duckdb_manager import TableQueryResponse


@pytest.fixture
def client(monkeypatch):
  app = FastAPI()
  app.include_router(query.router)

  fake_manager = SimpleNamespace(
    query_table=None,
    query_table_streaming=None,
  )
  monkeypatch.setattr(query, "table_manager", fake_manager)

  test_client = TestClient(app)
  test_client.fake_manager = fake_manager
  return test_client


def test_query_tables_json_response(client):
  client.fake_manager.query_table = lambda req: TableQueryResponse(
    columns=["id"],
    rows=[[1]],
    execution_time_ms=10.0,
    row_count=1,
  )

  response = client.post(
    "/databases/graph-123/tables/query",
    json={"graph_id": "graph-ignored", "sql": "SELECT 1 AS id"},
  )

  assert response.status_code == 200
  payload = response.json()
  assert payload["row_count"] == 1
  assert payload["columns"] == ["id"]


def test_query_tables_streaming_ndjson(client):
  chunks = [
    {
      "chunk_index": 0,
      "rows": [[1]],
      "row_count": 1,
      "total_rows_sent": 1,
      "is_last_chunk": False,
      "execution_time_ms": 5,
    },
    {
      "chunk_index": 1,
      "rows": [[2]],
      "row_count": 1,
      "total_rows_sent": 2,
      "is_last_chunk": True,
      "execution_time_ms": 9,
    },
  ]

  client.fake_manager.query_table_streaming = lambda req, chunk_size: iter(chunks)

  response = client.post(
    "/databases/graph-123/tables/query",
    json={"graph_id": "graph-ignored", "sql": "SELECT id FROM Entity"},
    headers={"accept": "application/x-ndjson"},
  )

  assert response.status_code == 200
  assert response.headers["X-Streaming"] == "true"

  lines = [line for line in response.text.strip().split("\n") if line]
  assert len(lines) == 2
  payload0 = json.loads(lines[0])
  assert payload0["rows"] == [[1]]


def test_query_tables_error(client):
  def _raise(req):
    raise RuntimeError("bad query")

  client.fake_manager.query_table = _raise

  response = client.post(
    "/databases/graph-123/tables/query",
    json={"graph_id": "graph-ignored", "sql": "SELECT * FROM Missing"},
  )

  assert response.status_code == 400
  assert "failed" in response.json()["detail"].lower()
