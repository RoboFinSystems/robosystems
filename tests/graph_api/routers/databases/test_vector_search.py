"""Tests for vector search router endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app


@pytest.mark.unit
class TestVectorSearchRouter:
  """Test cases for vector index endpoints."""

  @pytest.fixture
  def client(self, monkeypatch):
    """Create a test client with mocked LanceManager."""
    monkeypatch.setenv("GRAPH_BACKEND_TYPE", "ladybug")

    app = create_app()
    return TestClient(app)

  @pytest.fixture
  def mock_manager(self):
    """Create a mock LanceManager."""
    return MagicMock()

  # -----------------------------------------------------------------------
  # POST /vector/search
  # -----------------------------------------------------------------------

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_search_success(self, mock_get_manager, client):
    mock_mgr = MagicMock()
    mock_mgr.search.return_value = {
      "results": [
        {"qname": "us-gaap:Revenues", "distance": 0.05},
        {"qname": "us-gaap:Assets", "distance": 0.12},
      ],
      "total": 2,
      "execution_time_ms": 4.5,
    }
    mock_get_manager.return_value = mock_mgr

    response = client.post(
      "/databases/sec/tables/Element/vector/search",
      json={"embedding": [0.1] * 384, "limit": 10},
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["total"] == 2
    assert len(data["results"]) == 2
    assert data["results"][0]["qname"] == "us-gaap:Revenues"
    assert data["execution_time_ms"] == 4.5

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_search_with_select_columns(self, mock_get_manager, client):
    mock_mgr = MagicMock()
    mock_mgr.search.return_value = {
      "results": [{"qname": "us-gaap:Revenues", "distance": 0.05}],
      "total": 1,
      "execution_time_ms": 3.0,
    }
    mock_get_manager.return_value = mock_mgr

    response = client.post(
      "/databases/sec/tables/Element/vector/search",
      json={"embedding": [0.1] * 384, "select": ["qname"]},
    )

    assert response.status_code == status.HTTP_200_OK
    mock_mgr.search.assert_called_once()
    call_kwargs = mock_mgr.search.call_args[1]
    assert call_kwargs["select_columns"] == ["qname"]

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_search_no_index_returns_404(self, mock_get_manager, client):
    mock_mgr = MagicMock()
    mock_mgr.search.side_effect = ValueError("No vector index for sec/Element")
    mock_get_manager.return_value = mock_mgr

    response = client.post(
      "/databases/sec/tables/Element/vector/search",
      json={"embedding": [0.1] * 384},
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND

  def test_search_empty_embedding_rejected(self, client):
    response = client.post(
      "/databases/sec/tables/Element/vector/search",
      json={"embedding": []},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

  def test_search_limit_too_high_rejected(self, client):
    response = client.post(
      "/databases/sec/tables/Element/vector/search",
      json={"embedding": [0.1] * 384, "limit": 200},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

  # -----------------------------------------------------------------------
  # POST /vector/build
  # -----------------------------------------------------------------------

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_build_success(self, mock_get_manager, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.build.return_value = {
      "graph_id": "sec",
      "table_name": "Element",
      "row_count": 2600000,
      "num_partitions": 256,
      "index_size_mb": 3500.0,
      "duration_ms": 650000.0,
    }
    mock_get_manager.return_value = mock_mgr

    response = client.post(
      "/databases/sec/tables/Element/vector/build",
      json={
        "query": "SELECT embedding::FLOAT[384] AS vector FROM Element",
        "memory_limit": "8GB",
      },
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["row_count"] == 2600000
    assert data["num_partitions"] == 256

  def test_build_blocked_on_replica(self, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "replica")

    response = client.post(
      "/databases/sec/tables/Element/vector/build",
      json={"query": "SELECT 1 AS vector"},
    )

    assert response.status_code == status.HTTP_501_NOT_IMPLEMENTED

  def test_build_invalid_memory_limit_rejected(self, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    response = client.post(
      "/databases/sec/tables/Element/vector/build",
      json={"query": "SELECT 1 AS vector", "memory_limit": "8GB'; DROP TABLE x; --"},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_build_valid_memory_limit_formats(self, mock_get_manager, client, monkeypatch):
    """Various valid DuckDB memory formats are accepted by the validator."""
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.build.return_value = {
      "graph_id": "sec",
      "table_name": "Element",
      "row_count": 1,
      "num_partitions": 0,
      "index_size_mb": 0.1,
      "duration_ms": 10.0,
    }
    mock_get_manager.return_value = mock_mgr

    for ml in ["256MB", "8GB", "1TB", "512KB", "1.5GB"]:
      response = client.post(
        "/databases/sec/tables/Element/vector/build",
        json={"query": "SELECT 1 AS vector", "memory_limit": ml},
      )
      assert response.status_code == status.HTTP_200_OK, (
        f"memory_limit '{ml}' was incorrectly rejected"
      )

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_build_bad_query_returns_400(self, mock_get_manager, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.build.side_effect = ValueError("Query must return a 'vector' column")
    mock_get_manager.return_value = mock_mgr

    response = client.post(
      "/databases/sec/tables/Element/vector/build",
      json={"query": "SELECT qname FROM Element"},
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST

  def test_build_rejects_extra_fields(self, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    response = client.post(
      "/databases/sec/tables/Element/vector/build",
      json={
        "query": "SELECT 1 AS vector",
        "malicious_field": "drop table",
      },
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

  # -----------------------------------------------------------------------
  # POST /vector/export
  # -----------------------------------------------------------------------

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_export_success(self, mock_get_manager, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.export.return_value = {
      "graph_id": "sec",
      "table_name": "Element",
      "tar_path": "/app/data/lance/sec/Element.lance.tar.gz",
      "size_mb": 3200.0,
      "duration_ms": 45000.0,
    }
    mock_get_manager.return_value = mock_mgr

    response = client.post("/databases/sec/tables/Element/vector/export")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["tar_path"] == "/app/data/lance/sec/Element.lance.tar.gz"
    assert data["size_mb"] == 3200.0

  def test_export_blocked_on_replica(self, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "replica")

    response = client.post("/databases/sec/tables/Element/vector/export")

    assert response.status_code == status.HTTP_501_NOT_IMPLEMENTED

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_export_no_index_returns_404(self, mock_get_manager, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.export.side_effect = ValueError("No vector index for sec/Element")
    mock_get_manager.return_value = mock_mgr

    response = client.post("/databases/sec/tables/Element/vector/export")

    assert response.status_code == status.HTTP_404_NOT_FOUND

  # -----------------------------------------------------------------------
  # GET /vector (info)
  # -----------------------------------------------------------------------

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_info_returns_metadata(self, mock_get_manager, client):
    mock_mgr = MagicMock()
    mock_mgr.get_index_info.return_value = {
      "graph_id": "sec",
      "table_name": "Element",
      "row_count": 2600000,
      "size_mb": 3500.0,
      "path": "/app/data/lance/sec/Element.lance",
    }
    mock_get_manager.return_value = mock_mgr

    response = client.get("/databases/sec/tables/Element/vector")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["row_count"] == 2600000
    assert data["size_mb"] == 3500.0

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_info_returns_null_for_missing(self, mock_get_manager, client):
    mock_mgr = MagicMock()
    mock_mgr.get_index_info.return_value = None
    mock_get_manager.return_value = mock_mgr

    response = client.get("/databases/sec/tables/Element/vector")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() is None

  # -----------------------------------------------------------------------
  # DELETE /vector
  # -----------------------------------------------------------------------

  @patch("robosystems.graph_api.routers.databases.vector_search._get_lance_manager")
  def test_delete_success(self, mock_get_manager, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "master")

    mock_mgr = MagicMock()
    mock_mgr.delete.return_value = {
      "graph_id": "sec",
      "deleted": ["/app/data/lance/sec/Element.lance"],
    }
    mock_get_manager.return_value = mock_mgr

    response = client.delete("/databases/sec/tables/Element/vector")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "sec"
    assert len(data["deleted"]) == 1

  def test_delete_blocked_on_replica(self, client, monkeypatch):
    monkeypatch.setenv("LBUG_ROLE", "replica")

    response = client.delete("/databases/sec/tables/Element/vector")

    assert response.status_code == status.HTTP_501_NOT_IMPLEMENTED
