"""Tests for database query router endpoints."""

from unittest.mock import MagicMock, patch
import pytest
from fastapi import status
from fastapi.testclient import TestClient
import json

from robosystems.graph_api.app import create_app
from robosystems.graph_api.core.admission_control import AdmissionDecision


class TestDatabaseQueryRouter:
  """Test cases for database query endpoints."""

  @pytest.fixture
  def client(self, monkeypatch):
    """Create a test client."""
    monkeypatch.setenv("GRAPH_BACKEND_TYPE", "ladybug")

    app = create_app()

    # Override the cluster service dependency factory function
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = MagicMock()
    app.dependency_overrides[_get_cluster_service_for_request] = lambda: mock_service

    return TestClient(app)

  @pytest.fixture
  def mock_query_request(self):
    """Create a mock query request."""
    return {
      "database": "kg1a2b3c4d5",
      "cypher": "MATCH (n:Entity) RETURN n LIMIT 10",
      "parameters": {},
    }

  @pytest.fixture
  def mock_query_response(self):
    """Create a mock query response."""
    return {
      "columns": ["n"],
      "rows": [
        {"n": {"id": 1, "name": "Entity1", "type": "Company"}},
        {"n": {"id": 2, "name": "Entity2", "type": "Company"}},
        {"n": {"id": 3, "name": "Entity3", "type": "Company"}},
      ],
      "count": 3,
      "execution_time_ms": 25.5,
    }

  def test_execute_query_success(self, client, mock_query_request, mock_query_response):
    """Test successful query execution."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = mock_query_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=mock_query_request,
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["count"] == 3
      assert len(data["rows"]) == 3
      assert data["execution_time_ms"] == 25.5

      # Verify admission control was checked
      mock_admission.check_admission.assert_called_once_with("kg1a2b3c4d5", "query")
      # Verify connection tracking
      mock_admission.register_connection.assert_called_once_with("kg1a2b3c4d5")
      mock_admission.release_connection.assert_called_once_with("kg1a2b3c4d5")

  def test_execute_query_with_parameters(self, client):
    """Test query execution with parameters."""
    parameterized_query = {
      "database": "kg1a2b3c4d5",
      "cypher": "MATCH (n:Entity {name: $name}) RETURN n",
      "parameters": {"name": "TestEntity"},
    }

    expected_response = {
      "columns": ["n"],
      "rows": [{"n": {"id": 1, "name": "TestEntity"}}],
      "count": 1,
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = expected_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=parameterized_query,
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["count"] == 1
      assert data["rows"][0]["n"]["name"] == "TestEntity"

      # Verify the query request was created correctly
      called_request = mock_service.execute_query.call_args[0][0]
      assert called_request.database == "kg1a2b3c4d5"
      assert called_request.parameters == {"name": "TestEntity"}

  def test_execute_query_admission_rejected(self, client, mock_query_request):
    """Test query rejected by admission control."""
    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (
        AdmissionDecision.REJECT_CPU,
        "Server overloaded",
      )
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=mock_query_request,
      )

      assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
      data = response.json()
      assert "temporarily unavailable" in data["detail"]["error"]
      assert data["detail"]["reason"] == "Server overloaded"
      assert data["detail"]["retry_after"] == 5

  def test_execute_query_streaming(self, client, mock_query_request):
    """Test streaming query execution."""
    # Streaming responses generate NDJSON
    streaming_chunks = [
      {"columns": ["n"], "rows": [{"n": {"id": 1}}], "count": 1},
      {"columns": ["n"], "rows": [{"n": {"id": 2}}], "count": 1},
      {"columns": ["n"], "rows": [{"n": {"id": 3}}], "count": 1},
    ]

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query_streaming.return_value = streaming_chunks

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query?streaming=true",
        json=mock_query_request,
      )

      assert response.status_code == status.HTTP_200_OK
      assert response.headers["X-Streaming"] == "true"
      assert response.headers["Cache-Control"] == "no-cache"
      assert "application/x-ndjson" in response.headers["content-type"]

      # Parse NDJSON response
      lines = response.text.strip().split("\n")
      assert len(lines) == 3
      for i, line in enumerate(lines):
        chunk = json.loads(line)
        assert chunk["count"] == 1
        assert chunk["rows"][0]["n"]["id"] == i + 1

  def test_execute_query_empty_result(self, client):
    """Test query with empty result set."""
    empty_query = {
      "database": "kg1a2b3c4d5",
      "cypher": "MATCH (n:NonExistent) RETURN n",
      "parameters": {},
    }

    empty_response = {
      "columns": ["n"],
      "rows": [],
      "count": 0,
      "execution_time_ms": 5.0,
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = empty_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=empty_query,
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["count"] == 0
      assert data["rows"] == []

  def test_execute_query_complex_cypher(self, client):
    """Test execution of complex Cypher query."""
    complex_query = {
      "database": "kg1a2b3c4d5",
      "cypher": """
                MATCH (e:Entity)-[r:RELATED_TO]->(other:Entity)
                WHERE e.revenue > $min_revenue
                RETURN e.name as entity, type(r) as relationship, other.name as related
                ORDER BY e.revenue DESC
                LIMIT 5
            """,
      "parameters": {"min_revenue": 1000000},
    }

    complex_response = {
      "columns": ["entity", "relationship", "related"],
      "rows": [
        {"entity": "CompanyA", "relationship": "SUBSIDIARY_OF", "related": "CompanyB"},
        {"entity": "CompanyC", "relationship": "PARTNER_WITH", "related": "CompanyD"},
      ],
      "count": 2,
      "execution_time_ms": 45.0,
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = complex_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=complex_query,
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["count"] == 2
      assert data["columns"] == ["entity", "relationship", "related"]

  def test_execute_query_shared_database(
    self, client, mock_query_request, mock_query_response
  ):
    """Test query execution on shared database (e.g., SEC)."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = mock_query_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/sec/query",  # Using shared SEC database
        json=mock_query_request,
      )

      assert response.status_code == status.HTTP_200_OK
      # Verify the query was routed to SEC database
      called_request = mock_service.execute_query.call_args[0][0]
      assert called_request.database == "sec"

  def test_execute_query_connection_tracking_on_error(self, client, mock_query_request):
    """Test that connections are released even when query fails."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.side_effect = Exception("Query execution failed")

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      with pytest.raises(Exception):
        client.post(
          "/databases/kg1a2b3c4d5/query",
          json=mock_query_request,
        )

      # Connection should still be released despite error
      mock_admission.register_connection.assert_called_once_with("kg1a2b3c4d5")
      mock_admission.release_connection.assert_called_once_with("kg1a2b3c4d5")

  def test_execute_query_large_result_non_streaming(self, client):
    """Test non-streaming query with large result (should respect MAX_ROWS limit)."""
    large_query = {
      "database": "kg1a2b3c4d5",
      "cypher": "MATCH (n) RETURN n",  # No LIMIT clause
      "parameters": {},
    }

    # Response should be limited by internal MAX_ROWS (10000)
    large_response = {
      "columns": ["n"],
      "rows": [{"n": {"id": i}} for i in range(10000)],  # Maximum allowed
      "count": 10000,
      "execution_time_ms": 250.0,
      "truncated": True,  # Indicates more results available
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.databases.query import (
      _get_cluster_service_for_request,
    )

    mock_service = client.app.dependency_overrides[_get_cluster_service_for_request]()
    mock_service.execute_query.return_value = large_response

    with patch(
      "robosystems.graph_api.routers.databases.query.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.check_admission.return_value = (AdmissionDecision.ACCEPT, "OK")
      mock_get_admission.return_value = mock_admission

      response = client.post(
        "/databases/kg1a2b3c4d5/query",
        json=large_query,
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["count"] == 10000
      assert data.get("truncated") is True
