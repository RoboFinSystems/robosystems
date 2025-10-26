# pyright: basic
"""
End-to-End Data Ingestion Tests.

Tests the complete workflow from user creation through data ingestion and querying:
1. User registration and authentication
2. Graph creation (async operation with SSE)
3. Parquet file upload to S3
4. DuckDB staging table creation
5. Data ingestion into Kuzu graph
6. Cypher query execution

This validates the entire platform stack working together in a realistic scenario.

IMPORTANT: These tests require a FULL running API server at localhost:8000
(not just database access). They make real HTTP requests to test the complete
end-to-end workflow.

Prerequisites:
  - Docker stack running: just start robosystems
  - All services healthy: API, workers, PostgreSQL, Valkey, S3, Graph API

To run:
  just test-e2e
  OR: uv run pytest -m e2e
"""

import pytest
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@pytest.mark.e2e
@pytest.mark.integration
@pytest.mark.slow
class TestE2EDataIngestion:
  """Test complete end-to-end data ingestion workflow."""

  def test_complete_workflow_entity_data(
    self,
    integration_client,
    test_user_with_api_key,
    sample_parquet_file,
    cleanup_graphs,
  ):
    """
    Test complete workflow: user → graph → upload → staging → ingest → query.

    This is the golden path that validates the entire platform works correctly.
    """
    user = test_user_with_api_key
    file_path, expected_rows = sample_parquet_file

    graph_data = {
      "metadata": {
        "graph_name": f"e2e_test_graph_{int(time.time())}",
        "description": "E2E integration test graph",
      }
    }

    create_response = integration_client.post(
      "/v1/graphs",
      headers=user["headers"],
      json=graph_data,
    )
    assert create_response.status_code in [200, 201, 202]

    response_data = create_response.json()
    graph_id = response_data.get("graph_id")

    if not graph_id:
      operation_id = response_data.get("operation_id")
      assert operation_id, "Expected either graph_id or operation_id"

      max_wait = 60
      start_time = time.time()

      while time.time() - start_time < max_wait:
        status_response = integration_client.get(
          f"/v1/operations/{operation_id}/status",
          headers=user["headers"],
        )
        if status_response.status_code == 200:
          status_data = status_response.json()
          if status_data.get("status") == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              break
          elif status_data.get("status") == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
        time.sleep(2)

      assert graph_id, "Graph creation timed out (checked database directly)"

    cleanup_graphs.append(graph_id)

    upload_request = {
      "file_name": file_path.name,
      "content_type": "application/x-parquet",
    }

    upload_url_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/Entity/files",
      headers=user["headers"],
      json=upload_request,
    )
    assert upload_url_response.status_code == 200
    upload_data = upload_url_response.json()

    upload_url = upload_data["upload_url"]
    file_id = upload_data["file_id"]

    if "localstack:4566" in upload_url:
      upload_url = upload_url.replace("localstack:4566", "localhost:4566")

    with open(file_path, "rb") as f:
      file_content = f.read()
      file_size = len(file_content)

      import httpx

      s3_client = httpx.Client(timeout=30.0)
      s3_response = s3_client.put(
        upload_url,
        content=file_content,
        headers={"Content-Type": "application/x-parquet"},
      )
      assert s3_response.status_code in [200, 204]

    metadata_update = {
      "file_size_bytes": file_size,
      "row_count": expected_rows,
    }

    metadata_response = integration_client.patch(
      f"/v1/graphs/{graph_id}/tables/files/{file_id}",
      headers=user["headers"],
      json=metadata_update,
    )
    assert metadata_response.status_code == 200

    tables_response = integration_client.get(
      f"/v1/graphs/{graph_id}/tables",
      headers=user["headers"],
    )
    assert tables_response.status_code == 200

    tables_data = tables_response.json()
    assert len(tables_data["tables"]) == 1

    entity_table = tables_data["tables"][0]
    assert entity_table["table_name"] == "Entity"
    assert entity_table["row_count"] == expected_rows
    assert entity_table["file_count"] == 1
    assert entity_table["total_size_bytes"] == file_size

    ingest_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/ingest",
      headers=user["headers"],
      json={"ignore_errors": False, "rebuild": False},
    )
    assert ingest_response.status_code == 200

    ingest_data = ingest_response.json()
    assert ingest_data["status"] in ["success", "partial"]
    assert ingest_data["successful_tables"] >= 1
    assert ingest_data["total_rows_ingested"] == expected_rows

    count_query = {"query": "MATCH (n:Entity) RETURN count(n) AS total_nodes"}
    query_response = integration_client.post(
      f"/v1/graphs/{graph_id}/query?mode=sync",
      headers=user["headers"],
      json=count_query,
    )
    assert query_response.status_code == 200

    count_data = query_response.json()
    assert count_data["success"] is True
    assert len(count_data["data"]) == 1
    assert count_data["data"][0]["total_nodes"] == expected_rows

    property_query = {
      "query": "MATCH (n:Entity) RETURN n.identifier, n.name, n.industry LIMIT 5"
    }
    query_response = integration_client.post(
      f"/v1/graphs/{graph_id}/query?mode=sync",
      headers=user["headers"],
      json=property_query,
    )
    assert query_response.status_code == 200

    property_data = query_response.json()
    assert len(property_data["data"]) == 5

    for record in property_data["data"]:
      assert "n.identifier" in record
      assert record["n.identifier"].startswith("test_entity_")
      assert "n.name" in record
      assert "n.industry" in record
      assert record["n.industry"] == "Technology"

    filter_query = {
      "query": "MATCH (n:Entity) WHERE n.ticker IS NOT NULL RETURN n.ticker, n.name ORDER BY n.ticker"
    }
    query_response = integration_client.post(
      f"/v1/graphs/{graph_id}/query?mode=sync",
      headers=user["headers"],
      json=filter_query,
    )
    assert query_response.status_code == 200

    filter_data = query_response.json()
    assert len(filter_data["data"]) == 5

    for record in filter_data["data"]:
      assert record["n.ticker"].startswith("TEST")

  def test_multiple_file_uploads_same_table(
    self,
    integration_client,
    test_user_with_api_key,
    tmp_path,
    cleanup_graphs,
  ):
    """Test uploading multiple parquet files to the same table."""
    user = test_user_with_api_key

    graph_data = {
      "metadata": {
        "graph_name": f"multi_file_test_{int(time.time())}",
        "description": "Multi-file upload test",
      }
    }

    create_response = integration_client.post(
      "/v1/graphs",
      headers=user["headers"],
      json=graph_data,
    )
    assert create_response.status_code in [200, 201, 202]

    response_data = create_response.json()
    graph_id = response_data.get("graph_id") or response_data.get("operation_id")

    if "operation_id" in response_data:
      operation_id = response_data.get("operation_id")
      max_wait = 60
      start_time = time.time()

      while time.time() - start_time < max_wait:
        status_response = integration_client.get(
          f"/v1/operations/{operation_id}/status",
          headers=user["headers"],
        )
        if status_response.status_code == 200:
          status_data = status_response.json()
          if status_data.get("status") == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              break
          elif status_data.get("status") == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
        time.sleep(2)

      assert graph_id, "Graph creation timed out"

    cleanup_graphs.append(graph_id)

    file_ranges = [(1, 20), (21, 40), (41, 60)]
    uploaded_files = []

    for start, end in file_ranges:
      num_rows = end - start + 1

      df = pd.DataFrame(
        {
          "identifier": [f"entity_{i:03d}" for i in range(start, end + 1)],
          "name": [f"Entity {i}" for i in range(start, end + 1)],
          "uri": [None] * num_rows,
          "scheme": [None] * num_rows,
          "cik": [None] * num_rows,
          "ticker": [None] * num_rows,
          "exchange": [None] * num_rows,
          "legal_name": [None] * num_rows,
          "industry": [None] * num_rows,
          "entity_type": [None] * num_rows,
          "sic": [None] * num_rows,
          "sic_description": [None] * num_rows,
          "category": [None] * num_rows,
          "state_of_incorporation": [None] * num_rows,
          "fiscal_year_end": [None] * num_rows,
          "ein": [None] * num_rows,
          "tax_id": [None] * num_rows,
          "lei": [None] * num_rows,
          "phone": [None] * num_rows,
          "website": [None] * num_rows,
          "status": [None] * num_rows,
          "is_parent": pd.Series([None] * num_rows, dtype="boolean"),
          "parent_entity_id": [None] * num_rows,
          "created_at": pd.date_range(
            "2025-01-01", periods=num_rows, freq="h"
          ).strftime("%Y-%m-%d %H:%M:%S"),
          "updated_at": [None] * num_rows,
        }
      )

      file_path = tmp_path / f"entities_{start}_{end}.parquet"
      table = pa.Table.from_pandas(df)
      pq.write_table(table, file_path)

      upload_request = {
        "file_name": file_path.name,
        "content_type": "application/x-parquet",
      }

      upload_url_response = integration_client.post(
        f"/v1/graphs/{graph_id}/tables/Entity/files",
        headers=user["headers"],
        json=upload_request,
      )
      assert upload_url_response.status_code == 200

      uploaded_files.append(upload_url_response.json()["file_id"])

    tables_response = integration_client.get(
      f"/v1/graphs/{graph_id}/tables",
      headers=user["headers"],
    )
    assert tables_response.status_code == 200

    tables_data = tables_response.json()
    entity_table = next(
      (t for t in tables_data["tables"] if t["table_name"] == "Entity"), None
    )
    assert entity_table is not None

  def test_invalid_schema_rejection(
    self,
    integration_client,
    test_user_with_api_key,
    tmp_path,
    cleanup_graphs,
  ):
    """Test that ingestion fails gracefully with schema mismatch."""
    user = test_user_with_api_key

    graph_data = {
      "metadata": {
        "graph_name": f"invalid_schema_test_{int(time.time())}",
        "description": "Invalid schema test",
      }
    }

    create_response = integration_client.post(
      "/v1/graphs",
      headers=user["headers"],
      json=graph_data,
    )
    assert create_response.status_code in [200, 201, 202]

    response_data = create_response.json()
    graph_id = response_data.get("graph_id")

    if not graph_id:
      operation_id = response_data.get("operation_id")
      assert operation_id, "Expected either graph_id or operation_id"

      max_wait = 60
      start_time = time.time()

      while time.time() - start_time < max_wait:
        status_response = integration_client.get(
          f"/v1/operations/{operation_id}/status",
          headers=user["headers"],
        )
        if status_response.status_code == 200:
          status_data = status_response.json()
          if status_data.get("status") == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              break
          elif status_data.get("status") == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
        time.sleep(2)

      assert graph_id, "Graph creation timed out"

    cleanup_graphs.append(graph_id)

    df = pd.DataFrame(
      {
        "wrong_field": ["value1", "value2", "value3"],
        "another_wrong": [1, 2, 3],
      }
    )

    file_path = tmp_path / "invalid.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    upload_request = {
      "file_name": file_path.name,
      "content_type": "application/x-parquet",
    }

    upload_url_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/Entity/files",
      headers=user["headers"],
      json=upload_request,
    )

    assert upload_url_response.status_code == 200
    upload_data = upload_url_response.json()

    upload_url = upload_data["upload_url"]
    file_id = upload_data["file_id"]

    if "localstack:4566" in upload_url:
      upload_url = upload_url.replace("localstack:4566", "localhost:4566")

    with open(file_path, "rb") as f:
      file_content = f.read()
      file_size = len(file_content)

      import httpx

      s3_client = httpx.Client(timeout=30.0)
      s3_response = s3_client.put(
        upload_url,
        content=file_content,
        headers={"Content-Type": "application/x-parquet"},
      )
      assert s3_response.status_code in [200, 204]

    metadata_update = {
      "file_size_bytes": file_size,
      "row_count": 3,
    }

    metadata_response = integration_client.patch(
      f"/v1/graphs/{graph_id}/tables/files/{file_id}",
      headers=user["headers"],
      json=metadata_update,
    )
    assert metadata_response.status_code == 200

    ingest_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/ingest",
      headers=user["headers"],
      json={"ignore_errors": False, "rebuild": False},
    )

    ingest_data = ingest_response.json()

    if ingest_response.status_code == 500:
      assert "detail" in ingest_data
    else:
      assert ingest_data["status"] in ["failed", "partial"]
      assert ingest_data["failed_tables"] >= 1


@pytest.mark.e2e
@pytest.mark.integration
class TestE2EEdgeCases:
  """Test edge cases in the E2E workflow."""

  def test_empty_file_upload(
    self,
    integration_client,
    test_user_with_api_key,
    tmp_path,
    cleanup_graphs,
  ):
    """Test handling of empty parquet files."""
    user = test_user_with_api_key

    graph_data = {
      "metadata": {
        "graph_name": f"empty_file_test_{int(time.time())}",
        "description": "Empty file test",
      }
    }

    create_response = integration_client.post(
      "/v1/graphs",
      headers=user["headers"],
      json=graph_data,
    )

    response_data = create_response.json()
    graph_id = response_data.get("graph_id")

    if not graph_id:
      operation_id = response_data.get("operation_id")
      assert operation_id, "Expected either graph_id or operation_id"

      max_wait = 60
      start_time = time.time()

      while time.time() - start_time < max_wait:
        status_response = integration_client.get(
          f"/v1/operations/{operation_id}/status",
          headers=user["headers"],
        )
        if status_response.status_code == 200:
          status_data = status_response.json()
          if status_data.get("status") == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              break
          elif status_data.get("status") == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
        time.sleep(2)

      assert graph_id, "Graph creation timed out"

    cleanup_graphs.append(graph_id)

    df = pd.DataFrame(
      {
        "identifier": [],
        "name": [],
        "uri": [],
        "scheme": [],
        "cik": [],
        "ticker": [],
        "exchange": [],
        "legal_name": [],
        "industry": [],
        "entity_type": [],
        "sic": [],
        "sic_description": [],
        "category": [],
        "state_of_incorporation": [],
        "fiscal_year_end": [],
        "ein": [],
        "tax_id": [],
        "lei": [],
        "phone": [],
        "website": [],
        "status": [],
        "is_parent": pd.Series([], dtype="boolean"),
        "parent_entity_id": [],
        "created_at": [],
        "updated_at": [],
      }
    )

    file_path = tmp_path / "empty.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    upload_request = {
      "file_name": file_path.name,
      "content_type": "application/x-parquet",
    }

    upload_url_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/Entity/files",
      headers=user["headers"],
      json=upload_request,
    )
    assert upload_url_response.status_code == 200

  def test_concurrent_query_execution(
    self,
    integration_client,
    test_user_with_api_key,
    sample_parquet_file,
    cleanup_graphs,
  ):
    """Test that multiple queries can run concurrently on same graph."""
    user = test_user_with_api_key
    file_path, expected_rows = sample_parquet_file

    graph_data = {
      "metadata": {
        "graph_name": f"concurrent_test_{int(time.time())}",
        "description": "Concurrent query test",
      }
    }

    create_response = integration_client.post(
      "/v1/graphs",
      headers=user["headers"],
      json=graph_data,
    )
    assert create_response.status_code in [200, 201, 202]

    response_data = create_response.json()
    graph_id = response_data.get("graph_id")

    if not graph_id:
      operation_id = response_data.get("operation_id")
      assert operation_id, "Expected either graph_id or operation_id"

      max_wait = 60
      start_time = time.time()

      while time.time() - start_time < max_wait:
        status_response = integration_client.get(
          f"/v1/operations/{operation_id}/status",
          headers=user["headers"],
        )
        if status_response.status_code == 200:
          status_data = status_response.json()
          if status_data.get("status") == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              break
          elif status_data.get("status") == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
        time.sleep(2)

      assert graph_id, "Graph creation timed out"

    cleanup_graphs.append(graph_id)

    upload_request = {
      "file_name": file_path.name,
      "content_type": "application/x-parquet",
    }

    upload_url_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/Entity/files",
      headers=user["headers"],
      json=upload_request,
    )
    assert upload_url_response.status_code == 200
    upload_data = upload_url_response.json()

    upload_url = upload_data["upload_url"]
    file_id = upload_data["file_id"]

    if "localstack:4566" in upload_url:
      upload_url = upload_url.replace("localstack:4566", "localhost:4566")

    with open(file_path, "rb") as f:
      file_content = f.read()
      file_size = len(file_content)

      import httpx

      s3_client = httpx.Client(timeout=30.0)
      s3_response = s3_client.put(
        upload_url,
        content=file_content,
        headers={"Content-Type": "application/x-parquet"},
      )
      assert s3_response.status_code in [200, 204]

    metadata_update = {
      "file_size_bytes": file_size,
      "row_count": expected_rows,
    }

    metadata_response = integration_client.patch(
      f"/v1/graphs/{graph_id}/tables/files/{file_id}",
      headers=user["headers"],
      json=metadata_update,
    )
    assert metadata_response.status_code == 200

    ingest_response = integration_client.post(
      f"/v1/graphs/{graph_id}/tables/ingest",
      headers=user["headers"],
      json={"ignore_errors": False, "rebuild": False},
    )
    assert ingest_response.status_code == 200

    queries = [
      "MATCH (n:Entity) RETURN count(n) AS total_nodes",
      "MATCH (n:Entity) RETURN n.identifier, n.name LIMIT 10",
      "MATCH (n:Entity) WHERE n.ticker IS NOT NULL RETURN n.ticker, n.name ORDER BY n.ticker",
      "MATCH (n:Entity) RETURN n.category LIMIT 5",
      "MATCH (n:Entity) WHERE n.industry = 'Technology' RETURN count(n) AS tech_count",
    ]

    import concurrent.futures
    import threading

    results = []
    errors = []
    lock = threading.Lock()

    def execute_query(query_str):
      try:
        query_response = integration_client.post(
          f"/v1/graphs/{graph_id}/query?mode=sync",
          headers=user["headers"],
          json={"query": query_str},
        )
        with lock:
          results.append(
            {
              "query": query_str,
              "status_code": query_response.status_code,
              "data": query_response.json()
              if query_response.status_code == 200
              else None,
            }
          )
      except Exception as e:
        with lock:
          errors.append({"query": query_str, "error": str(e)})

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
      futures = [executor.submit(execute_query, q) for q in queries]
      concurrent.futures.wait(futures)

    assert len(errors) == 0, f"Errors occurred during concurrent execution: {errors}"
    assert len(results) == len(queries), "Not all queries completed"

    for result in results:
      assert result["status_code"] == 200, f"Query failed: {result['query']}"
      assert result["data"]["success"] is True, f"Query unsuccessful: {result['query']}"
      assert "data" in result["data"], f"No data in response: {result['query']}"

    count_result = next(r for r in results if "total_nodes" in r["data"]["data"][0])
    assert count_result["data"]["data"][0]["total_nodes"] == expected_rows
