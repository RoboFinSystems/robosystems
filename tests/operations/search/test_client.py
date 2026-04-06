"""Tests for OpenSearch client wrapper."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.search.client import (
  HYBRID_PIPELINE_NAME,
  INDEX_MAPPING,
  OpenSearchClient,
)

DUMMY_EMBEDDING = [0.1] * 384


@pytest.fixture
def mock_opensearch():
  mock_instance = MagicMock()
  with patch("opensearchpy.OpenSearch", return_value=mock_instance):
    yield mock_instance


@pytest.fixture
def client(mock_opensearch):
  c = OpenSearchClient("http://localhost:9200", "test-documents")
  c._client = mock_opensearch
  return c


class TestCreateIndex:
  def test_creates_index_when_not_exists(self, client, mock_opensearch):
    mock_opensearch.indices.exists.return_value = False
    client.create_index_if_not_exists()
    mock_opensearch.indices.create.assert_called_once_with(
      index="test-documents", body=INDEX_MAPPING
    )

  def test_skips_when_exists(self, client, mock_opensearch):
    mock_opensearch.indices.exists.return_value = True
    client.create_index_if_not_exists()
    mock_opensearch.indices.create.assert_not_called()

  def test_creates_hybrid_pipeline(self, client, mock_opensearch):
    mock_opensearch.indices.exists.return_value = True
    client.create_index_if_not_exists()
    mock_opensearch.http.put.assert_called_once()
    call_args = mock_opensearch.http.put.call_args
    assert HYBRID_PIPELINE_NAME in call_args.args[0]


class TestIndexDocument:
  def test_requires_graph_id(self, client):
    with pytest.raises(ValueError, match="graph_id"):
      client.index_document({"content": "test"})

  def test_indexes_with_graph_id(self, client, mock_opensearch):
    doc = {"graph_id": "sec", "document_id": "doc1", "content": "test"}
    client.index_document(doc)
    mock_opensearch.index.assert_called_once()
    call_kwargs = mock_opensearch.index.call_args
    assert call_kwargs.kwargs["id"] == "doc1"
    assert "indexed_at" in call_kwargs.kwargs["body"]


class TestBulkIndex:
  def test_requires_graph_id_on_all_docs(self, client):
    docs = [{"graph_id": "sec"}, {"content": "no graph_id"}]
    with pytest.raises(ValueError, match="graph_id"):
      client.bulk_index(docs)

  @patch("opensearchpy.helpers.bulk")
  def test_bulk_indexes_documents(self, mock_bulk, client):
    mock_bulk.return_value = (5, [])
    docs = [
      {"graph_id": "sec", "document_id": f"doc{i}", "content": f"text {i}"}
      for i in range(5)
    ]
    result = client.bulk_index(docs)
    assert result["indexed"] == 5
    assert result["errors"] == 0

  @patch("opensearchpy.helpers.bulk")
  def test_bulk_index_disables_per_request_refresh(self, mock_bulk, client):
    mock_bulk.return_value = (1, [])
    docs = [{"graph_id": "sec", "document_id": "doc1", "content": "test"}]
    client.bulk_index(docs)
    assert mock_bulk.call_args.kwargs["refresh"] is False

  @patch("opensearchpy.helpers.bulk")
  def test_bulk_index_chunks_large_batches(self, mock_bulk, client):
    mock_bulk.return_value = (500, [])
    docs = [
      {"graph_id": "sec", "document_id": f"doc{i}", "content": f"text {i}"}
      for i in range(1100)
    ]
    result = client.bulk_index(docs, chunk_size=500)
    assert mock_bulk.call_count == 3  # 500 + 500 + 100
    assert result["indexed"] == 1500


class TestBulkWriteMode:
  def test_sets_slow_refresh_during_writes(self, client, mock_opensearch):
    with client.bulk_write_mode():
      mock_opensearch.indices.put_settings.assert_called_once_with(
        index="test-documents",
        body={"index": {"refresh_interval": "60s"}},
      )

  def test_restores_refresh_and_forces_refresh_on_exit(self, client, mock_opensearch):
    with client.bulk_write_mode():
      pass
    mock_opensearch.indices.refresh.assert_called_once_with(index="test-documents")
    # Second put_settings call restores steady interval
    restore_call = mock_opensearch.indices.put_settings.call_args_list[-1]
    assert restore_call.kwargs["body"] == {"index": {"refresh_interval": "30s"}}

  def test_restores_on_exception(self, client, mock_opensearch):
    with pytest.raises(RuntimeError):
      with client.bulk_write_mode():
        raise RuntimeError("ingestion failed")
    mock_opensearch.indices.refresh.assert_called_once()
    assert mock_opensearch.indices.put_settings.call_count == 2

  def test_cleanup_failure_does_not_mask_original_exception(
    self, client, mock_opensearch
  ):
    """If cleanup fails, the original ingestion error must still propagate."""
    mock_opensearch.indices.refresh.side_effect = ConnectionError("cluster down")
    with pytest.raises(RuntimeError, match="ingestion failed"):
      with client.bulk_write_mode():
        raise RuntimeError("ingestion failed")

  def test_custom_intervals(self, client, mock_opensearch):
    with client.bulk_write_mode(write_interval="120s", steady_interval="10s"):
      pass
    first_call = mock_opensearch.indices.put_settings.call_args_list[0]
    assert first_call.kwargs["body"] == {"index": {"refresh_interval": "120s"}}
    last_call = mock_opensearch.indices.put_settings.call_args_list[-1]
    assert last_call.kwargs["body"] == {"index": {"refresh_interval": "10s"}}


class TestSearch:
  """Tests for BM25-only search (default mode)."""

  def test_filters_by_graph_id(self, client, mock_opensearch):
    """Critical security test: query must filter by graph_id."""
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test query", graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    filter_clauses = body["query"]["bool"]["filter"]
    assert filter_clauses[0] == {"term": {"graph_id": "sec"}}

  def test_uses_bm25_not_hybrid(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "bool" in body["query"]
    assert "hybrid" not in body["query"]
    # No search pipeline needed for BM25-only
    assert (
      "params" not in call_args.kwargs
      or "search_pipeline" not in call_args.kwargs.get("params", {})
    )

  def test_no_post_filter(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "post_filter" not in body

  def test_applies_entity_filter(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec", filters={"entity": "NVDA"})

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    filter_clauses = body["query"]["bool"]["filter"]
    assert len(filter_clauses) == 2  # graph_id + entity

  def test_applies_form_type_filter(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec", filters={"form_type": "10-K"})

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    filter_clauses = body["query"]["bool"]["filter"]
    assert {"term": {"form_type": "10-K"}} in filter_clauses

  def test_includes_highlights(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "highlight" in body
    assert "content" in body["highlight"]["fields"]

  def test_excludes_embedding_from_source(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search("test", graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "embedding" in body["_source"]["excludes"]


class TestSearchHybrid:
  """Tests for hybrid BM25 + KNN search (opt-in mode)."""

  def test_bm25_filters_by_graph_id(self, client, mock_opensearch):
    """Critical security test: BM25 sub-query must filter by graph_id."""
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test query", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    bm25_query = body["query"]["hybrid"]["queries"][0]
    filter_clauses = bm25_query["bool"]["filter"]
    assert filter_clauses[0] == {"term": {"graph_id": "sec"}}

  def test_knn_filters_by_graph_id(self, client, mock_opensearch):
    """Critical security test: KNN sub-query must filter by graph_id."""
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test query", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    knn_query = body["query"]["hybrid"]["queries"][1]
    knn_filter = knn_query["knn"]["embedding"]["filter"]
    assert {"term": {"graph_id": "sec"}} in knn_filter["bool"]["filter"]

  def test_no_post_filter(self, client, mock_opensearch):
    """Tenant isolation must be inside sub-queries, not post_filter."""
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "post_filter" not in body

  def test_uses_hybrid_query(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "hybrid" in body["query"]
    queries = body["query"]["hybrid"]["queries"]
    assert len(queries) == 2
    assert "bool" in queries[0]  # BM25 wrapped in bool for filtering
    assert "knn" in queries[1]

  def test_uses_search_pipeline(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    assert call_args.kwargs["params"]["search_pipeline"] == HYBRID_PIPELINE_NAME

  def test_applies_entity_filter_to_both_subqueries(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid(
      "test", DUMMY_EMBEDDING, graph_id="sec", filters={"entity": "NVDA"}
    )

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    # BM25 sub-query has graph_id + entity filters
    bm25_filters = body["query"]["hybrid"]["queries"][0]["bool"]["filter"]
    assert len(bm25_filters) == 2  # graph_id + entity
    # KNN sub-query also has the filters
    knn_filter = body["query"]["hybrid"]["queries"][1]["knn"]["embedding"]["filter"]
    assert len(knn_filter["bool"]["filter"]) == 2

  def test_applies_form_type_filter(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid(
      "test", DUMMY_EMBEDDING, graph_id="sec", filters={"form_type": "10-K"}
    )

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    bm25_filters = body["query"]["hybrid"]["queries"][0]["bool"]["filter"]
    assert {"term": {"form_type": "10-K"}} in bm25_filters

  def test_includes_highlights(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "highlight" in body
    assert "content" in body["highlight"]["fields"]

  def test_excludes_embedding_from_source(self, client, mock_opensearch):
    mock_opensearch.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    client.search_hybrid("test", DUMMY_EMBEDDING, graph_id="sec")

    call_args = mock_opensearch.search.call_args
    body = call_args.kwargs["body"]
    assert "embedding" in body["_source"]["excludes"]


class TestGetDocument:
  def test_returns_document_when_graph_id_matches(self, client, mock_opensearch):
    mock_opensearch.get.return_value = {
      "_source": {"graph_id": "sec", "content": "test"}
    }
    result = client.get_document("doc1", "sec")
    assert result is not None
    assert result["content"] == "test"

  def test_returns_none_when_graph_id_mismatch(self, client, mock_opensearch):
    """Defense in depth: reject documents with wrong graph_id."""
    mock_opensearch.get.return_value = {
      "_source": {"graph_id": "other_graph", "content": "test"}
    }
    result = client.get_document("doc1", "sec")
    assert result is None

  def test_returns_none_when_not_found(self, client, mock_opensearch):
    from opensearchpy import NotFoundError

    mock_opensearch.get.side_effect = NotFoundError(404, "not found")
    result = client.get_document("doc1", "sec")
    assert result is None


class TestHealth:
  def test_returns_cluster_health(self, client, mock_opensearch):
    mock_opensearch.cluster.health.return_value = {"status": "green"}
    result = client.health()
    assert result["status"] == "green"

  def test_returns_unavailable_on_error(self, client, mock_opensearch):
    mock_opensearch.cluster.health.side_effect = ConnectionError("timeout")
    result = client.health()
    assert result["status"] == "unavailable"
