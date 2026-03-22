"""OpenSearch client wrapper with graph_id tenant isolation.

Every query is filtered by graph_id to ensure multi-tenant isolation.
This is a platform-level client — not specific to any adapter.
"""

from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger

# Index mapping definition
INDEX_MAPPING = {
  "mappings": {
    "properties": {
      # Tenant isolation
      "graph_id": {"type": "keyword"},
      # Document identity
      "document_id": {"type": "keyword"},
      "source_type": {
        "type": "keyword"
      },  # "xbrl_textblock", "narrative_section", "ixbrl_disclosure", "uploaded_doc", "memory", "connection_doc"
      # Entity metadata
      "entity_ticker": {"type": "keyword"},
      "entity_cik": {"type": "keyword"},
      "entity_name": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword"}},
      },
      # Section identification
      "element_qname": {"type": "keyword"},  # For textblocks
      "section_id": {"type": "keyword"},  # For narratives: item_1, item_1a, etc.
      "section_label": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword"}},
      },
      # XBRL element metadata (for ixbrl_disclosure source type)
      "xbrl_elements": {"type": "keyword"},  # Element qnames in this section
      "xbrl_element_count": {"type": "integer"},
      # Content
      "content": {"type": "text", "analyzer": "standard"},
      "content_url": {"type": "keyword"},  # CDN URL for full retrieval
      "content_length": {"type": "integer"},
      # Filing metadata
      "filing_date": {"type": "date"},
      "fiscal_year": {"type": "integer"},
      "fiscal_period": {"type": "keyword"},
      "form_type": {"type": "keyword"},
      "accession_number": {"type": "keyword"},
      # Document metadata (uploaded docs, memories, connections)
      "tags": {"type": "keyword"},
      "category": {"type": "keyword"},
      "document_title": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword"}},
      },
      "folder": {"type": "keyword"},
      # Connection-specific
      "source_provider": {"type": "keyword"},
      "source_file_id": {"type": "keyword"},
      "source_file_name": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword"}},
      },
      "last_modified": {"type": "date"},
      # Timestamps
      "indexed_at": {"type": "date"},
      # Embedding (populated by future vector search phase; field reserved now
      # to avoid index recreation later)
      "embedding": {
        "type": "knn_vector",
        "dimension": 384,  # fastembed BAAI/bge-small-en-v1.5
        "method": {
          "name": "hnsw",
          "space_type": "cosinesimil",
          "engine": "nmslib",
        },
      },
      "embedding_model": {"type": "keyword"},  # "fastembed" or "bedrock"
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,  # Single node for dev; production will differ
    "index.knn": True,
  },
}


class OpenSearchClient:
  """OpenSearch client with mandatory graph_id filtering on all operations."""

  def __init__(self, url: str, index_name: str) -> None:
    self.url = url
    self.index_name = index_name
    self._client = None

  @property
  def client(self):
    """Lazy-initialize the OpenSearch client.

    Detects AWS managed domains by URL pattern and uses SigV4 auth automatically.
    Local/Docker URLs use direct connection with no auth.
    """
    if self._client is None:
      from opensearchpy import OpenSearch

      is_aws = ".es.amazonaws.com" in self.url or ".aoss.amazonaws.com" in self.url

      if is_aws:
        from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

        from robosystems.config import env

        session = __import__("boto3").Session()
        credentials = session.get_credentials()
        service = "aoss" if ".aoss." in self.url else "es"
        auth = AWSV4SignerAuth(credentials, env.AWS_REGION, service)

        # Parse host from URL (opensearch-py wants host without scheme)
        host = self.url.replace("https://", "").replace("http://", "").rstrip("/")
        self._client = OpenSearch(
          hosts=[{"host": host, "port": 443}],
          http_auth=auth,
          use_ssl=True,
          verify_certs=True,
          connection_class=RequestsHttpConnection,
          timeout=30,
          max_retries=3,
          retry_on_timeout=True,
        )
      else:
        self._client = OpenSearch(
          hosts=[self.url],
          use_ssl=self.url.startswith("https"),
          verify_certs=False,
          timeout=30,
          max_retries=3,
          retry_on_timeout=True,
        )
    return self._client

  def create_index_if_not_exists(self) -> None:
    """Create the index with mapping if it doesn't exist."""
    try:
      if not self.client.indices.exists(index=self.index_name):
        self.client.indices.create(index=self.index_name, body=INDEX_MAPPING)
        logger.info(f"Created OpenSearch index: {self.index_name}")
      else:
        logger.debug(f"OpenSearch index already exists: {self.index_name}")
    except Exception as e:
      logger.error(f"Failed to create OpenSearch index: {e}")
      raise

  def index_document(self, document: dict[str, Any]) -> None:
    """Index a single document. Requires graph_id field."""
    if "graph_id" not in document:
      raise ValueError("Document must contain graph_id field")

    document["indexed_at"] = datetime.now(UTC).isoformat()
    doc_id = document.get("document_id")

    self.client.index(
      index=self.index_name,
      id=doc_id,
      body=document,
    )

  def bulk_index(
    self, documents: list[dict[str, Any]], chunk_size: int = 500
  ) -> dict[str, int]:
    """Bulk index documents. All must contain graph_id."""
    from opensearchpy.helpers import bulk

    now = datetime.now(UTC).isoformat()
    actions = []

    for doc in documents:
      if "graph_id" not in doc:
        raise ValueError("All documents must contain graph_id field")
      doc["indexed_at"] = now
      action = {
        "_index": self.index_name,
        "_source": doc,
      }
      if "document_id" in doc:
        action["_id"] = doc["document_id"]
      actions.append(action)

    total_indexed = 0
    total_errors = 0

    for i in range(0, len(actions), chunk_size):
      chunk = actions[i : i + chunk_size]
      success_count, errors = bulk(self.client, chunk, raise_on_error=False)
      total_indexed += success_count
      if errors:
        total_errors += len(errors)
        for err in errors[:5]:
          logger.warning(f"Bulk index error: {err}")

    logger.info(
      f"Bulk indexed {total_indexed} documents ({total_errors} errors) "
      f"into {self.index_name}"
    )
    return {"indexed": total_indexed, "errors": total_errors}

  def _build_filter_clauses(
    self, graph_id: str, filters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Build OpenSearch filter clauses with mandatory graph_id tenant isolation."""
    filter_clauses: list[dict[str, Any]] = [
      {"term": {"graph_id": graph_id}},
    ]

    if filters:
      if filters.get("entity"):
        entity = filters["entity"]
        filter_clauses.append(
          {
            "bool": {
              "should": [
                {"term": {"entity_ticker": entity.upper()}},
                {"term": {"entity_cik": entity}},
                {"match": {"entity_name": entity}},
              ],
              "minimum_should_match": 1,
            }
          }
        )
      if filters.get("form_type"):
        filter_clauses.append({"term": {"form_type": filters["form_type"].upper()}})
      if filters.get("section"):
        filter_clauses.append({"term": {"section_id": filters["section"].lower()}})
      if filters.get("fiscal_year"):
        filter_clauses.append({"term": {"fiscal_year": filters["fiscal_year"]}})
      if filters.get("element"):
        filter_clauses.append({"term": {"xbrl_elements": filters["element"]}})
      if filters.get("source_type"):
        filter_clauses.append({"term": {"source_type": filters["source_type"]}})
      if filters.get("date_from"):
        filter_clauses.append({"range": {"filing_date": {"gte": filters["date_from"]}}})
      if filters.get("date_to"):
        filter_clauses.append({"range": {"filing_date": {"lte": filters["date_to"]}}})
      if filters.get("tags"):
        filter_clauses.append({"terms": {"tags": filters["tags"]}})
      if filters.get("folder"):
        filter_clauses.append({"term": {"folder": filters["folder"]}})
      if filters.get("category"):
        filter_clauses.append({"term": {"category": filters["category"]}})

    return filter_clauses

  @staticmethod
  def _highlight_config() -> dict[str, Any]:
    """Standard highlight configuration for search results."""
    return {
      "fields": {
        "content": {
          "fragment_size": 200,
          "number_of_fragments": 3,
          "pre_tags": [""],
          "post_tags": [""],
        }
      }
    }

  def search(
    self,
    query: str,
    graph_id: str,
    filters: dict[str, Any] | None = None,
    size: int = 10,
    offset: int = 0,
  ) -> dict[str, Any]:
    """Search documents with mandatory graph_id filtering.

    Args:
        query: Search query string
        graph_id: Required tenant filter
        filters: Optional additional filters (entity, form_type, section, fiscal_year, etc.)
        size: Max results to return
        offset: Pagination offset

    Returns:
        OpenSearch response with hits and highlights
    """
    filter_clauses = self._build_filter_clauses(graph_id, filters)

    search_body: dict[str, Any] = {
      "query": {
        "bool": {
          "must": [
            {
              "multi_match": {
                "query": query,
                "fields": [
                  "content",
                  "section_label^2",
                  "entity_name^1.5",
                ],
                "type": "best_fields",
              }
            }
          ],
          "filter": filter_clauses,
        }
      },
      "highlight": self._highlight_config(),
      "size": size,
      "from": offset,
      "_source": {
        "excludes": ["content"],
      },
    }

    return self.client.search(index=self.index_name, body=search_body)

  def hybrid_search(
    self,
    query: str,
    query_embedding: list[float],
    graph_id: str,
    filters: dict[str, Any] | None = None,
    size: int = 10,
    offset: int = 0,
  ) -> dict[str, Any]:
    """Hybrid text + vector search with mandatory graph_id filtering.

    Combines BM25 text matching with KNN vector similarity by nesting both
    inside a bool.should clause. Documents matching either text or vector
    (or both) are returned, ranked by combined score.

    Note on pagination: KNN does not support offset-based pagination natively.
    When offset > 0, we over-fetch (size + offset) results and let OpenSearch
    handle the windowing. This is fine for typical result sets but becomes
    inefficient at very high offsets.

    Args:
        query: Search query string
        query_embedding: 384-dim embedding of the query from fastembed
        graph_id: Required tenant filter
        filters: Optional additional filters
        size: Max results to return
        offset: Pagination offset

    Returns:
        OpenSearch response with hits and highlights
    """
    filter_clauses = self._build_filter_clauses(graph_id, filters)

    # Over-fetch for KNN to support offset pagination
    knn_k = min(size + offset, 100)  # Cap at 100 to limit KNN cost

    search_body: dict[str, Any] = {
      "query": {
        "bool": {
          "should": [
            {
              "multi_match": {
                "query": query,
                "fields": [
                  "content",
                  "section_label^2",
                  "entity_name^1.5",
                ],
                "type": "best_fields",
              }
            },
            {
              "knn": {
                "embedding": {
                  "vector": query_embedding,
                  "k": knn_k,
                }
              }
            },
          ],
          "filter": filter_clauses,
        }
      },
      "highlight": self._highlight_config(),
      "size": size,
      "from": offset,
      "_source": {
        "excludes": ["content", "embedding"],
      },
    }

    return self.client.search(index=self.index_name, body=search_body)

  def get_document(self, document_id: str, graph_id: str) -> dict[str, Any] | None:
    """Get a document by ID, with graph_id verification for tenant isolation."""
    try:
      result = self.client.get(index=self.index_name, id=document_id)
      source = result.get("_source", {})

      # Verify graph_id matches — defense in depth
      if source.get("graph_id") != graph_id:
        logger.warning(
          f"graph_id mismatch on document {document_id}: "
          f"expected {graph_id}, got {source.get('graph_id')}"
        )
        return None

      return source
    except Exception as e:
      if "NotFoundError" in type(e).__name__:
        return None
      logger.error(f"Error fetching document {document_id}: {e}")
      raise

  def delete_by_graph_id(self, graph_id: str) -> int:
    """Delete all documents for a graph_id. Used for graph cleanup."""
    result = self.client.delete_by_query(
      index=self.index_name,
      body={"query": {"term": {"graph_id": graph_id}}},
    )
    deleted = result.get("deleted", 0)
    logger.info(f"Deleted {deleted} documents for graph_id={graph_id}")
    return deleted

  def delete_document(self, document_id: str, graph_id: str) -> bool:
    """Delete a single document by ID with graph_id verification."""
    doc = self.get_document(document_id, graph_id)
    if doc is None:
      return False
    try:
      self.client.delete(index=self.index_name, id=document_id)
      return True
    except Exception as e:
      logger.error(f"Error deleting document {document_id}: {e}")
      raise

  def delete_by_document_prefix(self, graph_id: str, prefix: str) -> int:
    """Delete all documents matching a document_id prefix within a graph.

    Used to remove all sections of a document during re-upload.
    """
    result = self.client.delete_by_query(
      index=self.index_name,
      body={
        "query": {
          "bool": {
            "filter": [
              {"term": {"graph_id": graph_id}},
              {"prefix": {"document_id": prefix}},
            ]
          }
        }
      },
    )
    deleted = result.get("deleted", 0)
    logger.info(
      f"Deleted {deleted} documents with prefix={prefix} for graph_id={graph_id}"
    )
    return deleted

  def count_by_source_type(self, graph_id: str, source_type: str) -> int:
    """Count documents matching graph_id and source_type."""
    result = self.client.count(
      index=self.index_name,
      body={
        "query": {
          "bool": {
            "filter": [
              {"term": {"graph_id": graph_id}},
              {"term": {"source_type": source_type}},
            ]
          }
        }
      },
    )
    return result.get("count", 0)

  def knn_search(
    self,
    query_embedding: list[float],
    graph_id: str,
    filters: dict[str, Any] | None = None,
    size: int = 10,
  ) -> dict[str, Any]:
    """Pure kNN vector search with mandatory graph_id filtering.

    Unlike hybrid_search, this uses ONLY vector similarity (no BM25).
    Used by recall-text for semantic memory retrieval.
    """
    filter_clauses = self._build_filter_clauses(graph_id, filters)

    search_body: dict[str, Any] = {
      "query": {
        "knn": {
          "embedding": {
            "vector": query_embedding,
            "k": min(size, 100),
            "filter": {"bool": {"filter": filter_clauses}},
          }
        }
      },
      "size": size,
      "_source": {
        "excludes": ["embedding"],
      },
    }

    return self.client.search(index=self.index_name, body=search_body)

  def list_documents(
    self,
    graph_id: str,
    source_type: str | None = None,
    size: int = 100,
  ) -> dict[str, Any]:
    """List unique documents by title using terms aggregation.

    Returns document titles with section counts, grouped by source_type.
    """
    filter_clauses: list[dict[str, Any]] = [{"term": {"graph_id": graph_id}}]
    if source_type:
      filter_clauses.append({"term": {"source_type": source_type}})

    search_body: dict[str, Any] = {
      "size": 0,
      "query": {"bool": {"filter": filter_clauses}},
      "aggs": {
        "documents": {
          "terms": {
            "field": "document_title.keyword",
            "size": size,
            "order": {"_key": "asc"},
          },
          "aggs": {
            "source_type": {"terms": {"field": "source_type", "size": 10}},
            "folder": {"terms": {"field": "folder", "size": 1}},
            "tags": {"terms": {"field": "tags", "size": 20}},
            "last_indexed": {"max": {"field": "indexed_at"}},
          },
        },
      },
    }

    return self.client.search(index=self.index_name, body=search_body)

  def health(self) -> dict[str, Any]:
    """Check OpenSearch cluster health."""
    try:
      return self.client.cluster.health()
    except Exception as e:
      return {"status": "unavailable", "error": str(e)}
