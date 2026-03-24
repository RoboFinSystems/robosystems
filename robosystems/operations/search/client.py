"""OpenSearch client wrapper with graph_id tenant isolation.

Every query is filtered by graph_id to ensure multi-tenant isolation.
This is a platform-level client — not specific to any adapter.
"""

from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger

# Search pipeline for hybrid (BM25 + KNN) score normalization.
# Without this, BM25 scores (~10-40) drown out KNN cosine scores (~0-1).
# The normalization processor maps both to [0,1] before combining.
HYBRID_PIPELINE_NAME = "hybrid-search-pipeline"
HYBRID_PIPELINE_BODY: dict[str, Any] = {
  "description": "Normalizes and combines BM25 + KNN scores for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "min_max",
        },
        "combination": {
          "technique": "arithmetic_mean",
          "parameters": {
            # [BM25 weight, KNN weight] — slightly favor semantic relevance
            "weights": [0.4, 0.6],
          },
        },
      }
    }
  ],
}

# Index mapping definition — compatible with both managed OpenSearch and Serverless (AOSS).
# Serverless requires faiss engine (nmslib not available). For normalized embeddings
# (bge-small-en-v1.5), innerproduct is equivalent to cosine similarity.
INDEX_MAPPING = {
  "mappings": {
    "properties": {
      # Tenant isolation
      "graph_id": {"type": "keyword"},
      # Document identity
      "document_id": {"type": "keyword"},
      "source_type": {
        "type": "keyword"
      },  # "xbrl_textblock", "narrative_section", or "ixbrl_disclosure"
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
      # Timestamps
      "indexed_at": {"type": "date"},
      # Embedding — faiss engine for Serverless compatibility
      "embedding": {
        "type": "knn_vector",
        "dimension": 384,  # fastembed BAAI/bge-small-en-v1.5
        "method": {
          "name": "hnsw",
          "space_type": "innerproduct",
          "engine": "faiss",
        },
      },
      "embedding_model": {"type": "keyword"},  # "fastembed" or "bedrock"
    }
  },
  "settings": {
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
        import boto3
        from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

        from robosystems.config import env

        session = boto3.Session()
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

  @property
  def is_serverless(self) -> bool:
    """True if connected to OpenSearch Serverless (AOSS)."""
    return ".aoss." in self.url

  def create_index_if_not_exists(self) -> None:
    """Create the index with mapping if it doesn't exist."""
    try:
      if self.is_serverless:
        # AOSS: indices.exists() may not work reliably; try-create instead
        from opensearchpy.exceptions import RequestError

        try:
          self.client.indices.create(index=self.index_name, body=INDEX_MAPPING)
          logger.info(f"Created AOSS index: {self.index_name}")
        except RequestError as e:
          if e.error == "resource_already_exists_exception":
            logger.debug(f"AOSS index already exists: {self.index_name}")
          else:
            raise
      else:
        if not self.client.indices.exists(index=self.index_name):
          self.client.indices.create(index=self.index_name, body=INDEX_MAPPING)
          logger.info(f"Created OpenSearch index: {self.index_name}")
        else:
          logger.debug(f"OpenSearch index already exists: {self.index_name}")
    except Exception as e:
      logger.error(f"Failed to create OpenSearch index: {e}")
      raise

    self._create_hybrid_pipeline()

  def _create_hybrid_pipeline(self) -> None:
    """Create or update the hybrid search pipeline for score normalization.

    This pipeline normalizes BM25 and KNN scores to the same scale before
    combining them. Without it, BM25 scores (~10-40) dominate KNN cosine
    similarity scores (~0-1), making the vector component negligible.
    """
    try:
      self.client.http.put(
        f"/_search/pipeline/{HYBRID_PIPELINE_NAME}",
        body=HYBRID_PIPELINE_BODY,
      )
      logger.info(f"Created/updated hybrid search pipeline: {HYBRID_PIPELINE_NAME}")
    except Exception as e:
      logger.warning(f"Failed to create hybrid search pipeline: {e}")

  def index_document(self, document: dict[str, Any]) -> None:
    """Index a single document. Requires graph_id field."""
    if "graph_id" not in document:
      raise ValueError("Document must contain graph_id field")

    document["indexed_at"] = datetime.now(UTC).isoformat()
    # AOSS VECTOR_SEARCH collections don't support custom _id on index.
    # document_id is stored as a field for retrieval; OS generates the _id.
    doc_id = document.get("document_id") if not self.is_serverless else None

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
      action: dict[str, Any] = {
        "_index": self.index_name,
        "_source": doc,
      }
      # AOSS VECTOR_SEARCH collections don't support custom _id on index
      if not self.is_serverless and "document_id" in doc:
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

    Uses OpenSearch's native hybrid query type with a normalization search
    pipeline. The pipeline normalizes BM25 and KNN scores to [0,1] via
    min_max, then combines them with weighted arithmetic mean (0.4 BM25,
    0.6 KNN). This ensures vector similarity actually influences ranking
    instead of being drowned out by raw BM25 scores.

    Filters are applied via post_filter (OpenSearch 2.x and Serverless
    don't support filters inside hybrid queries — that's OpenSearch 3.0+).

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

    # The hybrid query's queries array is positional — index 0 maps to
    # weight 0 (BM25=0.4) and index 1 maps to weight 1 (KNN=0.6) in
    # the normalization pipeline.
    search_body: dict[str, Any] = {
      "query": {
        "hybrid": {
          "queries": [
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
        }
      },
      # Filters via post_filter (hybrid.filter is OpenSearch 3.0+ only)
      "post_filter": {
        "bool": {
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

    return self.client.search(
      index=self.index_name,
      body=search_body,
      params={"search_pipeline": HYBRID_PIPELINE_NAME},
    )

  def get_document(self, document_id: str, graph_id: str) -> dict[str, Any] | None:
    """Get a document by ID, with graph_id verification for tenant isolation.

    On AOSS VECTOR_SEARCH collections, _id is auto-generated so we search
    by the document_id field instead of using the GET API.
    """
    try:
      if self.is_serverless:
        # AOSS: search by document_id field (custom _id not available)
        result = self.client.search(
          index=self.index_name,
          body={
            "query": {
              "bool": {
                "filter": [
                  {"term": {"document_id": document_id}},
                  {"term": {"graph_id": graph_id}},
                ]
              }
            },
            "size": 1,
          },
        )
        hits = result.get("hits", {}).get("hits", [])
        if not hits:
          return None
        return hits[0].get("_source", {})
      else:
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
    """Delete all documents for a graph_id. Used for graph cleanup.

    On AOSS VECTOR_SEARCH collections, _delete_by_query is not supported.
    Falls back to scroll-and-bulk-delete pattern.
    """
    if not self.is_serverless:
      result = self.client.delete_by_query(
        index=self.index_name,
        body={"query": {"term": {"graph_id": graph_id}}},
      )
      deleted = result.get("deleted", 0)
      logger.info(f"Deleted {deleted} documents for graph_id={graph_id}")
      return deleted

    # AOSS: scroll and bulk delete by _id
    from opensearchpy.helpers import bulk

    deleted = 0
    while True:
      result = self.client.search(
        index=self.index_name,
        body={
          "query": {"term": {"graph_id": graph_id}},
          "size": 500,
          "_source": False,
        },
      )
      hits = result.get("hits", {}).get("hits", [])
      if not hits:
        break
      actions = [
        {"_op_type": "delete", "_index": self.index_name, "_id": h["_id"]} for h in hits
      ]
      success, errors = bulk(self.client, actions, raise_on_error=False)
      if errors:
        logger.warning(
          f"Bulk delete had {len(errors)} failures for graph_id={graph_id}"
        )
      if success == 0:
        logger.error(f"Bulk delete made no progress for graph_id={graph_id}, aborting")
        break
      deleted += success

    logger.info(f"Deleted {deleted} documents for graph_id={graph_id}")
    return deleted

  def health(self) -> dict[str, Any]:
    """Check OpenSearch health."""
    try:
      if self.is_serverless:
        # AOSS doesn't support cluster.health() — use index check as probe
        self.client.indices.get(index=self.index_name)
        return {"status": "green", "backend": "serverless"}
      return self.client.cluster.health()
    except Exception as e:
      return {"status": "unavailable", "error": str(e)}
