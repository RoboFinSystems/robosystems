"""OpenSearch client wrapper with graph_id tenant isolation.

Every query is filtered by graph_id. BM25 is the default mode; hybrid
BM25 + KNN (search_hybrid) is opt-in.
"""

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger

# Without normalization, BM25 scores (~10-40) drown out KNN cosine (~0-1).
HYBRID_PIPELINE_NAME = "hybrid-search-pipeline"

# Per-shard candidates each hybrid sub-query contributes, independent of page
# size: min_max normalizes over this pool, so a fixed pool keeps scores stable
# across pages. Also the pagination_depth OpenSearch requires once `from` > 0.
# Matches Faiss's default ef_search, so costs no extra HNSW traversal.
HYBRID_CANDIDATE_DEPTH = 100

# A hit's snippet is highlight fragments joined with " ... ": the standard
# shape is three of about 200 characters.
HIGHLIGHT_FRAGMENT_SIZE = 200
HIGHLIGHT_FRAGMENTS = 3
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

# A long SEC section is indexed as consecutive parts (1-based part, part
# count, shared parent id, next part's id). Added to existing indexes on
# startup.
PART_FIELDS: dict[str, Any] = {
  "part": {"type": "integer"},
  "part_count": {"type": "integer"},
  "parent_document_id": {"type": "keyword"},
  "next_document_id": {"type": "keyword"},
}

# Embeddings are normalized, so innerproduct equals cosine without the
# per-query normalization.
INDEX_MAPPING = {
  "mappings": {
    "properties": {
      "graph_id": {"type": "keyword"},
      "document_id": {"type": "keyword"},
      "source_type": {
        "type": "keyword"
      },  # "xbrl_textblock", "narrative_section", "ixbrl_disclosure", "uploaded_doc", "connection_doc"
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
      **PART_FIELDS,
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
      # fp16 quantization halves HNSW memory; vectors lie in [-1, 1], so clip
      # stays off (an out-of-range value is a bug to reject). Changing the
      # encoder requires recreating the index.
      "embedding": {
        "type": "knn_vector",
        "dimension": 384,  # fastembed BAAI/bge-small-en-v1.5
        "method": {
          "name": "hnsw",
          "space_type": "innerproduct",
          "engine": "faiss",
          "parameters": {
            "encoder": {"name": "sq", "parameters": {"type": "fp16", "clip": False}},
          },
        },
      },
      "embedding_model": {"type": "keyword"},  # "fastembed" or "bedrock"
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,  # Single node for dev; production will differ
    "index.knn": True,
    "index.refresh_interval": "30s",
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

    AWS managed domains (by hostname) use SigV4; anything else connects
    unauthenticated.
    """
    if self._client is None:
      from opensearchpy import OpenSearch

      host = self.url.replace("https://", "").replace("http://", "").split("/")[0]
      is_aws = host.endswith(".es.amazonaws.com")

      if is_aws:
        import boto3
        from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

        from robosystems.config import env

        session = boto3.Session()
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, env.AWS_REGION, "es")
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
    """Create the index with mapping and hybrid search pipeline if needed."""
    try:
      if not self.client.indices.exists(index=self.index_name):
        self.client.indices.create(index=self.index_name, body=INDEX_MAPPING)
        logger.info(f"Created OpenSearch index: {self.index_name}")
      else:
        logger.debug(f"OpenSearch index already exists: {self.index_name}")
        self._ensure_part_fields()
    except Exception as e:
      logger.error(f"Failed to create OpenSearch index: {e}")
      raise

    self._create_hybrid_pipeline()

  def _ensure_part_fields(self) -> None:
    """Add the section-part fields to an index created before they existed.

    Adding fields is idempotent, so this is safe to repeat.
    """
    try:
      self.client.indices.put_mapping(
        index=self.index_name, body={"properties": PART_FIELDS}
      )
    except Exception as e:
      logger.warning(f"Failed to add section-part fields to the mapping: {e}")

  def _create_hybrid_pipeline(self) -> None:
    """Create or update the hybrid search score-normalization pipeline."""
    try:
      self.client.http.put(
        f"/_search/pipeline/{HYBRID_PIPELINE_NAME}",
        body=HYBRID_PIPELINE_BODY,
      )
      logger.info(f"Created/updated hybrid search pipeline: {HYBRID_PIPELINE_NAME}")
    except Exception as e:
      logger.warning(f"Failed to create hybrid search pipeline: {e}")

  @contextmanager
  def bulk_write_mode(self, write_interval: str = "60s", steady_interval: str = "30s"):
    """Slow refresh during bulk writes, restore normal interval on exit.

    Each refresh creates Lucene segments and FAISS graph rebuilds, which
    degrade search latency during bulk ingestion.
    """
    try:
      self.client.indices.put_settings(
        index=self.index_name,
        body={"index": {"refresh_interval": write_interval}},
      )
      logger.info(
        f"Bulk write mode: refresh_interval set to {write_interval} "
        f"on {self.index_name}"
      )
      yield
    finally:
      try:
        self.client.indices.refresh(index=self.index_name)
        self.client.indices.put_settings(
          index=self.index_name,
          body={"index": {"refresh_interval": steady_interval}},
        )
        logger.info(
          f"Bulk write mode ended: refresh_interval restored to {steady_interval} "
          f"on {self.index_name}"
        )
      except Exception as e:
        logger.warning(f"Failed to restore index settings after bulk write: {e}")

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
      success_count, errors = bulk(
        self.client, chunk, raise_on_error=False, refresh=False
      )
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
        # Narrative ids are lower case ("item_1a"); disclosure ids are
        # case-significant qnames.
        section = filters["section"]
        filter_clauses.append(
          {"terms": {"section_id": sorted({section, section.lower()})}}
        )
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
  def _highlight_config(snippet_chars: int | None = None) -> dict[str, Any]:
    """Highlight configuration for search results.

    ``snippet_chars`` is an approximate per-hit budget: fragments stay at
    the standard size and the budget sets how many of them a hit may carry,
    so a smaller budget means fewer matches shown, not shorter ones. None
    keeps the standard three fragments.
    """
    fragment_size = HIGHLIGHT_FRAGMENT_SIZE
    fragments = HIGHLIGHT_FRAGMENTS
    if snippet_chars is not None:
      fragment_size = min(fragment_size, snippet_chars)
      fragments = max(1, snippet_chars // fragment_size)
    return {
      "fields": {
        "content": {
          "fragment_size": fragment_size,
          "number_of_fragments": fragments,
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
    snippet_chars: int | None = None,
  ) -> dict[str, Any]:
    """BM25 text search with mandatory graph_id filtering.

    Returns the raw OpenSearch response (hits plus highlights).
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
      "highlight": self._highlight_config(snippet_chars),
      "size": size,
      "from": offset,
      "_source": {
        "excludes": ["content", "embedding"],
      },
    }

    return self.client.search(index=self.index_name, body=search_body)

  def search_hybrid(
    self,
    query: str,
    query_embedding: list[float],
    graph_id: str,
    filters: dict[str, Any] | None = None,
    size: int = 10,
    offset: int = 0,
    snippet_chars: int | None = None,
  ) -> dict[str, Any]:
    """Hybrid text + vector search with mandatory graph_id filtering.

    Scores are normalized by ``HYBRID_PIPELINE_NAME``. Slower than BM25, so
    best with narrow filters. Pages past HYBRID_CANDIDATE_DEPTH return fewer
    results than asked. ``query_embedding`` must be a 384-dim fastembed
    vector. Returns the raw OpenSearch response.
    """
    filter_clauses = self._build_filter_clauses(graph_id, filters)

    filter_body: dict[str, Any] = {"bool": {"filter": filter_clauses}}

    # Sub-query order is positional: [BM25, KNN] match the pipeline weights.
    # Filters go inside each sub-query (OpenSearch 2.x has no top-level hybrid
    # filter); a post_filter would let other tenants crowd the KNN top-K.
    search_body: dict[str, Any] = {
      "query": {
        "hybrid": {
          "pagination_depth": HYBRID_CANDIDATE_DEPTH,
          "queries": [
            {
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
            {
              "knn": {
                "embedding": {
                  "vector": query_embedding,
                  "k": HYBRID_CANDIDATE_DEPTH,
                  "filter": filter_body,
                }
              }
            },
          ],
        }
      },
      "highlight": self._highlight_config(snippet_chars),
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
    """Get a document by ID, with graph_id verification for tenant isolation."""
    try:
      result = self.client.get(index=self.index_name, id=document_id)
      source = result.get("_source", {})

      # Defense in depth.
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

  def delete_by_accession(self, graph_id: str, source_type: str, accession: str) -> int:
    """Delete every document of one source type for one filing.

    Run before a re-index: ids depend on how sections split into parts.
    """
    result = self.client.delete_by_query(
      index=self.index_name,
      body={
        "query": {
          "bool": {
            "filter": [
              {"term": {"graph_id": graph_id}},
              {"term": {"source_type": source_type}},
              {"term": {"accession_number": accession}},
            ]
          }
        }
      },
      conflicts="proceed",
    )
    return result.get("deleted", 0)

  def delete_by_document_prefix(self, graph_id: str, prefix: str) -> int:
    """Delete all documents matching a document_id prefix within a graph."""
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
