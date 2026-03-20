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
      "source_type": {"type": "keyword"},  # "xbrl_textblock" or "narrative_section"
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
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,  # Single node for dev; production will differ
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
    # Build filter clauses — graph_id is always required
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
      if filters.get("date_from"):
        filter_clauses.append({"range": {"filing_date": {"gte": filters["date_from"]}}})
      if filters.get("date_to"):
        filter_clauses.append({"range": {"filing_date": {"lte": filters["date_to"]}}})

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
      "highlight": {
        "fields": {
          "content": {
            "fragment_size": 200,
            "number_of_fragments": 3,
            "pre_tags": [""],
            "post_tags": [""],
          }
        }
      },
      "size": size,
      "from": offset,
      "_source": {
        "excludes": ["content"],
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

  def health(self) -> dict[str, Any]:
    """Check OpenSearch cluster health."""
    try:
      return self.client.cluster.health()
    except Exception as e:
      return {"status": "unavailable", "error": str(e)}
