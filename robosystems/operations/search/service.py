"""Search service — business logic layer over OpenSearch client.

Provides graph_id-scoped search and retrieval, mapping OpenSearch
responses to Pydantic models.
"""

from typing import Any

from robosystems.logger import logger
from robosystems.models.api.search import (
  DocumentSection,
  SearchHit,
  SearchRequest,
  SearchResponse,
)

from .client import OpenSearchClient

# Snippet fallback length when no highlights available
SNIPPET_FALLBACK_LENGTH = 300


class SearchService:
  """Full-text search service scoped by graph_id."""

  def __init__(self, client: OpenSearchClient) -> None:
    self.client = client

  def search_documents(self, graph_id: str, request: SearchRequest) -> SearchResponse:
    """Search documents with graph_id isolation."""
    filters: dict[str, Any] = {}
    if request.entity:
      filters["entity"] = request.entity
    if request.form_type:
      filters["form_type"] = request.form_type
    if request.section:
      filters["section"] = request.section
    if request.element:
      filters["element"] = request.element
    if request.source_type:
      filters["source_type"] = request.source_type
    if request.fiscal_year:
      filters["fiscal_year"] = request.fiscal_year
    if request.date_from:
      filters["date_from"] = request.date_from
    if request.date_to:
      filters["date_to"] = request.date_to

    result = self.client.search(
      query=request.query,
      graph_id=graph_id,
      filters=filters if filters else None,
      size=request.size,
      offset=request.offset,
    )

    hits = []
    for hit in result.get("hits", {}).get("hits", []):
      source = hit.get("_source", {})
      highlight = hit.get("highlight", {})

      # Build snippet from highlights or fallback
      content_fragments = highlight.get("content", [])
      if content_fragments:
        snippet = " ... ".join(content_fragments)
      else:
        # No highlight — we excluded content from _source in search,
        # so use section_label as minimal context
        snippet = source.get("section_label", "")

      hits.append(
        SearchHit(
          document_id=hit.get("_id", source.get("document_id", "")),
          score=hit.get("_score", 0.0),
          source_type=source.get("source_type", ""),
          entity_ticker=source.get("entity_ticker"),
          entity_name=source.get("entity_name"),
          section_label=source.get("section_label"),
          section_id=source.get("section_id"),
          element_qname=source.get("element_qname"),
          xbrl_elements=source.get("xbrl_elements"),
          filing_date=source.get("filing_date"),
          fiscal_year=source.get("fiscal_year"),
          form_type=source.get("form_type"),
          snippet=snippet,
          content_length=source.get("content_length", 0),
          content_url=source.get("content_url"),
        )
      )

    total = result.get("hits", {}).get("total", {})
    total_count = total.get("value", 0) if isinstance(total, dict) else total

    return SearchResponse(
      total=total_count,
      hits=hits,
      query=request.query,
      graph_id=graph_id,
    )

  def get_document_section(
    self, graph_id: str, document_id: str
  ) -> DocumentSection | None:
    """Retrieve full document section by ID with graph_id verification."""
    doc = self.client.get_document(document_id, graph_id)
    if doc is None:
      return None

    return DocumentSection(
      document_id=document_id,
      graph_id=doc.get("graph_id", graph_id),
      source_type=doc.get("source_type", ""),
      entity_ticker=doc.get("entity_ticker"),
      entity_name=doc.get("entity_name"),
      entity_cik=doc.get("entity_cik"),
      section_label=doc.get("section_label"),
      section_id=doc.get("section_id"),
      element_qname=doc.get("element_qname"),
      filing_date=doc.get("filing_date"),
      fiscal_year=doc.get("fiscal_year"),
      fiscal_period=doc.get("fiscal_period"),
      form_type=doc.get("form_type"),
      accession_number=doc.get("accession_number"),
      content=doc.get("content", ""),
      content_url=doc.get("content_url"),
      content_length=doc.get("content_length", 0),
    )

  def index_documents(
    self, graph_id: str, documents: list[dict[str, Any]]
  ) -> dict[str, int]:
    """Index documents for a graph_id."""
    for doc in documents:
      doc["graph_id"] = graph_id
    return self.client.bulk_index(documents)

  def health(self) -> dict[str, Any]:
    """Check search service health."""
    return self.client.health()


# Lazy singleton
_service: SearchService | None = None


def get_search_service() -> SearchService | None:
  """Get the search service singleton. Returns None if TEXT_SEARCH_ENABLED is false."""
  global _service
  if _service is not None:
    return _service

  from robosystems.config import env

  if not env.TEXT_SEARCH_ENABLED:
    return None

  try:
    client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
    client.create_index_if_not_exists()
    _service = SearchService(client)
    logger.info("Search service initialized")
    return _service
  except Exception as e:
    logger.warning(f"Failed to initialize search service: {e}")
    return None
