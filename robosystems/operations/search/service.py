"""Search service — business logic layer over OpenSearch client.

Provides graph_id-scoped search and retrieval, mapping OpenSearch
responses to Pydantic models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.config import env
from robosystems.logger import logger
from robosystems.models.api.search import (
  DocumentListItem,
  DocumentListResponse,
  DocumentSection,
  DocumentUploadRequest,
  DocumentUploadResponse,
  SearchHit,
  SearchRequest,
  SearchResponse,
)

from .client import OpenSearchClient

if TYPE_CHECKING:
  from .embeddings import EmbeddingService

# Snippet fallback length when no highlights available
SNIPPET_FALLBACK_LENGTH = 300


class SearchService:
  """Full-text search service scoped by graph_id."""

  def __init__(self, client: OpenSearchClient) -> None:
    self.client = client
    self._embedding_service: EmbeddingService | None = None

  @property
  def embedding_service(self) -> EmbeddingService:
    """Lazy-load the platform embedding service."""
    if self._embedding_service is None:
      from .embeddings import get_embedding_service

      self._embedding_service = get_embedding_service()
      logger.info("Loaded EmbeddingService for search")
    return self._embedding_service

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

    use_semantic = env.SEMANTIC_SEARCH_ENABLED and request.semantic

    if use_semantic:
      query_embedding = self.embedding_service.embed_single(request.query)
      result = self.client.hybrid_search(
        query=request.query,
        query_embedding=query_embedding,
        graph_id=graph_id,
        filters=filters if filters else None,
        size=request.size,
        offset=request.offset,
      )
    else:
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
          document_title=source.get("document_title"),
          tags=source.get("tags"),
          folder=source.get("folder"),
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
      document_title=doc.get("document_title"),
      tags=doc.get("tags"),
      folder=doc.get("folder"),
    )

  def upload_document(
    self,
    graph_id: str,
    request: DocumentUploadRequest,
    tier: str | None = None,
  ) -> DocumentUploadResponse:
    """Upload a markdown document: parse, section, embed, and index.

    Args:
        graph_id: Target graph for tenant isolation.
        request: Document upload request with title, content, etc.
        tier: Subscription tier for limit enforcement (None = no limit check).

    Returns:
        DocumentUploadResponse with section IDs and counts.

    Raises:
        ValueError: If document section limit exceeded for the tier.
    """
    from .embeddings import EMBEDDING_MODEL_ID
    from .markdown_parser import content_hash, parse_document

    # Check tier limits if tier provided
    if tier:
      from robosystems.config.billing.core import get_tier_max_document_sections

      max_sections = get_tier_max_document_sections(tier)
      if max_sections is not None:
        current_count = self.client.count_by_source_type(graph_id, "uploaded_doc")
        if current_count >= max_sections:
          raise ValueError(
            f"Document section limit reached ({current_count}/{max_sections}). "
            f"Upgrade your plan for more capacity."
          )

    # Parse markdown
    metadata, sections = parse_document(request.content, request.title)
    if not sections:
      raise ValueError("Document produced no indexable sections")

    # Generate base document ID
    if request.external_id:
      base_id = f"doc_{graph_id}_{request.external_id}"
    else:
      base_id = f"doc_{graph_id}_{content_hash(request.content)}"

    # Remove old sections if re-uploading
    self.client.delete_by_document_prefix(graph_id, base_id)

    # Use frontmatter values, falling back to request fields
    doc_title = metadata.get("title", request.title)
    doc_tags = metadata.get("tags", request.tags)
    doc_folder = metadata.get("folder", request.folder)

    # Embed all section contents
    section_texts = [s["content"] for s in sections]
    embeddings = self.embedding_service.embed_batch(section_texts)

    # Build OpenSearch documents
    os_docs: list[dict[str, Any]] = []
    section_ids: list[str] = []
    total_length = 0

    for idx, (section, embedding) in enumerate(zip(sections, embeddings, strict=True)):
      doc_id = f"{base_id}_{idx}"
      section_ids.append(doc_id)
      total_length += len(section["content"])

      os_docs.append(
        {
          "graph_id": graph_id,
          "document_id": doc_id,
          "source_type": "uploaded_doc",
          "document_title": doc_title,
          "section_id": section["section_id"],
          "section_label": section["section_label"],
          "content": section["content"],
          "content_length": len(section["content"]),
          "tags": doc_tags,
          "folder": doc_folder,
          "embedding": embedding,
          "embedding_model": EMBEDDING_MODEL_ID,
        }
      )

    self.client.bulk_index(os_docs)

    return DocumentUploadResponse(
      document_id=base_id,
      sections_indexed=len(sections),
      total_content_length=total_length,
      section_ids=section_ids,
    )

  def upload_documents_bulk(
    self,
    graph_id: str,
    requests: list[DocumentUploadRequest],
    tier: str | None = None,
  ) -> tuple[list[DocumentUploadResponse], list[dict]]:
    """Upload multiple documents. Returns (results, errors)."""
    results: list[DocumentUploadResponse] = []
    errors: list[dict] = []

    for i, request in enumerate(requests):
      try:
        result = self.upload_document(graph_id, request, tier)
        results.append(result)
      except Exception as e:
        errors.append({"index": i, "title": request.title, "error": str(e)})

    return results, errors

  def delete_document(self, graph_id: str, document_id: str) -> bool:
    """Delete a document and all its sections."""
    # Try exact match first
    if self.client.delete_document(document_id, graph_id):
      return True
    # Try prefix match (delete all sections of a multi-section doc)
    deleted = self.client.delete_by_document_prefix(graph_id, document_id)
    return deleted > 0

  def list_documents(
    self, graph_id: str, source_type: str | None = None
  ) -> DocumentListResponse:
    """List indexed documents grouped by title."""
    result = self.client.list_documents(graph_id, source_type)
    aggs = result.get("aggregations", {})
    doc_buckets = aggs.get("documents", {}).get("buckets", [])

    documents: list[DocumentListItem] = []
    for bucket in doc_buckets:
      # Extract nested aggregation values
      source_types = bucket.get("source_type", {}).get("buckets", [])
      st = source_types[0]["key"] if source_types else "uploaded_doc"
      folders = bucket.get("folder", {}).get("buckets", [])
      folder = folders[0]["key"] if folders else None
      tags_buckets = bucket.get("tags", {}).get("buckets", [])
      tags = [t["key"] for t in tags_buckets] if tags_buckets else None
      last_indexed = bucket.get("last_indexed", {}).get("value_as_string")

      documents.append(
        DocumentListItem(
          document_title=bucket["key"],
          section_count=bucket["doc_count"],
          source_type=st,
          folder=folder,
          tags=tags,
          last_indexed=last_indexed,
        )
      )

    return DocumentListResponse(
      total=len(documents),
      documents=documents,
      graph_id=graph_id,
    )

  def index_documents(
    self, graph_id: str, documents: list[dict[str, Any]]
  ) -> dict[str, int]:
    """Index documents for a graph_id (used by SEC pipeline)."""
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
