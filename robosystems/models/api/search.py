"""Search request/response models for full-text document search."""

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
  """Request model for document search."""

  query: str = Field(..., min_length=1, max_length=500, description="Search query")
  entity: str | None = Field(None, description="Filter by ticker, CIK, or entity name")
  form_type: str | None = Field(
    None, description="Filter by SEC form type (10-K, 10-Q)"
  )
  section: str | None = Field(
    None, description="Filter by section ID (item_1, item_1a, item_7, etc.)"
  )
  element: str | None = Field(
    None,
    description="Filter by XBRL element qname (e.g., us-gaap:Goodwill)",
  )
  source_type: str | None = Field(
    None,
    description="Filter by source type (xbrl_textblock, narrative_section, ixbrl_disclosure, uploaded_doc, memory)",
  )
  fiscal_year: int | None = Field(None, description="Filter by fiscal year")
  date_from: str | None = Field(
    None,
    pattern=r"^\d{4}-\d{2}-\d{2}$",
    description="Filter filings on or after date (YYYY-MM-DD)",
  )
  date_to: str | None = Field(
    None,
    pattern=r"^\d{4}-\d{2}-\d{2}$",
    description="Filter filings on or before date (YYYY-MM-DD)",
  )
  semantic: bool = Field(
    False,
    description="Enable hybrid semantic search (BM25 + KNN). Default is BM25-only for speed.",
  )
  size: int = Field(10, ge=1, le=50, description="Max results to return")
  offset: int = Field(0, ge=0, description="Pagination offset")


class SearchHit(BaseModel):
  """A single search result with snippet."""

  document_id: str
  score: float
  source_type: str
  parent_document_id: str | None = None
  entity_ticker: str | None = None
  entity_name: str | None = None
  section_label: str | None = None
  section_id: str | None = None
  element_qname: str | None = None
  filing_date: str | None = None
  fiscal_year: int | None = None
  form_type: str | None = None
  xbrl_elements: list[str] | None = None
  snippet: str
  content_length: int = 0
  content_url: str | None = None
  document_title: str | None = None
  tags: list[str] | None = None
  folder: str | None = None


class SearchResponse(BaseModel):
  """Response model for document search."""

  total: int
  hits: list[SearchHit]
  query: str
  graph_id: str


class DocumentSection(BaseModel):
  """Full document section retrieved by ID."""

  document_id: str
  graph_id: str
  source_type: str
  entity_ticker: str | None = None
  entity_name: str | None = None
  entity_cik: str | None = None
  section_label: str | None = None
  section_id: str | None = None
  element_qname: str | None = None
  filing_date: str | None = None
  fiscal_year: int | None = None
  fiscal_period: str | None = None
  form_type: str | None = None
  accession_number: str | None = None
  xbrl_elements: list[str] | None = None
  content: str
  content_url: str | None = None
  content_length: int = 0
  document_title: str | None = None
  tags: list[str] | None = None
  folder: str | None = None


# --- Document upload models ---


class DocumentUploadRequest(BaseModel):
  """Upload a markdown document for text indexing."""

  title: str = Field(..., description="Document title", max_length=500)
  content: str = Field(..., description="Markdown content", max_length=500_000)
  tags: list[str] | None = Field(None, description="Optional tags for filtering")
  folder: str | None = Field(None, description="Optional folder/category")
  external_id: str | None = Field(
    None,
    description="Optional external identifier for upsert (e.g., Google Drive file ID)",
  )


class DocumentUploadResponse(BaseModel):
  """Response from document upload."""

  id: str
  document_id: str
  sections_indexed: int
  total_content_length: int
  section_ids: list[str]


class BulkDocumentUploadRequest(BaseModel):
  """Bulk upload multiple markdown documents."""

  documents: list[DocumentUploadRequest] = Field(
    ..., max_length=50, description="Documents to upload (max 50)"
  )


class BulkDocumentUploadResponse(BaseModel):
  """Response from bulk document upload."""

  total_documents: int
  total_sections_indexed: int
  results: list[DocumentUploadResponse]
  errors: list[dict] | None = None


class DocumentListItem(BaseModel):
  """A document in the document list."""

  id: str
  document_title: str
  section_count: int
  source_type: str
  folder: str | None = None
  tags: list[str] | None = None
  created_at: str
  updated_at: str


class DocumentListResponse(BaseModel):
  """Response from listing indexed documents."""

  total: int
  documents: list[DocumentListItem]
  graph_id: str


class DocumentDetailResponse(BaseModel):
  """Full document detail with raw content."""

  id: str
  graph_id: str
  user_id: str
  title: str
  content: str
  tags: list[str] | None = None
  folder: str | None = None
  external_id: str | None = None
  source_type: str
  source_provider: str | None = None
  sections_indexed: int
  created_at: str
  updated_at: str


class DocumentUpdateRequest(BaseModel):
  """Update a document's metadata and/or content."""

  title: str | None = Field(None, max_length=500)
  content: str | None = Field(None, max_length=500_000)
  tags: list[str] | None = None
  folder: str | None = None
