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
    description="Filter by source type (xbrl_textblock, narrative_section, ixbrl_disclosure)",
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
  size: int = Field(10, ge=1, le=50, description="Max results to return")
  offset: int = Field(0, ge=0, description="Pagination offset")


class SearchHit(BaseModel):
  """A single search result with snippet."""

  document_id: str
  score: float
  source_type: str  # "xbrl_textblock" or "narrative_section"
  entity_ticker: str | None = None
  entity_name: str | None = None
  section_label: str | None = None
  section_id: str | None = None
  element_qname: str | None = None
  filing_date: str | None = None
  fiscal_year: int | None = None
  form_type: str | None = None
  xbrl_elements: list[str] | None = None  # XBRL element qnames in this section
  snippet: str  # Highlighted excerpt from content
  content_length: int = 0
  content_url: str | None = None


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
