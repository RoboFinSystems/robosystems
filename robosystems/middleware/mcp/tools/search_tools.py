"""Hybrid search MCP tools for SEC filing document discovery and retrieval.

Two-tool pattern:
1. search-documents: Hybrid (BM25 + KNN) search returning ranked snippets with metadata
2. get-document-section: Drill into a specific result for full content

All searches use hybrid mode combining keyword matching (BM25) with vector
similarity (KNN) via a normalization pipeline for balanced scoring.

A tool result is context the model pays for on every later turn, so a hit
carries what is needed to choose it and the section call carries what is
needed to answer. The REST surface keeps the full response models; the
trimming lives here, in ``compact_search_response`` and ``window_section``.
"""

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.security.error_handling import safe_error_message

if TYPE_CHECKING:
  from robosystems.models.api.search import DocumentSection, SearchResponse

# Bounds mirror SearchRequest.snippet_chars; the default is the tool's own.
SNIPPET_CHARS_DEFAULT = 400
SNIPPET_CHARS_MIN = 80
SNIPPET_CHARS_MAX = 1500

# The window a section read returns, matching xbrlkit's read_text so the
# hosted and local document tools page the same way.
SECTION_READ_DEFAULT = 4000
SECTION_READ_MAX = 8000

# Filing fields that repeat identically on every hit of a single-filing
# search; they move to the result root when every hit agrees.
HOISTED_HIT_FIELDS = (
  "entity_ticker",
  "entity_name",
  "form_type",
  "filing_date",
  "fiscal_year",
)

# The element qnames of every fact in a section were 44% of the measured
# search payload and never used to choose a hit; get-document-section
# carries them for the hit the model picks.
DROPPED_HIT_FIELDS = ("xbrl_elements",)


def compact_search_response(response: "SearchResponse") -> dict[str, Any]:
  """The search response as a model reads it: decision fields only.

  Drops null fields and the per-hit element list, and hoists the filing
  fields to the root when every hit shares the same value. Hits that span
  filings keep those fields on each hit.
  """
  hits = [hit.model_dump(exclude_none=True) for hit in response.hits]
  for hit in hits:
    for field in DROPPED_HIT_FIELDS:
      hit.pop(field, None)

  result: dict[str, Any] = {
    "total": response.total,
    "query": response.query,
    "graph_id": response.graph_id,
  }
  if hits:
    for field in HOISTED_HIT_FIELDS:
      values = {hit.get(field) for hit in hits}
      if len(values) != 1:
        continue
      value = values.pop()
      if value is None:
        continue
      result[field] = value
      for hit in hits:
        hit.pop(field, None)
  result["hits"] = hits
  return result


def window_section(
  section: "DocumentSection", offset: int, length: int
) -> dict[str, Any]:
  """One window of a section's content, with the offset to read on from."""
  content = section.content
  total = len(content)
  if offset and offset >= total:
    return {
      "error": f"offset {offset} is past the end of this part ({total} chars)",
    }
  end = min(total, offset + length)

  data = section.model_dump(exclude_none=True)
  data["content"] = content[offset:end]
  data["content_length"] = total
  data["offset"] = offset
  if end < total:
    data["next_offset"] = end
  return data


class _SearchToolMixin:
  """Shared logic for search tools."""

  client: Any  # graph_client with .graph_id

  def _resolve_search_graph_id(self) -> str:
    """Resolve to parent graph_id for search queries.

    Search indexes use the parent graph_id (e.g. "sec" not "sec_historical").
    Subgraphs are a storage optimization, not a search boundary.
    For non-shared-repository graphs, returns the graph_id unchanged.
    """
    from robosystems.config.shared_repositories import (
      resolve_shared_repository_parent,
    )

    graph_id = self.client.graph_id
    try:
      return resolve_shared_repository_parent(graph_id)
    except ValueError:
      # Not a shared repository — use as-is
      return graph_id


class SearchDocumentsTool(_SearchToolMixin):
  """Search filing narratives and text content across a graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "search-documents",
      "description": """Search across documents. Keyword (BM25) by default; set `semantic=true` to add KNN. Covers SEC filing narratives, user-uploaded documents (policies, procedures, notes), and any other indexed content.

**WHEN TO USE:**
- To find accounting policies, close procedures, or reference documents
- To search SEC filing narratives (MD&A, risk factors, disclosures)
- To discover any document content by topic or keyword
- For cross-company topic search on shared repositories (e.g., "which companies mention tariff risk?")

**PARAMETERS:**
- `query` (required) — the search text
- `semantic` — adds KNN to the BM25 pass; defaults to false, so a plain call
  is keyword-only
- `entity`, `form_type`, `section`, `element`, `fiscal_year`, `size` — filters
- `snippet_chars` — approximate snippet budget per hit (default 400, max 1500);
  raise it when the snippets are too short to choose between hits

**RELATED TOOLS:**
- get-document-section / get-document — read what a hit points at
- read-graph-cypher — structured data (numbers, relationships), not prose
- list-documents — browses by metadata; does not search content
- resolve-element — takes a qname from get-document-section's xbrl_elements on
  to its structured values

**RETURNS:**
- Ranked hits, each with document_id, score, source_type, section_label,
  section_id, snippet, content_length and content_url (element_qname for an
  iXBRL disclosure)
- Filing fields every hit shares (entity_ticker, entity_name, form_type,
  filing_date, fiscal_year) appear once at the result root; when the hits span
  filings they stay on each hit
- A snippet is an excerpt around the match, not the passage — read the section
  with get-document-section before quoting a figure from it
- A long section (an MD&A, a commitments note) is indexed in parts of about 25K
  characters; a hit carries part / part_count and the parts share a
  parent_document_id. get-document-section returns one part and its next_document_id
- For user docs, use get-document to retrieve the complete document

**NOTES:**
- Searches user-uploaded documents (created via create-document), SEC filing
  text blocks and narrative sections, and iXBRL disclosure sections
- To tie narrative back to reported numbers: get-document-section returns an
  iXBRL disclosure's xbrl_elements (e.g. us-gaap:Goodwill); look one up with
  resolve-element, then read-graph-cypher for its structured values. Note
  resolve-element is only published on graphs with semantic enrichment
- Natural language queries work well ("depreciation policy", "month end close procedures")
- Use entity filter to focus on one company's filings
- Use section filter (item_1a, item_7) to target specific filing sections, or an
  element qname (us-gaap:CommitmentsAndContingenciesDisclosureTextBlock) to
  target one iXBRL disclosure across filings; the `element` filter finds the
  disclosures that contain a given fact""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "Search query (e.g., 'tariff exposure supply chain risk')",
          },
          "entity": {
            "type": "string",
            "description": "Optional: filter by ticker, CIK, or company name",
          },
          "form_type": {
            "type": "string",
            "description": "Optional: filter by form type (10-K, 10-Q)",
          },
          "section": {
            "type": "string",
            "description": "Optional: filter by section ID — an Item (item_1, item_1a, item_1c, item_2, item_7, item_7a) or an iXBRL disclosure's element qname (us-gaap:GoodwillDisclosureTextBlock)",
          },
          "element": {
            "type": "string",
            "description": "Optional: filter by XBRL element qname to find disclosures containing that fact (e.g., us-gaap:Goodwill, us-gaap:Revenues)",
          },
          "fiscal_year": {
            "type": "integer",
            "description": "Optional: filter by fiscal year",
          },
          "semantic": {
            "type": "boolean",
            "description": "Enable hybrid semantic search (BM25 + KNN vector similarity). Default is BM25-only which is faster. Use semantic=true for meaning-based search when keyword matching is insufficient.",
            "default": False,
          },
          "size": {
            "type": "integer",
            "description": "Max results (default 10, max 50)",
            "default": 10,
          },
          "snippet_chars": {
            "type": "integer",
            "description": f"Approximate snippet budget per hit in characters (default {SNIPPET_CHARS_DEFAULT}, max {SNIPPET_CHARS_MAX})",
            "default": SNIPPET_CHARS_DEFAULT,
          },
        },
        "required": ["query"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    # Lazy import to avoid opensearch-py at module load time
    from robosystems.models.api.search import SearchRequest
    from robosystems.operations.search import get_search_service

    service = get_search_service()
    if service is None:
      return {"error": "Text search is not available"}

    # Search indexes use the parent graph_id (e.g. "sec" not "sec_historical").
    # Subgraphs are a storage split, not a search boundary.
    graph_id = self._resolve_search_graph_id()

    snippet_chars = int(arguments.get("snippet_chars") or SNIPPET_CHARS_DEFAULT)
    request = SearchRequest(
      query=arguments["query"],
      entity=arguments.get("entity"),
      form_type=arguments.get("form_type"),
      section=arguments.get("section"),
      element=arguments.get("element"),
      fiscal_year=arguments.get("fiscal_year"),
      semantic=arguments.get("semantic", False),
      size=min(arguments.get("size", 10), 50),
      snippet_chars=max(SNIPPET_CHARS_MIN, min(snippet_chars, SNIPPET_CHARS_MAX)),
    )

    logger.info(f"MCP search-documents: query='{request.query}' graph_id={graph_id}")

    try:
      response = service.search_documents(graph_id, request)
      return compact_search_response(response)
    except Exception as e:
      # opensearch-py exception text embeds the endpoint hostname and query
      # internals — the LLM-facing result gets the fixed message instead.
      logger.error(f"search-documents failed: {e}", exc_info=True)
      return {
        "error": "search_failed",
        "message": safe_error_message(e)
        or "search failed on a backend error; see server logs",
      }


class GetDocumentSectionTool(_SearchToolMixin):
  """Retrieve the text of a document section found via search, a window at a time."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-document-section",
      "description": f"""Read a document section by ID, a window at a time. Use this after search-documents to read the text behind a hit before answering from it.

**WHEN TO USE:**
- After search-documents returns results, use the document_id from a hit to read the section
- When you need the text of an MD&A, risk factor, or business description
- To read the full context around a search snippet

**PARAMETERS:**
- `document_id` (required) — from a search-documents hit, or the next_document_id of a part
- `offset` — character offset to start from (default 0)
- `length` — characters to return (default {SECTION_READ_DEFAULT}, max {SECTION_READ_MAX})

**RETURNS:**
- `content` — the requested window. `content_length` is the whole part, and
  `next_offset` is present when more of it follows: call again with it as
  `offset` to read on rather than answering from a partial read
- Entity, filing, and section metadata
- A long section is stored in parts of about 25K characters: the result is one
  part (part of part_count, section_label like "MD&A (2/6)") and carries
  next_document_id — call again with it to read the next part; parent_document_id
  is shared by the section's parts
- content_url for the CDN-hosted clean text (when available)
- For iXBRL disclosures: xbrl_elements, the XBRL fact tags in this section — use resolve-element or read-graph-cypher to cross-reference with the knowledge graph""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Document ID from a search-documents result, or the next_document_id of a part",
          },
          "offset": {
            "type": "integer",
            "description": "Character offset to start from (default 0); pass a result's next_offset to read on",
            "default": 0,
          },
          "length": {
            "type": "integer",
            "description": f"Characters to return (default {SECTION_READ_DEFAULT}, max {SECTION_READ_MAX})",
            "default": SECTION_READ_DEFAULT,
          },
        },
        "required": ["document_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.search import get_search_service

    service = get_search_service()
    if service is None:
      return {"error": "Text search is not available"}

    # Search indexes use the parent graph_id (e.g. "sec" not "sec_historical")
    graph_id = self._resolve_search_graph_id()
    document_id = arguments["document_id"]
    offset = max(0, int(arguments.get("offset") or 0))
    length = max(
      1, min(int(arguments.get("length") or SECTION_READ_DEFAULT), SECTION_READ_MAX)
    )

    logger.info(f"MCP get-document-section: doc_id={document_id} graph_id={graph_id}")

    try:
      result = service.get_document_section(graph_id, document_id)
      if result is None:
        return {"error": f"Document {document_id} not found"}
      return window_section(result, offset, length)
    except Exception as e:
      logger.error(f"get-document-section failed: {e}", exc_info=True)
      return {
        "error": "retrieval_failed",
        "message": safe_error_message(e)
        or "retrieval failed on a backend error; see server logs",
      }
