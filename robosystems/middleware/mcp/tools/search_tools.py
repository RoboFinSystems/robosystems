"""Hybrid search MCP tools for SEC filing document discovery and retrieval.

Two-tool pattern:
1. search-documents: Hybrid (BM25 + KNN) search returning ranked snippets with metadata
2. get-document-section: Drill into a specific result for full content

All searches use hybrid mode combining keyword matching (BM25) with vector
similarity (KNN) via a normalization pipeline for balanced scoring.
"""

from typing import Any

from robosystems.logger import logger
from robosystems.security.error_handling import safe_error_message


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

**RELATED TOOLS:**
- read-graph-cypher — structured data (numbers, relationships), not prose
- list-documents — browses by metadata; does not search content
- get-document-section / get-document — retrieve what a hit points at

**RETURNS:**
- Ranked results with relevance scores and text snippets
- Each result includes a document_id — use get-document-section for the full section text
- For user docs, use get-document to retrieve the complete document
- iXBRL results include xbrl_elements for graph cross-reference

**NOTES:**
- Searches user-uploaded documents (created via create-document), SEC filing
  text blocks and narrative sections, and iXBRL disclosure sections
- iXBRL results carry xbrl_elements (e.g. us-gaap:Goodwill). Look one up with
  resolve-element, then read-graph-cypher for its structured values — this is
  the bridge from narrative context to financial facts. Note resolve-element
  is only published on graphs with semantic enrichment
- Natural language queries work well ("depreciation policy", "month end close procedures")
- Use entity filter to focus on one company's filings
- Use section filter (item_1a, item_7) to target specific filing sections""",
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
            "description": "Optional: filter by section ID (item_1, item_1a, item_1c, item_2, item_7, item_7a)",
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

    request = SearchRequest(
      query=arguments["query"],
      entity=arguments.get("entity"),
      form_type=arguments.get("form_type"),
      section=arguments.get("section"),
      element=arguments.get("element"),
      fiscal_year=arguments.get("fiscal_year"),
      semantic=arguments.get("semantic", False),
      size=min(arguments.get("size", 10), 50),
    )

    logger.info(f"MCP search-documents: query='{request.query}' graph_id={graph_id}")

    try:
      response = service.search_documents(graph_id, request)
      return response.model_dump()
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
  """Retrieve the full text of a document section found via search."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-document-section",
      "description": """Retrieve the full text of a document section by ID. Use this after search-documents to read the complete narrative content of a relevant result.

**WHEN TO USE:**
- After search-documents returns results, use the document_id from a hit to get the full section
- When you need the complete text of an MD&A, risk factor, or business description
- To read the full context around a search snippet

**RETURNS:**
- Complete section text with entity, filing, and section metadata
- content_url for the CDN-hosted clean text (when available)
- For iXBRL disclosures: xbrl_elements list of XBRL fact tags in this section — use resolve-element or read-graph-cypher to cross-reference with the knowledge graph""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Document ID from a search-documents result",
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

    logger.info(f"MCP get-document-section: doc_id={document_id} graph_id={graph_id}")

    try:
      result = service.get_document_section(graph_id, document_id)
      if result is None:
        return {"error": f"Document {document_id} not found"}
      return result.model_dump()
    except Exception as e:
      logger.error(f"get-document-section failed: {e}", exc_info=True)
      return {
        "error": "retrieval_failed",
        "message": safe_error_message(e)
        or "retrieval failed on a backend error; see server logs",
      }
