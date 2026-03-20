"""Full-text search MCP tools for document discovery and retrieval.

Two-tool pattern:
1. search-documents: Broad search returning snippets with metadata
2. get-document-section: Drill into a specific result for full content
"""

from typing import Any

from robosystems.logger import logger


class SearchDocumentsTool:
  """Search filing narratives and text content across a graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "search-documents",
      "description": """Full-text search across SEC filing narratives and text content. Searches MD&A, risk factors, business descriptions, cybersecurity disclosures, and other qualitative content.

**WHEN TO USE:**
- When the user asks about topics, risks, strategies, or disclosures mentioned in filings
- To find qualitative context that structured financial data (numbers) cannot provide
- For cross-company topic search (e.g., "which companies mention tariff risk?")
- To discover narrative content before drilling into specific sections

**HOW IT DIFFERS FROM read-graph-cypher:**
- read-graph-cypher queries structured data (numbers, relationships, metadata)
- search-documents finds unstructured text content (narratives, descriptions, disclosures)

**RETURNS:**
- Ranked results with relevance scores and text snippets showing matched content
- Each result includes entity, filing date, form type, section label, and a document_id
- Use get-document-section with the document_id to read the full section text

**TIPS:**
- Use entity filter to focus on one company's filings
- Use section filter (item_1a, item_7) to target specific filing sections
- Results include both XBRL text blocks and extracted narrative sections""",
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
          "fiscal_year": {
            "type": "integer",
            "description": "Optional: filter by fiscal year",
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

    graph_id = self.client.graph_id

    request = SearchRequest(
      query=arguments["query"],
      entity=arguments.get("entity"),
      form_type=arguments.get("form_type"),
      section=arguments.get("section"),
      fiscal_year=arguments.get("fiscal_year"),
      size=min(arguments.get("size", 10), 50),
    )

    logger.info(f"MCP search-documents: query='{request.query}' graph_id={graph_id}")

    try:
      response = service.search_documents(graph_id, request)
      return response.model_dump()
    except Exception as e:
      logger.error(f"search-documents failed: {e}")
      return {"error": f"Search failed: {e}"}


class GetDocumentSectionTool:
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
- content_url for the CDN-hosted clean text (when available)""",
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

    graph_id = self.client.graph_id
    document_id = arguments["document_id"]

    logger.info(f"MCP get-document-section: doc_id={document_id} graph_id={graph_id}")

    try:
      result = service.get_document_section(graph_id, document_id)
      if result is None:
        return {"error": f"Document {document_id} not found"}
      return result.model_dump()
    except Exception as e:
      logger.error(f"get-document-section failed: {e}")
      return {"error": f"Retrieval failed: {e}"}
