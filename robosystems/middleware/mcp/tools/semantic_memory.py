"""Semantic memory MCP tools — remember and recall unstructured text.

Two-tool pattern:
1. remember-text: Store text memories with embeddings in OpenSearch
2. recall-text: Retrieve memories by semantic similarity (kNN search)

Memories are scoped by graph_id. remember-text requires subgraph context
(same as write-graph-cypher). recall-text works on any graph context.
"""

from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool
from .memory import _validate_subgraph_context


class RememberTextTool(BaseTool):
  """Store an unstructured text memory in the semantic index."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "remember-text",
      "description": """Store a text note in semantic memory for later recall.
Use this for observations, analysis, research findings, or any unstructured context
that would be useful to recall later by meaning (not just keywords).

**WHEN TO USE (instead of write-graph-cypher):**
- Observations and analysis that don't fit a structured schema
- Research context and reasoning ("why" not just "what")
- Conversation summaries and key takeaways
- Notes that you'd want to find by asking a natural question later

**WHEN TO USE write-graph-cypher INSTEAD:**
- Structured facts with clear entities and relationships
- Data that needs to be queried by specific properties
- Information that connects to the existing knowledge graph

Memories are scoped to the current graph and retrievable via recall-text.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "text": {
            "type": "string",
            "description": "The memory text to store (1-5000 characters)",
          },
          "category": {
            "type": "string",
            "description": "Optional category for filtering (e.g., 'analysis', 'observation', 'preference')",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional tags for filtering",
          },
        },
        "required": ["text"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    # Enforce subgraph-only writes
    error = _validate_subgraph_context(graph_id)
    if error:
      return error

    text = arguments.get("text", "")
    category = arguments.get("category")
    tags = arguments.get("tags")

    # Validate text length
    if not text or len(text) < 1:
      return {"error": "invalid_input", "message": "Text must not be empty"}
    if len(text) > 5000:
      return {
        "error": "invalid_input",
        "message": f"Text exceeds 5000 character limit ({len(text)} chars)",
      }

    # Check memory limit
    try:
      from robosystems.config.billing.core import get_tier_max_memories
      from robosystems.operations.search import get_search_service

      service = get_search_service()
      if service is None:
        return {"error": "unavailable", "message": "Search service is not available"}

      # Resolve tier
      tier = self._resolve_tier(graph_id)
      if tier:
        max_memories = get_tier_max_memories(tier)
        if max_memories is not None:
          current_count = service.client.count_by_source_type(graph_id, "memory")
          if current_count >= max_memories:
            return {
              "error": "limit_reached",
              "message": f"Memory limit reached ({current_count}/{max_memories}). "
              "Upgrade your plan for more capacity.",
            }

      # Generate embedding
      from robosystems.operations.search.embeddings import (
        EMBEDDING_MODEL_ID,
        get_embedding_service,
      )

      embedding_service = get_embedding_service()
      embedding = embedding_service.embed_single(text)

      # Generate document ID
      timestamp = int(datetime.now(UTC).timestamp())
      document_id = f"mem_{graph_id}_{timestamp}"

      # Index the memory
      service.client.index_document(
        {
          "graph_id": graph_id,
          "document_id": document_id,
          "source_type": "memory",
          "content": text,
          "content_length": len(text),
          "category": category,
          "tags": tags,
          "embedding": embedding,
          "embedding_model": EMBEDDING_MODEL_ID,
        }
      )

      logger.info(f"Stored memory {document_id} for graph_id={graph_id}")

      return {
        "success": True,
        "document_id": document_id,
        "message": f"Memory stored ({len(text)} chars)",
      }

    except Exception as e:
      logger.error(f"Failed to store memory for graph_id={graph_id}: {e}")
      return {"error": "store_failed", "message": str(e)}

  @staticmethod
  def _resolve_tier(graph_id: str) -> str | None:
    """Resolve subscription tier for a graph."""
    try:
      from robosystems.database import SessionFactory
      from robosystems.middleware.graph.utils import parse_subgraph_id
      from robosystems.models.core.graph import Graph

      # Use parent graph ID for tier lookup
      sub_info = parse_subgraph_id(graph_id)
      parent_id = sub_info.parent_graph_id if sub_info else graph_id

      session = SessionFactory()
      try:
        graph = Graph.get_by_id(parent_id, session)
        return graph.graph_tier if graph else None
      finally:
        session.close()
    except Exception:
      return None


class RecallTextTool:
  """Semantically search text memories."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "recall-text",
      "description": """Search text memories by meaning (semantic similarity).
Returns memories most relevant to your query, even if they don't share exact keywords.

**WHEN TO USE:**
- Before starting analysis, recall what you've previously noted about the topic
- When the user asks "what did we discuss about X?"
- To retrieve prior observations that inform current work

**HOW IT DIFFERS FROM search-documents:**
- recall-text searches YOUR notes/memories (things stored via remember-text)
- search-documents searches indexed documents (SEC filings, uploaded docs)
- Use both together for comprehensive context""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "Natural language query to search memories by meaning",
          },
          "category": {
            "type": "string",
            "description": "Optional: filter by category",
          },
          "size": {
            "type": "integer",
            "description": "Max results (default 10)",
            "default": 10,
          },
        },
        "required": ["query"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    query = arguments.get("query", "")
    category = arguments.get("category")
    size = arguments.get("size", 10)

    if not query:
      return {"error": "invalid_input", "message": "Query must not be empty"}

    try:
      from robosystems.operations.search import get_search_service
      from robosystems.operations.search.embeddings import get_embedding_service

      service = get_search_service()
      if service is None:
        return {"error": "unavailable", "message": "Search service is not available"}

      # Embed the query
      embedding_service = get_embedding_service()
      query_embedding = embedding_service.embed_single(query)

      # Build filters
      filters: dict[str, Any] = {"source_type": "memory"}
      if category:
        filters["category"] = category

      # kNN search
      result = service.client.knn_search(
        query_embedding=query_embedding,
        graph_id=graph_id,
        filters=filters,
        size=size,
      )

      # Map results
      memories = []
      for hit in result.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        memories.append(
          {
            "document_id": hit.get("_id", source.get("document_id", "")),
            "text": source.get("content", ""),
            "category": source.get("category"),
            "tags": source.get("tags"),
            "score": hit.get("_score", 0.0),
            "indexed_at": source.get("indexed_at"),
          }
        )

      total = result.get("hits", {}).get("total", {})
      total_count = total.get("value", 0) if isinstance(total, dict) else total

      return {
        "memories": memories,
        "total": total_count,
        "query": query,
      }

    except Exception as e:
      logger.error(f"Failed to recall memories for graph_id={graph_id}: {e}")
      return {"error": "recall_failed", "message": str(e)}
