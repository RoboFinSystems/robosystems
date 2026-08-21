"""Semantic memory MCP tools (LanceDB vector store).

Gives AI Operators a stateful memory layer: `remember` (write), `recall` (ranked
semantic read), `update-memory` (edit in place), `forget` (delete). Thin adapters
over the transport-independent ``MemoryService`` kernel — no business logic here.

Distinct from ``subgraph_write_tools.py`` (write-graph-cypher / add-node-table),
which builds structural knowledge graphs on subgraphs. This is the per-graph
semantic-vector store, gated by SEMANTIC_MEMORY_ENABLED plus the MCP sub-gate
MCP_SEMANTIC_MEMORY_ENABLED at registration.
"""

from typing import Any

from robosystems.logger import logger

from ..exceptions import GraphAPIError
from .base_tool import BaseTool

# Lazy import to avoid circular imports with the adapter chain
_is_shared_repository_or_subgraph = None


def _get_is_shared_repository_or_subgraph():
  global _is_shared_repository_or_subgraph
  if _is_shared_repository_or_subgraph is None:
    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    _is_shared_repository_or_subgraph = is_shared_repository_or_subgraph
  return _is_shared_repository_or_subgraph


def _block_shared_repo(graph_id: str) -> dict[str, Any] | None:
  if _get_is_shared_repository_or_subgraph()(graph_id):
    return {
      "error": "read_only",
      "message": "Semantic memory is not available on shared repository graphs.",
    }
  return None


class _SemanticMemoryToolMixin:
  """Shared writer-client acquisition + kernel wiring for semantic memory tools."""

  async def _with_service(self, run):
    """Run ``run(service, graph_id)`` against a writer-routed MemoryService."""
    from robosystems.graph_api.client.factory import get_graph_client
    from robosystems.operations.memory import get_memory_service

    graph_id = self.client.graph_id  # type: ignore[attr-defined]
    blocked = _block_shared_repo(graph_id)
    if blocked:
      return blocked

    # Memory lives only on the writer/master (not in replica sync).
    client = await get_graph_client(graph_id=graph_id, operation_type="write")
    try:
      service = get_memory_service(client)
      if service is None:
        return {
          "error": "disabled",
          "message": "Semantic memory is not enabled on this deployment.",
        }
      return await run(service, graph_id)
    finally:
      await client.close()


class SemanticRememberTool(_SemanticMemoryToolMixin, BaseTool):
  """Store a semantic memory for the active graph."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "remember",
      "description": (
        "Store a durable semantic memory for the active graph.\n\n"
        "**WHEN TO USE:**\n"
        "- For facts, decisions, preferences and context worth persisting "
        "across sessions\n"
        "- When something learned in this session should survive it\n\n"
        "**RETURNS:** The stored memory's id, for later `update-memory` or "
        "`forget`.\n\n"
        "**RELATED TOOLS:**\n"
        "- `recall` retrieves what this stores\n"
        "- write-graph-cypher builds structural graph data on subgraphs; this "
        "is vector memory and does not put anything in the graph"
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "text": {
            "type": "string",
            "description": "The memory content to store.",
          },
          "memory_type": {
            "type": "string",
            "description": "Optional classifier, e.g. 'note', 'fact', 'preference'.",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional labels.",
          },
        },
        "required": ["text"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("remember", arguments)
    text = (arguments.get("text") or "").strip()
    if not text:
      return {"error": "invalid_input", "message": "text is required"}

    from robosystems.models.api.memory import MemoryCreateRequest

    request = MemoryCreateRequest(
      text=text,
      source="mcp",
      memory_type=arguments.get("memory_type") or "note",
      tags=arguments.get("tags"),
    )

    async def _run(service, graph_id):
      try:
        record = await service.remember(graph_id, request, created_by=None)
      except Exception as e:
        logger.error(f"remember failed: {e}")
        raise GraphAPIError(f"remember failed: {e}")
      return {"success": True, "memory": record.model_dump(mode="json")}

    return await self._with_service(_run)


class SemanticRecallTool(_SemanticMemoryToolMixin, BaseTool):
  """Recall semantic memories by similarity."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "recall",
      "description": (
        "Recall stored semantic memories for the active graph by meaning "
        "rather than keywords.\n\n"
        "**WHEN TO USE:**\n"
        "- Before answering, to retrieve remembered context\n"
        "- When the user refers to something established in an earlier "
        "session\n\n"
        "**RETURNS:** The top-k most relevant memories with their ids and "
        "similarity scores."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "What to recall (natural language).",
          },
          "k": {
            "type": "integer",
            "description": "Max results to return (1-50, default 10).",
          },
          "memory_type": {
            "type": "string",
            "description": "Optional filter by memory type.",
          },
        },
        "required": ["query"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("recall", arguments)
    query = (arguments.get("query") or "").strip()
    if not query:
      return {"error": "invalid_input", "message": "query is required"}

    from robosystems.models.api.memory import MemoryRecallRequest

    k = arguments.get("k")
    request = MemoryRecallRequest(
      query=query,
      k=k if isinstance(k, int) and 1 <= k <= 50 else 10,
      memory_type=arguments.get("memory_type"),
    )

    async def _run(service, graph_id):
      try:
        response = await service.recall(graph_id, request)
      except Exception as e:
        logger.error(f"recall failed: {e}")
        raise GraphAPIError(f"recall failed: {e}")
      return {
        "total": response.total,
        "results": [
          {"id": h.document_id, "score": h.score, "text": h.snippet, "tags": h.tags}
          for h in response.hits
        ],
      }

    return await self._with_service(_run)


class SemanticUpdateMemoryTool(_SemanticMemoryToolMixin, BaseTool):
  """Partially update a stored semantic memory by id."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "update-memory",
      "description": (
        "Update a stored semantic memory in place by its id.\n\n"
        "**WHEN TO USE:**\n"
        "- To correct or refine a memory — prefer this over forget plus "
        "remember, which mints a new id and drops the memory's history\n\n"
        "**PARAMETERS:**\n"
        "- memory_id: from a prior `remember` or `recall`\n"
        "- Only the fields supplied change; the memory is re-embedded when "
        "`text` changes\n\n"
        "**RETURNS:** The updated memory."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "memory_id": {
            "type": "string",
            "description": "The memory id to update (e.g. 'mem_...').",
          },
          "text": {
            "type": "string",
            "description": "New memory content (omit to keep current).",
          },
          "memory_type": {
            "type": "string",
            "description": "New classifier (omit to keep current).",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "New labels (omit to keep current).",
          },
        },
        "required": ["memory_id"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("update-memory", arguments)
    memory_id = (arguments.get("memory_id") or "").strip()
    if not memory_id:
      return {"error": "invalid_input", "message": "memory_id is required"}

    from robosystems.models.api.memory import MemoryUpdateRequest

    # Forward only the fields the caller actually set, so the service (which
    # keys on model_fields_set) does a true partial update instead of wiping
    # the omitted fields to NULL.
    update_fields: dict[str, Any] = {}
    for field in ("text", "memory_type", "tags"):
      if field in arguments:
        update_fields[field] = arguments[field]
    if not update_fields:
      return {"error": "invalid_input", "message": "No fields to update"}

    request = MemoryUpdateRequest(**update_fields)

    async def _run(service, graph_id):
      try:
        record = await service.update_memory(graph_id, memory_id, request)
      except Exception as e:
        logger.error(f"update-memory failed: {e}")
        raise GraphAPIError(f"update-memory failed: {e}")
      if record is None:
        return {"error": "not_found", "message": "Memory not found"}
      return {"success": True, "memory": record.model_dump(mode="json")}

    return await self._with_service(_run)


class SemanticForgetTool(_SemanticMemoryToolMixin, BaseTool):
  """Delete a semantic memory by id."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "forget",
      "description": (
        "Delete a semantic memory by its id.\n\n"
        "**WHEN TO USE:**\n"
        "- To remove an outdated or incorrect memory\n"
        "- To correct one instead, prefer `update-memory`, which keeps the id "
        "and its history\n\n"
        "**PARAMETERS:**\n"
        "- memory_id: from a prior `remember` or `recall`\n\n"
        "**RETURNS:** Confirmation of the deletion."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "memory_id": {
            "type": "string",
            "description": "The memory id to forget (e.g. 'mem_...').",
          },
        },
        "required": ["memory_id"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("forget", arguments)
    memory_id = (arguments.get("memory_id") or "").strip()
    if not memory_id:
      return {"error": "invalid_input", "message": "memory_id is required"}

    async def _run(service, graph_id):
      try:
        result = await service.forget(graph_id, memory_id)
      except Exception as e:
        logger.error(f"forget failed: {e}")
        raise GraphAPIError(f"forget failed: {e}")
      return {"success": True, "id": result.id, "deleted": result.deleted}

    return await self._with_service(_run)
