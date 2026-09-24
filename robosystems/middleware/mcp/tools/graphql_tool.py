"""Read-only MCP access to the extensions GraphQL schema.

The context is built from the MCP client rather than `get_context`, since the
MCP layer has already authenticated. Mutations and subscriptions are refused,
and a depth / field / alias gate bounds query cost.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

from graphql import (
  DocumentNode,
  OperationType,
  get_introspection_query,
)
from graphql import (
  parse as gql_parse,
)

from robosystems.logger import logger

from .base_tool import BaseTool

# Imported lazily (MCP loads before the app is wired); module-level so tests
# can patch it.
gql_schema: Any = None


def _ensure_gql_schema() -> Any:
  global gql_schema
  if gql_schema is None:
    from robosystems.graphql import schema as _s

    gql_schema = _s
  return gql_schema


# The schema is fixed for the process lifetime, so no TTL.
_SCHEMA_CACHE: dict[str, str] = {}

_MAX_DEPTH = 10
_MAX_FIELDS = 200
_MAX_ALIASES = 20


def _anon_ctx(graph_id: str = "") -> dict[str, Any]:
  return {
    "request": SimpleNamespace(),
    "user": None,
    "graph_id": graph_id,
    "schema_extensions": (),
    "graph_type": "",
  }


def _reject_non_query(doc: DocumentNode) -> str | None:
  for defn in doc.definitions:
    if hasattr(defn, "operation"):
      if defn.operation in (OperationType.MUTATION, OperationType.SUBSCRIPTION):
        return defn.operation.value
  return None


def _walk_complexity(
  selections: Any,
  depth: int,
  fragments: dict[str, Any],
  visited: set[str],
) -> tuple[int, int, int]:
  """Return (max_depth, fields, aliases).

  Fragment spreads are expanded inline, or fields could hide behind named
  fragments; ``visited`` guards against spread cycles.
  """
  max_depth = depth
  total_fields = 0
  total_aliases = 0

  for sel in selections:
    sel_kind = type(sel).__name__
    if sel_kind == "FieldNode":
      total_fields += 1
      if sel.alias is not None:
        total_aliases += 1
      if sel.selection_set:
        child_depth, child_fields, child_aliases = _walk_complexity(
          sel.selection_set.selections, depth + 1, fragments, visited
        )
        max_depth = max(max_depth, child_depth)
        total_fields += child_fields
        total_aliases += child_aliases
    elif sel_kind == "InlineFragmentNode":
      if sel.selection_set:
        child_depth, child_fields, child_aliases = _walk_complexity(
          sel.selection_set.selections, depth, fragments, visited
        )
        max_depth = max(max_depth, child_depth)
        total_fields += child_fields
        total_aliases += child_aliases
    elif sel_kind == "FragmentSpreadNode":
      name = sel.name.value
      if name in visited:
        continue
      fragment = fragments.get(name)
      if fragment is None or not fragment.selection_set:
        continue
      visited.add(name)
      try:
        child_depth, child_fields, child_aliases = _walk_complexity(
          fragment.selection_set.selections, depth, fragments, visited
        )
      finally:
        visited.discard(name)
      max_depth = max(max_depth, child_depth)
      total_fields += child_fields
      total_aliases += child_aliases

  return max_depth, total_fields, total_aliases


def _check_complexity(doc: DocumentNode) -> str | None:
  """Error string when an operation exceeds a limit; unused fragments cost nothing."""
  fragments: dict[str, Any] = {}
  for defn in doc.definitions:
    if type(defn).__name__ == "FragmentDefinitionNode":
      fragments[defn.name.value] = defn

  for defn in doc.definitions:
    if type(defn).__name__ != "OperationDefinitionNode":
      continue
    if not getattr(defn, "selection_set", None):
      continue
    depth, fields, aliases = _walk_complexity(
      defn.selection_set.selections, 1, fragments, set()
    )
    if depth > _MAX_DEPTH:
      return f"Query depth {depth} exceeds limit of {_MAX_DEPTH}"
    if fields > _MAX_FIELDS:
      return f"Field count {fields} exceeds limit of {_MAX_FIELDS}"
    if aliases > _MAX_ALIASES:
      return f"Alias count {aliases} exceeds limit of {_MAX_ALIASES}"
  return None


class GraphqlSchemaTool(BaseTool):
  """Return the extensions GraphQL schema as SDL or introspection JSON."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-graphql-schema",
      "description": (
        "Return the GraphQL schema for the extensions endpoint.\n\n"
        "**WHEN TO USE:**\n"
        "- Before query-graphql, to discover types, fields and input arguments\n"
        "- When a query fails on an unknown field — the schema is "
        "per-deployment and depends on which extensions (roboledger, "
        "roboinvestor) are enabled\n\n"
        "**PARAMETERS:**\n"
        "- ``format``: ``sdl`` (default) is human-readable Schema Definition "
        "Language, best for scanning types and writing queries; "
        "``introspection`` is the full JSON introspection result, for tooling "
        "that consumes the standard format.\n\n"
        "**RETURNS:** The schema in the requested format."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "format": {
            "type": "string",
            "enum": ["sdl", "introspection"],
            "description": "Schema format to return. Defaults to 'sdl'.",
          },
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("get-graphql-schema", arguments)

    format_ = arguments.get("format", "sdl")
    if format_ not in ("sdl", "introspection"):
      return {
        "error": "invalid_argument",
        "message": "format must be 'sdl' or 'introspection'",
      }

    if format_ in _SCHEMA_CACHE:
      return {"schema": _SCHEMA_CACHE[format_]}

    schema = _ensure_gql_schema()

    if format_ == "sdl":
      result = str(schema)
    else:
      r = schema.execute_sync(
        get_introspection_query(),
        context_value=_anon_ctx(),
      )
      if r.errors or r.data is None:
        # Not cached, or the failure would stick until restart.
        logger.warning(f"Introspection query failed: {r.errors}")
        return {
          "error": "introspection_failed",
          "message": "; ".join(str(e.message) for e in (r.errors or []))
          or "no data returned",
        }
      result = json.dumps(r.data)

    _SCHEMA_CACHE[format_] = result
    return {"schema": result}


class GraphqlQueryTool(BaseTool):
  """Execute a read-only GraphQL query against the extensions endpoint."""

  def __init__(self, client: Any, schema_extensions: tuple[str, ...] = ()) -> None:
    super().__init__(client)
    self._schema_extensions = schema_extensions

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "query-graphql",
      "description": (
        "Execute a read-only GraphQL query against the extensions "
        "endpoint.\n\n"
        "**WHEN TO USE:**\n"
        "- For typed reads of extensions data (roboledger, roboinvestor)\n"
        "- After get-graphql-schema, which gives the types and fields to "
        "query against\n\n"
        "**PARAMETERS:**\n"
        "- ``query``: the GraphQL query string. Mutations and subscriptions "
        "are rejected.\n"
        "- **graph_id is set from context** — never pass it as a query "
        "argument. The schema already reflects the graph_id from the URL.\n\n"
        "**RETURNS:** The query result, shaped by the fields requested.\n\n"
        "**NOTES:** A minimal query looks like "
        "``{ fiscalCalendar { closedThrough closeTarget } }``."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "GraphQL query string to execute.",
          },
          "variables": {
            "type": "object",
            "description": "Optional variable values for the query.",
            "additionalProperties": True,
          },
          "operationName": {
            "type": "string",
            "description": "Optional operation name for multi-operation documents.",
          },
        },
        "required": ["query"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("query-graphql", arguments)

    # As on HTTP (`graphql/context.py`): a subgraph has no extensions schema,
    # and resolvers would otherwise dead-end or resolve to the parent.
    from robosystems.middleware.graph.utils.subgraph import is_subgraph

    if is_subgraph(getattr(self.client, "graph_id", "") or ""):
      return {
        "error": "invalid_target",
        "message": (
          "query-graphql is not available on subgraphs; switch the "
          "workspace to the parent graph to use the extensions surface"
        ),
      }

    query = arguments.get("query", "").strip()
    if not query:
      return {"error": "invalid_query", "message": "query is required"}

    # graphql-core raises more than GraphQLSyntaxError on malformed input.
    try:
      doc = gql_parse(query)
    except Exception as e:
      return {"error": "parse_error", "message": str(e)}

    op_kind = _reject_non_query(doc)
    if op_kind:
      return {
        "error": "read_only_violation",
        "message": f"{op_kind} operations are not allowed via MCP",
      }

    complexity_error = _check_complexity(doc)
    if complexity_error:
      return {"error": "query_too_complex", "message": complexity_error}

    schema = _ensure_gql_schema()

    ctx = await self._build_context()
    variables = arguments.get("variables") or {}
    operation_name = arguments.get("operationName")

    result = await schema.execute(
      query,
      context_value=ctx,
      variable_values=variables,
      operation_name=operation_name,
    )

    response: dict[str, Any] = {}
    if result.data is not None:
      response["data"] = result.data
    if result.errors:
      response["errors"] = [
        {
          "message": e.message,
          "extensions": e.extensions or {},
        }
        for e in result.errors
      ]
    return response

  async def _build_context(self) -> dict[str, Any]:
    user = await asyncio.to_thread(self._fetch_user)
    return {
      "request": SimpleNamespace(),
      "user": user,
      "graph_id": self.client.graph_id,
      "schema_extensions": self._schema_extensions,
      # These tools serve user entity graphs only; shared repos never get them.
      "graph_type": "entity",
    }

  def _fetch_user(self) -> Any:
    # By-id lookup only for callers that thread `user_id` without `user`.
    user = getattr(self.client, "user", None)
    if user is not None:
      return user

    user_id = getattr(self.client, "user_id", None)
    if not user_id:
      return None

    from robosystems.database import SessionFactory
    from robosystems.models.core import User

    # Independent session (see platform_session docs).
    db = SessionFactory()
    try:
      return db.get(User, user_id)
    finally:
      db.close()
