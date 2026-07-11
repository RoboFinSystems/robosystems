"""
GraphQL Tools — read-only escape hatch for typed OLTP queries via MCP.

Two Layer 1 tools exposing the extensions GraphQL surface to agents:

- ``get-graphql-schema`` — returns the schema SDL (or JSON introspection) so
  agents can discover types and fields before writing queries.
- ``query-graphql``       — executes a parameterized read-only GraphQL query
  against the same schema served at ``/extensions/{graph_id}/graphql``.

Both tools reuse the existing Strawberry schema, auth model, and per-extension
gating. The key difference from the HTTP path is that ``get_context`` is
bypassed — the MCP layer has already authenticated the request, so the context
is constructed directly from the client's graph_id / user_id / schema_extensions.

Mutations and subscriptions are rejected before execution. A simple depth /
field / alias complexity gate prevents runaway queries.
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

# Lazy module-level reference populated on first access. Kept at module scope
# so tests can patch ``robosystems.middleware.mcp.tools.graphql_tool.gql_schema``.
# The import is deferred to avoid pulling in Strawberry/resolvers during the
# MCP module import phase (which happens before the FastAPI app is fully wired).
gql_schema: Any = None


def _ensure_gql_schema() -> Any:
  global gql_schema
  if gql_schema is None:
    from robosystems.graphql import schema as _s

    gql_schema = _s
  return gql_schema


# Process-lifetime cache — the Strawberry schema is built once at startup and
# never changes without a restart, so TTL is not needed here.
_SCHEMA_CACHE: dict[str, str] = {}

# Complexity limits (tunable via SSM in the future)
_MAX_DEPTH = 10
_MAX_FIELDS = 200
_MAX_ALIASES = 20


def _anon_ctx(graph_id: str = "") -> dict[str, Any]:
  """Minimal context for introspection queries (user=None is acceptable)."""
  return {
    "request": SimpleNamespace(),
    "user": None,
    "graph_id": graph_id,
    "schema_extensions": (),
    "graph_type": "",
  }


def _reject_non_query(doc: DocumentNode) -> str | None:
  """Return the offending operation kind if a mutation/subscription is found."""
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
  """Recursively walk a selection set, returning (max_depth, fields, aliases).

  ``fragments`` maps fragment name → ``FragmentDefinitionNode`` so that
  ``FragmentSpreadNode``s can be expanded inline — counting their fields the
  same way they would actually execute. Without this expansion, agents could
  bypass the field-count limit by hiding fields behind named fragments.

  ``visited`` tracks fragment names currently being expanded, providing
  cycle protection on top of GraphQL's own cyclic-spread prohibition.
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
        # Cycle — GraphQL spec already forbids this; defense in depth.
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
  """Return an error string if the document exceeds complexity limits.

  Only ``OperationDefinitionNode``s are walked; ``FragmentDefinitionNode``s
  are inlined into operations via spread expansion. An unused fragment
  contributes zero cost, which is correct — it never executes.
  """
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
  """Return the extensions GraphQL schema SDL or JSON introspection document.

  Agents should call this once per conversation to discover available types
  and fields before calling ``query-graphql``.
  """

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-graphql-schema",
      "description": (
        "Return the GraphQL schema for the extensions endpoint. "
        "Call this before query-graphql to discover types, fields, and input "
        "arguments. The schema is per-deployment — fields depend on which "
        "extensions (roboledger, roboinvestor) are enabled.\n\n"
        "**format options:**\n"
        "- ``sdl`` (default) — human-readable Schema Definition Language; "
        "best for scanning types and writing queries.\n"
        "- ``introspection`` — full JSON introspection result; useful for "
        "programmatic tooling that consumes the standard introspection format."
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
        # Don't cache failure — let the next call retry. Caching ``"null"``
        # would be a permanent process-lifetime poison pill until restart.
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
  """Execute a read-only GraphQL query against the extensions endpoint.

  Call ``get-graphql-schema`` first to discover available fields.
  Mutations and subscriptions are rejected before execution.
  """

  def __init__(self, client: Any, schema_extensions: tuple[str, ...] = ()) -> None:
    super().__init__(client)
    self._schema_extensions = schema_extensions

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "query-graphql",
      "description": (
        "Execute a read-only GraphQL query against the extensions endpoint. "
        "Call get-graphql-schema first to discover types and fields. "
        "Mutations and subscriptions are rejected.\n\n"
        "**Usage pattern:**\n"
        "1. Call get-graphql-schema to get the SDL.\n"
        "2. Write a query against the SDL.\n"
        "3. Call query-graphql with the query string.\n\n"
        "**graph_id is always set from context** — do not pass it as a "
        "query argument. The schema reflects the graph_id from the URL.\n\n"
        "**Example:**\n"
        "```graphql\n"
        "{ fiscalCalendar { closedThrough closeTarget } }\n"
        "```"
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

    # Subgraph guard — mirrors the HTTP surface (`graphql/context.py`):
    # a subgraph is a modality container, not an extensions domain target,
    # and has no extensions schema. Reject up front instead of letting
    # resolvers dead-end (or silently resolve to the parent) downstream.
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

    # Parse first — surface syntax errors before mutation check.
    # ``GraphQLSyntaxError`` is the typical case, but graphql-core can raise
    # other ``GraphQLError`` subclasses for malformed documents (e.g. unicode
    # issues), so catch them all uniformly.
    try:
      doc = gql_parse(query)
    except Exception as e:
      return {"error": "parse_error", "message": str(e)}

    # Mutation / subscription gate — checked before schema execution so
    # this is authoritative regardless of schema configuration.
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
    # ``_fetch_user`` is sync (uses the platform DB sync session). Run it on
    # a worker thread so the event loop isn't blocked while the DB query
    # is in flight.
    user = await asyncio.to_thread(self._fetch_user)
    return {
      "request": SimpleNamespace(),
      "user": user,
      "graph_id": self.client.graph_id,
      "schema_extensions": self._schema_extensions,
      # Hardcoded — the GraphQL MCP tools are intentionally scoped to user
      # entity graphs. Shared repositories (SEC, library) are read-only and
      # exercise different MCP paths; if a resolver ever branches on
      # graph_type for a shared-repo MCP call, that's a bug to surface.
      "graph_type": "entity",
    }

  def _fetch_user(self) -> Any:
    # The MCP handler attaches the authenticated User object to the client
    # (`client.user`); use it directly rather than re-fetching by id. Fall
    # back to a by-id lookup for callers that only thread `user_id`.
    user = getattr(self.client, "user", None)
    if user is not None:
      return user

    user_id = getattr(self.client, "user_id", None)
    if not user_id:
      return None

    from robosystems.database import get_db_session
    from robosystems.models.core import User

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      return db.get(User, user_id)
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
