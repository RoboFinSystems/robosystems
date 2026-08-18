"""Extension feature gate for MCP write tools.

The REST and GraphQL surfaces gate write operations via
`middleware/extensions.py:require_graph_extension` as a FastAPI dependency.
MCP tools dispatch through `GraphMCPTools.call_tool`, which bypasses FastAPI
DI entirely — so a parallel call-time gate is needed.

This module mirrors the REST contract:

  - Repository graphs (shared repos like SEC) are rejected for command writes.
    Ingestion pipelines are the only legitimate write path into shared tenant
    schemas.
  - Graphs whose `schema_extensions` doesn't list the required extension are
    rejected explicitly, rather than falling through to a "schema missing"
    DB error.

The gate raises `MCPExtensionGateError`, not `HTTPException`. The manager
translates that into the MCP error envelope so callers get a typed, parseable
error code instead of a transport-layer 403.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.database import get_db_session
from robosystems.middleware.extensions import (
  GraphExtensionContext,
  load_graph_metadata,
)
from robosystems.middleware.graph.utils.subgraph import is_subgraph

if TYPE_CHECKING:
  from sqlalchemy.orm import Session


class MCPExtensionGateError(Exception):
  """Raised by the gate when a tool is disallowed on the current graph.

  Carries a stable `code` (e.g. `"repository_write_forbidden"`,
  `"extension_not_provisioned"`, `"access_denied"`) so callers can branch on
  it, and a human-readable `message` for surface. The manager serializes this
  to `{"error": code, "message": message}` on the wire.
  """

  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code
    self.message = message


def _load_with_short_lived_session(graph_id: str) -> GraphExtensionContext:
  """Open a platform DB session, load metadata, close the session.

  MCP handlers don't receive a FastAPI `Depends`-injected session, so the
  gate opens its own. The session lifetime is one lookup — no concurrency
  concerns, no long-held connections.
  """
  db_gen = get_db_session()
  session: Session = next(db_gen)
  try:
    return load_graph_metadata(graph_id, session)
  finally:
    try:
      next(db_gen)
    except StopIteration:
      pass


def require_graph_extension_mcp(
  extension: str,
  graph_id: str,
  meta: GraphExtensionContext | None = None,
) -> GraphExtensionContext:
  """Call-time gate for MCP write tools.

  Args:
      extension: e.g. `"roboledger"` / `"roboinvestor"`. The extension must
          appear in `graph.schema_extensions`.
      graph_id: The graph being written to.
      meta: Optional pre-loaded metadata. Supplied by the manager when the
          handler caches the lookup across multiple tool calls in one
          request. When `None`, the gate opens a short-lived platform DB
          session to load it.

  Returns:
      `GraphExtensionContext` so callers that need the graph type or
      extension list don't re-query.

  Raises:
      MCPExtensionGateError: repo write, missing extension, or access denied.
  """
  # A subgraph is a modality container with no tenant schema; refuse it here
  # (the same answer the REST extension dependency and the GraphQL endpoint
  # give) instead of letting the session factory reject the id inside the
  # tool as a 404 or a 500.
  if is_subgraph(graph_id):
    raise MCPExtensionGateError(
      code="subgraph_not_addressable",
      message=(
        f"Subgraph '{graph_id}' is not addressable via {extension} tools; "
        "target the parent graph."
      ),
    )

  if meta is None:
    try:
      meta = _load_with_short_lived_session(graph_id)
    except Exception as exc:
      # load_graph_metadata raises HTTPException(403) on missing graph.
      # Translate to MCP-native error so the manager's error envelope sees
      # a typed failure, not a transport exception.
      raise MCPExtensionGateError(
        code="access_denied",
        message=f"Access denied to graph: {graph_id}",
      ) from exc

  if meta.is_repository or meta.graph_type == "repository":
    raise MCPExtensionGateError(
      code="repository_write_forbidden",
      message=f"{extension} commands are not available on repository graphs",
    )
  if extension not in meta.schema_extensions:
    raise MCPExtensionGateError(
      code="extension_not_provisioned",
      message=f"{extension} is not provisioned for this graph",
    )
  return meta


def require_not_repository_mcp(
  graph_id: str,
  meta: GraphExtensionContext | None = None,
) -> GraphExtensionContext:
  """Subset gate for tools that aren't extension-bound but still must
  reject repository graphs (e.g. memory/workspace/document tools).

  Same contract as `require_graph_extension_mcp` but skips the extension
  presence check.
  """
  if meta is None:
    try:
      meta = _load_with_short_lived_session(graph_id)
    except Exception as exc:
      raise MCPExtensionGateError(
        code="access_denied",
        message=f"Access denied to graph: {graph_id}",
      ) from exc

  if meta.is_repository or meta.graph_type == "repository":
    raise MCPExtensionGateError(
      code="repository_write_forbidden",
      message="This operation is not available on repository graphs",
    )
  return meta


__all__ = [
  "MCPExtensionGateError",
  "require_graph_extension_mcp",
  "require_not_repository_mcp",
]
