"""Call-time extension gate for MCP write tools.

MCP dispatch bypasses FastAPI DI, so this mirrors `require_graph_extension`:
repository graphs and graphs without the extension are refused, with a typed
`MCPExtensionGateError` rather than an HTTP error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.middleware.extensions import (
  GraphExtensionContext,
  load_graph_metadata,
)
from robosystems.middleware.graph.utils.subgraph import is_subgraph

if TYPE_CHECKING:
  from sqlalchemy.orm import Session


class MCPExtensionGateError(Exception):
  """Serialized as `{"error": code, "message": message}`; `code` is stable."""

  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code
    self.message = message


def _load_with_short_lived_session(graph_id: str) -> GraphExtensionContext:
  # Independent session: the scoped one is the request's own, and closing it
  # here would close it mid-flight.
  from robosystems.database import SessionFactory

  session: Session = SessionFactory()
  try:
    return load_graph_metadata(graph_id, session)
  finally:
    session.close()


def require_graph_extension_mcp(
  extension: str,
  graph_id: str,
  meta: GraphExtensionContext | None = None,
) -> GraphExtensionContext:
  """Return the graph's metadata, or raise MCPExtensionGateError.

  `meta` is the manager's cached lookup; when None it is loaded here.
  """
  # A subgraph has no tenant schema; refuse it as REST and GraphQL do.
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
      # load_graph_metadata raises HTTPException(403) on a missing graph.
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
  """`require_graph_extension_mcp` without the extension check."""
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
