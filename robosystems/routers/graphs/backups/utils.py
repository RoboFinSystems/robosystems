"""
Shared utilities for backup operations.
"""

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.core import GraphUser, User
from robosystems.operations.graph.engine.backup_manager import create_backup_manager

# Lazy initialization of backup manager to avoid S3 connection during import
_backup_manager = None


def get_backup_manager():
  """Get or create backup manager instance."""
  global _backup_manager
  if _backup_manager is None:
    _backup_manager = create_backup_manager()
  return _backup_manager


def verify_graph_access(current_user: User, graph_id: str, db: Session) -> None:
  """Verify the user has access to the graph, raising 403 otherwise."""
  if not GraphUser.user_has_access(current_user.id, graph_id, db):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this graph"
    )


def verify_admin_access(current_user: User, graph_id: str, db: Session) -> None:
  """Verify the user has admin access to the graph, raising 403 otherwise."""
  if not GraphUser.user_has_admin_access(current_user.id, graph_id, db):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access required for this operation",
    )


def restore_unsupported_reason(graph_id: str, db: Session) -> tuple[int, str] | None:
  """Why this graph can't be restored to as ``(status_code, detail)``, or None.

  Restore is a graph-type property, not a property of an individual backup.
  Shared repositories are platform-managed and download-only; entity graphs
  are a projection of the extensions OLTP database, so overwriting one with a
  snapshot desynchronizes it from its source of truth.

  Fails closed on an unresolvable graph: a restore is destructive, so an
  unknown graph type is a refusal rather than a default-allow.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core import Graph

  if is_shared_repository_or_subgraph(graph_id):
    return (
      status.HTTP_403_FORBIDDEN,
      f"Restore operations are not allowed on shared repository '{graph_id}'. "
      "Shared repositories are platform-managed and download-only.",
    )

  graph_record = Graph.get_by_id(graph_id, db)
  if graph_record is None:
    return (
      status.HTTP_400_BAD_REQUEST,
      f"Graph '{graph_id}' was not found in the registry, so its type cannot be "
      "confirmed. Restore is refused rather than attempted.",
    )

  if graph_record.graph_type == "entity":
    return (
      status.HTTP_400_BAD_REQUEST,
      "Cannot restore backups for entity graphs. The graph is automatically "
      "materialized from the extensions database. Use the materialize operation "
      "instead.",
    )

  return None
