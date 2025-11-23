"""
Shared utilities for backup operations.
"""

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphUser
from robosystems.operations.lbug.backup_manager import create_backup_manager


# Lazy initialization of backup manager to avoid S3 connection during import
_backup_manager = None


def get_backup_manager():
  """Get or create backup manager instance."""
  global _backup_manager
  if _backup_manager is None:
    _backup_manager = create_backup_manager()
  return _backup_manager


def verify_graph_access(current_user: User, graph_id: str, db: Session) -> None:
  """
  Verify user has access to the specified graph.

  Args:
      current_user: Authenticated user
      graph_id: Graph identifier to check access for
      db: Database session

  Raises:
      HTTPException: If user doesn't have access to the graph
  """
  user_graphs = GraphUser.get_by_user_id(current_user.id, db)
  user_graph_ids = [ug.graph_id for ug in user_graphs]

  if graph_id not in user_graph_ids:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this graph"
    )


def verify_admin_access(current_user: User, graph_id: str, db: Session) -> GraphUser:
  """
  Verify user has admin access to the specified graph.

  Args:
      current_user: Authenticated user
      graph_id: Graph identifier to check admin access for
      db: Database session

  Returns:
      GraphUser object with admin role

  Raises:
      HTTPException: If user doesn't have admin access to the graph
  """
  user_graphs = GraphUser.get_by_user_id(current_user.id, db)
  user_graph = next((ug for ug in user_graphs if ug.graph_id == graph_id), None)

  if not user_graph or user_graph.role != "admin":
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access required for this operation",
    )

  return user_graph
