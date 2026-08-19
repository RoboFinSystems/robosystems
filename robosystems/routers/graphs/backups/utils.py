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


def verify_graph_access(
  current_user: User, graph_id: str, db: Session, *, allow_deprovisioned: bool = False
) -> None:
  """Verify the user has access to the graph, raising 403 otherwise.

  ``allow_deprovisioned`` is set only by the backup export path, so a departing
  org's OWNER/ADMIN can list/export a torn-down graph during the grace period.
  """
  if not GraphUser.user_has_access(
    current_user.id, graph_id, db, allow_deprovisioned=allow_deprovisioned
  ):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this graph"
    )


def verify_admin_access(
  current_user: User, graph_id: str, db: Session, *, allow_deprovisioned: bool = False
) -> None:
  """Verify the user has admin access to the graph, raising 403 otherwise.

  ``allow_deprovisioned`` is set only by the backup download path (see
  ``verify_graph_access``).
  """
  if not GraphUser.user_has_admin_access(
    current_user.id, graph_id, db, allow_deprovisioned=allow_deprovisioned
  ):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access required for this operation",
    )
