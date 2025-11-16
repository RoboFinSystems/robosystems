"""GraphUser model for graph access control.

Access Control Model:
- Graphs are owned by organizations (Graph.org_id)
- Only users within the organization can be granted access
- This model tracks which specific users have access to which graphs
- Roles: admin (full control), member (read/write), viewer (read-only)
"""

from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import (
  Column,
  String,
  DateTime,
  ForeignKey,
  Boolean,
  UniqueConstraint,
  Index,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship, Session

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class GraphUser(Model):
  """GraphUser model for managing user access to graph databases."""

  __tablename__ = "graph_users"
  __table_args__ = (
    UniqueConstraint("graph_id", "user_id", name="_graph_user_uc"),
    Index("idx_graph_users_graph_user_id", "graph_id", "user_id"),
    Index("idx_graph_users_user_selected", "user_id", "is_selected"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("gu"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  graph_id = Column(
    String, ForeignKey("graphs.graph_id"), nullable=False, index=True
  )  # References graphs table
  role = Column(String, nullable=False, default="member")  # admin, member, viewer
  is_selected = Column(Boolean, default=False, nullable=False)  # Currently active graph
  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  # Relationships
  user = relationship("User", back_populates="graph_users")
  graph = relationship("Graph", back_populates="graph_users")

  def __repr__(self) -> str:
    """String representation of the graph-user relationship."""
    return f"<GraphUser {self.id} graph={self.graph_id} user={self.user_id} role={self.role}>"

  @classmethod
  def create(
    cls,
    user_id: str,
    graph_id: str,
    role: str = "member",
    is_selected: bool = False,
    session: Optional[Session] = None,
  ) -> "GraphUser":
    """Create a new graph-user access relationship."""
    if session is None:
      raise ValueError("Session is required for GraphUser creation")

    graph_user = cls(
      user_id=user_id,
      graph_id=graph_id,
      role=role,
      is_selected=is_selected,
    )

    session.add(graph_user)
    try:
      session.commit()
      session.refresh(graph_user)
    except SQLAlchemyError:
      session.rollback()
      raise
    return graph_user

  @classmethod
  def get_by_user_id(cls, user_id: str, session: Session) -> Sequence["GraphUser"]:
    """Get all graph relationships for a user."""
    return session.query(cls).filter(cls.user_id == user_id).all()

  @classmethod
  def get_by_graph_id(cls, graph_id: str, session: Session) -> Sequence["GraphUser"]:
    """Get all user relationships for a graph."""
    return session.query(cls).filter(cls.graph_id == graph_id).all()

  @classmethod
  def get_by_user_and_graph(
    cls, user_id: str, graph_id: str, session: Session
  ) -> Optional["GraphUser"]:
    """Get a specific user-graph relationship."""
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

  @classmethod
  def get_selected_graph(cls, user_id: str, session: Session) -> Optional["GraphUser"]:
    """Get the currently selected graph for a user."""
    return session.query(cls).filter(cls.user_id == user_id, cls.is_selected).first()

  @classmethod
  def set_selected_graph(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """Set a graph as the selected one for a user."""
    # Find the target graph first
    graph_user = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

    if not graph_user:
      return False

    try:
      # Perform both operations in a single transaction to avoid race conditions
      # First, deselect all graphs for this user
      session.query(cls).filter(cls.user_id == user_id).update({"is_selected": False})

      # Then select the specific graph
      graph_user.is_selected = True
      graph_user.updated_at = datetime.now(timezone.utc)

      session.commit()
      session.refresh(graph_user)
      return True
    except SQLAlchemyError:
      session.rollback()
      raise

  @classmethod
  def user_has_access(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """
    Check if a user has access to a specific graph.

    For subgraphs (e.g., 'kg123_dev'), this method checks access to the parent graph ('kg123')
    since subgraphs inherit permissions from their parent.
    """
    from ...middleware.graph.types import parse_graph_id

    # Resolve subgraph to parent graph for permission check
    # Subgraphs inherit parent's permissions
    parent_id, _ = parse_graph_id(graph_id)

    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == parent_id)
      .first()
      is not None
    )

  @classmethod
  def user_has_admin_access(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """
    Check if a user has admin access to a specific graph.

    For subgraphs (e.g., 'kg123_dev'), this method checks admin access to the parent graph ('kg123')
    since subgraphs inherit permissions from their parent.
    """
    from ...middleware.graph.types import parse_graph_id

    # Resolve subgraph to parent graph for permission check
    # Subgraphs inherit parent's permissions
    parent_id, _ = parse_graph_id(graph_id)

    graph_user = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == parent_id)
      .first()
    )

    return graph_user is not None and graph_user.role == "admin"

  def update_role(self, role: str, session: Session) -> None:
    """Update the user's role for this graph."""
    self.role = role
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Remove the user-graph relationship."""
    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
