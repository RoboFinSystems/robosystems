"""UserGraph model for multitenant graph access control."""

import secrets
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


class UserGraph(Model):
  """UserGraph model for managing user access to graph databases."""

  __tablename__ = "user_graphs"
  __table_args__ = (
    UniqueConstraint("user_id", "graph_id", name="_user_graph_uc"),
    Index("idx_user_graphs_user_graph_id", "user_id", "graph_id"),
    Index("idx_user_graphs_user_selected", "user_id", "is_selected"),
  )

  id = Column(
    String, primary_key=True, default=lambda: f"ug_{secrets.token_urlsafe(16)}"
  )
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
  user = relationship("User", back_populates="user_graphs")
  graph = relationship("Graph", back_populates="user_graphs")

  def __repr__(self) -> str:
    """String representation of the user graph relationship."""
    return f"<UserGraph {self.id} user={self.user_id} graph={self.graph_id} role={self.role}>"

  @classmethod
  def create(
    cls,
    user_id: str,
    graph_id: str,
    role: str = "member",
    is_selected: bool = False,
    session: Optional[Session] = None,
  ) -> "UserGraph":
    """Create a new user-graph relationship."""
    if session is None:
      raise ValueError("Session is required for UserGraph creation")

    user_graph = cls(
      user_id=user_id,
      graph_id=graph_id,
      role=role,
      is_selected=is_selected,
    )

    session.add(user_graph)
    try:
      session.commit()
      session.refresh(user_graph)
    except SQLAlchemyError:
      session.rollback()
      raise
    return user_graph

  @classmethod
  def get_by_user_id(cls, user_id: str, session: Session) -> Sequence["UserGraph"]:
    """Get all graph relationships for a user."""
    return session.query(cls).filter(cls.user_id == user_id).all()

  @classmethod
  def get_by_graph_id(cls, graph_id: str, session: Session) -> Sequence["UserGraph"]:
    """Get all user relationships for a graph."""
    return session.query(cls).filter(cls.graph_id == graph_id).all()

  @classmethod
  def get_by_user_and_graph(
    cls, user_id: str, graph_id: str, session: Session
  ) -> Optional["UserGraph"]:
    """Get a specific user-graph relationship."""
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

  @classmethod
  def get_selected_graph(cls, user_id: str, session: Session) -> Optional["UserGraph"]:
    """Get the currently selected graph for a user."""
    return session.query(cls).filter(cls.user_id == user_id, cls.is_selected).first()

  @classmethod
  def set_selected_graph(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """Set a graph as the selected one for a user."""
    # Find the target graph first
    user_graph = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

    if not user_graph:
      return False

    try:
      # Perform both operations in a single transaction to avoid race conditions
      # First, deselect all graphs for this user
      session.query(cls).filter(cls.user_id == user_id).update({"is_selected": False})

      # Then select the specific graph
      user_graph.is_selected = True
      user_graph.updated_at = datetime.now(timezone.utc)

      session.commit()
      session.refresh(user_graph)
      return True
    except SQLAlchemyError:
      session.rollback()
      raise

  @classmethod
  def user_has_access(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """Check if a user has access to a specific graph."""
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
      is not None
    )

  @classmethod
  def user_has_admin_access(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """Check if a user has admin access to a specific graph."""
    user_graph = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

    return user_graph is not None and user_graph.role == "admin"

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
