"""UserLimits model - Simple safety valve to prevent runaway graph creation."""

import secrets
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, String, DateTime, ForeignKey, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship, Session

from ...database import Model
from ...config import env


class UserLimits(Model):
  """Safety limits to prevent system abuse from runaway graph creation."""

  __tablename__ = "user_limits"

  id = Column(
    String, primary_key=True, default=lambda: f"ul_{secrets.token_urlsafe(16)}"
  )
  user_id = Column(String, ForeignKey("users.id"), nullable=False, unique=True)

  # Safety limit - prevent runaway graph creation that could crash the system
  # Set high enough for legitimate use (e.g., customer wants 50 graphs)
  # But low enough to prevent accidents (e.g., script gone wrong creating 10,000)
  max_user_graphs = Column(
    Integer, nullable=False, default=env.USER_GRAPHS_DEFAULT_LIMIT
  )

  # Metadata
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
  user = relationship("User", back_populates="limits")

  def __repr__(self) -> str:
    """String representation of the user limits."""
    return f"<UserLimits user={self.user_id} max_graphs={self.max_user_graphs}>"

  @classmethod
  def create_default_limits(cls, user_id: str, session: Session) -> "UserLimits":
    """Create default safety limits for a new user."""
    limits = cls(
      user_id=user_id,
      max_user_graphs=env.USER_GRAPHS_DEFAULT_LIMIT,  # Configurable default - safety valve only
    )
    session.add(limits)
    try:
      session.commit()
      session.refresh(limits)
    except SQLAlchemyError:
      session.rollback()
      raise
    return limits

  @classmethod
  def get_by_user_id(cls, user_id: str, session: Session) -> Optional["UserLimits"]:
    """Get limits for a specific user."""
    return session.query(cls).filter(cls.user_id == user_id).first()

  @classmethod
  def get_or_create_for_user(cls, user_id: str, session: Session) -> "UserLimits":
    """Get existing limits or create default ones for a user."""
    limits = cls.get_by_user_id(user_id, session)
    if not limits:
      limits = cls.create_default_limits(user_id, session)
    return limits

  def can_create_user_graph(self, session: Session) -> tuple[bool, str]:
    """
    Check if user can create another graph (safety check only).

    Returns:
        tuple: (can_create: bool, reason: str)
    """
    from .user_graph import UserGraph

    current_count = (
      session.query(UserGraph).filter(UserGraph.user_id == self.user_id).count()
    )

    if current_count >= self.max_user_graphs:
      return (
        False,
        f"Safety limit reached ({self.max_user_graphs} graphs). Please contact support if you need more.",
      )

    return True, "Can create graph"

  def get_current_usage(self, session: Session) -> dict:
    """Get current usage statistics for the user."""
    from .user_graph import UserGraph

    current_graphs = (
      session.query(UserGraph).filter(UserGraph.user_id == self.user_id).count()
    )

    # Calculate remaining graphs
    remaining_graphs = max(0, self.max_user_graphs - current_graphs)

    # Return simple usage statistics
    return {
      "graphs": {
        "current": current_graphs,
        "limit": self.max_user_graphs,
        "remaining": remaining_graphs,
      }
    }

  def update_limit(self, new_limit: int, session: Session) -> None:
    """
    Update the graph creation limit for this user.

    Args:
        new_limit: New maximum number of graphs
        session: Database session
    """
    self.max_user_graphs = new_limit
    self.updated_at = datetime.now(timezone.utc)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise
