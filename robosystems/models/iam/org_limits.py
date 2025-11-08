"""OrgLimits model - Safety limits for organization resource provisioning."""

import secrets
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, String, DateTime, ForeignKey, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship, Session

from ...database import Model
from ...config import env


class OrgLimits(Model):
  """Safety limits to prevent system abuse from runaway resource creation.

  Limits are org-based since orgs are the billing entity.
  When team members join, they share the org's resource limits.
  """

  __tablename__ = "org_limits"

  id = Column(
    String, primary_key=True, default=lambda: f"ol_{secrets.token_urlsafe(16)}"
  )
  org_id = Column(String, ForeignKey("orgs.id"), nullable=False, unique=True)

  max_graphs = Column(Integer, nullable=False, default=env.ORG_GRAPHS_DEFAULT_LIMIT)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  org = relationship("Org")

  def __repr__(self) -> str:
    """String representation of the org limits."""
    return f"<OrgLimits org={self.org_id} max_graphs={self.max_graphs}>"

  @classmethod
  def create_default_limits(cls, org_id: str, session: Session) -> "OrgLimits":
    """Create default safety limits for a new organization."""
    limits = cls(
      org_id=org_id,
      max_graphs=env.ORG_GRAPHS_DEFAULT_LIMIT,
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
  def get_by_org_id(cls, org_id: str, session: Session) -> Optional["OrgLimits"]:
    """Get limits for a specific organization."""
    return session.query(cls).filter(cls.org_id == org_id).first()

  @classmethod
  def get_or_create_for_org(cls, org_id: str, session: Session) -> "OrgLimits":
    """Get existing limits or create default ones for an organization."""
    limits = cls.get_by_org_id(org_id, session)
    if not limits:
      limits = cls.create_default_limits(org_id, session)
    return limits

  def can_create_graph(self, session: Session) -> tuple[bool, str]:
    """
    Check if org can create another graph (safety check only).

    Returns:
        tuple: (can_create: bool, reason: str)
    """
    from .graph_user import GraphUser
    from .org_user import OrgUser

    if self.max_graphs == -1:
      return True, "Can create graph (unlimited)"

    org_user_ids = [
      ou.user_id
      for ou in session.query(OrgUser).filter(OrgUser.org_id == self.org_id).all()
    ]

    current_count = (
      session.query(GraphUser).filter(GraphUser.user_id.in_(org_user_ids)).count()
    )

    if current_count >= self.max_graphs:
      return (
        False,
        f"Organization graph limit reached ({self.max_graphs} graphs). Please contact support if you need more.",
      )

    return True, "Can create graph"

  def get_current_usage(self, session: Session) -> dict:
    """Get current usage statistics for the organization."""
    from .graph_user import GraphUser
    from .org_user import OrgUser
    from .graph import Graph

    org_user_ids = [
      ou.user_id
      for ou in session.query(OrgUser).filter(OrgUser.org_id == self.org_id).all()
    ]

    current_graphs = (
      session.query(GraphUser).filter(GraphUser.user_id.in_(org_user_ids)).count()
    )

    # Get actual graphs owned by the org (new model)
    org_graphs = session.query(Graph).filter(Graph.org_id == self.org_id).count()

    current_count = max(current_graphs, org_graphs)
    remaining_graphs = max(0, self.max_graphs - current_count)

    return {
      "graphs": {
        "current": current_count,
        "limit": self.max_graphs,
        "remaining": remaining_graphs,
      }
    }

  def update_limit(self, new_limit: int, session: Session) -> None:
    """
    Update the graph creation limit for this organization.

    Args:
        new_limit: New maximum number of graphs
        session: Database session
    """
    self.max_graphs = new_limit
    self.updated_at = datetime.now(timezone.utc)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise
