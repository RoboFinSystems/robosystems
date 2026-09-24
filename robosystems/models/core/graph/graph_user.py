"""Per-graph access grants for members of the owning org.

Roles: admin, member (read/write), viewer (read-only). Org OWNER/ADMIN hold
implicit graph admin on every org-owned graph; GraphUser rows grant everyone
else.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid


class GraphRole(str, Enum):
  """Ordered per-graph roles: viewer < member < admin."""

  VIEWER = "viewer"
  MEMBER = "member"
  ADMIN = "admin"

  @property
  def rank(self) -> int:
    return _ROLE_ORDER.index(self)

  def at_least(self, required: "GraphRole") -> bool:
    """True when this role meets or exceeds the required role."""
    return self.rank >= required.rank

  @classmethod
  def coerce(cls, value: "str | GraphRole") -> "GraphRole":
    """Normalize a role value, raising ValueError for unknown strings."""
    if isinstance(value, cls):
      return value
    return cls(value)


_ROLE_ORDER = [GraphRole.VIEWER, GraphRole.MEMBER, GraphRole.ADMIN]


class GraphUser(Model):
  """GraphUser model for managing user access to graph databases."""

  __tablename__ = "graph_users"
  __table_args__ = (
    UniqueConstraint("graph_id", "user_id", name="_graph_user_uc"),
    Index("idx_graph_users_graph_user_id", "graph_id", "user_id"),
    Index("idx_graph_users_user_selected", "user_id", "is_selected"),
    CheckConstraint(
      "role IN ('viewer', 'member', 'admin')", name="ck_graph_users_role"
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("gu"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  graph_id = Column(String, ForeignKey("graphs.graph_id"), nullable=False, index=True)
  role = Column(String, nullable=False, default=GraphRole.MEMBER.value)
  is_selected = Column(Boolean, default=False, nullable=False)  # user's current graph
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  user = relationship("User", back_populates="graph_users")
  graph = relationship("Graph", back_populates="graph_users")

  def __repr__(self) -> str:
    return f"<GraphUser {self.id} graph={self.graph_id} user={self.user_id} role={self.role}>"

  @classmethod
  def create(
    cls,
    user_id: str,
    graph_id: str,
    role: str | GraphRole = GraphRole.MEMBER,
    is_selected: bool = False,
    session: Session | None = None,
  ) -> "GraphUser":
    """Create a new graph-user access relationship."""
    if session is None:
      raise ValueError("Session is required for GraphUser creation")

    graph_user = cls(
      user_id=user_id,
      graph_id=graph_id,
      role=GraphRole.coerce(role).value,
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
    graph_user = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == graph_id)
      .first()
    )

    if not graph_user:
      return False

    try:
      # Deselect-all and select in one transaction.
      session.query(cls).filter(cls.user_id == user_id).update({"is_selected": False})

      graph_user.is_selected = True
      graph_user.updated_at = datetime.now(UTC)

      session.commit()
      session.refresh(graph_user)
      return True
    except SQLAlchemyError:
      session.rollback()
      raise

  @classmethod
  def get_effective_role(
    cls,
    user_id: str,
    graph_id: str,
    session: Session,
    *,
    allow_deprovisioned: bool = False,
  ) -> tuple[GraphRole | None, bool]:
    """Resolve the user's effective role on a graph, as ``(role, implicit)``.

    ``role`` is None when the user has no access; ``implicit`` is True when the
    role comes from the owning org rather than an explicit GraphUser row.

    Subgraphs resolve to their parent. The effective role is the stronger of
    the explicit row and the org OWNER/ADMIN implicit admin grant.

    A missing, deprovisioned, or ``deleted_at``-stamped graph (or subgraph)
    resolves to no role for everyone, org owners included; every authorizer
    relies on this denial for tenant isolation.

    ``allow_deprovisioned`` is passed only by the backup list/download paths,
    so owners can export during the grace period. It never admits a missing
    graph. Do not widen its use.
    """
    from robosystems.middleware.graph.types import parse_graph_id
    from robosystems.models.core.graph.graph import Graph, GraphStatus

    parent_id, _ = parse_graph_id(graph_id)

    graph_ids = {parent_id, graph_id}
    rows = (
      session.query(Graph.graph_id, Graph.org_id, Graph.status, Graph.deleted_at)
      .filter(Graph.graph_id.in_(graph_ids))
      .all()
    )
    by_id = {r.graph_id: r for r in rows}
    parent_row = by_id.get(parent_id)
    if parent_row is None:
      return None, False
    if not allow_deprovisioned:
      for row in by_id.values():
        if row.status == GraphStatus.DEPROVISIONED.value or row.deleted_at is not None:
          return None, False

    explicit_role: GraphRole | None = None
    membership = (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.graph_id == parent_id)
      .first()
    )
    if membership is not None:
      try:
        explicit_role = GraphRole.coerce(membership.role)
      except ValueError:
        explicit_role = GraphRole.VIEWER

    if explicit_role == GraphRole.ADMIN:
      return GraphRole.ADMIN, False

    from robosystems.models.core.org.org_user import OrgRole, OrgUser

    org_id = parent_row.org_id
    if org_id is not None:
      org_user = OrgUser.get_by_org_and_user(org_id, user_id, session)
      if org_user is not None and org_user.role in (OrgRole.OWNER, OrgRole.ADMIN):
        return GraphRole.ADMIN, True

    return explicit_role, False

  @classmethod
  def user_has_access(
    cls,
    user_id: str,
    graph_id: str,
    session: Session,
    *,
    allow_deprovisioned: bool = False,
  ) -> bool:
    """Whether the user has any role on the graph (see ``get_effective_role``)."""
    role, _ = cls.get_effective_role(
      user_id, graph_id, session, allow_deprovisioned=allow_deprovisioned
    )
    return role is not None

  @classmethod
  def user_has_write_access(cls, user_id: str, graph_id: str, session: Session) -> bool:
    """Whether the user's role is member or admin."""
    role, _ = cls.get_effective_role(user_id, graph_id, session)
    return role is not None and role.at_least(GraphRole.MEMBER)

  @classmethod
  def user_has_admin_access(
    cls,
    user_id: str,
    graph_id: str,
    session: Session,
    *,
    allow_deprovisioned: bool = False,
  ) -> bool:
    """Check if a user has admin access to a specific graph."""
    role, _ = cls.get_effective_role(
      user_id, graph_id, session, allow_deprovisioned=allow_deprovisioned
    )
    return role is not None and role.at_least(GraphRole.ADMIN)

  def update_role(self, role: str | GraphRole, session: Session) -> None:
    """Update the user's role for this graph."""
    self.role = GraphRole.coerce(role).value
    self.updated_at = datetime.now(UTC)
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
