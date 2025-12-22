"""Organization model for multi-tenant billing and resource management."""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Column, DateTime, String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class OrgType(str, Enum):
  PERSONAL = "personal"
  TEAM = "team"
  ENTERPRISE = "enterprise"


class Org(Model):
  """Organization model for grouping users and managing billing."""

  __tablename__ = "orgs"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("org"))
  name = Column(String, nullable=False)
  org_type = Column(SQLEnum(OrgType), nullable=False, default=OrgType.PERSONAL)

  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )
  deleted_at = Column(DateTime, nullable=True)

  users = relationship("OrgUser", back_populates="org", cascade="all, delete-orphan")
  graphs = relationship("Graph", back_populates="org", cascade="all, delete-orphan")

  @property
  def is_deleted(self) -> bool:
    """Check if organization is soft-deleted."""
    return self.deleted_at is not None

  def __repr__(self) -> str:
    return f"<Org {self.id} {self.name} ({self.org_type})>"

  @classmethod
  def get_by_id(
    cls, org_id: str, session: Session, include_deleted: bool = False
  ) -> Optional["Org"]:
    """Get organization by ID.

    Args:
        org_id: Organization ID
        session: Database session
        include_deleted: If True, include soft-deleted orgs (default: False)

    Returns:
        Organization if found and not deleted (unless include_deleted=True)
    """
    query = session.query(cls).filter(cls.id == org_id)
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.first()

  @classmethod
  def create(
    cls,
    name: str,
    org_type: OrgType,
    session: Session,
    auto_commit: bool = True,
  ) -> "Org":
    org = cls(name=name, org_type=org_type)
    session.add(org)
    session.flush()

    if auto_commit:
      try:
        session.commit()
        session.refresh(org)
      except SQLAlchemyError:
        session.rollback()
        raise

    return org

  @classmethod
  def create_personal_org_for_user(
    cls, user_id: str, user_name: str, session: Session
  ) -> "Org":
    """Create a personal organization for a new user.

    RoboSystems is an org-centric platform - all resources (graphs, subscriptions,
    billing) belong to organizations, not individual users. When a user registers,
    we automatically create a personal organization for them as the foundation.

    Users can later upgrade their personal org to a team or enterprise organization
    by inviting members and changing the org type. This provides a smooth onboarding
    experience while maintaining the org-centric architecture.

    Args:
        user_id: The user ID who will be the owner
        user_name: User's name for workspace naming
        session: Database session

    Returns:
        The created personal organization with the user as OWNER
    """
    org = cls.create(
      name="My Organization",
      org_type=OrgType.PERSONAL,
      session=session,
      auto_commit=False,
    )

    from .org_user import OrgRole, OrgUser

    OrgUser.create(
      org_id=org.id,
      user_id=user_id,
      role=OrgRole.OWNER,
      session=session,
      auto_commit=False,
    )

    try:
      session.commit()
      session.refresh(org)
    except SQLAlchemyError:
      session.rollback()
      raise

    return org

  @classmethod
  def get_all(cls, session: Session, include_deleted: bool = False) -> Sequence["Org"]:
    """Get all organizations.

    Args:
        session: Database session
        include_deleted: If True, include soft-deleted orgs (default: False)

    Returns:
        List of organizations (excluding deleted unless include_deleted=True)
    """
    query = session.query(cls)
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.all()

  def update(self, session: Session, **kwargs) -> None:
    for key, value in kwargs.items():
      if hasattr(self, key):
        setattr(self, key, value)
    self.updated_at = datetime.now(UTC)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def soft_delete(self, session: Session) -> None:
    """Soft-delete the organization.

    Marks the organization as deleted without removing data.
    Safety checks prevent deletion of orgs with active subscriptions.

    Raises:
        ValueError: If organization has active subscriptions
    """
    from ..billing import BillingSubscription

    if self.is_deleted:
      return

    active_subscriptions = (
      session.query(BillingSubscription)
      .filter(
        BillingSubscription.org_id == self.id,
        BillingSubscription.status.in_(["active", "pending", "provisioning"]),
      )
      .count()
    )

    if active_subscriptions > 0:
      raise ValueError(
        f"Cannot delete organization with {active_subscriptions} active subscriptions. "
        "Cancel all subscriptions first."
      )

    self.deleted_at = datetime.now(UTC)
    self.updated_at = datetime.now(UTC)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def restore(self, session: Session) -> None:
    """Restore a soft-deleted organization."""
    if not self.is_deleted:
      return

    self.deleted_at = None
    self.updated_at = datetime.now(UTC)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Hard delete the organization (DANGEROUS - use soft_delete instead).

    This permanently removes the organization and all related data.
    Only use for testing or with extreme caution in production.
    """
    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def get_users(self, session: Session):
    """Get all users of this organization."""
    from .org_user import OrgUser

    return session.query(OrgUser).filter(OrgUser.org_id == self.id).all()

  def get_user_count(self, session: Session) -> int:
    """Get count of organization users."""
    from .org_user import OrgUser

    return session.query(OrgUser).filter(OrgUser.org_id == self.id).count()

  def has_user(self, user_id: str, session: Session) -> bool:
    """Check if a user is a member of this organization."""
    from .org_user import OrgUser

    return (
      session.query(OrgUser)
      .filter(
        OrgUser.org_id == self.id,
        OrgUser.user_id == user_id,
      )
      .first()
      is not None
    )

  def get_owner(self, session: Session):
    """Get the owner of this organization."""
    from .org_user import OrgRole, OrgUser

    return (
      session.query(OrgUser)
      .filter(
        OrgUser.org_id == self.id,
        OrgUser.role == OrgRole.OWNER,
      )
      .first()
    )
