"""Organization model for multi-tenant billing and resource management."""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Column, DateTime, String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid


class OrgType(str, Enum):
  PERSONAL = "personal"
  TEAM = "team"
  ENTERPRISE = "enterprise"


class Org(Model):
  """The billing and ownership boundary: users, graphs, subscriptions.

  Deletion is soft (``deleted_at``) so billing history survives; the default
  lookups filter deleted rows out.
  """

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
    """Get organization by ID, skipping soft-deleted rows by default."""
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
    cls, user_id: str, user_name: str, session: Session, auto_commit: bool = True
  ) -> "Org":
    """Create a personal org with the user as OWNER.

    Every resource — graphs, subscriptions, billing — hangs off an org rather
    than a user, so registration has to mint one. A personal org becomes a team
    or enterprise org by inviting members and changing ``org_type``; no new org
    is created.
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

    if auto_commit:
      try:
        session.commit()
        session.refresh(org)
      except SQLAlchemyError:
        session.rollback()
        raise

    return org

  @classmethod
  def get_all(cls, session: Session, include_deleted: bool = False) -> Sequence["Org"]:
    """Get all organizations, skipping soft-deleted rows by default."""
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
    """Soft-delete the organization, leaving its data in place.

    Raises ``ValueError`` if any subscription is still active, pending, or
    provisioning — deleting the org would strand the billing relationship.
    """
    from robosystems.models.core.billing import BillingSubscription

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
    """Hard-delete the org and everything cascading off it.

    Prefer ``soft_delete``; this bypasses the active-subscription guard and is
    irreversible.
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
