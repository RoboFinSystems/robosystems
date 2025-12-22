"""Organization-user junction table for managing user roles within organizations."""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
  Column,
  DateTime,
  ForeignKey,
  String,
  UniqueConstraint,
)
from sqlalchemy import (
  Enum as SQLEnum,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class OrgRole(str, Enum):
  OWNER = "owner"
  ADMIN = "admin"
  MEMBER = "member"


class OrgUser(Model):
  """Junction table for organization-user relationships with roles."""

  __tablename__ = "org_users"
  __table_args__ = (UniqueConstraint("org_id", "user_id", name="uq_org_user"),)

  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("orgusr"),
  )
  org_id = Column(String, ForeignKey("orgs.id"), nullable=False, index=True)
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  role = Column(SQLEnum(OrgRole), nullable=False, default=OrgRole.MEMBER)

  joined_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  org = relationship("Org", back_populates="users")
  user = relationship("User")

  def __repr__(self) -> str:
    return f"<OrgUser org={self.org_id} user={self.user_id} role={self.role}>"

  @classmethod
  def get_by_id(cls, id: str, session: Session) -> Optional["OrgUser"]:
    return session.query(cls).filter(cls.id == id).first()

  @classmethod
  def get_by_org_and_user(
    cls, org_id: str, user_id: str, session: Session
  ) -> Optional["OrgUser"]:
    return (
      session.query(cls).filter(cls.org_id == org_id, cls.user_id == user_id).first()
    )

  @classmethod
  def create(
    cls,
    org_id: str,
    user_id: str,
    role: OrgRole,
    session: Session,
    auto_commit: bool = True,
  ) -> "OrgUser":
    org_user = cls(org_id=org_id, user_id=user_id, role=role)
    session.add(org_user)

    if auto_commit:
      try:
        session.commit()
        session.refresh(org_user)
      except SQLAlchemyError:
        session.rollback()
        raise

    return org_user

  @classmethod
  def get_user_orgs(cls, user_id: str, session: Session) -> Sequence["OrgUser"]:
    """Get all organizations a user belongs to."""
    return session.query(cls).filter(cls.user_id == user_id).all()

  @classmethod
  def get_org_users(cls, org_id: str, session: Session) -> Sequence["OrgUser"]:
    """Get all users of an organization."""
    return session.query(cls).filter(cls.org_id == org_id).all()

  def update_role(self, new_role: OrgRole, session: Session) -> None:
    """Update user's role in the organization."""
    self.role = new_role
    self.updated_at = datetime.now(UTC)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Remove user from organization."""
    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def is_owner(self) -> bool:
    """Check if user is an owner."""
    return self.role == OrgRole.OWNER

  def is_admin(self) -> bool:
    """Check if user is an admin or owner."""
    return self.role in (OrgRole.OWNER, OrgRole.ADMIN)

  def can_manage_members(self) -> bool:
    """Check if user can add/remove other members."""
    return self.is_admin()

  def can_manage_billing(self) -> bool:
    """Check if user can manage billing settings."""
    return self.is_admin()
