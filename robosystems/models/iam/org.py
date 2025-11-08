"""Organization model for multi-tenant billing and resource management."""

import secrets
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Sequence

from sqlalchemy import Column, String, DateTime, Enum as SQLEnum
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError

from ...database import Model


class OrgType(str, Enum):
  PERSONAL = "personal"
  TEAM = "team"
  ENTERPRISE = "enterprise"


class Org(Model):
  """Organization model for grouping users and managing billing."""

  __tablename__ = "orgs"

  id = Column(
    String, primary_key=True, default=lambda: f"org_{secrets.token_urlsafe(16)}"
  )
  name = Column(String, nullable=False)
  org_type = Column(SQLEnum(OrgType), nullable=False, default=OrgType.PERSONAL)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  users = relationship("OrgUser", back_populates="org", cascade="all, delete-orphan")
  graphs = relationship("Graph", back_populates="org", cascade="all, delete-orphan")

  def __repr__(self) -> str:
    return f"<Org {self.id} {self.name} ({self.org_type})>"

  @classmethod
  def get_by_id(cls, org_id: str, session: Session) -> Optional["Org"]:
    return session.query(cls).filter(cls.id == org_id).first()

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
      name=f"{user_name}'s Workspace",
      org_type=OrgType.PERSONAL,
      session=session,
      auto_commit=False,
    )

    from .org_user import OrgUser, OrgRole

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
  def get_all(cls, session: Session) -> Sequence["Org"]:
    return session.query(cls).all()

  def update(self, session: Session, **kwargs) -> None:
    for key, value in kwargs.items():
      if hasattr(self, key):
        setattr(self, key, value)
    self.updated_at = datetime.now(timezone.utc)

    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
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
    from .org_user import OrgUser, OrgRole

    return (
      session.query(OrgUser)
      .filter(
        OrgUser.org_id == self.id,
        OrgUser.role == OrgRole.OWNER,
      )
      .first()
    )
