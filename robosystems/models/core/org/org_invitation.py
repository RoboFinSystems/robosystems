"""Organization invitation model for inviting new users into an org by email."""

import hashlib
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Optional

from sqlalchemy import (
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
  text,
)
from sqlalchemy import (
  Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.config.constants import ORG_INVITATION_EXPIRY_DAYS
from robosystems.database import Model
from robosystems.logger import logger
from robosystems.utils.ulid import generate_prefixed_ulid

from .org_user import OrgRole


class InvitationStatus(str, Enum):
  PENDING = "pending"
  ACCEPTED = "accepted"
  REVOKED = "revoked"


class OrgInvitation(Model):
  """Pending invitation for an email address to join an organization.

  Invitations are issued to emails that do not yet have a user account; the
  invited registrant joins the issuing org (at the invited role) instead of
  having a personal org created. The raw token is only ever sent in the
  invitation email; the database stores its SHA-256 hash.
  """

  __tablename__ = "org_invitations"
  __table_args__ = (
    Index(
      "uq_org_invitations_pending_email",
      "org_id",
      "email",
      unique=True,
      postgresql_where=text("status = 'pending'"),
    ),
  )

  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("orginv"),
  )
  org_id = Column(String, ForeignKey("orgs.id"), nullable=False, index=True)
  email = Column(String, nullable=False, index=True)
  role = Column(SQLEnum(OrgRole), nullable=False, default=OrgRole.MEMBER)
  # Reserved for a follow-up: initial per-graph grants applied at acceptance,
  # shaped as [{"graph_id": ..., "role": ...}]. Unused in v1.
  graph_grants = Column(JSONB, nullable=True)
  invited_by = Column(String, ForeignKey("users.id"), nullable=False)
  token_hash = Column(String, nullable=False, unique=True, index=True)
  status = Column(String(20), nullable=False, default=InvitationStatus.PENDING.value)
  expires_at = Column(DateTime, nullable=False)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  accepted_at = Column(DateTime, nullable=True)
  revoked_at = Column(DateTime, nullable=True)
  accepted_user_id = Column(String, ForeignKey("users.id"), nullable=True)

  org = relationship("Org")
  invited_by_user = relationship("User", foreign_keys=[invited_by])

  def __repr__(self) -> str:
    return f"<OrgInvitation {self.id} {self.email} -> {self.org_id} ({self.status})>"

  @property
  def is_expired(self) -> bool:
    """Whether the invitation is past its expiry (regardless of status)."""
    expires = self.expires_at
    if expires.tzinfo is None:
      expires = expires.replace(tzinfo=UTC)
    return expires < datetime.now(UTC)

  @staticmethod
  def _hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode()).hexdigest()

  @classmethod
  def create_invitation(
    cls,
    org_id: str,
    email: str,
    role: OrgRole,
    invited_by: str,
    session: Session,
  ) -> tuple["OrgInvitation", str]:
    """Create a pending invitation and return it with the raw token.

    The raw token is returned exactly once for inclusion in the invitation
    email; only its hash is persisted.
    """
    raw_token = secrets.token_urlsafe(32)

    invitation = cls(
      org_id=org_id,
      email=email.strip().lower(),
      role=role,
      invited_by=invited_by,
      token_hash=cls._hash_token(raw_token),
      status=InvitationStatus.PENDING.value,
      expires_at=datetime.now(UTC) + timedelta(days=ORG_INVITATION_EXPIRY_DAYS),
    )

    session.add(invitation)
    try:
      session.commit()
      session.refresh(invitation)
      logger.info(f"Created org invitation {invitation.id} for org {org_id}")
    except SQLAlchemyError:
      session.rollback()
      raise

    return invitation, raw_token

  @classmethod
  def get_by_id(cls, invitation_id: str, session: Session) -> Optional["OrgInvitation"]:
    return session.query(cls).filter(cls.id == invitation_id).first()

  @classmethod
  def get_valid_by_token(
    cls, raw_token: str, session: Session
  ) -> Optional["OrgInvitation"]:
    """Look up a pending, unexpired invitation by its raw token."""
    return (
      session.query(cls)
      .filter(
        cls.token_hash == cls._hash_token(raw_token),
        cls.status == InvitationStatus.PENDING.value,
        cls.expires_at > datetime.now(UTC),
      )
      .first()
    )

  @classmethod
  def get_pending_for_org(
    cls, org_id: str, session: Session
  ) -> Sequence["OrgInvitation"]:
    """All pending invitations for an org (including expired-but-pending)."""
    return (
      session.query(cls)
      .filter(cls.org_id == org_id, cls.status == InvitationStatus.PENDING.value)
      .order_by(cls.created_at.desc())
      .all()
    )

  @classmethod
  def get_pending_by_org_and_email(
    cls, org_id: str, email: str, session: Session
  ) -> Optional["OrgInvitation"]:
    return (
      session.query(cls)
      .filter(
        cls.org_id == org_id,
        cls.email == email.strip().lower(),
        cls.status == InvitationStatus.PENDING.value,
      )
      .first()
    )

  def mark_accepted(
    self, user_id: str, session: Session, auto_commit: bool = True
  ) -> None:
    """Mark the invitation as accepted by the given user."""
    self.status = InvitationStatus.ACCEPTED.value
    self.accepted_at = datetime.now(UTC)
    self.accepted_user_id = user_id

    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise

  def revoke(self, session: Session) -> None:
    """Revoke a pending invitation, invalidating its token."""
    self.status = InvitationStatus.REVOKED.value
    self.revoked_at = datetime.now(UTC)

    try:
      session.commit()
      logger.info(f"Revoked org invitation {self.id}")
    except SQLAlchemyError:
      session.rollback()
      raise

  def rotate_token(self, session: Session) -> str:
    """Issue a fresh token and expiry for a pending invitation (resend)."""
    raw_token = secrets.token_urlsafe(32)
    self.token_hash = self._hash_token(raw_token)
    self.expires_at = datetime.now(UTC) + timedelta(days=ORG_INVITATION_EXPIRY_DAYS)

    try:
      session.commit()
      logger.info(f"Rotated token for org invitation {self.id}")
    except SQLAlchemyError:
      session.rollback()
      raise

    return raw_token
