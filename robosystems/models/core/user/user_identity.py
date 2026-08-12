"""External identity link between a user and an IdP subject."""

from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import (
  Column,
  DateTime,
  ForeignKey,
  String,
  UniqueConstraint,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid


class UserIdentity(Model):
  """Links a local user to an identity at an external IdP.

  Written once at the first successful OIDC login (the email-match link step);
  thereafter ``(issuer, subject)`` is the primary lookup. Login never creates
  users — resolution is link-only, and the link fallback is gated on the
  target user carrying a SCIM ``external_id`` (provenance, not email match).
  """

  __tablename__ = "user_identities"
  __table_args__ = (
    UniqueConstraint("issuer", "subject", name="uq_user_identity_issuer_subject"),
    UniqueConstraint("user_id", "issuer", name="uq_user_identity_user_issuer"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("usrid"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  # "oidc" today; "saml" reserved for a broker/phase-4 decision.
  provider_type = Column(String(20), nullable=False, default="oidc")
  issuer = Column(String, nullable=False, index=True)
  subject = Column(String, nullable=False)
  # The IdP-asserted email at the moment the link was written — an audit
  # anchor, not a login key; email changes at the IdP don't move the link.
  email_at_link = Column(String, nullable=False)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  last_login_at = Column(DateTime, nullable=True)

  user = relationship("User")

  def __repr__(self) -> str:
    return f"<UserIdentity {self.id} user={self.user_id} issuer={self.issuer}>"

  @classmethod
  def get_by_issuer_subject(
    cls, issuer: str, subject: str, session: Session
  ) -> Optional["UserIdentity"]:
    return (
      session.query(cls).filter(cls.issuer == issuer, cls.subject == subject).first()
    )

  @classmethod
  def get_by_user_and_issuer(
    cls, user_id: str, issuer: str, session: Session
  ) -> Optional["UserIdentity"]:
    return (
      session.query(cls).filter(cls.user_id == user_id, cls.issuer == issuer).first()
    )

  @classmethod
  def create(
    cls,
    user_id: str,
    issuer: str,
    subject: str,
    email_at_link: str,
    session: Session,
    provider_type: str = "oidc",
    auto_commit: bool = True,
  ) -> "UserIdentity":
    identity = cls(
      user_id=user_id,
      issuer=issuer,
      subject=subject,
      email_at_link=email_at_link.lower(),
      provider_type=provider_type,
    )
    session.add(identity)
    if auto_commit:
      try:
        session.commit()
        session.refresh(identity)
      except SQLAlchemyError:
        session.rollback()
        raise
    else:
      session.flush()
    return identity

  def touch_login(self, session: Session, auto_commit: bool = True) -> None:
    """Stamp ``last_login_at`` on a successful OIDC login."""
    self.last_login_at = datetime.now(UTC)
    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise
