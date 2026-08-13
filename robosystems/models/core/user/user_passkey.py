"""WebAuthn passkey credential registered to a user.

One row per registered authenticator. Stores the public half of the
credential only — WebAuthn has no server-side secret at rest, which is the
point of the passkey design. ``credential_id`` is base64url without padding
(the wire form the browser presents), normalized on store and lookup so the
unique constraint cannot silently fork identities.
"""

from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Integer,
  LargeBinary,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid


def normalize_credential_id(credential_id: str) -> str:
  """Canonical form for lookup/storage: base64url with padding stripped."""
  return credential_id.rstrip("=")


class UserPasskey(Model):
  """A WebAuthn credential (passkey) enrolled by a user."""

  __tablename__ = "user_passkeys"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("upk"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  credential_id = Column(String, nullable=False, unique=True, index=True)
  # COSE-encoded public key bytes, verbatim from the registration ceremony.
  public_key = Column(LargeBinary, nullable=False)
  sign_count = Column(Integer, nullable=False, default=0)
  transports = Column(JSONB, nullable=True)
  aaguid = Column(String, nullable=True)
  # WebAuthn BE/BS flags: eligibility distinguishes synced (multi-device)
  # from device-bound credentials; state tracks whether it is currently
  # backed up. Surfaced in the UI and useful for the audit narrative.
  backup_eligible = Column(Boolean, nullable=False, default=False)
  backup_state = Column(Boolean, nullable=False, default=False)
  # User-facing label ("MacBook Touch ID"); prompted at enrollment.
  name = Column(String(100), nullable=False, default="Passkey")
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  last_used_at = Column(DateTime, nullable=True)

  user = relationship("User", back_populates="passkeys")

  def __repr__(self) -> str:
    return f"<UserPasskey {self.id} user={self.user_id} name={self.name!r}>"

  @classmethod
  def get_by_credential_id(
    cls, credential_id: str, session: Session
  ) -> Optional["UserPasskey"]:
    return (
      session.query(cls)
      .filter(cls.credential_id == normalize_credential_id(credential_id))
      .first()
    )

  @classmethod
  def get_all_for_user(cls, user_id: str, session: Session) -> list["UserPasskey"]:
    return (
      session.query(cls).filter(cls.user_id == user_id).order_by(cls.created_at).all()
    )

  @classmethod
  def count_for_user(cls, user_id: str, session: Session) -> int:
    return session.query(cls).filter(cls.user_id == user_id).count()

  @classmethod
  def create(
    cls,
    user_id: str,
    credential_id: str,
    public_key: bytes,
    session: Session,
    sign_count: int = 0,
    transports: list[str] | None = None,
    aaguid: str | None = None,
    backup_eligible: bool = False,
    backup_state: bool = False,
    name: str = "Passkey",
    auto_commit: bool = True,
  ) -> "UserPasskey":
    passkey = cls(
      user_id=user_id,
      credential_id=normalize_credential_id(credential_id),
      public_key=public_key,
      sign_count=sign_count,
      transports=transports,
      aaguid=aaguid,
      backup_eligible=backup_eligible,
      backup_state=backup_state,
      name=name,
    )
    session.add(passkey)
    if auto_commit:
      try:
        session.commit()
        session.refresh(passkey)
      except SQLAlchemyError:
        session.rollback()
        raise
    else:
      session.flush()
    return passkey

  def touch_used(
    self,
    session: Session,
    new_sign_count: int | None = None,
    backup_state: bool | None = None,
    auto_commit: bool = True,
  ) -> None:
    """Stamp a successful assertion: last-used time, sign count, BS flag."""
    self.last_used_at = datetime.now(UTC)
    if new_sign_count is not None:
      self.sign_count = new_sign_count
    if backup_state is not None:
      self.backup_state = backup_state
    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise
