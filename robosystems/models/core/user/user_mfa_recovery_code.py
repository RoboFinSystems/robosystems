"""Single-use MFA recovery codes — the universal backstop for passkey loss.

A set of codes is generated at first-passkey enrollment (shown exactly once)
and replaced wholesale on regeneration. Only SHA-256 hashes are stored; the
codes carry enough entropy that an indexed equality lookup on the hash is the
correct verification (no per-row bcrypt walk). Deliberately NOT a
``UserToken`` variant: that model enforces one live token per type per user,
which conflicts with holding a set.
"""

import hashlib
import secrets
from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid

RECOVERY_CODE_COUNT = 10
# Groups of A-Z2-7 (RFC 4648 base32, no confusable 0/1/8) — 50 bits of entropy.
_CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
_CODE_GROUPS = 2
_CODE_GROUP_LENGTH = 5


def _hash_code(code: str) -> str:
  return hashlib.sha256(_canonicalize(code).encode()).hexdigest()


def _canonicalize(code: str) -> str:
  """Uppercase and strip separators/whitespace so entry is forgiving."""
  return code.strip().upper().replace("-", "").replace(" ", "")


def generate_code() -> str:
  groups = [
    "".join(secrets.choice(_CODE_ALPHABET) for _ in range(_CODE_GROUP_LENGTH))
    for _ in range(_CODE_GROUPS)
  ]
  return "-".join(groups)


class UserMfaRecoveryCode(Model):
  """One single-use recovery code (hash only) belonging to a user."""

  __tablename__ = "user_mfa_recovery_codes"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("umrc"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  code_hash = Column(String(64), nullable=False, index=True)
  used_at = Column(DateTime, nullable=True)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

  user = relationship("User", back_populates="mfa_recovery_codes")

  def __repr__(self) -> str:
    return f"<UserMfaRecoveryCode {self.id} user={self.user_id} used={self.used_at is not None}>"

  @classmethod
  def create_set(
    cls,
    user_id: str,
    session: Session,
    count: int = RECOVERY_CODE_COUNT,
    auto_commit: bool = True,
  ) -> list[str]:
    """Replace the user's recovery codes; returns the plaintext codes once."""
    session.query(cls).filter(cls.user_id == user_id).delete()
    codes = [generate_code() for _ in range(count)]
    for code in codes:
      session.add(cls(user_id=user_id, code_hash=_hash_code(code)))
    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise
    else:
      session.flush()
    return codes

  @classmethod
  def consume(
    cls, user_id: str, code: str, session: Session, auto_commit: bool = True
  ) -> bool:
    """Mark a matching unused code as used. Returns False when none matches."""
    row = (
      session.query(cls)
      .filter(
        cls.user_id == user_id,
        cls.code_hash == _hash_code(code),
        cls.used_at.is_(None),
      )
      .first()
    )
    if row is None:
      return False
    row.used_at = datetime.now(UTC)
    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise
    else:
      session.flush()
    return True

  @classmethod
  def remaining_count(cls, user_id: str, session: Session) -> int:
    return (
      session.query(cls).filter(cls.user_id == user_id, cls.used_at.is_(None)).count()
    )
