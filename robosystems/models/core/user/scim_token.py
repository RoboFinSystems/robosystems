"""SCIM bearer token for IdP-driven provisioning, scoped to one org."""

import hashlib
import secrets
from datetime import UTC, datetime
from typing import Optional

import bcrypt
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.utils.ulid import generate_prefixed_ulid


class ScimToken(Model):
  """Bearer credential the customer's IdP presents to ``/scim/v2``.

  Storage mirrors ``UserAPIKey``: bcrypt ``token_hash`` as the credential,
  deterministic SHA-256 ``key_fingerprint`` as the indexed lookup, ``prefix``
  for human identification. The raw token is returned exactly once at mint
  (the admin bootstrap path). Rotation = mint a second token + revoke the
  first; two live tokens are legal during the swap.

  A SCIM token authenticates provisioning only. It is never accepted by the
  normal auth dependencies, and user JWTs/API keys are never accepted at the
  SCIM surface.
  """

  __tablename__ = "scim_tokens"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("scim"))
  org_id = Column(String, ForeignKey("orgs.id"), nullable=False, index=True)
  name = Column(String, nullable=False)
  token_hash = Column(String, nullable=False, unique=True)
  key_fingerprint = Column(String(64), nullable=False, unique=True, index=True)
  prefix = Column(String, nullable=False)
  is_active = Column(Boolean, default=True, nullable=False)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  last_used_at = Column(DateTime, nullable=True)
  expires_at = Column(DateTime, nullable=True)
  revoked_at = Column(DateTime, nullable=True)

  org = relationship("Org")

  def __repr__(self) -> str:
    return f"<ScimToken {self.id} org={self.org_id} active={self.is_active}>"

  @classmethod
  def create(
    cls,
    org_id: str,
    name: str,
    session: Session,
    expires_at: datetime | None = None,
  ) -> tuple["ScimToken", str]:
    """Mint a SCIM token, returning ``(row, plaintext)``.

    The plaintext is shown once and never stored — only its bcrypt hash and
    SHA-256 fingerprint persist.
    """
    plain_token = f"rfss{secrets.token_hex(32)}"

    token = cls(
      org_id=org_id,
      name=name,
      token_hash=cls._hash_token(plain_token),
      key_fingerprint=cls._fingerprint_token(plain_token),
      prefix=plain_token[:8],
      expires_at=expires_at,
    )

    session.add(token)
    try:
      session.commit()
      session.refresh(token)
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "scim_token_created",
          "org_id": org_id,
          "scim_token_id": token.id,
          "token_prefix": token.prefix,
        },
        risk_level="low",
      )
    except SQLAlchemyError:
      session.rollback()
      raise

    return token, plain_token

  @classmethod
  def get_by_id(cls, token_id: str, session: Session) -> Optional["ScimToken"]:
    return session.query(cls).filter(cls.id == token_id).first()

  @classmethod
  def get_active_by_org_id(cls, org_id: str, session: Session) -> list["ScimToken"]:
    return session.query(cls).filter(cls.org_id == org_id, cls.is_active).all()

  @classmethod
  def validate_token(cls, plain_token: str, session: Session) -> Optional["ScimToken"]:
    """Resolve a presented bearer token to an active, unexpired row.

    Fingerprint narrows to one candidate row; bcrypt then verifies the
    credential. Returns ``None`` on any miss — callers must not distinguish
    unknown / revoked / expired in their response.
    """
    candidate = (
      session.query(cls)
      .filter(cls.key_fingerprint == cls._fingerprint_token(plain_token))
      .first()
    )
    if candidate is None or not candidate.is_active:
      return None
    expires = candidate.expires_at
    if expires is not None:
      if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
      if expires < datetime.now(UTC):
        return None
    if not cls._verify_token(plain_token, str(candidate.token_hash)):
      return None
    return candidate

  def update_last_used(self, session: Session, auto_commit: bool = True) -> None:
    self.last_used_at = datetime.now(UTC)
    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise

  def revoke(self, session: Session) -> None:
    self.is_active = False
    self.revoked_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "scim_token_revoked",
          "org_id": self.org_id,
          "scim_token_id": self.id,
          "token_prefix": self.prefix,
        },
        risk_level="medium",
      )
    except SQLAlchemyError:
      session.rollback()
      raise

  @staticmethod
  def _fingerprint_token(plain_token: str) -> str:
    """Deterministic SHA-256 lookup fingerprint (not credential storage)."""
    return hashlib.sha256(plain_token.encode("utf-8")).hexdigest()

  @staticmethod
  def _hash_token(plain_token: str) -> str:
    """Hash the token with bcrypt at cost 12 (the UserAPIKey pattern)."""
    try:
      salt = bcrypt.gensalt(rounds=12)
      return bcrypt.hashpw(plain_token.encode("utf-8"), salt).decode("utf-8")
    except Exception as e:
      logger.error(f"Failed to hash SCIM token: {e}")
      raise ValueError("SCIM token hashing failed")

  @staticmethod
  def _verify_token(plain_token: str, stored_hash: str) -> bool:
    """Constant-time comparison of a plaintext token against its bcrypt hash."""
    try:
      return bcrypt.checkpw(plain_token.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception as e:
      logger.error(f"SCIM token verification failed: {e}")
      return False
