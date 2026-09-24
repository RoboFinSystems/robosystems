"""User API Key model for programmatic access."""

import hashlib
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Optional

import bcrypt
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.utils.ulid import generate_prefixed_ulid


class UserAPIKey(Model):
  """User API Key model for programmatic access to the API."""

  __tablename__ = "user_api_keys"

  # bcrypt cost (~250ms at 12); a class constant so tests can patch it down.
  BCRYPT_ROUNDS = 12

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("uak"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  name = Column(String, nullable=False)
  key_hash = Column(String, nullable=False, unique=True, index=True)  # bcrypt
  # SHA-256 of the plaintext: the validation cache key, stored so the entry
  # can be invalidated without the plaintext. NULL until the key is first
  # validated (the only place plaintext is available to backfill it).
  key_fingerprint = Column(String(64), nullable=True, unique=True, index=True)
  prefix = Column(String, nullable=False, index=True)  # first 8 chars
  # NULL = account-wide; otherwise restricted to this graph and its
  # subgraphs. Only scoped keys are accepted via the MCP URL query parameter,
  # and they are rejected on endpoints with no graph context.
  graph_id = Column(String, nullable=True, index=True)
  is_active = Column(Boolean, default=True, nullable=False)
  description = Column(Text, nullable=True)
  last_used_at = Column(DateTime, nullable=True)
  expires_at = Column(DateTime, nullable=True)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  user = relationship("User", back_populates="user_api_keys")

  __table_args__ = (
    Index("idx_user_api_keys_hash_active", "key_hash", "is_active"),
    Index("idx_user_api_keys_last_used", "last_used_at"),
    Index("idx_user_api_keys_prefix_active", "prefix", "is_active"),
  )

  def __repr__(self) -> str:
    return f"<UserAPIKey {self.id} {self.name} user={self.user_id}>"

  @classmethod
  def create(
    cls,
    user_id: str,
    name: str,
    description: str | None = None,
    expires_at: datetime | None = None,
    session: Session | None = None,
    graph_id: str | None = None,
  ) -> tuple["UserAPIKey", str]:
    """Mint an API key, returning ``(row, plaintext)``; the plaintext is
    never stored."""
    # The scoped prefix is for human legibility only; scope checks read the
    # row's graph_id.
    plain_key = (
      f"rfsc{secrets.token_hex(32)}" if graph_id else f"rfs{secrets.token_hex(32)}"
    )

    key_hash = cls._hash_api_key(plain_key)
    key_fingerprint = cls._fingerprint_api_key(plain_key)
    prefix = plain_key[:8]

    user_api_key = cls(
      user_id=user_id,
      name=name,
      description=description,
      expires_at=expires_at,
      key_hash=key_hash,
      key_fingerprint=key_fingerprint,
      prefix=prefix,
      graph_id=graph_id,
    )

    if session is None:
      raise ValueError("Session is required for API key creation")

    session.add(user_api_key)
    try:
      session.commit()
      session.refresh(user_api_key)

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "secure_api_key_created",
          "user_id": user_id,
          "api_key_id": user_api_key.id,
          "key_prefix": prefix,
          "graph_scope": graph_id,
        },
        risk_level="low",
      )

    except SQLAlchemyError:
      session.rollback()
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "api_key_creation_failed",
          "user_id": user_id,
          "error": "database_error",
        },
        risk_level="medium",
      )
      raise

    return user_api_key, plain_key

  @classmethod
  def get_by_key(cls, plain_key: str, session: Session) -> Optional["UserAPIKey"]:
    """Resolve a plaintext API key to its active, unexpired row.

    Narrows on the indexed ``prefix``, then bcrypt-verifies each candidate.
    """
    if not plain_key or not isinstance(plain_key, str):
      SecurityAuditLogger.log_input_validation_failure(
        field_name="api_key",
        invalid_value="[REDACTED]",
        validation_error="Invalid API key format",
      )
      return None

    prefix = plain_key[:8] if len(plain_key) >= 8 else plain_key
    potential_keys = (
      session.query(cls).filter(cls.prefix == prefix, cls.is_active).all()
    )

    for api_key in potential_keys:
      try:
        if cls._verify_api_key(plain_key, str(api_key.key_hash)):
          # The only place plaintext is available to backfill the fingerprint.
          if not api_key.key_fingerprint:
            api_key.key_fingerprint = cls._fingerprint_api_key(plain_key)

          if api_key.is_expired():
            logger.warning(f"API key {api_key.id} is expired")
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.AUTHORIZATION_DENIED,
              details={
                "action": "api_key_expired",
                "api_key_id": api_key.id,
                "user_id": api_key.user_id,
                "expired_at": api_key.expires_at.isoformat(),
              },
              risk_level="low",
            )
            continue

          api_key.update_last_used(session, auto_commit=False)
          session.commit()

          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.AUTH_SUCCESS,
            details={
              "action": "api_key_verification_success",
              "api_key_id": api_key.id,
              "user_id": api_key.user_id,
            },
            risk_level="low",
          )

          return api_key

      except Exception as e:
        logger.error(f"Error verifying API key {api_key.id}: {e}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "api_key_verification_error",
            "api_key_id": api_key.id,
            "error": str(e),
          },
          risk_level="medium",
        )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      details={
        "action": "api_key_verification_failed",
        "key_prefix": prefix,
        "attempted_keys_checked": len(potential_keys),
      },
      risk_level="medium",
    )

    return None

  def is_expired(self, now: datetime | None = None) -> bool:
    """Whether the key is past its expiry. The column is naive UTC."""
    if self.expires_at is None:
      return False
    expires_at = self.expires_at
    if expires_at.tzinfo is None:
      expires_at = expires_at.replace(tzinfo=UTC)
    return (now or datetime.now(UTC)) > expires_at

  @classmethod
  def get_by_hash(cls, key_hash: str, session: Session) -> Optional["UserAPIKey"]:
    """Get a user API key by its hash value."""
    return session.query(cls).filter(cls.key_hash == key_hash, cls.is_active).first()

  @classmethod
  def get_by_user_id(cls, user_id: str, session: Session) -> Sequence["UserAPIKey"]:
    """Get all API keys for a user."""
    return session.query(cls).filter(cls.user_id == user_id).all()

  @classmethod
  def get_active_by_user_id(
    cls, user_id: str, session: Session
  ) -> Sequence["UserAPIKey"]:
    """Get all active API keys for a user."""
    return session.query(cls).filter(cls.user_id == user_id, cls.is_active).all()

  def update_last_used(self, session: Session, auto_commit: bool = True) -> None:
    """Stamp ``last_used_at``, committing unless the caller batches it."""
    self.last_used_at = datetime.now(UTC)
    self.updated_at = datetime.now(UTC)

    if auto_commit:
      try:
        session.commit()
        session.refresh(self)
      except SQLAlchemyError:
        session.rollback()
        raise

  def deactivate(self, session: Session) -> bool:
    """Deactivate the key; returns whether its cache entry was cleared (a
    surviving entry keeps the key usable until TTL)."""
    self.is_active = False
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

    return self.invalidate_cache()

  def activate(self, session: Session) -> None:
    """Activate the API key and invalidate cache."""
    self.is_active = True
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

    self.invalidate_cache()

  def delete(self, session: Session) -> None:
    """Delete the API key, clearing its cache entry first — the row's
    fingerprint is needed to find it."""
    self.invalidate_cache()

    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  @staticmethod
  def _fingerprint_api_key(plain_key: str) -> str:
    """Deterministic SHA-256 fingerprint used as the validation cache key.

    Not credential storage (that is bcrypt in ``key_hash``); a KDF is
    unnecessary because API keys are generated at full entropy.
    """
    return hashlib.sha256(plain_key.encode("utf-8")).hexdigest()

  @staticmethod
  def _hash_api_key(plain_key: str) -> str:
    """Hash an API key with bcrypt at cost ``BCRYPT_ROUNDS`` (~250ms at 12)."""
    try:
      salt = bcrypt.gensalt(rounds=UserAPIKey.BCRYPT_ROUNDS)
      hashed = bcrypt.hashpw(plain_key.encode("utf-8"), salt)
      return hashed.decode("utf-8")
    except Exception as e:
      logger.error(f"Failed to hash API key: {e}")
      raise ValueError("API key hashing failed")

  @staticmethod
  def _verify_api_key(plain_key: str, stored_hash: str) -> bool:
    """Constant-time comparison of a plaintext key against its bcrypt hash."""
    try:
      return bcrypt.checkpw(plain_key.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception as e:
      logger.error(f"API key verification failed: {e}")
      return False

  def invalidate_cache(self) -> bool:
    """Drop this key's cached validation result; True when it took.

    A NULL fingerprint (never validated) means nothing is cached, so it
    reports success. False means an entry may survive until TTL; revocation
    callers should retry.
    """
    try:
      # Lazy import: circular dependency on the auth middleware.
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache

      fingerprint = self.key_fingerprint
      if not fingerprint:
        logger.warning(
          f"API key {self.id} has no key_fingerprint; cache cannot be "
          f"invalidated until the key is validated once after deploy."
        )
        return True

      cleared = bool(api_key_cache.invalidate_api_key(fingerprint))

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "action": "api_key_cache_invalidated",
          "api_key_id": self.id,
        },
        risk_level="low",
      )
      return cleared

    except Exception as e:
      logger.error(f"Failed to invalidate cache for user API key {self.id}: {e}")
      return False
