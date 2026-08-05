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

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("uak"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  name = Column(String, nullable=False)  # User-friendly name for the key
  key_hash = Column(
    String, nullable=False, unique=True, index=True
  )  # bcrypt hashed API key
  # Deterministic SHA-256 of the plaintext key. Used as the cache key for
  # validate_api_key so that deactivate/delete can invalidate the same entry.
  # Nullable for rows created before this column existed; backfilled on the
  # next successful validation since plaintext is available there.
  #
  # DEPLOY NOTE: Existing pre-deploy keys cannot be precisely cache-invalidated
  # until they're validated once post-deploy (which backfills the column).
  # If a key is revoked before its first post-deploy use, ``_invalidate_cache``
  # logs a warning and returns — the positive cache entry (if any) will
  # persist for the cache TTL. Mitigations: (a) flush the API-key Redis
  # namespace at deploy time as a one-shot, or (b) accept the bounded window.
  key_fingerprint = Column(String(64), nullable=True, unique=True, index=True)
  prefix = Column(
    String, nullable=False, index=True
  )  # First few chars for identification
  # Scope restriction: NULL = account-wide (full historical behavior); a value
  # restricts the key to that graph (and its subgraphs). Scoped keys are the
  # only kind accepted via the MCP endpoint's URL query parameter, and are
  # rejected on endpoints that carry no graph context.
  graph_id = Column(String, nullable=True, index=True)
  is_active = Column(Boolean, default=True, nullable=False)
  description = Column(Text, nullable=True)  # Optional description
  last_used_at = Column(DateTime, nullable=True)
  expires_at = Column(DateTime, nullable=True)  # Optional expiration date
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  # Relationships
  user = relationship("User", back_populates="user_api_keys")

  # Performance indexes
  __table_args__ = (
    Index("idx_user_api_keys_hash_active", "key_hash", "is_active"),
    Index("idx_user_api_keys_last_used", "last_used_at"),
    Index("idx_user_api_keys_prefix_active", "prefix", "is_active"),
  )

  def __repr__(self) -> str:
    """String representation of the user API key."""
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
    """
    Create a new API key for a user with secure bcrypt hashing.

    Returns:
        tuple: (UserAPIKey instance, plain text key)
    """
    # Generate a cryptographically secure API key. Graph-scoped keys get a
    # distinguishable prefix for human/incident legibility; the authoritative
    # scope check is always the row's graph_id, never the prefix.
    plain_key = (
      f"rfsc{secrets.token_hex(32)}" if graph_id else f"rfs{secrets.token_hex(32)}"
    )

    # Hash the key using bcrypt with high work factor
    key_hash = cls._hash_api_key(plain_key)

    # Deterministic fingerprint for cache lookup/invalidation
    key_fingerprint = cls._fingerprint_api_key(plain_key)

    # Store prefix for identification (first 8 chars)
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

      # Log secure API key creation
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
    """
    Get a user API key by its plain text value using secure bcrypt verification.
    """
    if not plain_key or not isinstance(plain_key, str):
      SecurityAuditLogger.log_input_validation_failure(
        field_name="api_key",
        invalid_value="[REDACTED]",
        validation_error="Invalid API key format",
      )
      return None

    # Get all active API keys with matching prefix for efficiency
    prefix = plain_key[:8] if len(plain_key) >= 8 else plain_key
    potential_keys = (
      session.query(cls).filter(cls.prefix == prefix, cls.is_active).all()
    )

    for api_key in potential_keys:
      try:
        if cls._verify_api_key(plain_key, str(api_key.key_hash)):
          # Backfill key_fingerprint for rows created before the column existed.
          # Plaintext is only available here, so this is the right place.
          if not api_key.key_fingerprint:
            api_key.key_fingerprint = cls._fingerprint_api_key(plain_key)

          # Check if API key is expired
          if api_key.expires_at and datetime.now(UTC) > api_key.expires_at:
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
            continue  # Try next potential key

          # Update last used timestamp
          api_key.update_last_used(session, auto_commit=False)
          session.commit()

          # Log successful API key verification
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

    # Log failed verification attempt
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
    """Update the last used timestamp.

    Args:
        session: Database session
        auto_commit: Whether to automatically commit the transaction (default: True)
    """
    self.last_used_at = datetime.now(UTC)
    self.updated_at = datetime.now(UTC)

    if auto_commit:
      try:
        session.commit()
        session.refresh(self)
      except SQLAlchemyError:
        session.rollback()
        raise

  def deactivate(self, session: Session) -> None:
    """Deactivate the API key and invalidate cache."""
    self.is_active = False
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

    # Invalidate cache
    self._invalidate_cache()

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

    # Invalidate cache
    self._invalidate_cache()

  def delete(self, session: Session) -> None:
    """Delete the API key and invalidate cache."""
    # Invalidate cache before deletion
    self._invalidate_cache()

    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  @staticmethod
  def _fingerprint_api_key(plain_key: str) -> str:
    """Deterministic SHA-256 fingerprint of a plaintext API key.

    Used as the cache key in `validate_api_key`. Stored on the row so
    `_invalidate_cache` can clear the same entry without needing the plaintext.

    NOT credential storage: the API key itself is stored as bcrypt in
    ``key_hash`` (see ``_hash_api_key`` below). This SHA-256 is solely a
    deterministic lookup fingerprint so the cache key derived from the
    plaintext (``sha256(plain_key)``) matches what ``_invalidate_cache``
    reads off the row. CodeQL's "weak hash on sensitive data" rule is a
    false positive here — silenced via lgtm[py/weak-sensitive-data-hashing].
    """
    return hashlib.sha256(  # lgtm[py/weak-sensitive-data-hashing]
      plain_key.encode("utf-8")
    ).hexdigest()

  @staticmethod
  def _hash_api_key(plain_key: str) -> str:
    """
    Hash an API key using bcrypt with high work factor.

    Args:
        plain_key: The plain text API key

    Returns:
        Bcrypt hash string
    """
    try:
      # Use a high work factor (cost) for security
      # 12 rounds = ~250ms on modern hardware, good security/performance balance
      salt = bcrypt.gensalt(rounds=12)
      hashed = bcrypt.hashpw(plain_key.encode("utf-8"), salt)
      return hashed.decode("utf-8")
    except Exception as e:
      logger.error(f"Failed to hash API key: {e}")
      raise ValueError("API key hashing failed")

  @staticmethod
  def _verify_api_key(plain_key: str, stored_hash: str) -> bool:
    """
    Verify an API key against its bcrypt hash.

    Args:
        plain_key: The plain text API key to verify
        stored_hash: The stored bcrypt hash from database

    Returns:
        True if verification succeeds
    """
    try:
      # Use bcrypt verification (constant-time, secure)
      return bcrypt.checkpw(plain_key.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception as e:
      logger.error(f"API key verification failed: {e}")
      return False

  def _invalidate_cache(self) -> None:
    """Invalidate cached data for this API key.

    Uses ``key_fingerprint`` (sha256 of plaintext) which matches the cache key
    used by ``validate_api_key``. Pre-migration rows may have a NULL
    fingerprint until they get backfilled on next validation; for those we
    can't invalidate by fingerprint and the cache will expire on its TTL.
    """
    try:
      # Dynamically import only when needed to avoid circular dependency
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache

      fingerprint = self.key_fingerprint
      if not fingerprint:
        logger.warning(
          f"API key {self.id} has no key_fingerprint; cache cannot be "
          f"invalidated until the key is validated once after deploy."
        )
        return

      api_key_cache.invalidate_api_key(fingerprint)

      # Log cache invalidation
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "action": "api_key_cache_invalidated",
          "api_key_id": self.id,
        },
        risk_level="low",
      )

    except Exception as e:
      logger.error(f"Failed to invalidate cache for user API key {self.id}: {e}")
