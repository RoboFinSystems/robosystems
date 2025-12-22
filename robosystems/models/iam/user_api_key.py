"""User API Key model for programmatic access."""

import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Optional

import bcrypt
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from ...database import Model
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType
from ...utils.ulid import generate_prefixed_ulid


class UserAPIKey(Model):
  """User API Key model for programmatic access to the API."""

  __tablename__ = "user_api_keys"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("uak"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  name = Column(String, nullable=False)  # User-friendly name for the key
  key_hash = Column(
    String, nullable=False, unique=True, index=True
  )  # bcrypt hashed API key
  prefix = Column(
    String, nullable=False, index=True
  )  # First few chars for identification
  is_active = Column(Boolean, default=True, nullable=False)
  description = Column(Text, nullable=True)  # Optional description
  last_used_at = Column(DateTime, nullable=True)
  expires_at = Column(DateTime, nullable=True)  # Optional expiration date
  created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=datetime.now(UTC),
    onupdate=datetime.now(UTC),
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
  ) -> tuple["UserAPIKey", str]:
    """
    Create a new API key for a user with secure bcrypt hashing.

    Returns:
        tuple: (UserAPIKey instance, plain text key)
    """
    # Generate a cryptographically secure API key
    plain_key = f"rfs{secrets.token_hex(32)}"

    # Hash the key using bcrypt with high work factor
    key_hash = cls._hash_api_key(plain_key)

    # Store prefix for identification (first 8 chars)
    prefix = plain_key[:8]

    user_api_key = cls(
      user_id=user_id,
      name=name,
      description=description,
      expires_at=expires_at,
      key_hash=key_hash,
      prefix=prefix,
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
    """Invalidate cached data for this API key."""
    try:
      # Dynamically import only when needed to avoid circular dependency
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache

      api_key_cache.invalidate_api_key(self.key_hash)

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
