"""User authentication model."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.logger import logger
from robosystems.utils.ulid import generate_prefixed_ulid


class User(Model):
  """User model for authentication and authorization."""

  __tablename__ = "users"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("user"))
  email = Column(String, unique=True, nullable=False, index=True)
  name = Column(String, nullable=False)
  password_hash = Column(String, nullable=False)
  is_active = Column(Boolean, default=True, nullable=False)
  email_verified = Column(Boolean, default=False, nullable=False)
  # Bumped on password reset / logout-everywhere; embedded in JWT payload and
  # checked on every auth so prior tokens (incl. refresh chain) stop working.
  session_version = Column(Integer, default=0, nullable=False, server_default="0")
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  # Relationships
  user_api_keys = relationship(
    "UserAPIKey", back_populates="user", cascade="all, delete-orphan"
  )
  graph_users = relationship(
    "GraphUser", back_populates="user", cascade="all, delete-orphan"
  )
  user_repositories = relationship(
    "UserRepository",
    back_populates="user",
    cascade="all, delete-orphan",
    foreign_keys="UserRepository.user_id",
  )

  def __repr__(self) -> str:
    """String representation of the user."""
    return f"<User {self.id} {self.email}>"

  @classmethod
  def get_by_id(cls, user_id: str, session: Session) -> Optional["User"]:
    """Get a user by ID."""
    return session.query(cls).filter(cls.id == user_id).first()

  @classmethod
  def get_by_email(cls, email: str, session: Session) -> Optional["User"]:
    """Get a user by email (case-insensitive).

    Emails are stored in lowercase, so we normalize the input email
    and can use a direct indexed lookup.
    """
    return session.query(cls).filter(cls.email == email.lower()).first()

  @classmethod
  def create(
    cls, email: str, name: str, password_hash: str, session: Session
  ) -> "User":
    """Create a new user."""
    user = cls(email=email.lower(), name=name, password_hash=password_hash)
    session.add(user)
    try:
      session.commit()
      session.refresh(user)
    except SQLAlchemyError:
      session.rollback()
      raise
    return user

  @classmethod
  def get_all(cls, session: Session) -> Sequence["User"]:
    """Get all users."""
    return session.query(cls).all()

  def update(self, session: Session, auto_commit: bool = True, **kwargs) -> None:
    """Update user fields.

    Args:
        session: Database session
        auto_commit: Whether to automatically commit the transaction (default: True)
        **kwargs: Fields to update
    """
    for key, value in kwargs.items():
      if hasattr(self, key):
        if key == "email" and isinstance(value, str):
          setattr(self, key, value.lower())
        else:
          setattr(self, key, value)
    self.updated_at = datetime.now(UTC)

    if auto_commit:
      try:
        session.commit()
        session.refresh(self)
      except SQLAlchemyError:
        session.rollback()
        raise

  def delete(self, session: Session) -> None:
    """Delete the user."""
    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def verify_email(self, session: Session) -> None:
    """Mark user's email as verified."""
    self.email_verified = True
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def deactivate(self, session: Session) -> None:
    """Deactivate the user."""
    self.is_active = False
    self.session_version = (self.session_version or 0) + 1
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      self._invalidate_auth_cache()
    except SQLAlchemyError:
      session.rollback()
      raise

  def activate(self, session: Session) -> None:
    """Activate the user."""
    self.is_active = True
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      self._invalidate_auth_cache()
    except SQLAlchemyError:
      session.rollback()
      raise

  def invalidate_sessions(self, session: Session) -> None:
    """Bump session_version, invalidating all existing JWTs for this user."""
    self.session_version = (self.session_version or 0) + 1
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      self._invalidate_auth_cache()
    except SQLAlchemyError:
      session.rollback()
      raise

  def _invalidate_auth_cache(self) -> None:
    """Best-effort invalidation/refresh of auth caches derived from this user.

    Tradeoff: if the Redis write/DEL fails (network blip, eviction, etc.) the
    pre-bump cache entry persists with the prior session_version. A token
    still holding that prior version would continue to satisfy the cache
    lookup until the entry expires on its TTL (~30 min). The DB
    session_version is the source of truth, but the cache hit path doesn't
    re-read the DB. Bounded staleness is acceptable for password-reset
    semantics; for stronger guarantees, harden with retries or move the
    version-of-record into a separate Redis key without TTL.
    """
    try:
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache
      user_id = str(self.id)
      if self.is_active:
        session_version = int(self.session_version or 0)
        api_key_cache.cache_jwt_user_data(
          user_id,
          {
            "id": user_id,
            "email": self.email,
            "name": self.name,
            "is_active": True,
            "session_version": session_version,
          },
          session_version,
        )
      else:
        api_key_cache.invalidate_jwt_user_data(user_id)
      api_key_cache.invalidate_user_jwt_graph_access(str(self.id))
    except Exception as e:
      logger.error(f"Failed to invalidate auth cache for user {self.id}: {e}")
