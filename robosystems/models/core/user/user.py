"""User authentication model."""

import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.logger import logger
from robosystems.utils.ulid import generate_prefixed_ulid


@dataclass(frozen=True)
class DeactivationResult:
  """What a deactivation actually accomplished.

  The DB flag flip always commits (or raises); cache invalidation and key
  revocation are best-effort, and a kill-switch caller (SCIM) needs to know
  whether they took so it can fail loud and retry.
  """

  # -1 when the key list could not even be loaded (count unknown).
  keys_found: int
  keys_revoked: int
  cache_invalidated: bool  # user-level JWT/session caches
  # A revoked key with a surviving cache entry authenticates until TTL.
  key_caches_invalidated: bool

  @property
  def fully_applied(self) -> bool:
    return (
      self.cache_invalidated
      and self.key_caches_invalidated
      and self.keys_revoked == self.keys_found
    )


class User(Model):
  """User model for authentication and authorization."""

  __tablename__ = "users"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("user"))
  email = Column(String, unique=True, nullable=False, index=True)
  name = Column(String, nullable=False)
  # NULL for IdP-governed accounts; password reset refuses NULL so a reset
  # email cannot bootstrap a password onto one.
  password_hash = Column(String, nullable=True)
  # SCIM externalId. Non-null marks the account IdP-provisioned, which the
  # OIDC email-match link requires: email alone must never bind an IdP login
  # to a locally-created account.
  external_id = Column(String, nullable=True, index=True)
  is_active = Column(Boolean, default=True, nullable=False)
  email_verified = Column(Boolean, default=False, nullable=False)
  # Embedded in JWTs and checked on every auth; bumping it kills prior tokens.
  session_version = Column(Integer, default=0, nullable=False, server_default="0")
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )
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
  passkeys = relationship(
    "UserPasskey", back_populates="user", cascade="all, delete-orphan"
  )
  mfa_recovery_codes = relationship(
    "UserMfaRecoveryCode", back_populates="user", cascade="all, delete-orphan"
  )

  def __repr__(self) -> str:
    return f"<User {self.id} {self.email}>"

  @classmethod
  def get_by_id(cls, user_id: str, session: Session) -> Optional["User"]:
    """Get a user by ID."""
    return session.query(cls).filter(cls.id == user_id).first()

  @classmethod
  def get_by_email(cls, email: str, session: Session) -> Optional["User"]:
    """Get a user by email, case-insensitively (emails are stored lowercased)."""
    return session.query(cls).filter(cls.email == email.lower()).first()

  @classmethod
  def create(
    cls,
    email: str,
    name: str,
    password_hash: str | None,
    session: Session,
    external_id: str | None = None,
    auto_commit: bool = True,
  ) -> "User":
    """Create a user. ``password_hash`` has no default so a passwordless
    (IdP-governed) account is always an explicit ``None``."""
    user = cls(
      email=email.lower(),
      name=name,
      password_hash=password_hash,
      external_id=external_id,
    )
    session.add(user)
    if auto_commit:
      try:
        session.commit()
        session.refresh(user)
      except SQLAlchemyError:
        session.rollback()
        raise
    else:
      session.flush()
    return user

  @classmethod
  def get_all(cls, session: Session) -> Sequence["User"]:
    """Get all users."""
    return session.query(cls).all()

  def update(self, session: Session, auto_commit: bool = True, **kwargs) -> None:
    """Update the named fields, normalizing ``email`` to lowercase."""
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

  def deactivate(self, session: Session) -> DeactivationResult:
    """Deactivate the user, reporting how completely the kill switch applied.

    Bumps ``session_version`` (killing JWTs) and revokes API keys and OAuth
    tokens, which carry no session version. A reactivated user must
    re-authenticate and mint new keys; nothing old resumes. Safe to re-run
    after a partial failure: it repeats the cache invalidation and key sweep.
    """
    self.is_active = False
    self.session_version = (self.session_version or 0) + 1
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      cache_invalidated = self._invalidate_auth_cache()
      keys_revoked, keys_found, key_caches_invalidated = self._revoke_api_keys(session)
      self._revoke_oauth_tokens(session, reason="user_deactivated")
      return DeactivationResult(
        keys_found=keys_found,
        keys_revoked=keys_revoked,
        cache_invalidated=cache_invalidated,
        key_caches_invalidated=key_caches_invalidated,
      )
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
    """Bump session_version, invalidating all JWTs, and revoke OAuth tokens
    (which carry no session version)."""
    self.session_version = (self.session_version or 0) + 1
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      self._invalidate_auth_cache()
    except SQLAlchemyError:
      session.rollback()
      raise
    self._revoke_oauth_tokens(session, reason="sessions_invalidated")

  def _revoke_oauth_tokens(self, session: Session, *, reason: str) -> int:
    """Revoke every OAuth token for this user. Best-effort: failures are
    logged; the validator's is_active and grant checks are the backstop."""
    from .oauth_token import OAuthToken

    try:
      return OAuthToken.revoke_all_for_user(str(self.id), session, reason=reason)
    except Exception as e:
      logger.error(f"Failed to revoke OAuth tokens for user {self.id}: {e}")
      return 0

  def _revoke_api_keys(self, session: Session) -> tuple[int, int, bool]:
    """Deactivate this user's API keys and clear their validation caches.

    Returns ``(revoked, found, caches_cleared)``; ``found`` is -1 when the
    keys could not be loaded. Inactive keys get their caches cleared too, so
    a retry repairs a key whose earlier cache invalidation failed. Per-key
    failures are logged, never raised; ``validate_api_key``'s is_active guard
    is the backstop.
    """
    from .user_api_key import UserAPIKey

    try:
      all_keys = UserAPIKey.get_by_user_id(str(self.id), session)
    except Exception as e:
      logger.error(f"Failed to load API keys for deactivated user {self.id}: {e}")
      return 0, -1, False

    active_keys = [key for key in all_keys if key.is_active]
    stale_keys = [key for key in all_keys if not key.is_active]

    revoked = 0
    caches_cleared = True
    for key in active_keys:
      try:
        caches_cleared = key.deactivate(session) and caches_cleared
        revoked += 1
      except Exception as e:
        caches_cleared = False
        logger.error(
          f"Failed to revoke API key {key.id} for deactivated user {self.id}: {e}"
        )
    for key in stale_keys:
      caches_cleared = key.invalidate_cache() and caches_cleared

    if revoked != len(active_keys):
      logger.error(
        f"CRITICAL: revoked {revoked} of {len(active_keys)} API keys for "
        f"deactivated user {self.id}; the remainder are still live in the key "
        f"table and are refused only by the user.is_active guard. Re-run the "
        f"deactivation."
      )
    if not caches_cleared:
      logger.error(
        f"CRITICAL: validation-cache invalidation incomplete for user "
        f"{self.id}'s API keys; a revoked key may keep authenticating from "
        f"cache until TTL. Re-run the deactivation."
      )

    return revoked, len(active_keys), caches_cleared

  def _invalidate_auth_cache(self) -> bool:
    """Delete auth caches derived from this user; False if unconfirmed.

    Delete, never rewrite: the cached session_version gates the cache hit,
    so a surviving entry defeats the bump. An entry that outlives a failed
    delete is bounded by its TTL.
    """
    import importlib

    try:
      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache
    except Exception as e:
      logger.error(f"Auth cache module unavailable for user {self.id}: {e}")
      return False

    user_id = str(self.id)

    def _try_invalidate() -> bool:
      # Both must succeed: a stale entry in either lets an old token through.
      try:
        user_ok = api_key_cache.invalidate_jwt_user_data(user_id)
        graph_ok = api_key_cache.invalidate_user_jwt_graph_access(user_id)
        return bool(user_ok) and bool(graph_ok)
      except Exception as e:
        logger.warning(f"Auth cache invalidation attempt failed for {user_id}: {e}")
        return False

    # One short retry for transient Redis blips.
    if _try_invalidate():
      return True
    time.sleep(0.05)
    if _try_invalidate():
      return True

    # Fail-open window: log as an error so it is monitorable.
    logger.error(
      f"CRITICAL: auth cache invalidation failed twice for user {user_id}; "
      f"prior session may remain valid for up to JWT/cache TTL"
    )
    return False
