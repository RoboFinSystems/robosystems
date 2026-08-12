"""User authentication model."""

import time
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
  # NULL for IdP-governed accounts (SCIM-provisioned, and later passwordless
  # signup). Password login and password change already refuse a falsy hash;
  # the password-reset flow refuses NULL outright so a reset email can never
  # bootstrap a password onto an IdP-governed account.
  password_hash = Column(String, nullable=True)
  # SCIM externalId, round-tripped to the IdP. A non-null value marks the
  # account as IdP-provisioned — the provenance predicate the OIDC email-match
  # link step gates on (email alone must never bind an IdP login to a
  # locally-created account).
  external_id = Column(String, nullable=True, index=True)
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
    return f"<User {self.id} {self.email}>"

  @classmethod
  def get_by_id(cls, user_id: str, session: Session) -> Optional["User"]:
    """Get a user by ID."""
    return session.query(cls).filter(cls.id == user_id).first()

  @classmethod
  def get_by_email(cls, email: str, session: Session) -> Optional["User"]:
    """Get a user by email, case-insensitively.

    Emails are stored lowercased, so lowering the input keeps this an indexed
    equality lookup rather than a functional scan.
    """
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
    """Create a new user.

    ``password_hash`` is required but nullable: pass ``None`` explicitly for
    IdP-governed accounts rather than omitting it, so a passwordless user is
    always a deliberate call-site decision.
    """
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

  def deactivate(self, session: Session) -> int:
    """Deactivate the user, returning how many API keys were revoked.

    Bumps ``session_version``, which invalidates every JWT issued for this
    user, and revokes all of the user's API keys. If the user is reactivated
    later, they must re-authenticate and regenerate API keys — prior tokens
    and keys cannot resume. This is intentional: a deactivated account is a
    security boundary, and we don't want suspended/banned/restored accounts
    to be revivable just by replaying an old token or a still-valid API key.

    API-key revocation is essential: keys don't carry ``session_version`` and
    ``UserAPIKey.get_by_key`` filters only on the key's own active flag, so
    without this a deactivated user would keep full programmatic access.
    """
    self.is_active = False
    self.session_version = (self.session_version or 0) + 1
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
      self._invalidate_auth_cache()
      return self._revoke_api_keys(session)
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

  def _revoke_api_keys(self, session: Session) -> int:
    """Deactivate this user's active API keys and clear their caches.

    Returns how many were actually revoked, which is what callers must report:
    revocation is best-effort per key, so the number attempted is not the
    number that took, and an incident response that reads "4 keys revoked"
    when one failed is worse than no number at all.

    Called on deactivate so programmatic access is revoked alongside JWT
    invalidation. Each key's ``deactivate`` flips ``is_active`` and invalidates
    that key's validation-cache entry. Failures are logged per-key but never
    abort the user deactivation; the ``user.is_active`` guard in
    ``validate_api_key`` is the authoritative backstop if a key is missed, and
    re-running the deactivation retries whatever did not take.
    """
    from .user_api_key import UserAPIKey

    try:
      active_keys = UserAPIKey.get_active_by_user_id(str(self.id), session)
    except Exception as e:
      logger.error(f"Failed to load API keys for deactivated user {self.id}: {e}")
      return 0

    revoked = 0
    for key in active_keys:
      try:
        key.deactivate(session)
        revoked += 1
      except Exception as e:
        logger.error(
          f"Failed to revoke API key {key.id} for deactivated user {self.id}: {e}"
        )

    if revoked != len(active_keys):
      logger.error(
        f"CRITICAL: revoked {revoked} of {len(active_keys)} API keys for "
        f"deactivated user {self.id}; the remainder are still live in the key "
        f"table and are refused only by the user.is_active guard. Re-run the "
        f"deactivation."
      )

    return revoked

  def _invalidate_auth_cache(self) -> None:
    """Invalidate auth caches derived from this user.

    Always DELETE rather than rewrite: the cached entry's session_version
    is what gates the cache hit, so an old entry left in place defeats the
    DB session_version bump. Letting the cache repopulate lazily on the
    next legitimate request is one extra DB hit per invalidation event
    (password reset / deactivate / logout-everywhere) — cheap.

    Tradeoff: if BOTH retries against Redis fail, the prior cache entry
    persists until its TTL expires (~30 min). The DB session_version is
    the source of truth, but the cache hit path doesn't re-read the DB,
    so during this window an attacker holding a token with the prior
    session_version would continue to authenticate. This is bounded by
    JWT TTL (30 min) and Redis recovery time, and is the standard
    cache-invalidation tradeoff. For stronger guarantees, move the
    version-of-record into a no-TTL Redis key (or back to per-request DB).
    """
    import importlib

    try:
      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      api_key_cache = cache_module.api_key_cache
    except Exception as e:
      logger.error(f"Auth cache module unavailable for user {self.id}: {e}")
      return

    user_id = str(self.id)

    def _try_invalidate() -> bool:
      # Both methods return True on success, False on Redis failure.
      # We require BOTH to succeed: a stale entry in either cache could
      # let an old token continue to authenticate.
      try:
        user_ok = api_key_cache.invalidate_jwt_user_data(user_id)
        graph_ok = api_key_cache.invalidate_user_jwt_graph_access(user_id)
        return bool(user_ok) and bool(graph_ok)
      except Exception as e:
        logger.warning(f"Auth cache invalidation attempt failed for {user_id}: {e}")
        return False

    # One retry with a short backoff handles transient Redis blips
    # (connection reset, single-node failover) without going to extremes.
    if _try_invalidate():
      return
    time.sleep(0.05)
    if _try_invalidate():
      return

    # Both attempts failed. Log loudly — this is the fail-open window.
    # Surface as an error so it's monitorable; downstream alerting should
    # treat repeated occurrences as a Redis-availability incident.
    logger.error(
      f"CRITICAL: auth cache invalidation failed twice for user {user_id}; "
      f"prior session may remain valid for up to JWT/cache TTL"
    )
