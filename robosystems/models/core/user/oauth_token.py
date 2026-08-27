"""Opaque OAuth access and refresh tokens, stored as SHA-256 digests.

Both token kinds are generated at full entropy (256 bits), so the digest is
a lookup key, not a password hash — the ``UserToken`` precedent, and the
reason ``UserAPIKey``'s own docstring gives for why a KDF is the wrong tool
for machine secrets. The plaintext exists once, in the token response.

Refresh tokens rotate: every use marks the presented token consumed and
mints a successor in the same ``family_id``. A consumed refresh token
presented again is a replay — the whole family is revoked, which is what
lets a stolen-and-used refresh token be detected at all.
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Optional

from sqlalchemy import Column, DateTime, ForeignKey, Index, String, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.config.constants import (
  OAUTH_ACCESS_TOKEN_TTL_SECONDS,
  OAUTH_REFRESH_TOKEN_TTL_DAYS,
)
from robosystems.database import Model
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.utils.ulid import generate_prefixed_ulid

TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"

# Plaintext prefixes. The access prefix is what the MCP auth dependency keys
# on to tell an opaque OAuth bearer apart from the app's JWT before any
# parsing; the refresh prefix keeps a refresh token from ever validating as
# an access token even if the type column were mishandled.
ACCESS_TOKEN_PREFIX = "rfso"
REFRESH_TOKEN_PREFIX = "rfsr"
# Length of the stored identification prefix (mirrors UserAPIKey.prefix).
TOKEN_ID_PREFIX_LENGTH = 8


def _as_utc(value: datetime | None) -> datetime | None:
  if value is None:
    return None
  return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class OAuthToken(Model):
  """An issued access or refresh token (digest only)."""

  __tablename__ = "oauth_tokens"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("oat"))
  grant_id = Column(String, ForeignKey("oauth_grants.id"), nullable=False, index=True)
  # Denormalized from the grant so revoke-all-for-user is one indexed query
  # (idx_oauth_tokens_user_type).
  user_id = Column(String, nullable=False)
  token_type = Column(String(8), nullable=False)
  token_hash = Column(String(64), nullable=False, unique=True, index=True)
  prefix = Column(String(TOKEN_ID_PREFIX_LENGTH), nullable=False)
  # Refresh rotation family; the access token carries the family of the
  # refresh token it was minted alongside.
  family_id = Column(String, nullable=False)
  expires_at = Column(DateTime, nullable=False)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  last_used_at = Column(DateTime, nullable=True)
  # Refresh tokens: set when rotated (consumed). Presenting a consumed token
  # is replay.
  used_at = Column(DateTime, nullable=True)
  revoked_at = Column(DateTime, nullable=True)

  __table_args__ = (
    Index("idx_oauth_tokens_user_type", "user_id", "token_type"),
    Index("idx_oauth_tokens_family_type", "family_id", "token_type"),
    Index("idx_oauth_tokens_expires", "expires_at"),
  )

  def __repr__(self) -> str:
    return f"<OAuthToken {self.id} {self.token_type} grant={self.grant_id}>"

  # --- digests ---------------------------------------------------------

  @staticmethod
  def fingerprint(plain_token: str) -> str:
    """SHA-256 of the plaintext: the stored digest AND the validation-cache
    key, so revocation can clear the cache entry without the plaintext."""
    return hashlib.sha256(plain_token.encode("utf-8")).hexdigest()

  @property
  def is_expired(self) -> bool:
    expires_at = _as_utc(self.expires_at)
    return expires_at is not None and datetime.now(UTC) > expires_at

  @property
  def is_live(self) -> bool:
    return self.revoked_at is None and self.used_at is None and not self.is_expired

  # --- minting ---------------------------------------------------------

  @classmethod
  def mint_pair(
    cls,
    *,
    grant_id: str,
    user_id: str,
    session: Session,
    family_id: str | None = None,
  ) -> tuple["OAuthToken", str, "OAuthToken", str, int]:
    """Mint an access + refresh token for a grant.

    Returns ``(access_row, access_plain, refresh_row, refresh_plain,
    access_ttl_seconds)``. A new family starts on code exchange; a refresh
    rotation passes the existing ``family_id`` so replay detection spans the
    whole chain.
    """
    family = family_id or generate_prefixed_ulid("oaf")
    now = datetime.now(UTC)

    access_plain = f"{ACCESS_TOKEN_PREFIX}{secrets.token_hex(32)}"
    refresh_plain = f"{REFRESH_TOKEN_PREFIX}{secrets.token_hex(32)}"

    access = cls(
      grant_id=grant_id,
      user_id=user_id,
      token_type=TOKEN_TYPE_ACCESS,
      token_hash=cls.fingerprint(access_plain),
      prefix=access_plain[:TOKEN_ID_PREFIX_LENGTH],
      family_id=family,
      expires_at=now + timedelta(seconds=OAUTH_ACCESS_TOKEN_TTL_SECONDS),
    )
    refresh = cls(
      grant_id=grant_id,
      user_id=user_id,
      token_type=TOKEN_TYPE_REFRESH,
      token_hash=cls.fingerprint(refresh_plain),
      prefix=refresh_plain[:TOKEN_ID_PREFIX_LENGTH],
      family_id=family,
      expires_at=now + timedelta(days=OAUTH_REFRESH_TOKEN_TTL_DAYS),
    )
    session.add(access)
    session.add(refresh)
    try:
      session.commit()
      session.refresh(access)
      session.refresh(refresh)
    except SQLAlchemyError:
      session.rollback()
      raise

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=user_id,
      details={
        "action": "oauth_tokens_issued",
        "oauth_grant_id": grant_id,
        "family_id": family,
        "access_token_id": access.id,
        "refresh_token_id": refresh.id,
        "rotation": family_id is not None,
      },
      risk_level="low",
    )
    return access, access_plain, refresh, refresh_plain, OAUTH_ACCESS_TOKEN_TTL_SECONDS

  # --- lookup ----------------------------------------------------------

  @classmethod
  def get_by_plaintext(
    cls, plain_token: str, token_type: str, session: Session
  ) -> Optional["OAuthToken"]:
    """Resolve a presented token to its row, regardless of state. Callers
    decide what an expired / consumed / revoked row means (a consumed
    refresh token is a replay signal, not merely a miss)."""
    if not plain_token or not isinstance(plain_token, str):
      return None
    return (
      session.query(cls)
      .filter(
        cls.token_hash == cls.fingerprint(plain_token),
        cls.token_type == token_type,
      )
      .first()
    )

  @classmethod
  def consume_refresh(cls, token_id: str, session: Session) -> bool:
    """Atomically mark a refresh token consumed. True when this caller won
    the claim; False when it was already consumed (replay) — a conditional
    UPDATE, so two concurrent refreshes cannot both rotate."""
    now = datetime.now(UTC)
    try:
      claimed = (
        session.query(cls)
        .filter(
          cls.id == token_id,
          cls.token_type == TOKEN_TYPE_REFRESH,
          cls.used_at.is_(None),
          cls.revoked_at.is_(None),
        )
        .update({"used_at": now, "last_used_at": now}, synchronize_session=False)
      )
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
    return claimed == 1

  def touch(self, session: Session) -> None:
    self.last_used_at = datetime.now(UTC)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  # --- revocation ------------------------------------------------------

  @classmethod
  def _revoke_query(cls, query, session: Session, *, reason: str) -> int:
    """Revoke every row the query selects and clear their cache entries.
    Returns the count revoked. Cache clearing is best-effort per row; the
    DB flip is the authoritative fact and the cache lapses on TTL."""
    rows = query.filter(cls.revoked_at.is_(None)).all()
    if not rows:
      return 0
    now = datetime.now(UTC)
    for row in rows:
      row.revoked_at = now
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
    for row in rows:
      row.invalidate_cache()
    logger.info(f"Revoked {len(rows)} OAuth tokens ({reason})")
    return len(rows)

  @classmethod
  def revoke_family(cls, family_id: str, session: Session, *, reason: str) -> int:
    return cls._revoke_query(
      session.query(cls).filter(cls.family_id == family_id), session, reason=reason
    )

  @classmethod
  def revoke_for_grant(cls, grant_id: str, session: Session) -> int:
    return cls._revoke_query(
      session.query(cls).filter(cls.grant_id == grant_id),
      session,
      reason="grant_revoked",
    )

  @classmethod
  def revoke_all_for_user(cls, user_id: str, session: Session, *, reason: str) -> int:
    """The session_version analog: password change, deactivation, and any
    other whole-account invalidation reach OAuth tokens through here."""
    return cls._revoke_query(
      session.query(cls).filter(cls.user_id == user_id), session, reason=reason
    )

  def revoke(self, session: Session, *, reason: str) -> None:
    if self.revoked_at is None:
      self.revoked_at = datetime.now(UTC)
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise
    self.invalidate_cache()
    logger.info(f"Revoked OAuth token {self.id} ({reason})")

  def invalidate_cache(self) -> bool:
    """Drop this token's cached validation entry (keyed on ``token_hash``,
    which is the same digest the validator caches under)."""
    if self.token_type != TOKEN_TYPE_ACCESS:
      return True
    try:
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      return bool(cache_module.api_key_cache.invalidate_api_key(str(self.token_hash)))
    except Exception as e:
      logger.error(f"Failed to invalidate cache for OAuth token {self.id}: {e}")
      return False

  @classmethod
  def cleanup_expired(cls, session: Session, *, older_than_days: int = 30) -> int:
    """Delete tokens that expired, or were revoked, more than
    ``older_than_days`` ago. Recently dead rows are kept so a late refresh
    replay is still detectable — rotation revokes the previous access token
    and a replay revokes a whole family, so revoked rows accrue as fast as
    expired ones."""
    cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
    try:
      count = (
        session.query(cls)
        .filter(or_(cls.expires_at < cutoff, cls.revoked_at < cutoff))
        .delete(synchronize_session=False)
      )
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
    if count:
      logger.info(f"Cleaned up {count} expired OAuth tokens")
    return count
