"""Opaque OAuth access and refresh tokens, stored as SHA-256 digests.

Tokens are 256-bit random, so the digest is a lookup key, not a password hash.
Refresh tokens rotate within a ``family_id``; presenting a consumed one is a
replay and revokes the whole family.
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

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

# The MCP auth dependency uses the access prefix to tell an OAuth bearer from a
# JWT before parsing; the distinct refresh prefix keeps a refresh token from
# ever validating as an access token.
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

  def rotated_within(self, seconds: float) -> bool:
    """Consumed no more than ``seconds`` ago, and not revoked since."""
    used_at = _as_utc(self.used_at)
    return (
      used_at is not None
      and self.revoked_at is None
      and datetime.now(UTC) - used_at <= timedelta(seconds=seconds)
    )

  # --- minting ---------------------------------------------------------

  @classmethod
  def mint_pair(
    cls,
    *,
    grant_id: str,
    user_id: str,
    session: Session,
    family_id: str | None = None,
    commit: bool = True,
  ) -> tuple[OAuthToken, str, OAuthToken, str, int]:
    """Mint an access + refresh token for a grant.

    Returns ``(access_row, access_plain, refresh_row, refresh_plain,
    access_ttl_seconds)``. Rotation passes the existing ``family_id``.
    With ``commit=False`` the pair is only flushed: the caller commits it
    with the rest of its transaction, then calls :meth:`log_issued`.
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
      created_at=now,
    )
    refresh = cls(
      grant_id=grant_id,
      user_id=user_id,
      token_type=TOKEN_TYPE_REFRESH,
      token_hash=cls.fingerprint(refresh_plain),
      prefix=refresh_plain[:TOKEN_ID_PREFIX_LENGTH],
      family_id=family,
      expires_at=now + timedelta(days=OAUTH_REFRESH_TOKEN_TTL_DAYS),
      created_at=now,
    )
    session.add(access)
    session.add(refresh)
    if not commit:
      session.flush()
      return (
        access,
        access_plain,
        refresh,
        refresh_plain,
        OAUTH_ACCESS_TOKEN_TTL_SECONDS,
      )
    try:
      session.commit()
      session.refresh(access)
      session.refresh(refresh)
    except SQLAlchemyError:
      session.rollback()
      raise

    cls.log_issued(access, refresh, rotation=family_id is not None)
    return access, access_plain, refresh, refresh_plain, OAUTH_ACCESS_TOKEN_TTL_SECONDS

  @staticmethod
  def log_issued(
    access: OAuthToken,
    refresh: OAuthToken,
    *,
    rotation: bool,
    reuse_grace: bool = False,
  ) -> None:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(access.user_id),
      details={
        "action": "oauth_tokens_issued",
        "oauth_grant_id": access.grant_id,
        "family_id": access.family_id,
        "access_token_id": access.id,
        "refresh_token_id": refresh.id,
        "rotation": rotation,
        "reuse_grace": reuse_grace,
      },
      risk_level="low",
    )

  # --- lookup ----------------------------------------------------------

  @classmethod
  def get_by_plaintext(
    cls, plain_token: str, token_type: str, session: Session
  ) -> OAuthToken | None:
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
  def consume_refresh(
    cls, token_id: str, session: Session, *, commit: bool = True
  ) -> bool:
    """Atomically mark a refresh token consumed. True when this caller won
    the claim; False when it was already consumed (replay) — a conditional
    UPDATE, so two concurrent refreshes cannot both rotate. With
    ``commit=False`` the claim's row lock is held until the caller commits,
    so a concurrent claim waits for the whole rotation, not just the claim."""
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
      if commit:
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
    rows = cls._mark_revoked(query)
    if not rows:
      return 0
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
  def _mark_revoked(cls, query) -> list[OAuthToken]:
    """Set ``revoked_at`` on every live row the query selects, without
    committing. The caller commits, then clears the rows' cache entries."""
    rows = query.filter(cls.revoked_at.is_(None)).all()
    now = datetime.now(UTC)
    for row in rows:
      row.revoked_at = now
    return rows

  @classmethod
  def mark_revoked_for_user(cls, user_id: str, session: Session) -> list[OAuthToken]:
    return cls._mark_revoked(session.query(cls).filter(cls.user_id == user_id))

  @classmethod
  def mark_pair_access_revoked(
    cls, refresh: OAuthToken, session: Session
  ) -> list[OAuthToken]:
    """Retire the access token minted with ``refresh`` (``mint_pair`` stamps
    both with one ``created_at``). Each pair's access token goes when its own
    refresh token rotates, so a sibling pair from the reuse grace, held by
    another caller, keeps working whichever sibling rotates first."""
    return cls._mark_revoked(
      session.query(cls).filter(
        cls.family_id == refresh.family_id,
        cls.token_type == TOKEN_TYPE_ACCESS,
        cls.created_at == refresh.created_at,
      )
    )

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
    """Delete tokens expired or revoked more than ``older_than_days`` ago.
    Recently dead rows are kept so a late refresh replay is still detected."""
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
