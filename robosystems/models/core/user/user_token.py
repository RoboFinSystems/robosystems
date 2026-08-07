"""User token model for email verification and password reset."""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, DateTime, Index, String, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import Model
from robosystems.logger import logger


class UserToken(Model):
  """Single-use token for email verification and password reset.

  Only the SHA-256 hash is stored; the raw token exists once, in the return
  value of ``create_token``.
  """

  __tablename__ = "user_tokens"

  id = Column(
    String, primary_key=True, default=lambda: f"tok_{secrets.token_urlsafe(16)}"
  )
  user_id = Column(String, nullable=False, index=True)
  token_hash = Column(String, nullable=False, unique=True, index=True)
  token_type = Column(String(50), nullable=False)  # email_verification, password_reset
  expires_at = Column(DateTime, nullable=False)
  used_at = Column(DateTime, nullable=True)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  ip_address = Column(String(45), nullable=True)
  user_agent = Column(Text, nullable=True)

  __table_args__ = (
    Index("idx_user_tokens", "user_id", "token_type"),
    Index("idx_expires", "expires_at"),
    Index("idx_expired_unused", "expires_at", "used_at"),
  )

  def __repr__(self) -> str:
    return f"<UserToken {self.id} {self.token_type} for user {self.user_id}>"

  @classmethod
  def create_token(
    cls,
    user_id: str,
    token_type: str,
    hours: int,
    session: Session,
    ip_address: str | None = None,
    user_agent: str | None = None,
  ) -> str:
    """Issue a token, returning the raw string to send to the user.

    Any unused token of the same type for this user is invalidated first, so
    a user only ever holds one live verification or reset token.
    """
    valid_types = ["email_verification", "password_reset"]
    if token_type not in valid_types:
      raise ValueError(f"Invalid token type. Must be one of: {valid_types}")

    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    cls.invalidate_user_tokens(user_id, token_type, session)

    token = cls(
      user_id=user_id,
      token_hash=token_hash,
      token_type=token_type,
      expires_at=datetime.now(UTC) + timedelta(hours=hours),
      ip_address=ip_address,
      user_agent=user_agent,
    )

    session.add(token)
    try:
      session.commit()
      logger.info(f"Created {token_type} token for user {user_id}")
    except SQLAlchemyError as e:
      session.rollback()
      logger.error(f"Failed to create token: {e}")
      raise

    return raw_token

  @classmethod
  def verify_token(
    cls, raw_token: str, token_type: str, session: Session
  ) -> str | None:
    """Verify a token and consume it, returning its user_id or None.

    Marks the row used, so a second call with the same token fails.
    """
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    token = (
      session.query(cls)
      .filter(
        cls.token_hash == token_hash,
        cls.token_type == token_type,
        cls.used_at.is_(None),
        cls.expires_at > datetime.now(UTC),
      )
      .first()
    )

    if not token:
      logger.warning(f"Invalid or expired {token_type} token attempted")
      return None

    token.used_at = datetime.now(UTC)
    try:
      session.commit()
      logger.info(f"Successfully verified {token_type} token for user {token.user_id}")
    except SQLAlchemyError as e:
      session.rollback()
      logger.error(f"Failed to mark token as used: {e}")
      raise

    return token.user_id

  @classmethod
  def validate_token(
    cls, raw_token: str, token_type: str, session: Session
  ) -> str | None:
    """Check a token without consuming it, returning its user_id or None."""
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    token = (
      session.query(cls)
      .filter(
        cls.token_hash == token_hash,
        cls.token_type == token_type,
        cls.used_at.is_(None),
        cls.expires_at > datetime.now(UTC),
      )
      .first()
    )

    if not token:
      return None

    return token.user_id

  @classmethod
  def invalidate_user_tokens(
    cls, user_id: str, token_type: str, session: Session
  ) -> int:
    """Mark every unused token of this type used; returns how many."""
    try:
      count = (
        session.query(cls)
        .filter(
          cls.user_id == user_id,
          cls.token_type == token_type,
          cls.used_at.is_(None),
        )
        .update({"used_at": datetime.now(UTC)})
      )
      session.commit()
      if count > 0:
        logger.info(f"Invalidated {count} {token_type} tokens for user {user_id}")
      return count
    except SQLAlchemyError as e:
      session.rollback()
      logger.error(f"Failed to invalidate tokens: {e}")
      raise

  @classmethod
  def cleanup_expired_tokens(cls, session: Session) -> int:
    """Delete spent tokens; returns how many.

    Two cases qualify: expired and never used, or used more than 30 days ago.
    """
    try:
      cutoff_date = datetime.now(UTC) - timedelta(days=30)

      count = (
        session.query(cls)
        .filter(
          ((cls.expires_at < datetime.now(UTC)) & cls.used_at.is_(None))
          | (cls.used_at < cutoff_date)
        )
        .delete()
      )
      session.commit()
      if count > 0:
        logger.info(f"Cleaned up {count} expired tokens")
      return count
    except SQLAlchemyError as e:
      session.rollback()
      logger.error(f"Failed to cleanup expired tokens: {e}")
      raise

  @classmethod
  def get_active_tokens_for_user(
    cls, user_id: str, session: Session, token_type: str | None = None
  ) -> list:
    """Unused, unexpired tokens for a user, optionally filtered by type."""
    query = session.query(cls).filter(
      cls.user_id == user_id,
      cls.used_at.is_(None),
      cls.expires_at > datetime.now(UTC),
    )

    if token_type:
      query = query.filter(cls.token_type == token_type)

    return query.all()
