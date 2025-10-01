"""User token model for email verification and password reset."""

import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import Column, String, DateTime, Index, Text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ...database import Model
from ...logger import logger


class UserToken(Model):
  """Token model for email verification and password reset."""

  __tablename__ = "user_tokens"

  id = Column(
    String, primary_key=True, default=lambda: f"tok_{secrets.token_urlsafe(16)}"
  )
  user_id = Column(String, nullable=False, index=True)
  token_hash = Column(String, nullable=False, unique=True, index=True)
  token_type = Column(String(50), nullable=False)  # email_verification, password_reset
  expires_at = Column(DateTime, nullable=False)
  used_at = Column(DateTime, nullable=True)
  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  ip_address = Column(String(45), nullable=True)
  user_agent = Column(Text, nullable=True)

  # Create composite index for efficient lookups
  __table_args__ = (
    Index("idx_user_tokens", "user_id", "token_type"),
    Index("idx_expires", "expires_at"),
    Index("idx_expired_unused", "expires_at", "used_at"),
  )

  def __repr__(self) -> str:
    """String representation of the token."""
    return f"<UserToken {self.id} {self.token_type} for user {self.user_id}>"

  @classmethod
  def create_token(
    cls,
    user_id: str,
    token_type: str,
    hours: int,
    session: Session,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
  ) -> str:
    """
    Create a new token for a user.

    Args:
        user_id: The user ID
        token_type: Type of token (email_verification, password_reset)
        hours: Token validity in hours
        session: Database session
        ip_address: Optional IP address of requester
        user_agent: Optional user agent of requester

    Returns:
        The raw token string (to be sent to user)
    """
    # Validate token type
    valid_types = ["email_verification", "password_reset"]
    if token_type not in valid_types:
      raise ValueError(f"Invalid token type. Must be one of: {valid_types}")

    # Generate secure random token
    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    # Invalidate any existing unused tokens of this type for the user
    cls.invalidate_user_tokens(user_id, token_type, session)

    # Create new token
    token = cls(
      user_id=user_id,
      token_hash=token_hash,
      token_type=token_type,
      expires_at=datetime.now(timezone.utc) + timedelta(hours=hours),
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
  ) -> Optional[str]:
    """
    Verify and consume a token.

    Args:
        raw_token: The raw token string from the user
        token_type: Expected type of token
        session: Database session

    Returns:
        The user_id if token is valid, None otherwise
    """
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    token = (
      session.query(cls)
      .filter(
        cls.token_hash == token_hash,
        cls.token_type == token_type,
        cls.used_at.is_(None),
        cls.expires_at > datetime.now(timezone.utc),
      )
      .first()
    )

    if not token:
      logger.warning(f"Invalid or expired {token_type} token attempted")
      return None

    # Mark token as used
    token.used_at = datetime.now(timezone.utc)
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
  ) -> Optional[str]:
    """
    Validate a token without consuming it.

    Args:
        raw_token: The raw token string from the user
        token_type: Expected type of token
        session: Database session

    Returns:
        The user_id if token is valid, None otherwise
    """
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    token = (
      session.query(cls)
      .filter(
        cls.token_hash == token_hash,
        cls.token_type == token_type,
        cls.used_at.is_(None),
        cls.expires_at > datetime.now(timezone.utc),
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
    """
    Invalidate all unused tokens of a specific type for a user.

    Args:
        user_id: The user ID
        token_type: Type of tokens to invalidate
        session: Database session

    Returns:
        Number of tokens invalidated
    """
    try:
      count = (
        session.query(cls)
        .filter(
          cls.user_id == user_id,
          cls.token_type == token_type,
          cls.used_at.is_(None),
        )
        .update({"used_at": datetime.now(timezone.utc)})
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
    """
    Clean up expired and unused tokens.

    Args:
        session: Database session

    Returns:
        Number of tokens deleted
    """
    try:
      # Delete tokens that are either:
      # 1. Expired and unused
      # 2. Used more than 30 days ago
      cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)

      count = (
        session.query(cls)
        .filter(
          ((cls.expires_at < datetime.now(timezone.utc)) & cls.used_at.is_(None))
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
    cls, user_id: str, session: Session, token_type: Optional[str] = None
  ) -> list:
    """
    Retrieve active tokens for a user.

    Args:
        user_id: The user ID
        session: Database session
        token_type: Optional token type filter

    Returns:
        List of active tokens for the user
    """
    query = session.query(cls).filter(
      cls.user_id == user_id,
      cls.used_at.is_(None),
      cls.expires_at > datetime.now(timezone.utc),
    )

    if token_type:
      query = query.filter(cls.token_type == token_type)

    return query.all()
