"""User authentication model."""

from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class User(Model):
  """User model for authentication and authorization."""

  __tablename__ = "users"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("user"))
  email = Column(String, unique=True, nullable=False, index=True)
  name = Column(String, nullable=False)
  password_hash = Column(String, nullable=False)
  is_active = Column(Boolean, default=True, nullable=False)
  email_verified = Column(Boolean, default=False, nullable=False)
  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
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
    self.updated_at = datetime.now(timezone.utc)

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
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def deactivate(self, session: Session) -> None:
    """Deactivate the user."""
    self.is_active = False
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def activate(self, session: Session) -> None:
    """Activate the user."""
    self.is_active = True
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise
