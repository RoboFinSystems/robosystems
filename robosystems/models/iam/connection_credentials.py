"""Connection credentials model for secure storage of OAuth tokens and API keys."""

import secrets
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Sequence
from cryptography.fernet import Fernet
import base64

from sqlalchemy import Column, String, DateTime, Text, Boolean
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import Model
from ...config import env
from ...logger import logger


class ConnectionCredentials(Model):
  """Secure storage for connection credentials (OAuth tokens, API keys, etc.)."""

  __tablename__ = "connection_credentials"

  id = Column(
    String, primary_key=True, default=lambda: f"cred_{secrets.token_urlsafe(16)}"
  )

  # Connection identification
  connection_id = Column(
    String, nullable=False, index=True
  )  # References Graph Connection.connection_id
  provider = Column(String, nullable=False, index=True)  # QuickBooks, Plaid, SEC
  user_id = Column(String, nullable=False, index=True)  # References User.id

  # Encrypted credential storage
  encrypted_credentials = Column(
    Text, nullable=False
  )  # JSON blob of credentials, encrypted

  # Metadata
  expires_at = Column(DateTime, nullable=True)  # When credentials expire
  is_active = Column(Boolean, default=True, nullable=False)
  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  def __repr__(self) -> str:
    """String representation of the credentials."""
    return f"<ConnectionCredentials {self.id} {self.provider} {self.connection_id}>"

  @staticmethod
  def _get_encryption_key() -> bytes:
    """Get or generate encryption key from environment."""
    key = env.CONNECTION_CREDENTIALS_KEY
    if not key:
      # In production, this MUST be set in environment
      # For development, derive from JWT_SECRET_KEY for consistency
      logger.warning(
        "CONNECTION_CREDENTIALS_KEY is not set. Falling back to JWT_SECRET_KEY for development."
      )
      jwt_secret = env.JWT_SECRET_KEY
      if jwt_secret:
        # Create a deterministic key from JWT secret
        import hashlib

        # Use SHA256 to create a 32-byte key from the JWT secret
        hash_input = f"{jwt_secret}_connection_encryption".encode()
        key_bytes = hashlib.sha256(hash_input).digest()[:32]  # Ensure exactly 32 bytes
        # Fernet.generate_key() creates a 32-byte key, we need to match that format
        key = base64.urlsafe_b64encode(key_bytes).decode()
      else:
        # Fail fast if no key is available
        logger.error(
          "No encryption key found. Please set CONNECTION_CREDENTIALS_KEY or JWT_SECRET_KEY."
        )
        raise ValueError(
          "CONNECTION_CREDENTIALS_KEY must be set for credential encryption."
        )

    # Fernet expects the key as base64-encoded bytes (not decoded)
    # So we return the base64 string as bytes
    try:
      # Validate it's proper base64
      base64.urlsafe_b64decode(key.encode())
      # Return the base64 string as bytes (what Fernet expects)
      return key.encode()
    except Exception:
      # If key is malformed, raise an exception
      logger.error(
        "Invalid encryption key format. The key must be a URL-safe base64-encoded string."
      )
      raise ValueError("Invalid encryption key format.")

  def _encrypt_credentials(self, credentials: Dict[str, Any]) -> str:
    """Encrypt credentials dictionary."""
    key = self._get_encryption_key()
    try:
      fernet = Fernet(key)
    except Exception as e:
      logger.error(f"Failed to create Fernet with key: {e}")
      logger.error(
        f"Key type: {type(key)}, Key length: {len(key) if isinstance(key, (str, bytes)) else 'N/A'}"
      )
      raise
    credentials_json = json.dumps(credentials)
    encrypted_data = fernet.encrypt(credentials_json.encode())
    return base64.urlsafe_b64encode(encrypted_data).decode()

  def _decrypt_credentials(self) -> Dict[str, Any]:
    """Decrypt and return credentials dictionary."""
    key = self._get_encryption_key()
    fernet = Fernet(key)
    encrypted_data = base64.urlsafe_b64decode(self.encrypted_credentials.encode())
    decrypted_json = fernet.decrypt(encrypted_data).decode()
    return json.loads(decrypted_json)

  def set_credentials(self, credentials: Dict[str, Any]) -> None:
    """Set and encrypt credentials."""
    self.encrypted_credentials = self._encrypt_credentials(credentials)

  def get_credentials(self) -> Dict[str, Any]:
    """Get and decrypt credentials."""
    return self._decrypt_credentials()

  @classmethod
  def create(
    cls,
    connection_id: str,
    provider: str,
    user_id: str,
    credentials: Dict[str, Any],
    session: Session,
    expires_at: Optional[datetime] = None,
  ) -> "ConnectionCredentials":
    """Create new connection credentials."""
    cred = cls(
      connection_id=connection_id,
      provider=provider,
      user_id=user_id,
      expires_at=expires_at,
    )
    cred.set_credentials(credentials)

    session.add(cred)
    try:
      session.commit()
      session.refresh(cred)
    except SQLAlchemyError:
      session.rollback()
      raise
    return cred

  @classmethod
  def get_by_connection_id(
    cls, connection_id: str, session: Session
  ) -> Optional["ConnectionCredentials"]:
    """Get credentials by connection ID."""
    return (
      session.query(cls)
      .filter(cls.connection_id == connection_id, cls.is_active)
      .first()
    )

  @classmethod
  def get_by_user_and_provider(
    cls, user_id: str, provider: str, session: Session
  ) -> Sequence["ConnectionCredentials"]:
    """Get all active credentials for a user and provider."""
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.provider == provider)
      .filter(
        cls.user_id == user_id,
        cls.provider == provider,
        cls.is_active,
      )
      .all()
    )

  def update_credentials(self, credentials: Dict[str, Any], session: Session) -> None:
    """Update existing credentials."""
    self.set_credentials(credentials)
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def deactivate(self, session: Session) -> None:
    """Deactivate credentials (don't delete for audit trail)."""
    self.is_active = False
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def is_expired(self) -> bool:
    """Check if credentials are expired."""
    if not self.expires_at:
      return False
    return datetime.now(timezone.utc) > self.expires_at  # type: ignore[return-value]

  def update_expiry(self, expires_at: datetime, session: Session) -> None:
    """Update credential expiry time."""
    self.expires_at = expires_at
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise
