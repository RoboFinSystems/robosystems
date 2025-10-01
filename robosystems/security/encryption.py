"""
Data encryption and decryption utilities.

This module provides secure encryption/decryption for sensitive data like backups.
Uses Fernet (symmetric encryption) from the cryptography library which provides:
- AES 128-bit encryption in CBC mode
- HMAC for authentication
- Secure key derivation

Note: The encryption key should be stored securely in AWS Secrets Manager
or environment variables, never in code.
"""

import base64
from typing import Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from robosystems.logger import logger
from robosystems.config import env


def _get_or_create_encryption_key() -> bytes:
  """
  Get or create the encryption key for backup operations.

  Uses KUZU_BACKUP_ENCRYPTION_KEY from environment configuration.
  For development, generates a key from a password if not set.

  Returns:
      bytes: The encryption key
  """
  # Try to get key from environment variable first
  encryption_key = env.KUZU_BACKUP_ENCRYPTION_KEY

  if encryption_key:
    # Key is stored as base64 encoded string
    try:
      return base64.urlsafe_b64decode(encryption_key)
    except Exception as e:
      logger.error(f"Invalid encryption key format: {e}")
      raise ValueError("Invalid KUZU_BACKUP_ENCRYPTION_KEY format")

  # In production, we must have the encryption key set
  if env.ENVIRONMENT == "prod":
    raise ValueError("KUZU_BACKUP_ENCRYPTION_KEY must be set in production environment")

  # For development/staging, derive a key from a password
  # This is less secure but acceptable for non-production environments
  # Use centralized config to get from Secrets Manager if available
  password = env.KUZU_BACKUP_ENCRYPTION_PASSWORD or "robosystems-dev-backup-key-2024"
  salt = b"robosystems-kuzu-backup-salt-v1"  # Static salt for development consistency

  kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=100000,
  )
  key = base64.urlsafe_b64encode(kdf.derive(password.encode()))

  logger.warning(
    f"Using derived encryption key for {env.ENVIRONMENT} environment. "
    "Set KUZU_BACKUP_ENCRYPTION_KEY for production use."
  )

  return key


def encrypt_data(data: Union[bytes, str]) -> bytes:
  """
  Encrypt data using Fernet symmetric encryption.

  Args:
      data: The data to encrypt (bytes or string)

  Returns:
      bytes: The encrypted data

  Raises:
      ValueError: If encryption fails
  """
  try:
    # Convert string to bytes if necessary
    if isinstance(data, str):
      data = data.encode("utf-8")

    # Get encryption key
    key = _get_or_create_encryption_key()

    # Create Fernet instance
    fernet = Fernet(key)

    # Encrypt the data
    encrypted_data = fernet.encrypt(data)

    logger.debug(f"Encrypted {len(data)} bytes to {len(encrypted_data)} bytes")

    return encrypted_data

  except Exception as e:
    logger.error(f"Encryption failed: {e}")
    raise ValueError(f"Failed to encrypt data: {str(e)}")


def decrypt_data(encrypted_data: bytes) -> bytes:
  """
  Decrypt data that was encrypted with encrypt_data.

  Args:
      encrypted_data: The encrypted data

  Returns:
      bytes: The decrypted data

  Raises:
      ValueError: If decryption fails (wrong key, corrupted data, etc.)
  """
  try:
    # Get encryption key
    key = _get_or_create_encryption_key()

    # Create Fernet instance
    fernet = Fernet(key)

    # Decrypt the data
    decrypted_data = fernet.decrypt(encrypted_data)

    logger.debug(
      f"Decrypted {len(encrypted_data)} bytes to {len(decrypted_data)} bytes"
    )

    return decrypted_data

  except Exception as e:
    logger.error(f"Decryption failed: {e}")
    raise ValueError(f"Failed to decrypt data: {str(e)}")


def generate_encryption_key() -> str:
  """
  Generate a new Fernet encryption key.

  This is a utility function to generate keys for configuration.
  The generated key should be stored securely in AWS Secrets Manager
  or as an environment variable.

  Returns:
      str: A base64-encoded encryption key suitable for use with KUZU_BACKUP_ENCRYPTION_KEY
  """
  key = Fernet.generate_key()
  return key.decode("utf-8")


# For backward compatibility, provide these aliases
def encrypt(data: Union[bytes, str]) -> bytes:
  """Alias for encrypt_data for backward compatibility."""
  return encrypt_data(data)


def decrypt(encrypted_data: bytes) -> bytes:
  """Alias for decrypt_data for backward compatibility."""
  return decrypt_data(encrypted_data)
