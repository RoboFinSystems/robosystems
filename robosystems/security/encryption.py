"""
Data encryption and decryption utilities.

This module provides secure encryption/decryption for sensitive data like backups.
Uses Fernet (symmetric encryption) from the cryptography library which provides:
- AES 128-bit encryption in CBC mode
- HMAC for authentication

Note: The encryption key should be stored securely in AWS Secrets Manager
or environment variables, never in code.
"""

import base64

from cryptography.fernet import Fernet

from robosystems.config import env
from robosystems.logger import logger


def _get_encryption_key() -> bytes:
  """
  Get the encryption key for backup operations.

  Uses GRAPH_BACKUP_ENCRYPTION_KEY from environment configuration.
  The key must be set in all environments (dev, staging, prod).

  Returns:
      bytes: The encryption key

  Raises:
      ValueError: If the key is not set or has invalid format
  """
  encryption_key = env.GRAPH_BACKUP_ENCRYPTION_KEY

  if not encryption_key:
    raise ValueError(
      "GRAPH_BACKUP_ENCRYPTION_KEY must be set. "
      "Generate a key with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
    )

  try:
    return base64.urlsafe_b64decode(encryption_key)
  except Exception as e:
    logger.error(f"Invalid encryption key format: {e}")
    raise ValueError(
      f"Invalid GRAPH_BACKUP_ENCRYPTION_KEY format. Key must be base64-encoded: {e}"
    )


def encrypt_data(data: bytes | str) -> bytes:
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
    key = _get_encryption_key()

    # Create Fernet instance
    fernet = Fernet(key)

    # Encrypt the data
    encrypted_data = fernet.encrypt(data)

    logger.debug(f"Encrypted {len(data)} bytes to {len(encrypted_data)} bytes")

    return encrypted_data

  except Exception as e:
    logger.error(f"Encryption failed: {e}")
    raise ValueError(f"Failed to encrypt data: {e!s}")


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
    key = _get_encryption_key()

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
    raise ValueError(f"Failed to decrypt data: {e!s}")


def generate_encryption_key() -> str:
  """
  Generate a new Fernet encryption key.

  This is a utility function to generate keys for configuration.
  The generated key should be stored securely in AWS Secrets Manager
  or as an environment variable.

  Returns:
      str: A base64-encoded encryption key suitable for use with GRAPH_BACKUP_ENCRYPTION_KEY
  """
  key = Fernet.generate_key()
  return key.decode("utf-8")


# For backward compatibility, provide these aliases
def encrypt(data: bytes | str) -> bytes:
  """Alias for encrypt_data for backward compatibility."""
  return encrypt_data(data)


def decrypt(encrypted_data: bytes) -> bytes:
  """Alias for decrypt_data for backward compatibility."""
  return decrypt_data(encrypted_data)
