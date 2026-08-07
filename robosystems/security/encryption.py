"""
Data encryption and decryption for sensitive artifacts such as backups.

Uses Fernet symmetric encryption (AES-128-CBC plus HMAC authentication) from
the cryptography library. The key lives in AWS Secrets Manager or the
environment — never in code.
"""

import base64

from cryptography.fernet import Fernet

from robosystems.config import env
from robosystems.logger import logger


def _get_encryption_key() -> bytes:
  """
  Get the encryption key for backup operations.

  Reads GRAPH_BACKUP_ENCRYPTION_KEY, which must be set in every environment
  (dev, staging, prod).

  Raises:
      ValueError: If the key is unset or not base64-encoded.
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
  Encrypt data using Fernet symmetric encryption. Strings are UTF-8 encoded.

  Raises:
      ValueError: If encryption fails.
  """
  try:
    if isinstance(data, str):
      data = data.encode("utf-8")

    key = _get_encryption_key()
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(data)

    logger.debug(f"Encrypted {len(data)} bytes to {len(encrypted_data)} bytes")

    return encrypted_data

  except Exception as e:
    logger.error(f"Encryption failed: {e}")
    raise ValueError(f"Failed to encrypt data: {e!s}")


def decrypt_data(encrypted_data: bytes) -> bytes:
  """
  Decrypt data that was encrypted with :func:`encrypt_data`.

  Raises:
      ValueError: If decryption fails — wrong key, corrupted or tampered data.
  """
  try:
    key = _get_encryption_key()
    fernet = Fernet(key)
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
  Generate a new Fernet key for GRAPH_BACKUP_ENCRYPTION_KEY.

  Returns a base64-encoded key to store in AWS Secrets Manager or the
  environment.
  """
  key = Fernet.generate_key()
  return key.decode("utf-8")


def encrypt(data: bytes | str) -> bytes:
  """Alias for :func:`encrypt_data`."""
  return encrypt_data(data)


def decrypt(encrypted_data: bytes) -> bytes:
  """Alias for :func:`decrypt_data`."""
  return decrypt_data(encrypted_data)
