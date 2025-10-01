"""
Comprehensive tests for encryption functionality.

Tests the encryption module which provides secure encryption/decryption
for sensitive data using Fernet symmetric encryption.
"""

import pytest
from unittest.mock import patch
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes

from robosystems.security.encryption import (
  encrypt_data,
  decrypt_data,
  generate_encryption_key,
  encrypt,
  decrypt,
  _get_or_create_encryption_key,
)


class TestEncryptionKeyGeneration:
  """Test encryption key generation and management."""

  def test_generate_encryption_key_format(self):
    """Test that generated keys are in proper format."""
    key = generate_encryption_key()

    assert isinstance(key, str)
    assert len(key) == 44  # Fernet keys are 44 characters when base64 encoded

    # Verify it's valid base64 (URL-safe variant used by Fernet)
    try:
      decoded = base64.urlsafe_b64decode(key)
      assert len(decoded) == 32  # 32 bytes for Fernet
    except Exception:
      pytest.fail("Generated key is not valid base64")

  def test_generate_encryption_key_uniqueness(self):
    """Test that generated keys are unique."""
    key1 = generate_encryption_key()
    key2 = generate_encryption_key()

    assert key1 != key2

  def test_generated_key_works_with_fernet(self):
    """Test that generated key works with Fernet encryption."""
    key = generate_encryption_key()

    # Should not raise exception
    fernet = Fernet(key.encode())

    # Test basic encryption/decryption
    test_data = b"test data"
    encrypted = fernet.encrypt(test_data)
    decrypted = fernet.decrypt(encrypted)

    assert decrypted == test_data


class TestGetOrCreateEncryptionKey:
  """Test encryption key retrieval and creation logic."""

  def test_get_key_from_environment_variable(self):
    """Test getting encryption key from environment variable."""
    test_key = Fernet.generate_key()
    encoded_key = base64.urlsafe_b64encode(test_key).decode()

    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = encoded_key

      result = _get_or_create_encryption_key()

      assert result == test_key

  def test_get_key_invalid_format(self):
    """Test handling of invalid encryption key format."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = "invalid_key_format"

      with pytest.raises(ValueError, match="Invalid KUZU_BACKUP_ENCRYPTION_KEY format"):
        _get_or_create_encryption_key()

  def test_production_requires_encryption_key(self):
    """Test that production environment requires explicit encryption key."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "prod"

      with pytest.raises(
        ValueError, match="KUZU_BACKUP_ENCRYPTION_KEY must be set in production"
      ):
        _get_or_create_encryption_key()

  def test_development_derives_key_from_password(self):
    """Test key derivation for development environment."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "test_password"

      with patch("robosystems.security.encryption.logger") as mock_logger:
        result = _get_or_create_encryption_key()

        assert isinstance(result, bytes)
        assert len(result) == 44  # Base64 encoded 32-byte key

        # Should log warning about derived key
        mock_logger.warning.assert_called_once()
        assert "Using derived encryption key" in mock_logger.warning.call_args[0][0]

  def test_development_key_deterministic(self):
    """Test that development key derivation is deterministic."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "same_password"

      with patch("robosystems.security.encryption.logger"):
        key1 = _get_or_create_encryption_key()
        key2 = _get_or_create_encryption_key()

        assert key1 == key2

  def test_development_different_passwords_different_keys(self):
    """Test that different passwords produce different keys."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "dev"

      with patch("robosystems.security.encryption.logger"):
        mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "password1"
        key1 = _get_or_create_encryption_key()

        mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "password2"
        key2 = _get_or_create_encryption_key()

        assert key1 != key2

  def test_pbkdf2_parameters(self):
    """Test PBKDF2 parameters for security."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "test_password"

      with patch("robosystems.security.encryption.logger"):
        with patch("robosystems.security.encryption.PBKDF2HMAC") as mock_kdf:
          mock_kdf.return_value.derive.return_value = b"x" * 32

          _get_or_create_encryption_key()

          # Verify PBKDF2 was called with secure parameters
          mock_kdf.assert_called_once()
          call_kwargs = mock_kdf.call_args[1]
          assert isinstance(call_kwargs["algorithm"], hashes.SHA256)
          assert call_kwargs["length"] == 32
          assert call_kwargs["iterations"] == 100000  # High iteration count
          assert call_kwargs["salt"] == b"robosystems-kuzu-backup-salt-v1"


class TestEncryptData:
  """Test data encryption functionality."""

  def test_encrypt_string_data(self):
    """Test encrypting string data."""
    test_string = "Hello, World!"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      encrypted = encrypt_data(test_string)

      assert isinstance(encrypted, bytes)
      assert len(encrypted) > len(test_string)  # Encrypted data is larger

  def test_encrypt_bytes_data(self):
    """Test encrypting bytes data."""
    test_bytes = b"Hello, World!"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      encrypted = encrypt_data(test_bytes)

      assert isinstance(encrypted, bytes)
      assert len(encrypted) > len(test_bytes)

  def test_encrypt_empty_data(self):
    """Test encrypting empty data."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      encrypted_str = encrypt_data("")
      encrypted_bytes = encrypt_data(b"")

      assert isinstance(encrypted_str, bytes)
      assert isinstance(encrypted_bytes, bytes)
      assert len(encrypted_str) > 0
      assert len(encrypted_bytes) > 0

  def test_encrypt_large_data(self):
    """Test encrypting large data."""
    large_data = "x" * 10000  # 10KB string

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      encrypted = encrypt_data(large_data)

      assert isinstance(encrypted, bytes)
      assert len(encrypted) > len(large_data)

  def test_encrypt_unicode_data(self):
    """Test encrypting unicode data."""
    unicode_string = "Hello 🌍! 你好世界! مرحبا بالعالم!"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      encrypted = encrypt_data(unicode_string)

      assert isinstance(encrypted, bytes)
      assert len(encrypted) > 0

  def test_encrypt_key_derivation_failure(self):
    """Test encryption failure when key derivation fails."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.side_effect = ValueError("Key derivation failed")

      with pytest.raises(ValueError, match="Failed to encrypt data"):
        encrypt_data("test data")

  def test_encrypt_fernet_failure(self):
    """Test encryption failure when Fernet operations fail."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = b"invalid_key"  # Invalid Fernet key

      with pytest.raises(ValueError, match="Failed to encrypt data"):
        encrypt_data("test data")

  def test_encrypt_logging(self):
    """Test encryption debug logging."""
    test_data = "test data"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with patch("robosystems.security.encryption.logger") as mock_logger:
        encrypt_data(test_data)

        # Should log debug message about encryption
        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert "Encrypted" in log_message
        assert "bytes" in log_message


class TestDecryptData:
  """Test data decryption functionality."""

  def test_decrypt_valid_data(self):
    """Test decrypting valid encrypted data."""
    test_data = b"Hello, World!"
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(test_data)

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      decrypted = decrypt_data(encrypted)

      assert decrypted == test_data

  def test_decrypt_string_roundtrip(self):
    """Test string encryption/decryption roundtrip."""
    test_string = "Hello, World! 🌍"
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      encrypted = encrypt_data(test_string)
      decrypted = decrypt_data(encrypted)

      # Decrypted data should match original when decoded
      assert decrypted.decode("utf-8") == test_string

  def test_decrypt_bytes_roundtrip(self):
    """Test bytes encryption/decryption roundtrip."""
    test_bytes = b"Binary data \x00\x01\x02\xff"
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      encrypted = encrypt_data(test_bytes)
      decrypted = decrypt_data(encrypted)

      assert decrypted == test_bytes

  def test_decrypt_wrong_key(self):
    """Test decryption with wrong key."""
    test_data = b"secret data"
    key1 = Fernet.generate_key()
    key2 = Fernet.generate_key()

    # Encrypt with key1
    fernet1 = Fernet(key1)
    encrypted = fernet1.encrypt(test_data)

    # Try to decrypt with key2
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key2

      with pytest.raises(ValueError, match="Failed to decrypt data"):
        decrypt_data(encrypted)

  def test_decrypt_corrupted_data(self):
    """Test decryption with corrupted data."""
    corrupted_data = b"this is not encrypted data"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with pytest.raises(ValueError, match="Failed to decrypt data"):
        decrypt_data(corrupted_data)

  def test_decrypt_empty_data(self):
    """Test decryption with empty data."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with pytest.raises(ValueError, match="Failed to decrypt data"):
        decrypt_data(b"")

  def test_decrypt_key_derivation_failure(self):
    """Test decryption failure when key derivation fails."""
    encrypted_data = b"some encrypted data"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.side_effect = ValueError("Key derivation failed")

      with pytest.raises(ValueError, match="Failed to decrypt data"):
        decrypt_data(encrypted_data)

  def test_decrypt_logging(self):
    """Test decryption debug logging."""
    test_data = b"test data"
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(test_data)

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      with patch("robosystems.security.encryption.logger") as mock_logger:
        decrypt_data(encrypted)

        # Should log debug message about decryption
        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert "Decrypted" in log_message
        assert "bytes" in log_message

  def test_decrypt_error_logging(self):
    """Test decryption error logging."""
    corrupted_data = b"corrupted"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with patch("robosystems.security.encryption.logger") as mock_logger:
        with pytest.raises(ValueError):
          decrypt_data(corrupted_data)

        # Should log error message
        mock_logger.error.assert_called_once()
        log_message = mock_logger.error.call_args[0][0]
        assert "Decryption failed" in log_message


class TestBackwardCompatibility:
  """Test backward compatibility aliases."""

  def test_encrypt_alias(self):
    """Test encrypt function alias."""
    test_data = "test data"

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      result1 = encrypt(test_data)
      result2 = encrypt_data(test_data)

      # Both should produce encrypted bytes (content will differ due to randomization)
      assert isinstance(result1, bytes)
      assert isinstance(result2, bytes)

  def test_decrypt_alias(self):
    """Test decrypt function alias."""
    test_data = b"test data"
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(test_data)

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      result1 = decrypt(encrypted)
      result2 = decrypt_data(encrypted)

      assert result1 == test_data
      assert result2 == test_data
      assert result1 == result2

  def test_alias_roundtrip(self):
    """Test roundtrip using backward compatibility aliases."""
    test_string = "Hello, compatibility!"
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      # Encrypt with alias
      encrypted = encrypt(test_string)

      # Decrypt with alias
      decrypted = decrypt(encrypted)

      assert decrypted.decode("utf-8") == test_string


class TestEncryptionSecurity:
  """Test security aspects of encryption implementation."""

  def test_encryption_randomization(self):
    """Test that encryption produces different output for same input."""
    test_data = "same input data"
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      encrypted1 = encrypt_data(test_data)
      encrypted2 = encrypt_data(test_data)

      # Same input should produce different encrypted output (due to IV/nonce)
      assert encrypted1 != encrypted2

      # But both should decrypt to the same value
      decrypted1 = decrypt_data(encrypted1)
      decrypted2 = decrypt_data(encrypted2)
      assert decrypted1 == decrypted2

  def test_fernet_authentication(self):
    """Test Fernet's built-in authentication."""
    test_data = b"authenticated data"
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(test_data)

    # Tamper with encrypted data
    tampered = bytearray(encrypted)
    tampered[10] = (tampered[10] + 1) % 256  # Flip a bit
    tampered_bytes = bytes(tampered)

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      # Should detect tampering and fail
      with pytest.raises(ValueError, match="Failed to decrypt data"):
        decrypt_data(tampered_bytes)

  def test_key_material_handling(self):
    """Test secure handling of key material."""
    # Test that keys are properly formatted
    with patch("robosystems.security.encryption.env") as mock_env:
      # Valid Fernet key
      valid_key = Fernet.generate_key()
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = base64.urlsafe_b64encode(valid_key).decode()

      retrieved_key = _get_or_create_encryption_key()
      assert retrieved_key == valid_key

  def test_salt_consistency(self):
    """Test that salt is consistent for development key derivation."""
    password = "test_password"

    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = password

      with patch("robosystems.security.encryption.logger"):
        key1 = _get_or_create_encryption_key()
        key2 = _get_or_create_encryption_key()

        # Same password and salt should produce same key
        assert key1 == key2

  def test_environment_isolation(self):
    """Test that different environments can use different keys."""
    # This is more of a documentation test to ensure environment isolation
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = None

      # Test dev environment
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "dev_password"
      with patch("robosystems.security.encryption.logger"):
        dev_key = _get_or_create_encryption_key()

      # Test staging environment
      mock_env.ENVIRONMENT = "staging"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "staging_password"
      with patch("robosystems.security.encryption.logger"):
        staging_key = _get_or_create_encryption_key()

      # Different environments should be able to have different keys
      assert isinstance(dev_key, bytes)
      assert isinstance(staging_key, bytes)
      # Keys could be same or different depending on passwords


class TestEncryptionPerformance:
  """Test performance aspects of encryption."""

  def test_encrypt_large_data_performance(self):
    """Test encryption performance with large data."""
    # 1MB of data
    large_data = "x" * (1024 * 1024)
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      # Should complete without timeout
      encrypted = encrypt_data(large_data)
      decrypted = decrypt_data(encrypted)

      assert decrypted.decode("utf-8") == large_data

  def test_key_caching_behavior(self):
    """Test that key derivation is not unnecessarily repeated."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      # Multiple encryption calls
      encrypt_data("data1")
      encrypt_data("data2")
      encrypt_data("data3")

      # Key should be retrieved for each call (no caching in current implementation)
      assert mock_key.call_count == 3

  def test_memory_efficiency(self):
    """Test memory efficiency with various data sizes."""
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      # Test various sizes
      sizes = [100, 1000, 10000, 100000]  # 100B to 100KB

      for size in sizes:
        test_data = "x" * size
        encrypted = encrypt_data(test_data)
        decrypted = decrypt_data(encrypted)

        assert len(decrypted.decode("utf-8")) == size
        # Encrypted size should be reasonable (not exponentially larger)
        # Fernet uses base64 encoding (33% expansion) plus encryption overhead
        # Total overhead can be ~50% of original + fixed overhead
        overhead_factor = 1.5  # 50% expansion
        fixed_overhead = 100  # Fixed bytes for IV, HMAC, metadata
        assert len(encrypted) < size * overhead_factor + fixed_overhead


class TestEncryptionEdgeCases:
  """Test edge cases and error conditions."""

  def test_none_input_handling(self):
    """Test handling of None inputs."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with pytest.raises(
        ValueError, match="Failed to encrypt data"
      ):  # Wrapped in ValueError
        encrypt_data(None)

  def test_numeric_input_handling(self):
    """Test handling of numeric inputs."""
    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = Fernet.generate_key()

      with pytest.raises(
        ValueError, match="Failed to encrypt data"
      ):  # Wrapped in ValueError
        encrypt_data(12345)

  def test_binary_data_with_null_bytes(self):
    """Test handling of binary data containing null bytes."""
    test_data = b"data\x00with\x00null\x00bytes"
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      encrypted = encrypt_data(test_data)
      decrypted = decrypt_data(encrypted)

      assert decrypted == test_data

  def test_very_long_string(self):
    """Test encryption of very long strings."""
    # Create a string that might cause buffer issues
    very_long_string = "A" * 1000000  # 1 million characters
    key = Fernet.generate_key()

    with patch(
      "robosystems.security.encryption._get_or_create_encryption_key"
    ) as mock_key:
      mock_key.return_value = key

      encrypted = encrypt_data(very_long_string)
      decrypted = decrypt_data(encrypted)

      assert decrypted.decode("utf-8") == very_long_string

  def test_environment_variable_edge_cases(self):
    """Test edge cases in environment variable handling."""
    with patch("robosystems.security.encryption.env") as mock_env:
      # Test empty string
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.KUZU_BACKUP_ENCRYPTION_PASSWORD = "fallback_password"

      with patch("robosystems.security.encryption.logger"):
        # Should fall back to key derivation
        key = _get_or_create_encryption_key()
        assert isinstance(key, bytes)

  def test_invalid_base64_key(self):
    """Test handling of invalid base64 encryption key."""
    with patch("robosystems.security.encryption.env") as mock_env:
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = "invalid base64 key!"

      # Invalid base64 will decode but produce wrong length, causing Fernet to fail
      with pytest.raises(ValueError, match="Failed to encrypt data"):
        encrypt_data("test")

  def test_short_key_handling(self):
    """Test handling of keys that are too short."""
    with patch("robosystems.security.encryption.env") as mock_env:
      # Valid base64 but wrong length for Fernet
      short_key = base64.urlsafe_b64encode(b"short").decode()
      mock_env.KUZU_BACKUP_ENCRYPTION_KEY = short_key

      with pytest.raises(ValueError):
        # This should fail when trying to create Fernet instance
        encrypt_data("test")
