"""Comprehensive tests for the ConnectionCredentials model."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import ConnectionCredentials


class TestConnectionCredentialsModel:
  """Test suite for the ConnectionCredentials model."""

  def test_connection_credentials_initialization(self):
    """Test ConnectionCredentials model can be instantiated with required fields."""
    cred = ConnectionCredentials(
      connection_id="conn_test123",
      provider="QuickBooks",
      user_id="user_test123",
    )

    assert cred.connection_id == "conn_test123"
    assert cred.provider == "QuickBooks"
    assert cred.user_id == "user_test123"
    assert cred.encrypted_credentials is None
    assert cred.expires_at is None
    assert cred.is_active is None  # Default set by SQLAlchemy

  def test_connection_credentials_id_generation(self):
    """Test that ConnectionCredentials ID is generated with proper format."""
    ConnectionCredentials(
      connection_id="conn_test",
      provider="Plaid",
      user_id="user_test",
    )

    # Call the default lambda to generate ID
    generated_id = ConnectionCredentials.id.default.arg(None)
    assert generated_id.startswith("cred_")
    assert len(generated_id) > 5  # cred_ + token

  def test_connection_credentials_repr(self):
    """Test ConnectionCredentials string representation."""
    cred = ConnectionCredentials(
      connection_id="conn_test123",
      provider="SEC",
      user_id="user_test123",
    )
    cred.id = "cred_test123"

    assert repr(cred) == "<ConnectionCredentials cred_test123 SEC conn_test123>"

  @patch("robosystems.models.iam.connection_credentials.env")
  def test_get_encryption_key_with_env_key(self, mock_env):
    """Test getting encryption key when CONNECTION_CREDENTIALS_KEY is set."""
    # Generate a valid Fernet key
    valid_key = Fernet.generate_key().decode()
    mock_env.CONNECTION_CREDENTIALS_KEY = valid_key

    key = ConnectionCredentials._get_encryption_key()

    assert key == valid_key.encode()
    # Verify it's a valid Fernet key
    Fernet(key)  # Should not raise

  @patch("robosystems.models.iam.connection_credentials.env")
  @patch("robosystems.models.iam.connection_credentials.logger")
  def test_get_encryption_key_fallback_to_jwt(self, mock_logger, mock_env):
    """Test falling back to JWT_SECRET_KEY when CONNECTION_CREDENTIALS_KEY is not set."""
    mock_env.CONNECTION_CREDENTIALS_KEY = None
    mock_env.JWT_SECRET_KEY = "test_jwt_secret_key_123"

    key = ConnectionCredentials._get_encryption_key()

    assert key is not None
    # Verify it's a valid Fernet key
    Fernet(key)  # Should not raise
    mock_logger.warning.assert_called_once()

  @patch("robosystems.models.iam.connection_credentials.env")
  @patch("robosystems.models.iam.connection_credentials.logger")
  def test_get_encryption_key_no_keys(self, mock_logger, mock_env):
    """Test error when no encryption keys are available."""
    mock_env.CONNECTION_CREDENTIALS_KEY = None
    mock_env.JWT_SECRET_KEY = None

    with pytest.raises(ValueError, match="CONNECTION_CREDENTIALS_KEY must be set"):
      ConnectionCredentials._get_encryption_key()

    mock_logger.error.assert_called()

  @patch("robosystems.models.iam.connection_credentials.env")
  @patch("robosystems.models.iam.connection_credentials.logger")
  def test_get_encryption_key_invalid_format(self, mock_logger, mock_env):
    """Test error when encryption key has invalid format."""
    mock_env.CONNECTION_CREDENTIALS_KEY = "invalid_base64_key!!!"

    with pytest.raises(ValueError, match="Invalid encryption key format"):
      ConnectionCredentials._get_encryption_key()

    mock_logger.error.assert_called()

  def test_encrypt_decrypt_credentials(self):
    """Test encrypting and decrypting credentials."""
    cred = ConnectionCredentials(
      connection_id="conn_test",
      provider="QuickBooks",
      user_id="user_test",
    )

    # Test credentials
    test_creds = {
      "access_token": "test_access_token_123",
      "refresh_token": "test_refresh_token_456",
      "realm_id": "test_realm_789",
      "expires_in": 3600,
    }

    # Mock the encryption key
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Encrypt
      encrypted = cred._encrypt_credentials(test_creds)
      assert encrypted is not None
      assert isinstance(encrypted, str)
      assert encrypted != json.dumps(test_creds)  # Should be encrypted

      # Set and decrypt
      cred.encrypted_credentials = encrypted
      decrypted = cred._decrypt_credentials()
      assert decrypted == test_creds

  def test_set_get_credentials(self):
    """Test setting and getting credentials through public methods."""
    cred = ConnectionCredentials(
      connection_id="conn_test",
      provider="Plaid",
      user_id="user_test",
    )

    test_creds = {
      "client_id": "plaid_client_id",
      "secret": "plaid_secret",
      "access_token": "plaid_access_token",
    }

    # Mock the encryption key
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Set credentials
      cred.set_credentials(test_creds)
      assert cred.encrypted_credentials is not None

      # Get credentials
      retrieved = cred.get_credentials()
      assert retrieved == test_creds

  @patch("robosystems.models.iam.connection_credentials.logger")
  def test_encrypt_credentials_invalid_key(self, mock_logger):
    """Test encryption with invalid Fernet key."""
    cred = ConnectionCredentials(
      connection_id="conn_test",
      provider="SEC",
      user_id="user_test",
    )

    test_creds = {"api_key": "test_key"}

    # Mock an invalid encryption key
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=b"invalid_key_not_base64",
    ):
      with pytest.raises(Exception):
        cred._encrypt_credentials(test_creds)

      mock_logger.error.assert_called()

  def test_create_connection_credentials(self, db_session):
    """Test creating new connection credentials."""
    test_creds = {
      "access_token": "test_token",
      "refresh_token": "refresh_token",
      "expires_in": 3600,
    }

    # SQLAlchemy DateTime columns don't preserve timezone, so we use naive datetime
    expires_at = datetime.now().replace(microsecond=0) + timedelta(hours=1)

    # Mock the encryption key
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      cred = ConnectionCredentials.create(
        connection_id="conn_create",
        provider="QuickBooks",
        user_id="user_create",
        credentials=test_creds,
        session=db_session,
        expires_at=expires_at,
      )

      assert cred.id is not None
      assert cred.id.startswith("cred_")
      assert cred.connection_id == "conn_create"
      assert cred.provider == "QuickBooks"
      assert cred.user_id == "user_create"
      assert cred.expires_at == expires_at
      assert cred.is_active is True
      assert cred.encrypted_credentials is not None

      # Verify credentials were encrypted
      decrypted = cred.get_credentials()
      assert decrypted == test_creds

      # Verify in database
      db_cred = db_session.query(ConnectionCredentials).filter_by(id=cred.id).first()
      assert db_cred is not None
      assert db_cred.connection_id == "conn_create"

  def test_create_rollback_on_error(self):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    test_creds = {"token": "test"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      with pytest.raises(SQLAlchemyError):
        ConnectionCredentials.create(
          connection_id="conn_error",
          provider="SEC",
          user_id="user_error",
          credentials=test_creds,
          session=mock_session,
        )

      mock_session.rollback.assert_called_once()

  def test_get_by_connection_id(self, db_session):
    """Test getting credentials by connection ID."""
    test_creds = {"token": "find_me"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create credentials
      cred = ConnectionCredentials.create(
        connection_id="conn_find",
        provider="Plaid",
        user_id="user_find",
        credentials=test_creds,
        session=db_session,
      )

      # Find by connection ID
      found = ConnectionCredentials.get_by_connection_id("conn_find", db_session)
      assert found is not None
      assert found.id == cred.id
      assert found.connection_id == "conn_find"

      # Not found
      not_found = ConnectionCredentials.get_by_connection_id("nonexistent", db_session)
      assert not_found is None

  def test_get_by_connection_id_inactive(self, db_session):
    """Test that inactive credentials are not returned."""
    test_creds = {"token": "inactive"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create and deactivate credentials
      cred = ConnectionCredentials.create(
        connection_id="conn_inactive",
        provider="QuickBooks",
        user_id="user_inactive",
        credentials=test_creds,
        session=db_session,
      )

      cred.deactivate(db_session)

      # Should not find inactive credentials
      found = ConnectionCredentials.get_by_connection_id("conn_inactive", db_session)
      assert found is None

  def test_get_by_user_and_provider(self, db_session):
    """Test getting all credentials for a user and provider."""
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create multiple credentials
      for i in range(3):
        ConnectionCredentials.create(
          connection_id=f"conn_qb_{i}",
          provider="QuickBooks",
          user_id="user_multi",
          credentials={"token": f"token_{i}"},
          session=db_session,
        )

      # Create one for different provider
      ConnectionCredentials.create(
        connection_id="conn_plaid",
        provider="Plaid",
        user_id="user_multi",
        credentials={"token": "plaid_token"},
        session=db_session,
      )

      # Get QuickBooks credentials
      qb_creds = ConnectionCredentials.get_by_user_and_provider(
        "user_multi", "QuickBooks", db_session
      )
      assert len(qb_creds) == 3

      # Get Plaid credentials
      plaid_creds = ConnectionCredentials.get_by_user_and_provider(
        "user_multi", "Plaid", db_session
      )
      assert len(plaid_creds) == 1

  def test_update_credentials(self, db_session):
    """Test updating existing credentials."""
    initial_creds = {"token": "old_token", "secret": "old_secret"}
    new_creds = {"token": "new_token", "secret": "new_secret", "extra": "data"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create credentials
      cred = ConnectionCredentials.create(
        connection_id="conn_update",
        provider="SEC",
        user_id="user_update",
        credentials=initial_creds,
        session=db_session,
      )

      original_updated_at = cred.updated_at

      # Update credentials
      cred.update_credentials(new_creds, db_session)

      assert cred.updated_at > original_updated_at

      # Verify new credentials
      retrieved = cred.get_credentials()
      assert retrieved == new_creds

      # Verify in database
      db_cred = db_session.query(ConnectionCredentials).filter_by(id=cred.id).first()
      assert db_cred.get_credentials() == new_creds

  def test_update_credentials_rollback_on_error(self):
    """Test that update_credentials rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    cred = ConnectionCredentials(
      id="cred_test",
      connection_id="conn_test",
      provider="QuickBooks",
      user_id="user_test",
    )

    new_creds = {"token": "new"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      with pytest.raises(SQLAlchemyError):
        cred.update_credentials(new_creds, mock_session)

      mock_session.rollback.assert_called_once()

  def test_deactivate_credentials(self, db_session):
    """Test deactivating credentials."""
    test_creds = {"token": "deactivate_me"}

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create credentials
      cred = ConnectionCredentials.create(
        connection_id="conn_deactivate",
        provider="Plaid",
        user_id="user_deactivate",
        credentials=test_creds,
        session=db_session,
      )

      assert cred.is_active is True
      original_updated_at = cred.updated_at

      # Deactivate
      cred.deactivate(db_session)

      assert cred.is_active is False
      assert cred.updated_at > original_updated_at

      # Verify in database
      db_cred = db_session.query(ConnectionCredentials).filter_by(id=cred.id).first()
      assert db_cred.is_active is False

  def test_deactivate_rollback_on_error(self):
    """Test that deactivate rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    cred = ConnectionCredentials(
      id="cred_test",
      connection_id="conn_test",
      provider="SEC",
      user_id="user_test",
      is_active=True,
    )

    with pytest.raises(SQLAlchemyError):
      cred.deactivate(mock_session)

    mock_session.rollback.assert_called_once()

  def test_is_expired(self):
    """Test checking if credentials are expired."""
    # No expiry set
    cred1 = ConnectionCredentials(
      connection_id="conn_1",
      provider="QuickBooks",
      user_id="user_1",
    )
    assert cred1.is_expired() is False

    # Future expiry
    cred2 = ConnectionCredentials(
      connection_id="conn_2",
      provider="Plaid",
      user_id="user_2",
      expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    assert cred2.is_expired() is False

    # Past expiry
    cred3 = ConnectionCredentials(
      connection_id="conn_3",
      provider="SEC",
      user_id="user_3",
      expires_at=datetime.now(UTC) - timedelta(hours=1),
    )
    assert cred3.is_expired() is True

  def test_update_expiry(self, db_session):
    """Test updating credential expiry time."""
    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Create credentials without expiry
      cred = ConnectionCredentials.create(
        connection_id="conn_expiry",
        provider="QuickBooks",
        user_id="user_expiry",
        credentials={"token": "test"},
        session=db_session,
      )

      assert cred.expires_at is None
      original_updated_at = cred.updated_at

      # Update expiry (SQLAlchemy stores as naive datetime)
      new_expiry = datetime.now().replace(microsecond=0) + timedelta(hours=2)
      cred.update_expiry(new_expiry, db_session)

      assert cred.expires_at == new_expiry
      assert cred.updated_at > original_updated_at

      # Verify in database
      db_cred = db_session.query(ConnectionCredentials).filter_by(id=cred.id).first()
      assert db_cred.expires_at == new_expiry

  def test_update_expiry_rollback_on_error(self):
    """Test that update_expiry rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    cred = ConnectionCredentials(
      id="cred_test",
      connection_id="conn_test",
      provider="Plaid",
      user_id="user_test",
    )

    new_expiry = datetime.now(UTC) + timedelta(hours=1)

    with pytest.raises(SQLAlchemyError):
      cred.update_expiry(new_expiry, mock_session)

    mock_session.rollback.assert_called_once()

  def test_credentials_with_complex_data(self):
    """Test encrypting/decrypting complex credential structures."""
    cred = ConnectionCredentials(
      connection_id="conn_complex",
      provider="QuickBooks",
      user_id="user_complex",
    )

    complex_creds = {
      "tokens": {
        "access": "access_token_123",
        "refresh": "refresh_token_456",
        "id_token": "id_token_789",
      },
      "metadata": {
        "realm_id": "123456789",
        "company_name": "Test Company",
        "minor_version": 65,
        "base_url": "https://sandbox.api.intuit.com",
      },
      "scopes": ["com.intuit.quickbooks.accounting", "openid", "profile", "email"],
      "expires_at": "2024-01-01T00:00:00Z",
      "nested": {
        "deep": {
          "value": "nested_value",
          "array": [1, 2, 3, "four", {"five": 5}],
        }
      },
    }

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      # Set complex credentials
      cred.set_credentials(complex_creds)

      # Retrieve and verify
      retrieved = cred.get_credentials()
      assert retrieved == complex_creds
      assert retrieved["tokens"]["access"] == "access_token_123"
      assert retrieved["metadata"]["realm_id"] == "123456789"
      assert len(retrieved["scopes"]) == 4
      assert retrieved["nested"]["deep"]["array"][4]["five"] == 5

  def test_multiple_providers_same_user(self, db_session):
    """Test that a user can have credentials for multiple providers."""
    user_id = "user_multi_provider"
    providers = ["QuickBooks", "Plaid", "SEC", "Stripe"]

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=Fernet.generate_key(),
    ):
      created_creds = []
      for provider in providers:
        cred = ConnectionCredentials.create(
          connection_id=f"conn_{provider.lower()}",
          provider=provider,
          user_id=user_id,
          credentials={f"{provider.lower()}_token": "token_value"},
          session=db_session,
        )
        created_creds.append(cred)

      # Verify each provider's credentials
      for i, provider in enumerate(providers):
        creds_list = ConnectionCredentials.get_by_user_and_provider(
          user_id, provider, db_session
        )
        assert len(creds_list) == 1
        assert creds_list[0].provider == provider
        assert creds_list[0].id == created_creds[i].id

  def test_credential_encryption_consistency(self):
    """Test that encryption/decryption is consistent across instances."""
    test_creds = {
      "consistent": "value",
      "number": 42,
      "boolean": True,
    }

    # Use the same key for both instances
    encryption_key = Fernet.generate_key()

    with patch.object(
      ConnectionCredentials,
      "_get_encryption_key",
      return_value=encryption_key,
    ):
      # First instance encrypts
      cred1 = ConnectionCredentials(
        connection_id="conn_1",
        provider="Test",
        user_id="user_1",
      )
      cred1.set_credentials(test_creds)
      encrypted_value = cred1.encrypted_credentials

      # Second instance decrypts
      cred2 = ConnectionCredentials(
        connection_id="conn_2",
        provider="Test",
        user_id="user_2",
      )
      cred2.encrypted_credentials = encrypted_value
      decrypted = cred2.get_credentials()

      assert decrypted == test_creds
