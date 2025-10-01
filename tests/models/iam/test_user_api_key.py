"""Comprehensive tests for the UserAPIKey model."""

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
import bcrypt

from robosystems.models.iam import UserAPIKey, User


class TestUserAPIKeyModel:
  """Test suite for the UserAPIKey model."""

  def test_user_api_key_initialization(self):
    """Test UserAPIKey model can be instantiated with required fields."""
    api_key = UserAPIKey(
      user_id="user_test123",
      name="Test API Key",
      key_hash="hashed_key",
      prefix="rfs12345",
    )

    assert api_key.user_id == "user_test123"
    assert api_key.name == "Test API Key"
    assert api_key.key_hash == "hashed_key"
    assert api_key.prefix == "rfs12345"
    # Default values are set by SQLAlchemy when the object is added to session
    assert api_key.is_active is None
    assert api_key.description is None
    assert api_key.last_used_at is None

  def test_user_api_key_id_generation(self):
    """Test that UserAPIKey ID is generated with proper format."""
    UserAPIKey(
      user_id="user_test123",
      name="Test API Key",
      key_hash="hashed_key",
      prefix="rfs12345",
    )

    # Call the default lambda to generate ID
    generated_id = UserAPIKey.id.default.arg(None)
    assert generated_id.startswith("uak_")
    assert len(generated_id) > 4  # uak_ + token

  def test_user_api_key_repr(self):
    """Test UserAPIKey string representation."""
    api_key = UserAPIKey(
      user_id="user_test123",
      name="Test API Key",
      key_hash="hashed_key",
      prefix="rfs12345",
    )
    api_key.id = "uak_test123"

    assert repr(api_key) == "<UserAPIKey uak_test123 Test API Key user=user_test123>"

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_create_api_key(self, mock_audit_logger, db_session):
    """Test creating a new API key."""
    # Create a test user first
    user = User.create(
      email="apikey@example.com",
      name="API Key User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create API key
    api_key, plain_key = UserAPIKey.create(
      user_id=user.id,
      name="My API Key",
      description="Test API key description",
      session=db_session,
    )

    assert api_key.id is not None
    assert api_key.id.startswith("uak_")
    assert api_key.user_id == user.id
    assert api_key.name == "My API Key"
    assert api_key.description == "Test API key description"
    assert api_key.is_active is True
    assert api_key.created_at is not None
    assert api_key.updated_at is not None

    # Check plain key format
    assert plain_key.startswith("rfs")
    assert len(plain_key) == 67  # rfs + 64 hex chars
    assert api_key.prefix == plain_key[:8]

    # Verify in database
    db_api_key = db_session.query(UserAPIKey).filter_by(id=api_key.id).first()
    assert db_api_key is not None
    assert db_api_key.id == api_key.id

    # Verify security audit logging
    mock_audit_logger.log_security_event.assert_called_once()

  def test_create_api_key_without_session(self):
    """Test that creating API key without session raises error."""
    with pytest.raises(ValueError, match="Session is required"):
      UserAPIKey.create(
        user_id="user_test123",
        name="Test Key",
        session=None,
      )

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_create_api_key_rollback_on_error(self, mock_audit_logger):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      UserAPIKey.create(
        user_id="user_test123",
        name="Error Key",
        session=mock_session,
      )

    mock_session.rollback.assert_called_once()

    # Verify failure was logged
    calls = mock_audit_logger.log_security_event.call_args_list
    assert len(calls) == 1
    assert calls[0][1]["event_type"].value == "suspicious_activity"

  def test_hash_api_key(self):
    """Test API key hashing."""
    plain_key = "rfs" + "a" * 64
    hashed = UserAPIKey._hash_api_key(plain_key)

    assert hashed is not None
    assert len(hashed) > 0
    assert hashed != plain_key

    # Verify it's a valid bcrypt hash
    assert bcrypt.checkpw(plain_key.encode("utf-8"), hashed.encode("utf-8"))

  def test_verify_api_key(self):
    """Test API key verification."""
    plain_key = "rfs" + "a" * 64
    hashed = UserAPIKey._hash_api_key(plain_key)

    # Correct key should verify
    assert UserAPIKey._verify_api_key(plain_key, hashed) is True

    # Wrong key should not verify
    wrong_key = "rfs" + "b" * 64
    assert UserAPIKey._verify_api_key(wrong_key, hashed) is False

  def test_verify_api_key_with_invalid_hash(self):
    """Test API key verification with invalid hash."""
    plain_key = "rfs" + "a" * 64
    invalid_hash = "invalid_hash"

    assert UserAPIKey._verify_api_key(plain_key, invalid_hash) is False

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_get_by_key(self, mock_audit_logger, db_session):
    """Test getting API key by plain text key."""
    # Create a test user and API key
    user = User.create(
      email="getkey@example.com",
      name="Get Key User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, plain_key = UserAPIKey.create(
      user_id=user.id,
      name="Find Me",
      session=db_session,
    )

    # Test get_by_key with correct key
    found = UserAPIKey.get_by_key(plain_key, db_session)
    assert found is not None
    assert found.id == api_key.id
    assert found.name == "Find Me"
    assert found.last_used_at is not None  # Should be updated

    # Verify success was logged
    calls = mock_audit_logger.log_security_event.call_args_list
    assert any(
      call[1]["details"]["action"] == "api_key_verification_success" for call in calls
    )

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_get_by_key_wrong_key(self, mock_audit_logger, db_session):
    """Test getting API key with wrong plain text key."""
    # Create a test user and API key
    user = User.create(
      email="wrongkey@example.com",
      name="Wrong Key User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, plain_key = UserAPIKey.create(
      user_id=user.id,
      name="Can't Find Me",
      session=db_session,
    )

    # Test get_by_key with wrong key but same prefix
    wrong_key = plain_key[:8] + "x" * 59  # Same prefix, different key
    found = UserAPIKey.get_by_key(wrong_key, db_session)
    assert found is None

    # Verify failure was logged
    calls = mock_audit_logger.log_security_event.call_args_list
    assert any(
      call[1]["details"]["action"] == "api_key_verification_failed" for call in calls
    )

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_get_by_key_invalid_input(self, mock_audit_logger, db_session):
    """Test getting API key with invalid input."""
    # Test with None
    assert UserAPIKey.get_by_key(None, db_session) is None

    # Test with empty string
    assert UserAPIKey.get_by_key("", db_session) is None

    # Test with non-string
    assert UserAPIKey.get_by_key(123, db_session) is None

    # Verify validation failure was logged
    assert mock_audit_logger.log_input_validation_failure.called

  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_get_by_key_inactive(self, mock_audit_logger, db_session):
    """Test that inactive API keys are not returned."""
    # Create a test user and API key
    user = User.create(
      email="inactive@example.com",
      name="Inactive User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, plain_key = UserAPIKey.create(
      user_id=user.id,
      name="Inactive Key",
      session=db_session,
    )

    # Deactivate the key
    api_key.deactivate(db_session)

    # Try to get the inactive key
    found = UserAPIKey.get_by_key(plain_key, db_session)
    assert found is None

  def test_get_by_hash(self, db_session):
    """Test getting API key by hash."""
    # Create a test user and API key
    user = User.create(
      email="byhash@example.com",
      name="By Hash User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Hash Key",
      session=db_session,
    )

    # Test get_by_hash
    found = UserAPIKey.get_by_hash(api_key.key_hash, db_session)
    assert found is not None
    assert found.id == api_key.id

    # Test with non-existent hash
    not_found = UserAPIKey.get_by_hash("nonexistent_hash", db_session)
    assert not_found is None

  def test_get_by_user_id(self, db_session):
    """Test getting all API keys for a user."""
    # Create a test user
    user = User.create(
      email="userkeys@example.com",
      name="User Keys",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create multiple API keys
    keys = []
    for i in range(3):
      api_key, _ = UserAPIKey.create(
        user_id=user.id,
        name=f"Key {i}",
        session=db_session,
      )
      keys.append(api_key)

    # Get all keys for user
    user_keys = UserAPIKey.get_by_user_id(user.id, db_session)
    assert len(user_keys) == 3

    key_names = [k.name for k in user_keys]
    assert "Key 0" in key_names
    assert "Key 1" in key_names
    assert "Key 2" in key_names

  def test_get_active_by_user_id(self, db_session):
    """Test getting only active API keys for a user."""
    # Create a test user
    user = User.create(
      email="activekeys@example.com",
      name="Active Keys",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create multiple API keys
    active_key1, _ = UserAPIKey.create(
      user_id=user.id,
      name="Active Key 1",
      session=db_session,
    )
    active_key2, _ = UserAPIKey.create(
      user_id=user.id,
      name="Active Key 2",
      session=db_session,
    )
    inactive_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Inactive Key",
      session=db_session,
    )

    # Deactivate one key
    inactive_key.deactivate(db_session)

    # Get only active keys
    active_keys = UserAPIKey.get_active_by_user_id(user.id, db_session)
    assert len(active_keys) == 2

    key_names = [k.name for k in active_keys]
    assert "Active Key 1" in key_names
    assert "Active Key 2" in key_names
    assert "Inactive Key" not in key_names

  def test_update_last_used(self, db_session):
    """Test updating last used timestamp."""
    # Create a test user and API key
    user = User.create(
      email="lastused@example.com",
      name="Last Used",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Update Last Used",
      session=db_session,
    )

    original_updated_at = api_key.updated_at
    assert api_key.last_used_at is None

    # Update last used
    api_key.update_last_used(db_session)

    assert api_key.last_used_at is not None
    assert api_key.updated_at > original_updated_at

    # Verify in database
    db_key = db_session.query(UserAPIKey).filter_by(id=api_key.id).first()
    assert db_key.last_used_at is not None

  def test_update_last_used_no_autocommit(self, db_session):
    """Test updating last used without auto-commit."""
    # Create a test user and API key
    user = User.create(
      email="nocommit@example.com",
      name="No Commit",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="No Auto Commit",
      session=db_session,
    )

    # Update without auto-commit
    api_key.update_last_used(db_session, auto_commit=False)
    assert api_key.last_used_at is not None

    # Rollback and check that change wasn't persisted
    db_session.rollback()
    db_key = db_session.query(UserAPIKey).filter_by(id=api_key.id).first()
    assert db_key.last_used_at is None

  @patch("robosystems.models.iam.user_api_key.UserAPIKey._invalidate_cache")
  def test_deactivate_api_key(self, mock_invalidate, db_session):
    """Test deactivating an API key."""
    # Create a test user and API key
    user = User.create(
      email="deactivate@example.com",
      name="Deactivate User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Deactivate Me",
      session=db_session,
    )

    assert api_key.is_active is True
    original_updated_at = api_key.updated_at

    # Deactivate
    api_key.deactivate(db_session)

    assert api_key.is_active is False
    assert api_key.updated_at > original_updated_at

    # Verify cache invalidation
    mock_invalidate.assert_called_once()

    # Verify in database
    db_key = db_session.query(UserAPIKey).filter_by(id=api_key.id).first()
    assert db_key.is_active is False

  @patch("robosystems.models.iam.user_api_key.UserAPIKey._invalidate_cache")
  def test_activate_api_key(self, mock_invalidate, db_session):
    """Test activating an API key."""
    # Create a test user and API key
    user = User.create(
      email="activate@example.com",
      name="Activate User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Activate Me",
      session=db_session,
    )

    # Deactivate first
    api_key.deactivate(db_session)
    assert api_key.is_active is False
    original_updated_at = api_key.updated_at

    # Clear the mock
    mock_invalidate.reset_mock()

    # Activate
    api_key.activate(db_session)

    assert api_key.is_active is True
    assert api_key.updated_at > original_updated_at

    # Verify cache invalidation
    mock_invalidate.assert_called_once()

    # Verify in database
    db_key = db_session.query(UserAPIKey).filter_by(id=api_key.id).first()
    assert db_key.is_active is True

  @patch("robosystems.models.iam.user_api_key.UserAPIKey._invalidate_cache")
  def test_delete_api_key(self, mock_invalidate, db_session):
    """Test deleting an API key."""
    # Create a test user and API key
    user = User.create(
      email="delete@example.com",
      name="Delete User",
      password_hash="hashed_password",
      session=db_session,
    )

    api_key, _ = UserAPIKey.create(
      user_id=user.id,
      name="Delete Me",
      session=db_session,
    )
    key_id = api_key.id

    # Delete
    api_key.delete(db_session)

    # Verify cache invalidation
    mock_invalidate.assert_called_once()

    # Verify deletion in database
    db_key = db_session.query(UserAPIKey).filter_by(id=key_id).first()
    assert db_key is None

  @patch("importlib.import_module")
  @patch("robosystems.models.iam.user_api_key.SecurityAuditLogger")
  def test_invalidate_cache(self, mock_audit_logger, mock_import_module, db_session):
    """Test cache invalidation."""
    # Mock the cache module
    mock_cache_module = MagicMock()
    mock_api_key_cache = MagicMock()
    mock_cache_module.api_key_cache = mock_api_key_cache
    mock_import_module.return_value = mock_cache_module

    api_key = UserAPIKey(
      id="uak_test",
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
    )

    # Call invalidate cache
    api_key._invalidate_cache()

    # Verify cache invalidation was called
    mock_api_key_cache.invalidate_api_key.assert_called_once_with("test_hash")

    # Verify security audit logging
    mock_audit_logger.log_security_event.assert_called_once()

  @patch("importlib.import_module")
  @patch("robosystems.models.iam.user_api_key.logger")
  def test_invalidate_cache_error(self, mock_logger, mock_import_module):
    """Test cache invalidation error handling."""
    # Mock import failure
    mock_import_module.side_effect = ImportError("Module not found")

    api_key = UserAPIKey(
      id="uak_test",
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
    )

    # Call invalidate cache - should not raise
    api_key._invalidate_cache()

    # Verify error was logged
    mock_logger.error.assert_called_once()

  def test_user_api_key_relationships(self):
    """Test that UserAPIKey model has correct relationship definitions."""
    api_key = UserAPIKey(
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
    )

    # Check relationship attributes exist
    assert hasattr(api_key, "user")

  @patch("robosystems.models.iam.user_api_key.Session")
  def test_deactivate_rollback_on_error(self, mock_session_class):
    """Test that deactivate rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    api_key = UserAPIKey(
      id="uak_test",
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
    )

    with pytest.raises(SQLAlchemyError):
      api_key.deactivate(mock_session)

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user_api_key.Session")
  def test_activate_rollback_on_error(self, mock_session_class):
    """Test that activate rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    api_key = UserAPIKey(
      id="uak_test",
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
      is_active=False,
    )

    with pytest.raises(SQLAlchemyError):
      api_key.activate(mock_session)

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user_api_key.Session")
  def test_delete_rollback_on_error(self, mock_session_class):
    """Test that delete rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    api_key = UserAPIKey(
      id="uak_test",
      user_id="user_test",
      name="Test Key",
      key_hash="test_hash",
      prefix="rfs12345",
    )

    # Mock _invalidate_cache to avoid import issues
    with patch.object(api_key, "_invalidate_cache"):
      with pytest.raises(SQLAlchemyError):
        api_key.delete(mock_session)

      mock_session.rollback.assert_called_once()

  def test_hash_api_key_error(self):
    """Test API key hashing error handling."""
    with patch("robosystems.models.iam.user_api_key.bcrypt.gensalt") as mock_gensalt:
      mock_gensalt.side_effect = Exception("Hashing failed")

      with pytest.raises(ValueError, match="API key hashing failed"):
        UserAPIKey._hash_api_key("test_key")
