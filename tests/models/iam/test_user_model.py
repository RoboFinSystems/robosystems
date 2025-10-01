"""Comprehensive tests for the User model."""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import User


class TestUserModel:
  """Test suite for the User model."""

  def test_user_initialization(self):
    """Test User model can be instantiated with required fields."""
    user = User(
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    assert user.email == "test@example.com"
    assert user.name == "Test User"
    assert user.password_hash == "hashed_password"
    # Default values are set by SQLAlchemy when the object is added to session
    # Before that, they are None
    assert user.is_active is None
    assert user.email_verified is None
    assert user.id is None  # ID is generated on commit

  def test_user_id_generation(self):
    """Test that User ID is generated with proper format."""
    User(
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    # Call the default lambda to generate ID
    # The lambda takes a context argument but doesn't use it
    generated_id = User.id.default.arg(None)
    assert generated_id.startswith("user_")
    assert len(generated_id) > 5  # user_ + token

  def test_user_repr(self):
    """Test User string representation."""
    user = User(
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    user.id = "user_test123"

    assert repr(user) == "<User user_test123 test@example.com>"

  def test_user_timestamps(self):
    """Test that created_at and updated_at are properly set."""
    User(
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    # Test default timestamp generation
    # The lambda takes a context argument but doesn't use it
    created = User.created_at.default.arg(None)
    updated = User.updated_at.default.arg(None)

    assert isinstance(created, datetime)
    assert isinstance(updated, datetime)
    assert created.tzinfo == timezone.utc
    assert updated.tzinfo == timezone.utc

  def test_get_by_id(self, db_session):
    """Test getting user by ID."""
    # Create a test user
    user = User(
      id="user_test123",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    db_session.add(user)
    db_session.commit()

    # Test get_by_id
    found_user = User.get_by_id("user_test123", db_session)
    assert found_user is not None
    assert found_user.id == "user_test123"
    assert found_user.email == "test@example.com"

    # Test with non-existent ID
    not_found = User.get_by_id("user_nonexistent", db_session)
    assert not_found is None

  def test_get_by_email(self, db_session):
    """Test getting user by email."""
    # Create a test user with unique identifiers
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    user_id = f"user_test_{unique_id}"
    email = f"test_{unique_id}@example.com"

    user = User(
      id=user_id,
      email=email,
      name="Test User",
      password_hash="hashed_password",
    )
    db_session.add(user)
    db_session.commit()

    # Test get_by_email
    found_user = User.get_by_email(email, db_session)
    assert found_user is not None
    assert found_user.id == user_id
    assert found_user.email == email

    # Test with non-existent email
    not_found = User.get_by_email("nonexistent@example.com", db_session)
    assert not_found is None

  def test_create_user(self, db_session):
    """Test creating a new user."""
    user = User.create(
      email="create@example.com",
      name="Created User",
      password_hash="hashed_password",
      session=db_session,
    )

    assert user.id is not None
    assert user.id.startswith("user_")
    assert user.email == "create@example.com"
    assert user.name == "Created User"
    assert user.password_hash == "hashed_password"
    assert user.is_active is True
    assert user.email_verified is False
    assert user.created_at is not None
    assert user.updated_at is not None

    # Verify in database
    db_user = db_session.query(User).filter_by(email="create@example.com").first()
    assert db_user is not None
    assert db_user.id == user.id

  def test_create_user_duplicate_email(self, db_session):
    """Test that creating user with duplicate email fails."""
    # Create first user
    User.create(
      email="duplicate@example.com",
      name="First User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Try to create second user with same email
    with pytest.raises(SQLAlchemyError):
      User.create(
        email="duplicate@example.com",
        name="Second User",
        password_hash="hashed_password",
        session=db_session,
      )

  def test_get_all_users(self, db_session):
    """Test getting all users."""
    # Clean up existing users to ensure test isolation
    from robosystems.models.iam import UserGraph, Graph, GraphCredits
    from robosystems.models.iam.graph_credits import GraphCreditTransaction
    from robosystems.models.iam.graph_usage_tracking import GraphUsageTracking

    try:
      from robosystems.models.iam.graph_backup import GraphBackup as _GraphBackup

      GraphBackup = _GraphBackup
      has_graph_backup = True
    except ImportError:
      GraphBackup = None  # type: ignore
      has_graph_backup = False
    try:
      from robosystems.models.iam.user_api_key import UserAPIKey as _UserAPIKey

      UserAPIKey = _UserAPIKey
      has_user_api_keys = True
    except ImportError:
      UserAPIKey = None  # type: ignore
      has_user_api_keys = False
    try:
      from robosystems.models.iam.user_limits import UserLimits as _UserLimits

      UserLimits = _UserLimits
      has_user_limits = True
    except ImportError:
      UserLimits = None  # type: ignore
      has_user_limits = False
    try:
      from robosystems.models.iam.graph_subscription import (
        GraphSubscription as _GraphSubscription,
      )

      GraphSubscription = _GraphSubscription
      has_graph_subscription = True
    except ImportError:
      GraphSubscription = None  # type: ignore
      has_graph_subscription = False

    # Delete in dependency order
    db_session.query(GraphUsageTracking).delete()
    db_session.query(GraphCreditTransaction).delete()
    db_session.query(GraphCredits).delete()
    db_session.query(UserGraph).delete()
    if has_graph_backup:
      db_session.query(GraphBackup).delete()  # type: ignore
    if has_user_api_keys:
      db_session.query(UserAPIKey).delete()  # type: ignore
    if has_user_limits:
      db_session.query(UserLimits).delete()  # type: ignore
    if has_graph_subscription:
      db_session.query(GraphSubscription).delete()  # type: ignore
    db_session.query(Graph).delete()
    db_session.query(User).delete()
    db_session.commit()

    # Create multiple users with unique emails
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    users_data = [
      (f"user1_{unique_id}@example.com", "User 1"),
      (f"user2_{unique_id}@example.com", "User 2"),
      (f"user3_{unique_id}@example.com", "User 3"),
    ]

    for email, name in users_data:
      User.create(
        email=email,
        name=name,
        password_hash="hashed_password",
        session=db_session,
      )

    # Get all users
    all_users = User.get_all(db_session)
    assert len(all_users) == 3

    emails = [user.email for user in all_users]
    assert f"user1_{unique_id}@example.com" in emails
    assert f"user2_{unique_id}@example.com" in emails
    assert f"user3_{unique_id}@example.com" in emails

  def test_update_user(self, db_session):
    """Test updating user fields."""
    # Create a user
    user = User.create(
      email="update@example.com",
      name="Original Name",
      password_hash="hashed_password",
      session=db_session,
    )
    original_updated_at = user.updated_at

    # Update user
    user.update(
      session=db_session,
      name="Updated Name",
      email="updated@example.com",
    )

    assert user.name == "Updated Name"
    assert user.email == "updated@example.com"
    assert user.updated_at > original_updated_at

    # Verify in database
    db_user = db_session.query(User).filter_by(id=user.id).first()
    assert db_user.name == "Updated Name"
    assert db_user.email == "updated@example.com"

  def test_update_user_no_autocommit(self, db_session):
    """Test updating user without auto-commit."""
    # Create a user
    user = User.create(
      email="nocommit@example.com",
      name="Original Name",
      password_hash="hashed_password",
      session=db_session,
    )

    # Update without auto-commit
    user.update(
      session=db_session,
      auto_commit=False,
      name="Updated Name",
    )

    assert user.name == "Updated Name"

    # Rollback and check that change wasn't persisted
    db_session.rollback()
    db_user = db_session.query(User).filter_by(id=user.id).first()
    assert db_user.name == "Original Name"

  def test_update_user_invalid_field(self, db_session):
    """Test that updating with invalid field doesn't cause error."""
    user = User.create(
      email="invalid@example.com",
      name="Test User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Update with invalid field - should be ignored
    user.update(
      session=db_session,
      invalid_field="value",
      name="Updated Name",
    )

    assert user.name == "Updated Name"
    assert not hasattr(user, "invalid_field")

  def test_delete_user(self, db_session):
    """Test deleting a user."""
    # Create a user
    user = User.create(
      email="delete@example.com",
      name="Delete Me",
      password_hash="hashed_password",
      session=db_session,
    )
    user_id = user.id

    # Delete user
    user.delete(db_session)

    # Verify user is deleted
    db_user = db_session.query(User).filter_by(id=user_id).first()
    assert db_user is None

  def test_verify_email(self, db_session):
    """Test email verification."""
    # Create a user
    user = User.create(
      email="verify@example.com",
      name="Unverified User",
      password_hash="hashed_password",
      session=db_session,
    )

    assert user.email_verified is False
    original_updated_at = user.updated_at

    # Verify email
    user.verify_email(db_session)

    assert user.email_verified is True
    assert user.updated_at > original_updated_at

    # Verify in database
    db_user = db_session.query(User).filter_by(id=user.id).first()
    assert db_user.email_verified is True

  def test_deactivate_user(self, db_session):
    """Test deactivating a user."""
    # Create an active user
    user = User.create(
      email="deactivate@example.com",
      name="Active User",
      password_hash="hashed_password",
      session=db_session,
    )

    assert user.is_active is True
    original_updated_at = user.updated_at

    # Deactivate user
    user.deactivate(db_session)

    assert user.is_active is False
    assert user.updated_at > original_updated_at

    # Verify in database
    db_user = db_session.query(User).filter_by(id=user.id).first()
    assert db_user.is_active is False

  def test_activate_user(self, db_session):
    """Test activating a user."""
    # Create a user and deactivate it
    user = User.create(
      email="activate@example.com",
      name="Inactive User",
      password_hash="hashed_password",
      session=db_session,
    )
    user.deactivate(db_session)

    assert user.is_active is False
    original_updated_at = user.updated_at

    # Activate user
    user.activate(db_session)

    assert user.is_active is True
    assert user.updated_at > original_updated_at

    # Verify in database
    db_user = db_session.query(User).filter_by(id=user.id).first()
    assert db_user.is_active is True

  def test_user_relationships(self):
    """Test that User model has correct relationship definitions."""
    user = User(
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    # Check relationship attributes exist
    assert hasattr(user, "user_api_keys")
    assert hasattr(user, "user_graphs")
    assert hasattr(user, "limits")
    assert hasattr(user, "user_repositories")
    assert hasattr(user, "graph_subscriptions")

  @patch("robosystems.models.iam.user.Session")
  def test_create_user_rollback_on_error(self, mock_session_class):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      User.create(
        email="error@example.com",
        name="Error User",
        password_hash="hashed_password",
        session=mock_session,
      )

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user.Session")
  def test_update_rollback_on_error(self, mock_session_class):
    """Test that update rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user = User(
      id="user_test",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    with pytest.raises(SQLAlchemyError):
      user.update(session=mock_session, name="New Name")

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user.Session")
  def test_delete_rollback_on_error(self, mock_session_class):
    """Test that delete rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user = User(
      id="user_test",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    with pytest.raises(SQLAlchemyError):
      user.delete(mock_session)

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user.Session")
  def test_verify_email_rollback_on_error(self, mock_session_class):
    """Test that verify_email rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user = User(
      id="user_test",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    with pytest.raises(SQLAlchemyError):
      user.verify_email(mock_session)

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user.Session")
  def test_deactivate_rollback_on_error(self, mock_session_class):
    """Test that deactivate rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user = User(
      id="user_test",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    with pytest.raises(SQLAlchemyError):
      user.deactivate(mock_session)

    mock_session.rollback.assert_called_once()

  @patch("robosystems.models.iam.user.Session")
  def test_activate_rollback_on_error(self, mock_session_class):
    """Test that activate rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user = User(
      id="user_test",
      email="test@example.com",
      name="Test User",
      password_hash="hashed_password",
    )

    with pytest.raises(SQLAlchemyError):
      user.activate(mock_session)

    mock_session.rollback.assert_called_once()
