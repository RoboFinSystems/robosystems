"""Comprehensive tests for the User model."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.core import User


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
    assert created.tzinfo == UTC
    assert updated.tzinfo == UTC

  def test_get_by_id(self, db_session):
    """Test getting user by ID."""
    # Use unique identifiers because db_session shares a session-scoped
    # Postgres with the rest of the suite (notably test_auth.py registers
    # users with the literal address ``test@example.com``).
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

    # Test get_by_id
    found_user = User.get_by_id(user_id, db_session)
    assert found_user is not None
    assert found_user.id == user_id
    assert found_user.email == email

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
    # Use a unique email — db_session is session-scoped and shared with
    # tests/routers/auth/test_auth.py::test_register_duplicate_email,
    # which already inserts ``duplicate@example.com``. The conftest
    # cleanup fixture has known holes (e.g. it doesn't delete OrgLimits,
    # which blocks Org deletion via FK and rolls back the User delete),
    # so we can't rely on the email being absent.
    import uuid

    unique_email = f"duplicate_{uuid.uuid4().hex[:8]}@example.com"

    # Create first user
    User.create(
      email=unique_email,
      name="First User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Try to create second user with same email
    with pytest.raises(SQLAlchemyError):
      User.create(
        email=unique_email,
        name="Second User",
        password_hash="hashed_password",
        session=db_session,
      )

  def test_get_all_users(self, db_session):
    """Test getting all users."""
    # Clean up existing users to ensure test isolation
    from robosystems.models.core import Graph, GraphCredits, GraphUser
    from robosystems.models.core.graph.graph_credits import GraphCreditTransaction
    from robosystems.models.core.graph.graph_usage import GraphUsage

    try:
      from robosystems.models.core.graph.graph_backup import GraphBackup as _GraphBackup

      GraphBackup = _GraphBackup
      has_graph_backup = True
    except ImportError:
      GraphBackup = None  # type: ignore
      has_graph_backup = False
    try:
      from robosystems.models.core.user.user_api_key import UserAPIKey as _UserAPIKey

      UserAPIKey = _UserAPIKey
      has_user_api_keys = True
    except ImportError:
      UserAPIKey = None  # type: ignore
      has_user_api_keys = False
    UserLimits = None  # type: ignore[assignment]
    has_user_limits = False
    try:
      from robosystems.models.core.graph_subscription import (  # type: ignore
        GraphSubscription as _GraphSubscription,
      )

      GraphSubscription = _GraphSubscription
      has_graph_subscription = True
    except ImportError:
      GraphSubscription = None  # type: ignore
      has_graph_subscription = False

    # Delete in dependency order
    db_session.query(GraphUsage).delete()
    db_session.query(GraphCreditTransaction).delete()
    db_session.query(GraphCredits).delete()
    db_session.query(GraphUser).delete()
    if has_graph_backup:
      db_session.query(GraphBackup).delete()  # type: ignore
    if has_user_api_keys:
      db_session.query(UserAPIKey).delete()  # type: ignore
    if has_user_limits:
      db_session.query(UserLimits).delete()  # type: ignore
    if has_graph_subscription:
      db_session.query(GraphSubscription).delete()  # type: ignore
    db_session.query(Graph).delete()

    # Delete billing tables before deleting users (in dependency order)
    try:
      from robosystems.models.core.billing import (
        BillingAuditLog,
        BillingCustomer,
        BillingInvoice,
        BillingInvoiceLineItem,
        BillingSubscription,
      )

      db_session.query(BillingAuditLog).delete()
      db_session.query(BillingInvoiceLineItem).delete()
      db_session.query(BillingInvoice).delete()
      db_session.query(BillingSubscription).delete()
      db_session.query(BillingCustomer).delete()
    except ImportError:
      pass

    # Delete org-related tables before deleting users
    try:
      from robosystems.models.core import Org, OrgLimits, OrgUser

      db_session.query(OrgUser).delete()
      db_session.query(OrgLimits).delete()
      db_session.query(Org).delete()
    except ImportError:
      pass

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
    assert hasattr(user, "graph_users")  # Changed from user_graphs
    # Note: limits no longer exists - now handled at org level
    assert hasattr(user, "user_repositories")

  @patch("robosystems.models.core.user.user.Session")
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

  @patch("robosystems.models.core.user.user.Session")
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

  @patch("robosystems.models.core.user.user.Session")
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

  @patch("robosystems.models.core.user.user.Session")
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

  @patch("robosystems.models.core.user.user.Session")
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

  @patch("robosystems.models.core.user.user.Session")
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


class TestInvalidateAuthCache:
  """Regression tests for ``User._invalidate_auth_cache``.

  These cover the retry/critical-log behavior when the underlying Redis
  invalidation methods report failure via their bool return contract.
  Without these, a regression to "fire-and-forget" invalidation (e.g. if
  the cache methods went back to swallowing exceptions and returning
  None) would silently re-introduce the post-password-reset stale-token
  window.
  """

  def _make_user(self) -> User:
    return User(
      id="user_invalidate",
      email="invalidate@example.com",
      name="Invalidate Test",
      password_hash="hashed",
      is_active=True,
      session_version=1,
    )

  @patch("robosystems.models.core.user.user.logger")
  def test_first_attempt_succeeds_no_critical_log(self, mock_logger):
    """If both cache calls return True on the first attempt, no retry,
    no error log."""
    cache_module = MagicMock()
    cache_module.api_key_cache.invalidate_jwt_user_data.return_value = True
    cache_module.api_key_cache.invalidate_user_jwt_graph_access.return_value = True

    with patch("importlib.import_module", return_value=cache_module):
      self._make_user()._invalidate_auth_cache()

    assert cache_module.api_key_cache.invalidate_jwt_user_data.call_count == 1, (
      "should not retry when first attempt succeeds"
    )
    mock_logger.error.assert_not_called()

  @patch("robosystems.models.core.user.user.time.sleep")
  @patch("robosystems.models.core.user.user.logger")
  def test_first_attempt_fails_second_succeeds_no_critical_log(
    self, mock_logger, mock_sleep
  ):
    """Transient Redis failure on attempt 1, success on retry → no
    critical log. We assert the retry sleep happened so the contract
    (one backoff between attempts) doesn't silently disappear."""
    cache_module = MagicMock()
    cache_module.api_key_cache.invalidate_jwt_user_data.side_effect = [False, True]
    cache_module.api_key_cache.invalidate_user_jwt_graph_access.return_value = True

    with patch("importlib.import_module", return_value=cache_module):
      self._make_user()._invalidate_auth_cache()

    assert cache_module.api_key_cache.invalidate_jwt_user_data.call_count == 2
    mock_sleep.assert_called_once()
    # No CRITICAL: ... line should fire.
    for call in mock_logger.error.call_args_list:
      assert "CRITICAL" not in str(call), (
        f"unexpected critical log on successful retry: {call}"
      )

  @patch("robosystems.models.core.user.user.time.sleep")
  @patch("robosystems.models.core.user.user.logger")
  def test_both_attempts_fail_emits_critical_log(self, mock_logger, mock_sleep):
    """Hard Redis failure on both attempts → CRITICAL: error log so
    monitoring can alert. Verifies the fail-open window is at least
    surfaced rather than silenced."""
    cache_module = MagicMock()
    cache_module.api_key_cache.invalidate_jwt_user_data.return_value = False
    cache_module.api_key_cache.invalidate_user_jwt_graph_access.return_value = False

    with patch("importlib.import_module", return_value=cache_module):
      self._make_user()._invalidate_auth_cache()

    assert cache_module.api_key_cache.invalidate_jwt_user_data.call_count == 2
    mock_sleep.assert_called_once()

    critical_logs = [
      call for call in mock_logger.error.call_args_list if "CRITICAL" in str(call)
    ]
    assert len(critical_logs) == 1, (
      f"expected exactly one CRITICAL log on dual failure, got: "
      f"{mock_logger.error.call_args_list}"
    )
    # The user_id must appear in the log so the alerting context is useful.
    assert "user_invalidate" in str(critical_logs[0])

  @patch("robosystems.models.core.user.user.time.sleep")
  @patch("robosystems.models.core.user.user.logger")
  def test_graph_cache_failure_alone_triggers_retry(self, mock_logger, mock_sleep):
    """Both caches must succeed for the attempt to count as success.
    If only the graph cache invalidation fails, we still retry."""
    cache_module = MagicMock()
    cache_module.api_key_cache.invalidate_jwt_user_data.return_value = True
    cache_module.api_key_cache.invalidate_user_jwt_graph_access.side_effect = [
      False,
      True,
    ]

    with patch("importlib.import_module", return_value=cache_module):
      self._make_user()._invalidate_auth_cache()

    assert (
      cache_module.api_key_cache.invalidate_user_jwt_graph_access.call_count == 2
    ), "graph-cache failure must trigger a retry"
    mock_sleep.assert_called_once()
