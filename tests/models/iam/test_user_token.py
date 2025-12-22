"""Tests for UserToken model."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import UserToken


class TestUserTokenModel:
  """Tests for UserToken model functionality."""

  def test_create_email_verification_token(self, db_session):
    """Test creating an email verification token."""
    user_id = "user_test123"

    token = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
      ip_address="127.0.0.1",
      user_agent="Test Agent",
    )

    assert token is not None
    assert len(token) > 30  # Token should be sufficiently long

    # Check token is in database
    db_token = db_session.query(UserToken).filter_by(user_id=user_id).first()
    assert db_token is not None
    assert db_token.token_type == "email_verification"
    assert db_token.ip_address == "127.0.0.1"
    assert db_token.user_agent == "Test Agent"
    assert db_token.used_at is None
    # Compare without timezone info since SQLAlchemy might return naive datetimes
    # Database stores UTC times but might return naive datetimes
    if db_token.expires_at.tzinfo is None:
      assert db_token.expires_at > datetime.utcnow()
    else:
      assert db_token.expires_at > datetime.now(UTC)

  def test_create_password_reset_token(self, db_session):
    """Test creating a password reset token."""
    user_id = "user_test456"

    token = UserToken.create_token(
      user_id=user_id,
      token_type="password_reset",
      hours=1,
      session=db_session,
    )

    assert token is not None

    # Check token expiry is correct
    db_token = db_session.query(UserToken).filter_by(user_id=user_id).first()
    assert db_token is not None
    assert db_token.token_type == "password_reset"

    # Handle both naive and aware datetimes
    # Database stores UTC times but might return naive datetimes
    if db_token.expires_at.tzinfo is None:
      expected_expiry = datetime.utcnow() + timedelta(hours=1)
    else:
      expected_expiry = datetime.now(UTC) + timedelta(hours=1)
    # Allow 1 minute tolerance for test execution time
    assert abs((db_token.expires_at - expected_expiry).total_seconds()) < 60

  def test_invalid_token_type(self, db_session):
    """Test that invalid token types are rejected."""
    with pytest.raises(ValueError, match="Invalid token type"):
      UserToken.create_token(
        user_id="user123",
        token_type="invalid_type",
        hours=1,
        session=db_session,
      )

  def test_verify_valid_token(self, db_session):
    """Test verifying a valid token."""
    user_id = "user_verify123"

    # Create token
    raw_token = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
    )

    # Verify token
    verified_user_id = UserToken.verify_token(
      raw_token=raw_token,
      token_type="email_verification",
      session=db_session,
    )

    assert verified_user_id == user_id

    # Check token is marked as used
    db_token = db_session.query(UserToken).filter_by(user_id=user_id).first()
    assert db_token.used_at is not None

  def test_verify_invalid_token(self, db_session):
    """Test verifying an invalid token."""
    result = UserToken.verify_token(
      raw_token="invalid_token_xyz",
      token_type="email_verification",
      session=db_session,
    )

    assert result is None

  def test_verify_wrong_token_type(self, db_session):
    """Test verifying a token with wrong type."""
    user_id = "user_wrong_type"

    # Create email verification token
    raw_token = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
    )

    # Try to verify as password reset token
    result = UserToken.verify_token(
      raw_token=raw_token,
      token_type="password_reset",
      session=db_session,
    )

    assert result is None

  def test_verify_expired_token(self, db_session):
    """Test verifying an expired token."""
    user_id = "user_expired"

    # Create token
    raw_token = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
    )

    # Manually set token as expired
    db_token = db_session.query(UserToken).filter_by(user_id=user_id).first()
    # Use naive datetime if database uses naive datetimes
    if db_token.expires_at.tzinfo is None:
      db_token.expires_at = datetime.utcnow() - timedelta(hours=1)
    else:
      db_token.expires_at = datetime.now(UTC) - timedelta(hours=1)
    db_session.commit()

    # Try to verify expired token
    result = UserToken.verify_token(
      raw_token=raw_token,
      token_type="email_verification",
      session=db_session,
    )

    assert result is None

  def test_verify_used_token(self, db_session):
    """Test that a used token cannot be verified again."""
    user_id = "user_reuse"

    # Create and verify token
    raw_token = UserToken.create_token(
      user_id=user_id,
      token_type="password_reset",
      hours=1,
      session=db_session,
    )

    # First verification should succeed
    result1 = UserToken.verify_token(
      raw_token=raw_token,
      token_type="password_reset",
      session=db_session,
    )
    assert result1 == user_id

    # Second verification should fail
    result2 = UserToken.verify_token(
      raw_token=raw_token,
      token_type="password_reset",
      session=db_session,
    )
    assert result2 is None

  def test_validate_token(self, db_session):
    """Test validating a token without consuming it."""
    user_id = "user_validate"

    # Create token
    raw_token = UserToken.create_token(
      user_id=user_id,
      token_type="password_reset",
      hours=1,
      session=db_session,
    )

    # Validate token (doesn't consume it)
    result1 = UserToken.validate_token(
      raw_token=raw_token,
      token_type="password_reset",
      session=db_session,
    )
    assert result1 == user_id

    # Token should still be unused
    db_token = db_session.query(UserToken).filter_by(user_id=user_id).first()
    assert db_token.used_at is None

    # Can validate again
    result2 = UserToken.validate_token(
      raw_token=raw_token,
      token_type="password_reset",
      session=db_session,
    )
    assert result2 == user_id

  def test_invalidate_user_tokens(self, db_session):
    """Test invalidating all unused tokens for a user."""
    user_id = "user_invalidate"

    # Create a token
    token1 = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
    )

    # Token should be valid initially
    assert UserToken.validate_token(token1, "email_verification", db_session) == user_id

    # Create another token (this auto-invalidates the first one by design)
    token2 = UserToken.create_token(
      user_id=user_id,
      token_type="email_verification",
      hours=24,
      session=db_session,
    )

    # Only the second token should be valid now
    assert UserToken.validate_token(token1, "email_verification", db_session) is None
    assert UserToken.validate_token(token2, "email_verification", db_session) == user_id

    # Invalidate all email verification tokens
    count = UserToken.invalidate_user_tokens(
      user_id=user_id,
      token_type="email_verification",
      session=db_session,
    )

    # Should invalidate the one remaining valid token
    assert count == 1

    # Both tokens should now be invalid
    assert UserToken.validate_token(token1, "email_verification", db_session) is None
    assert UserToken.validate_token(token2, "email_verification", db_session) is None

  def test_auto_invalidate_on_new_token(self, db_session):
    """Test that creating a new token invalidates old ones."""
    user_id = "user_auto_invalidate"

    # Create first token
    token1 = UserToken.create_token(
      user_id=user_id,
      token_type="password_reset",
      hours=1,
      session=db_session,
    )

    # First token should be valid
    assert UserToken.validate_token(token1, "password_reset", db_session) == user_id

    # Create second token (should invalidate first)
    token2 = UserToken.create_token(
      user_id=user_id,
      token_type="password_reset",
      hours=1,
      session=db_session,
    )

    # First token should be invalid, second should be valid
    assert UserToken.validate_token(token1, "password_reset", db_session) is None
    assert UserToken.validate_token(token2, "password_reset", db_session) == user_id

  def test_cleanup_expired_tokens(self, db_session):
    """Test cleanup of expired and old used tokens."""
    import uuid

    # Use unique hashes to avoid conflicts with other tests
    hash_suffix = str(uuid.uuid4())[:8]

    # Create expired unused token (use naive datetime)
    expired_token = UserToken(
      user_id=f"user_expired_cleanup_{hash_suffix}",
      token_hash=f"hash_expired_{hash_suffix}",
      token_type="email_verification",
      expires_at=datetime.utcnow() - timedelta(days=1),
    )
    db_session.add(expired_token)

    # Create old used token
    old_used_token = UserToken(
      user_id=f"user_old_used_{hash_suffix}",
      token_hash=f"hash_old_used_{hash_suffix}",
      token_type="password_reset",
      expires_at=datetime.utcnow() + timedelta(days=1),
      used_at=datetime.utcnow() - timedelta(days=35),
    )
    db_session.add(old_used_token)

    # Create valid token
    valid_token = UserToken(
      user_id=f"user_valid_cleanup_{hash_suffix}",
      token_hash=f"hash_valid_{hash_suffix}",
      token_type="email_verification",
      expires_at=datetime.utcnow() + timedelta(days=1),
    )
    db_session.add(valid_token)

    db_session.commit()

    # Count tokens before cleanup
    initial_count = db_session.query(UserToken).count()

    # Run cleanup
    count = UserToken.cleanup_expired_tokens(db_session)

    # Count tokens after cleanup
    final_count = db_session.query(UserToken).count()

    # Should have cleaned up 2 tokens
    assert count >= 2  # At least our 2 tokens
    assert final_count == initial_count - count

    # Check that our valid token remains
    valid_remains = (
      db_session.query(UserToken)
      .filter_by(token_hash=f"hash_valid_{hash_suffix}")
      .first()
    )
    assert valid_remains is not None

  def test_token_representation(self, db_session):
    """Test string representation of token."""
    token = UserToken(
      id="tok_test123",
      user_id="user_123",
      token_hash="hash123",
      token_type="email_verification",
      expires_at=datetime.utcnow() + timedelta(hours=1),
    )

    repr_str = repr(token)
    assert "tok_test123" in repr_str
    assert "email_verification" in repr_str
    assert "user_123" in repr_str

  @patch("robosystems.models.iam.user_token.logger")
  def test_error_handling_on_commit_failure(self, mock_logger, db_session):
    """Test error handling when database commit fails."""
    # Mock session to raise error on commit
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")
    mock_session.rollback = MagicMock()

    with pytest.raises(SQLAlchemyError):
      UserToken.create_token(
        user_id="user_error",
        token_type="email_verification",
        hours=24,
        session=mock_session,
      )

    # Check that rollback was called
    mock_session.rollback.assert_called_once()

    # Check that error was logged
    mock_logger.error.assert_called()
