"""Tests for password reset endpoints."""

from unittest.mock import Mock, patch

import pytest

from robosystems.models.iam import User, UserToken


class TestPasswordResetEndpoints:
  """Tests for password reset functionality."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.password_reset.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.password_reset.build_email_job_config")
  @patch.object(UserToken, "create_token")
  @patch.object(User, "get_by_email")
  async def test_forgot_password_success(
    self,
    mock_get_user,
    mock_create_token,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test successful password reset request."""
    # Mock user retrieval
    mock_user = Mock(spec=User)
    mock_user.id = "user_forgot_123"
    mock_user.email = "forgot@example.com"
    mock_user.name = "Forgot User"
    mock_user.is_active = True
    mock_get_user.return_value = mock_user

    # Mock token creation
    mock_create_token.return_value = "reset_token_123"

    # Mock Dagster job
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    # Request password reset
    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "forgot@example.com"},
    )

    assert response.status_code == 200
    data = response.json()
    # Message should be generic to prevent email enumeration
    assert (
      data["message"]
      == "If an account exists with this email, a password reset link has been sent."
    )

    # Verify user was looked up
    mock_get_user.assert_called_once_with("forgot@example.com", test_db)

    # Verify token was created
    mock_create_token.assert_called_once()
    token_args = mock_create_token.call_args
    assert token_args.kwargs["user_id"] == "user_forgot_123"
    assert token_args.kwargs["token_type"] == "password_reset"
    assert token_args.kwargs["hours"] == 1  # Default expiry

    # Verify email job config was built
    mock_build_config.assert_called_once()
    config_args = mock_build_config.call_args
    assert config_args.kwargs["email_type"] == "password_reset"
    assert config_args.kwargs["to_email"] == "forgot@example.com"
    assert config_args.kwargs["user_name"] == "Forgot User"
    assert config_args.kwargs["token"] == "reset_token_123"

  @pytest.mark.asyncio
  @patch.object(User, "get_by_email")
  async def test_forgot_password_nonexistent_user(self, mock_get_user, client, test_db):
    """Test password reset for non-existent email (should still return success)."""
    mock_get_user.return_value = None

    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "nonexistent@example.com"},
    )

    assert response.status_code == 200
    data = response.json()
    # Same message for security (prevent email enumeration)
    assert (
      data["message"]
      == "If an account exists with this email, a password reset link has been sent."
    )

  @pytest.mark.asyncio
  @patch.object(User, "get_by_email")
  async def test_forgot_password_inactive_user(self, mock_get_user, client, test_db):
    """Test password reset for inactive user (should not send email)."""
    mock_user = Mock(spec=User)
    mock_user.is_active = False
    mock_get_user.return_value = mock_user

    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "inactive@example.com"},
    )

    assert response.status_code == 200
    data = response.json()
    # Same message for security
    assert (
      data["message"]
      == "If an account exists with this email, a password reset link has been sent."
    )

  @pytest.mark.asyncio
  async def test_forgot_password_invalid_email(self, client):
    """Test password reset with invalid email format."""
    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "not-an-email"},
    )

    assert response.status_code == 422  # Validation error

  @pytest.mark.asyncio
  @patch.object(UserToken, "validate_token")
  @patch.object(User, "get_by_id")
  async def test_validate_reset_token_success(
    self, mock_get_user, mock_validate_token, client, test_db
  ):
    """Test successful reset token validation."""
    mock_validate_token.return_value = "user_validate_123"

    mock_user = Mock(spec=User)
    mock_user.email = "validate@example.com"
    mock_get_user.return_value = mock_user

    response = client.get(
      "/v1/auth/password/reset/validate",
      params={"token": "valid_reset_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["valid"] is True
    assert data["email"] == "va***@example.com"  # Masked email

  @pytest.mark.asyncio
  @patch.object(UserToken, "validate_token")
  async def test_validate_reset_token_invalid(
    self, mock_validate_token, client, test_db
  ):
    """Test validation of invalid reset token."""
    mock_validate_token.return_value = None

    response = client.get(
      "/v1/auth/password/reset/validate",
      params={"token": "invalid_reset_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["valid"] is False
    assert data["email"] is None

  @pytest.mark.asyncio
  @patch.object(UserToken, "validate_token")
  @patch.object(User, "get_by_id")
  async def test_validate_reset_token_email_masking(
    self, mock_get_user, mock_validate_token, client, test_db
  ):
    """Test that email is properly masked in validation response."""
    mock_validate_token.return_value = "user_mask"

    # Test various email formats
    test_cases = [
      ("longusername@example.com", "lo***@example.com"),
      ("ab@example.com", "***@example.com"),  # Only 2 chars, so masked completely
      ("a@example.com", "***@example.com"),
      ("test.user@sub.example.com", "te***@sub.example.com"),
    ]

    for email, expected_mask in test_cases:
      mock_user = Mock(spec=User)
      mock_user.email = email
      mock_get_user.return_value = mock_user

      response = client.get(
        "/v1/auth/password/reset/validate",
        params={"token": "token_for_masking"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["email"] == expected_mask

  @pytest.mark.asyncio
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  @patch("robosystems.routers.auth.password_reset.hash_password")
  async def test_reset_password_success(
    self, mock_hash_password, mock_get_user, mock_verify_token, client, test_db
  ):
    """Test successful password reset."""
    mock_verify_token.return_value = "user_reset_123"

    mock_user = Mock(spec=User)
    mock_user.id = "user_reset_123"
    mock_user.email = "reset@example.com"
    mock_user.name = "Reset User"
    mock_user.is_active = True
    mock_user.update = Mock()  # Mock the update method on the instance
    mock_get_user.return_value = mock_user

    mock_hash_password.return_value = "hashed_new_password"

    response = client.post(
      "/v1/auth/password/reset",
      json={
        "token": "valid_reset_token",
        "new_password": "NewS3cur3P@ssw0rd!",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Password reset successfully. You are now logged in."
    assert "token" in data  # JWT for auto-login
    assert data["user"]["id"] == "user_reset_123"
    assert data["user"]["email"] == "reset@example.com"

    # Verify token was verified
    mock_verify_token.assert_called_once_with(
      raw_token="valid_reset_token",
      token_type="password_reset",
      session=test_db,
    )

    # Verify password was hashed
    mock_hash_password.assert_called_once_with("NewS3cur3P@ssw0rd!")

    # Verify user was updated with new password
    mock_user.update.assert_called_once_with(
      test_db, password_hash="hashed_new_password"
    )

  @pytest.mark.asyncio
  @patch.object(UserToken, "verify_token")
  async def test_reset_password_invalid_token(self, mock_verify_token, client, test_db):
    """Test password reset with invalid token."""
    mock_verify_token.return_value = None

    response = client.post(
      "/v1/auth/password/reset",
      json={
        "token": "invalid_token",
        "new_password": "NewP@ssw0rd123",
      },
    )

    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "Invalid or expired reset token"

  @pytest.mark.asyncio
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_reset_password_user_not_found(
    self, mock_get_user, mock_verify_token, client, test_db
  ):
    """Test password reset when user no longer exists."""
    mock_verify_token.return_value = "deleted_user"
    mock_get_user.return_value = None

    response = client.post(
      "/v1/auth/password/reset",
      json={
        "token": "orphaned_token",
        "new_password": "NewP@ssw0rd123",
      },
    )

    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "User not found"

  @pytest.mark.asyncio
  async def test_reset_password_weak_password(self, client):
    """Test that weak passwords are rejected during reset."""
    response = client.post(
      "/v1/auth/password/reset",
      json={
        "token": "some_token",
        "new_password": "weak",  # Too short and simple
      },
    )

    assert response.status_code == 422  # Validation error

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.password_reset.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.password_reset.build_email_job_config")
  @patch("robosystems.routers.auth.password_reset.detect_app_source")
  @patch.object(UserToken, "create_token")
  @patch.object(User, "get_by_email")
  async def test_forgot_password_app_detection(
    self,
    mock_get_user,
    mock_create_token,
    mock_detect_app,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test that app source is detected and passed to email service."""
    mock_user = Mock(spec=User)
    mock_user.id = "user_app"
    mock_user.email = "app@example.com"
    mock_user.name = "App User"
    mock_user.is_active = True
    mock_get_user.return_value = mock_user

    mock_create_token.return_value = "token_app"
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}
    mock_detect_app.return_value = "robosystems"

    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "app@example.com"},
      headers={"X-App-Source": "robosystems"},
    )

    assert response.status_code == 200

    # Verify app detection
    mock_detect_app.assert_called_once()

    # Verify correct app was passed to email job config
    config_args = mock_build_config.call_args
    assert config_args.kwargs["app"] == "robosystems"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.password_reset.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.password_reset.build_email_job_config")
  @patch("robosystems.routers.auth.password_reset.SecurityAuditLogger")
  @patch.object(UserToken, "create_token")
  @patch.object(User, "get_by_email")
  async def test_forgot_password_security_logging(
    self,
    mock_get_user,
    mock_create_token,
    mock_audit_logger,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test that password reset requests are logged for security."""
    # Test with existing user
    mock_user = Mock(spec=User)
    mock_user.id = "user_audit"
    mock_user.email = "audit@example.com"
    mock_user.name = "Audit User"
    mock_user.is_active = True
    mock_get_user.return_value = mock_user

    mock_create_token.return_value = "token_audit"
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    response = client.post(
      "/v1/auth/password/forgot",
      json={"email": "audit@example.com"},
      headers={"User-Agent": "Security Test"},
    )

    assert response.status_code == 200

    # Verify security event was logged
    mock_audit_logger.log_security_event.assert_called()
    log_args = mock_audit_logger.log_security_event.call_args
    assert log_args.kwargs["event_type"].value == "password_reset_requested"
    assert log_args.kwargs["risk_level"] == "medium"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.password_reset.SecurityAuditLogger")
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_reset_password_security_logging(
    self, mock_get_user, mock_verify_token, mock_audit_logger, client, test_db
  ):
    """Test that successful password resets are logged."""
    mock_verify_token.return_value = "user_complete"

    mock_user = Mock(spec=User)
    mock_user.id = "user_complete"
    mock_user.email = "complete@example.com"
    mock_user.name = "Complete User"
    mock_user.is_active = True
    mock_user.update = Mock()  # Mock the update method on the instance
    mock_get_user.return_value = mock_user

    response = client.post(
      "/v1/auth/password/reset",
      json={
        "token": "complete_token",
        "new_password": "CompletedP@ssw0rd!",
      },
    )

    assert response.status_code == 200

    # Verify high-risk security event was logged
    mock_audit_logger.log_security_event.assert_called()
    log_args = mock_audit_logger.log_security_event.call_args
    assert log_args.kwargs["event_type"].value == "password_reset_completed"
    assert log_args.kwargs["user_id"] == "user_complete"
    assert log_args.kwargs["risk_level"] == "high"
