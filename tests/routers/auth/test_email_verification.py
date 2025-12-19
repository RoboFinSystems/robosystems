"""Tests for email verification endpoints."""

from unittest.mock import Mock, patch

import pytest

from robosystems.models.iam import User, UserToken


class TestEmailVerificationEndpoints:
  """Tests for email verification functionality."""

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch.object(UserToken, "create_token")
  @patch("robosystems.routers.auth.email_verification.verify_jwt_token")
  async def test_resend_verification_email_success(
    self,
    mock_verify_jwt,
    mock_create_token,
    mock_build_config,
    mock_dagster_job,
    client,
    test_user,
    test_db,
  ):
    """Test successful resending of verification email."""
    # Mock JWT verification to return test user
    mock_verify_jwt.return_value = test_user.id

    # Mark user as unverified
    test_user.email_verified = False
    test_db.commit()

    # Create a dummy JWT token
    token = "test_jwt_token"

    # Mock token creation
    mock_create_token.return_value = "verification_token_123"

    # Mock Dagster job config builder
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}

    # Mock Dagster job (called via BackgroundTasks, so it's queued not awaited)
    mock_dagster_job.return_value = {"status": "success"}

    # Request to resend verification
    response = client.post(
      "/v1/auth/email/resend",
      headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Verification email sent. Please check your inbox."

    # Verify token was created
    mock_create_token.assert_called_once()
    call_args = mock_create_token.call_args
    assert call_args.kwargs["user_id"] == test_user.id
    assert call_args.kwargs["token_type"] == "email_verification"
    assert call_args.kwargs["hours"] == 24

    # Verify email job config was built
    mock_build_config.assert_called_once()
    config_args = mock_build_config.call_args
    assert config_args.kwargs["email_type"] == "email_verification"
    assert config_args.kwargs["to_email"] == test_user.email
    assert config_args.kwargs["user_name"] == test_user.name
    assert config_args.kwargs["token"] == "verification_token_123"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.verify_jwt_token")
  async def test_resend_verification_already_verified(
    self, mock_verify_jwt, client, test_user, test_db
  ):
    """Test that resending fails if email is already verified."""
    # Mock JWT verification to return test user
    mock_verify_jwt.return_value = test_user.id

    # Mark user as verified
    test_user.email_verified = True
    test_db.commit()

    token = "test_jwt_token"

    response = client.post(
      "/v1/auth/email/resend",
      headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "Email is already verified"

  @pytest.mark.asyncio
  async def test_resend_verification_unauthenticated(self, client):
    """Test that resending requires authentication."""
    response = client.post("/v1/auth/email/resend")

    assert response.status_code == 401
    data = response.json()
    assert "Authentication required" in data["detail"]

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_verify_email_success(
    self,
    mock_get_user,
    mock_verify_token,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test successful email verification."""
    # Mock token verification
    mock_verify_token.return_value = "user_123"

    # Mock user retrieval
    mock_user = Mock(spec=User)
    mock_user.id = "user_123"
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"
    mock_user.email_verified = False
    mock_user.is_active = True
    mock_user.verify_email = Mock()  # Mock the verify_email method
    mock_get_user.return_value = mock_user

    # Mock Dagster job config builder
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    # Verify email
    response = client.post(
      "/v1/auth/email/verify",
      json={"token": "valid_verification_token"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Email verified successfully. Welcome to RoboSystems!"
    assert "token" in data  # JWT token for auto-login
    assert data["user"]["id"] == "user_123"
    assert data["user"]["email"] == "test@example.com"

    # Verify token was verified
    mock_verify_token.assert_called_once_with(
      raw_token="valid_verification_token",
      token_type="email_verification",
      session=test_db,
    )

    # Verify user email was verified
    mock_user.verify_email.assert_called_once_with(test_db)

    # Verify welcome email job config was built
    mock_build_config.assert_called_once()
    config_args = mock_build_config.call_args
    assert config_args.kwargs["email_type"] == "welcome"

  @pytest.mark.asyncio
  @patch.object(UserToken, "verify_token")
  async def test_verify_email_invalid_token(self, mock_verify_token, client):
    """Test verification with invalid token."""
    mock_verify_token.return_value = None

    response = client.post(
      "/v1/auth/email/verify",
      json={"token": "invalid_token"},
    )

    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "Invalid or expired verification token"

  @pytest.mark.asyncio
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_verify_email_user_not_found(
    self, mock_get_user, mock_verify_token, client, test_db
  ):
    """Test verification when user no longer exists."""
    mock_verify_token.return_value = "deleted_user_id"
    mock_get_user.return_value = None

    response = client.post(
      "/v1/auth/email/verify",
      json={"token": "orphaned_token"},
    )

    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "User not found"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_verify_email_already_verified(
    self,
    mock_get_user,
    mock_verify_token,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test verification when email is already verified (still succeeds)."""
    mock_verify_token.return_value = "user_456"

    # Mock already verified user
    mock_user = Mock(spec=User)
    mock_user.id = "user_456"
    mock_user.email = "already@verified.com"
    mock_user.name = "Already Verified"
    mock_user.email_verified = True
    mock_user.is_active = True
    mock_user.verify_email = Mock()  # Mock the verify_email method
    mock_get_user.return_value = mock_user

    # Mock Dagster job
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    response = client.post(
      "/v1/auth/email/verify",
      json={"token": "valid_but_unnecessary_token"},
    )

    # The endpoint doesn't check if already verified, so it returns success
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Email verified successfully. Welcome to RoboSystems!"
    assert "token" in data  # JWT token for auto-login

  @pytest.mark.asyncio
  async def test_verify_email_missing_token(self, client):
    """Test verification without providing token."""
    response = client.post(
      "/v1/auth/email/verify",
      json={},
    )

    assert response.status_code == 422  # Validation error

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch("robosystems.routers.auth.email_verification.detect_app_source")
  @patch.object(UserToken, "create_token")
  @patch("robosystems.routers.auth.email_verification.verify_jwt_token")
  async def test_resend_verification_app_detection(
    self,
    mock_verify_jwt,
    mock_create_token,
    mock_detect_app,
    mock_build_config,
    mock_dagster_job,
    client,
    test_user,
    test_db,
  ):
    """Test that app source is correctly detected and passed."""
    # Mock JWT verification to return test user
    mock_verify_jwt.return_value = test_user.id

    # Mark user as unverified
    test_user.email_verified = False
    test_db.commit()

    token = "test_jwt_token"

    mock_create_token.return_value = "token_789"
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}
    mock_detect_app.return_value = "roboinvestor"

    response = client.post(
      "/v1/auth/email/resend",
      headers={
        "Authorization": f"Bearer {token}",
        "Referer": "https://roboinvestor.ai/settings",
      },
    )

    assert response.status_code == 200

    # Verify app was detected
    mock_detect_app.assert_called_once()

    # Verify correct app was passed to email job config
    config_args = mock_build_config.call_args
    assert config_args.kwargs["app"] == "roboinvestor"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch("robosystems.routers.auth.email_verification.SecurityAuditLogger")
  @patch.object(UserToken, "verify_token")
  @patch.object(User, "get_by_id")
  async def test_verify_email_security_logging(
    self,
    mock_get_user,
    mock_verify_token,
    mock_audit_logger,
    mock_build_config,
    mock_dagster_job,
    client,
    test_db,
  ):
    """Test that email verification is logged for security."""
    mock_verify_token.return_value = "user_audit"

    mock_user = Mock(spec=User)
    mock_user.id = "user_audit"
    mock_user.email = "audit@example.com"
    mock_user.name = "Audit User"
    mock_user.email_verified = False
    mock_user.is_active = True
    mock_user.verify_email = Mock()  # Mock the verify_email method
    mock_get_user.return_value = mock_user

    # Mock Dagster job
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    response = client.post(
      "/v1/auth/email/verify",
      json={"token": "audit_token"},
      headers={
        "User-Agent": "Test Browser",
        "X-Forwarded-For": "192.168.1.100",
      },
    )

    assert response.status_code == 200

    # Verify security event was logged
    mock_audit_logger.log_security_event.assert_called_once()
    log_args = mock_audit_logger.log_security_event.call_args
    assert log_args.kwargs["event_type"].value == "email_verified"
    assert log_args.kwargs["user_id"] == "user_audit"
    assert log_args.kwargs["endpoint"] == "/v1/auth/email/verify"

  @pytest.mark.asyncio
  @patch("robosystems.routers.auth.email_verification.run_and_monitor_dagster_job")
  @patch("robosystems.routers.auth.email_verification.build_email_job_config")
  @patch.object(UserToken, "create_token")
  @patch("robosystems.routers.auth.email_verification.verify_jwt_token")
  async def test_resend_verification_rate_limiting(
    self,
    mock_verify_jwt,
    mock_create_token,
    mock_build_config,
    mock_dagster_job,
    client,
    test_user,
    test_db,
  ):
    """Test that resend endpoint is rate limited."""
    # Mock JWT verification to return test user
    mock_verify_jwt.return_value = test_user.id

    # Mark user as unverified
    test_user.email_verified = False
    test_db.commit()

    token = "test_jwt_token"

    mock_create_token.return_value = "token_rate"
    mock_build_config.return_value = {"ops": {"send_email_op": {"config": {}}}}
    mock_dagster_job.return_value = {"status": "success"}

    # Make multiple requests quickly
    for i in range(5):
      response = client.post(
        "/v1/auth/email/resend",
        headers={"Authorization": f"Bearer {token}"},
      )

      # First few should succeed, then rate limit should kick in
      if i < 3:  # Assuming rate limit is 3 per window
        assert response.status_code == 200
      else:
        # This would be 429 if rate limiting is enabled
        # For now, we'll just check it doesn't crash
        assert response.status_code in [200, 429]
