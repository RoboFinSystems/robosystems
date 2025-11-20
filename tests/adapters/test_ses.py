"""Tests for SES email service adapter."""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from robosystems.adapters.ses import SESEmailService


@pytest.fixture
def mock_ses_client():
  """Create a mock SES client."""
  return Mock()


@pytest.fixture
def ses_service(mock_ses_client):
  """Create SES email service with mocked client."""
  with patch("robosystems.adapters.ses.boto3") as mock_boto3:
    mock_boto3.client.return_value = mock_ses_client

    with patch("robosystems.adapters.ses.env") as mock_env:
      mock_env.AWS_REGION = "us-east-1"
      mock_env.EMAIL_FROM_ADDRESS = "noreply@example.com"
      mock_env.EMAIL_FROM_NAME = "Test"
      mock_env.ENVIRONMENT = "test"
      mock_env.ROBOLEDGER_URL = "https://roboledger.ai"
      mock_env.ROBOINVESTOR_URL = "https://roboinvestor.ai"
      mock_env.ROBOSYSTEMS_URL = "https://robosystems.ai"
      mock_env.EMAIL_TOKEN_EXPIRY_HOURS = 48
      mock_env.PASSWORD_RESET_TOKEN_EXPIRY_HOURS = 2

      service = SESEmailService()
      service.ses_client = mock_ses_client
      service.from_address = "noreply@example.com"
      return service


class TestSESEmailService:
  """Tests for SES email service functionality."""

  @pytest.mark.asyncio
  async def test_send_verification_email_success(self, ses_service, mock_ses_client):
    """Test sending verification email successfully."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-123"}

    result = await ses_service.send_verification_email(
      user_email="test@example.com",
      user_name="Test User",
      token="verification_token_123",
      app="roboledger",
    )

    assert result is True

    # Check SES send_email was called correctly
    mock_ses_client.send_email.assert_called_once()
    call_args = mock_ses_client.send_email.call_args

    # Verify source
    assert "noreply@example.com" in call_args.kwargs["Source"]

    # Verify destination
    assert call_args.kwargs["Destination"]["ToAddresses"] == ["test@example.com"]

    # Verify message content
    message = call_args.kwargs["Message"]
    assert "Verify Your Roboledger Email" in message["Subject"]["Data"]
    assert "Test User" in message["Body"]["Html"]["Data"]
    assert "verification_token_123" in message["Body"]["Html"]["Data"]

    # Verify tags
    tags = call_args.kwargs["Tags"]
    assert any(
      tag["Name"] == "EmailType" and tag["Value"] == "email_verification"
      for tag in tags
    )

  @pytest.mark.asyncio
  async def test_send_password_reset_email_success(self, ses_service, mock_ses_client):
    """Test sending password reset email successfully."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-456"}

    result = await ses_service.send_password_reset_email(
      user_email="reset@example.com",
      user_name="Reset User",
      token="reset_token_456",
      app="roboinvestor",
    )

    assert result is True

    # Check SES send_email was called
    mock_ses_client.send_email.assert_called_once()
    call_args = mock_ses_client.send_email.call_args

    # Verify message content
    message = call_args.kwargs["Message"]
    assert "Roboinvestor Password Reset" in message["Subject"]["Data"]
    assert "Reset User" in message["Body"]["Html"]["Data"]
    assert "reset_token_456" in message["Body"]["Html"]["Data"]

  @pytest.mark.asyncio
  async def test_send_welcome_email_success(self, ses_service, mock_ses_client):
    """Test sending welcome email successfully."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-789"}

    result = await ses_service.send_welcome_email(
      user_email="welcome@example.com",
      user_name="Welcome User",
      app="robosystems",
    )

    assert result is True

    # Check message content
    call_args = mock_ses_client.send_email.call_args
    message = call_args.kwargs["Message"]
    assert "Welcome to Robosystems!" in message["Subject"]["Data"]
    assert "Welcome User" in message["Body"]["Html"]["Data"]
    assert "dashboard" in message["Body"]["Html"]["Data"]

  @pytest.mark.asyncio
  async def test_send_email_with_client_error(self, ses_service, mock_ses_client):
    """Test handling of AWS client errors."""
    mock_ses_client.send_email.side_effect = ClientError(
      {"Error": {"Code": "MessageRejected", "Message": "Email address not verified"}},
      "SendEmail",
    )

    result = await ses_service.send_email(
      email_type="email_verification",
      to_email="error@example.com",
      template_data={"user_name": "Error User"},
    )

    assert result is False

  @pytest.mark.asyncio
  async def test_send_email_with_generic_exception(self, ses_service, mock_ses_client):
    """Test handling of generic exceptions."""
    mock_ses_client.send_email.side_effect = Exception("Unexpected error")

    result = await ses_service.send_email(
      email_type="password_reset",
      to_email="exception@example.com",
      template_data={"user_name": "Exception User"},
    )

    assert result is False

  @pytest.mark.asyncio
  async def test_no_from_address_configured(self):
    """Test behavior when from address is not configured."""
    with patch("robosystems.adapters.ses.env") as mock_env:
      mock_env.EMAIL_FROM_ADDRESS = ""
      mock_env.EMAIL_FROM_NAME = "Test"
      mock_env.ENVIRONMENT = "test"
      mock_env.AWS_REGION = "us-east-1"
      mock_env.ROBOLEDGER_URL = "https://roboledger.ai"
      mock_env.ROBOINVESTOR_URL = "https://roboinvestor.ai"
      mock_env.ROBOSYSTEMS_URL = "https://robosystems.ai"

      # Mock boto3 client
      with patch("robosystems.adapters.ses.boto3") as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        service = SESEmailService()
        service.from_address = ""  # Simulate no address configured

        result = await service.send_verification_email(
          user_email="test@example.com",
          user_name="Test User",
          token="token123",
        )

        assert result is False
        # Should not try to send when no from address
        mock_client.send_email.assert_not_called()

  @pytest.mark.asyncio
  async def test_app_url_selection(self, ses_service, mock_ses_client):
    """Test that correct app URLs are used for different apps."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-app"}

    # Test roboledger
    await ses_service.send_verification_email(
      user_email="test@example.com",
      user_name="Test",
      token="token1",
      app="roboledger",
    )

    call_args = mock_ses_client.send_email.call_args
    message = call_args.kwargs["Message"]
    assert "roboledger" in message["Body"]["Html"]["Data"].lower()

    # Reset mock
    mock_ses_client.reset_mock()
    mock_ses_client.send_email.return_value = {"MessageId": "msg-app2"}

    # Test roboinvestor
    await ses_service.send_verification_email(
      user_email="test@example.com",
      user_name="Test",
      token="token2",
      app="roboinvestor",
    )

    call_args = mock_ses_client.send_email.call_args
    message = call_args.kwargs["Message"]
    assert "roboinvestor" in message["Body"]["Html"]["Data"].lower()

  @pytest.mark.asyncio
  async def test_default_app_fallback(self, ses_service, mock_ses_client):
    """Test that unknown app names fall back to roboledger."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-default"}

    await ses_service.send_verification_email(
      user_email="test@example.com",
      user_name="Test",
      token="token_default",
      app="unknown_app",
    )

    call_args = mock_ses_client.send_email.call_args
    message = call_args.kwargs["Message"]
    # Should fall back to roboledger URL
    assert "roboledger" in message["Body"]["Html"]["Data"].lower()

  @pytest.mark.asyncio
  async def test_email_environment_included(self, mock_ses_client):
    """Test that environment is included in SES tags."""
    with patch("robosystems.adapters.ses.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"
      mock_env.EMAIL_TOKEN_EXPIRY_HOURS = 48
      mock_env.PASSWORD_RESET_TOKEN_EXPIRY_HOURS = 2
      mock_env.AWS_REGION = "us-east-1"
      mock_env.EMAIL_FROM_ADDRESS = "noreply@example.com"
      mock_env.EMAIL_FROM_NAME = "Test"
      mock_env.ROBOLEDGER_URL = "https://roboledger.ai"
      mock_env.ROBOINVESTOR_URL = "https://roboinvestor.ai"
      mock_env.ROBOSYSTEMS_URL = "https://robosystems.ai"

      mock_ses_client.send_email.return_value = {"MessageId": "msg-env"}

      # Mock boto3 to avoid real AWS calls
      with patch("robosystems.adapters.ses.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_ses_client

        # Re-initialize to pick up new env
        service = SESEmailService()
        service.ses_client = mock_ses_client
        service.from_address = "noreply@example.com"

        await service.send_email(
          email_type="email_verification",
          to_email="env@example.com",
          template_data={"user_name": "Env User"},
        )

        call_args = mock_ses_client.send_email.call_args
        tags = call_args.kwargs["Tags"]

        # Check environment tag
        assert any(
          tag["Name"] == "Environment" and tag["Value"] == "staging" for tag in tags
        )

  @patch("robosystems.adapters.ses.logger")
  @pytest.mark.asyncio
  async def test_logging_on_success(self, mock_logger, ses_service, mock_ses_client):
    """Test that successful sends are logged."""
    mock_ses_client.send_email.return_value = {"MessageId": "msg-log-success"}

    await ses_service.send_verification_email(
      user_email="log@example.com",
      user_name="Log User",
      token="token_log",
    )

    # Check info log was called
    mock_logger.info.assert_called()
    log_call = mock_logger.info.call_args[0][0]
    assert "email_verification" in log_call
    assert "log@example.com" in log_call
    assert "msg-log-success" in log_call

  @patch("robosystems.adapters.ses.logger")
  @pytest.mark.asyncio
  async def test_logging_on_failure(self, mock_logger, ses_service, mock_ses_client):
    """Test that failures are logged."""
    mock_ses_client.send_email.side_effect = ClientError(
      {"Error": {"Code": "MessageRejected", "Message": "Bad request"}},
      "SendEmail",
    )

    await ses_service.send_email(
      email_type="password_reset",
      to_email="fail@example.com",
      template_data={"user_name": "Fail User"},
    )

    # Check error log was called
    mock_logger.error.assert_called()
    log_call = mock_logger.error.call_args[0][0]
    assert "password_reset" in log_call or "SES rejected" in log_call
    assert "fail@example.com" in log_call
