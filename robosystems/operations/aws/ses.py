"""AWS SES adapter for sending transactional emails."""

from typing import Any

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger


class SESEmailService:
  """Service for sending transactional emails via Amazon SES."""

  def __init__(self):
    """Initialize SES client."""
    self.ses_client = boto3.client("ses", region_name=env.AWS_REGION)
    self.from_address = env.EMAIL_FROM_ADDRESS
    self.from_name = env.EMAIL_FROM_NAME

    # App URLs from existing environment variables
    self.app_urls = {
      "roboledger": env.ROBOLEDGER_URL,
      "roboinvestor": env.ROBOINVESTOR_URL,
      "robosystems": env.ROBOSYSTEMS_URL,
    }

    # Check if email is configured
    if not self.from_address:
      logger.warning("EMAIL_FROM_ADDRESS not configured - emails will not be sent")

  def _get_email_template(
    self, email_type: str, template_data: dict[str, Any]
  ) -> dict[str, str]:
    """Get email subject and body templates based on email type."""
    app_name = template_data.get("app_name", "RoboSystems")
    user_name = template_data.get("user_name", "User")

    templates = {
      "email_verification": {
        "subject": f"Verify Your {app_name} Email",
        "html": f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #1a1a2e; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ padding: 30px; background-color: #f8f9fa; border: 1px solid #dee2e6; border-top: none; }}
        .button {{ display: inline-block; padding: 12px 30px; background-color: #007bff; color: white !important; text-decoration: none; border-radius: 5px; margin: 20px 0; font-weight: bold; }}
        .footer {{ text-align: center; padding: 20px; color: #6c757d; font-size: 12px; }}
        .link {{ word-break: break-all; color: #007bff; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{app_name}</h1>
        </div>
        <div class="content">
            <h2>Welcome, {user_name}!</h2>
            <p>Thank you for signing up for {app_name}. Please verify your email address by clicking the button below:</p>
            <div style="text-align: center;">
                <a href="{template_data.get("verification_url", "#")}" class="button">Verify Email Address</a>
            </div>
            <p style="color: #6c757d; font-size: 14px;">Or copy and paste this link into your browser:</p>
            <p class="link">{template_data.get("verification_url", "#")}</p>
            <p style="color: #6c757d; font-size: 14px; margin-top: 30px;">This link will expire in {template_data.get("expiry_hours", "24")} hours.</p>
            <p style="color: #6c757d; font-size: 14px;">If you didn't create an account, you can safely ignore this email.</p>
        </div>
        <div class="footer">
            <p>&copy; 2024 RoboSystems. All rights reserved.</p>
            <p>This is an automated message, please do not reply to this email.</p>
        </div>
    </div>
</body>
</html>""",
        "text": f"""Welcome to {app_name}, {user_name}!

Please verify your email address by clicking the link below:
{template_data.get("verification_url", "")}

This link will expire in {template_data.get("expiry_hours", "24")} hours.

If you didn't create an account, you can safely ignore this email.

Best regards,
The {app_name} Team""",
      },
      "password_reset": {
        "subject": f"{app_name} Password Reset Request",
        "html": f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #1a1a2e; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ padding: 30px; background-color: #f8f9fa; border: 1px solid #dee2e6; border-top: none; }}
        .button {{ display: inline-block; padding: 12px 30px; background-color: #dc3545; color: white !important; text-decoration: none; border-radius: 5px; margin: 20px 0; font-weight: bold; }}
        .footer {{ text-align: center; padding: 20px; color: #6c757d; font-size: 12px; }}
        .link {{ word-break: break-all; color: #007bff; }}
        .warning {{ background-color: #fff3cd; border: 1px solid #ffc107; padding: 10px; border-radius: 5px; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{app_name}</h1>
        </div>
        <div class="content">
            <h2>Password Reset Request</h2>
            <p>Hi {user_name},</p>
            <p>We received a request to reset your password for your {app_name} account.</p>
            <div style="text-align: center;">
                <a href="{template_data.get("reset_url", "#")}" class="button">Reset Password</a>
            </div>
            <p style="color: #6c757d; font-size: 14px;">Or copy and paste this link into your browser:</p>
            <p class="link">{template_data.get("reset_url", "#")}</p>
            <p style="color: #6c757d; font-size: 14px; margin-top: 30px;">This link will expire in {template_data.get("expiry_hours", "1")} hour(s).</p>
            <div class="warning">
                <strong>Security Notice:</strong> If you didn't request this password reset, please ignore this email. Your password will remain unchanged.
            </div>
        </div>
        <div class="footer">
            <p>&copy; 2024 RoboSystems. All rights reserved.</p>
            <p>This is an automated message, please do not reply to this email.</p>
        </div>
    </div>
</body>
</html>""",
        "text": f"""Password Reset Request for {app_name}

Hi {user_name},

We received a request to reset your password for your {app_name} account.

Click the link below to reset your password:
{template_data.get("reset_url", "")}

This link will expire in {template_data.get("expiry_hours", "1")} hour(s).

Security Notice: If you didn't request this password reset, please ignore this email. Your password will remain unchanged.

Best regards,
The {app_name} Team""",
      },
      "welcome": {
        "subject": f"Welcome to {app_name}!",
        "html": f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background-color: #1a1a2e; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
        .content {{ padding: 30px; background-color: #f8f9fa; border: 1px solid #dee2e6; border-top: none; }}
        .button {{ display: inline-block; padding: 12px 30px; background-color: #28a745; color: white !important; text-decoration: none; border-radius: 5px; margin: 20px 0; font-weight: bold; }}
        .footer {{ text-align: center; padding: 20px; color: #6c757d; font-size: 12px; }}
        .features {{ background-color: white; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        .features ul {{ margin: 10px 0; padding-left: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Welcome to {app_name}!</h1>
        </div>
        <div class="content">
            <h2>Your email has been verified!</h2>
            <p>Hi {user_name},</p>
            <p>Thank you for verifying your email address. Your account is now fully activated and ready to use.</p>
            <div style="text-align: center;">
                <a href="{template_data.get("dashboard_url", "#")}" class="button">Go to Dashboard</a>
            </div>
            <div class="features">
                <h3>Get Started:</h3>
                <ul>
                    <li>Complete your profile settings</li>
                    <li>Connect your data sources</li>
                    <li>Explore our documentation and tutorials</li>
                    <li>Join our community forum</li>
                </ul>
            </div>
            <p>If you have any questions or need assistance, our support team is here to help.</p>
        </div>
        <div class="footer">
            <p>&copy; 2024 RoboSystems. All rights reserved.</p>
            <p>This is an automated message, please do not reply to this email.</p>
        </div>
    </div>
</body>
</html>""",
        "text": f"""Welcome to {app_name}!

Hi {user_name},

Thank you for verifying your email address. Your account is now fully activated and ready to use.

Visit your dashboard: {template_data.get("dashboard_url", "")}

Get Started:
- Complete your profile settings
- Connect your data sources
- Explore our documentation and tutorials
- Join our community forum

If you have any questions or need assistance, our support team is here to help.

Best regards,
The {app_name} Team""",
      },
    }

    return templates.get(
      email_type,
      {
        "subject": f"{app_name} Notification",
        "html": f"<p>{template_data}</p>",
        "text": str(template_data),
      },
    )

  async def send_email(
    self, email_type: str, to_email: str, template_data: dict[str, Any]
  ) -> bool:
    """
    Send an email via Amazon SES.

    Args:
        email_type: Type of email (email_verification, password_reset, welcome)
        to_email: Recipient email address
        template_data: Data for the email template

    Returns:
        True if email was sent successfully, False otherwise
    """
    if not self.from_address:
      logger.warning(
        f"Cannot send {email_type} email - EMAIL_FROM_ADDRESS not configured"
      )
      return False

    try:
      # Get email template
      template = self._get_email_template(email_type, template_data)

      # Prepare the email
      message = {
        "Subject": {"Data": template["subject"], "Charset": "UTF-8"},
        "Body": {
          "Text": {"Data": template["text"], "Charset": "UTF-8"},
          "Html": {"Data": template["html"], "Charset": "UTF-8"},
        },
      }

      # Send email via SES
      response = self.ses_client.send_email(
        Source=f"{self.from_name} <{self.from_address}>",
        Destination={"ToAddresses": [to_email]},
        Message=message,
        Tags=[
          {"Name": "EmailType", "Value": email_type},
          {"Name": "Environment", "Value": env.ENVIRONMENT},
        ],
      )

      logger.info(
        f"Sent {email_type} email to {to_email}. MessageId: {response['MessageId']}"
      )
      return True

    except ClientError as e:
      error_code = e.response["Error"]["Code"]
      error_message = e.response["Error"]["Message"]

      if error_code == "MessageRejected":
        logger.error(f"SES rejected email to {to_email}: {error_message}")
      elif error_code == "MailFromDomainNotVerified":
        logger.error(f"SES sender domain not verified: {self.from_address}")
      elif error_code == "ConfigurationSetDoesNotExist":
        logger.error("SES configuration set does not exist")
      else:
        logger.error(
          f"AWS SES error sending {email_type} email to {to_email}: {error_code} - {error_message}"
        )
      return False

    except Exception as e:
      logger.error(f"Unexpected error sending {email_type} email to {to_email}: {e!s}")
      return False

  async def send_verification_email(
    self,
    user_email: str,
    user_name: str,
    token: str,
    app: str = "roboledger",
  ) -> bool:
    """
    Send email verification email.

    Args:
        user_email: User's email address
        user_name: User's name
        token: Verification token
        app: App identifier (roboledger, roboinvestor, robosystems)

    Returns:
        True if email was sent successfully, False otherwise
    """
    # Get app-specific URL
    base_url = self.app_urls.get(app, self.app_urls["roboledger"])

    # Map app names for display
    app_display_names = {
      "roboledger": "Roboledger",
      "roboinvestor": "Roboinvestor",
      "robosystems": "Robosystems",
    }

    template_data = {
      "user_name": user_name,
      "app_name": app_display_names.get(app, "RoboSystems"),
      "verification_url": f"{base_url}/auth/verify-email?token={token}",
      "expiry_hours": env.EMAIL_TOKEN_EXPIRY_HOURS,
    }

    return await self.send_email("email_verification", user_email, template_data)

  async def send_password_reset_email(
    self,
    user_email: str,
    user_name: str,
    token: str,
    app: str = "roboledger",
  ) -> bool:
    """
    Send password reset email.

    Args:
        user_email: User's email address
        user_name: User's name
        token: Reset token
        app: App identifier (roboledger, roboinvestor, robosystems)

    Returns:
        True if email was sent successfully, False otherwise
    """
    # Get app-specific URL
    base_url = self.app_urls.get(app, self.app_urls["roboledger"])

    # Map app names for display
    app_display_names = {
      "roboledger": "Roboledger",
      "roboinvestor": "Roboinvestor",
      "robosystems": "Robosystems",
    }

    template_data = {
      "user_name": user_name,
      "app_name": app_display_names.get(app, "RoboSystems"),
      "reset_url": f"{base_url}/auth/reset-password?token={token}",
      "expiry_hours": env.PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
    }

    return await self.send_email("password_reset", user_email, template_data)

  async def send_welcome_email(
    self, user_email: str, user_name: str, app: str = "roboledger"
  ) -> bool:
    """
    Send welcome email after email verification.

    Args:
        user_email: User's email address
        user_name: User's name
        app: App identifier (roboledger, roboinvestor, robosystems)

    Returns:
        True if email was sent successfully, False otherwise
    """
    # Get app-specific URL
    base_url = self.app_urls.get(app, self.app_urls["roboledger"])

    # Map app names for display
    app_display_names = {
      "roboledger": "Roboledger",
      "roboinvestor": "Roboinvestor",
      "robosystems": "Robosystems",
    }

    template_data = {
      "user_name": user_name,
      "app_name": app_display_names.get(app, "RoboSystems"),
      "dashboard_url": f"{base_url}/dashboard",
    }

    return await self.send_email("welcome", user_email, template_data)


# Create a singleton instance
ses_service = SESEmailService()
