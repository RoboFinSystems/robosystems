"""AWS SES adapter for sending transactional emails."""

from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.config.constants import (
  EMAIL_TOKEN_EXPIRY_HOURS,
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
)
from robosystems.logger import logger

# Brand colors (from tailwind.config.ts)
_BRAND_PRIMARY = "#1B3A57"  # primary-800 (dark blue)
_BRAND_BLUE = "#3B7AF5"  # primary-500 (bright blue)
_BRAND_GREEN = "#00D4AA"  # secondary-500 (bright green)
_BRAND_DARK = "#172E47"  # primary-950
_BRAND_LIGHT = "#EFF6FF"  # primary-50
_BRAND_BORDER = "#BFDBFE"  # primary-200
_BRAND_MUTED = "#64748b"
_BRAND_DANGER = "#dc2626"

# App display names
_APP_DISPLAY_NAMES = {
  "roboledger": "RoboLedger",
  "roboinvestor": "RoboInvestor",
  "robosystems": "RoboSystems",
}
_DEFAULT_APP = "robosystems"


def _base_template(app_name: str, content_html: str) -> str:
  """Wrap content in the shared email layout."""
  year = datetime.now(UTC).year
  return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{app_name}</title>
</head>
<body style="margin:0;padding:0;background-color:{_BRAND_LIGHT};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:{_BRAND_LIGHT};">
    <tr>
      <td align="center" style="padding:40px 20px;">
        <table role="presentation" width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
          <!-- Header -->
          <tr>
            <td style="padding:28px 32px;background-color:{_BRAND_PRIMARY};border-radius:12px 12px 0 0;">
              <span style="font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">{app_name}</span>
            </td>
          </tr>
          <!-- Accent bar -->
          <tr>
            <td style="height:3px;background-color:{_BRAND_GREEN};font-size:0;line-height:0;">&nbsp;</td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="padding:36px 32px;background-color:#ffffff;border-left:1px solid {_BRAND_BORDER};border-right:1px solid {_BRAND_BORDER};">
              {content_html}
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:24px 32px;background-color:{_BRAND_LIGHT};border:1px solid {_BRAND_BORDER};border-top:none;border-radius:0 0 12px 12px;text-align:center;">
              <p style="margin:0;font-size:12px;color:{_BRAND_MUTED};">&copy; {year} RFS LLC. All rights reserved.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _button(url: str, label: str, color: str = _BRAND_BLUE) -> str:
  """Render a CTA button."""
  return f"""\
<table role="presentation" cellpadding="0" cellspacing="0" style="margin:28px 0;">
  <tr>
    <td style="border-radius:8px;background-color:{color};">
      <a href="{url}" target="_blank" style="display:inline-block;padding:14px 32px;font-size:15px;font-weight:600;color:#ffffff;text-decoration:none;border-radius:8px;">{label}</a>
    </td>
  </tr>
</table>"""


def _muted(text: str) -> str:
  return f'<p style="margin:0 0 8px;font-size:13px;line-height:1.5;color:{_BRAND_MUTED};">{text}</p>'


def _paragraph(text: str) -> str:
  return f'<p style="margin:0 0 16px;font-size:15px;line-height:1.6;color:{_BRAND_DARK};">{text}</p>'


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
    user_name = template_data.get("user_name", "there")

    templates = {
      "email_verification": self._verification_template(
        app_name, user_name, template_data
      ),
      "password_reset": self._password_reset_template(
        app_name, user_name, template_data
      ),
      "welcome": self._welcome_template(app_name, user_name, template_data),
      "capacity_warning": self._capacity_warning_template(
        app_name, user_name, template_data
      ),
    }

    return templates.get(
      email_type,
      {
        "subject": f"{app_name} Notification",
        "html": f"<p>{template_data}</p>",
        "text": str(template_data),
      },
    )

  # -- Templates --------------------------------------------------------

  @staticmethod
  def _verification_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("verification_url", "#")
    expiry = data.get("expiry_hours", 24)

    content = (
      _paragraph(f"Hi {user_name},")
      + _paragraph(
        f"Thanks for creating a {app_name} account. "
        "Please verify your email address to get started."
      )
      + _button(url, "Verify Email Address")
      + _muted("Or copy and paste this URL into your browser:")
      + f'<p style="margin:0 0 20px;font-size:13px;line-height:1.5;color:{_BRAND_PRIMARY};word-break:break-all;">{url}</p>'
      + _muted(f"This link expires in {expiry} hours.")
      + _muted("If you didn't create this account, you can safely ignore this email.")
    )

    return {
      "subject": f"Verify your {app_name} email",
      "html": _base_template(app_name, content),
      "text": f"""Hi {user_name},

Thanks for creating a {app_name} account. Please verify your email address by visiting:

{url}

This link expires in {expiry} hours.

If you didn't create this account, you can safely ignore this email.

{app_name}""",
    }

  @staticmethod
  def _password_reset_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("reset_url", "#")
    expiry = data.get("expiry_hours", 1)

    content = (
      _paragraph(f"Hi {user_name},")
      + _paragraph(
        f"We received a request to reset the password for your {app_name} account."
      )
      + _button(url, "Reset Password", _BRAND_DANGER)
      + _muted("Or copy and paste this URL into your browser:")
      + f'<p style="margin:0 0 20px;font-size:13px;line-height:1.5;color:{_BRAND_PRIMARY};word-break:break-all;">{url}</p>'
      + _muted(f"This link expires in {expiry} hour{'s' if expiry != 1 else ''}.")
      + '<div style="margin-top:24px;padding:16px;background-color:#fef2f2;border:1px solid #fecaca;border-radius:8px;">'
      + '<p style="margin:0;font-size:13px;line-height:1.5;color:#991b1b;">'
      "If you didn't request this, no action is needed. Your password will not change."
      "</p></div>"
    )

    return {
      "subject": f"{app_name} password reset",
      "html": _base_template(app_name, content),
      "text": f"""Hi {user_name},

We received a request to reset the password for your {app_name} account.

Reset your password: {url}

This link expires in {expiry} hour{"s" if expiry != 1 else ""}.

If you didn't request this, no action is needed. Your password will not change.

{app_name}""",
    }

  @staticmethod
  def _welcome_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("dashboard_url", "#")

    content = (
      _paragraph(f"Hi {user_name},")
      + _paragraph("Your email has been verified and your account is ready to go.")
      + _button(url, "Go to Dashboard")
    )

    return {
      "subject": f"Welcome to {app_name}",
      "html": _base_template(app_name, content),
      "text": f"""Hi {user_name},

Your email has been verified and your account is ready to go.

Visit your dashboard: {url}

{app_name}""",
    }

  @staticmethod
  def _capacity_warning_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("dashboard_url", "#")
    usage_pct = data.get("usage_percentage", 0)
    used_gb = data.get("used_gb", 0)
    limit_gb = data.get("limit_gb", 0)
    graph_id = data.get("graph_id", "")
    tier = data.get("tier", "")
    instance_status = data.get("status", "approaching")
    databases: list[dict[str, Any]] = data.get("databases", [])

    is_over = instance_status == "over_limit"
    status_color = _BRAND_DANGER if is_over else "#d97706"  # red or amber
    status_label = "Over Limit" if is_over else "Approaching Limit"

    # Build database breakdown rows
    db_rows = ""
    for db in databases:
      db_id = db.get("graph_id", "")
      size = db.get("size_mb")
      label = "parent" if db.get("is_parent") else "subgraph"
      size_str = f"{size:,.1f} MB" if size is not None else "N/A"
      db_rows += (
        f'<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;font-size:13px;color:{_BRAND_DARK};">'
        f"{db_id} ({label})</td>"
        f'<td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;font-size:13px;color:{_BRAND_DARK};text-align:right;">'
        f"{size_str}</td></tr>"
      )

    db_table = ""
    if db_rows:
      db_table = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="margin:16px 0;border:1px solid #e2e8f0;border-radius:8px;border-collapse:collapse;">'
        f'<tr><th style="padding:8px 12px;text-align:left;font-size:12px;color:{_BRAND_MUTED};'
        f'border-bottom:2px solid #e2e8f0;background:{_BRAND_LIGHT};">Database</th>'
        f'<th style="padding:8px 12px;text-align:right;font-size:12px;color:{_BRAND_MUTED};'
        f'border-bottom:2px solid #e2e8f0;background:{_BRAND_LIGHT};">Size</th></tr>'
        f"{db_rows}</table>"
      )

    perf_warning = ""
    if is_over:
      perf_warning = (
        '<div style="margin:16px 0;padding:16px;background-color:#fef2f2;'
        'border:1px solid #fecaca;border-radius:8px;">'
        '<p style="margin:0;font-size:13px;line-height:1.5;color:#991b1b;">'
        "Your instance is over its storage limit. Query performance may degrade "
        "and operations may become unreliable. Consider upgrading your tier.</p></div>"
      )

    content = (
      _paragraph(f"Hi {user_name},")
      + _paragraph(
        f"Your graph instance <strong>{graph_id}</strong> ({tier}) is at "
        f"<strong style='color:{status_color}'>{usage_pct:.0f}%</strong> storage capacity."
      )
      + f'<div style="margin:20px 0;padding:20px;background:{_BRAND_LIGHT};'
      f'border:1px solid {_BRAND_BORDER};border-radius:8px;">'
      f'<div style="display:flex;justify-content:space-between;align-items:center;">'
      f'<div><span style="font-size:28px;font-weight:700;color:{status_color};">'
      f"{used_gb:.1f} GB</span>"
      f'<span style="font-size:14px;color:{_BRAND_MUTED};"> / {limit_gb:.0f} GB</span></div>'
      f'<div style="display:inline-block;padding:4px 12px;border-radius:4px;'
      f'background-color:{status_color};color:#fff;font-size:12px;font-weight:600;">'
      f"{status_label}</div></div></div>"
      + db_table
      + perf_warning
      + _button(url, "View Usage Details")
    )

    # Plain text version
    db_lines = ""
    for db in databases:
      db_id = db.get("graph_id", "")
      size = db.get("size_mb")
      label = "parent" if db.get("is_parent") else "subgraph"
      size_str = f"{size:,.1f} MB" if size is not None else "N/A"
      db_lines += f"  - {db_id} ({label}): {size_str}\n"

    perf_text = ""
    if is_over:
      perf_text = (
        "\nWARNING: Your instance is over its storage limit. "
        "Query performance may degrade. Consider upgrading your tier.\n"
      )

    return {
      "subject": f"{app_name} — Your graph instance is at {usage_pct:.0f}% storage capacity",
      "html": _base_template(app_name, content),
      "text": f"""Hi {user_name},

Your graph instance {graph_id} ({tier}) is at {usage_pct:.0f}% storage capacity.

Storage: {used_gb:.1f} GB / {limit_gb:.0f} GB
Status: {status_label}

Databases:
{db_lines}{perf_text}
View usage details: {url}

{app_name}""",
    }

  # -- Send -------------------------------------------------------------

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
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      "app_name": _APP_DISPLAY_NAMES.get(app, _APP_DISPLAY_NAMES[_DEFAULT_APP]),
      "verification_url": f"{base_url}/auth/verify-email?token={token}",
      "expiry_hours": EMAIL_TOKEN_EXPIRY_HOURS,
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
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      "app_name": _APP_DISPLAY_NAMES.get(app, _APP_DISPLAY_NAMES[_DEFAULT_APP]),
      "reset_url": f"{base_url}/auth/reset-password?token={token}",
      "expiry_hours": PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
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
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      "app_name": _APP_DISPLAY_NAMES.get(app, _APP_DISPLAY_NAMES[_DEFAULT_APP]),
      "dashboard_url": f"{base_url}/home",
    }

    return await self.send_email("welcome", user_email, template_data)

  async def send_capacity_warning_email(
    self,
    user_email: str,
    user_name: str,
    graph_id: str,
    tier: str,
    usage_percentage: float,
    used_gb: float,
    limit_gb: float,
    instance_status: str,
    databases: list[dict[str, Any]],
    app: str = "robosystems",
  ) -> bool:
    """
    Send capacity warning email when instance storage approaches or exceeds limits.

    Args:
        user_email: User's email address
        user_name: User's name
        graph_id: Parent graph identifier
        tier: Graph tier name
        usage_percentage: Current usage as percentage (e.g. 85.3)
        used_gb: Current storage used in GB
        limit_gb: Storage limit in GB
        instance_status: "approaching" or "over_limit"
        databases: Per-database breakdown list
        app: App identifier

    Returns:
        True if email was sent successfully, False otherwise
    """
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      "app_name": _APP_DISPLAY_NAMES.get(app, _APP_DISPLAY_NAMES[_DEFAULT_APP]),
      "dashboard_url": f"{base_url}/home",
      "graph_id": graph_id,
      "tier": tier,
      "usage_percentage": usage_percentage,
      "used_gb": used_gb,
      "limit_gb": limit_gb,
      "status": instance_status,
      "databases": databases,
    }

    return await self.send_email("capacity_warning", user_email, template_data)


# Create a singleton instance
ses_service = SESEmailService()
