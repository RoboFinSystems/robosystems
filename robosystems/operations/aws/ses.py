"""AWS SES adapter for sending transactional emails."""

import asyncio
import html
import smtplib
import ssl
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid
from typing import Any
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.config.constants import (
  EMAIL_TOKEN_EXPIRY_HOURS,
  ORG_INVITATION_EXPIRY_DAYS,
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
)
from robosystems.logger import logger

# The apps are dark, so the email is dark in every client: one palette, and a
# declared dark color scheme so a client's own dark mode leaves it alone.
_PAGE_BG = "#09090B"  # zinc-950
_CARD_BG = "#18181B"  # zinc-900
_PANEL_BG = "#111113"
_BORDER = "#27272A"  # zinc-800
_TEXT = "#E4E4E7"  # zinc-200
_HEADING = "#FAFAFA"
_MUTED = "#A1A1AA"  # zinc-400
_FAINT = "#71717A"  # zinc-500
_DANGER = "#F87171"
_DANGER_BG = "#2A1114"
_DANGER_BORDER = "#7F1D1D"
_DANGER_TEXT = "#FECACA"
_WARNING = "#FBBF24"

# Space Grotesk and Orbitron are the apps' fonts. Clients that load web fonts
# (Apple Mail, iOS Mail) get them from Google Fonts; the rest fall back.
_FONT_BODY = (
  "'Space Grotesk',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,"
  "'Helvetica Neue',Arial,sans-serif"
)
_FONT_HEADING = f"Orbitron,{_FONT_BODY}"
_FONT_MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"
_FONTS_URL = (
  "https://fonts.googleapis.com/css2?"
  "family=Orbitron:wght@600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap"
)

# Each app's accent, from its tailwind.config.ts: the bar's gradient, a button
# shade that holds white text, and a link shade that reads on the dark card.
_APP_ACCENTS = {
  "robosystems": {
    "from": "#3B7AF5",
    "to": "#06B6D4",
    "button": "#2563EB",
    "link": "#6098FA",
  },
  "roboledger": {
    "from": "#8B5CF6",
    "to": "#D946EF",
    "button": "#7C3AED",
    "link": "#A78BFA",
  },
  "roboinvestor": {
    "from": "#10B981",
    "to": "#06B6D4",
    "button": "#047857",
    "link": "#34D399",
  },
}

# App display names
_APP_DISPLAY_NAMES = {
  "roboledger": "RoboLedger",
  "roboinvestor": "RoboInvestor",
  "robosystems": "RoboSystems",
}
_DEFAULT_APP = "robosystems"


def _accent(data: dict[str, Any]) -> dict[str, str]:
  return _APP_ACCENTS.get(data.get("app", ""), _APP_ACCENTS[_DEFAULT_APP])


def _esc(value: Any) -> str:
  return html.escape(str(value), quote=True)


def _base_template(
  data: dict[str, Any], content_html: str, heading: str, preheader: str = ""
) -> str:
  """Wrap content in the shared email layout.

  `data` carries the brand keys `_brand` adds (`app`, `app_name`, `home_url`,
  `logo_url`). `heading` and `preheader` are escaped here.
  """
  year = datetime.now(UTC).year
  accent = _accent(data)
  app_name = _esc(data.get("app_name", _APP_DISPLAY_NAMES[_DEFAULT_APP]))
  home_url = _esc(data.get("home_url", "#"))
  logo_url = data.get("logo_url")
  logo = (
    f'<td style="padding-right:12px;"><img src="{_esc(logo_url)}" width="32" height="32" alt="" '
    'style="display:block;border:0;outline:none;"></td>'
    if logo_url
    else ""
  )
  return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="dark">
  <meta name="supported-color-schemes" content="dark">
  <title>{app_name}</title>
  <!--[if !mso]><!-->
  <link href="{_FONTS_URL}" rel="stylesheet">
  <!--<![endif]-->
  <style>
    :root {{ color-scheme: dark; }}
    @media only screen and (max-width: 480px) {{
      .rs-outer {{ padding: 16px 8px !important; }}
      .rs-pad {{ padding-left: 20px !important; padding-right: 20px !important; }}
    }}
  </style>
</head>
<body style="margin:0;padding:0;background-color:{_PAGE_BG};font-family:{_FONT_BODY};">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:{_PAGE_BG};">{_esc(preheader)}</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" bgcolor="{_PAGE_BG}" style="background-color:{_PAGE_BG};">
    <tr>
      <td class="rs-outer" align="center" style="padding:40px 16px;">
        <table role="presentation" width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
          <!-- Brand -->
          <tr>
            <td style="padding:0 4px 20px;">
              <table role="presentation" cellpadding="0" cellspacing="0">
                <tr>
                  {logo}
                  <td style="font-family:{_FONT_HEADING};font-size:18px;font-weight:700;letter-spacing:0.5px;color:{_HEADING};"><a href="{home_url}" target="_blank" style="color:{_HEADING};text-decoration:none;">{app_name}</a></td>
                </tr>
              </table>
            </td>
          </tr>
          <!-- Accent bar -->
          <tr>
            <td bgcolor="{accent["from"]}" style="height:3px;font-size:0;line-height:0;background-color:{accent["from"]};background-image:linear-gradient(90deg,{accent["from"]},{accent["to"]});border-radius:12px 12px 0 0;">&nbsp;</td>
          </tr>
          <!-- Body -->
          <tr>
            <td class="rs-pad" bgcolor="{_CARD_BG}" style="padding:36px 36px 28px;background-color:{_CARD_BG};border:1px solid {_BORDER};border-top:none;border-radius:0 0 12px 12px;">
              <h1 style="margin:0 0 20px;font-family:{_FONT_HEADING};font-size:20px;line-height:1.35;font-weight:600;color:{_HEADING};">{_esc(heading)}</h1>
              {content_html}
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:24px 4px 0;text-align:center;font-family:{_FONT_BODY};">
              <p style="margin:0 0 6px;font-size:12px;line-height:1.5;color:{_FAINT};">You are receiving this email because of activity on your <a href="{home_url}" target="_blank" style="color:{_MUTED};text-decoration:underline;">{app_name}</a> account.</p>
              <p style="margin:0;font-size:12px;line-height:1.5;color:{_FAINT};">&copy; {year} RFS LLC. All rights reserved.</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _button(data: dict[str, Any], url: str, label: str) -> str:
  """Render a CTA button in the app's accent."""
  color = _accent(data)["button"]
  return f"""<table role="presentation" cellpadding="0" cellspacing="0" style="margin:28px 0;">
  <tr>
    <td bgcolor="{color}" style="border-radius:8px;background-color:{color};">
      <a href="{_esc(url)}" target="_blank" style="display:inline-block;padding:13px 28px;font-family:{_FONT_BODY};font-size:15px;font-weight:600;color:#ffffff;text-decoration:none;border-radius:8px;">{label}</a>
    </td>
  </tr>
</table>"""


def _link_fallback(data: dict[str, Any], url: str) -> str:
  """The button's URL in the open, for a client that will not follow a button."""
  return (
    _muted("Or copy and paste this URL into your browser:")
    + f'<p style="margin:0 0 20px;padding:12px 14px;background-color:{_PANEL_BG};border:1px solid {_BORDER};border-radius:8px;font-family:{_FONT_MONO};font-size:12px;line-height:1.5;word-break:break-all;">'
    f'<a href="{_esc(url)}" target="_blank" style="color:{_accent(data)["link"]};text-decoration:none;">{_esc(url)}</a></p>'
  )


def _callout(text: str) -> str:
  """A security notice: what to do if the recipient did not take this action."""
  return (
    f'<div style="margin-top:24px;padding:14px 16px;background-color:{_DANGER_BG};'
    f'border:1px solid {_DANGER_BORDER};border-radius:8px;">'
    f'<p style="margin:0;font-size:13px;line-height:1.5;color:{_DANGER_TEXT};">{text}</p></div>'
  )


def _muted(text: str) -> str:
  return f'<p style="margin:0 0 8px;font-size:13px;line-height:1.5;color:{_MUTED};">{text}</p>'


def _paragraph(text: str) -> str:
  return f'<p style="margin:0 0 16px;font-size:15px;line-height:1.6;color:{_TEXT};">{text}</p>'


class SMTPTransport:
  """Delivery over SMTP, chosen by `SMTP_HOST`: the dev stack's Mailpit, or a
  relay on a self-hosted deployment's own network. Port 465 is implicit TLS;
  any other port negotiates STARTTLS unless `SMTP_STARTTLS` is false."""

  IMPLICIT_TLS_PORT = 465

  def __init__(
    self,
    host: str,
    port: int,
    username: str = "",
    password: str = "",
    starttls: bool = True,
    timeout: float = 10.0,
  ):
    self.host = host
    self.port = port
    self.username = username
    self.password = password
    self.starttls = starttls
    self.timeout = timeout

  def send(
    self,
    *,
    from_name: str,
    from_address: str,
    to_email: str,
    subject: str,
    text: str,
    html: str,
  ) -> str:
    """Send one message and return its Message-ID. Raises on any failure."""
    message = EmailMessage()
    message["From"] = formataddr((from_name, from_address))
    message["To"] = to_email
    message["Subject"] = subject
    message["Date"] = formatdate(usegmt=True)
    message["Message-ID"] = make_msgid(domain=from_address.rpartition("@")[2] or None)
    message.set_content(text)
    message.add_alternative(html, subtype="html")

    context = ssl.create_default_context()
    if self.port == self.IMPLICIT_TLS_PORT:
      connection = smtplib.SMTP_SSL(
        self.host, self.port, timeout=self.timeout, context=context
      )
    else:
      connection = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
    with connection as session:
      if self.port != self.IMPLICIT_TLS_PORT and self.starttls:
        session.starttls(context=context)
      if self.username:
        session.login(self.username, self.password)
      session.send_message(message)
    return message["Message-ID"]


class SESEmailService:
  """Service for sending transactional emails: Amazon SES, or SMTP when
  `SMTP_HOST` is set."""

  def __init__(self):
    self.smtp = (
      SMTPTransport(
        env.SMTP_HOST,
        env.SMTP_PORT,
        env.SMTP_USERNAME,
        env.SMTP_PASSWORD,
        env.SMTP_STARTTLS,
      )
      if env.SMTP_HOST
      else None
    )
    self.ses_client = (
      None if self.smtp else boto3.client("ses", region_name=env.AWS_REGION)
    )
    self.from_address = env.EMAIL_FROM_ADDRESS
    self.from_name = env.EMAIL_FROM_NAME

    self.app_urls = {
      "roboledger": env.ROBOLEDGER_URL,
      "roboinvestor": env.ROBOINVESTOR_URL,
      "robosystems": env.ROBOSYSTEMS_URL,
    }
    self.login_home_url = self.app_urls.get(
      env.LOGIN_HOME_APP, self.app_urls[_DEFAULT_APP]
    )

    if not self.from_address:
      logger.warning("EMAIL_FROM_ADDRESS not configured - emails will not be sent")

  def _auth_link_parts(self, app: str) -> tuple[str, str]:
    """Base URL and return_to suffix for auth action links (verify/reset/invite).

    Always the login home (product apps have no auth surface, and the bounce
    can strip the token), with the originating app as return_to unless it is
    the login home itself.
    """
    suffix = "" if app == env.LOGIN_HOME_APP else f"&return_to={quote(app)}"
    return self.login_home_url, suffix

  def _brand(self, app: str) -> dict[str, str]:
    """Brand keys every template's layout reads (unknown app → robosystems)."""
    key = app if app in _APP_DISPLAY_NAMES else _DEFAULT_APP
    home_url = self.app_urls[key]
    return {
      "app": key,
      "app_name": _APP_DISPLAY_NAMES[key],
      "home_url": home_url,
      "logo_url": f"{home_url}/images/logos/{key}.png",
    }

  def _get_email_template(
    self, email_type: str, template_data: dict[str, Any]
  ) -> dict[str, str]:
    """Render subject/html/text for `email_type`.

    An unrecognized type falls back to a generic notification that dumps
    `template_data` rather than raising, so a caller typo degrades to a sent
    email instead of a failed one.
    """
    app_name = template_data.get("app_name", "RoboSystems")
    user_name = template_data.get("user_name", "there")

    templates = {
      "email_verification": self._verification_template,
      "password_reset": self._password_reset_template,
      "welcome": self._welcome_template,
      "org_invitation": self._org_invitation_template,
      "capacity_warning": self._capacity_warning_template,
      "passkey_enrolled": self._passkey_enrolled_template,
      "email_changed": self._email_changed_template,
    }

    render = templates.get(email_type)
    if render is None:
      return {
        "subject": f"{app_name} Notification",
        "html": f"<p>{_esc(template_data)}</p>",
        "text": str(template_data),
      }
    return render(app_name, user_name, template_data)

  # -- Templates --------------------------------------------------------
  # user_name, org and inviter names, and passkey names are user-controlled:
  # every one is escaped before it reaches the HTML part.

  @staticmethod
  def _verification_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("verification_url", "#")
    expiry = data.get("expiry_hours", 24)

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph(
        f"Thanks for creating a {app_name} account. "
        "Please verify your email address to get started."
      )
      + _button(data, url, "Verify Email Address")
      + _link_fallback(data, url)
      + _muted(f"This link expires in {expiry} hours.")
      + _muted("If you didn't create this account, you can safely ignore this email.")
    )

    return {
      "subject": f"Verify your {app_name} email",
      "html": _base_template(
        data,
        content,
        heading="Verify your email address",
        preheader=f"Confirm this address to finish setting up your {app_name} account.",
      ),
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
    expires = f"This link expires in {expiry} hour{'s' if expiry != 1 else ''}."

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph(
        f"We received a request to reset the password for your {app_name} account."
      )
      + _button(data, url, "Reset Password")
      + _link_fallback(data, url)
      + _muted(expires)
      + _callout(
        "If you didn't request this, no action is needed. Your password will not change."
      )
    )

    return {
      "subject": f"{app_name} password reset",
      "html": _base_template(
        data,
        content,
        heading="Reset your password",
        preheader=f"Use this link to choose a new password. {expires}",
      ),
      "text": f"""Hi {user_name},

We received a request to reset the password for your {app_name} account.

Reset your password: {url}

{expires}

If you didn't request this, no action is needed. Your password will not change.

{app_name}""",
    }

  @staticmethod
  def _passkey_enrolled_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    # This email exists to catch an attacker enrolling a credential, so its
    # HTML must not be overridable by the very input that attacker chose.
    passkey_name = data.get("passkey_name", "a new passkey")

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph(
        f"A passkey ({_esc(passkey_name)}) was just added to your {app_name} "
        "account. It can now be used to sign in."
      )
      + _callout(
        "If you didn't add this passkey, reset your password immediately and "
        "remove it from your account's security settings."
      )
    )

    return {
      "subject": f"A passkey was added to your {app_name} account",
      "html": _base_template(
        data,
        content,
        heading="A passkey was added",
        preheader=f"A new passkey can now sign in to your {app_name} account.",
      ),
      "text": f"""Hi {user_name},

A passkey ({passkey_name}) was just added to your {app_name} account. It can now be used to sign in.

If you didn't add this passkey, reset your password immediately and remove it from your account's security settings.

{app_name}""",
    }

  @staticmethod
  def _email_changed_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    # Goes to the PREVIOUS address. new_email is already masked by the caller.
    new_email = data.get("new_email", "a new address")

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph(
        f"The email address on your {app_name} account was just changed to "
        f"{_esc(new_email)}. You are receiving this at your previous address."
      )
      + _callout(
        "If you didn't make this change, contact support immediately — someone "
        "may have access to your account."
      )
    )

    return {
      "subject": f"The email on your {app_name} account was changed",
      "html": _base_template(
        data,
        content,
        heading="Your email address was changed",
        preheader=f"The sign-in email on your {app_name} account is now different.",
      ),
      "text": f"""Hi {user_name},

The email address on your {app_name} account was just changed to {new_email}. You are receiving this at your previous address.

If you didn't make this change, contact support immediately — someone may have access to your account.

{app_name}""",
    }

  @staticmethod
  def _welcome_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("dashboard_url", "#")

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph("Your email has been verified and your account is ready to go.")
      + _button(data, url, "Go to Dashboard")
    )

    return {
      "subject": f"Welcome to {app_name}",
      "html": _base_template(
        data,
        content,
        heading=f"Welcome to {app_name}",
        preheader="Your email is verified and your account is ready.",
      ),
      "text": f"""Hi {user_name},

Your email has been verified and your account is ready to go.

Visit your dashboard: {url}

{app_name}""",
    }

  @staticmethod
  def _org_invitation_template(
    app_name: str, user_name: str, data: dict[str, Any]
  ) -> dict[str, str]:
    url = data.get("invite_url", "#")
    inviter_name = data.get("inviter_name", "A teammate")
    org_name = data.get("org_name", "an organization")
    expiry_days = data.get("expiry_days", ORG_INVITATION_EXPIRY_DAYS)

    content = (
      _paragraph("Hi there,")
      + _paragraph(
        f'<strong style="color:{_HEADING};">{_esc(inviter_name)}</strong> has invited you to join '
        f'<strong style="color:{_HEADING};">{_esc(org_name)}</strong> on {app_name}. '
        "Accept the invitation to create your account and join the team."
      )
      + _button(data, url, "Accept Invitation")
      + _link_fallback(data, url)
      + _muted(f"This invitation expires in {expiry_days} days.")
      + _muted(
        "If you weren't expecting this invitation, you can safely ignore this email."
      )
    )

    return {
      "subject": f"You've been invited to join {org_name} on {app_name}",
      "html": _base_template(
        data,
        content,
        heading=f"Join {org_name} on {app_name}",
        preheader=f"{inviter_name} invited you. The invitation expires in {expiry_days} days.",
      ),
      "text": f"""Hi there,

{inviter_name} has invited you to join {org_name} on {app_name}. Accept the invitation to create your account and join the team:

{url}

This invitation expires in {expiry_days} days.

If you weren't expecting this invitation, you can safely ignore this email.

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
    status_color = _DANGER if is_over else _WARNING
    status_label = "Over Limit" if is_over else "Approaching Limit"

    cell = f"padding:8px 12px;border-bottom:1px solid {_BORDER};font-size:13px"
    db_rows = ""
    for db in databases:
      db_id = db.get("graph_id", "")
      size = db.get("size_mb")
      label = "parent" if db.get("is_parent") else "subgraph"
      size_str = f"{size:,.1f} MB" if size is not None else "N/A"
      db_rows += (
        f'<tr><td style="{cell};color:{_TEXT};">{_esc(db_id)} ({label})</td>'
        f'<td style="{cell};color:{_TEXT};text-align:right;">{size_str}</td></tr>'
      )

    db_table = ""
    if db_rows:
      head = f"{cell};font-size:12px;font-weight:600;color:{_MUTED};background-color:{_PANEL_BG}"
      db_table = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="margin:16px 0;border:1px solid {_BORDER};border-bottom:none;">'
        f'<tr><th style="{head};text-align:left;">Database</th>'
        f'<th style="{head};text-align:right;">Size</th></tr>'
        f"{db_rows}</table>"
      )

    perf_warning = ""
    if is_over:
      perf_warning = _callout(
        "Your instance is over its storage limit. Query performance may degrade "
        "and operations may become unreliable. Consider upgrading your tier."
      )

    content = (
      _paragraph(f"Hi {_esc(user_name)},")
      + _paragraph(
        f'Your graph instance <strong style="color:{_HEADING};">{_esc(graph_id)}</strong> '
        f"({_esc(tier)}) is at "
        f'<strong style="color:{status_color};">{usage_pct:.0f}%</strong> storage capacity.'
      )
      + f'<div style="margin:20px 0;padding:20px;background-color:{_PANEL_BG};'
      f'border:1px solid {_BORDER};border-radius:8px;">'
      f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
      f'<td style="vertical-align:middle;">'
      f'<span style="font-size:28px;font-weight:700;color:{status_color};">'
      f"{used_gb:.1f} GB</span>"
      f'<span style="font-size:14px;color:{_MUTED};"> / {limit_gb:.0f} GB</span></td>'
      f'<td style="vertical-align:middle;text-align:right;">'
      f'<span style="display:inline-block;padding:4px 12px;border-radius:4px;'
      f'border:1px solid {status_color};color:{status_color};font-size:12px;font-weight:600;">'
      f"{status_label}</span></td></tr></table></div>"
      + db_table
      + perf_warning
      + _button(data, url, "View Usage Details")
    )

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
      "html": _base_template(
        data,
        content,
        heading=f"Storage at {usage_pct:.0f}% of capacity",
        preheader=f"{graph_id}: {used_gb:.1f} GB of {limit_gb:.0f} GB used.",
      ),
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
    """Render `email_type` and send it over SES, or over SMTP when
    `SMTP_HOST` is set.

    Never raises: a send failure (unconfigured sender, SES rejection, SMTP or
    network error) is logged and reported as False, so email delivery can't
    fail a request that only needed to notify someone.
    """
    if not self.from_address:
      logger.warning(
        f"Cannot send {email_type} email - EMAIL_FROM_ADDRESS not configured"
      )
      return False

    try:
      template = self._get_email_template(email_type, template_data)

      if self.smtp:
        message_id = await asyncio.to_thread(
          self.smtp.send,
          from_name=self.from_name,
          from_address=self.from_address,
          to_email=to_email,
          subject=template["subject"],
          text=template["text"],
          html=template["html"],
        )
        logger.info(
          f"Sent {email_type} email to {to_email} over SMTP ({self.smtp.host}). "
          f"Message-ID: {message_id}"
        )
        return True

      assert self.ses_client is not None
      message = {
        "Subject": {"Data": template["subject"], "Charset": "UTF-8"},
        "Body": {
          "Text": {"Data": template["text"], "Charset": "UTF-8"},
          "Html": {"Data": template["html"], "Charset": "UTF-8"},
        },
      }

      response = await asyncio.to_thread(
        self.ses_client.send_email,
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
    """Send the email-verification link for `app` (unknown app → robosystems)."""
    base_url, return_to = self._auth_link_parts(app)

    template_data = {
      "user_name": user_name,
      **self._brand(app),
      "verification_url": f"{base_url}/auth/verify-email?token={token}{return_to}",
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
    """Send the password-reset link for `app` (unknown app → robosystems)."""
    base_url, return_to = self._auth_link_parts(app)

    template_data = {
      "user_name": user_name,
      **self._brand(app),
      "reset_url": f"{base_url}/auth/reset-password?token={token}{return_to}",
      "expiry_hours": PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
    }

    return await self.send_email("password_reset", user_email, template_data)

  async def send_welcome_email(
    self, user_email: str, user_name: str, app: str = "roboledger"
  ) -> bool:
    """Send the post-verification welcome email."""
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      **self._brand(app),
      "dashboard_url": f"{base_url}/home",
    }

    return await self.send_email("welcome", user_email, template_data)

  async def send_passkey_enrolled_email(
    self,
    user_email: str,
    user_name: str,
    passkey_name: str,
    app: str = "robosystems",
  ) -> bool:
    """Security notification: a passkey was added to the account."""
    template_data = {
      "user_name": user_name,
      **self._brand(app),
      "passkey_name": passkey_name,
    }

    return await self.send_email("passkey_enrolled", user_email, template_data)

  async def send_email_changed_notice(
    self,
    user_email: str,
    user_name: str,
    new_email: str,
    app: str = "robosystems",
  ) -> bool:
    """Security notification to the PREVIOUS address that the account's email
    was changed. ``new_email`` is expected pre-masked by the caller."""
    template_data = {
      "user_name": user_name,
      **self._brand(app),
      "new_email": new_email,
    }

    return await self.send_email("email_changed", user_email, template_data)

  async def send_org_invitation_email(
    self,
    user_email: str,
    inviter_name: str,
    org_name: str,
    token: str,
    app: str = "robosystems",
  ) -> bool:
    """Invite `user_email` to an org.

    The recipient has no account yet — `token` is carried into the registration
    link so accepting and signing up are one step.
    """
    base_url, return_to = self._auth_link_parts(app)

    template_data = {
      **self._brand(app),
      "inviter_name": inviter_name,
      "org_name": org_name,
      "invite_url": f"{base_url}/register?invite={token}{return_to}",
      "expiry_days": ORG_INVITATION_EXPIRY_DAYS,
    }

    return await self.send_email("org_invitation", user_email, template_data)

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
    """Warn that a graph instance is near or past its storage limit.

    `usage_percentage` is a percentage (85.3, not 0.853) and the two size
    figures are GB. `instance_status` is "approaching" or "over_limit" — the
    latter switches the email to the red over-limit treatment. `databases`
    itemizes the parent and its subgraphs, which share the instance's limit.
    """
    base_url = self.app_urls.get(app, self.app_urls[_DEFAULT_APP])

    template_data = {
      "user_name": user_name,
      **self._brand(app),
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


ses_service = SESEmailService()
