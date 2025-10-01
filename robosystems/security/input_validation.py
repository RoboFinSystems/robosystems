"""Input validation and sanitization utilities."""

import re
import html
from typing import Optional


def sanitize_string(value: str, max_length: int = 1000) -> str:
  """Sanitize string input to prevent XSS and injection attacks.

  Args:
      value: String to sanitize
      max_length: Maximum allowed length

  Returns:
      Sanitized string
  """
  # Truncate to max length
  value = value[:max_length]

  # HTML escape
  value = html.escape(value)

  # Remove potentially dangerous characters
  value = re.sub(r'[<>"\'\0\r\n]', "", value)

  return value.strip()


def validate_email(email: str) -> bool:
  """Validate email format.

  Args:
      email: Email address to validate

  Returns:
      True if email is valid, False otherwise
  """
  pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
  return bool(re.match(pattern, email)) and len(email) <= 254


def sanitize_user_input(data: dict) -> dict:
  """Sanitize all string values in a dictionary.

  Args:
      data: Dictionary containing user input

  Returns:
      Dictionary with sanitized values
  """
  sanitized = {}
  for key, value in data.items():
    if isinstance(value, str):
      sanitized[key] = sanitize_string(value)
    elif isinstance(value, dict):
      sanitized[key] = sanitize_user_input(value)
    elif isinstance(value, list):
      sanitized[key] = [
        sanitize_string(item) if isinstance(item, str) else item for item in value
      ]
    else:
      sanitized[key] = value
  return sanitized


def validate_password_strength(password: str) -> tuple[bool, list[str]]:
  """Validate password meets security requirements.

  Args:
      password: Password to validate

  Returns:
      Tuple of (is_valid, list_of_issues)
  """
  issues = []

  if len(password) < 8:
    issues.append("Password must be at least 8 characters long")

  if not re.search(r"[a-z]", password):
    issues.append("Password must contain at least one lowercase letter")

  if not re.search(r"[A-Z]", password):
    issues.append("Password must contain at least one uppercase letter")

  if not re.search(r"[0-9]", password):
    issues.append("Password must contain at least one digit")

  if not re.search(r"[!@#$%^&*()_+\-=\[\]{};:,.<>?]", password):
    issues.append("Password must contain at least one special character")

  return len(issues) == 0, issues


def validate_username(username: str) -> bool:
  """Validate username format.

  Args:
      username: Username to validate

  Returns:
      True if username is valid, False otherwise
  """
  # Allow alphanumeric, underscore, dash, 3-30 characters
  pattern = r"^[a-zA-Z0-9_-]{3,30}$"
  return bool(re.match(pattern, username))


def sanitize_sql_identifier(identifier: str) -> str:
  """Sanitize SQL identifiers (table names, column names).

  Args:
      identifier: SQL identifier to sanitize

  Returns:
      Sanitized identifier
  """
  # Allow only alphanumeric and underscore
  sanitized = re.sub(r"[^a-zA-Z0-9_]", "", identifier)

  # Ensure it doesn't start with a number
  if sanitized and sanitized[0].isdigit():
    sanitized = "_" + sanitized

  return sanitized[:63]  # PostgreSQL identifier limit


def validate_uuid(value: str) -> bool:
  """Validate UUID format.

  Args:
      value: String to validate as UUID

  Returns:
      True if valid UUID, False otherwise
  """
  uuid_pattern = r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"
  return bool(re.match(uuid_pattern, value.lower()))


def sanitize_url(url: str) -> Optional[str]:
  """Sanitize and validate URL.

  Args:
      url: URL to sanitize

  Returns:
      Sanitized URL or None if invalid
  """
  # Basic URL validation
  url_pattern = r"^https?://[a-zA-Z0-9.-]+(\.[a-zA-Z]{2,})+(/.*)?$"

  if re.match(url_pattern, url):
    # Remove any javascript: or data: protocols
    if url.lower().startswith(("javascript:", "data:", "vbscript:")):
      return None
    return url

  return None


def strip_html_tags(text: str) -> str:
  """Remove all HTML tags from text.

  Args:
      text: Text containing HTML

  Returns:
      Text with HTML tags removed
  """
  # Remove HTML tags
  clean = re.compile("<.*?>")
  return re.sub(clean, "", text)


def validate_api_key(api_key: str) -> bool:
  """Validate API key format.

  Args:
      api_key: API key to validate

  Returns:
      True if valid format, False otherwise
  """
  # API keys should match expected format
  # Example: rsk_1234567890abcdef... (64+ chars)
  pattern = r"^rsk_[a-zA-Z0-9]{60,}$"
  return bool(re.match(pattern, api_key))
