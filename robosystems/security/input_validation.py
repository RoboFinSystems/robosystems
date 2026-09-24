"""Input validation and sanitization utilities."""

import html
import re


def sanitize_string(value: str, max_length: int = 1000) -> str:
  """Truncate to ``max_length``, HTML-escape, then strip angle brackets,
  quotes, NULs and newlines."""
  value = value[:max_length]
  value = html.escape(value)
  value = re.sub(r'[<>"\'\0\r\n]', "", value)

  return value.strip()


def validate_email(email: str) -> bool:
  """Validate email format and enforce the 254-character RFC limit."""
  pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
  return bool(re.match(pattern, email)) and len(email) <= 254


def sanitize_user_input(data: dict) -> dict:
  """Sanitize every string value in a dictionary, recursing into nested dicts."""
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


def validate_username(username: str) -> bool:
  """Alphanumeric, underscore and dash, 3-30 characters."""
  pattern = r"^[a-zA-Z0-9_-]{3,30}$"
  return bool(re.match(pattern, username))


def sanitize_sql_identifier(identifier: str) -> str:
  """Reduce a SQL identifier to alphanumerics and underscores, not leading
  with a digit."""
  sanitized = re.sub(r"[^a-zA-Z0-9_]", "", identifier)

  if sanitized and sanitized[0].isdigit():
    sanitized = "_" + sanitized

  return sanitized[:63]  # PostgreSQL identifier limit


def validate_uuid(value: str) -> bool:
  """Validate UUID format (case-insensitive)."""
  uuid_pattern = r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"
  return bool(re.match(uuid_pattern, value.lower()))


def sanitize_url(url: str) -> str | None:
  """Validate an http(s) URL, returning None for anything else."""
  url_pattern = r"^https?://[a-zA-Z0-9.-]+(\.[a-zA-Z]{2,})+(/.*)?$"

  if re.match(url_pattern, url):
    if url.lower().startswith(("javascript:", "data:", "vbscript:")):
      return None
    return url

  return None


def strip_html_tags(text: str) -> str:
  """Remove all HTML tags from text."""
  clean = re.compile("<.*?>")
  return re.sub(clean, "", text)
