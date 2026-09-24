"""Device fingerprinting for token binding security."""

import hashlib
import json
from typing import Any

from fastapi import Request


def extract_device_fingerprint(request: Request) -> dict[str, Any]:
  """Extract device fingerprint components from a request.

  The client IP is deliberately excluded: it changes too often to bind to.
  """
  fingerprint = {
    "user_agent": request.headers.get("user-agent", ""),
    "accept_language": request.headers.get("accept-language", ""),
    "accept_encoding": request.headers.get("accept-encoding", ""),
    "sec_ch_ua": request.headers.get("sec-ch-ua", ""),
    "sec_ch_ua_platform": request.headers.get("sec-ch-ua-platform", ""),
  }

  return fingerprint


def create_device_hash(fingerprint: dict[str, Any]) -> str:
  fingerprint_json = json.dumps(fingerprint, sort_keys=True)
  return hashlib.sha256(fingerprint_json.encode()).hexdigest()


def validate_device_fingerprint(
  stored_hash: str, current_fingerprint: dict[str, Any]
) -> bool:
  """Check whether the current request matches a stored device fingerprint."""
  current_hash = create_device_hash(current_fingerprint)
  return stored_hash == current_hash


def is_fingerprint_suspicious(
  stored_fingerprint: dict[str, Any], current_fingerprint: dict[str, Any]
) -> tuple[bool, list[str]]:
  """Check whether fingerprint changes indicate potential token theft.

  Returns ``(is_suspicious, changes)``. Language and encoding shifts are
  reported as changes but are not on their own suspicious.
  """
  changes = []
  suspicious = False

  if stored_fingerprint.get("user_agent") != current_fingerprint.get("user_agent"):
    changes.append("user_agent_changed")
    suspicious = True

  if stored_fingerprint.get("sec_ch_ua") != current_fingerprint.get("sec_ch_ua"):
    changes.append("browser_hints_changed")
    suspicious = True

  if stored_fingerprint.get("accept_language") != current_fingerprint.get(
    "accept_language"
  ):
    changes.append("language_changed")

  if stored_fingerprint.get("accept_encoding") != current_fingerprint.get(
    "accept_encoding"
  ):
    changes.append("encoding_changed")

  return suspicious, changes
