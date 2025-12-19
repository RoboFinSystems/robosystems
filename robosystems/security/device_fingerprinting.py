"""Device fingerprinting for token binding security."""

import hashlib
import json
from typing import Any

from fastapi import Request


def extract_device_fingerprint(request: Request) -> dict[str, Any]:
  """Extract device fingerprint components from request.

  Args:
      request: FastAPI request object

  Returns:
      Dictionary containing device fingerprint components
  """
  # Get client IP (with proxy support)
  client_ip = None
  if request.client:
    client_ip = request.client.host

  # Check for forwarded IP headers (common with load balancers/proxies)
  forwarded_for = request.headers.get("x-forwarded-for")
  if forwarded_for:
    # Take the first IP in the chain (original client)
    client_ip = forwarded_for.split(",")[0].strip()

  fingerprint = {
    "user_agent": request.headers.get("user-agent", ""),
    "accept_language": request.headers.get("accept-language", ""),
    "accept_encoding": request.headers.get("accept-encoding", ""),
    "client_ip": client_ip,
    # Add more headers that are typically consistent per device
    "sec_ch_ua": request.headers.get("sec-ch-ua", ""),
    "sec_ch_ua_platform": request.headers.get("sec-ch-ua-platform", ""),
  }

  return fingerprint


def create_device_hash(fingerprint: dict[str, Any]) -> str:
  """Create a hash from device fingerprint components.

  Args:
      fingerprint: Device fingerprint dictionary

  Returns:
      SHA256 hash of the fingerprint
  """
  # Sort keys for consistent hashing
  fingerprint_json = json.dumps(fingerprint, sort_keys=True)
  return hashlib.sha256(fingerprint_json.encode()).hexdigest()


def validate_device_fingerprint(
  stored_hash: str, current_fingerprint: dict[str, Any]
) -> bool:
  """Validate if current request matches stored device fingerprint.

  Args:
      stored_hash: Previously stored device fingerprint hash
      current_fingerprint: Current request fingerprint

  Returns:
      True if fingerprints match, False otherwise
  """
  current_hash = create_device_hash(current_fingerprint)
  return stored_hash == current_hash


def is_fingerprint_suspicious(
  stored_fingerprint: dict[str, Any], current_fingerprint: dict[str, Any]
) -> tuple[bool, list[str]]:
  """Check if device fingerprint changes indicate potential token theft.

  Args:
      stored_fingerprint: Originally stored fingerprint
      current_fingerprint: Current request fingerprint

  Returns:
      Tuple of (is_suspicious, list_of_changes)
  """
  changes = []
  suspicious = False

  # Critical changes that indicate token theft
  if stored_fingerprint.get("client_ip") != current_fingerprint.get("client_ip"):
    changes.append("ip_address_changed")
    suspicious = True

  if stored_fingerprint.get("user_agent") != current_fingerprint.get("user_agent"):
    changes.append("user_agent_changed")
    # User agent changes are highly suspicious
    suspicious = True

  # Less critical changes (could be legitimate)
  if stored_fingerprint.get("accept_language") != current_fingerprint.get(
    "accept_language"
  ):
    changes.append("language_changed")

  if stored_fingerprint.get("accept_encoding") != current_fingerprint.get(
    "accept_encoding"
  ):
    changes.append("encoding_changed")

  return suspicious, changes
