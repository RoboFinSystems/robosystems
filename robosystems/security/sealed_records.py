"""HMAC seals for records the platform keeps in Valkey.

Graph instances hold the Valkey token but not ``JWT_SECRET_KEY``. A record
sealed under a key derived from that secret cannot be written by anything that
holds only the token, so a reader that checks the seal trusts only what the
platform wrote.
"""

import hashlib
import hmac
import json
from typing import Any

SEAL_FIELD = "seal"


def _key(purpose: str) -> bytes:
  from robosystems.config import env

  secret = env.JWT_SECRET_KEY
  if not secret:
    raise RuntimeError("JWT_SECRET_KEY is required to seal Valkey records")
  return hmac.new(
    secret.encode(), f"robosystems:{purpose}:v1".encode(), hashlib.sha256
  ).digest()


def _mac(purpose: str, record: dict[str, Any]) -> str:
  body = {k: v for k, v in record.items() if k != SEAL_FIELD}
  canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
  return hmac.new(_key(purpose), canonical.encode(), hashlib.sha256).hexdigest()


def seal(purpose: str, record: dict[str, Any]) -> dict[str, Any]:
  """The record with its seal for ``purpose`` (replacing any earlier one)."""
  body = {k: v for k, v in record.items() if k != SEAL_FIELD}
  return {**body, SEAL_FIELD: _mac(purpose, body)}


def is_sealed(purpose: str, record: Any) -> bool:
  """Whether ``record`` carries a valid seal for ``purpose``."""
  if not isinstance(record, dict):
    return False
  mark = record.get(SEAL_FIELD)
  return isinstance(mark, str) and hmac.compare_digest(mark, _mac(purpose, record))


def unseal(purpose: str, record: Any) -> dict[str, Any] | None:
  """The record without its seal, or None when the seal is missing or wrong."""
  if not is_sealed(purpose, record):
    return None
  return {k: v for k, v in record.items() if k != SEAL_FIELD}
