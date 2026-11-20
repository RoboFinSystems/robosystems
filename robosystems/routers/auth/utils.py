"""Authentication utilities and helpers."""

import hashlib
import json
from typing import Any

from fastapi import HTTPException, status

from ...config import env
from ...config.logging import get_logger
from ...config.valkey_registry import ValkeyDatabase, ValkeyURLBuilder
from ...security.password import PasswordSecurity
from ...security.sealed_records import seal, unseal

logger = get_logger("robosystems.auth.utils")

SSO_TOKEN_EXPIRY_SECONDS = 300  # 5 minutes for better UX
SSO_SESSION_EXPIRY_SECONDS = 30
AVAILABLE_APPS = ["roboledger", "roboinvestor", "robosystems"]

_SSO_SESSION_SEAL = "sso-session"


def dump_sso_session(session_id: str, data: dict[str, Any]) -> str:
  """The stored form of a handoff session: sealed to its session id."""
  return json.dumps(seal(_SSO_SESSION_SEAL, {**data, "session_id": session_id}))


def load_sso_session(session_id: str, raw: str) -> dict[str, Any] | None:
  """A stored handoff session, or None unless the platform sealed it for this
  session id. Raises ``json.JSONDecodeError`` on a value that is not JSON."""
  record = unseal(_SSO_SESSION_SEAL, json.loads(raw))
  if record is None or record.get("session_id") != session_id:
    return None
  return record


class Config:
  """Configuration management for environment variables."""

  @staticmethod
  def get_valkey_url() -> str:
    return ValkeyURLBuilder.build_authenticated_url(ValkeyDatabase.AUTH)

  @staticmethod
  def get_jwt_secret() -> str:
    secret = env.JWT_SECRET_KEY
    if not secret:
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="JWT secret key is not set",
      )
    return secret

  @staticmethod
  def get_app_urls() -> dict[str, str]:
    return {
      "roboledger": env.ROBOLEDGER_URL,
      "roboinvestor": env.ROBOINVESTOR_URL,
      "robosystems": env.ROBOSYSTEMS_URL,
    }


def require_password_auth() -> None:
  """Dependency guard for every password-credential endpoint.

  ``PASSWORD_AUTH_ENABLED=false`` (SSO-primary deployments) must disable the
  whole credential surface, not just its advertisement in ``/auth/providers``:
  the IdP is the authority.
  """
  if not env.PASSWORD_AUTH_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Password authentication is disabled on this deployment",
    )


def require_passkeys_enabled() -> None:
  """Dependency guard for every passkey/MFA endpoint.

  The routers mount unconditionally; this guard is what makes
  ``PASSKEYS_ENABLED=false`` a real off switch.
  """
  if not env.PASSKEYS_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Passkey authentication is disabled on this deployment",
    )


def may_issue_session_without_login(session, user) -> bool:
  """Whether a token-redemption flow (reset, verify) may mint a session itself.

  Not when login would demand a passkey step: those users sign in normally.
  """
  if not user.is_active:
    return False
  if not env.PASSKEYS_ENABLED:
    return True

  from ...models.core import UserPasskey
  from ...operations.passkeys import user_requires_mfa_enrollment

  if UserPasskey.count_for_user(str(user.id), session) > 0:
    return False
  return not user_requires_mfa_enrollment(session, user)


def hash_password(password: str) -> str:
  """Hash a password using secure bcrypt settings."""
  return PasswordSecurity.hash_password(password)


def verify_password(password: str, hashed: str) -> bool:
  """Verify a password against its hash."""
  return PasswordSecurity.verify_password(password, hashed)


async def hash_password_async(password: str) -> str:
  """hash_password off the event loop — bcrypt at cost 14 blocks ~0.5-1 s."""
  return await PasswordSecurity.hash_password_async(password)


async def verify_password_async(password: str, hashed: str) -> bool:
  """verify_password off the event loop; see hash_password_async."""
  return await PasswordSecurity.verify_password_async(password, hashed)


def is_safe_relative_path(path: str) -> bool:
  """True when path is a same-app relative path.

  Rejects "//host", absolute URLs, backslashes, and tab/newline characters,
  all of which browsers can normalize into "//host".
  """
  if not path.startswith("/") or path.startswith("//"):
    return False
  return not any(c in path for c in "\\\t\n\r")


def detect_app_source(request) -> str:
  """Calling app from Referer, then Origin, then X-App-Source; else robosystems."""
  referer = request.headers.get("referer", "").lower()

  if "roboinvestor" in referer:
    return "roboinvestor"
  elif "robosystems" in referer:
    return "robosystems"
  elif "roboledger" in referer:
    return "roboledger"

  origin = request.headers.get("origin", "").lower()

  if "roboinvestor" in origin:
    return "roboinvestor"
  elif "robosystems" in origin:
    return "robosystems"
  elif "roboledger" in origin:
    return "roboledger"

  app_header = request.headers.get("x-app-source", "").lower()
  if app_header in ["roboledger", "roboinvestor", "robosystems"]:
    return app_header

  return "robosystems"


def get_token_hash(token: str) -> str:
  """SHA-256 of the token, so the revocation list never stores the JWT itself."""
  return hashlib.sha256(token.encode()).hexdigest()
