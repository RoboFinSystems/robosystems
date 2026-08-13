"""Authentication utilities and helpers."""

import hashlib

from fastapi import HTTPException, status

from ...config import env
from ...config.logging import get_logger
from ...config.valkey_registry import ValkeyDatabase, ValkeyURLBuilder
from ...security.password import PasswordSecurity

# Import JWT functions from middleware to avoid duplication

logger = get_logger("robosystems.auth.utils")

# Constants
SSO_TOKEN_EXPIRY_SECONDS = 300  # 5 minutes for better UX
SSO_SESSION_EXPIRY_SECONDS = 30
AVAILABLE_APPS = ["roboledger", "roboinvestor", "robosystems"]


# Configuration
class Config:
  """Configuration management for environment variables."""

  @staticmethod
  def get_valkey_url() -> str:
    # Use auth cache database from registry with authentication in prod/staging
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
  whole credential surface — login, registration, reset, change — not just
  its advertisement in ``/auth/providers``. A login that the UI hides but the
  API still accepts is posture drift, not a posture: it would let a password
  bypass the IdP's MFA and conditional-access policies.
  """
  if not env.PASSWORD_AUTH_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Password authentication is disabled on this deployment",
    )


def require_passkeys_enabled() -> None:
  """Dependency guard for every passkey/MFA endpoint.

  Same posture rule as ``require_password_auth``: the routers mount
  unconditionally (so the posture-drift test table can prove the 403s) and
  this runtime guard is what makes ``PASSKEYS_ENABLED=false`` a real off
  switch rather than an advertisement.
  """
  if not env.PASSKEYS_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Passkey authentication is disabled on this deployment",
    )


# Redis clients are now imported from middleware.auth.jwt


def hash_password(password: str) -> str:
  """Hash a password using secure bcrypt settings."""
  return PasswordSecurity.hash_password(password)


def verify_password(password: str, hashed: str) -> bool:
  """Verify a password against its hash."""
  return PasswordSecurity.verify_password(password, hashed)


# JWT token functions are now imported from middleware.auth.jwt


def is_safe_relative_path(path: str) -> bool:
  """True when path is a same-app relative path.

  Rejects scheme-relative URLs ("//host"), absolute URLs, backslashes
  (browsers normalize "/\\host" to "//host"), and ASCII tab/newline
  characters (browsers strip those before URL parsing, so "/<TAB>/host"
  would normalize to "//host").
  """
  if not path.startswith("/") or path.startswith("//"):
    return False
  return not any(c in path for c in "\\\t\n\r")


def detect_app_source(request) -> str:
  """Detect the calling app from the Referer and Origin headers.

  Returns one of `roboledger`, `roboinvestor`, or `robosystems`.
  """
  # Check referer header
  referer = request.headers.get("referer", "").lower()

  if "roboinvestor" in referer:
    return "roboinvestor"
  elif "robosystems" in referer:
    return "robosystems"
  elif "roboledger" in referer:
    return "roboledger"

  # Check origin header
  origin = request.headers.get("origin", "").lower()

  if "roboinvestor" in origin:
    return "roboinvestor"
  elif "robosystems" in origin:
    return "robosystems"
  elif "roboledger" in origin:
    return "roboledger"

  # Check custom header if frontend sets it
  app_header = request.headers.get("x-app-source", "").lower()
  if app_header in ["roboledger", "roboinvestor", "robosystems"]:
    return app_header

  # Default to robosystems (main hub)
  return "robosystems"


def get_token_hash(token: str) -> str:
  """Generate a hash of the token for revocation list storage.

  We hash the token to avoid storing the actual JWT in Redis.
  """
  return hashlib.sha256(token.encode()).hexdigest()


# JWT verification functions are now imported from middleware.auth.jwt
