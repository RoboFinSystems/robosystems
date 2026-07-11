"""JWT token utilities.

This module provides JWT-related functionality that is shared between
routers and middleware to avoid circular dependencies.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
import redis
from fastapi import HTTPException, status

from ...config import env
from ...config.constants import JWT_EXPIRY_HOURS, JWT_REVOCATION_GRACE_SECONDS
from ...config.logging import get_logger
from ...config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
  create_redis_client,
)
from ...security.device_fingerprinting import create_device_hash

logger = get_logger("robosystems.auth.jwt")


class JWTConfig:
  """JWT configuration management."""

  @staticmethod
  def get_jwt_secret() -> str:
    """Get JWT secret key."""
    secret = env.JWT_SECRET_KEY
    if not secret:
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="JWT secret key is not set",
      )
    return secret


def get_redis_client():
  """Get synchronous Redis client for token tracking with proper ElastiCache support."""
  return create_redis_client(ValkeyDatabase.AUTH)


async def get_async_redis_client():
  """Get async Redis client for token tracking with proper ElastiCache support."""
  return create_async_redis_client(ValkeyDatabase.AUTH)


def is_jwt_token_revoked(token: str) -> bool:
  """Check if a JWT token has been revoked.

  Args:
    token: The JWT token to check

  Returns:
    True if token is revoked, False otherwise
  """
  # Grace period for tokens revoked during session refresh
  # This handles race conditions where /me and /refresh are called simultaneously
  REFRESH_GRACE_PERIOD = timedelta(seconds=JWT_REVOCATION_GRACE_SECONDS)

  try:
    secret_key = JWTConfig.get_jwt_secret()
    # Decode without verification to get JTI
    payload = jwt.decode(
      token,
      secret_key,
      algorithms=["HS256"],
      options={
        "verify_exp": False,
        "verify_aud": False,
        "verify_iss": False,
      },
    )

    jti = payload.get("jti")
    if not jti:
      # Old tokens without JTI can't be revoked
      return False

    try:
      redis_client = get_redis_client()
      revocation_key = f"revoked_jwt:{jti}"

      # Get revocation data to check reason and timing
      revocation_data = redis_client.hgetall(revocation_key)

      if not revocation_data:
        return False  # Not revoked

      # Check for grace period on session_refresh revocations
      # This allows in-flight requests to complete during token refresh
      # Note: decode_responses=True is used, so keys are strings not bytes
      reason = revocation_data.get("reason", "")
      if reason == "session_refresh":
        revoked_at_str = revocation_data.get("revoked_at", "")
        if revoked_at_str:
          revoked_at = datetime.fromisoformat(revoked_at_str)
          if datetime.now(UTC) - revoked_at < REFRESH_GRACE_PERIOD:
            logger.debug(
              f"Token {jti[:8]}... in refresh grace period, allowing request"
            )
            return False

      return True  # Revoked (or grace period expired)

    except redis.ConnectionError as conn_err:
      # Log connection errors explicitly
      logger.error(f"Redis connection error during token revocation check: {conn_err}")
      # Fail closed during Redis connection issues
      return True

  except jwt.InvalidTokenError as jwt_err:
    # Fail closed for invalid JWT tokens
    logger.warning(f"Invalid JWT token during revocation check: {jwt_err}")
    return True
  except Exception as e:
    # Unexpected errors: log and fail closed
    logger.error(f"Unexpected error checking token revocation: {e}")
    return True


def _get_user_session_version(user_id: str, session: Any = None) -> int | None:
  """Look up a user's current session_version.

  If ``session`` is provided, queries on it without closing — caller owns the
  lifecycle. Otherwise opens a short-lived session.

  Returns None if the user does not exist (caller treats this as auth failure).
  """
  from ...models.core import User

  if session is not None:
    user = session.query(User).filter(User.id == user_id).first()
    if not user:
      return None
    return int(user.session_version or 0)

  from ...database import SessionFactory

  sess = SessionFactory()
  try:
    user = sess.query(User).filter(User.id == user_id).first()
    if not user:
      return None
    return int(user.session_version or 0)
  finally:
    sess.close()


def verify_jwt_claims(
  token: str, device_fingerprint: dict[str, Any] | None = None
) -> tuple[str, int] | None:
  """Verify a JWT token's claims and return (user_id, session_version) if valid.

  Validates: signature, expiry, issuer, audience, jti revocation, and
  optional device fingerprint binding.

  Does NOT compare session_version against the User row — that requires DB
  access and is the caller's responsibility. Caller must compare the returned
  session_version against ``User.session_version`` before treating the token
  as authenticated. The auth dependency layer handles this via
  ``_get_user_for_verified_jwt``; direct callers must do it explicitly.

  Args:
    token: The JWT token to verify
    device_fingerprint: Optional device fingerprint to validate against token

  Returns:
    (user_id, session_version) if signature-level validation passed, else None
  """
  try:
    # First check if token is revoked
    if is_jwt_token_revoked(token):
      logger.info("JWT token verification failed: token is revoked")
      return None

    secret_key = JWTConfig.get_jwt_secret()
    payload = jwt.decode(
      token,
      secret_key,
      algorithms=["HS256"],
      issuer=env.JWT_ISSUER,
      audience=env.JWT_AUDIENCE,
    )

    # F3: single-use SSO handoff tokens (`create_sso_token`: {"sso": true},
    # no jti, unrevocable) must never authenticate as a full session bearer.
    # They are consumed only at /sso-exchange, which decodes them directly —
    # not through this function. Reject `sso` here, and reject any non-`access`
    # token type so a future purpose-scoped token (e.g. an SSE stream token)
    # can't be replayed as a session bearer. Legacy session tokens minted
    # before the `type` claim existed have no `type` and are grandfathered.
    token_type = payload.get("type")
    if payload.get("sso") or (token_type is not None and token_type != "access"):
      logger.info("JWT token verification failed: non-access token presented as bearer")
      # A signature-valid but wrong-purpose token used as a bearer is a distinct
      # signal (possible replay of a leaked single-use SSO handoff) that the
      # generic AUTH_TOKEN_INVALID at the caller would otherwise bury.
      from ...security import SecurityAuditLogger, SecurityEventType

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        user_id=payload.get("user_id"),
        details={
          "reason": "non_access_token_as_bearer",
          "token_type": "sso" if payload.get("sso") else token_type,
        },
        risk_level="high",
      )
      return None

    # Verify device fingerprint if both token and request fingerprint are available
    if device_fingerprint and payload.get("device_hash"):
      from ...security.device_fingerprinting import create_device_hash

      current_device_hash = create_device_hash(device_fingerprint)
      stored_device_hash = payload.get("device_hash")

      if current_device_hash != stored_device_hash:
        logger.warning(
          f"JWT token verification failed: device fingerprint mismatch for user {payload.get('user_id')}"
        )
        return None

    user_id = payload.get("user_id")
    if not user_id:
      return None

    try:
      token_session_version = int(payload.get("session_version", 0))
    except (TypeError, ValueError):
      logger.info("JWT token verification failed: invalid session_version claim")
      return None

    return user_id, token_session_version

  except jwt.ExpiredSignatureError:
    logger.info("JWT token verification failed: token expired")
    return None
  except (jwt.InvalidTokenError, jwt.InvalidIssuerError, jwt.InvalidAudienceError) as e:
    logger.info(f"JWT token verification failed: {type(e).__name__}")
    return None
  except Exception as e:
    logger.error(f"Unexpected error verifying JWT token: {e}")
    return None


def create_jwt_token(
  user_id: str,
  device_fingerprint: dict[str, Any] | None = None,
  session: Any = None,
) -> str:
  """Create a JWT token for authentication with optional device binding.

  Args:
    user_id: The user ID to encode in the token
    device_fingerprint: Optional device fingerprint for token binding
    session: Optional DB session to read session_version from. If provided,
      the caller owns the lifecycle (no close). Otherwise a short-lived
      session is opened.

  Returns:
    The encoded JWT token
  """
  secret_key = JWTConfig.get_jwt_secret()

  # Generate unique JTI (JWT ID) for revocation tracking
  jti = str(uuid.uuid4())

  payload = {
    "user_id": user_id,
    "jti": jti,  # JWT ID for revocation tracking
    "type": "access",  # Session bearer — distinguishes from single-use `sso` tokens
    "session_version": _get_user_session_version(user_id, session=session) or 0,
    "exp": datetime.now(UTC) + timedelta(hours=JWT_EXPIRY_HOURS),
    "iat": datetime.now(UTC),
    "iss": env.JWT_ISSUER,
    "aud": env.JWT_AUDIENCE,
  }

  # Add device fingerprint hash for token binding if provided
  if device_fingerprint:
    payload["device_hash"] = create_device_hash(device_fingerprint)
  return jwt.encode(payload, secret_key, algorithm="HS256")


def create_sso_token(user_id: str, session: Any = None) -> tuple[str, str]:
  """Create a temporary SSO token for cross-app authentication.

  Args:
    user_id: The user ID to encode in the token

  Returns:
    Tuple of (token, token_id) where token_id is used for single-use tracking
  """
  secret_key = JWTConfig.get_jwt_secret()

  # Generate unique token ID for single-use tracking
  token_id = str(uuid.uuid4())

  payload = {
    "user_id": user_id,
    "sso": True,
    "token_id": token_id,
    "session_version": _get_user_session_version(user_id, session=session) or 0,
    "exp": datetime.now(UTC) + timedelta(seconds=300),  # 5 minutes for better UX
    "iat": datetime.now(UTC),
    "iss": env.JWT_ISSUER,  # Issuer claim - must match env for all environments
    "aud": env.JWT_AUDIENCE,  # Audience claim - must match env for all environments
  }
  token = jwt.encode(payload, secret_key, algorithm="HS256")
  return token, token_id


def revoke_jwt_token(token: str, reason: str = "user_logout") -> bool:
  """Add a JWT token to the revocation list.

  Args:
    token: The JWT token to revoke
    reason: Reason for revocation (for logging/auditing)

  Returns:
    True if token was successfully revoked, False otherwise
  """
  try:
    # First decode to get expiration for TTL
    secret_key = JWTConfig.get_jwt_secret()
    payload = jwt.decode(
      token,
      secret_key,
      algorithms=["HS256"],
      options={
        "verify_exp": False,
        "verify_aud": False,
        "verify_iss": False,
      },  # Don't verify claims for revocation
    )

    jti = payload.get("jti")
    exp = payload.get("exp")
    user_id = payload.get("user_id")

    if not jti or not exp:
      logger.warning("Cannot revoke token: missing jti or exp claim")
      return False

    # Calculate TTL (time until natural expiration)
    exp_datetime = datetime.fromtimestamp(exp, tz=UTC)
    ttl_seconds = int((exp_datetime - datetime.now(UTC)).total_seconds())

    if ttl_seconds <= 0:
      logger.info("Token already expired, no need to revoke")
      return True

    # Store in revocation list with TTL
    redis_client = get_redis_client()
    revocation_key = f"revoked_jwt:{jti}"
    revocation_data = {
      "revoked_at": datetime.now(UTC).isoformat(),
      "reason": reason,
      "user_id": user_id,
    }

    # Use pipeline for atomic operation
    pipe = redis_client.pipeline()
    pipe.hset(revocation_key, mapping=revocation_data)
    pipe.expire(revocation_key, ttl_seconds)
    pipe.execute()

    logger.info(f"Token revoked for user {user_id}: {reason}")
    return True

  except Exception as e:
    logger.error(f"Failed to revoke token: {e}")
    return False
