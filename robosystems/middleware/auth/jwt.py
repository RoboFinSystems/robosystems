"""JWT token utilities.

This module provides JWT-related functionality that is shared between
routers and middleware to avoid circular dependencies.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple

import jwt
import redis
from fastapi import HTTPException, status

from ...config import env
from ...config.valkey_registry import ValkeyDatabase
from ...config.valkey_registry import create_redis_client, create_async_redis_client
from ...config.logging import get_logger
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
  return create_redis_client(ValkeyDatabase.AUTH_CACHE)


async def get_async_redis_client():
  """Get async Redis client for token tracking with proper ElastiCache support."""
  return create_async_redis_client(ValkeyDatabase.AUTH_CACHE)


def is_jwt_token_revoked(token: str) -> bool:
  """Check if a JWT token has been revoked.

  Args:
    token: The JWT token to check

  Returns:
    True if token is revoked, False otherwise
  """
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
      return bool(redis_client.exists(revocation_key))
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


def verify_jwt_token(
  token: str, device_fingerprint: Optional[Dict[str, Any]] = None
) -> Optional[str]:
  """Verify a JWT token and return the user_id if valid.

  Args:
    token: The JWT token to verify
    device_fingerprint: Optional device fingerprint to validate against token

  Returns:
    The user_id if token is valid, None otherwise
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

    return payload.get("user_id")

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
  user_id: str, device_fingerprint: Optional[Dict[str, Any]] = None
) -> str:
  """Create a JWT token for authentication with optional device binding.

  Args:
    user_id: The user ID to encode in the token
    device_fingerprint: Optional device fingerprint for token binding

  Returns:
    The encoded JWT token
  """
  secret_key = JWTConfig.get_jwt_secret()

  # Generate unique JTI (JWT ID) for revocation tracking
  jti = str(uuid.uuid4())

  payload = {
    "user_id": user_id,
    "jti": jti,  # JWT ID for revocation tracking
    "exp": datetime.now(timezone.utc) + timedelta(hours=env.JWT_EXPIRY_HOURS),
    "iat": datetime.now(timezone.utc),
    "iss": env.JWT_ISSUER,
    "aud": env.JWT_AUDIENCE,
  }

  # Add device fingerprint hash for token binding if provided
  if device_fingerprint:
    payload["device_hash"] = create_device_hash(device_fingerprint)
  return jwt.encode(payload, secret_key, algorithm="HS256")


def create_sso_token(user_id: str) -> Tuple[str, str]:
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
    "exp": datetime.now(timezone.utc)
    + timedelta(seconds=300),  # 5 minutes for better UX
    "iat": datetime.now(timezone.utc),
    "iss": "api.robosystems.ai",  # Issuer claim
    "aud": ["robosystems.ai", "roboledger.ai", "roboinvestor.ai"],  # Audience claim
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
    exp_datetime = datetime.fromtimestamp(exp, tz=timezone.utc)
    ttl_seconds = int((exp_datetime - datetime.now(timezone.utc)).total_seconds())

    if ttl_seconds <= 0:
      logger.info("Token already expired, no need to revoke")
      return True

    # Store in revocation list with TTL
    redis_client = get_redis_client()
    revocation_key = f"revoked_jwt:{jti}"
    revocation_data = {
      "revoked_at": datetime.now(timezone.utc).isoformat(),
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
