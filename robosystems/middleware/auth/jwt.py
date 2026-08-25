"""JWT minting, verification, and revocation.

Lives here rather than in the routers so middleware can import it without a
circular dependency.

Two token kinds share this secret: session bearers (``type: "access"``, with a
``jti`` so they can be revoked) and single-use SSO handoff tokens
(``sso: true``, no ``jti``). `verify_jwt_claims` accepts only the former.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
import redis
from fastapi import HTTPException, status

from ...config import env
from ...config.constants import (
  JWT_EXPIRY_HOURS,
  JWT_REVOCATION_GRACE_SECONDS,
  JWT_REVOCATION_KEY_PREFIX,
)
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
    """Return the signing secret, raising 500 when it is unset."""
    secret = env.JWT_SECRET_KEY
    if not secret:
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="JWT secret key is not set",
      )
    return secret


def get_redis_client():
  """Synchronous Redis client for token revocation tracking."""
  return create_redis_client(ValkeyDatabase.AUTH)


async def get_async_redis_client():
  """Async Redis client for token revocation tracking."""
  return create_async_redis_client(ValkeyDatabase.AUTH)


def is_jwt_token_revoked(token: str) -> bool:
  """Whether a token's `jti` is in the revocation list.

  Fails closed: an unreadable token or an unreachable Redis both count as
  revoked, so a revocation list outage cannot be used to keep using a
  revoked token.

  Tokens revoked with reason ``session_refresh`` stay usable for a short
  grace period, so requests already in flight when a session refreshes
  don't fail.
  """
  REFRESH_GRACE_PERIOD = timedelta(seconds=JWT_REVOCATION_GRACE_SECONDS)

  try:
    secret_key = JWTConfig.get_jwt_secret()
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
      # A token with no jti has nothing to look up in the revocation list.
      return False

    try:
      redis_client = get_redis_client()
      revocation_key = f"{JWT_REVOCATION_KEY_PREFIX}{jti}"

      revocation_data = redis_client.hgetall(revocation_key)

      if not revocation_data:
        return False  # Not revoked

      # `decode_responses=True`, so these keys are str, not bytes.
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
      logger.error(f"Redis connection error during token revocation check: {conn_err}")
      return True

  except jwt.InvalidTokenError as jwt_err:
    logger.warning(f"Invalid JWT token during revocation check: {jwt_err}")
    return True
  except Exception as e:
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


def is_session_access_token(payload: dict[str, Any]) -> bool:
  """Whether this JWT is a session bearer, not a purpose-scoped token.

  Session tokens carry ``type: "access"``. Tokens with no ``type`` at all are
  grandfathered (pre-type-claim sessions). SSO handoff tokens (``sso: true``)
  and any other ``type`` (``mfa``, ``stream``, …) are not session bearers.
  Shared by ``verify_jwt_claims`` and the ``/refresh`` grace path so a
  purpose-scoped token cannot mint a session after it expires.
  """
  if payload.get("sso"):
    return False
  token_type = payload.get("type")
  return token_type is None or token_type == "access"


def verify_jwt_claims(
  token: str, device_fingerprint: dict[str, Any] | None = None
) -> tuple[str, int] | None:
  """Verify a JWT token's claims and return (user_id, session_version) if valid.

  Validates: signature, expiry, issuer, audience, jti revocation, and
  optional device fingerprint binding.

  Does NOT compare session_version against the User row — that needs DB
  access and is the caller's job. Compare the returned session_version
  against ``User.session_version`` before treating the token as
  authenticated; the auth dependency layer does this via
  ``_get_user_for_verified_jwt``.
  """
  try:
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

    # Single-use SSO handoff tokens (`create_sso_token`: {"sso": true}, no
    # jti, unrevocable) must never authenticate as a session bearer. They are
    # consumed only at /sso-exchange, which decodes them directly rather than
    # through this function. Rejecting every non-`access` type also stops a
    # future purpose-scoped token (an SSE stream token, say) from being
    # replayed as a bearer. Tokens with no `type` claim at all are accepted.
    if not is_session_access_token(payload):
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
          "token_type": "sso" if payload.get("sso") else payload.get("type"),
        },
        risk_level="high",
      )
      return None

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
  """Mint a session bearer token, optionally bound to a device fingerprint.

  The token carries the user's current ``session_version``; bumping that
  column invalidates every token issued before it. When ``session`` is
  passed the caller owns its lifecycle, otherwise a short-lived one is
  opened to read the version.
  """
  secret_key = JWTConfig.get_jwt_secret()

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

  if device_fingerprint:
    payload["device_hash"] = create_device_hash(device_fingerprint)
  return jwt.encode(payload, secret_key, algorithm="HS256")


def create_sso_token(user_id: str, session: Any = None) -> tuple[str, str]:
  """Mint a short-lived SSO handoff token for cross-app authentication.

  Returns `(token, token_id)`; `token_id` is what /sso-exchange tracks to
  enforce single use. These tokens have no `jti` and cannot be revoked,
  which is why `verify_jwt_claims` refuses them as session bearers.
  """
  secret_key = JWTConfig.get_jwt_secret()

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


MFA_TOKEN_EXPIRY_SECONDS = 300


def create_mfa_token(
  user_id: str, purpose: str, session: Any = None
) -> tuple[str, str]:
  """Mint a short-lived MFA challenge token; returns ``(token, jti)``.

  Issued by login after password verification when the flow cannot complete
  in one step: ``purpose="login"`` authorizes the second-factor handshake
  (/mfa/options + /mfa/verify), ``purpose="enroll"`` authorizes the forced
  first-enrollment ceremony. The purposes are mutually exclusive by claim
  check, and ``type: "mfa"`` means ``verify_jwt_claims`` refuses the token
  as a session bearer. Carries ``session_version`` so redemption can re-check
  against the live user row, mirroring the SSO completion path.
  """
  if purpose not in ("login", "enroll"):
    raise ValueError(f"Invalid MFA token purpose: {purpose}")
  secret_key = JWTConfig.get_jwt_secret()

  jti = str(uuid.uuid4())

  payload = {
    "user_id": user_id,
    "jti": jti,
    "type": "mfa",
    "purpose": purpose,
    "session_version": _get_user_session_version(user_id, session=session) or 0,
    "exp": datetime.now(UTC) + timedelta(seconds=MFA_TOKEN_EXPIRY_SECONDS),
    "iat": datetime.now(UTC),
    "iss": env.JWT_ISSUER,
    "aud": env.JWT_AUDIENCE,
  }
  return jwt.encode(payload, secret_key, algorithm="HS256"), jti


def decode_mfa_token(token: str, expected_purpose: str) -> dict[str, Any] | None:
  """Decode and validate an MFA challenge token; None on any mismatch.

  Direct decode (the /sso-exchange pattern) rather than ``verify_jwt_claims``
  — these tokens are deliberately refused there. Validates signature, expiry,
  issuer, audience, ``type: "mfa"``, and the purpose scope.
  """
  try:
    payload = jwt.decode(
      token,
      JWTConfig.get_jwt_secret(),
      algorithms=["HS256"],
      issuer=env.JWT_ISSUER,
      audience=env.JWT_AUDIENCE,
    )
  except jwt.InvalidTokenError:
    return None
  if payload.get("type") != "mfa" or payload.get("purpose") != expected_purpose:
    return None
  if not payload.get("user_id") or not payload.get("jti"):
    return None
  return payload


def revoke_jwt_token(token: str, reason: str = "user_logout") -> bool:
  """Add a token's `jti` to the revocation list until its natural expiry.

  `reason` is stored alongside; ``session_refresh`` triggers the grace
  period in `is_jwt_token_revoked`.
  """
  try:
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

    exp_datetime = datetime.fromtimestamp(exp, tz=UTC)
    ttl_seconds = int((exp_datetime - datetime.now(UTC)).total_seconds())

    if ttl_seconds <= 0:
      logger.info("Token already expired, no need to revoke")
      return True

    redis_client = get_redis_client()
    revocation_key = f"{JWT_REVOCATION_KEY_PREFIX}{jti}"
    revocation_data = {
      "revoked_at": datetime.now(UTC).isoformat(),
      "reason": reason,
      "user_id": user_id,
    }

    pipe = redis_client.pipeline()
    pipe.hset(revocation_key, mapping=revocation_data)
    pipe.expire(revocation_key, ttl_seconds)
    pipe.execute()

    logger.info(f"Token revoked for user {user_id}: {reason}")
    return True

  except Exception as e:
    logger.error(f"Failed to revoke token: {e}")
    return False
