"""Single Sign-On (SSO) endpoints."""

import json
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
import redis
from sqlalchemy.orm import Session
from fastapi import (
  APIRouter,
  Cookie,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)

from ...models.iam import User
from ...models.api.auth import (
  AuthResponse,
  SSOTokenResponse,
  SSOExchangeRequest,
  SSOExchangeResponse,
  SSOCompleteRequest,
)
from ...models.api.common import ErrorResponse
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.rate_limits import sso_rate_limit_dependency
from ...middleware.auth.distributed_lock import get_sso_lock_manager
from ...config import env

from ...security import SecurityAuditLogger, SecurityEventType

from .utils import (
  Config,
  SSO_TOKEN_EXPIRY_SECONDS,
  SSO_SESSION_EXPIRY_SECONDS,
  AVAILABLE_APPS,
)
from ...middleware.auth.jwt import (
  create_jwt_token,
  create_sso_token,
  verify_jwt_token,
  get_async_redis_client,
)

# Create router for SSO endpoints
router = APIRouter()


@router.post(
  "/sso-token",
  response_model=SSOTokenResponse,
  status_code=status.HTTP_200_OK,
  summary="Generate SSO Token",
  description="Generate a temporary SSO token for cross-app authentication.",
  operation_id="generateSSOToken",
  responses={
    401: {"model": ErrorResponse, "description": "Not authenticated"},
  },
)
async def generate_sso_token(
  request: Request,
  auth_token: Optional[str] = Cookie(
    None, alias="auth-token"
  ),  # Backward compatibility
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(sso_rate_limit_dependency),
) -> SSOTokenResponse:
  """
  Generate a temporary SSO token for cross-app authentication.

  Args:
      auth_token: JWT auth token from HTTP-only cookie (deprecated, for backward compatibility)

  Returns:
      SSOTokenResponse: Temporary token for cross-app authentication

  Raises:
      HTTPException: If not authenticated or token is invalid
  """
  try:
    # Extract JWT token from Authorization header (doesn't show in OpenAPI params) or fall back to cookie
    authorization = request.headers.get("authorization")
    jwt_token = None
    if authorization and authorization.startswith("Bearer "):
      jwt_token = authorization[7:]  # Remove "Bearer " prefix
    elif auth_token:
      jwt_token = auth_token  # Fallback to cookie for backward compatibility

    if not jwt_token:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Verify current JWT token
    user_id = verify_jwt_token(jwt_token)
    if not user_id:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Get user
    user = User.get_by_id(user_id, session)
    if not user or not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
      )

    # Create temporary SSO token
    sso_token, token_id = create_sso_token(user.id)
    expires_at = datetime.now(timezone.utc) + timedelta(
      seconds=SSO_TOKEN_EXPIRY_SECONDS
    )

    # Store token ID in Valkey for single-use tracking with distributed locking
    try:
      redis_client = await get_async_redis_client()

      # Use distributed lock to prevent race conditions during token creation
      lock_manager = get_sso_lock_manager()
      if lock_manager:
        async with lock_manager.lock_sso_token(token_id, "token_creation"):
          # Atomic token storage
          existing_token = await redis_client.get(f"sso_token:{token_id}")
          if existing_token:
            # Token ID collision (very unlikely with UUID4)
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
              details={
                "action": "sso_token_id_collision",
                "token_id": token_id[:8] + "...",
                "user_id": user.id,
              },
              risk_level="high",
            )
            raise HTTPException(
              status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail="Token generation conflict",
            )

          await redis_client.setex(
            f"sso_token:{token_id}",
            SSO_TOKEN_EXPIRY_SECONDS,
            user.id,
          )
      else:
        # Fallback without locking if lock manager unavailable
        logger.warning(
          "SSO lock manager unavailable, proceeding without distributed locking"
        )
        await redis_client.setex(
          f"sso_token:{token_id}",
          SSO_TOKEN_EXPIRY_SECONDS,
          user.id,
        )

    except redis.RedisError as e:
      logger.error(f"Failed to store SSO token in Redis: {str(e)}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "sso_token_storage_failed",
          "error": str(e),
          "user_id": user.id,
        },
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="SSO token generation failed",
      )

    # Use available apps constant
    available_apps = AVAILABLE_APPS

    return SSOTokenResponse(
      token=sso_token,
      expires_at=expires_at,
      apps=available_apps,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"SSO token generation error: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="SSO token generation failed",
    )


@router.post(
  "/sso-exchange",
  response_model=SSOExchangeResponse,
  status_code=status.HTTP_200_OK,
  summary="SSO Token Exchange",
  description="Exchange SSO token for secure session handoff to target application.",
  operation_id="ssoTokenExchange",
  responses={
    401: {"model": ErrorResponse, "description": "Invalid SSO token"},
    400: {"model": ErrorResponse, "description": "Invalid request data"},
  },
)
async def sso_token_exchange(
  request: SSOExchangeRequest,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(sso_rate_limit_dependency),
) -> SSOExchangeResponse:
  """
  Exchange SSO token for secure session handoff.

  This endpoint provides a more secure alternative to URL-based token passing.
  It exchanges a short-lived SSO token for an even shorter session ID that
  can be used in a POST-based authentication flow.

  Args:
      request: SSO exchange request with token and target app

  Returns:
      SSOExchangeResponse: Session ID and redirect URL for secure handoff

  Raises:
      HTTPException: If SSO token is invalid or target app is unknown
  """
  try:
    # Validate token structure
    if not request.token or not isinstance(request.token, str):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SSO token format"
      )

    # Validate target app
    if request.target_app not in AVAILABLE_APPS:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid target application"
      )

    # Verify SSO token
    secret_key = Config.get_jwt_secret()

    try:
      payload = jwt.decode(
        request.token,
        secret_key,
        algorithms=["HS256"],
        issuer=env.JWT_ISSUER,
        audience=env.JWT_AUDIENCE,
      )
      user_id = payload.get("user_id")
      is_sso = payload.get("sso", False)
      token_id = payload.get("token_id")

      if not user_id or not is_sso or not token_id:
        raise HTTPException(
          status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SSO token"
        )

      # Check if token has already been used with distributed locking
      try:
        redis_client = await get_async_redis_client()
        lock_manager = get_sso_lock_manager()

        if lock_manager:
          async with lock_manager.lock_sso_token(token_id, "token_exchange"):
            # Atomic token verification and exchange marking
            stored_user_id = await redis_client.get(f"sso_token:{token_id}")

            if not stored_user_id:
              SecurityAuditLogger.log_security_event(
                event_type=SecurityEventType.AUTHORIZATION_DENIED,
                details={
                  "action": "sso_token_exchange_not_found",
                  "token_id": token_id[:8] + "...",
                  "user_id": user_id,
                },
                risk_level="medium",
              )
              raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="SSO token expired or already used",
              )

            if not secrets.compare_digest(stored_user_id, user_id):
              SecurityAuditLogger.log_security_event(
                event_type=SecurityEventType.AUTHORIZATION_DENIED,
                details={
                  "action": "sso_token_exchange_user_mismatch",
                  "token_id": token_id[:8] + "...",
                  "expected_user": user_id,
                },
                risk_level="high",
              )
              raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SSO token"
              )

            # Check if token is already being exchanged
            exchange_marker = await redis_client.get(f"sso_token_exchange:{token_id}")
            if exchange_marker:
              SecurityAuditLogger.log_security_event(
                event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                details={
                  "action": "sso_token_double_exchange_attempt",
                  "token_id": token_id[:8] + "...",
                  "user_id": user_id,
                },
                risk_level="high",
              )
              raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="SSO token is already being processed",
              )

            # Atomically mark token as being exchanged to prevent race conditions
            await redis_client.setex(
              f"sso_token_exchange:{token_id}", SSO_SESSION_EXPIRY_SECONDS, "exchanged"
            )

            # Log successful token exchange marking
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.AUTH_SUCCESS,
              details={
                "action": "sso_token_exchange_marked",
                "token_id": token_id[:8] + "...",
                "user_id": user_id,
              },
              risk_level="low",
            )
        else:
          # Fallback without locking
          logger.warning("SSO lock manager unavailable for token exchange")
          stored_user_id = await redis_client.get(f"sso_token:{token_id}")

          if not stored_user_id:
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="SSO token expired or already used",
            )

          if not secrets.compare_digest(stored_user_id, user_id):
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SSO token"
            )

          await redis_client.setex(
            f"sso_token_exchange:{token_id}", SSO_SESSION_EXPIRY_SECONDS, "exchanged"
          )

      except redis.RedisError as e:
        logger.error(f"Redis error during SSO token exchange: {str(e)}")
        raise HTTPException(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail="SSO token validation failed",
        )

    except jwt.ExpiredSignatureError:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="SSO token expired"
      )
    except jwt.InvalidTokenError:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SSO token"
      )

    # Verify user exists and is active
    user = User.get_by_id(user_id, session)
    if not user or not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
      )

    # Create temporary session ID for secure handoff
    session_id = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(
      seconds=SSO_SESSION_EXPIRY_SECONDS
    )  # Very short

    # Store session mapping in Redis with distributed locking
    try:
      redis_client = await get_async_redis_client()
      lock_manager = get_sso_lock_manager()

      if lock_manager:
        async with lock_manager.lock_sso_session(session_id, "session_creation"):
          # Check for session ID collision
          existing_session = await redis_client.get(f"sso_session:{session_id}")
          if existing_session:
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
              details={
                "action": "sso_session_id_collision",
                "session_id": session_id[:8] + "...",
                "user_id": user_id,
              },
              risk_level="high",
            )
            raise HTTPException(
              status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail="Session creation conflict",
            )

          session_data = {
            "user_id": user_id,
            "token_id": token_id,  # Reference to original SSO token
            "target_app": request.target_app,
            "return_url": request.return_url,
            "created_at": datetime.now(timezone.utc).isoformat(),
          }

          # Atomically store session data
          await redis_client.setex(
            f"sso_session:{session_id}",
            SSO_SESSION_EXPIRY_SECONDS,
            json.dumps(session_data),
          )

          # Log successful session creation
          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.AUTH_SUCCESS,
            details={
              "action": "sso_session_created",
              "session_id": session_id[:8] + "...",
              "user_id": user_id,
              "target_app": request.target_app,
            },
            risk_level="low",
          )
      else:
        # Fallback without locking
        logger.warning("SSO lock manager unavailable for session creation")
        session_data = {
          "user_id": user_id,
          "token_id": token_id,
          "target_app": request.target_app,
          "return_url": request.return_url,
        }

        await redis_client.setex(
          f"sso_session:{session_id}",
          SSO_SESSION_EXPIRY_SECONDS,
          json.dumps(session_data),
        )

    except redis.RedisError as e:
      logger.error(f"Failed to store SSO session in Redis: {str(e)}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Session creation failed",
      )

    # Build app-specific redirect URLs
    app_urls = Config.get_app_urls()

    base_url = app_urls.get(request.target_app)
    if not base_url:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Target app URL not configured"
      )

    # Create secure handoff URL - uses POST endpoint instead of URL params
    redirect_url = f"{base_url}/auth/sso-complete"

    return SSOExchangeResponse(
      session_id=session_id,
      redirect_url=redirect_url,
      expires_at=expires_at,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"SSO token exchange error: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="SSO exchange failed"
    )


@router.post(
  "/sso-complete",
  response_model=AuthResponse,
  status_code=status.HTTP_200_OK,
  summary="Complete SSO Authentication",
  description="Complete SSO authentication using session ID from secure handoff.",
  operation_id="completeSSOAuth",
  responses={
    401: {"model": ErrorResponse, "description": "Invalid session"},
  },
)
async def sso_complete(
  request: SSOCompleteRequest,
  response: Response,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(sso_rate_limit_dependency),
) -> AuthResponse:
  """
  Complete SSO authentication using session ID.

  This endpoint completes the secure SSO handoff by exchanging a session ID
  for a full authentication session. It's designed to be called via POST
  from the frontend after receiving a session ID from sso-exchange.

  Args:
      session_id: Temporary session ID from secure handoff
      response: FastAPI response object for setting cookies

  Returns:
      AuthResponse: Success response with user data

  Raises:
      HTTPException: If session is invalid or expired
  """
  try:
    session_id = request.session_id

    # Retrieve and validate session with distributed locking
    try:
      redis_client = await get_async_redis_client()
      lock_manager = get_sso_lock_manager()

      if lock_manager:
        async with lock_manager.lock_sso_session(session_id, "session_completion"):
          # Atomic session consumption
          session_data_str = await redis_client.get(f"sso_session:{session_id}")

          if not session_data_str:
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.AUTHORIZATION_DENIED,
              details={
                "action": "sso_session_not_found",
                "session_id": session_id[:8] + "...",
              },
              risk_level="medium",
            )
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="Session expired or invalid",
            )

          # Parse session data
          session_data = json.loads(session_data_str)
          user_id = session_data.get("user_id")
          token_id = session_data.get("token_id")

          if not user_id or not token_id:
            SecurityAuditLogger.log_security_event(
              event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
              details={
                "action": "sso_session_invalid_data",
                "session_id": session_id[:8] + "...",
                "session_data": session_data,
              },
              risk_level="high",
            )
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session data"
            )

          # Atomically delete session and related tokens (single use)
          await redis_client.delete(f"sso_session:{session_id}")
          await redis_client.delete(f"sso_token:{token_id}")
          await redis_client.delete(f"sso_token_exchange:{token_id}")

          # Log successful session completion
          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.AUTH_SUCCESS,
            details={
              "action": "sso_session_completed",
              "session_id": session_id[:8] + "...",
              "user_id": user_id,
              "target_app": session_data.get("target_app"),
            },
            risk_level="low",
          )
      else:
        # Fallback without locking
        logger.warning("SSO lock manager unavailable for session completion")
        session_data_str = await redis_client.get(f"sso_session:{session_id}")

        if not session_data_str:
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired or invalid",
          )

        session_data = json.loads(session_data_str.decode("utf-8"))
        user_id = session_data.get("user_id")
        token_id = session_data.get("token_id")

        if not user_id or not token_id:
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session data"
          )

        await redis_client.delete(f"sso_session:{session_id}")
        await redis_client.delete(f"sso_token:{token_id}")
        await redis_client.delete(f"sso_token_exchange:{token_id}")

    except redis.RedisError as e:
      logger.error(f"Redis error during SSO completion: {str(e)}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Session validation failed",
      )
    except json.JSONDecodeError as e:
      logger.error(f"Invalid session data format: {str(e)}")
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session data"
      )

    # Verify user exists and is active
    user = User.get_by_id(user_id, session)
    if not user or not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
      )

    # Create new JWT token for this session
    jwt_token = create_jwt_token(user.id)

    # No longer setting auth cookies - using Bearer token authentication
    # Token is returned in the response body for the frontend to store

    return AuthResponse(
      user={
        "id": user.id,
        "name": user.name,
        "email": user.email,
      },
      message="SSO authentication completed successfully",
      token=jwt_token,  # Return JWT for Bearer authentication
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"SSO completion error: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="SSO completion failed"
    )
