"""SCIM bearer-token authentication.

The SCIM surface has its own credential (``scim_tokens``, per-org) and its own
auth path. A SCIM token is accepted **only** here; user JWTs and API keys are
never accepted at ``/scim/v2``, and a SCIM token is never accepted by the
normal auth dependencies. Failures answer with the SCIM error envelope, not
FastAPI's ``{"detail": ...}``.
"""

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from ...db.platform import get_db_session
from ...logger import get_logger
from ...models.api.scim import ERROR_SCHEMA
from ...models.core import ScimToken
from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

logger = get_logger(__name__)


def _scim_unauthorized() -> HTTPException:
  # SCIM error envelope, not the default detail shape.
  return HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail={
      "schemas": [ERROR_SCHEMA],
      "detail": "Invalid or missing SCIM bearer token",
      "status": "401",
    },
    headers={"WWW-Authenticate": "Bearer"},
  )


def require_scim_org(
  request: Request,
  session: Session = Depends(get_db_session),
) -> str:
  """Authenticate a SCIM request and return the token's org scope.

  Resolves the presented bearer to an active, unexpired ``scim_tokens`` row,
  stamps its ``last_used_at``, and publishes the org on
  ``request.state.scim_org_id``. Never distinguishes unknown / revoked /
  expired in the response.
  """
  auth_header = request.headers.get("Authorization", "")
  client_ip = request.client.host if request.client else None

  if not auth_header.startswith("Bearer "):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SCIM_AUTH_FAILURE,
      ip_address=client_ip,
      user_agent=request.headers.get("User-Agent"),
      endpoint=str(request.url.path),
      details={"failure_reason": "missing_bearer"},
      risk_level="high",
    )
    raise _scim_unauthorized()

  token = ScimToken.validate_token(auth_header[7:], session)
  if token is None:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SCIM_AUTH_FAILURE,
      ip_address=client_ip,
      user_agent=request.headers.get("User-Agent"),
      endpoint=str(request.url.path),
      details={"failure_reason": "invalid_token"},
      risk_level="high",
    )
    raise _scim_unauthorized()

  token.update_last_used(session)
  org_id = str(token.org_id)
  request.state.scim_org_id = org_id
  request.state.scim_token_id = str(token.id)
  return org_id
