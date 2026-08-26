"""Token endpoint semantics: code exchange, refresh rotation, revocation.

Errors follow RFC 6749 §5.2 (``invalid_grant`` for anything wrong with the
code or refresh token, ``invalid_client`` for client authentication,
``invalid_request`` for shape). A dead refresh token answers
``invalid_grant`` — that is the signal hosted clients key on to re-prompt
instead of retrying forever.
"""

import hmac
from dataclasses import dataclass

from authlib.oauth2.rfc7636 import create_s256_code_challenge
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.core import OAuthClient, OAuthGrant, OAuthToken, User
from robosystems.models.core.user.oauth_token import (
  REFRESH_TOKEN_PREFIX,
  TOKEN_TYPE_ACCESS,
  TOKEN_TYPE_REFRESH,
)
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .authorization import AuthorizationCodeStore
from .clients import redirect_uri_matches
from .resources import normalize_scope, resolve_resource

# RFC 7636 §4.1: the verifier is 43-128 unreserved characters.
_VERIFIER_MIN = 43
_VERIFIER_MAX = 128


class TokenError(Exception):
  def __init__(self, error: str, description: str, *, status_code: int = 400):
    super().__init__(description)
    self.error = error
    self.description = description
    self.status_code = status_code


@dataclass(frozen=True)
class TokenResponse:
  access_token: str
  refresh_token: str
  expires_in: int
  scope: str
  token_type: str = "Bearer"

  def as_dict(self) -> dict:
    return {
      "access_token": self.access_token,
      "token_type": self.token_type,
      "expires_in": self.expires_in,
      "refresh_token": self.refresh_token,
      "scope": self.scope,
    }


def _verify_pkce(code_verifier: str | None, code_challenge: str) -> bool:
  if (
    not code_verifier
    or not isinstance(code_verifier, str)
    or not (_VERIFIER_MIN <= len(code_verifier) <= _VERIFIER_MAX)
    or not all(c.isalnum() or c in "-._~" for c in code_verifier)
  ):
    return False
  return hmac.compare_digest(create_s256_code_challenge(code_verifier), code_challenge)


def _live_grant(grant_id: str, session: Session) -> OAuthGrant:
  grant = OAuthGrant.get_by_id(grant_id, session)
  if grant is None or grant.is_revoked:
    raise TokenError("invalid_grant", "The grant has been revoked")
  user = User.get_by_id(str(grant.user_id), session)
  if user is None or not bool(user.is_active):
    raise TokenError("invalid_grant", "The grant has been revoked")
  return grant


def exchange_authorization_code(
  *,
  code: str | None,
  client: OAuthClient,
  code_verifier: str | None,
  redirect_uri: str | None,
  resource: str | None,
  session: Session,
) -> TokenResponse:
  """``grant_type=authorization_code``. The code is consumed before any
  check, so a failed exchange burns it — a second attempt with a guessed
  verifier finds nothing."""
  payload = AuthorizationCodeStore.consume(code or "")
  if payload is None:
    raise TokenError("invalid_grant", "Invalid or expired authorization code")

  bound_client_row = payload.get("client_row_id")
  if bound_client_row != str(client.id):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      details={
        "action": "oauth_code_client_mismatch",
        "oauth_client_id": client.id,
        "bound_client_id": bound_client_row,
      },
      risk_level="high",
    )
    raise TokenError("invalid_grant", "Authorization code was issued to another client")

  if redirect_uri is not None and not redirect_uri_matches(
    str(payload.get("redirect_uri", "")), redirect_uri
  ):
    raise TokenError("invalid_grant", "redirect_uri does not match")

  if resource:
    target = resolve_resource(resource)
    if target is None or target.resource != payload.get("resource"):
      raise TokenError("invalid_target", "resource does not match the authorization")

  if not _verify_pkce(code_verifier, str(payload.get("code_challenge", ""))):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      details={
        "action": "oauth_pkce_verification_failed",
        "oauth_client_id": client.id,
      },
      risk_level="high",
    )
    raise TokenError("invalid_grant", "PKCE verification failed")

  grant = _live_grant(str(payload.get("grant_id", "")), session)
  _, access_plain, _, refresh_plain, ttl = OAuthToken.mint_pair(
    grant_id=str(grant.id), user_id=str(grant.user_id), session=session
  )
  grant.touch(session)
  return TokenResponse(
    access_token=access_plain,
    refresh_token=refresh_plain,
    expires_in=ttl,
    scope=str(grant.scope),
  )


def refresh_access_token(
  *,
  refresh_token: str | None,
  client: OAuthClient,
  scope: str | None,
  resource: str | None,
  session: Session,
) -> TokenResponse:
  """``grant_type=refresh_token`` with rotation and replay detection.

  The presented token is claimed with a conditional UPDATE; losing the
  claim means it was already rotated — replay — and the whole family is
  revoked so neither the attacker's nor the victim's copy works again.
  """
  if not refresh_token or not refresh_token.startswith(REFRESH_TOKEN_PREFIX):
    raise TokenError("invalid_grant", "Invalid refresh token")

  row = OAuthToken.get_by_plaintext(refresh_token, TOKEN_TYPE_REFRESH, session)
  if row is None:
    raise TokenError("invalid_grant", "Invalid refresh token")

  grant = OAuthGrant.get_by_id(str(row.grant_id), session)
  if grant is None or str(grant.oauth_client_id) != str(client.id):
    raise TokenError("invalid_grant", "Invalid refresh token")

  if row.revoked_at is not None or row.is_expired:
    raise TokenError("invalid_grant", "Refresh token expired or revoked")

  def _replay() -> TokenError:
    revoked = OAuthToken.revoke_family(
      str(row.family_id), session, reason="refresh_token_replay"
    )
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      user_id=str(row.user_id),
      details={
        "action": "oauth_refresh_token_replay",
        "oauth_grant_id": row.grant_id,
        "family_id": row.family_id,
        "tokens_revoked": revoked,
      },
      risk_level="high",
    )
    return TokenError("invalid_grant", "Refresh token has already been used")

  if row.used_at is not None:
    raise _replay()

  if grant.is_revoked:
    raise TokenError("invalid_grant", "The grant has been revoked")
  user = User.get_by_id(str(grant.user_id), session)
  if user is None or not bool(user.is_active):
    raise TokenError("invalid_grant", "The grant has been revoked")

  # Request-shape checks come before the token is consumed: a client that
  # asks for the wrong resource or a wider scope gets an error, not a dead
  # session.
  if resource:
    target = resolve_resource(resource)
    if target is None or target.resource != str(grant.resource):
      raise TokenError("invalid_target", "resource does not match the grant")

  if scope:
    requested = normalize_scope(scope)
    granted = set(str(grant.scope).split())
    if requested is None or not set(requested.split()).issubset(granted):
      raise TokenError("invalid_scope", "Requested scope exceeds the grant")

  # Claim the token. Losing the claim means a concurrent request rotated
  # it first — replay.
  if not OAuthToken.consume_refresh(str(row.id), session):
    raise _replay()

  # The previous access token dies with its refresh token: one live pair
  # per family at a time.
  previous_access = (
    session.query(OAuthToken)
    .filter(
      OAuthToken.family_id == row.family_id,
      OAuthToken.token_type == TOKEN_TYPE_ACCESS,
      OAuthToken.revoked_at.is_(None),
    )
    .all()
  )
  for token in previous_access:
    token.revoke(session, reason="refresh_rotation")

  _, access_plain, _, refresh_plain, ttl = OAuthToken.mint_pair(
    grant_id=str(grant.id),
    user_id=str(grant.user_id),
    session=session,
    family_id=str(row.family_id),
  )
  grant.touch(session)
  return TokenResponse(
    access_token=access_plain,
    refresh_token=refresh_plain,
    expires_in=ttl,
    scope=str(grant.scope),
  )


def revoke_presented_token(
  *, token: str | None, client: OAuthClient, session: Session
) -> None:
  """RFC 7009. Revoking a refresh token revokes its family; revoking an
  access token revokes just that token. Unknown tokens and tokens that
  belong to another client are silently ignored (the RFC's 200), so the
  endpoint is not a validity oracle."""
  if not token or not isinstance(token, str):
    return
  token_type = (
    TOKEN_TYPE_REFRESH if token.startswith(REFRESH_TOKEN_PREFIX) else TOKEN_TYPE_ACCESS
  )
  row = OAuthToken.get_by_plaintext(token, token_type, session)
  if row is None:
    return
  grant = OAuthGrant.get_by_id(str(row.grant_id), session)
  if grant is None or str(grant.oauth_client_id) != str(client.id):
    return
  if token_type == TOKEN_TYPE_REFRESH:
    OAuthToken.revoke_family(str(row.family_id), session, reason="client_revocation")
  else:
    row.revoke(session, reason="client_revocation")
  logger.info(f"OAuth {token_type} token revoked by client {client.id}")
