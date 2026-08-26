"""Access-token validation for the MCP routes — the OAuth counterpart of
``utils.validate_api_key``.

An opaque ``rfso…`` bearer resolves to an ``OAuthPrincipal``: the user
plus the grant's tenant scope (one graph, one canonical resource). The
route then checks the resource matches (audience) and runs the same live
access checks every carriage runs. Validation results are cached under
the token's SHA-256 digest through ``api_key_cache`` — the same encrypted,
signed store the API-key path uses — so revocation (``OAuthToken.revoke``)
clears the entry by digest.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ...logger import logger
from ...models.core import User
from ...models.core.user.oauth_token import (
  ACCESS_TOKEN_PREFIX,
  TOKEN_TYPE_ACCESS,
  OAuthToken,
)
from .cache import api_key_cache


@dataclass(frozen=True)
class OAuthPrincipal:
  user: User
  token_id: str
  grant_id: str
  client_row_id: str
  graph_id: str
  resource: str
  scope: str

  @property
  def user_id(self) -> str:
    return str(self.user.id)


def is_oauth_access_token(candidate: str | None) -> bool:
  return bool(candidate) and str(candidate).startswith(ACCESS_TOKEN_PREFIX)


def _serialize(user: User, token: OAuthToken, grant: Any) -> dict[str, Any]:
  return {
    "id": str(user.id),
    "email": user.email,
    "name": user.name,
    "email_verified": bool(user.email_verified),
    "is_active": bool(user.is_active),
    "session_version": int(getattr(user, "session_version", 0) or 0),
    "oauth_token_id": str(token.id),
    "oauth_grant_id": str(grant.id),
    "oauth_client_row_id": str(grant.oauth_client_id),
    "oauth_graph_id": str(grant.graph_id),
    "oauth_resource": str(grant.resource),
    "oauth_scope": str(grant.scope),
    "oauth_expires_at": token.expires_at.replace(tzinfo=UTC).isoformat()
    if token.expires_at.tzinfo is None
    else token.expires_at.isoformat(),
  }


def _principal_from_payload(user_data: dict[str, Any]) -> OAuthPrincipal | None:
  try:
    expires_at = datetime.fromisoformat(str(user_data["oauth_expires_at"]))
    if expires_at.tzinfo is None:
      expires_at = expires_at.replace(tzinfo=UTC)
    if datetime.now(UTC) >= expires_at:
      return None
    if not user_data.get("is_active", False):
      return None
    user = User(
      id=user_data["id"],
      email=user_data["email"],
      name=user_data.get("name"),
      email_verified=bool(user_data.get("email_verified", False)),
      is_active=True,
      session_version=int(user_data.get("session_version", 0) or 0),
    )
    return OAuthPrincipal(
      user=user,
      token_id=str(user_data["oauth_token_id"]),
      grant_id=str(user_data["oauth_grant_id"]),
      client_row_id=str(user_data["oauth_client_row_id"]),
      graph_id=str(user_data["oauth_graph_id"]),
      resource=str(user_data["oauth_resource"]),
      scope=str(user_data.get("oauth_scope", "mcp")),
    )
  except (KeyError, TypeError, ValueError) as exc:
    logger.warning(f"Discarding malformed cached OAuth principal: {exc}")
    return None


def validate_oauth_access_token(plain_token: str) -> OAuthPrincipal | None:
  """Resolve a presented access token, or ``None`` when it is not live.

  Expired, revoked, unknown, and tokens whose grant, client, or user are
  no longer active all read as ``None`` — the caller answers 401
  ``invalid_token`` without distinguishing them.
  """
  if not is_oauth_access_token(plain_token) or len(plain_token) > 256:
    return None

  fingerprint = OAuthToken.fingerprint(plain_token)

  try:
    cached = api_key_cache.get_cached_api_key_validation(fingerprint)
  except Exception as exc:
    logger.warning(f"OAuth token cache read failed, falling back to DB: {exc}")
    cached = None
  if isinstance(cached, dict):
    user_data = cached.get("user_data")
    if isinstance(user_data, dict) and "oauth_token_id" in user_data:
      principal = _principal_from_payload(user_data)
      if principal is not None:
        return principal

  from ...database import SessionFactory
  from ...models.core import OAuthClient, OAuthGrant

  sess = SessionFactory()
  try:
    token = OAuthToken.get_by_plaintext(plain_token, TOKEN_TYPE_ACCESS, sess)
    if token is None or not token.is_live:
      return None
    grant = OAuthGrant.get_by_id(str(token.grant_id), sess)
    if grant is None or grant.is_revoked:
      return None
    client = OAuthClient.get_by_id(str(grant.oauth_client_id), sess)
    if client is None or not client.is_active:
      return None
    user = User.get_by_id(str(grant.user_id), sess)
    if user is None or not bool(user.is_active):
      return None

    token.last_used_at = datetime.now(UTC)
    grant.last_used_at = token.last_used_at
    try:
      sess.commit()
    except Exception as exc:
      sess.rollback()
      logger.warning(f"Failed to stamp OAuth token use: {exc}")

    payload = _serialize(user, token, grant)
    try:
      api_key_cache.cache_api_key_validation(fingerprint, payload)
    except Exception as exc:
      logger.warning(f"OAuth token cache write failed: {exc}")

    sess.expunge(user)
    return OAuthPrincipal(
      user=user,
      token_id=str(token.id),
      grant_id=str(grant.id),
      client_row_id=str(grant.oauth_client_id),
      graph_id=str(grant.graph_id),
      resource=str(grant.resource),
      scope=str(grant.scope),
    )
  finally:
    sess.close()
