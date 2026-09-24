"""Generic OAuth2 handler for connection providers.

Providers implement `OAuthProviderProtocol` (endpoints, credentials, vendor
parameters); `OAuthHandler` runs the shared authorization-code flow.
"""

import hashlib
import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...config import env
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...logger import logger
from ...models.core import ConnectionCredentials


class OAuthProviderProtocol(Protocol):
  """Per-provider endpoints, credentials, and vendor-specific parameters."""

  @property
  def name(self) -> str: ...

  @property
  def client_id(self) -> str: ...

  @property
  def client_secret(self) -> str: ...

  @property
  def authorize_url(self) -> str: ...

  @property
  def token_url(self) -> str: ...

  @property
  def scopes(self) -> list[str]: ...

  def get_additional_auth_params(self) -> dict[str, str]:
    """Extra query parameters for the authorize URL."""
    return {}

  def extract_provider_data(self, callback_data: dict[str, Any]) -> dict[str, Any]:
    """Pull provider-specific fields out of the callback and into credentials."""
    return {}

  def get_refresh_params(self) -> dict[str, str]:
    """Extra form fields for the token-refresh request."""
    return {}


STATE_TTL_SECONDS = 600
_STATE_KEY_PREFIX = "oauth:state:"


class OAuthState:
  """Single-use, 10-minute CSRF state for an in-flight authorization.

  In Valkey because the callback lands on an arbitrary API task; ``GETDEL``
  makes single use atomic across tasks. Defense in depth: the callback route
  also checks the caller is the user the state was minted for.
  """

  @staticmethod
  def _key(state: str) -> str:
    """SHA-256 of the token, so a store read can't replay a flow. No KDF: the
    input is 256 random bits, not a human secret."""
    return f"{_STATE_KEY_PREFIX}{hashlib.sha256(state.encode()).hexdigest()}"

  @classmethod
  def create(
    cls,
    connection_id: str,
    user_id: str,
    redirect_uri: str,
    ttl_seconds: int = STATE_TTL_SECONDS,
  ) -> str:
    """Mint a state token and record what the callback should resume.

    Raises 503 if the store is unreachable rather than fail later at the
    callback.
    """
    state = secrets.token_urlsafe(32)
    now = datetime.now(UTC)
    payload = {
      "connection_id": connection_id,
      "user_id": user_id,
      "redirect_uri": redirect_uri,
      "created_at": now.isoformat(),
      "expires_at": (now + timedelta(seconds=ttl_seconds)).isoformat(),
    }

    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      client.setex(cls._key(state), ttl_seconds, json.dumps(payload))
    except Exception as exc:
      logger.error(f"Failed to persist OAuth state: {exc}")
      raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Unable to start the authorization flow. Please try again.",
      ) from exc

    return state

  @classmethod
  def validate(cls, state: str) -> dict[str, Any] | None:
    """Consume a state token, returning what it recorded, or None if invalid.

    Fails closed: an unreachable store or malformed payload reads as invalid.
    """
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.getdel(cls._key(state))
    except Exception as exc:
      logger.error(f"Failed to read OAuth state: {exc}")
      return None

    if not raw:
      return None

    try:
      payload = json.loads(raw)
      payload["created_at"] = datetime.fromisoformat(payload["created_at"])
      payload["expires_at"] = datetime.fromisoformat(payload["expires_at"])
    except (ValueError, KeyError, TypeError) as exc:
      logger.error(f"Discarding malformed OAuth state: {exc}")
      return None

    return payload


class OAuthHandler:
  """Authorization-code flow for one provider."""

  def __init__(self, provider: OAuthProviderProtocol):
    self.provider = provider

  @staticmethod
  def _validate_redirect_uri(redirect_uri: str) -> None:
    """Reject a client-supplied redirect_uri outside the trusted origins.

    Otherwise a caller could redirect the authorization code to any host.
    Only the CORS allowlist origins pass.
    """
    parsed = urlparse(redirect_uri)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin not in env.get_main_cors_origins():
      logger.warning("Rejected OAuth redirect_uri outside allowed origins: %s", origin)
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="redirect_uri is not an allowed origin",
      )

  def get_authorization_url(
    self, connection_id: str, user_id: str, redirect_uri: str | None = None
  ) -> tuple[str, str]:
    """Build the provider authorize URL, returning it with its state token.

    Defaults `redirect_uri` to this API's callback; a caller-supplied override
    must be a trusted frontend origin.
    """
    if not redirect_uri:
      base_url = env.ROBOSYSTEMS_API_URL
      redirect_uri = f"{base_url}/v1/oauth/callback/{self.provider.name}"
    else:
      self._validate_redirect_uri(redirect_uri)

    state = OAuthState.create(connection_id, user_id, redirect_uri)

    auth_params = {
      "client_id": self.provider.client_id,
      "response_type": "code",
      "redirect_uri": redirect_uri,
      "scope": " ".join(self.provider.scopes),
      "state": state,
      **self.provider.get_additional_auth_params(),
    }

    auth_url = f"{self.provider.authorize_url}?{urlencode(auth_params)}"
    return auth_url, state

  async def exchange_code_for_tokens(
    self, code: str, redirect_uri: str
  ) -> dict[str, Any]:
    """Exchange an authorization code for tokens.

    `redirect_uri` must be byte-identical to the one sent to authorize.
    Adds an absolute `expires_at` from `expires_in`.
    """
    token_data = {
      "grant_type": "authorization_code",
      "code": code,
      "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient() as client:
      response = await client.post(
        self.provider.token_url,
        data=token_data,
        auth=(self.provider.client_id, self.provider.client_secret),
        headers={
          "Accept": "application/json",
          "Content-Type": "application/x-www-form-urlencoded",
        },
      )

      if response.status_code != 200:
        logger.error(f"Token exchange failed: {response.text}")
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Token exchange failed: {response.status_code}",
        )

      tokens = response.json()

      if "expires_in" in tokens:
        tokens["expires_at"] = datetime.now(UTC) + timedelta(
          seconds=tokens["expires_in"]
        )

      return tokens

  async def refresh_tokens(self, refresh_token: str) -> dict[str, Any]:
    """Trade a refresh token for a fresh access token.

    Store the whole response: providers rotate the refresh token too.
    """
    refresh_data = {
      "grant_type": "refresh_token",
      "refresh_token": refresh_token,
      **self.provider.get_refresh_params(),
    }

    async with httpx.AsyncClient() as client:
      response = await client.post(
        self.provider.token_url,
        data=refresh_data,
        auth=(self.provider.client_id, self.provider.client_secret),
        headers={
          "Accept": "application/json",
          "Content-Type": "application/x-www-form-urlencoded",
        },
      )

      if response.status_code != 200:
        logger.error(f"Token refresh failed: {response.text}")
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Token refresh failed: {response.status_code}",
        )

      tokens = response.json()

      if "expires_in" in tokens:
        tokens["expires_at"] = datetime.now(UTC) + timedelta(
          seconds=tokens["expires_in"]
        )

      return tokens

  def store_tokens(
    self,
    connection_id: str,
    tokens: dict[str, Any],
    provider_data: dict[str, Any],
    db: Session,
    user_id: str | None = None,
  ):
    """Persist tokens (encrypted at rest by `ConnectionCredentials`).

    Upserts on `connection_id`.
    """
    expires_at = tokens.get("expires_at")
    credential_data = {
      "access_token": tokens.get("access_token"),
      "refresh_token": tokens.get("refresh_token"),
      "token_type": tokens.get("token_type", "Bearer"),
      "expires_at": expires_at.isoformat() if expires_at is not None else None,
      "scope": tokens.get("scope"),
      **provider_data,
    }

    existing = ConnectionCredentials.get_by_connection_id(connection_id, db)
    if existing:
      existing.update_credentials(credential_data, db)
      if expires_at:
        existing.update_expiry(expires_at, db)
      logger.info(f"Updated OAuth tokens for connection {connection_id}")
    else:
      ConnectionCredentials.create(
        connection_id=connection_id,
        provider=self.provider.name,
        user_id=user_id or "",
        credentials=credential_data,
        session=db,
        expires_at=expires_at,
      )
      logger.info(f"Created OAuth tokens for connection {connection_id}")

  async def validate_connection(self, access_token: str) -> bool:
    """Only checks that a token is present."""
    return bool(access_token)
