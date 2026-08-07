"""Generic OAuth2 handler for connection providers.

`OAuthHandler` drives the authorization-code flow against any provider that
implements `OAuthProviderProtocol` (see `quickbooks_provider.py` for the one
concrete implementation). Providers supply endpoints, credentials, and the
handful of vendor-specific parameters; everything else is shared here.
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...config import env
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


class OAuthState:
  """Single-use, 10-minute CSRF state for an in-flight authorization.

  Held in process memory, so a flow must complete on the worker that started
  it and no state survives a restart. Both are acceptable because the window
  is short and the user simply retries; nothing durable is lost.
  """

  _states: dict[str, dict[str, Any]] = {}

  @classmethod
  def create(cls, connection_id: str, user_id: str, redirect_uri: str) -> str:
    """Mint a state token and record what the callback should resume."""
    state = secrets.token_urlsafe(32)
    state_hash = hashlib.sha256(state.encode()).hexdigest()

    cls._states[state_hash] = {
      "connection_id": connection_id,
      "user_id": user_id,
      "redirect_uri": redirect_uri,
      "created_at": datetime.now(UTC),
      "expires_at": datetime.now(UTC) + timedelta(minutes=10),
    }

    return state

  @classmethod
  def validate(cls, state: str) -> dict[str, Any] | None:
    """Consume a state token, returning what it recorded, or None if invalid.

    Consuming is the point: an expired or already-redeemed token is dropped so
    the same authorization code can't be replayed.
    """
    state_hash = hashlib.sha256(state.encode()).hexdigest()
    state_data = cls._states.get(state_hash)

    if not state_data:
      return None

    if datetime.now(UTC) > state_data["expires_at"]:
      del cls._states[state_hash]
      return None

    del cls._states[state_hash]
    return state_data

  @classmethod
  def cleanup_expired(cls):
    """Drop states past their expiry (abandoned flows never call validate)."""
    now = datetime.now(UTC)
    expired_states = [
      state_hash for state_hash, data in cls._states.items() if now > data["expires_at"]
    ]
    for state_hash in expired_states:
      del cls._states[state_hash]


class OAuthHandler:
  """Authorization-code flow for one provider."""

  def __init__(self, provider: OAuthProviderProtocol):
    self.provider = provider

  @staticmethod
  def _validate_redirect_uri(redirect_uri: str) -> None:
    """Reject a client-supplied redirect_uri outside the trusted origins.

    The value is echoed into the provider authorize URL and replayed at
    token exchange, so an unconstrained value lets a caller redirect the
    authorization code to an arbitrary host. Allow only the origins we
    already trust for the frontends (the CORS allowlist); the server-built
    default is exempt because it isn't caller-controlled.
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

    `redirect_uri` must be byte-identical to the one sent to the authorize
    endpoint or the provider rejects the exchange. An absolute `expires_at`
    is derived from the returned `expires_in` before the value is stored.
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

    Providers commonly rotate the refresh token too, so store the whole
    response — keeping the old refresh token strands the connection.
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

    Upserts on `connection_id`, so re-authorizing an existing connection
    replaces its tokens rather than leaving two credential rows behind.
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
    """Whether the connection still works.

    This default only checks that a token is present. Providers that can cheaply
    call an authenticated endpoint should override it — see
    `QuickBooksOAuthProvider.validate_connection`.
    """
    return bool(access_token)
