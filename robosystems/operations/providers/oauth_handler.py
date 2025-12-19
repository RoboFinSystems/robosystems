"""Generic OAuth2 handler for connection providers."""

# Standard library
import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlencode

# Third-party
import httpx
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...config import env

# Local imports
from ...logger import logger
from ...models.iam import ConnectionCredentials


class OAuthProviderProtocol(Protocol):
  """Protocol for OAuth providers."""

  @property
  def name(self) -> str:
    """Provider name."""
    ...

  @property
  def client_id(self) -> str:
    """OAuth client ID."""
    ...

  @property
  def client_secret(self) -> str:
    """OAuth client secret."""
    ...

  @property
  def authorize_url(self) -> str:
    """OAuth authorization URL."""
    ...

  @property
  def token_url(self) -> str:
    """OAuth token URL."""
    ...

  @property
  def scopes(self) -> list[str]:
    """Required OAuth scopes."""
    ...

  def get_additional_auth_params(self) -> dict[str, str]:
    """Get provider-specific auth parameters."""
    return {}

  def extract_provider_data(self, callback_data: dict[str, Any]) -> dict[str, Any]:
    """Extract provider-specific data from callback."""
    return {}

  def get_refresh_params(self) -> dict[str, str]:
    """Get provider-specific refresh parameters."""
    return {}


class OAuthState:
  """Manage OAuth state for security."""

  # In production, these should be stored in Redis or database
  # For now, we'll use in-memory storage
  _states: dict[str, dict[str, Any]] = {}

  @classmethod
  def create(cls, connection_id: str, user_id: str, redirect_uri: str) -> str:
    """Create and store OAuth state."""
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
    """Validate and retrieve OAuth state data."""
    state_hash = hashlib.sha256(state.encode()).hexdigest()
    state_data = cls._states.get(state_hash)

    if not state_data:
      return None

    # Check expiry
    if datetime.now(UTC) > state_data["expires_at"]:
      del cls._states[state_hash]
      return None

    # Remove state after validation (one-time use)
    del cls._states[state_hash]
    return state_data

  @classmethod
  def cleanup_expired(cls):
    """Clean up expired states."""
    now = datetime.now(UTC)
    expired_states = [
      state_hash for state_hash, data in cls._states.items() if now > data["expires_at"]
    ]
    for state_hash in expired_states:
      del cls._states[state_hash]


class OAuthHandler:
  """Generic OAuth2 handler."""

  def __init__(self, provider: OAuthProviderProtocol):
    self.provider = provider

  def get_authorization_url(
    self, connection_id: str, user_id: str, redirect_uri: str | None = None
  ) -> tuple[str, str]:
    """Generate OAuth authorization URL."""
    # Use provided redirect_uri or default from environment
    if not redirect_uri:
      base_url = env.ROBOSYSTEMS_API_URL
      redirect_uri = f"{base_url}/v1/oauth/callback/{self.provider.name}"

    # Create state for security validation
    state = OAuthState.create(connection_id, user_id, redirect_uri)

    # Build authorization URL
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
    """Exchange authorization code for tokens."""
    token_data = {
      "grant_type": "authorization_code",
      "code": code,
      "redirect_uri": redirect_uri,
      "client_id": self.provider.client_id,
      "client_secret": self.provider.client_secret,
    }

    async with httpx.AsyncClient() as client:
      response = await client.post(
        self.provider.token_url,
        data=token_data,
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

      # Calculate token expiry if provided
      if "expires_in" in tokens:
        tokens["expires_at"] = datetime.now(UTC) + timedelta(
          seconds=tokens["expires_in"]
        )

      return tokens

  async def refresh_tokens(self, refresh_token: str) -> dict[str, Any]:
    """Refresh OAuth tokens."""
    refresh_data = {
      "grant_type": "refresh_token",
      "refresh_token": refresh_token,
      "client_id": self.provider.client_id,
      "client_secret": self.provider.client_secret,
      **self.provider.get_refresh_params(),
    }

    async with httpx.AsyncClient() as client:
      response = await client.post(
        self.provider.token_url,
        data=refresh_data,
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

      # Calculate token expiry if provided
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
  ):
    """Store OAuth tokens securely."""
    # Store in ConnectionCredentials model
    credentials = ConnectionCredentials.get_or_create(
      connection_id=connection_id, db=db
    )

    # Encrypt and store tokens
    expires_at = tokens.get("expires_at")
    credentials.credentials = {
      "access_token": tokens.get("access_token"),
      "refresh_token": tokens.get("refresh_token"),
      "token_type": tokens.get("token_type", "Bearer"),
      "expires_at": expires_at.isoformat() if expires_at is not None else None,
      "scope": tokens.get("scope"),
      **provider_data,
    }

    db.commit()
    logger.info(f"Stored OAuth tokens for connection {connection_id}")

  async def validate_connection(self, access_token: str) -> bool:
    """Validate OAuth connection is working."""
    # This should be implemented by specific providers
    # Default implementation just checks token exists
    return bool(access_token)
