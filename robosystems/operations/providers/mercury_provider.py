"""Mercury connection provider — the bank as a first-class source.

Two credential modes:

- ``oauth`` — the hosted default: partner OAuth, one-hour access token,
  30-day single-use refresh token. Consent is recorded to the audit log.
- ``api_key`` — a pasted read-only token, gated by
  ``MERCURY_API_KEY_CONNECTIONS_ENABLED``. Never on in hosted production:
  Mercury's terms bar third-party automated access without the written
  permission the OAuth approval grants. Self-hosted deployments may enable it.

Disconnect revokes the grant and purges the feed (``purge_bank_feed``), as the
partnership agreement requires.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...config import env
from ...logger import logger
from ...models.api.graphs.connections import MercuryConnectionConfig
from ...operations.connection_service import ConnectionService
from .bank_feed import (
  purge_bank_feed_connection,
  record_bank_feed_purged,
)
from .bank_feed import (
  record_bank_feed_consent as record_consent,
)
from .oauth_handler import OAuthHandler
from .types import SyncOutcome

PROVIDER = "mercury"
SCOPES = ["read", "offline_access"]


class MercuryOAuthProvider:
  """Mercury endpoints and credentials for ``OAuthHandler``.

  Sandbox and production are separate OAuth clients on separate hosts, each
  with its own pre-registered redirect URIs. Authorize takes the standard
  code-flow parameters (``state`` at least 8 characters — ours is 43); the
  token endpoint takes Basic client auth; refresh must re-send ``scope``.
  PKCE is optional for the client and not used.
  """

  def __init__(self) -> None:
    self.environment = env.MERCURY_ENVIRONMENT
    suffix = "-sandbox" if self.environment == "sandbox" else ""
    self._oauth_base_url = f"https://oauth2{suffix}.mercury.com"
    self.api_base_url = f"https://api{suffix}.mercury.com/api/v1"

  @property
  def name(self) -> str:
    return PROVIDER

  @property
  def client_id(self) -> str:
    return env.MERCURY_CLIENT_ID

  @property
  def client_secret(self) -> str:
    return env.MERCURY_CLIENT_SECRET

  @property
  def authorize_url(self) -> str:
    return f"{self._oauth_base_url}/oauth2/auth"

  @property
  def token_url(self) -> str:
    return f"{self._oauth_base_url}/oauth2/token"

  @property
  def revoke_url(self) -> str:
    return f"{self._oauth_base_url}/oauth2/revoke"

  @property
  def scopes(self) -> list[str]:
    return list(SCOPES)

  def get_additional_auth_params(self) -> dict[str, str]:
    return {}

  def extract_provider_data(self, callback_data: dict[str, Any]) -> dict[str, Any]:
    """Mercury has no realm; the connect-time sync config rides along."""
    sync_config = callback_data.get("sync_config")
    return {"auth_mode": "oauth", "sync_config": dict(sync_config or {})}

  def get_refresh_params(self) -> dict[str, str]:
    """``scope`` on every refresh — omitting it is Mercury's most common
    partner failure."""
    return {"scope": " ".join(SCOPES)}

  async def get_entity_info(self, access_token: str) -> dict[str, Any]:
    """The organization behind a token, from its accounts; ``{}`` on failure."""
    async with httpx.AsyncClient(timeout=15.0) as client:
      response = await client.get(
        f"{self.api_base_url}/accounts",
        headers={
          "Authorization": f"Bearer {access_token}",
          "Accept": "application/json",
        },
      )
    if response.status_code != 200:
      logger.error(f"Failed to list Mercury accounts: {response.status_code}")
      return {}
    body = response.json() if response.content else {}
    accounts = list((body or {}).get("accounts") or [])
    if not accounts:
      return {}
    return {
      "legal_business_name": accounts[0].get("legalBusinessName"),
      "account_count": len(accounts),
    }

  async def validate_connection(self, access_token: str) -> bool:
    try:
      return bool(await self.get_entity_info(access_token))
    except Exception as exc:
      logger.error(f"Mercury connection validation failed: {exc}")
      return False

  async def revoke_token(self, token: str) -> bool:
    """Revoke a token at Mercury (ORY Hydra: ``token`` form field, Basic
    client auth). Revoking the refresh token tears the grant down."""
    async with httpx.AsyncClient(timeout=10.0) as client:
      response = await client.post(
        self.revoke_url,
        data={"token": token},
        auth=(self.client_id, self.client_secret),
        headers={"Accept": "application/json"},
      )
    if response.status_code == 200:
      return True
    logger.warning(
      f"Mercury token revocation returned {response.status_code}: {response.text[:200]}"
    )
    return False


mercury_oauth_provider = MercuryOAuthProvider()
mercury_oauth_handler = OAuthHandler(mercury_oauth_provider)


def _sync_config(config: MercuryConnectionConfig | None) -> dict[str, Any]:
  if config is None:
    return {"since_date": None, "include_treasury": True}
  return {
    "since_date": config.since_date.isoformat() if config.since_date else None,
    "include_treasury": bool(config.include_treasury),
  }


async def create_mercury_connection(
  entity_id: str,
  config: MercuryConnectionConfig | None,
  user_id: str,
  graph_id: str,
  db: Session,
) -> str:
  """Open the connection.

  OAuth mode: a ``pending_oauth`` row holding only the connect-time sync
  config; tokens land at the callback. API-key mode (flag-gated): the key is
  proven against ``/accounts``, the row is ``connected`` at once and the
  first sync is dispatched.
  """
  sync_config = _sync_config(config)
  api_key = (config.api_key or "").strip() if config is not None else ""

  if api_key:
    if not env.MERCURY_API_KEY_CONNECTIONS_ENABLED:
      raise ValueError(
        "Mercury API-key connections are not enabled on this deployment; "
        "connect over OAuth."
      )
    info = await mercury_oauth_provider.get_entity_info(api_key)
    if not info:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Mercury rejected the API token, or it can read no accounts.",
      )
    metadata = {
      "status": "connected",
      "entity_name": info.get("legal_business_name"),
      "institution_name": "Mercury",
    }
    credentials: dict[str, Any] = {
      "auth_mode": "api_key",
      "api_key": api_key,
      "sync_config": sync_config,
    }
  else:
    metadata = {"status": "pending_oauth", "institution_name": "Mercury"}
    credentials = {"auth_mode": "oauth", "sync_config": sync_config}

  connection_data = await ConnectionService.create_connection(
    entity_id=graph_id,
    provider=PROVIDER,
    user_id=user_id,
    credentials=credentials,
    metadata=metadata,
    graph_id=graph_id,
  )
  connection_id = str(connection_data["connection_id"])

  if api_key:
    record_bank_feed_consent(
      graph_id=graph_id,
      connection_id=connection_id,
      user_id=user_id,
      auth_mode="api_key",
      scope="read",
    )
    await _dispatch_first_sync(graph_id, connection_id, user_id)

  return connection_id


async def _dispatch_first_sync(graph_id: str, connection_id: str, user_id: str) -> None:
  """Best-effort: a failed dispatch leaves a connected row the operator can
  sync by hand; it never fails the connect."""
  from ...operations.connection_service import dispatch_connection_sync

  try:
    await dispatch_connection_sync(
      graph_id=graph_id,
      connection_id=connection_id,
      user_id=user_id,
      full_rebuild=True,
    )
  except Exception as exc:
    logger.warning(
      f"First Mercury sync not dispatched for connection {connection_id}: {exc}"
    )


async def sync_mercury_connection(
  connection: dict[str, Any], sync_options: dict[str, Any] | None, graph_id: str
) -> SyncOutcome:
  """Submit the ``mercury_sync`` Dagster job; returns its run id.

  ``sync_options`` accepts ``full_rebuild``, ``lookback_days`` (default 60),
  ``since_date`` (overrides the lookback) and ``sync_lock_id``.
  """
  from robosystems.middleware.sse.dagster_monitor import submit_dagster_job_sync

  options = sync_options or {}
  connection_id = str(connection.get("connection_id", ""))
  since_date = options.get("since_date", "") or ""
  sync_config = {
    "graph_id": graph_id,
    "connection_id": connection_id,
    "user_id": str(connection.get("user_id", "")),
    "full_rebuild": bool(options.get("full_rebuild", False)),
    "lookback_days": int(options.get("lookback_days", 60)),
    "since_date": str(since_date),
    "sync_lock_id": str(options.get("sync_lock_id", "") or ""),
  }
  run_id = submit_dagster_job_sync(
    job_name="mercury_sync",
    run_config={"ops": {"mercury_feed": {"config": sync_config}}},
    tags={"graph_id": graph_id, "connection_id": connection_id, "pipeline": PROVIDER},
  )
  logger.info(
    f"Mercury sync submitted for graph={graph_id}, connection={connection_id}, "
    f"run_id={run_id}, full_rebuild={sync_config['full_rebuild']}, "
    f"since_date={since_date or '<lookback>'}"
  )
  return SyncOutcome(status="dispatched", task_id=run_id)


async def cleanup_mercury_connection(connection: dict[str, Any], graph_id: str) -> None:
  """Disconnect: revoke the grant, purge the feed, empty the credentials.

  Revocation is best-effort; a purge failure surfaces so the disconnect is
  retried rather than leaving feed data behind.
  """
  from ...database import platform_session
  from ...models.core import ConnectionCredentials

  connection_id = str(connection.get("connection_id") or connection.get("id") or "")
  if not connection_id:
    logger.warning("Mercury cleanup: connection payload has no id; nothing to do")
    return

  auth_mode = "oauth"
  refresh_token: str | None = None
  with platform_session() as db:
    creds = ConnectionCredentials.get_by_connection_id(connection_id, db)
    if creds:
      data = creds.get_credentials()
      auth_mode = str(data.get("auth_mode") or "oauth")
      refresh_token = data.get("refresh_token") or data.get("access_token")

  if auth_mode == "oauth" and refresh_token:
    try:
      revoked = await mercury_oauth_provider.revoke_token(refresh_token)
      logger.info(
        f"Mercury cleanup for connection {connection_id}: token revoke "
        f"{'succeeded' if revoked else 'failed'}"
      )
    except Exception as exc:
      logger.warning(f"Mercury token revocation errored for {connection_id}: {exc}")

  purged = purge_bank_feed_connection(
    graph_id, provider=PROVIDER, connection_id=connection_id
  )

  with platform_session() as db:
    creds = ConnectionCredentials.get_by_connection_id(connection_id, db)
    if creds:
      creds.update_credentials(
        {"auth_mode": auth_mode, "revoked_at": datetime.now(UTC).isoformat()}, db
      )

  record_bank_feed_purged(
    provider=PROVIDER,
    connection=connection,
    graph_id=graph_id,
    connection_id=connection_id,
    auth_mode=auth_mode,
    purged=purged,
  )


def record_bank_feed_consent(
  *,
  graph_id: str,
  connection_id: str,
  user_id: str,
  auth_mode: str,
  scope: str | None,
  organization: str | None = None,
) -> None:
  """The consent record the partnership's DAA asks for: who connected which
  organization, over which credential, with what scope, when."""
  record_consent(
    provider=PROVIDER,
    environment=mercury_oauth_provider.environment,
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    auth_mode=auth_mode,
    scope=scope,
    organization=organization,
    institution="Mercury",
  )
