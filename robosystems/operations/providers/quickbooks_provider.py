"""QuickBooks provider-specific operations."""

from typing import Any

import httpx
from sqlalchemy.orm import Session

from ...config import env
from ...logger import logger
from ...models.api.graphs.connections import QuickBooksConnectionConfig
from ...operations.connection_service import ConnectionService
from .oauth_handler import OAuthHandler


class QuickBooksOAuthProvider:
  """QuickBooks OAuth2 provider implementation."""

  def __init__(self):
    self.environment = env.INTUIT_ENVIRONMENT
    self._base_url = (
      "https://sandbox-quickbooks.api.intuit.com"
      if self.environment == "sandbox"
      else "https://quickbooks.api.intuit.com"
    )
    self._auth_base_url = "https://appcenter.intuit.com"

  @property
  def name(self) -> str:
    return "quickbooks"

  @property
  def client_id(self) -> str:
    return env.INTUIT_CLIENT_ID

  @property
  def client_secret(self) -> str:
    return env.INTUIT_CLIENT_SECRET

  @property
  def authorize_url(self) -> str:
    return f"{self._auth_base_url}/connect/oauth2"

  @property
  def token_url(self) -> str:
    return "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

  @property
  def scopes(self) -> list[str]:
    return ["com.intuit.quickbooks.accounting"]

  def get_additional_auth_params(self) -> dict[str, str]:
    """QuickBooks-specific auth parameters."""
    return {
      "access_type": "offline",  # To get refresh token
    }

  def extract_provider_data(self, callback_data: dict[str, Any]) -> dict[str, Any]:
    """Extract QuickBooks-specific data from callback."""
    return {
      "realm_id": callback_data.get("realmId", ""),
    }

  async def get_entity_info(self, access_token: str, realm_id: str) -> dict[str, Any]:
    """Get QuickBooks company information."""
    url = f"{self._base_url}/v3/company/{realm_id}/companyinfo/{realm_id}"

    async with httpx.AsyncClient() as client:
      response = await client.get(
        url,
        headers={
          "Authorization": f"Bearer {access_token}",
          "Accept": "application/json",
          "Accept-Encoding": "identity",
        },
      )

      if response.status_code == 200:
        data = response.json()
        return data.get("CompanyInfo", {})
      else:
        logger.error(f"Failed to get QuickBooks company info: {response.status_code}")
        return {}

  async def validate_connection(self, access_token: str, realm_id: str) -> bool:
    """Validate QuickBooks connection by fetching entity info."""
    try:
      entity_info = await self.get_entity_info(access_token, realm_id)
      return bool(entity_info)
    except Exception as e:
      logger.error(f"QuickBooks connection validation failed: {e}")
      return False


# Global QuickBooks OAuth handler
quickbooks_oauth_provider = QuickBooksOAuthProvider()
quickbooks_oauth_handler = OAuthHandler(quickbooks_oauth_provider)


async def create_quickbooks_connection(
  entity_id: str,
  config: QuickBooksConnectionConfig,
  user_id: str,
  graph_id: str,
  db: Session,
) -> str:
  """Create QuickBooks connection - initiates OAuth flow."""
  # Create a pending connection that will be completed after OAuth
  metadata = {
    "status": "pending_oauth",
    "realm_id": config.realm_id if config and config.realm_id else None,
  }

  connection_data = await ConnectionService.create_connection(
    entity_id=entity_id,
    provider="quickbooks",
    user_id=user_id,
    credentials={},  # Will be populated after OAuth
    metadata=metadata,
    graph_id=graph_id,
  )

  return connection_data["connection_id"]


async def sync_quickbooks_connection(
  connection: dict[str, Any], sync_options: dict[str, Any] | None, graph_id: str
) -> str:
  """Trigger QuickBooks sync via Dagster pipeline.

  Submits the qb_sync_job with configuration derived from the
  connection metadata and sync options.

  Args:
      connection: Connection dict with metadata (realm_id, etc.)
      sync_options: Optional dict with full_rebuild, lookback_days
      graph_id: Target graph database ID

  Returns:
      Dagster run ID for progress tracking
  """
  from robosystems.middleware.sse.dagster_monitor import submit_dagster_job_sync

  metadata = connection.get("metadata", {}) or {}
  options = sync_options or {}

  realm_id = metadata.get("realm_id", "")
  connection_id = connection.get("connection_id", "")
  user_id = connection.get("user_id", "")
  full_rebuild = options.get("full_rebuild", False)
  lookback_days = options.get("lookback_days", 60)
  since_date = options.get("since_date", "") or ""

  if not realm_id:
    raise ValueError("QuickBooks realm_id not found in connection metadata")

  # Build config for all assets in the pipeline (they share QBSyncConfig).
  # `sync_lock_id` is plumbed through so `qb_load` can release the
  # B7 sync lock on completion rather than waiting for the TTL.
  sync_lock_id = options.get("sync_lock_id", "") or ""
  sync_config = {
    "graph_id": graph_id,
    "connection_id": connection_id,
    "user_id": user_id,
    "realm_id": realm_id,
    "full_rebuild": full_rebuild,
    "lookback_days": lookback_days,
    "since_date": since_date,
    "sync_lock_id": sync_lock_id,
  }

  run_config = {
    "ops": {
      "qb_extract": {"config": sync_config},
      "qb_transform": {"config": sync_config},
      "qb_load": {"config": sync_config},
    }
  }

  run_id = submit_dagster_job_sync(
    job_name="qb_sync",
    run_config=run_config,
    tags={
      "graph_id": graph_id,
      "connection_id": connection_id,
      "pipeline": "quickbooks",
    },
  )

  logger.info(
    f"QuickBooks sync submitted for graph={graph_id}, "
    f"connection={connection_id}, run_id={run_id}, "
    f"full_rebuild={full_rebuild}, since_date={since_date or '<lookback>'}"
  )
  return run_id


async def cleanup_quickbooks_connection(
  connection: dict[str, Any], graph_id: str
) -> None:
  """Clean up QuickBooks connection."""
  # QuickBooks cleanup would involve revoking OAuth tokens
  # For now, just log the cleanup
  logger.info(f"QuickBooks connection cleanup for entity {connection['entity_id']}")
