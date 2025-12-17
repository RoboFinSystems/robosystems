"""QuickBooks provider-specific operations."""

from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
import httpx

from ...logger import logger
from ...operations.connection_service import ConnectionService
from ...models.api.graphs.connections import QuickBooksConnectionConfig
from ...config import env
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
    return f"{self._auth_base_url}/oauth2/v1/tokens/bearer"

  @property
  def scopes(self) -> list[str]:
    return ["com.intuit.quickbooks.accounting"]

  def get_additional_auth_params(self) -> Dict[str, str]:
    """QuickBooks-specific auth parameters."""
    return {
      "access_type": "offline",  # To get refresh token
    }

  def extract_provider_data(self, callback_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract QuickBooks-specific data from callback."""
    return {
      "realm_id": callback_data.get("realmId", ""),
    }

  async def get_entity_info(self, access_token: str, realm_id: str) -> Dict[str, Any]:
    """Get QuickBooks entity information."""
    url = f"{self._base_url}/v3/entity/{realm_id}/entityinfo/{realm_id}"

    async with httpx.AsyncClient() as client:
      response = await client.get(
        url,
        headers={
          "Authorization": f"Bearer {access_token}",
          "Accept": "application/json",
        },
      )

      if response.status_code == 200:
        data = response.json()
        return data.get("EntityInfo", {})
      else:
        logger.error(f"Failed to get QuickBooks entity info: {response.text}")
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
    "realm_id": config.realm_id if config.realm_id else None,
  }

  connection_data = await ConnectionService.create_connection(
    entity_id=entity_id,
    provider="QuickBooks",
    user_id=user_id,
    credentials={},  # Will be populated after OAuth
    metadata=metadata,
    graph_id=graph_id,
  )

  return connection_data["connection_id"]


async def sync_quickbooks_connection(
  connection: Dict[str, Any], sync_options: Optional[Dict[str, Any]], graph_id: str
) -> str:
  """Trigger QuickBooks sync.

  TODO: Refactor to use Dagster pipeline.
  The QuickBooks sync has been migrated to Dagster assets:
  - See: robosystems/dagster/assets/quickbooks.py
  - Assets: qb_accounts, qb_transactions, qb_graph_data
  """
  entity_id = connection["entity_id"]

  # TODO: Trigger Dagster pipeline
  # For now, return a placeholder - provider refactoring needed
  logger.warning(
    f"QuickBooks sync requested for entity {entity_id}, graph {graph_id} - "
    "provider needs refactoring to use Dagster pipeline"
  )
  return f"dagster-pending-{entity_id}"


async def cleanup_quickbooks_connection(
  connection: Dict[str, Any], graph_id: str
) -> None:
  """Clean up QuickBooks connection."""
  # QuickBooks cleanup would involve revoking OAuth tokens
  # For now, just log the cleanup
  logger.info(f"QuickBooks connection cleanup for entity {connection['entity_id']}")
