"""Plaid provider-specific operations."""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...logger import logger
from ...operations.connection_service import ConnectionService
from ...models.api.graphs.connections import PlaidConnectionConfig
from ...config import env


class PlaidProvider:
  """Handle Plaid-specific operations."""

  def __init__(self):
    self.environment = env.PLAID_ENVIRONMENT
    self.client_id = env.PLAID_CLIENT_ID
    self.client_secret = env.PLAID_CLIENT_SECRET

  def _get_plaid_client(self):
    """Initialize Plaid client based on environment."""
    try:
      from plaid.api import plaid_api
      from plaid.configuration import Configuration
      from plaid.api_client import ApiClient
      from plaid import Environment

      if self.environment == "sandbox":
        host = Environment.sandbox
      elif self.environment == "production":
        host = Environment.production
      else:
        host = Environment.sandbox

      configuration = Configuration(
        host=host,
        api_key={
          "clientId": self.client_id,
          "secret": self.client_secret,
        },
      )
      api_client = ApiClient(configuration)
      return plaid_api.PlaidApi(api_client)

    except ImportError:
      logger.warning("plaid-python not available, using mock client")
      return None

  async def create_link_token(self, entity_id: str, user_id: str) -> Tuple[str, str]:
    """
    Create a Plaid Link token.

    Args:
        entity_id: Entity identifier
        user_id: User identifier

    Returns:
        Tuple of (link_token, expiration)
    """
    plaid_client = self._get_plaid_client()

    if plaid_client:
      try:
        from plaid.model.link_token_create_request import LinkTokenCreateRequest
        from plaid.model.link_token_create_request_user import (
          LinkTokenCreateRequestUser,
        )
        from plaid.model.country_code import CountryCode
        from plaid.model.products import Products

        link_request = LinkTokenCreateRequest(
          products=[Products("transactions")],
          client_name="RoboLedger",
          country_codes=[CountryCode("US")],
          language="en",
          user=LinkTokenCreateRequestUser(client_user_id=user_id),
        )

        response = plaid_client.link_token_create(link_request)
        return response["link_token"], response["expiration"]

      except Exception as e:
        logger.error(f"Plaid Link token creation failed: {e}")

    # Fallback to mock for development
    link_token = f"link-{self.environment}-{entity_id}-{user_id}"
    expiration = datetime.now(timezone.utc).isoformat()
    return link_token, expiration

  async def exchange_public_token(
    self, public_token: str
  ) -> Tuple[str, str, List[Dict[str, Any]]]:
    """
    Exchange public token for access token.

    Args:
        public_token: Plaid public token

    Returns:
        Tuple of (access_token, item_id, accounts)
    """
    plaid_client = self._get_plaid_client()

    if plaid_client:
      try:
        from plaid.model.item_public_token_exchange_request import (
          ItemPublicTokenExchangeRequest,
        )
        from plaid.model.accounts_get_request import AccountsGetRequest

        # Exchange public token
        exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
        exchange_response = plaid_client.item_public_token_exchange(exchange_request)
        access_token = exchange_response["access_token"]
        item_id = exchange_response["item_id"]

        # Get account information
        accounts_request = AccountsGetRequest(access_token=access_token)
        accounts_response = plaid_client.accounts_get(accounts_request)
        plaid_accounts = accounts_response["accounts"]

        # Convert accounts to dict format
        accounts = []
        for acc in plaid_accounts:
          accounts.append(
            {
              "account_id": acc.account_id,
              "name": acc.name,
              "type": acc.type,
              "subtype": getattr(acc, "subtype", None),
              "mask": getattr(acc, "mask", None),
            }
          )

        return access_token, item_id, accounts

      except Exception as e:
        logger.error(f"Plaid token exchange failed: {e}")

    # Fallback to mock for development
    access_token = f"access-{self.environment}-{public_token[-8:]}"
    item_id = f"item-{public_token[-4:]}"
    accounts = []

    return access_token, item_id, accounts

  async def remove_item(self, access_token: str) -> bool:
    """
    Remove a Plaid item.

    Args:
        access_token: Plaid access token

    Returns:
        True if successful, False otherwise
    """
    if access_token.startswith("access-sandbox-"):
      logger.info("Skipping item removal for mock/sandbox token")
      return True

    plaid_client = self._get_plaid_client()

    if plaid_client:
      try:
        from plaid.model.item_remove_request import ItemRemoveRequest

        remove_request = ItemRemoveRequest(access_token=access_token)
        plaid_client.item_remove(remove_request)
        logger.info("Successfully removed Plaid item")
        return True

      except Exception as e:
        logger.warning(f"Failed to remove Plaid item: {e}")

    return False

  async def create_connection(
    self,
    entity_id: str,
    config: PlaidConnectionConfig,
    user_id: str,
    graph_id: str,
    db: Session,
  ) -> str:
    """Create Plaid connection."""
    # For Plaid, we create a pending connection that will be completed after token exchange

    metadata = {
      "status": "pending_link",
      "institution_name": config.institution.get("name")
      if config.institution
      else None,
    }

    connection_data = await ConnectionService.create_connection(
      entity_id=entity_id,
      provider="Plaid",
      user_id=user_id,
      credentials={},  # Will be populated after token exchange
      metadata=metadata,
      graph_id=graph_id,
    )

    return connection_data["connection_id"]

  async def sync_connection(
    self,
    connection: Dict[str, Any],
    sync_options: Optional[Dict[str, Any]],
    graph_id: str,
  ) -> str:
    """Trigger Plaid sync.

    TODO: Refactor to use Dagster pipeline.
    The Plaid sync has been migrated to Dagster assets:
    - See: robosystems/dagster/assets/plaid.py
    - Assets: plaid_accounts, plaid_transactions, plaid_graph_data
    """
    entity_id = connection["entity_id"]
    item_id = connection["metadata"].get("item_id")

    if not item_id:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Plaid connection not properly configured. Missing item_id.",
      )

    # TODO: Trigger Dagster pipeline
    # For now, return a placeholder - provider refactoring needed
    logger.warning(
      f"Plaid sync requested for entity {entity_id}, item {item_id} - "
      "provider needs refactoring to use Dagster pipeline"
    )
    return f"dagster-pending-{entity_id}"

  async def cleanup_connection(self, connection: Dict[str, Any], graph_id: str) -> None:
    """Clean up Plaid connection."""
    # Try to remove item from Plaid if we have credentials
    if connection.get("credentials", {}).get("access_token"):
      await self.remove_item(connection["credentials"]["access_token"])
