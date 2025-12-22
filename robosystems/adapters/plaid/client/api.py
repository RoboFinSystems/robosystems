"""Plaid API client for banking data integration.

This client handles:
- Plaid API authentication and configuration
- Accounts retrieval
- Transaction synchronization with cursor-based pagination
"""

from typing import Any

import plaid
from plaid.api import plaid_api
from plaid.api_client import ApiClient
from plaid.configuration import Configuration
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest

from robosystems.config import env
from robosystems.logger import logger


class PlaidClient:
  """Plaid API client with environment-aware configuration."""

  def __init__(self, access_token: str):
    """Initialize Plaid client.

    Args:
        access_token: The Plaid access token for the linked account
    """
    self.access_token = access_token
    self._client = self._create_client()

  def _create_client(self) -> plaid_api.PlaidApi:
    """Create and configure the Plaid API client."""
    plaid_env = env.PLAID_ENVIRONMENT
    if plaid_env == "sandbox":
      host = plaid.Environment.Sandbox
    elif plaid_env == "production":
      host = plaid.Environment.Production
    else:
      host = plaid.Environment.Sandbox

    configuration = Configuration(
      host=host,
      api_key={
        "clientId": env.PLAID_CLIENT_ID,
        "secret": env.PLAID_CLIENT_SECRET,
      },
    )
    api_client = ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)

  def get_accounts(self) -> list[dict[str, Any]]:
    """Fetch all linked accounts.

    Returns:
        List of account dictionaries with balances
    """
    logger.info("Fetching accounts from Plaid")
    request = AccountsGetRequest(access_token=self.access_token)
    response = self._client.accounts_get(request)
    accounts = response["accounts"]
    logger.info(f"Retrieved {len(accounts)} accounts")
    return accounts

  def sync_transactions(self, cursor: str | None = None) -> dict[str, Any]:
    """Sync transactions using cursor-based pagination.

    Args:
        cursor: Optional cursor from previous sync for incremental updates

    Returns:
        Dictionary with added, modified, removed transactions and next cursor
    """
    if cursor is None:
      logger.info("No cursor found - performing initial sync")
      request = TransactionsSyncRequest(access_token=self.access_token)
    else:
      logger.info(f"Using cursor for incremental sync: {cursor}")
      request = TransactionsSyncRequest(access_token=self.access_token, cursor=cursor)

    response = self._client.transactions_sync(request)

    result = {
      "added": response["added"],
      "modified": response["modified"],
      "removed": response["removed"],
      "next_cursor": response["next_cursor"],
      "has_more": response.get("has_more", False),
    }

    logger.info(
      f"Transaction sync: {len(result['added'])} added, "
      f"{len(result['modified'])} modified, {len(result['removed'])} removed"
    )

    return result
