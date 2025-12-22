"""Plaid banking data pipeline Dagster assets.

This module defines the Plaid data pipeline as Dagster assets:
1. plaid_accounts - Fetch bank accounts from Plaid
2. plaid_transactions - Sync transactions using cursor-based pagination
3. plaid_graph_data - Ingest banking data into graph database

The pipeline leverages existing adapters:
- robosystems.adapters.plaid.PlaidClient - Plaid API client
- robosystems.adapters.plaid.PlaidTransactionsProcessor - Graph ingestion

Migration Notes:
- Replaces: robosystems.tasks.data_sync.plaid.sync_plaid_data
- Uses cursor-based sync for incremental transaction updates
"""

from typing import Any

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  MetadataValue,
  asset,
)

from robosystems.adapters.plaid import PlaidClient, PlaidTransactionsProcessor
from robosystems.dagster.resources import DatabaseResource, GraphResource


class PlaidSyncConfig(Config):
  """Configuration for Plaid sync execution."""

  entity_id: str  # Entity/graph ID to sync data for
  item_id: str  # Plaid item ID for the linked account
  graph_id: str | None = None  # Optional graph ID for multi-tenant


@asset(
  group_name="plaid_pipeline",
  description="Fetch bank accounts from Plaid for a linked item",
  compute_kind="extract",
)
def plaid_accounts(
  context: AssetExecutionContext,
  config: PlaidSyncConfig,
  db: DatabaseResource,
) -> MaterializeResult:
  """Fetch linked bank accounts from Plaid.

  This asset retrieves all bank accounts associated with a Plaid item,
  including current balances and account metadata.

  Returns:
      MaterializeResult with account information
  """
  from robosystems.operations.connection_service import ConnectionService

  context.log.info(
    f"Fetching accounts for entity {config.entity_id}, item {config.item_id}"
  )

  # Get Plaid access token from ConnectionService
  import asyncio

  async def get_access_token() -> str | None:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="Plaid",
      user_id="",  # System task
    )

    for conn in connections:
      if conn.get("metadata", {}).get("item_id") == config.item_id:
        connection_details = await ConnectionService.get_connection(
          connection_id=conn["connection_id"],
          user_id="",
        )
        if connection_details and connection_details.get("credentials"):
          return connection_details["credentials"].get("access_token")
    return None

  access_token = asyncio.get_event_loop().run_until_complete(get_access_token())

  if not access_token:
    context.log.error(
      f"Plaid credentials not found for entity {config.entity_id}, item {config.item_id}"
    )
    return MaterializeResult(
      metadata={
        "status": "error",
        "reason": "Credentials not found",
      }
    )

  # Initialize Plaid client and fetch accounts
  client = PlaidClient(access_token)
  accounts = client.get_accounts()

  context.log.info(f"Retrieved {len(accounts)} accounts")

  # Extract account summaries for metadata
  account_summaries = [
    {
      "account_id": acc.get("account_id"),
      "name": acc.get("name"),
      "type": acc.get("type"),
      "subtype": acc.get("subtype"),
      "current_balance": acc.get("balances", {}).get("current"),
    }
    for acc in accounts
  ]

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "item_id": config.item_id,
      "account_count": len(accounts),
      "accounts": MetadataValue.json(account_summaries),
    }
  )


@asset(
  group_name="plaid_pipeline",
  description="Sync transactions from Plaid using cursor-based pagination",
  compute_kind="extract",
  deps=[plaid_accounts],
)
def plaid_transactions(
  context: AssetExecutionContext,
  config: PlaidSyncConfig,
  db: DatabaseResource,
) -> MaterializeResult:
  """Sync bank transactions from Plaid.

  Uses Plaid's cursor-based sync API for efficient incremental updates.
  Handles added, modified, and removed transactions.

  Returns:
      MaterializeResult with transaction sync statistics
  """
  from robosystems.operations.connection_service import ConnectionService

  context.log.info(
    f"Syncing transactions for entity {config.entity_id}, item {config.item_id}"
  )

  # Get access token
  import asyncio

  async def get_credentials() -> tuple[str | None, str | None]:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="Plaid",
      user_id="",
    )

    for conn in connections:
      if conn.get("metadata", {}).get("item_id") == config.item_id:
        connection_details = await ConnectionService.get_connection(
          connection_id=conn["connection_id"],
          user_id="",
        )
        if connection_details and connection_details.get("credentials"):
          return (
            connection_details["credentials"].get("access_token"),
            connection_details.get("metadata", {}).get("sync_cursor"),
          )
    return None, None

  access_token, cursor = asyncio.get_event_loop().run_until_complete(get_credentials())

  if not access_token:
    context.log.error("Plaid credentials not found")
    return MaterializeResult(
      metadata={"status": "error", "reason": "Credentials not found"}
    )

  # Initialize client and sync transactions
  client = PlaidClient(access_token)

  total_added = 0
  total_modified = 0
  total_removed = 0
  all_transactions: list[dict[str, Any]] = []

  # Paginate through all available transactions
  has_more = True
  while has_more:
    sync_result = client.sync_transactions(cursor)

    total_added += len(sync_result["added"])
    total_modified += len(sync_result["modified"])
    total_removed += len(sync_result["removed"])

    all_transactions.extend(sync_result["added"])

    cursor = sync_result["next_cursor"]
    has_more = sync_result.get("has_more", False)

  context.log.info(
    f"Transaction sync complete: {total_added} added, "
    f"{total_modified} modified, {total_removed} removed"
  )

  # Save cursor for next sync (would be stored in connection metadata)
  # This would be done via ConnectionService.update_connection()

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "item_id": config.item_id,
      "transactions_added": total_added,
      "transactions_modified": total_modified,
      "transactions_removed": total_removed,
      "next_cursor": cursor,
    }
  )


@asset(
  group_name="plaid_pipeline",
  description="Ingest Plaid banking data into graph database",
  compute_kind="load",
  deps=[plaid_transactions],
)
async def plaid_graph_data(
  context: AssetExecutionContext,
  config: PlaidSyncConfig,
  db: DatabaseResource,
  graph: GraphResource,
) -> MaterializeResult:
  """Ingest Plaid data into the graph database.

  Uses PlaidTransactionsProcessor to transform and load:
  - Bank accounts as Account nodes
  - Transactions as Transaction nodes with line items
  - Relationships between accounts and transactions

  Returns:
      MaterializeResult with ingestion statistics
  """
  from robosystems.middleware.graph import get_graph_repository
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.operations.connection_service import ConnectionService

  context.log.info(f"Ingesting Plaid data for entity {config.entity_id}")

  # Get database context
  database_name = MultiTenantUtils.get_database_name(config.graph_id)

  # Get credentials and fetch fresh data
  async def get_credentials() -> tuple[str | None, str | None]:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="Plaid",
      user_id="",
    )

    for conn in connections:
      if conn.get("metadata", {}).get("item_id") == config.item_id:
        connection_details = await ConnectionService.get_connection(
          connection_id=conn["connection_id"],
          user_id="",
        )
        if connection_details and connection_details.get("credentials"):
          return (
            connection_details["credentials"].get("access_token"),
            connection_details.get("metadata", {}).get("sync_cursor"),
          )
    return None, None

  access_token, cursor = await get_credentials()

  if not access_token:
    return MaterializeResult(
      metadata={"status": "error", "reason": "Credentials not found"}
    )

  # Fetch accounts and transactions
  client = PlaidClient(access_token)
  accounts = client.get_accounts()
  sync_result = client.sync_transactions(cursor)

  # Initialize processor
  processor = PlaidTransactionsProcessor(entity_id=config.entity_id)

  # Get graph repository for database operations
  repository = await get_graph_repository(database_name, operation_type="write")

  async with repository:
    # Process accounts
    accounts_processed = await processor.process_accounts(repository, accounts)

    # Process transactions
    transaction_counts = await processor.process_transactions(
      repository,
      added=sync_result["added"],
      modified=sync_result["modified"],
      removed=[t["transaction_id"] for t in sync_result.get("removed", [])],
    )

  context.log.info(
    f"Ingestion complete: {accounts_processed} accounts, "
    f"{transaction_counts['added']} transactions added"
  )

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "graph_id": config.graph_id or database_name,
      "accounts_processed": accounts_processed,
      "transactions_added": transaction_counts["added"],
      "transactions_modified": transaction_counts["modified"],
      "transactions_removed": transaction_counts["removed"],
    }
  )
