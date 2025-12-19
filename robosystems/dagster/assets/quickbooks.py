"""QuickBooks accounting data pipeline Dagster assets.

This module defines the QuickBooks data pipeline as Dagster assets:
1. qb_accounts - Fetch chart of accounts from QuickBooks
2. qb_transactions - Fetch transactions (invoices, bills, payments)
3. qb_graph_data - Ingest accounting data into graph database

The pipeline leverages existing adapters:
- robosystems.adapters.quickbooks.QBClient - QuickBooks API client
- robosystems.adapters.quickbooks.QBTransactionsProcessor - Graph ingestion

Migration Notes:
- Replaces: robosystems.tasks.data_sync.qb.sync_task
- Handles OAuth token refresh automatically
"""

from decimal import Decimal
from typing import Any

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  MetadataValue,
  asset,
)

from robosystems.adapters.quickbooks import QBTransactionsProcessor
from robosystems.dagster.resources import DatabaseResource, GraphResource


class QuickBooksSyncConfig(Config):
  """Configuration for QuickBooks sync execution."""

  entity_id: str  # Entity identifier in QuickBooks
  graph_id: str | None = None  # Graph database ID for multi-tenant
  sync_type: str = "full"  # 'full' or 'incremental'


@asset(
  group_name="quickbooks_pipeline",
  description="Fetch chart of accounts from QuickBooks",
  compute_kind="extract",
)
def qb_accounts(
  context: AssetExecutionContext,
  config: QuickBooksSyncConfig,
  db: DatabaseResource,
) -> MaterializeResult:
  """Fetch chart of accounts from QuickBooks.

  Retrieves all accounts including:
  - Asset accounts (bank, AR, inventory)
  - Liability accounts (AP, credit cards)
  - Equity accounts
  - Revenue and expense accounts

  Returns:
      MaterializeResult with account information
  """
  from robosystems.adapters.quickbooks import QBClient
  from robosystems.operations.connection_service import (
    SYSTEM_USER_ID,
    ConnectionService,
  )

  context.log.info(f"Fetching QuickBooks accounts for entity {config.entity_id}")

  # Get QuickBooks credentials
  import asyncio

  async def get_qb_credentials() -> tuple[dict[str, Any] | None, str | None]:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="QuickBooks",
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connections:
      return None, None

    connection_id = connections[0]["connection_id"]
    connection_details = await ConnectionService.get_connection(
      connection_id=connection_id,
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connection_details or not connection_details.get("credentials"):
      return None, None

    return (
      connection_details["credentials"],
      connection_details.get("metadata", {}).get("realm_id"),
    )

  credentials, realm_id = asyncio.get_event_loop().run_until_complete(
    get_qb_credentials()
  )

  if not credentials or not realm_id:
    context.log.error(f"QuickBooks credentials not found for entity {config.entity_id}")
    return MaterializeResult(
      metadata={
        "status": "error",
        "reason": "Credentials not found",
      }
    )

  # Initialize QuickBooks client and fetch accounts
  client = QBClient(
    realm_id=realm_id,
    access_token=credentials.get("access_token"),
    refresh_token=credentials.get("refresh_token"),
  )

  accounts = client.get_accounts()

  context.log.info(f"Retrieved {len(accounts)} accounts from QuickBooks")

  # Summarize by account type
  account_type_counts: dict[str, int] = {}
  for acc in accounts:
    acc_type = acc.get("AccountType", "Unknown")
    account_type_counts[acc_type] = account_type_counts.get(acc_type, 0) + 1

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "realm_id": realm_id,
      "account_count": len(accounts),
      "account_types": MetadataValue.json(account_type_counts),
    }
  )


@asset(
  group_name="quickbooks_pipeline",
  description="Fetch transactions from QuickBooks (invoices, bills, payments)",
  compute_kind="extract",
  deps=[qb_accounts],
)
def qb_transactions(
  context: AssetExecutionContext,
  config: QuickBooksSyncConfig,
  db: DatabaseResource,
) -> MaterializeResult:
  """Fetch transactions from QuickBooks.

  Retrieves various transaction types:
  - Invoices and invoice payments
  - Bills and bill payments
  - Expenses and purchases
  - Journal entries
  - Deposits and transfers

  Returns:
      MaterializeResult with transaction statistics
  """
  from robosystems.adapters.quickbooks import QBClient
  from robosystems.operations.connection_service import (
    SYSTEM_USER_ID,
    ConnectionService,
  )

  context.log.info(f"Fetching QuickBooks transactions for entity {config.entity_id}")

  # Get credentials
  import asyncio

  async def get_qb_credentials() -> tuple[dict[str, Any] | None, str | None]:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="QuickBooks",
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connections:
      return None, None

    connection_details = await ConnectionService.get_connection(
      connection_id=connections[0]["connection_id"],
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connection_details or not connection_details.get("credentials"):
      return None, None

    return (
      connection_details["credentials"],
      connection_details.get("metadata", {}).get("realm_id"),
    )

  credentials, realm_id = asyncio.get_event_loop().run_until_complete(
    get_qb_credentials()
  )

  if not credentials or not realm_id:
    return MaterializeResult(
      metadata={"status": "error", "reason": "Credentials not found"}
    )

  # Initialize client
  client = QBClient(
    realm_id=realm_id,
    access_token=credentials.get("access_token"),
    refresh_token=credentials.get("refresh_token"),
  )

  # Fetch different transaction types
  transaction_counts: dict[str, int] = {}

  # Invoices
  invoices = client.get_invoices()
  transaction_counts["Invoice"] = len(invoices)

  # Bills
  bills = client.get_bills()
  transaction_counts["Bill"] = len(bills)

  # Payments
  payments = client.get_payments()
  transaction_counts["Payment"] = len(payments)

  # Expenses
  expenses = client.get_expenses()
  transaction_counts["Expense"] = len(expenses)

  total_transactions = sum(transaction_counts.values())

  context.log.info(f"Retrieved {total_transactions} transactions from QuickBooks")

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "realm_id": realm_id,
      "total_transactions": total_transactions,
      "transaction_types": MetadataValue.json(transaction_counts),
    }
  )


@asset(
  group_name="quickbooks_pipeline",
  description="Ingest QuickBooks data into graph database",
  compute_kind="load",
  deps=[qb_transactions],
)
def qb_graph_data(
  context: AssetExecutionContext,
  config: QuickBooksSyncConfig,
  db: DatabaseResource,
  graph: GraphResource,
) -> MaterializeResult:
  """Ingest QuickBooks data into the graph database.

  Uses QBTransactionsProcessor to transform and load:
  - Accounts as Account nodes
  - Transactions as Transaction nodes with line items
  - Vendors, customers as Entity nodes
  - Relationships between entities

  Returns:
      MaterializeResult with ingestion statistics
  """
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.operations.connection_service import (
    SYSTEM_USER_ID,
    ConnectionService,
  )
  from robosystems.operations.graph.credit_service import CreditService

  context.log.info(f"Ingesting QuickBooks data for entity {config.entity_id}")

  # Get database context
  database_name = MultiTenantUtils.get_database_name(config.graph_id)

  # Get credentials
  import asyncio

  async def get_qb_credentials() -> tuple[dict[str, Any] | None, str | None]:
    connections = await ConnectionService.list_connections(
      entity_id=config.entity_id,
      provider="QuickBooks",
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connections:
      return None, None

    connection_details = await ConnectionService.get_connection(
      connection_id=connections[0]["connection_id"],
      user_id=SYSTEM_USER_ID,
      graph_id=config.graph_id,
    )

    if not connection_details or not connection_details.get("credentials"):
      return None, None

    return (
      connection_details["credentials"],
      connection_details.get("metadata", {}).get("realm_id"),
    )

  credentials, realm_id = asyncio.get_event_loop().run_until_complete(
    get_qb_credentials()
  )

  if not credentials or not realm_id:
    return MaterializeResult(
      metadata={"status": "error", "reason": "Credentials not found"}
    )

  # Initialize processor and sync
  processor = QBTransactionsProcessor(
    entity_id=config.entity_id,
    realm_id=realm_id,
    qb_credentials=credentials,
    database_name=database_name,
  )

  # Run sync
  processor.sync()

  context.log.info(f"QuickBooks sync completed for entity {config.entity_id}")

  # Consume credits for sync
  if config.graph_id:
    try:
      with db.get_session() as session:
        credit_service = CreditService(session)
        credit_service.consume_credits(
          graph_id=config.graph_id,
          operation_type="connection_sync",
          base_cost=Decimal("10.0"),
          metadata={
            "provider": "QuickBooks",
            "entity_id": config.entity_id,
            "sync_type": config.sync_type,
            "source": "dagster_pipeline",
          },
        )
        context.log.info("Credits consumed for QuickBooks sync")
    except Exception as e:
      context.log.warning(f"Failed to consume credits: {e}")

  return MaterializeResult(
    metadata={
      "entity_id": config.entity_id,
      "graph_id": config.graph_id or database_name,
      "realm_id": realm_id,
      "sync_type": config.sync_type,
      "status": "completed",
    }
  )
