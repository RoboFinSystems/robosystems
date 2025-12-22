"""QuickBooks Transactions Processor.

STUB: This processor is not currently in active use. Connection nodes and
ENTITY_HAS_CONNECTION relationships have been moved from LadybugDB to PostgreSQL.

When QuickBooks integration is reimplemented:
1. Query connections from PostgreSQL instead of LadybugDB
2. Use connection_id references instead of graph relationships
3. Update the graph ingestion logic for the new schema

The original implementation is preserved here as a reference for the data
transformation patterns and QuickBooks API interactions.
"""

from datetime import datetime
from typing import Any

from dateutil.relativedelta import relativedelta

from robosystems.adapters.quickbooks.client import QBClient
from robosystems.logger import logger


class QBTransactionsProcessor:
  """Process QuickBooks transactions for graph ingestion.

  This processor syncs QuickBooks data (accounts, transactions) into the
  graph database. It creates:
  - Entity nodes with QuickBooks company info
  - Taxonomy nodes for the QuickBooks chart of accounts
  - Element nodes for each account
  - Transaction and LineItem nodes for journal entries

  NOTE: Currently stubbed - connections are now in PostgreSQL.
  """

  def __init__(
    self,
    entity_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    user_id: str | None = None,
    realm_id: str | None = None,
    database_name: str | None = None,
    qb_credentials: dict[str, Any] | None = None,
  ):
    """Initialize the QuickBooks transactions processor.

    Args:
        entity_id: The entity identifier in the graph database
        start_date: Start date for transaction sync (YYYY-MM-DD)
        end_date: End date for transaction sync (YYYY-MM-DD)
        user_id: User ID for connection lookup
        realm_id: QuickBooks realm ID
        database_name: Target graph database name
        qb_credentials: QuickBooks OAuth credentials dict with
                       'access_token' and 'refresh_token'
    """
    if entity_id is None and user_id is None and realm_id is None:
      raise ValueError("Must provide either entity_id, user_id, or realm_id")

    self.entity_id = entity_id
    self.database_name = database_name
    self.realm_id = realm_id
    self.qb_credentials = qb_credentials
    self.qb: QBClient | None = None
    self._initialized = False

    # Date range for transaction sync
    if end_date is None:
      self.end_date = datetime.now().strftime("%Y-%m-%d")
    else:
      self.end_date = end_date

    if start_date is None:
      self.start_date = (
        datetime.strptime(self.end_date, "%Y-%m-%d") - relativedelta(years=10)
      ).strftime("%Y-%m-%d")
    else:
      self.start_date = start_date

    logger.info(
      f"QBTransactionsProcessor initialized for entity={entity_id}, "
      f"realm={realm_id}, date_range={self.start_date} to {self.end_date}"
    )

  def _initialize(self) -> None:
    """Initialize the QuickBooks client with credentials."""
    if self._initialized:
      return

    if not self.qb_credentials:
      raise ValueError(
        "QuickBooks credentials required. "
        "Connection lookup from PostgreSQL not yet implemented."
      )

    self.qb = QBClient(realm_id=self.realm_id, qb_credentials=self.qb_credentials)
    self._initialized = True

  def sync(self) -> None:
    """Sync QuickBooks data to the graph database.

    This method orchestrates the full sync process:
    1. Create/update entity with QuickBooks company info
    2. Create taxonomy for chart of accounts
    3. Create COA structure and account elements
    4. Sync transactions and line items

    Raises:
        NotImplementedError: Graph ingestion not yet reimplemented
    """
    raise NotImplementedError(
      "QuickBooks sync not yet reimplemented. Connections have moved to PostgreSQL."
    )

  async def sync_async(self) -> None:
    """Async version of sync for use in async contexts.

    Raises:
        NotImplementedError: Graph ingestion not yet reimplemented
    """
    raise NotImplementedError(
      "QuickBooks async sync not yet reimplemented. "
      "Connections have moved to PostgreSQL."
    )

  def refresh_sync(self) -> None:
    """Clear and re-sync all QuickBooks data.

    This deletes existing QB data for the entity and performs a fresh sync.

    Raises:
        NotImplementedError: Graph ingestion not yet reimplemented
    """
    raise NotImplementedError(
      "QuickBooks refresh sync not yet reimplemented. "
      "Connections have moved to PostgreSQL."
    )

  def get_entity_info(self) -> dict[str, Any]:
    """Get QuickBooks company info.

    Returns:
        Dict containing company name, address, and other metadata
    """
    self._initialize()
    companies = self.qb.get_entity_info()
    if companies:
      return companies[0].to_dict()
    return {}

  def get_accounts(self) -> list:
    """Get all QuickBooks accounts.

    Returns:
        List of account dictionaries
    """
    self._initialize()
    return self.qb.get_accounts()

  def get_transactions(
    self, start_date: str | None = None, end_date: str | None = None
  ) -> dict[str, Any]:
    """Get QuickBooks transactions for date range.

    Args:
        start_date: Start date (YYYY-MM-DD), defaults to self.start_date
        end_date: End date (YYYY-MM-DD), defaults to self.end_date

    Returns:
        Transaction report data from QuickBooks
    """
    self._initialize()
    return self.qb.get_transactions(
      start_date=start_date or self.start_date,
      end_date=end_date or self.end_date,
    )
