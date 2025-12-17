"""Plaid transaction processor for graph ingestion.

This processor handles:
- Bank account node creation and updates
- Transaction node creation, modification, and deletion
- Relationship management between accounts and transactions
"""

from typing import Any

from robosystems.adapters.plaid.processors.uri_utils import (
  plaid_account_element_uri,
  plaid_account_qname,
)
from robosystems.logger import logger
from robosystems.utils import generate_deterministic_uuid7


class PlaidTransactionsProcessor:
  """Process Plaid data for graph ingestion."""

  def __init__(self, entity_id: str):
    """Initialize the processor.

    Args:
        entity_id: The entity/graph ID for this processor
    """
    self.entity_id = entity_id

  async def process_accounts(
    self, repository: Any, accounts: list[dict[str, Any]]
  ) -> int:
    """Process and store bank accounts in the graph.

    Args:
        repository: Graph repository for executing queries
        accounts: List of Plaid account dictionaries

    Returns:
        Number of accounts processed
    """
    processed = 0
    for account in accounts:
      try:
        await self._create_or_update_bank_account(repository, account)
        processed += 1
      except Exception as e:
        logger.error(f"Error processing account {account.get('account_id')}: {e}")

    return processed

  async def process_transactions(
    self,
    repository: Any,
    added: list[dict[str, Any]],
    modified: list[dict[str, Any]],
    removed: list[str],
  ) -> dict[str, int]:
    """Process transaction changes from Plaid sync.

    Args:
        repository: Graph repository for executing queries
        added: List of new transactions
        modified: List of modified transactions
        removed: List of removed transaction IDs

    Returns:
        Dictionary with counts of added, modified, removed
    """
    counts = {"added": 0, "modified": 0, "removed": 0}

    # Process added transactions
    for transaction in added:
      try:
        await self._upsert_transaction(repository, transaction)
        counts["added"] += 1
      except Exception as e:
        logger.error(
          f"Error adding transaction {transaction.get('transaction_id')}: {e}"
        )

    # Process modified transactions
    for transaction in modified:
      try:
        await self._upsert_transaction(repository, transaction)
        counts["modified"] += 1
      except Exception as e:
        logger.error(
          f"Error modifying transaction {transaction.get('transaction_id')}: {e}"
        )

    # Process removed transactions
    for transaction_id in removed:
      try:
        await self._remove_transaction(repository, transaction_id)
        counts["removed"] += 1
      except Exception as e:
        logger.error(f"Error removing transaction {transaction_id}: {e}")

    return counts

  async def _create_or_update_bank_account(
    self, repository: Any, account: dict[str, Any]
  ) -> None:
    """Create or update bank account node in graph database."""
    account_id = account["account_id"]
    account_name = account["name"]
    account_type = account["type"]
    account_subtype = account.get("subtype", "")
    mask = account.get("mask", "")

    balances = account.get("balances", {})
    current_balance = balances.get("current", 0)
    available_balance = balances.get("available", 0)
    currency = balances.get("iso_currency_code", "USD")

    logger.info(
      f"Creating/updating bank account {account_id} for entity {self.entity_id}: {account_name}"
    )

    # Generate URI and QName for bank account element
    element_uri = plaid_account_element_uri(account_id)
    element_qname = (
      f"plaid:{plaid_account_qname(account_name, account_type, account_subtype)}"
    )

    # Determine account properties based on type
    if account_type.lower() in ["depository", "investment"]:
      period_type = "instant"  # Balance sheet accounts
      balance = "debit"  # Assets are typically debit balance
    elif account_type.lower() in ["credit", "loan"]:
      period_type = "instant"
      balance = "credit"  # Liabilities are credit balance
    else:
      period_type = "instant"
      balance = "debit"  # Default to debit

    cypher = """
        MERGE (e:Element {uri: $uri})
        SET e.qname = $qname,
            e.period_type = $period_type,
            e.balance = $balance,
            e.type = 'Monetary',
            e.is_abstract = false,
            e.is_dimension_item = false,
            e.is_domain_member = false,
            e.is_hypercube_item = false,
            e.is_integer = false,
            e.is_numeric = true,
            e.is_shares = false,
            e.is_fraction = false,
            e.is_textblock = false,
            e.substitution_group = 'http://www.xbrl.org/2003/instance#item',
            e.item_type = 'http://www.xbrl.org/2003/instance#monetaryItemType',
            e.classification = 'bank_account',
            e.plaid_account_id = $account_id,
            e.plaid_account_type = $account_type,
            e.plaid_subtype = $subtype,
            e.plaid_mask = $mask,
            e.current_balance = $current_balance,
            e.available_balance = $available_balance,
            e.currency = $currency
        RETURN e
        """

    params = {
      "uri": element_uri,
      "qname": element_qname,
      "period_type": period_type,
      "balance": balance,
      "account_id": account_id,
      "account_type": account_type,
      "subtype": account_subtype,
      "mask": mask,
      "current_balance": current_balance,
      "available_balance": available_balance,
      "currency": currency,
    }

    await repository.execute_query(cypher, params)
    logger.info(f"Successfully created/updated bank account element: {element_uri}")

  async def _upsert_transaction(
    self, repository: Any, transaction: dict[str, Any]
  ) -> None:
    """Create or update a transaction in the graph."""
    transaction_id = transaction["transaction_id"]
    account_id = transaction["account_id"]
    amount = float(transaction["amount"])
    date = transaction["date"]
    name = transaction["name"]
    merchant_name = transaction.get("merchant_name", "")
    pending = transaction.get("pending", False)

    logger.debug(
      f"Upserting transaction {transaction_id} for account {account_id}: "
      f"{name} (${amount}) on {date}"
    )

    # Generate a unique identifier for the transaction
    unique_id = generate_deterministic_uuid7(
      f"{self.entity_id}_{transaction_id}", namespace="plaid_transaction"
    )

    cypher = """
        MERGE (t:Transaction {identifier: $identifier})
        SET t.plaid_transaction_id = $transaction_id,
            t.plaid_account_id = $account_id,
            t.amount = $amount,
            t.date = $date,
            t.name = $name,
            t.merchant_name = $merchant_name,
            t.pending = $pending
        RETURN t
        """

    params = {
      "identifier": unique_id,
      "transaction_id": transaction_id,
      "account_id": account_id,
      "amount": amount,
      "date": date,
      "name": name,
      "merchant_name": merchant_name,
      "pending": pending,
    }

    await repository.execute_query(cypher, params)

  async def _remove_transaction(self, repository: Any, transaction_id: str) -> None:
    """Remove a transaction from the graph."""
    logger.info(f"Removing transaction {transaction_id} for entity {self.entity_id}")

    # Generate the same unique identifier used when creating
    unique_id = generate_deterministic_uuid7(
      f"{self.entity_id}_{transaction_id}", namespace="plaid_transaction"
    )

    cypher = """
        MATCH (t:Transaction {identifier: $identifier})
        DELETE t
        RETURN count(t) as deleted_count
        """

    result = await repository.execute_query(cypher, {"identifier": unique_id})

    if result and result[0].get("deleted_count", 0) > 0:
      logger.info(f"Successfully removed transaction {transaction_id}")
    else:
      logger.warning(f"Transaction not found for removal: {transaction_id}")
