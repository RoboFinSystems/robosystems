from typing import Optional
import asyncio
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.exceptions import ApiException
from plaid.configuration import Configuration
from plaid.api_client import ApiClient

from robosystems.logger import logger
from robosystems.database import session
from robosystems.operations.connection_service import ConnectionService
from robosystems.config import env
from ...celery import celery_app, QUEUE_DEFAULT


# Plaid auth will be handled through the new auth system
# Access tokens will be stored in entity connections or user settings


@celery_app.task(ignore_result=True, queue=QUEUE_DEFAULT, priority=10)
def sync_plaid_data(entity_id: str, item_id: str):
  """Sync Plaid transactions and account data for a entity"""
  logger.info(f"Starting Plaid sync for entity {entity_id}, item {item_id}")

  asyncio.run(_sync_plaid_data_async(entity_id, item_id))


async def _sync_plaid_data_async(entity_id: str, item_id: str):
  """Async implementation of Plaid sync"""
  try:
    # Ensure we have a fresh database session for the Celery worker
    session.remove()  # Remove any existing session

    # Entity validation will happen when we connect to the graph database
    # The entity_id is the graph_id for entity-specific databases

    # Get Plaid access token from ConnectionService
    try:
      # Find connection by item_id
      connections = await ConnectionService.list_connections(
        entity_id=entity_id,
        provider="Plaid",
        user_id="",  # System task - no specific user
      )

      # Find connection with matching item_id
      matching_connection = None
      for conn in connections:
        if conn["metadata"]["item_id"] == item_id:
          matching_connection = conn
          break

      if not matching_connection:
        logger.error(
          f"Plaid connection not found for entity {entity_id}, item {item_id}"
        )
        return

      # Get credentials from connection
      connection_details = await ConnectionService.get_connection(
        connection_id=matching_connection["connection_id"],
        user_id="",  # System task - no specific user
      )

      if not connection_details or not connection_details.get("credentials"):
        logger.error(
          f"Plaid credentials not found for entity {entity_id}, item {item_id}"
        )
        return

      access_token = connection_details["credentials"]["access_token"]

    except Exception as auth_error:
      logger.error(
        f"Failed to retrieve Plaid credentials for entity {entity_id}, item {item_id}: {auth_error}"
      )
      return

    # Initialize Plaid client
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
    plaid_client = plaid_api.PlaidApi(api_client)

    # Fetch accounts
    logger.info(f"Fetching accounts for item {item_id}")
    accounts_request = AccountsGetRequest(access_token=access_token)
    accounts_response = plaid_client.accounts_get(accounts_request)
    accounts = accounts_response["accounts"]
    logger.info(f"Retrieved {len(accounts)} accounts")

    # Sync transactions using cursor-based approach
    logger.info(f"Fetching transactions for item {item_id}")
    cursor = get_last_cursor(entity_id, item_id)

    # For initial sync (no cursor), omit cursor parameter
    if cursor is None:
      logger.info("No cursor found - performing initial sync")
      transactions_request = TransactionsSyncRequest(access_token=access_token)
    else:
      logger.info(f"Using cursor for incremental sync: {cursor}")
      transactions_request = TransactionsSyncRequest(
        access_token=access_token, cursor=cursor
      )

    transactions_response = plaid_client.transactions_sync(transactions_request)

    added = transactions_response["added"]
    modified = transactions_response["modified"]
    removed = transactions_response["removed"]
    next_cursor = transactions_response["next_cursor"]

    logger.info(
      f"Transaction sync: {len(added)} added, {len(modified)} modified, {len(removed)} removed"
    )

    # Process data into graph database
    import asyncio

    asyncio.run(
      process_plaid_data_to_graph(entity_id, accounts, added, modified, removed)
    )

    # Save cursor for next sync
    save_cursor(entity_id, item_id, next_cursor)

    logger.info(
      f"Successfully completed Plaid sync for entity {entity_id}, item {item_id}"
    )
    return True

  except ApiException as e:
    logger.error(f"Plaid API error for entity {entity_id}, item {item_id}: {e}")
    raise
  except Exception as e:
    logger.error(
      f"Unexpected error during Plaid sync for entity {entity_id}, item {item_id}: {e}"
    )
    raise
  finally:
    # Clean up database session
    session.remove()


def get_last_cursor(entity_id: str, item_id: str) -> Optional[str]:
  """Get last sync cursor for incremental updates"""
  try:
    # Get cursor from connection metadata
    connections = ConnectionService.list_connections(
      entity_id=entity_id,
      provider="Plaid",
      user_id="",  # System task - no specific user
    )

    # Find connection with matching item_id
    for conn in connections:
      if conn["metadata"]["item_id"] == item_id:
        return conn["metadata"].get("last_cursor")

    logger.info(f"No cursor found for entity {entity_id}, item {item_id}")
    return None

  except Exception as e:
    logger.warning(f"Failed to get cursor for entity {entity_id}, item {item_id}: {e}")
    return None


def save_cursor(entity_id: str, item_id: str, cursor: str):
  """Save sync cursor for next incremental update"""
  try:
    # Find the connection and update its metadata
    connections = ConnectionService.list_connections(
      entity_id=entity_id,
      provider="Plaid",
      user_id="",  # System task - no specific user
    )

    # Find connection with matching item_id
    target_connection = None
    for conn in connections:
      if conn["metadata"]["item_id"] == item_id:
        target_connection = conn
        break

    if not target_connection:
      logger.error(
        f"Could not find Plaid connection for entity {entity_id}, item {item_id}"
      )
      return

    # Update the connection metadata with the new cursor
    # TODO: This should be updated to use ConnectionService.update_connection
    # For now, we'll log that the cursor needs to be saved
    logger.warning(
      f"Cursor saving not implemented - needs ConnectionService update: {cursor}"
    )

  except Exception as e:
    logger.error(f"Failed to save cursor for entity {entity_id}, item {item_id}: {e}")
    # Don't raise - this shouldn't stop the sync process


async def process_plaid_data_to_graph(
  entity_id: str,
  accounts,
  added_transactions,
  modified_transactions,
  removed_transactions,
):
  """Process and store Plaid data in graph database"""
  from robosystems.middleware.graph import get_graph_repository

  logger.info(f"Processing Plaid data to graph database for entity {entity_id}")

  # Get graph repository for this entity
  repository = await get_graph_repository(entity_id, operation_type="write")

  try:
    async with repository:
      # Verify entity exists in graph
      entity_query = "MATCH (c:Entity {identifier: $entity_id}) RETURN c"
      entity_result = await repository.execute_single(
        entity_query, {"entity_id": entity_id}
      )

      if not entity_result:
        logger.error(f"Entity not found in graph: {entity_id}")
        return

      # Process accounts
      for account in accounts:
        account_id = account["account_id"]
        account_name = account["name"]
        account_type = account["type"]
        account_subtype = account.get("subtype", "")
        mask = account.get("mask", "")

        balances = account.get("balances", {})
        current_balance = balances.get("current", 0)
        available_balance = balances.get("available", 0)
        currency = balances.get("iso_currency_code", "USD")

        # Create or update bank account node in graph database
        await create_or_update_bank_account_graph(
          repository=repository,
          entity_id=entity_id,
          account_id=account_id,
          name=account_name,
          account_type=account_type,
          subtype=account_subtype,
          mask=mask,
          current_balance=current_balance,
          available_balance=available_balance,
          currency=currency,
        )

      # Process added transactions
      for transaction in added_transactions:
        await process_transaction_graph(repository, entity_id, transaction, "added")

      # Process modified transactions
      for transaction in modified_transactions:
        await process_transaction_graph(repository, entity_id, transaction, "modified")

      # Process removed transactions
      for transaction_id in removed_transactions:
        await remove_transaction_graph(repository, entity_id, transaction_id)

      logger.info(f"Successfully processed Plaid data for entity {entity_id}")

  except Exception as e:
    logger.error(
      f"Error processing Plaid data to graph database for entity {entity_id}: {e}"
    )
    raise


async def create_or_update_bank_account_graph(
  repository,
  entity_id: str,
  account_id: str,
  name: str,
  account_type: str,
  subtype: str,
  mask: str,
  current_balance: float,
  available_balance: float,
  currency: str,
):
  """Create or update bank account node in graph database"""
  try:
    logger.info(
      f"Creating/updating bank account {account_id} for entity {entity_id}: {name}"
    )

    # Generate URI and QName for bank account element
    element_uri = plaid_account_element_uri(account_id)
    element_qname = f"plaid:{plaid_account_qname(name, account_type, subtype)}"

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

    # Create or update Element using Cypher query
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
      "subtype": subtype,
      "mask": mask,
      "current_balance": current_balance,
      "available_balance": available_balance,
      "currency": currency,
    }

    await repository.execute_query(cypher, params)
    logger.info(f"Successfully created/updated bank account element: {element_uri}")

  except Exception as e:
    logger.error(f"Error creating/updating bank account {account_id}: {e}")
    raise


async def process_transaction_graph(
  repository, entity_id: str, transaction: dict, operation: str
):
  """Process a single transaction (added or modified)"""
  from robosystems.utils import generate_deterministic_uuid7

  try:
    transaction_id = transaction["transaction_id"]
    account_id = transaction["account_id"]
    amount = float(transaction["amount"])
    date = transaction["date"]
    name = transaction["name"]
    merchant_name = transaction.get("merchant_name", "")
    transaction.get("category", [])
    pending = transaction.get("pending", False)

    logger.info(
      f"{operation.capitalize()} transaction {transaction_id} for account {account_id}: {name} (${amount}) on {date}"
    )

    # Generate a unique identifier for the transaction
    unique_id = generate_deterministic_uuid7(
      f"{entity_id}_{transaction_id}", namespace="plaid_transaction"
    )

    # Create or update transaction using Cypher query
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
    logger.info(f"Processed transaction: {unique_id}")

  except Exception as e:
    logger.error(
      f"Error processing transaction {transaction.get('transaction_id', 'unknown')}: {e}"
    )
    # Don't raise - continue processing other transactions


async def remove_transaction_graph(repository, entity_id: str, transaction_id: str):
  """Remove a transaction from graph database"""
  from robosystems.utils import generate_deterministic_uuid7

  try:
    logger.info(f"Removing transaction {transaction_id} for entity {entity_id}")

    # Generate the same unique identifier used when creating
    unique_id = generate_deterministic_uuid7(
      f"{entity_id}_{transaction_id}", namespace="plaid_transaction"
    )

    # Delete transaction using Cypher query
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

  except Exception as e:
    logger.error(f"Error removing transaction {transaction_id}: {e}")
    raise


# Helper functions for Plaid URI generation


def plaid_account_element_uri(account_id: str) -> str:
  """Generate URI for Plaid account element"""
  return f"https://plaid.com/account/{account_id}#element"


def plaid_account_qname(name: str, account_type: str, subtype: str) -> str:
  """Generate QName for Plaid account element"""
  # Clean name similar to QB account name cleaning
  clean_name = name.replace(" ", "").replace("(", "").replace(")", "")
  clean_name = clean_name.replace(".", "").replace(",", "").replace(":", "")
  clean_name = clean_name.replace(";", "").replace("!", "").replace("?", "")
  clean_name = clean_name.replace("/", "").replace("\\", "").replace("|", "")
  clean_name = clean_name.replace("+", "").replace("=", "").replace("*", "")
  clean_name = clean_name.replace("@", "").replace("#", "").replace("$", "")
  clean_name = clean_name.replace("%", "").replace("^", "").replace("<", "")
  clean_name = clean_name.replace(">", "").replace("~", "").replace("`", "")
  clean_name = clean_name.replace("&", "")

  # Include account type and subtype for uniqueness
  if subtype:
    return f"{clean_name}_{account_type}_{subtype}".replace(" ", "")
  else:
    return f"{clean_name}_{account_type}".replace(" ", "")


def plaid_transaction_uri(transaction_id: str) -> str:
  """Generate URI for Plaid transaction"""
  return f"https://plaid.com/transaction/{transaction_id}"


def plaid_line_item_uri(transaction_id: str, line_number: int) -> str:
  """Generate URI for Plaid transaction line item"""
  return f"{plaid_transaction_uri(transaction_id)}/line-item/{line_number}"
