"""
QuickBooks Transactions Processor.

NOTE: This module is currently NOT IN USE. Connection nodes and ENTITY_HAS_CONNECTION
relationships have been removed from the graph schema. Platform metadata (users,
connections) is now stored exclusively in PostgreSQL.

When connections are re-implemented, they will be managed in PostgreSQL tables,
not in the Kuzu graph database. This file needs refactoring to:
1. Query connections from PostgreSQL instead of Kuzu
2. Remove all Connection node queries from Cypher
3. Use connection_id references instead of graph relationships
"""

import asyncio
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, Optional
from arelle import XbrlConst
from robosystems.logger import logger
from robosystems.adapters.qb import QBClient
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.utils import generate_deterministic_uuid7
from robosystems.config import URIConstants
from robosystems.operations.connection_service import ConnectionService, SYSTEM_USER_ID


class QBTransactionsProcessor:
  def __init__(
    self,
    entity_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    user_id: Optional[str] = None,
    realm_id: Optional[str] = None,
    database_name: Optional[str] = None,
    qb_credentials: Optional[Dict[str, Any]] = None,
  ):
    if entity_id is None and user_id is None and realm_id is None:
      raise Exception("Must provide either entity_id or user_id")

    self.entity_id = entity_id
    self.database_name = database_name or MultiTenantUtils.get_database_name(entity_id)
    self.repository = None  # Will be initialized in async methods
    MultiTenantUtils.log_database_operation(
      "QB transactions processor initialization", self.database_name, entity_id
    )

    # Entity information will be loaded when needed
    self.entity_result = None
    self.entity_node = None
    self.co_node = None  # Backward compatibility

    # If credentials are provided, we can skip connection lookup
    if qb_credentials:
      self.realm_id = realm_id
      self.qb = QBClient(realm_id=self.realm_id, qb_credentials=qb_credentials)
    else:
      # Get the QuickBooks connection for the entity
      # Handle multiple connections by finding the best active one
      connection_query = """
      MATCH (e:Entity)-[:ENTITY_HAS_CONNECTION]->(conn:Connection)
      WHERE e.identifier = $entity_id AND conn.provider = $provider
      RETURN conn ORDER BY conn.created_at DESC
      """
      conn_result = self.repository.execute_query(
        connection_query, {"entity_id": entity_id, "provider": "QuickBooks"}
      )

      if not conn_result:
        raise Exception(f"No active QuickBooks connection found for entity {entity_id}")

      # Find the best active connection from multiple connections
      qb_connection = self._select_best_connection(conn_result, entity_id)
      self.realm_id = qb_connection["realm_id"]

      # Initialize QBClient with credentials
      try:
        import asyncio

        connection_data = asyncio.run(
          ConnectionService.get_connection(
            connection_id=qb_connection["connection_id"],
            user_id=SYSTEM_USER_ID,  # System-level access
          )
        )
        if not connection_data or "credentials" not in connection_data:
          raise Exception(
            f"Failed to retrieve QuickBooks credentials for entity {entity_id}"
          )
        credentials = connection_data["credentials"]
        self.qb = QBClient(realm_id=self.realm_id, qb_credentials=credentials)
      except Exception as e:
        logger.error(
          f"Error getting credentials for {qb_connection['connection_id']}: {e}"
        )
        raise

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

  def _initialize_sync(self):
    """Initialize the processor for synchronous usage."""
    if self._initialized:
      return

    # If credentials are provided, we can skip connection lookup
    if self.qb_credentials:
      self.qb = QBClient(realm_id=self.realm_id, qb_credentials=self.qb_credentials)
    else:
      # Get the QuickBooks connection for the entity
      # Handle multiple connections by finding the best active one
      connection_query = """
      MATCH (e:Entity)-[:ENTITY_HAS_CONNECTION]->(conn:Connection)
      WHERE e.identifier = $entity_id AND conn.provider = $provider
      RETURN conn ORDER BY conn.created_at DESC
      """
      conn_result = self.repository.execute_query(
        connection_query, {"entity_id": self.entity_id, "provider": "QuickBooks"}
      )

      if not conn_result:
        raise Exception(
          f"No active QuickBooks connection found for entity {self.entity_id}"
        )

      # Find the best active connection from multiple connections
      qb_connection = self._select_best_connection_sync(conn_result, self.entity_id)
      self.realm_id = qb_connection["realm_id"]

      # Initialize QBClient with credentials
      try:
        connection_data = asyncio.run(
          ConnectionService.get_connection(
            connection_id=qb_connection["connection_id"],
            user_id=SYSTEM_USER_ID,  # System-level access
          )
        )
        if not connection_data or "credentials" not in connection_data:
          raise Exception(
            f"Failed to retrieve QuickBooks credentials for entity {self.entity_id}"
          )
        credentials = connection_data["credentials"]
        self.qb = QBClient(realm_id=self.realm_id, qb_credentials=credentials)
      except Exception as e:
        logger.error(
          f"Error getting credentials for {qb_connection['connection_id']}: {e}"
        )
        raise

    self._initialized = True

  async def _initialize_async(self):
    """Initialize the processor for async usage."""
    if self._initialized:
      return

    # If credentials are provided, we can skip connection lookup
    if self.qb_credentials:
      self.qb = QBClient(realm_id=self.realm_id, qb_credentials=self.qb_credentials)
    else:
      # Get the QuickBooks connection for the entity
      # Handle multiple connections by finding the best active one
      connection_query = """
      MATCH (e:Entity)-[:ENTITY_HAS_CONNECTION]->(conn:Connection)
      WHERE e.identifier = $entity_id AND conn.provider = $provider
      RETURN conn ORDER BY conn.created_at DESC
      """
      conn_result = self.repository.execute_query(
        connection_query, {"entity_id": self.entity_id, "provider": "QuickBooks"}
      )

      if not conn_result:
        raise Exception(
          f"No active QuickBooks connection found for entity {self.entity_id}"
        )

      # Find the best active connection from multiple connections
      qb_connection = await self._select_best_connection_async(
        conn_result, self.entity_id
      )
      self.realm_id = qb_connection["realm_id"]

      # Initialize QBClient with credentials
      try:
        connection_data = await ConnectionService.get_connection(
          connection_id=qb_connection["connection_id"],
          user_id=SYSTEM_USER_ID,  # System-level access
        )
        if not connection_data or "credentials" not in connection_data:
          raise Exception(
            f"Failed to retrieve QuickBooks credentials for entity {self.entity_id}"
          )
        credentials = connection_data["credentials"]
        self.qb = QBClient(realm_id=self.realm_id, qb_credentials=credentials)
      except Exception as e:
        logger.error(
          f"Error getting credentials for {qb_connection['connection_id']}: {e}"
        )
        raise

    self._initialized = True

  def sync(self):
    """Sync QuickBooks transactions using Kuzu operations."""
    logger.info(f"Starting QB transactions sync for entity: {self.entity_id}")

    # Initialize if not already done
    if not self._initialized:
      self._initialize_sync()

    self.make_entity()
    self.make_taxonomy()
    self.make_coa_structure()
    self.make_coa_associations()
    self.sync_transactions(self.start_date, self.end_date)

    logger.info("QB transactions sync completed")

  async def sync_async(self):
    """Async version of sync method for use in async contexts."""
    logger.info(f"Starting async QB transactions sync for entity: {self.entity_id}")

    # Initialize if not already done
    if not self._initialized:
      await self._initialize_async()

    # Note: Kuzu operations are synchronous, so we just call the sync method
    self.make_entity()
    self.make_taxonomy()
    self.make_coa_structure()
    self.make_coa_associations()
    self.sync_transactions(self.start_date, self.end_date)

    logger.info("Async QB transactions sync completed")

  def make_entity(self):
    companies = self.qb.get_entity_info()
    for entity in companies:
      entity_dict = entity.to_dict()

      # Update entity node with QuickBooks data
      update_query = """
      MATCH (e:Entity)
      WHERE e.identifier = $entity_id
      SET e.uri = $uri,
          e.qb_id = $qb_id,
          e.name = $name,
          e.legal_name = $legal_name,
          e.address = $address,
          e.city = $city,
          e.state = $state,
          e.zip = $zip,
          e.country = $country
      RETURN e
      """

      params = {
        "entity_id": self.entity_id,
        "uri": rl_entity_uri(entity_dict["Id"]),
        "qb_id": self.realm_id,
        "name": entity_dict["EntityName"],
        "legal_name": entity_dict["LegalName"],
        "address": entity_dict["EntityAddr"]["Line1"],
        "city": entity_dict["EntityAddr"]["City"],
        "state": entity_dict["EntityAddr"]["CountrySubDivisionCode"],
        "zip": entity_dict["EntityAddr"]["PostalCode"],
        "country": entity_dict["EntityAddr"]["Country"],
      }

      self.repository.execute_query(update_query, params)
      self.entity_connection()

  def entity_connection(self):
    connection_uri = qb_entity_uri(self.realm_id)

    # Check if connection exists
    check_query = "MATCH (conn:Connection) WHERE conn.uri = $uri RETURN conn"
    existing_conn = self.repository.execute_query(check_query, {"uri": connection_uri})

    if not existing_conn:
      # Create new connection
      create_query = """
      CREATE (conn:Connection {
        uri: $uri,
        provider: $provider,
        connection_id: $connection_id,
        realm_id: $realm_id,
        status: $status
      })
      RETURN conn
      """

      params = {
        "uri": connection_uri,
        "provider": "QuickBooks",
        "connection_id": f"quickbooks_{self.realm_id}",
        "realm_id": self.realm_id,
        "status": "connected",
      }

      self.repository.execute_query(create_query, params)

      # Connect to entity
      connect_query = """
      MATCH (e:Entity), (conn:Connection)
      WHERE e.identifier = $entity_id AND conn.uri = $connection_uri
      CREATE (e)-[:ENTITY_HAS_CONNECTION]->(conn)
      """

      self.repository.execute_query(
        connect_query, {"entity_id": self.entity_id, "connection_uri": connection_uri}
      )

  def make_taxonomy(self):
    taxonomy_uri = qb_entity_uri(self.realm_id)

    # Check if taxonomy exists
    check_query = "MATCH (t:Taxonomy) WHERE t.uri = $uri RETURN t"
    existing_taxonomy = self.repository.execute_query(
      check_query, {"uri": taxonomy_uri}
    )

    if not existing_taxonomy:
      # Create new taxonomy
      create_query = """
      CREATE (t:Taxonomy {
        uri: $uri,
        version: $version,
        location: $location
      })
      RETURN t
      """

      params = {"uri": taxonomy_uri, "version": "QuickBooks", "location": taxonomy_uri}

      self.repository.execute_query(create_query, params)

    # Connect entity to taxonomy if not already connected
    connect_query = """
    MATCH (e:Entity), (t:Taxonomy)
    WHERE e.identifier = $entity_id AND t.uri = $taxonomy_uri
    MERGE (e)-[:HAS_TAXONOMY]->(t)
    """

    self.repository.execute_query(
      connect_query, {"entity_id": self.entity_id, "taxonomy_uri": taxonomy_uri}
    )

    self.taxonomy_uri = taxonomy_uri

  def make_coa_structure(self):
    structure_uri = qb_chart_of_accounts_uri(self.realm_id)

    # Create or update structure
    merge_query = """
    MERGE (s:Structure {uri: $uri})
    SET s.identifier = $identifier,
        s.network_uri = $network_uri,
        s.type = $type,
        s.name = $name
    RETURN s
    """

    params = {
      "uri": structure_uri,
      "identifier": generate_deterministic_uuid7(structure_uri, namespace="structure"),
      "network_uri": qb_coa_network_uri(self.realm_id),
      "type": "ChartOfAccounts",
      "name": "Chart Of Accounts",
    }

    self.repository.execute_query(merge_query, params)

    # Connect to taxonomy
    connect_query = """
    MATCH (s:Structure), (t:Taxonomy)
    WHERE s.uri = $structure_uri AND t.uri = $taxonomy_uri
    MERGE (s)-[:HAS_TAXONOMY]->(t)
    """

    self.repository.execute_query(
      connect_query, {"structure_uri": structure_uri, "taxonomy_uri": self.taxonomy_uri}
    )

    self.coa_struct_uri = structure_uri

  def make_coa_associations(self):
    root_element_uri = self.make_coa_root_element()
    root_order = 1
    accounts_df = self.qb.get_accounts_df()

    for _, r in accounts_df.iterrows():
      parent_ref = r["ParentRef"]
      child_element_uri = self.make_element(r)

      if parent_ref is None:
        parent_element_uri = root_element_uri
        root_type = True
      else:
        parent_acct = self.qb.get_account_by_id(parent_ref["value"])
        parent_element_uri = self.make_element(parent_acct)
        root_type = False

      # Check if association already exists
      check_query = """
      MATCH (s:Structure)-[:HAS_ASSOCIATION]->(a:Association)
      WHERE s.uri = $structure_uri AND
            a.from_element_uri = $parent_uri AND
            a.to_element_uri = $child_uri
      RETURN a
      """

      existing_assoc = self.repository.execute_query(
        check_query,
        {
          "structure_uri": self.coa_struct_uri,
          "parent_uri": parent_element_uri,
          "child_uri": child_element_uri,
        },
      )

      if not existing_assoc:
        # Create new association
        create_query = """
        CREATE (a:Association {
          arcrole: $arcrole,
          order_value: $order_value,
          sequence: $sequence,
          association_type: $association_type,
          root: $root,
          preferred_label: $preferred_label,
          from_element_uri: $parent_uri,
          to_element_uri: $child_uri
        })
        RETURN a
        """

        params = {
          "arcrole": XbrlConst.parentChild,
          "order_value": int(r["Order"]),
          "sequence": int(r["Sequence"]),
          "association_type": "Presentation",
          "root": root_type,
          "preferred_label": "http://www.xbrl.org/2003/role/terseLabel",
          "parent_uri": parent_element_uri,
          "child_uri": child_element_uri,
        }

        _assoc_result = self.repository.execute_query(create_query, params)

        # Connect association to structure and elements
        connect_query = """
        MATCH (s:Structure), (a:Association), (pe:Element), (ce:Element)
        WHERE s.uri = $structure_uri AND
              a.from_element_uri = $parent_uri AND
              a.to_element_uri = $child_uri AND
              pe.uri = $parent_uri AND
              ce.uri = $child_uri
        CREATE (s)-[:HAS_ASSOCIATION]->(a)
        CREATE (a)-[:ASSOCIATION_FROM_ELEMENT]->(pe)
        CREATE (a)-[:ASSOCIATION_TO_ELEMENT]->(ce)
        """

        self.repository.execute_query(
          connect_query,
          {
            "structure_uri": self.coa_struct_uri,
            "parent_uri": parent_element_uri,
            "child_uri": child_element_uri,
          },
        )
      else:
        # Update existing association
        update_query = """
        MATCH (s:Structure)-[:HAS_ASSOCIATION]->(a:Association)
        WHERE s.uri = $structure_uri AND
              a.from_element_uri = $parent_uri AND
              a.to_element_uri = $child_uri
        SET a.arcrole = $arcrole,
            a.order_value = $order_value,
            a.sequence = $sequence,
            a.association_type = $association_type,
            a.root = $root,
            a.preferred_label = $preferred_label
        """

        params = {
          "structure_uri": self.coa_struct_uri,
          "parent_uri": parent_element_uri,
          "child_uri": child_element_uri,
          "arcrole": XbrlConst.parentChild,
          "order_value": int(r["Order"]),
          "sequence": int(r["Sequence"]),
          "association_type": "Presentation",
          "root": root_type,
          "preferred_label": "http://www.xbrl.org/2003/role/terseLabel",
        }

        self.repository.execute_query(update_query, params)

      root_order += 1

  def make_coa_root_element(self):
    uri, qname = rl_coa_root_element_uri()

    # Create or update root element
    merge_query = """
    MERGE (e:Element {uri: $uri})
    SET e.qname = $qname,
        e.period_type = $period_type,
        e.type = $type,
        e.is_abstract = $is_abstract,
        e.is_dimension_item = $is_dimension_item,
        e.is_domain_member = $is_domain_member,
        e.is_hypercube_item = $is_hypercube_item,
        e.is_integer = $is_integer,
        e.is_numeric = $is_numeric,
        e.is_shares = $is_shares,
        e.is_fraction = $is_fraction,
        e.is_textblock = $is_textblock,
        e.substitution_group = $substitution_group,
        e.item_type = $item_type
    RETURN e
    """

    params = {
      "uri": uri,
      "qname": qname,
      "period_type": "duration",
      "type": "String",
      "is_abstract": True,
      "is_dimension_item": False,
      "is_domain_member": True,
      "is_hypercube_item": False,
      "is_integer": False,
      "is_numeric": True,
      "is_shares": False,
      "is_fraction": False,
      "is_textblock": False,
      "substitution_group": "http://www.xbrl.org/2003/instance#item",
      "item_type": "http://www.xbrl.org/2003/instance#stringItemType",
    }

    self.repository.execute_query(merge_query, params)
    return uri

  def make_element(self, acct):
    domain = acct["domain"].lower()
    acct_name = qb_stripped_account_name(acct["FullyQualifiedName"])
    element_uri = qb_element_uri(self.realm_id, acct["FullyQualifiedName"])
    acct_qname = f"{domain}:{acct_name}"

    # Determine period type based on classification
    if acct["Classification"] in ["Asset", "Liability", "Equity"]:
      period_type = "instant"
    elif acct["Classification"] in [
      "Revenue",
      "Expense",
      "Other Income",
      "Other Expense",
    ]:
      period_type = "duration"
    else:
      period_type = "instant"  # Default

    # Determine balance type based on classification
    if acct["Classification"] in ["Asset", "Expense", "Other Expense"]:
      balance = "debit"
    elif acct["Classification"] in [
      "Liability",
      "Equity",
      "Revenue",
      "Other Income",
    ]:
      balance = "credit"
    else:
      balance = "debit"  # Default

    # Create or update element
    merge_query = """
    MERGE (e:Element {uri: $uri})
    SET e.qname = $qname,
        e.period_type = $period_type,
        e.balance = $balance,
        e.type = $type,
        e.is_abstract = $is_abstract,
        e.is_dimension_item = $is_dimension_item,
        e.is_domain_member = $is_domain_member,
        e.is_hypercube_item = $is_hypercube_item,
        e.is_integer = $is_integer,
        e.is_numeric = $is_numeric,
        e.is_shares = $is_shares,
        e.is_fraction = $is_fraction,
        e.is_textblock = $is_textblock,
        e.substitution_group = $substitution_group,
        e.item_type = $item_type,
        e.classification = $classification
    RETURN e
    """

    params = {
      "uri": element_uri,
      "qname": acct_qname,
      "period_type": period_type,
      "balance": balance,
      "type": "Monetary",
      "is_abstract": False,
      "is_dimension_item": False,
      "is_domain_member": False,
      "is_hypercube_item": False,
      "is_integer": False,
      "is_numeric": True,
      "is_shares": False,
      "is_fraction": False,
      "is_textblock": False,
      "substitution_group": "http://www.xbrl.org/2003/instance#item",
      "item_type": "http://www.xbrl.org/2003/instance#monetaryItemType",
      "classification": "account",
    }

    self.repository.execute_query(merge_query, params)
    return element_uri

  def sync_transactions(self, start_date=None, end_date=None):
    tx_date = None
    tx_type = None
    tx_id = None
    tx_uri = None
    tx_li_num = 1

    transactions = self.qb.get_transactions(start_date=start_date, end_date=end_date)
    if (
      not transactions
      or "Rows" not in transactions
      or "Row" not in transactions["Rows"]
    ):
      logger.warning("No transaction data returned from QuickBooks")
      return

    for row in transactions["Rows"]["Row"]:
      if "Summary" in row.keys():
        tx_date = None
        tx_type = None
        tx_id = None
        tx_uri = None
        tx_li_num = 1
        continue

      if not tx_date:
        tx_date = row["ColData"][0]["value"]
      if not tx_type:
        tx_type = row["ColData"][1]["value"]
      if not tx_date or not tx_type:
        continue
      if not tx_id:
        tx_id = row["ColData"][1]["id"]
      if not tx_uri:
        tx_uri = qb_transaction_uri(self.realm_id, tx_type, tx_id)

      tx_hash = generate_deterministic_uuid7(
        json.dumps(row), namespace="transaction_hash"
      )

      # Check if transaction exists and if sync hash matches
      check_tx_query = (
        "MATCH (t:Transaction) WHERE t.uri = $uri RETURN t.sync_hash as sync_hash"
      )
      existing_tx = self.repository.execute_query(check_tx_query, {"uri": tx_uri})

      if existing_tx and existing_tx[0]["sync_hash"] == tx_hash:
        continue

      # Create or update transaction
      tx_date_parsed = datetime.strptime(tx_date, "%Y-%m-%d").date()
      merge_tx_query = """
      MERGE (t:Transaction {uri: $uri})
      SET t.date = $date,
          t.type = $type,
          t.number = $number,
          t.sync_hash = $sync_hash
      RETURN t
      """

      tx_params = {
        "uri": tx_uri,
        "date": tx_date_parsed.isoformat(),
        "type": tx_type,
        "number": tx_id,
        "sync_hash": tx_hash,
      }

      self.repository.execute_query(merge_tx_query, tx_params)

      # Connect transaction to entity
      connect_tx_query = """
      MATCH (e:Entity), (t:Transaction)
      WHERE e.identifier = $entity_id AND t.uri = $tx_uri
      MERGE (e)-[:ENTITY_HAS_TRANSACTION]->(t)
      """

      self.repository.execute_query(
        connect_tx_query, {"entity_id": self.entity_id, "tx_uri": tx_uri}
      )

      li_uri = qb_line_item_uri(self.realm_id, tx_type, tx_id, tx_li_num)

      account_name = row["ColData"][5]["value"]
      element_uri = qb_element_uri(self.realm_id, account_name)

      # Check if element exists
      check_element_query = "MATCH (e:Element) WHERE e.uri = $uri RETURN e"
      element_exists = self.repository.execute_query(
        check_element_query, {"uri": element_uri}
      )

      if not element_exists:
        logger.warning(f"Element not found: {element_uri}")
        continue

      li_debit_amt = row["ColData"][6]["value"]
      if li_debit_amt == "":
        li_debit_amt = 0
      li_credit_amt = row["ColData"][7]["value"]
      if li_credit_amt == "":
        li_credit_amt = 0

      # Create or update line item
      merge_li_query = """
      MERGE (li:LineItem {uri: $uri})
      SET li.description = $description,
          li.debit_amount = $debit_amount,
          li.credit_amount = $credit_amount
      RETURN li
      """

      li_params = {
        "uri": li_uri,
        "description": row["ColData"][4]["value"],
        "debit_amount": float(li_debit_amt),
        "credit_amount": float(li_credit_amt),
      }

      self.repository.execute_query(merge_li_query, li_params)

      # Connect line item to element
      connect_li_element_query = """
      MATCH (li:LineItem), (e:Element)
      WHERE li.uri = $li_uri AND e.uri = $element_uri
      MERGE (li)-[:HAS_ELEMENT]->(e)
      """

      self.repository.execute_query(
        connect_li_element_query, {"li_uri": li_uri, "element_uri": element_uri}
      )

      # Connect transaction to line item
      connect_tx_li_query = """
      MATCH (t:Transaction), (li:LineItem)
      WHERE t.uri = $tx_uri AND li.uri = $li_uri
      MERGE (t)-[:HAS_LINE_ITEM]->(li)
      """

      self.repository.execute_query(
        connect_tx_li_query, {"tx_uri": tx_uri, "li_uri": li_uri}
      )

      tx_li_num += 1

  def refresh_sync(self):
    """Refresh sync data using Kuzu operations."""
    logger.info(f"Refreshing QB sync data for entity: {self.entity_id}")

    taxonomy_uri = qb_entity_uri(self.realm_id)

    # Delete associations connected to structures connected to the taxonomy
    delete_assoc_query = """
    MATCH (t:Taxonomy)-[:HAS_STRUCTURE]-(s:Structure)-[:HAS_ASSOCIATION]-(a:Association)
    WHERE t.uri = $taxonomy_uri
    DETACH DELETE a
    """

    self.repository.execute_query(delete_assoc_query, {"taxonomy_uri": taxonomy_uri})

    # Delete structures connected to the taxonomy
    delete_struct_query = """
    MATCH (t:Taxonomy)-[:HAS_STRUCTURE]-(s:Structure)
    WHERE t.uri = $taxonomy_uri
    DETACH DELETE s
    """

    self.repository.execute_query(delete_struct_query, {"taxonomy_uri": taxonomy_uri})

    # Delete the taxonomy
    delete_tax_query = """
    MATCH (t:Taxonomy)
    WHERE t.uri = $taxonomy_uri
    DETACH DELETE t
    """

    self.repository.execute_query(delete_tax_query, {"taxonomy_uri": taxonomy_uri})

    # Delete line items connected to transactions for this entity
    delete_li_query = """
    MATCH (e:Entity)-[:ENTITY_HAS_TRANSACTION]-(t:Transaction)-[:HAS_LINE_ITEM]-(li:LineItem)
    WHERE e.identifier = $entity_id
    DETACH DELETE li
    """

    self.repository.execute_query(delete_li_query, {"entity_id": self.entity_id})

    # Delete transactions for this entity
    delete_tx_query = """
    MATCH (e:Entity)-[:ENTITY_HAS_TRANSACTION]-(t:Transaction)
    WHERE e.identifier = $entity_id
    DETACH DELETE t
    """

    self.repository.execute_query(delete_tx_query, {"entity_id": self.entity_id})

    logger.info("QB sync data refresh completed")

  def _select_best_connection_sync(self, connections, entity_id):
    """
    Select the best QuickBooks connection from multiple available connections (sync version).

    Priority order:
    1. Active connections with valid credentials
    2. Most recently created connection
    3. Connection with most recent successful sync

    Args:
        connections: List of connection objects from query result
        entity_id: Entity identifier for logging

    Returns:
        dict: Best connection object

    Raises:
        Exception: If no valid connection is found
    """
    if not connections:
      raise Exception(f"No connections provided for entity {entity_id}")

    valid_connections = []

    for conn_row in connections:
      conn = conn_row["conn"]

      # Check if connection has required fields
      if not conn.get("connection_id") or not conn.get("realm_id"):
        logger.warning(
          f"Connection {conn.get('connection_id')} missing required fields"
        )
        continue

      # Test connection validity by checking credentials
      try:
        connection_data = asyncio.run(
          ConnectionService.get_connection(
            connection_id=conn["connection_id"],
            user_id=SYSTEM_USER_ID,
          )
        )

        if connection_data and "credentials" in connection_data:
          # Add credential validation score
          conn["_validity_score"] = self._calculate_connection_score(
            conn, connection_data
          )
          valid_connections.append(conn)
          logger.debug(f"Valid connection found: {conn['connection_id']}")
        else:
          logger.warning(f"Connection {conn['connection_id']} has invalid credentials")

      except Exception as e:
        logger.warning(f"Error validating connection {conn.get('connection_id')}: {e}")
        continue

    if not valid_connections:
      # If no valid connections, return the most recent one and let error handling deal with it
      logger.warning(
        f"No valid connections found for entity {entity_id}, using most recent"
      )
      return connections[0]["conn"]

    # Sort by validity score (highest first) and creation time (newest first)
    valid_connections.sort(
      key=lambda x: (x.get("_validity_score", 0), x.get("created_at", "")), reverse=True
    )

    best_connection = valid_connections[0]
    logger.info(
      f"Selected connection {best_connection['connection_id']} for entity {entity_id} "
      f"(score: {best_connection.get('_validity_score', 0)})"
    )

    return best_connection

  async def _select_best_connection_async(self, connections, entity_id):
    """
    Select the best QuickBooks connection from multiple available connections (async version).

    Priority order:
    1. Active connections with valid credentials
    2. Most recently created connection
    3. Connection with most recent successful sync

    Args:
        connections: List of connection objects from query result
        entity_id: Entity identifier for logging

    Returns:
        dict: Best connection object

    Raises:
        Exception: If no valid connection is found
    """
    if not connections:
      raise Exception(f"No connections provided for entity {entity_id}")

    valid_connections = []

    for conn_row in connections:
      conn = conn_row["conn"]

      # Check if connection has required fields
      if not conn.get("connection_id") or not conn.get("realm_id"):
        logger.warning(
          f"Connection {conn.get('connection_id')} missing required fields"
        )
        continue

      # Test connection validity by checking credentials
      try:
        connection_data = await ConnectionService.get_connection(
          connection_id=conn["connection_id"],
          user_id=SYSTEM_USER_ID,
        )

        if connection_data and "credentials" in connection_data:
          # Add credential validation score
          conn["_validity_score"] = self._calculate_connection_score(
            conn, connection_data
          )
          valid_connections.append(conn)
          logger.debug(f"Valid connection found: {conn['connection_id']}")
        else:
          logger.warning(f"Connection {conn['connection_id']} has invalid credentials")

      except Exception as e:
        logger.warning(f"Error validating connection {conn.get('connection_id')}: {e}")
        continue

    if not valid_connections:
      # If no valid connections, return the most recent one and let error handling deal with it
      logger.warning(
        f"No valid connections found for entity {entity_id}, using most recent"
      )
      return connections[0]["conn"]

    # Sort by validity score (highest first) and creation time (newest first)
    valid_connections.sort(
      key=lambda x: (x.get("_validity_score", 0), x.get("created_at", "")), reverse=True
    )

    best_connection = valid_connections[0]
    logger.info(
      f"Selected connection {best_connection['connection_id']} for entity {entity_id} "
      f"(score: {best_connection.get('_validity_score', 0)})"
    )

    return best_connection

  def _calculate_connection_score(self, conn, connection_data=None):
    """Calculate a score for connection preference."""
    score = 0

    # Base score for having valid credentials
    score += 10

    # Bonus if connection_data has valid credentials
    if connection_data and "credentials" in connection_data:
      score += 5

    # Bonus for recent activity
    if conn.get("last_sync_at"):
      try:
        from datetime import datetime, timezone

        last_sync = datetime.fromisoformat(conn["last_sync_at"])
        days_since_sync = (datetime.now(timezone.utc) - last_sync).days
        if days_since_sync < 7:
          score += 5
        elif days_since_sync < 30:
          score += 2
      except Exception:
        pass

    # Bonus for having additional metadata
    if conn.get("entity_name"):
      score += 1

    # Penalty for error states
    if conn.get("status") == "error":
      score -= 5

    return score


def rl_entity_uri(entity_id):
  entity_uri = f"{URIConstants.ROBOSYSTEMS_BASE_URI}/api/entity/{entity_id}"
  return entity_uri


def qb_entity_uri(realm_id):
  entity_uri = f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}"
  return entity_uri


def qb_chart_of_accounts_uri(realm_id):
  chart_of_accounts_uri = (
    f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}#ChartOfAccounts"
  )
  return chart_of_accounts_uri


def qb_coa_network_uri(realm_id):
  coa_network_uri = (
    f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/role/ChartOfAccounts"
  )
  return coa_network_uri


def qb_transaction_uri(realm_id, tx_type, tx_id):
  tx_type = tx_type.replace(" ", "")
  tx_type = tx_type.replace("(Check)", "Check")
  transaction_uri = f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/transaction/{tx_type}/{tx_id}"
  return transaction_uri


def qb_line_item_uri(realm_id, tx_type, tx_id, li_num):
  line_item_uri = f"{qb_transaction_uri(realm_id, tx_type, tx_id)}/line-item/{li_num}"
  return line_item_uri


def qb_element_uri(realm_id, element_name):
  acct_name = qb_stripped_account_name(element_name)
  element_uri = (
    f"{URIConstants.QUICKBOOKS_BASE_URI}/entity/{realm_id}/element#{acct_name}"
  )
  return element_uri


def qb_stripped_account_name(acct_name):
  acct_name = acct_name.title()
  acct_name = acct_name.replace(" ", "")
  acct_name = acct_name.split("(")[0].strip()
  symbol_list = [
    "&",
    "(",
    ")",
    ".",
    ",",
    ":",
    ";",
    "!",
    "?",
    "/",
    "\\",
    "|",
    "+",
    "=",
    "*",
    "@",
    "#",
    "$",
    "%",
    "^",
    "<",
    ">",
    "~",
    "`",
  ]
  for symbol in symbol_list:
    acct_name = acct_name.replace(symbol, "")

  return acct_name


def rl_coa_root_element_uri():
  qname = "ChartOfAccountsAbstract"
  uri = f"{URIConstants.ROBOSYSTEMS_BASE_URI}/taxonomy#{qname}"
  return uri, qname
