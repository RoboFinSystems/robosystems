"""Generic OLTP loader — reads dbt output from DuckDB and inserts into PostgreSQL.

Connector-agnostic: the same code loads QuickBooks, Xero, NetSuite, or any
future adapter. All connector-specific transformation happens in dbt staging
models; the loader only sees the standardized OLTP output tables.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from robosystems.logger import logger


def _parse_metadata(raw) -> dict:
  """Parse metadata from dbt output — may be a dict, JSON string, or None."""
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      parsed = json.loads(raw)
      return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
      return {}
  return {}


@dataclass
class LoadResult:
  """Result of an OLTP load operation."""

  graph_id: str
  source: str
  connection_id: str
  accounts: int = 0
  transactions: int = 0
  entries: int = 0
  line_items: int = 0
  dimensions: int = 0
  errors: list[str] = field(default_factory=list)

  @property
  def total_rows(self) -> int:
    return (
      self.accounts
      + self.transactions
      + self.entries
      + self.line_items
      + self.dimensions
    )


class OLTPLoader:
  """Loads dbt OLTP output tables into the roboledger PostgreSQL database.

  Generic for all connectors — reads from DuckDB, resolves foreign keys,
  inserts into tenant schema. Same code for QB, Xero, NetSuite.

  The load is atomic: all existing data for the given source + connection_id
  is deleted and re-inserted in a single transaction.
  """

  def load(
    self,
    graph_id: str,
    source: str,
    connection_id: str,
    duckdb_path: str | Path,
    created_by: str,
  ) -> LoadResult:
    """Load dbt OLTP output into the roboledger tenant schema.

    Args:
        graph_id: The graph ID (maps to a PostgreSQL schema).
        source: The data source name (e.g., 'quickbooks', 'xero').
        connection_id: The connection ID for data isolation.
        duckdb_path: Path to the dbt output DuckDB database.
        created_by: User ID for audit trail.

    Returns:
        LoadResult with row counts per table.
    """
    import duckdb

    from robosystems.db.extensions import extensions_session, provision_tenant_schema
    from robosystems.models.extensions import (
      Account,
      Dimension,
      Entry,
      LineItem,
      Transaction,
    )
    from robosystems.utils.ulid import generate_prefixed_ulid

    result = LoadResult(graph_id=graph_id, source=source, connection_id=connection_id)

    # Ensure tenant schema exists
    provision_tenant_schema(graph_id)

    # Read dbt output tables from DuckDB
    # Use fetchall() instead of fetchdf() to avoid pyarrow/DuckDB segfault
    # in containerized Dagster workers where both libraries coexist.
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
      existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
      dbt_data: dict[str, list[dict]] = {}
      for table in ["accounts", "transactions", "entries", "line_items", "dimensions"]:
        if table in existing_tables:
          result_set = con.execute(f"SELECT * FROM {table}")
          columns = [desc[0] for desc in result_set.description]
          rows = result_set.fetchall()
          dbt_data[table] = [dict(zip(columns, row, strict=True)) for row in rows]
          logger.info(f"Read {len(dbt_data[table])} rows from dbt table '{table}'")
        else:
          logger.warning(f"Table '{table}' not found in DuckDB, skipping")
    finally:
      con.close()

    now = datetime.now(UTC)

    with extensions_session(graph_id) as session:
      # Delete existing data for this source + connection_id (reverse FK order)
      session.query(LineItem).filter(
        LineItem.entry_id.in_(
          session.query(Entry.id).filter(
            Entry.transaction_id.in_(
              session.query(Transaction.id).filter(
                Transaction.source == source,
                Transaction.connection_id == connection_id,
              )
            )
          )
        )
      ).delete(synchronize_session=False)

      session.query(Entry).filter(
        Entry.transaction_id.in_(
          session.query(Transaction.id).filter(
            Transaction.source == source,
            Transaction.connection_id == connection_id,
          )
        )
      ).delete(synchronize_session=False)

      session.query(Transaction).filter(
        Transaction.source == source,
        Transaction.connection_id == connection_id,
      ).delete(synchronize_session=False)

      # TODO: Account lacks connection_id column — if a graph has multiple
      # connections of the same source type, this deletes accounts from all of them.
      # Add Account.connection_id in a future migration to scope correctly.
      session.query(Account).filter(
        Account.external_source == source,
      ).delete(synchronize_session=False)

      session.query(Dimension).filter(
        Dimension.metadata_.contains({"source": source}),
      ).delete(synchronize_session=False)

      session.flush()
      logger.info(
        f"Deleted existing data for source={source}, connection_id={connection_id}"
      )

      # --- INSERT accounts ---
      account_lookup: dict[str, str] = {}  # external_id → oltp_id
      external_parent_map: dict[str, str] = {}  # oltp_id → external_parent_id

      if "accounts" in dbt_data:
        rows = dbt_data["accounts"]
        account_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("acct")
          ext_id = str(row["external_id"])
          account_lookup[ext_id] = oltp_id

          ext_parent = row.get("external_parent_id")
          if ext_parent and str(ext_parent) not in ("", "None", "nan"):
            external_parent_map[oltp_id] = str(ext_parent)

          account_objects.append(
            Account(
              id=oltp_id,
              code=str(row["code"]),
              name=str(row["name"]),
              description=str(row["description"]) if row.get("description") else None,
              classification=str(row["classification"]),
              sub_classification=str(row["sub_classification"])
              if row.get("sub_classification")
              else None,
              balance_type=str(row["balance_type"]),
              parent_id=None,  # resolved in second pass
              depth=int(row.get("depth", 0)),
              path=str(row.get("path", "")),
              currency=str(row.get("currency", "USD")),
              is_active=bool(row.get("is_active", True)),
              is_placeholder=bool(row.get("is_placeholder", False)),
              external_id=ext_id,
              external_source=str(row["external_source"]),
              metadata_=_parse_metadata(row.get("metadata")),
              version=1,
              created_at=now,
              updated_at=now,
              created_by=created_by,
            )
          )

        session.add_all(account_objects)
        session.flush()

        # Second pass: resolve parent_id
        for oltp_id, ext_parent_id in external_parent_map.items():
          parent_oltp_id = account_lookup.get(ext_parent_id)
          if parent_oltp_id:
            session.query(Account).filter(Account.id == oltp_id).update(
              {"parent_id": parent_oltp_id}, synchronize_session=False
            )

        session.flush()
        result.accounts = len(rows)
        logger.info(f"Inserted {result.accounts} accounts")

      # --- INSERT transactions ---
      transaction_lookup: dict[str, str] = {}  # external_id → oltp_id

      if "transactions" in dbt_data:
        rows = dbt_data["transactions"]
        txn_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("txn")
          ext_id = str(row["external_id"])
          transaction_lookup[ext_id] = oltp_id

          txn_objects.append(
            Transaction(
              id=oltp_id,
              number=str(row["number"]) if row.get("number") else None,
              idempotency_key=f"{source}:{connection_id}:{ext_id}",
              type=str(row["type"]),
              category=str(row["category"]) if row.get("category") else None,
              amount=int(row["amount"]),
              currency=str(row.get("currency", "USD")),
              date=row["date"],
              due_date=row.get("due_date"),
              merchant_name=str(row["merchant_name"])
              if row.get("merchant_name")
              else None,
              reference_number=str(row["reference_number"])
              if row.get("reference_number")
              else None,
              description=str(row["description"]) if row.get("description") else None,
              source=source,
              source_id=str(row.get("source_id", ext_id)),
              connection_id=connection_id,
              status=str(row.get("status", "posted")),
              posted_at=now,
              metadata_={},
              version=1,
              created_at=now,
              updated_at=now,
              created_by=created_by,
            )
          )

        session.add_all(txn_objects)
        session.flush()
        result.transactions = len(rows)
        logger.info(f"Inserted {result.transactions} transactions")

      # --- INSERT entries ---
      entry_lookup: dict[str, str] = {}  # external_id → oltp_id

      if "entries" in dbt_data:
        rows = dbt_data["entries"]
        entry_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("je")
          ext_id = str(row["external_id"])
          entry_lookup[ext_id] = oltp_id

          # Resolve transaction FK
          ext_txn_id = str(row.get("external_transaction_id", ext_id))
          txn_oltp_id = transaction_lookup.get(ext_txn_id)

          if not txn_oltp_id:
            result.errors.append(f"Entry references unknown transaction: {ext_txn_id}")
            continue

          entry_objects.append(
            Entry(
              id=oltp_id,
              number=str(row["number"]) if row.get("number") else None,
              idempotency_key=f"{source}:{connection_id}:{ext_id}",
              transaction_id=txn_oltp_id,
              type=str(row.get("type", "standard")),
              posting_date=row["posting_date"],
              memo=str(row["memo"]) if row.get("memo") else None,
              status=str(row.get("status", "posted")),
              posted_at=now,
              metadata_={},
              version=1,
              created_at=now,
              updated_at=now,
              created_by=created_by,
            )
          )

        session.add_all(entry_objects)
        session.flush()
        result.entries = len(rows)
        logger.info(f"Inserted {result.entries} entries")

      # --- INSERT line_items ---
      if "line_items" in dbt_data:
        rows = dbt_data["line_items"]
        li_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("li")

          # Resolve FKs
          entry_ext_id = str(row["entry_external_id"])
          account_ext_id = str(row["account_external_id"])
          entry_oltp_id = entry_lookup.get(entry_ext_id)
          account_oltp_id = account_lookup.get(account_ext_id)

          # Skip zero-amount lines (QB tax/memo lines)
          debit = int(row["debit_amount"])
          credit = int(row["credit_amount"])
          if debit == 0 and credit == 0:
            continue

          if not entry_oltp_id:
            result.errors.append(f"LineItem references unknown entry: {entry_ext_id}")
            continue
          if not account_oltp_id:
            result.errors.append(
              f"LineItem references unknown account: {account_ext_id}"
            )
            continue

          li_objects.append(
            LineItem(
              id=oltp_id,
              entry_id=entry_oltp_id,
              account_id=account_oltp_id,
              debit_amount=debit,
              credit_amount=credit,
              description=str(row["description"]) if row.get("description") else None,
              line_order=int(row.get("line_order", 0)),
              metadata_={},
              created_at=now,
              updated_at=now,
            )
          )

        session.add_all(li_objects)
        session.flush()
        result.line_items = len(li_objects)
        logger.info(f"Inserted {result.line_items} line items")

      # --- INSERT dimensions ---
      # TODO: populate line_item_dimensions junction table to link
      # line items to their dimensions (needed for graph materialization)
      if "dimensions" in dbt_data:
        rows = dbt_data["dimensions"]
        dim_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("dim")

          dim_objects.append(
            Dimension(
              id=oltp_id,
              dimension_type=str(row["dimension_type"]),
              name=str(row["name"]),
              value=str(row["value"]),
              metadata_={"source": source},
              is_active=True,
              created_at=now,
              updated_at=now,
            )
          )

        session.add_all(dim_objects)
        session.flush()
        result.dimensions = len(rows)
        logger.info(f"Inserted {result.dimensions} dimensions")

    # Update entity with connector CompanyInfo (if available)
    self._update_entity_from_company_info(
      graph_id=graph_id,
      source=source,
      connection_id=connection_id,
      duckdb_path=duckdb_path,
    )

    logger.info(
      f"OLTP load complete for graph={graph_id}, source={source}: "
      f"{result.total_rows} total rows"
    )
    return result

  def _update_entity_from_company_info(
    self,
    graph_id: str,
    source: str,
    connection_id: str,
    duckdb_path: str | Path,
  ) -> None:
    """Update the entity row with CompanyInfo from the connector.

    Reads from the dbt staging table (stg_qb_company_info or similar)
    and updates the existing entity in the roboledger OLTP schema.
    Only updates if entity exists and company info is available.
    """
    import duckdb

    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions.entity import Entity

    try:
      con = duckdb.connect(str(duckdb_path), read_only=True)
      try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

        # Look for company info staging table
        company_table = None
        for candidate in [
          "stg_qb_company_info",
          "stg_xero_organisation",
          "company_info",
        ]:
          if candidate in tables:
            company_table = candidate
            break

        if not company_table:
          logger.debug(
            "No company info table found in dbt output, skipping entity update"
          )
          return

        result_set = con.execute(f"SELECT * FROM {company_table} LIMIT 1")
        columns = [desc[0] for desc in result_set.description]
        row = result_set.fetchone()
        if not row:
          return
        company = dict(zip(columns, row, strict=True))
      finally:
        con.close()

      with extensions_session(graph_id) as session:
        entity = session.query(Entity).first()
        if not entity:
          logger.warning(
            f"No entity found in graph {graph_id}, skipping CompanyInfo update"
          )
          return

        # Update entity fields from company info
        entity.name = company.get("company_name") or entity.name
        entity.legal_name = company.get("legal_name") or entity.legal_name
        entity.address_line1 = company.get("address_line1")
        entity.address_city = company.get("city")
        entity.address_state = company.get("state")
        entity.address_postal_code = company.get("postal_code")
        entity.address_country = company.get("country", "US")
        entity.source = source
        entity.connection_id = connection_id
        entity.source_id = company.get("id", "")
        entity.updated_at = datetime.now(UTC)
        session.commit()

        logger.info(
          f"Updated entity for graph {graph_id} with {source} CompanyInfo: "
          f"name='{entity.name}'"
        )

    except Exception as e:
      logger.warning(f"Could not update entity from CompanyInfo: {e}")
