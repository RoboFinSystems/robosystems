"""Write synthetic demo data into a DuckDB file in the shape OLTPLoader expects.

This mirrors what `adapters/quickbooks/dbt` produces for the QB pipeline:
five tables (elements, transactions, entries, line_items, dimensions) that
`robosystems.operations.extensions.loader.OLTPLoader` reads and inserts
into the extensions PostgreSQL schema.
"""

from __future__ import annotations

from pathlib import Path

SOURCE = "native"


def write_demo_duckdb(
  duckdb_path: str | Path,
  accounts: list[tuple],
  transactions: list[tuple],
  source: str = SOURCE,
) -> dict[str, int]:
  """Write accounts + transactions to a DuckDB file with the 5 OLTP tables.

  Args:
    duckdb_path: Destination .duckdb file (overwritten if it exists).
    accounts: List of (code, name, classification, sub_classification,
      balance_type, parent_code) tuples from close_demo.data.ACCOUNTS.
    transactions: List of (date, type, description, merchant, lines) tuples
      from close_demo.data.get_all_transactions(), where `lines` is
      [(account_code, debit_cents, credit_cents), ...].

  Returns:
    Dict with row counts for each table.
  """
  import duckdb

  duckdb_path = Path(duckdb_path)
  if duckdb_path.exists():
    duckdb_path.unlink()
  duckdb_path.parent.mkdir(parents=True, exist_ok=True)

  con = duckdb.connect(str(duckdb_path))
  try:
    _create_schema(con)

    element_rows = _build_element_rows(accounts, source)
    txn_rows, entry_rows, line_item_rows = _build_transaction_rows(transactions)

    _insert_elements(con, element_rows)
    _insert_transactions(con, txn_rows)
    _insert_entries(con, entry_rows)
    _insert_line_items(con, line_item_rows)

    return {
      "elements": len(element_rows),
      "transactions": len(txn_rows),
      "entries": len(entry_rows),
      "line_items": len(line_item_rows),
      "dimensions": 0,
    }
  finally:
    con.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _create_schema(con) -> None:
  con.execute("""
    CREATE TABLE elements (
      external_id VARCHAR,
      code VARCHAR,
      name VARCHAR,
      description VARCHAR,
      classification VARCHAR,
      sub_classification VARCHAR,
      balance_type VARCHAR,
      external_parent_id VARCHAR,
      depth INTEGER,
      path VARCHAR,
      currency VARCHAR,
      is_active BOOLEAN,
      is_placeholder BOOLEAN,
      external_source VARCHAR,
      metadata VARCHAR
    )
  """)
  con.execute("""
    CREATE TABLE transactions (
      external_id VARCHAR,
      number VARCHAR,
      type VARCHAR,
      category VARCHAR,
      amount BIGINT,
      currency VARCHAR,
      date DATE,
      due_date DATE,
      merchant_name VARCHAR,
      reference_number VARCHAR,
      description VARCHAR,
      source_id VARCHAR,
      status VARCHAR
    )
  """)
  con.execute("""
    CREATE TABLE entries (
      external_id VARCHAR,
      external_transaction_id VARCHAR,
      number VARCHAR,
      type VARCHAR,
      posting_date DATE,
      memo VARCHAR,
      status VARCHAR
    )
  """)
  con.execute("""
    CREATE TABLE line_items (
      entry_external_id VARCHAR,
      element_external_id VARCHAR,
      debit_amount BIGINT,
      credit_amount BIGINT,
      description VARCHAR,
      line_order INTEGER
    )
  """)
  # dimensions left empty — loader handles missing tables gracefully, but we
  # create it so SHOW TABLES returns a consistent schema for debugging.
  con.execute("""
    CREATE TABLE dimensions (
      dimension_type VARCHAR,
      name VARCHAR,
      value VARCHAR
    )
  """)


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _build_element_rows(accounts: list[tuple], source: str) -> list[tuple]:
  """Convert ACCOUNTS tuples to DuckDB element rows."""
  rows = []
  for (
    code,
    name,
    classification,
    sub_classification,
    balance_type,
    parent_code,
  ) in accounts:
    rows.append(
      (
        code,  # external_id
        code,  # code
        name,  # name
        None,  # description
        classification,  # classification
        sub_classification,  # sub_classification
        balance_type,  # balance_type
        parent_code,  # external_parent_id
        0,  # depth
        code,  # path
        "USD",  # currency
        True,  # is_active
        False,  # is_placeholder
        source,  # external_source
        f'{{"account_code": "{code}"}}',  # metadata (JSON string)
      )
    )
  return rows


def _build_transaction_rows(
  transactions: list[tuple],
) -> tuple[list[tuple], list[tuple], list[tuple]]:
  """Convert (date, type, desc, merchant, lines) tuples into (txn_rows, entry_rows, line_item_rows).

  Entries are 1:1 with transactions for demo data (same pattern as QB).
  """
  txn_rows: list[tuple] = []
  entry_rows: list[tuple] = []
  line_item_rows: list[tuple] = []

  for i, (txn_date, txn_type, desc, merchant, lines) in enumerate(transactions):
    ext_id = f"demo_{i:04d}"
    entry_ext_id = f"je_demo_{i:04d}"
    total_amount = sum(dr for _, dr, _ in lines)

    txn_rows.append(
      (
        ext_id,  # external_id
        None,  # number
        txn_type,  # type
        None,  # category
        total_amount,  # amount (cents)
        "USD",  # currency
        txn_date,  # date
        None,  # due_date
        merchant,  # merchant_name
        None,  # reference_number
        desc,  # description
        ext_id,  # source_id
        "posted",  # status
      )
    )

    entry_rows.append(
      (
        entry_ext_id,  # external_id
        ext_id,  # external_transaction_id
        None,  # number
        "standard",  # type
        txn_date,  # posting_date
        desc,  # memo
        "posted",  # status
      )
    )

    for li_idx, (acct_code, debit, credit) in enumerate(lines):
      line_item_rows.append(
        (
          entry_ext_id,  # entry_external_id
          acct_code,  # element_external_id
          debit,  # debit_amount
          credit,  # credit_amount
          desc,  # description
          li_idx,  # line_order
        )
      )

  return txn_rows, entry_rows, line_item_rows


# ---------------------------------------------------------------------------
# Inserts
# ---------------------------------------------------------------------------


def _insert_elements(con, rows: list[tuple]) -> None:
  con.executemany(
    "INSERT INTO elements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    rows,
  )


def _insert_transactions(con, rows: list[tuple]) -> None:
  con.executemany(
    "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    rows,
  )


def _insert_entries(con, rows: list[tuple]) -> None:
  con.executemany(
    "INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
    rows,
  )


def _insert_line_items(con, rows: list[tuple]) -> None:
  con.executemany(
    "INSERT INTO line_items VALUES (?, ?, ?, ?, ?, ?)",
    rows,
  )


def default_duckdb_path() -> Path:
  """Return the default working path for the demo DuckDB file."""
  return Path("/tmp/close_demo/close_demo.duckdb")


if __name__ == "__main__":
  # Smoke test
  from .data import ACCOUNTS, get_all_transactions

  path = default_duckdb_path()
  txns = get_all_transactions()
  counts = write_demo_duckdb(path, ACCOUNTS, txns)
  print(f"Wrote {path}")
  for table, count in counts.items():
    print(f"  {table}: {count}")
