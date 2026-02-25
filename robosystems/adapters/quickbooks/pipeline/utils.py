"""QuickBooks pipeline utilities."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from robosystems.logger import logger


def get_pipeline_work_dir(graph_id: str) -> Path:
  """Get a deterministic work directory for a pipeline run.

  All assets in the same pipeline run share this directory so they
  can pass data between extract → transform → load without needing
  Dagster IO managers or metadata lookups.

  The directory persists for the lifetime of the temp dir (OS-managed).
  """
  base = Path(tempfile.gettempdir()) / "qb_pipeline" / graph_id
  base.mkdir(parents=True, exist_ok=True)
  return base


# Graph output tables in dependency order (nodes first, then relationships)
QB_NODE_TABLES = ["entity", "element", "dimension", "transaction", "line_item"]
QB_RELATIONSHIP_TABLES = [
  "entity_has_transaction",
  "transaction_has_line_item",
  "line_item_relates_to_element",
  "line_item_has_dimension",
]
QB_ALL_TABLES = QB_NODE_TABLES + QB_RELATIONSHIP_TABLES

# dbt project location (relative to repo root)
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def flatten_journal_lines(
  journal_entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  """Flatten journal entry Line arrays into individual rows.

  Each journal entry has a Line array with one row per debit/credit.
  This flattens them into a flat list suitable for a DataFrame.

  Args:
      journal_entries: List of JournalEntry.to_dict() objects from QBClient

  Returns:
      List of flattened line item dicts matching raw_journal_lines schema
  """
  lines = []
  for entry in journal_entries:
    entry_id = str(entry.get("Id", ""))
    for line_num, line in enumerate(entry.get("Line", []), start=1):
      detail = line.get("JournalEntryLineDetail", {})
      account_ref = detail.get("AccountRef", {}) or {}
      department_ref = detail.get("DepartmentRef", {}) or {}
      class_ref = detail.get("ClassRef", {}) or {}
      location_ref = detail.get("LocationRef", {}) or {}

      lines.append(
        {
          "journal_entry_id": entry_id,
          "line_num": line_num,
          "Amount": float(line.get("Amount", 0)),
          "PostingType": detail.get("PostingType", ""),
          "AccountRef_value": str(account_ref.get("value", "")),
          "AccountRef_name": account_ref.get("name", ""),
          "Description": line.get("Description", ""),
          "DetailType": line.get("DetailType", ""),
          "DepartmentRef_value": str(department_ref.get("value", ""))
          if department_ref.get("value")
          else "",
          "DepartmentRef_name": department_ref.get("name", ""),
          "ClassRef_value": str(class_ref.get("value", ""))
          if class_ref.get("value")
          else "",
          "ClassRef_name": class_ref.get("name", ""),
          "LocationRef_value": str(location_ref.get("value", ""))
          if location_ref.get("value")
          else "",
          "LocationRef_name": location_ref.get("name", ""),
        }
      )
  return lines


def flatten_company_info(company_info_list: list) -> list[dict[str, Any]]:
  """Flatten CompanyInfo objects into rows matching raw_company_info schema.

  Args:
      company_info_list: Result of QBClient.get_entity_info()

  Returns:
      List of company info dicts
  """
  rows = []
  for info in company_info_list:
    data = info.to_dict() if hasattr(info, "to_dict") else info
    addr = data.get("CompanyAddr", {}) or {}
    rows.append(
      {
        "Id": str(data.get("Id", "")),
        "CompanyName": data.get("CompanyName", ""),
        "LegalName": data.get("LegalName", data.get("CompanyName", "")),
        "CompanyAddr_Line1": addr.get("Line1", ""),
        "CompanyAddr_City": addr.get("City", ""),
        "CompanyAddr_CountrySubDivisionCode": addr.get("CountrySubDivisionCode", ""),
        "CompanyAddr_PostalCode": addr.get("PostalCode", ""),
        "CompanyAddr_Country": addr.get("Country", "US"),
      }
    )
  return rows


def flatten_journal_entries(
  journal_entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  """Flatten journal entries into rows matching raw_journal_entries schema.

  Args:
      journal_entries: List of JournalEntry.to_dict() objects

  Returns:
      List of journal entry header dicts (no lines)
  """
  rows = []
  for entry in journal_entries:
    rows.append(
      {
        "Id": str(entry.get("Id", "")),
        "TxnDate": entry.get("TxnDate", ""),
        "DocNumber": entry.get("DocNumber", ""),
        "TotalAmt": float(entry.get("TotalAmt", 0)),
        "PrivateNote": entry.get("PrivateNote", ""),
        "Adjustment": entry.get("Adjustment", False),
      }
    )
  return rows


def filter_entries_by_date(
  journal_entries: list[dict[str, Any]],
  lookback_days: int = 60,
) -> list[dict[str, Any]]:
  """Filter journal entries to only include those within the lookback window.

  Args:
      journal_entries: Full list of journal entries
      lookback_days: Number of days to look back from today

  Returns:
      Filtered list of journal entries
  """
  cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
  filtered = [e for e in journal_entries if (e.get("TxnDate", "") or "") >= cutoff]
  logger.info(
    f"Filtered journal entries: {len(filtered)}/{len(journal_entries)} "
    f"(cutoff: {cutoff})"
  )
  return filtered


def write_extract_parquet(
  output_dir: Path,
  accounts: list[dict[str, Any]],
  journal_entries: list[dict[str, Any]],
  journal_lines: list[dict[str, Any]],
  company_info: list[dict[str, Any]],
) -> None:
  """Write extracted QB data as parquet files.

  Args:
      output_dir: Directory to write parquet files
      accounts: Account list from QBClient.get_accounts()
      journal_entries: Flattened journal entry headers
      journal_lines: Flattened journal entry lines
      company_info: Flattened company info
  """
  output_dir.mkdir(parents=True, exist_ok=True)

  pd.DataFrame(accounts).to_parquet(output_dir / "raw_accounts.parquet", index=False)
  pd.DataFrame(journal_entries).to_parquet(
    output_dir / "raw_journal_entries.parquet", index=False
  )
  pd.DataFrame(journal_lines).to_parquet(
    output_dir / "raw_journal_lines.parquet", index=False
  )
  pd.DataFrame(company_info).to_parquet(
    output_dir / "raw_company_info.parquet", index=False
  )

  logger.info(
    f"Wrote extract parquet: {len(accounts)} accounts, "
    f"{len(journal_entries)} entries, {len(journal_lines)} lines"
  )


def export_duckdb_tables(duckdb_path: Path, output_dir: Path) -> dict[str, int]:
  """Export dbt output tables from DuckDB to parquet files.

  Exports each graph table as qb_{TableName}.parquet for loading
  into the Graph API staging infrastructure.

  Args:
      duckdb_path: Path to the dbt output DuckDB file
      output_dir: Directory to write qb_*.parquet files

  Returns:
      Dict mapping table name to row count
  """
  import duckdb

  output_dir.mkdir(parents=True, exist_ok=True)
  results = {}

  con = duckdb.connect(str(duckdb_path), read_only=True)
  try:
    existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

    for table in QB_ALL_TABLES:
      if table not in existing_tables:
        logger.warning(f"Table '{table}' not found in DuckDB, skipping export")
        continue

      result = con.execute(f"SELECT count(*) FROM {table}").fetchone()
      row_count = result[0] if result else 0
      if row_count == 0:
        logger.info(f"Table '{table}' is empty, skipping export")
        continue

      output_file = output_dir / f"qb_{table}.parquet"
      con.execute(f"COPY {table} TO '{output_file}' (FORMAT PARQUET)")
      results[table] = row_count
      logger.info(f"Exported {table}: {row_count} rows → {output_file.name}")
  finally:
    con.close()

  return results
