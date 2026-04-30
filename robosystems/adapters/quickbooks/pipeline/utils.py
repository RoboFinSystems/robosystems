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


# Ledger output tables from dbt (dependency order for FK resolution).
# `agents` is Phase 2: UPSERTed before transactions/events so event_blocks
# can resolve agent_id from the row's agent_external_id.
QB_LEDGER_TABLES = [
  "elements",
  "agents",
  "transactions",
  "entries",
  "line_items",
  "dimensions",
]

# dbt project location (relative to repo root)
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def parse_journal_report(
  report: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Parse a QuickBooks JournalReport into entries and lines.

  The JournalReport API returns ALL transaction types (invoices, bills,
  payments, purchases, deposits, journal entries, etc.) in double-entry
  format. This is the primary data source for the accounting graph.

  Report row format (ColData array):
    [0] Date, [1] Transaction Type (.id = tx ID), [2] Num,
    [3] Name, [4] Memo, [5] Account (.id = account ID),
    [6] Debit amount, [7] Credit amount

  Transactions are grouped by "Summary" rows that separate each
  transaction group.

  Returns:
      Tuple of (entries, lines) matching raw_journal_entries and
      raw_journal_lines schemas.
  """
  entries: list[dict[str, Any]] = []
  lines: list[dict[str, Any]] = []
  seen_entries: set[str] = set()

  if not report or "Rows" not in report or "Row" not in report.get("Rows", {}):
    logger.warning("JournalReport has no rows")
    return entries, lines

  # State tracking for transaction grouping
  tx_date: str | None = None
  tx_type: str | None = None
  tx_id: str | None = None
  tx_li_num = 1
  tx_total = 0.0

  for row in report["Rows"]["Row"]:
    # Summary rows mark the end of a transaction group
    if "Summary" in row:
      tx_date = None
      tx_type = None
      tx_id = None
      tx_li_num = 1
      tx_total = 0.0
      continue

    col_data = row.get("ColData", [])
    if len(col_data) < 8:
      continue

    # First row in a group sets the date/type/id
    row_date = col_data[0].get("value", "")
    if row_date and not tx_date:
      tx_date = row_date
    row_type = col_data[1].get("value", "")
    if row_type and not tx_type:
      tx_type = row_type
    row_id = col_data[1].get("id", "")
    if row_id and not tx_id:
      tx_id = row_id

    if not tx_date or not tx_type or not tx_id:
      continue

    # Build a stable entry ID from type + id
    entry_id = f"{tx_type}_{tx_id}"

    # Parse amounts
    debit_str = col_data[6].get("value", "")
    credit_str = col_data[7].get("value", "")
    debit_amt = float(debit_str) if debit_str else 0.0
    credit_amt = float(credit_str) if credit_str else 0.0
    amount = debit_amt or credit_amt
    posting_type = "Debit" if debit_amt else "Credit"

    tx_total += debit_amt  # Total is sum of debits (= sum of credits)

    # Account info
    account_id = col_data[5].get("id", "")
    account_name = col_data[5].get("value", "")

    # Build line
    lines.append(
      {
        "journal_entry_id": entry_id,
        "line_num": tx_li_num,
        "Amount": amount,
        "PostingType": posting_type,
        "AccountRef_value": str(account_id),
        "AccountRef_name": account_name,
        "Description": col_data[4].get("value", ""),
        "DetailType": "JournalEntryLineDetail",
        "DepartmentRef_value": "",
        "DepartmentRef_name": "",
        "ClassRef_value": "",
        "ClassRef_name": "",
        "LocationRef_value": "",
        "LocationRef_name": "",
      }
    )
    tx_li_num += 1

    # Build entry (once per transaction)
    if entry_id not in seen_entries:
      seen_entries.add(entry_id)
      entries.append(
        {
          "Id": entry_id,
          "TxnDate": tx_date,
          "DocNumber": col_data[2].get("value", ""),
          "TotalAmt": 0.0,  # Updated after all lines processed
          "PrivateNote": "",
          "Adjustment": False,
        }
      )

  # Update TotalAmt for each entry (sum of debits)
  total_by_entry: dict[str, float] = {}
  for line in lines:
    eid = line["journal_entry_id"]
    if line["PostingType"] == "Debit":
      total_by_entry[eid] = total_by_entry.get(eid, 0.0) + line["Amount"]
  for entry in entries:
    entry["TotalAmt"] = total_by_entry.get(entry["Id"], 0.0)

  logger.info(f"Parsed JournalReport: {len(entries)} transactions, {len(lines)} lines")
  return entries, lines


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


def _flatten_address(addr: dict[str, Any] | None) -> dict[str, Any]:
  """Normalize a QB Address dict into the JSONB shape Agent.address expects."""
  if not addr:
    return {}
  return {
    "line1": addr.get("Line1", "") or "",
    "line2": addr.get("Line2", "") or "",
    "city": addr.get("City", "") or "",
    "state": addr.get("CountrySubDivisionCode", "") or "",
    "postal_code": addr.get("PostalCode", "") or "",
    "country": addr.get("Country", "") or "",
  }


def _flatten_party(
  raw_list: list[Any], agent_type: str, ref_keys: dict[str, str]
) -> list[dict[str, Any]]:
  """Shared flattener for Customer / Vendor / Employee.

  Each QB party has: Id, DisplayName/CompanyName, GivenName/FamilyName,
  PrimaryEmailAddr.Address, PrimaryPhone.FreeFormNumber, BillAddr,
  TaxIdentifier (sometimes called TaxIdentifier or PrimaryTaxIdentifier).
  """
  rows: list[dict[str, Any]] = []
  for raw in raw_list:
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    name = (
      data.get(ref_keys.get("name", "DisplayName"))
      or data.get("CompanyName")
      or data.get("DisplayName")
      or ""
    )
    legal = data.get("CompanyName") or name
    email = (data.get("PrimaryEmailAddr") or {}).get("Address", "") or ""
    phone = (data.get("PrimaryPhone") or {}).get("FreeFormNumber", "") or ""
    addr = _flatten_address(data.get("BillAddr") or data.get("PrimaryAddr"))
    tax_id = (
      data.get("TaxIdentifier")
      or data.get("PrimaryTaxIdentifier")
      or data.get("SSN")
      or ""
    )
    rows.append(
      {
        "Id": str(data.get("Id", "")),
        "agent_type": agent_type,
        "name": name,
        "legal_name": legal,
        "email": email,
        "phone": phone,
        "address": addr,
        "tax_id": str(tax_id) if tax_id else "",
        "is_1099_recipient": bool(data.get("Vendor1099", False)),
        "is_active": bool(data.get("Active", True)),
      }
    )
  return rows


def flatten_customers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "customer", {})


def flatten_vendors(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "vendor", {})


def flatten_employees(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "employee", {})


def _flatten_txn_header(
  raw_list: list[Any], qb_class: str, agent_ref_field: str, agent_type: str
) -> list[dict[str, Any]]:
  """Shared flattener for Invoice / Bill / Payment headers.

  Produces one row per QB transaction with the agent reference resolved.
  Line items are NOT extracted here — JournalReport remains the GL
  posting source. These headers are used to enrich JournalReport-derived
  events with `event_type`, `event_category`, and `agent_external_id`.

  ``qb_class`` matches the prefix used by ``parse_journal_report`` to build
  composite tx ids (e.g. ``Invoice_123``).
  """
  rows: list[dict[str, Any]] = []
  for raw in raw_list:
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    tx_id = str(data.get("Id", ""))
    agent_ref = data.get(agent_ref_field) or {}
    rows.append(
      {
        "tx_type": qb_class,
        "tx_id": tx_id,
        "external_id": f"{qb_class}_{tx_id}",
        "txn_date": data.get("TxnDate", ""),
        "doc_number": data.get("DocNumber", ""),
        "total_amount": float(data.get("TotalAmt", 0) or 0),
        "currency": (data.get("CurrencyRef") or {}).get("value", "USD"),
        "memo": data.get("PrivateNote", "") or "",
        "agent_external_id": str(agent_ref.get("value", "")) if agent_ref else "",
        "agent_type": agent_type,
      }
    )
  return rows


def flatten_invoice_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "Invoice", "CustomerRef", "customer")


def flatten_bill_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "Bill", "VendorRef", "vendor")


def flatten_payment_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "Payment", "CustomerRef", "customer")


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


# Expected schemas for empty DataFrames (DuckDB needs at least one column)
_JOURNAL_ENTRIES_SCHEMA = {
  "Id": "str",
  "TxnDate": "str",
  "DocNumber": "str",
  "TotalAmt": "float64",
  "PrivateNote": "str",
  "Adjustment": "bool",
}
_JOURNAL_LINES_SCHEMA = {
  "journal_entry_id": "str",
  "line_num": "int64",
  "Amount": "float64",
  "PostingType": "str",
  "AccountRef_value": "str",
  "AccountRef_name": "str",
  "Description": "str",
  "DetailType": "str",
  "DepartmentRef_value": "str",
  "DepartmentRef_name": "str",
  "ClassRef_value": "str",
  "ClassRef_name": "str",
  "LocationRef_value": "str",
  "LocationRef_name": "str",
}
_PARTY_SCHEMA = {
  "Id": "str",
  "agent_type": "str",
  "name": "str",
  "legal_name": "str",
  "email": "str",
  "phone": "str",
  "address": "object",
  "tax_id": "str",
  "is_1099_recipient": "bool",
  "is_active": "bool",
}
_TXN_HEADER_SCHEMA = {
  "tx_type": "str",
  "tx_id": "str",
  "external_id": "str",
  "txn_date": "str",
  "doc_number": "str",
  "total_amount": "float64",
  "currency": "str",
  "memo": "str",
  "agent_external_id": "str",
  "agent_type": "str",
}


def _to_dataframe(
  data: list[dict[str, Any]], schema: dict[str, str] | None = None
) -> pd.DataFrame:
  """Create a DataFrame, using schema for column types when data is empty."""
  if data:
    return pd.DataFrame(data)
  if schema:
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in schema.items()})
  return pd.DataFrame()


def write_extract_parquet(
  output_dir: Path,
  accounts: list[dict[str, Any]],
  journal_entries: list[dict[str, Any]],
  journal_lines: list[dict[str, Any]],
  company_info: list[dict[str, Any]],
  customers: list[dict[str, Any]] | None = None,
  vendors: list[dict[str, Any]] | None = None,
  employees: list[dict[str, Any]] | None = None,
  invoice_headers: list[dict[str, Any]] | None = None,
  bill_headers: list[dict[str, Any]] | None = None,
  payment_headers: list[dict[str, Any]] | None = None,
) -> None:
  """Write extracted QB data as parquet files.

  Empty datasets are written with the expected schema so DuckDB
  can read them (parquet files with 0 columns are invalid).

  Phase 2 adds party + transaction-header parquet files. Line items still
  come from JournalReport via raw_journal_lines.
  """
  output_dir.mkdir(parents=True, exist_ok=True)

  pd.DataFrame(accounts).to_parquet(output_dir / "raw_accounts.parquet", index=False)
  _to_dataframe(journal_entries, _JOURNAL_ENTRIES_SCHEMA).to_parquet(
    output_dir / "raw_journal_entries.parquet", index=False
  )
  _to_dataframe(journal_lines, _JOURNAL_LINES_SCHEMA).to_parquet(
    output_dir / "raw_journal_lines.parquet", index=False
  )
  pd.DataFrame(company_info).to_parquet(
    output_dir / "raw_company_info.parquet", index=False
  )

  _to_dataframe(customers or [], _PARTY_SCHEMA).to_parquet(
    output_dir / "raw_customers.parquet", index=False
  )
  _to_dataframe(vendors or [], _PARTY_SCHEMA).to_parquet(
    output_dir / "raw_vendors.parquet", index=False
  )
  _to_dataframe(employees or [], _PARTY_SCHEMA).to_parquet(
    output_dir / "raw_employees.parquet", index=False
  )
  _to_dataframe(invoice_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_invoice_headers.parquet", index=False
  )
  _to_dataframe(bill_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_bill_headers.parquet", index=False
  )
  _to_dataframe(payment_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_payment_headers.parquet", index=False
  )

  logger.info(
    f"Wrote extract parquet: {len(accounts)} accounts, "
    f"{len(journal_entries)} entries, {len(journal_lines)} lines, "
    f"{len(customers or [])} customers, {len(vendors or [])} vendors, "
    f"{len(employees or [])} employees, {len(invoice_headers or [])} invoices, "
    f"{len(bill_headers or [])} bills, {len(payment_headers or [])} payments"
  )
