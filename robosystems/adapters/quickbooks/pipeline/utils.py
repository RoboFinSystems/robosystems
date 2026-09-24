"""QuickBooks pipeline utilities."""

import json
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from robosystems.logger import logger


def get_pipeline_work_dir(graph_id: str) -> Path:
  """Deterministic per-graph directory the extract → transform → load assets
  share in place of a Dagster IO manager."""
  base = Path(tempfile.gettempdir()) / "qb_pipeline" / graph_id
  base.mkdir(parents=True, exist_ok=True)
  return base


# dbt ledger tables in FK dependency order (agents before transactions, so
# events can resolve agent_id).
QB_LEDGER_TABLES = [
  "elements",
  "agents",
  "transactions",
  "entries",
  "line_items",
  "dimensions",
]


# JournalReport's display tx_type strings → python-quickbooks class names, so
# the dbt JOIN against the header pulls has one key. Unmapped values pass through.
_TX_TYPE_NORMALIZATION = {
  "Bill Payment (Check)": "BillPayment",
  "Bill Payment (CreditCard)": "BillPayment",
  "Bill Pmt -Check": "BillPayment",
  "Bill Pmt CC": "BillPayment",
  "Bill Pmt-Check": "BillPayment",
  "Sales Receipt": "SalesReceipt",
  "Credit Memo": "CreditMemo",
  "Vendor Credit": "VendorCredit",
  "Refund Receipt": "RefundReceipt",
  "Journal Entry": "JournalEntry",
}


def _normalize_tx_type(raw_tx_type: str) -> str:
  return _TX_TYPE_NORMALIZATION.get(raw_tx_type, raw_tx_type)


DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"


class JournalReportTruncatedError(Exception):
  """Intuit cut the JournalReport short; the window must be narrowed."""


# Intuit caps a report at 400,000 cells and ends it early with this notice
# instead of an error. The Reports API does not paginate.
_TRUNCATION_NOTICE = "unable to display more data"


def journal_report_truncated(report: dict[str, Any] | None) -> bool:
  """True when the report shows either sign of Intuit's cell cap.

  The two signs are the notice text anywhere in the rows or header, and a
  final transaction group with lines that never reaches its ``Summary`` row.
  Every group of a complete report closes with one; the parser relies on it.
  """
  if not report:
    return False
  if _TRUNCATION_NOTICE in json.dumps(report.get("Header") or {}).lower():
    return True
  rows = (report.get("Rows") or {}).get("Row") or []
  open_group = False
  for row in rows:
    if "Summary" in row:
      open_group = False
      continue
    col_data = row.get("ColData") or []
    for cell in col_data:
      if _TRUNCATION_NOTICE in str(cell.get("value", "")).lower():
        return True
    if len(col_data) >= 8:
      open_group = True
  return open_group


def parse_journal_report(
  report: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Parse a JournalReport (every transaction type, double-entry) into rows
  for raw_journal_entries and raw_journal_lines.

  ColData: [0] Date, [1] Type (.id = tx id), [2] Num, [3] Name, [4] Memo,
  [5] Account (.id = account id), [6] Debit, [7] Credit. "Summary" rows
  close each transaction group.
  """
  entries: list[dict[str, Any]] = []
  lines: list[dict[str, Any]] = []
  seen_entries: set[str] = set()

  if not report or "Rows" not in report or "Row" not in report.get("Rows", {}):
    logger.warning("JournalReport has no rows")
    return entries, lines

  tx_date: str | None = None
  tx_type: str | None = None
  tx_id: str | None = None
  tx_li_num = 1

  for row in report["Rows"]["Row"]:
    if "Summary" in row:
      tx_date = None
      tx_type = None
      tx_id = None
      tx_li_num = 1
      continue

    col_data = row.get("ColData", [])
    if len(col_data) < 8:
      continue

    # The group's first row carries the date/type/id.
    row_date = col_data[0].get("value", "")
    if row_date and not tx_date:
      tx_date = row_date
    row_type = col_data[1].get("value", "")
    if row_type and not tx_type:
      tx_type = _normalize_tx_type(row_type)
    row_id = col_data[1].get("id", "")
    if row_id and not tx_id:
      tx_id = row_id

    if not tx_date or not tx_type or not tx_id:
      continue

    entry_id = f"{tx_type}_{tx_id}"

    debit_str = col_data[6].get("value", "")
    credit_str = col_data[7].get("value", "")
    debit_amt = float(debit_str) if debit_str else 0.0
    credit_amt = float(credit_str) if credit_str else 0.0
    amount = debit_amt or credit_amt
    posting_type = "Debit" if debit_amt else "Credit"

    account_id = col_data[5].get("id", "")
    account_name = col_data[5].get("value", "")

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

    if entry_id not in seen_entries:
      seen_entries.add(entry_id)
      entries.append(
        {
          "Id": entry_id,
          "TxnDate": tx_date,
          "DocNumber": col_data[2].get("value", ""),
          "TotalAmt": 0.0,  # filled in below
          "PrivateNote": "",
          "Adjustment": False,
        }
      )

  total_by_entry: dict[str, float] = {}
  for line in lines:
    eid = line["journal_entry_id"]
    if line["PostingType"] == "Debit":
      total_by_entry[eid] = total_by_entry.get(eid, 0.0) + line["Amount"]
  for entry in entries:
    entry["TotalAmt"] = total_by_entry.get(entry["Id"], 0.0)

  logger.info(f"Parsed JournalReport: {len(entries)} transactions, {len(lines)} lines")
  return entries, lines


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
  """Shared flattener for Customer / Vendor / Employee."""
  rows: list[dict[str, Any]] = []
  for raw in raw_list:
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    # Employees often have only GivenName + FamilyName.
    given = (data.get("GivenName") or "").strip()
    family = (data.get("FamilyName") or "").strip()
    full_name = f"{given} {family}".strip()
    name = (
      data.get(ref_keys.get("name", "DisplayName"))
      or data.get("CompanyName")
      or data.get("DisplayName")
      or full_name
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
        # JSON string so parquet sees a plain str column.
        "address": json.dumps(addr) if addr else "{}",
        "tax_id": str(tax_id) if tax_id else "",
        "is_1099_recipient": bool(data.get("Vendor1099", False)),
        "is_active": bool(data.get("Active", True)),
        # QB's per-entity version counter; "" (not null) when absent.
        "sync_token": str(data.get("SyncToken", "") or ""),
      }
    )
  return rows


def flatten_customers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "customer", {})


def flatten_vendors(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "vendor", {})


def flatten_employees(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_party(raw, "employee", {})


def _extract_linked_txns(data: dict[str, Any]) -> str:
  """The Invoices / Bills a Payment / BillPayment settles, as a JSON string
  of ``{txn_id, txn_type}`` (a scalar column for parquet → DuckDB → dbt).

  QB's TxnType matches this pipeline's composite-id prefix, so the discharge
  handler rebuilds the settled event's ``external_id`` as ``{TxnType}_{TxnId}``.
  """
  refs: list[dict[str, str]] = []
  for line in data.get("Line", []) or []:
    for linked in line.get("LinkedTxn", []) or []:
      txn_id = str(linked.get("TxnId", "") or "")
      txn_type = str(linked.get("TxnType", "") or "")
      if txn_id and txn_type:
        refs.append({"txn_id": txn_id, "txn_type": txn_type})
  return json.dumps(refs)


def _flatten_txn_header(
  raw_list: list[Any],
  qb_class: str,
  agent_ref_field: str,
  agent_type: str,
  *,
  extract_linked_txns: bool = False,
) -> list[dict[str, Any]]:
  """One header row per QB transaction, enriching the JournalReport-derived
  events (which stay the GL posting source) with type and counterparty.

  ``qb_class`` is the composite-id prefix ``parse_journal_report`` uses.
  Headers without ``extract_linked_txns`` emit ``"[]"`` so the UNION ALL in
  transactions.sql sees one column shape.
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
        "linked_txns": _extract_linked_txns(data) if extract_linked_txns else "[]",
        "sync_token": str(data.get("SyncToken", "") or ""),
      }
    )
  return rows


def flatten_invoice_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "Invoice", "CustomerRef", "customer")


def flatten_bill_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "Bill", "VendorRef", "vendor")


def flatten_payment_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(
    raw, "Payment", "CustomerRef", "customer", extract_linked_txns=True
  )


def flatten_bill_payment_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(
    raw, "BillPayment", "VendorRef", "vendor", extract_linked_txns=True
  )


def flatten_sales_receipt_headers(raw: list[Any]) -> list[dict[str, Any]]:
  return _flatten_txn_header(raw, "SalesReceipt", "CustomerRef", "customer")


# Purchase.PaymentType → the tx_type labels JournalReport may use for it
# ("Expense" vs "Cash Expense" varies by company settings).
_PURCHASE_PAYMENT_TYPE_TX_TYPES: dict[str, list[str]] = {
  "Cash": ["Cash Expense", "Expense"],
  "Check": ["Check"],
  "CreditCard": ["Credit Card Expense"],
}


def flatten_purchase_headers(raw: list[Any]) -> list[dict[str, Any]]:
  """One header row per (Purchase, candidate tx_type), so the LEFT JOIN in
  transactions.sql matches whichever label JournalReport chose.

  ``EntityRef.type`` may be a customer, vendor or employee.
  """
  rows: list[dict[str, Any]] = []
  for raw_obj in raw:
    data = raw_obj.to_dict() if hasattr(raw_obj, "to_dict") else raw_obj
    tx_id = str(data.get("Id", ""))
    entity_ref = data.get("EntityRef") or {}
    payment_type = str(data.get("PaymentType", ""))
    candidate_tx_types = _PURCHASE_PAYMENT_TYPE_TX_TYPES.get(
      payment_type, [payment_type or "Expense"]
    )
    entity_type_raw = str(entity_ref.get("type", "")).lower() if entity_ref else ""
    agent_type = (
      entity_type_raw
      if entity_type_raw in {"customer", "vendor", "employee"}
      else "vendor"
    )
    base_row = {
      "tx_id": tx_id,
      "external_id": f"Purchase_{tx_id}",
      "txn_date": data.get("TxnDate", ""),
      "doc_number": data.get("DocNumber", ""),
      "total_amount": float(data.get("TotalAmt", 0) or 0),
      "currency": (data.get("CurrencyRef") or {}).get("value", "USD"),
      "memo": data.get("PrivateNote", "") or "",
      "agent_external_id": str(entity_ref.get("value", "")) if entity_ref else "",
      "agent_type": agent_type,
      "linked_txns": "[]",
      "sync_token": str(data.get("SyncToken", "") or ""),
    }
    for candidate in candidate_tx_types:
      rows.append({**base_row, "tx_type": candidate})
  return rows


def flatten_company_info(company_info_list: list) -> list[dict[str, Any]]:
  """Rows matching the raw_company_info schema."""
  rows = []
  for info in company_info_list:
    data = info.to_dict() if hasattr(info, "to_dict") else info
    addr = data.get("CompanyAddr", {}) or {}
    primary_phone = data.get("PrimaryPhone", {}) or {}
    web_addr = data.get("WebAddr", {}) or {}
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
        "PrimaryPhone_FreeFormNumber": primary_phone.get("FreeFormNumber", ""),
        "WebAddr_URI": web_addr.get("URI", ""),
        # A month name (January..December); dbt derives the fiscal year end.
        "FiscalYearStartMonth": data.get("FiscalYearStartMonth", ""),
      }
    )
  return rows


# Schemas for empty DataFrames: a zero-column parquet is unreadable by DuckDB.
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
  "address": "str",
  "tax_id": "str",
  "is_1099_recipient": "bool",
  "is_active": "bool",
  "sync_token": "str",
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
  "linked_txns": "str",
  "sync_token": "str",
}


def _to_dataframe(
  data: list[dict[str, Any]], schema: dict[str, str] | None = None
) -> pd.DataFrame:
  """``schema`` supplies the columns when ``data`` is empty."""
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
  bill_payment_headers: list[dict[str, Any]] | None = None,
  sales_receipt_headers: list[dict[str, Any]] | None = None,
  purchase_headers: list[dict[str, Any]] | None = None,
) -> None:
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
  _to_dataframe(bill_payment_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_bill_payment_headers.parquet", index=False
  )
  _to_dataframe(sales_receipt_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_sales_receipt_headers.parquet", index=False
  )
  _to_dataframe(purchase_headers or [], _TXN_HEADER_SCHEMA).to_parquet(
    output_dir / "raw_purchase_headers.parquet", index=False
  )

  logger.info(
    f"Wrote extract parquet: {len(accounts)} accounts, "
    f"{len(journal_entries)} entries, {len(journal_lines)} lines, "
    f"{len(customers or [])} customers, {len(vendors or [])} vendors, "
    f"{len(employees or [])} employees, {len(invoice_headers or [])} invoices, "
    f"{len(bill_headers or [])} bills, {len(payment_headers or [])} payments, "
    f"{len(bill_payment_headers or [])} bill payments, "
    f"{len(sales_receipt_headers or [])} sales receipts"
  )
