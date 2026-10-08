"""QuickBooks extract asset: QB API → raw parquet for dbt."""

import requests
from dagster import AssetExecutionContext, MaterializeResult, asset

from .cdc import PLAN_FILE, plan_cdc, write_plan
from .configs import QBSyncConfig
from .load import end_failed_sync
from .utils import (
  JournalReportTruncatedError,
  flatten_bill_headers,
  flatten_bill_payment_headers,
  flatten_company_info,
  flatten_customers,
  flatten_employees,
  flatten_invoice_headers,
  flatten_payment_headers,
  flatten_purchase_headers,
  flatten_sales_receipt_headers,
  flatten_vendors,
  get_pipeline_work_dir,
  journal_report_truncated,
  parse_journal_report,
  write_extract_parquet,
)

_FIRST_WINDOW_DAYS = 366
_MIN_WINDOW_DAYS = 7


def fetch_journal_report(client, start_date: str, end_date: str, log=None) -> dict:
  """The JournalReport for ``[start_date, end_date]``, fetched in windows.

  Starts from year-long windows and halves any window Intuit truncates. The
  rows of every window are concatenated in date order. Transaction groups
  never span a window, since each group is one transaction on one date.

  Raises JournalReportTruncatedError when even a one-week window is cut.
  """
  from datetime import date, timedelta

  first = date.fromisoformat(start_date)
  last = date.fromisoformat(end_date)
  pending: list[tuple[date, date]] = []
  cursor = first
  while cursor <= last:
    window_end = min(cursor + timedelta(days=_FIRST_WINDOW_DAYS - 1), last)
    pending.append((cursor, window_end))
    cursor = window_end + timedelta(days=1)

  rows: list[dict] = []
  while pending:
    lo, hi = pending.pop(0)
    span = (hi - lo).days + 1
    try:
      report = client.get_transactions(
        start_date=lo.isoformat(), end_date=hi.isoformat()
      )
      cut = journal_report_truncated(report)
    except requests.exceptions.ReadTimeout:
      # Too large to generate in time; the same answer as a truncated one.
      if span <= _MIN_WINDOW_DAYS:
        raise
      report, cut = None, True
    if cut:
      if span <= _MIN_WINDOW_DAYS:
        raise JournalReportTruncatedError(
          f"QuickBooks truncated the JournalReport for {lo} to {hi} even at "
          f"{span} days; the sync cannot import this period completely."
        )
      mid = lo + timedelta(days=span // 2 - 1)
      if log is not None:
        log.warning(f"JournalReport {lo}..{hi} too large for one call; splitting")
      pending[:0] = [(lo, mid), (mid + timedelta(days=1), hi)]
      continue
    rows.extend(((report or {}).get("Rows") or {}).get("Row") or [])
  return {"Rows": {"Row": rows}}


_HEADER_KEYS = (
  "invoice_headers",
  "bill_headers",
  "payment_headers",
  "bill_payment_headers",
  "sales_receipt_headers",
  "purchase_headers",
)


def _fetch_headers(client, start_date: str, end_date: str) -> dict[str, list[dict]]:
  """The transaction headers for one window; they carry the agent refs the
  JournalReport flattens away."""
  return {
    "invoice_headers": flatten_invoice_headers(
      client.get_invoices(start_date, end_date)
    ),
    "bill_headers": flatten_bill_headers(client.get_bills(start_date, end_date)),
    "payment_headers": flatten_payment_headers(
      client.get_payments(start_date, end_date)
    ),
    "bill_payment_headers": flatten_bill_payment_headers(
      client.get_bill_payments(start_date, end_date)
    ),
    "sales_receipt_headers": flatten_sales_receipt_headers(
      client.get_sales_receipts(start_date, end_date)
    ),
    "purchase_headers": flatten_purchase_headers(
      client.get_purchases(start_date, end_date)
    ),
  }


class MultiCurrencyNotSupportedError(Exception):
  """The dbt mart (transactions.sql, elements.sql) hardcodes USD, so a
  non-USD realm would load silently wrong; refuse it."""


def _assert_usd_only(
  *header_groups: list[dict],
  realm_id: str,
) -> None:
  """The flatteners default a missing CurrencyRef to USD, so any other code
  here is real QB multi-currency data."""
  offending: set[str] = set()
  for rows in header_groups:
    for row in rows:
      currency = row.get("currency")
      if currency and currency != "USD":
        offending.add(currency)
  if offending:
    codes = ", ".join(sorted(offending))
    raise MultiCurrencyNotSupportedError(
      f"QuickBooks realm {realm_id} contains transactions in non-USD "
      f"currencies ({codes}). Multi-currency QuickBooks companies are not "
      f"supported: the data would silently coerce to USD and corrupt your "
      f"reports."
    )


@asset(
  group_name="qb_pipeline",
  description="Extract data from QuickBooks API to parquet files",
  kinds={"quickbooks"},
  metadata={
    "pipeline": "quickbooks",
    "stage": "extract",
  },
)
def qb_extract(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  try:
    return _run_qb_extract(context, config)
  except BaseException as exc:
    # BaseException: a stopped run raises DagsterExecutionInterruptedError.
    end_failed_sync(context, config, exc)
    raise


def _run_qb_extract(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  from datetime import datetime, timedelta

  from robosystems.adapters.quickbooks.client import QBClient
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )

  context.log.info(
    f"Extracting QB data for graph={config.graph_id}, "
    f"connection={config.connection_id}, realm={config.realm_id}, "
    f"full_rebuild={config.full_rebuild}"
  )

  from robosystems.models.core.connection.connection import Connection

  with SessionFactory() as session:
    creds = ConnectionCredentials.get_by_connection_id(config.connection_id, session)
    if not creds:
      raise ValueError(f"No credentials found for connection {config.connection_id}")
    credentials = creds.get_credentials()
    connection = Connection.get_by_id(config.connection_id, session)
    watermark = connection.last_cdc_watermark if connection is not None else None

  realm_id = config.realm_id
  if not realm_id:
    raise ValueError("realm_id is required for QuickBooks extraction")

  # A plan is written at the end; the previous sync's must not outlive a
  # failed extract into the next load.
  extract_dir = get_pipeline_work_dir(config.graph_id) / "extract"
  (extract_dir / PLAN_FILE).unlink(missing_ok=True)

  client = QBClient(
    realm_id=realm_id,
    qb_credentials=credentials,
    connection_id=config.connection_id,
  )
  context.log.info("QBClient initialized, fetching data...")

  raw_company_info = client.get_entity_info()
  company_info = flatten_company_info(raw_company_info)
  context.log.info(f"Fetched company info: {len(company_info)} entities")

  accounts = client.get_accounts()
  context.log.info(f"Fetched {len(accounts)} accounts")

  # JournalReport returns nothing without explicit dates.
  end_date = datetime.now().strftime("%Y-%m-%d")
  if config.full_rebuild:
    start_date = "2000-01-01"
    context.log.info(f"Full rebuild: fetching transactions from {start_date}")
  elif config.since_date:
    start_date = config.since_date
    context.log.info(f"Since-date sync: fetching transactions from {start_date}")
  else:
    start_date = (datetime.now() - timedelta(days=config.lookback_days)).strftime(
      "%Y-%m-%d"
    )
    context.log.info(f"Incremental: fetching transactions from {start_date}")

  # CDC says what the window pull cannot: deletions, and edits dated before
  # the window. A full window still needs the deletions: a posted event
  # missing from the report is not inferred deleted.
  cdc_plan = plan_cdc(client, watermark, start_date, log=context.log)

  windows = [(start_date, end_date), *(tuple(w) for w in cdc_plan.extra_windows)]
  journal_entries: list[dict] = []
  journal_lines: list[dict] = []
  headers: dict[str, list[dict]] = {key: [] for key in _HEADER_KEYS}
  for lo, hi in windows:
    report = fetch_journal_report(client, lo, hi, log=context.log)
    entries, lines = parse_journal_report(report)
    journal_entries.extend(entries)
    journal_lines.extend(lines)
    for key, rows in _fetch_headers(client, lo, hi).items():
      headers[key].extend(rows)

  context.log.info(
    f"Parsed: {len(journal_entries)} transactions, {len(journal_lines)} lines "
    f"over {len(windows)} window(s)"
  )

  # Parties are a full snapshot; headers use the JournalReport windows.
  customers = flatten_customers(client.get_customers())
  vendors = flatten_vendors(client.get_vendors())
  employees = flatten_employees(client.get_employees())
  context.log.info(
    f"Fetched parties: {len(customers)} customers, {len(vendors)} vendors, "
    f"{len(employees)} employees"
  )

  invoice_headers = headers["invoice_headers"]
  bill_headers = headers["bill_headers"]
  payment_headers = headers["payment_headers"]
  bill_payment_headers = headers["bill_payment_headers"]
  sales_receipt_headers = headers["sales_receipt_headers"]
  purchase_headers = headers["purchase_headers"]
  context.log.info(
    f"Fetched headers: {len(invoice_headers)} invoices, {len(bill_headers)} bills, "
    f"{len(payment_headers)} payments, {len(bill_payment_headers)} bill payments, "
    f"{len(sales_receipt_headers)} sales receipts, "
    f"{len(purchase_headers)} purchases"
  )

  _assert_usd_only(
    invoice_headers,
    bill_headers,
    payment_headers,
    bill_payment_headers,
    sales_receipt_headers,
    purchase_headers,
    realm_id=realm_id,
  )

  write_extract_parquet(
    extract_dir,
    accounts,
    journal_entries,
    journal_lines,
    company_info,
    customers=customers,
    vendors=vendors,
    employees=employees,
    invoice_headers=invoice_headers,
    bill_headers=bill_headers,
    payment_headers=payment_headers,
    bill_payment_headers=bill_payment_headers,
    sales_receipt_headers=sales_receipt_headers,
    purchase_headers=purchase_headers,
  )

  write_plan(extract_dir, cdc_plan)

  context.log.info(f"Extract complete → {extract_dir}")

  return MaterializeResult(
    metadata={
      "extract_path": str(extract_dir),
      "graph_id": config.graph_id,
      "realm_id": realm_id,
      "cdc_checked": cdc_plan.checked,
      "cdc_reason": cdc_plan.reason or "",
      "cdc_deletions": len(cdc_plan.deletions),
      "cdc_extra_windows": len(cdc_plan.extra_windows),
      "accounts": len(accounts),
      "journal_entries": len(journal_entries),
      "journal_lines": len(journal_lines),
      "customers": len(customers),
      "vendors": len(vendors),
      "employees": len(employees),
      "invoice_headers": len(invoice_headers),
      "bill_headers": len(bill_headers),
      "payment_headers": len(payment_headers),
      "bill_payment_headers": len(bill_payment_headers),
      "sales_receipt_headers": len(sales_receipt_headers),
      "purchase_headers": len(purchase_headers),
      "full_rebuild": config.full_rebuild,
    }
  )
