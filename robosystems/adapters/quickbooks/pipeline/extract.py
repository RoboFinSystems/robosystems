"""QuickBooks Extract Asset.

Fetches data from QuickBooks API and writes raw parquet files
to a temp directory for dbt transformation.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import (
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
  parse_journal_report,
  write_extract_parquet,
)


class MultiCurrencyNotSupportedError(Exception):
  """QB returned non-USD data and the dbt mart can't preserve it.

  Fail-loud guard: the dbt mart at
  `dbt/models/ledger/transactions.sql:66` + `elements.sql:35` hardcodes
  `'USD'` even though `pipeline/utils.py` correctly extracts
  `CurrencyRef.value` into the flattened header rows. A non-USD realm
  would silently corrupt — reports would render numbers as if they were
  USD. Refuse to load until full currency thread-through ships.
  """


def _assert_usd_only(
  *header_groups: list[dict],
  realm_id: str,
) -> None:
  """Raise MultiCurrencyNotSupportedError if any extracted header row
  carries a non-USD currency.

  Scans every header dict's `currency` field across all groups
  (invoices / bills / payments / etc.). The flatten helpers at
  `pipeline/utils.py:393,476` default to `'USD'` when CurrencyRef is
  missing, so the only non-USD rows here are intentional QB-side
  multi-currency entries.
  """
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
      f"currencies ({codes}). Multi-currency is not yet supported by the "
      f"dbt mart pipeline; the data would silently coerce to USD and "
      f"corrupt your reports. Contact RoboSystems to enable multi-currency "
      f"support before re-syncing this realm."
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
  """Extract QuickBooks data to parquet files.

  Fetches accounts, company info, and the JournalReport (all transaction
  types in double-entry format) from the QuickBooks API, then writes
  raw parquet files for dbt transformation.

  The JournalReport includes invoices, bills, payments, purchases,
  deposits, and manual journal entries — everything needed for a
  complete accounting graph.
  """
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

  # Get credentials from PostgreSQL
  with SessionFactory() as session:
    creds = ConnectionCredentials.get_by_connection_id(config.connection_id, session)
    if not creds:
      raise ValueError(f"No credentials found for connection {config.connection_id}")
    credentials = creds.get_credentials()

  # Initialize QB client
  realm_id = config.realm_id
  if not realm_id:
    raise ValueError("realm_id is required for QuickBooks extraction")

  client = QBClient(
    realm_id=realm_id,
    qb_credentials=credentials,
    connection_id=config.connection_id,
  )
  context.log.info("QBClient initialized, fetching data...")

  # Fetch company info (always full, 1 API call)
  raw_company_info = client.get_entity_info()
  company_info = flatten_company_info(raw_company_info)
  context.log.info(f"Fetched company info: {len(company_info)} entities")

  # Fetch accounts (always full, small dataset)
  accounts = client.get_accounts()
  context.log.info(f"Fetched {len(accounts)} accounts")

  # Fetch JournalReport (all transaction types in double-entry format)
  # QB API requires explicit dates to return data. Resolution order:
  # full_rebuild → 2000-01-01, since_date if set → that, else lookback_days.
  end_date = datetime.now().strftime("%Y-%m-%d")
  if config.full_rebuild:
    start_date = "2000-01-01"  # Far enough back to catch all history
    context.log.info(f"Full rebuild: fetching transactions from {start_date}")
  elif config.since_date:
    start_date = config.since_date
    context.log.info(f"Since-date sync: fetching transactions from {start_date}")
  else:
    start_date = (datetime.now() - timedelta(days=config.lookback_days)).strftime(
      "%Y-%m-%d"
    )
    context.log.info(f"Incremental: fetching transactions from {start_date}")

  # testing_migration defaults to the INTUIT_REPORTS_TESTING_MIGRATION flag,
  # so this routes through Intuit's v2 reporting service unless SSM-disabled.
  report = client.get_transactions(start_date=start_date, end_date=end_date)
  journal_entries, journal_lines = parse_journal_report(report)

  context.log.info(
    f"Parsed: {len(journal_entries)} transactions, {len(journal_lines)} lines"
  )

  # Party entities (full snapshot, not date-filtered)
  customers = flatten_customers(client.get_customers())
  vendors = flatten_vendors(client.get_vendors())
  employees = flatten_employees(client.get_employees())
  context.log.info(
    f"Fetched parties: {len(customers)} customers, {len(vendors)} vendors, "
    f"{len(employees)} employees"
  )

  # Transaction-class headers (date-filtered, same window as JournalReport).
  # Headers carry agent refs that JournalReport flattens away — they enrich
  # JournalReport-derived events with class-specific event_type + agent_id.
  invoice_headers = flatten_invoice_headers(client.get_invoices(start_date, end_date))
  bill_headers = flatten_bill_headers(client.get_bills(start_date, end_date))
  payment_headers = flatten_payment_headers(client.get_payments(start_date, end_date))
  bill_payment_headers = flatten_bill_payment_headers(
    client.get_bill_payments(start_date, end_date)
  )
  sales_receipt_headers = flatten_sales_receipt_headers(
    client.get_sales_receipts(start_date, end_date)
  )
  # Purchase covers Expense / Cash Expense / Check / Credit Card Expense —
  # the four collapsed tx_types JournalReport surfaces with EntityRef
  # populated. Each Purchase emits multiple header rows (one per candidate
  # tx_type the JournalReport might use) so the LEFT JOIN in
  # transactions.sql resolves the agent regardless of which display
  # label QB picked for the row.
  purchase_headers = flatten_purchase_headers(
    client.get_purchases(start_date, end_date)
  )
  context.log.info(
    f"Fetched headers: {len(invoice_headers)} invoices, {len(bill_headers)} bills, "
    f"{len(payment_headers)} payments, {len(bill_payment_headers)} bill payments, "
    f"{len(sales_receipt_headers)} sales receipts, "
    f"{len(purchase_headers)} purchases"
  )

  # Multi-currency fail-loud guard. The dbt mart at
  # `dbt/models/ledger/transactions.sql:66` and `elements.sql:35`
  # hardcodes `'USD'`, silently dropping any non-USD `CurrencyRef`
  # captured at extract time. Until currency is threaded through the
  # full pipeline, refuse to load non-USD data — a Canadian/UK customer
  # would otherwise get silent corruption (reports showing USD values
  # for CAD/GBP amounts). Surfaces as a clean MultiCurrencyNotSupportedError
  # on the Dagster run; operator must work with us to enable.
  _assert_usd_only(
    invoice_headers,
    bill_headers,
    payment_headers,
    bill_payment_headers,
    sales_receipt_headers,
    purchase_headers,
    realm_id=realm_id,
  )

  # Write parquet to shared pipeline directory
  extract_dir = get_pipeline_work_dir(config.graph_id) / "extract"
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

  context.log.info(f"Extract complete → {extract_dir}")

  return MaterializeResult(
    metadata={
      "extract_path": str(extract_dir),
      "graph_id": config.graph_id,
      "realm_id": realm_id,
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
