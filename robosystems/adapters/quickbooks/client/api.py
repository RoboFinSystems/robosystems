from typing import Any

import numpy as np
import pandas as pd
from intuitlib.client import AuthClient
from quickbooks import QuickBooks

from robosystems.config import env
from robosystems.logger import logger


class QBClient:
  def __init__(
    self,
    realm_id: str,
    qb_credentials: dict[str, Any],
  ):
    """
    Initializes the QuickBooks client using the new connection credentials system.

    Args:
      realm_id: The QuickBooks realm ID.
      qb_credentials: A dictionary containing 'refresh_token' and 'access_token'.
    """
    if not realm_id or not qb_credentials:
      raise ValueError("realm_id and qb_credentials are required.")

    self.realm_id = realm_id
    refresh_token = qb_credentials.get("refresh_token")
    access_token = qb_credentials.get("access_token")

    if not refresh_token:
      raise ValueError("QuickBooks refresh_token not found in credentials")

    self.refresh_token = refresh_token
    self.access_token = access_token

    self.auth_client = AuthClient(
      client_id=env.INTUIT_CLIENT_ID,
      client_secret=env.INTUIT_CLIENT_SECRET,
      environment=env.INTUIT_ENVIRONMENT,
      redirect_uri=env.INTUIT_REDIRECT_URI,
      refresh_token=refresh_token,
      realm_id=self.realm_id,
    )

    # Set access token if we have it
    if self.access_token:
      self.auth_client.access_token = self.access_token

    if not refresh_token.startswith("mock_"):
      logger.info(f"Refreshing QuickBooks token for realm {self.realm_id}")
      self.auth_client.refresh(refresh_token=refresh_token)

    # Capture updated tokens after refresh
    self.refresh_token = self.auth_client.refresh_token
    self.access_token = self.auth_client.access_token
    logger.info(
      f"Token refresh complete: access_token={'yes' if self.access_token else 'no'}, "
      f"refresh_token={'yes' if self.refresh_token else 'no'}"
    )

    self.client = QuickBooks(
      auth_client=self.auth_client,
      refresh_token=self.refresh_token,
      company_id=self.realm_id,
      minorversion=75,
    )

  def get_entity_info(self):
    from quickbooks.objects.company_info import CompanyInfo

    return CompanyInfo.all(qb=self.client)

  def get_accounts(self):
    from quickbooks.objects.account import Account

    # QB Online's Account endpoint defaults to Active=true. Historical journal
    # lines can reference accounts that were later deactivated, so we must
    # pull both active and inactive — otherwise dbt's accounting-equation
    # test fails on lines that hit accounts missing from the elements table.
    # Paginate until empty rather than calling count() first (count() in this
    # library version doesn't accept a where clause).
    page_size = 100
    start = 1
    all_accounts: list[dict] = []
    while True:
      page = Account.query(
        f"SELECT * FROM Account WHERE Active IN (true, false) "
        f"STARTPOSITION {start} MAXRESULTS {page_size}",
        qb=self.client,
      )
      if not page:
        break
      for a in page:
        d = a.to_dict()
        if d not in all_accounts:
          all_accounts.append(d)
      if len(page) < page_size:
        break
      start += page_size
    return all_accounts

  def get_accounts_df(self):
    accounts = self.get_accounts()
    accounts_df = pd.DataFrame(accounts)
    accounts_df["AccountType"] = accounts_df.apply(
      lambda x: (
        f"Other {x.Classification}" if x.AccountType == "NaN" else x.AccountType
      ),
      axis=1,
    )
    for i, r in accounts_df.iterrows():
      if r.AccountType in ["Other Income", "Other Expense"]:
        accounts_df.loc[i, "Classification"] = r.AccountType

    accounts_df["Classification"] = pd.Categorical(
      accounts_df["Classification"],
      [
        "Asset",
        "Liability",
        "Equity",
        "Revenue",
        "Expense",
        "Other Income",
        "Other Expense",
      ],
    )

    accounts_df["AccountType"] = pd.Categorical(
      accounts_df["AccountType"],
      [
        "Bank",
        "Accounts Receivable",
        "Other Current Asset",
        "Fixed Asset",
        "Other Asset",
        "Accounts Payable",
        "Credit Card",
        "Other Current Liability",
        "Long Term Liability",
        "Equity",
        "Income",
        "Cost of Goods Sold",
        "Expense",
        "Other Income",
        "Other Expense",
      ],
    )
    accounts_df.sort_values(
      by=["Classification", "AccountType", "FullyQualifiedName"], inplace=True
    )
    accounts_df["Order"] = np.nan
    accounts_df["Sequence"] = np.nan
    accounts_df.reset_index(inplace=True, drop=True)

    def traverse(parentRef):
      children_df = accounts_df[accounts_df.ParentRef == parentRef]
      torder = 1
      for ci, cr in children_df.iterrows():
        accounts_df.loc[ci, "Order"] = torder
        torder += 1

    seq_cnt = 1
    order_cnt = 1
    for i, r in accounts_df.iterrows():
      accounts_df.loc[i, "Sequence"] = seq_cnt
      if not r.ParentRef:
        accounts_df.loc[i, "Order"] = order_cnt
        order_cnt += 1
      else:
        traverse(r.ParentRef)
      seq_cnt += 1
    return accounts_df

  def get_account_by_id(self, account_id):
    from quickbooks.objects.account import Account

    return Account.get(account_id, qb=self.client).to_dict()

  def get_account_by_name(self, account_name):
    from quickbooks.objects.account import Account

    return Account.filter(Name=account_name, qb=self.client)[0].to_dict()

  def _paginate(self, entity_class, where_clause: str | None = None):
    """Paginate over a QB entity, returning a list of dicts.

    Used by per-class methods. Date-filtering callers pass a `where_clause`
    matching QBO's SQL syntax (e.g., "TxnDate >= '2024-01-01'") — the
    quickbooks lib's .where() method handles the WHERE prefix.
    """
    page_size = 100
    page = 0
    all_rows: list[dict] = []
    while True:
      start_position = page * page_size + 1
      if where_clause:
        results = entity_class.where(
          where_clause,
          max_results=str(page_size),
          start_position=str(start_position),
          qb=self.client,
        )
      else:
        results = entity_class.all(
          max_results=page_size,
          start_position=str(start_position),
          qb=self.client,
        )
      if not results:
        break
      for row in results:
        all_rows.append(row.to_dict())
      if len(results) < page_size:
        break
      page += 1
    return all_rows

  def get_customers(self):
    from quickbooks.objects.customer import Customer

    return self._paginate(Customer)

  def get_vendors(self):
    from quickbooks.objects.vendor import Vendor

    return self._paginate(Vendor)

  def get_employees(self):
    from quickbooks.objects.employee import Employee

    return self._paginate(Employee)

  def get_invoices(self, start_date: str, end_date: str):
    from quickbooks.objects.invoice import Invoice

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(Invoice, where_clause=where)

  def get_bills(self, start_date: str, end_date: str):
    from quickbooks.objects.bill import Bill

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(Bill, where_clause=where)

  def get_payments(self, start_date: str, end_date: str):
    from quickbooks.objects.payment import Payment

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(Payment, where_clause=where)

  def get_bill_payments(self, start_date: str, end_date: str):
    from quickbooks.objects.billpayment import BillPayment

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(BillPayment, where_clause=where)

  def get_sales_receipts(self, start_date: str, end_date: str):
    from quickbooks.objects.salesreceipt import SalesReceipt

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(SalesReceipt, where_clause=where)

  def get_purchases(self, start_date: str, end_date: str):
    """Fetch Purchase entities — covers QB's Expense / Check / Cash Expense /
    Credit Card Expense in JournalReport. Each carries an EntityRef (the
    vendor or customer paid) plus a PaymentType discriminator
    ("Cash" | "Check" | "CreditCard"). Used to populate `agent_external_id`
    on the JournalReport-derived events the cmd_create_event handler
    captures."""
    from quickbooks.objects.purchase import Purchase

    where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    return self._paginate(Purchase, where_clause=where)

  def get_journal_entries(
    self, start_date: str | None = None, end_date: str | None = None
  ):
    from quickbooks.objects.journalentry import JournalEntry

    if start_date and end_date:
      where = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
      return self._paginate(JournalEntry, where_clause=where)
    return self._paginate(JournalEntry)

  def get_transactions(self, start_date=None, end_date=None):
    """Fetch JournalReport — retired in Phase 2 as the transactional source.

    Kept for backward compat with any external callers; the QB pipeline now
    pulls Invoice / Bill / Payment / JournalEntry per-entity instead.
    """
    params = {}
    if start_date:
      params["start_date"] = start_date
    if end_date:
      params["end_date"] = end_date
    transactions = self.client.get_report("JournalReport", params)
    return transactions
