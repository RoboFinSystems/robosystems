import pandas as pd
import numpy as np
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
from ..logger import logger
from typing import Dict, Any
from ..config import env


class QBClient:
  def __init__(
    self,
    realm_id: str,
    qb_credentials: Dict[str, Any],
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

    # The intuit-lib automatically updates the refresh token after a refresh call.
    # We can capture it if needed, but for read-only operations, this is sufficient.
    self.refresh_token = self.auth_client.refresh_token

    self.client = QuickBooks(
      auth_client=self.auth_client,
      refresh_token=self.refresh_token,
      entity_id=self.realm_id,
      minorversion=75,
    )

  def get_entity_info(self):
    from quickbooks.objects.company_info import CompanyInfo

    return CompanyInfo.all(qb=self.client)

  def get_accounts(self):
    from quickbooks.objects.account import Account

    count = Account.count(qb=self.client) or 0
    all_accounts = []

    # If count is 0, return empty list without making API calls
    if count == 0:
      return all_accounts

    for i in range(0, count, 25):
      accounts = Account.filter(max_results="25", start_position=str(i), qb=self.client)
      for a in accounts:
        if a.to_dict() not in all_accounts:
          all_accounts.append(a.to_dict())
    return all_accounts

  def get_accounts_df(self):
    accounts = self.get_accounts()
    accounts_df = pd.DataFrame(accounts)
    accounts_df["AccountType"] = accounts_df.apply(
      lambda x: f"Other {x.Classification}"
      if x.AccountType == "NaN"
      else x.AccountType,
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

  def get_journal_entries(self):
    from quickbooks.objects.journalentry import JournalEntry

    count = JournalEntry.count(qb=self.client) or 0
    all_entries = []

    # If count is 0, return empty list without making API calls
    if count == 0:
      return all_entries

    for i in range(0, count, 25):
      entries = JournalEntry.filter(
        max_results="25", start_position=str(i), qb=self.client
      )
      for entry in entries:
        all_entries.append(entry.to_dict())
    return all_entries

  def get_transactions(self, start_date=None, end_date=None):
    params = {}
    if start_date:
      params["start_date"] = start_date
    if end_date:
      params["end_date"] = end_date
    transactions = self.client.get_report("JournalReport", params)
    return transactions
