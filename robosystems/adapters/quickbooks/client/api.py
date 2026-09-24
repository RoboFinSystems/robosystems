from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
import requests
from intuitlib.client import AuthClient
from intuitlib.exceptions import AuthClientError
from quickbooks import QuickBooks
from quickbooks.exceptions import AuthorizationException, QuickbooksException
from retrying import retry

from robosystems.config import env
from robosystems.logger import logger


class QBAuthFailedError(Exception):
  """QB token refresh failed.

  ``recoverable=True``: transient network error; connection state untouched,
  the next sync retries. ``False``: Intuit rejected the credential and the
  connection has been marked ``needs_reauth``.
  """

  def __init__(self, message: str, *, recoverable: bool) -> None:
    super().__init__(message)
    self.recoverable = recoverable


def _is_retryable_qb_error(exc: BaseException) -> bool:
  """Retry network errors and HTTP 429/5xx; never auth (401) or other QB faults."""
  if isinstance(exc, AuthorizationException):
    return False
  if isinstance(exc, requests.exceptions.RequestException):
    if isinstance(exc, requests.exceptions.HTTPError):
      status = getattr(exc.response, "status_code", None) if exc.response else None
      return status in (429, 500, 502, 503, 504)
    return True
  if isinstance(exc, QuickbooksException):
    # The SDK embeds the HTTP status only in the message text.
    msg = str(exc)
    return (
      "status code '429'" in msg
      or "status code '500'" in msg
      or "status code '502'" in msg
      or "status code '503'" in msg
      or "status code '504'" in msg
    )
  return False


# Jitter spreads concurrent syncs against the same realm.
_QB_RETRY = retry(
  retry_on_exception=_is_retryable_qb_error,
  stop_max_attempt_number=5,
  wait_exponential_multiplier=1000,
  wait_exponential_max=60_000,
  wait_jitter_max=1000,
)


class QBClient:
  def __init__(
    self,
    realm_id: str,
    qb_credentials: dict[str, Any],
    connection_id: str | None = None,
  ):
    """Refreshes the token on construction (skipped for ``mock_`` tokens).

    ``connection_id`` enables persisting the rotated tokens and flipping the
    connection to ``needs_reauth`` on a rejected refresh.
    """
    if not realm_id or not qb_credentials:
      raise ValueError("realm_id and qb_credentials are required.")

    self.realm_id = realm_id
    self.connection_id = connection_id
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

    if self.access_token:
      self.auth_client.access_token = self.access_token

    if not refresh_token.startswith("mock_"):
      logger.info(f"Refreshing QuickBooks token for realm {self.realm_id}")
      try:
        self.auth_client.refresh(refresh_token=refresh_token)
      except AuthClientError as e:
        logger.warning(
          f"QB AuthClient.refresh raised AuthClientError for realm "
          f"{self.realm_id} (status={getattr(e, 'status_code', '?')}): {e}"
        )
        self._mark_needs_reauth()
        raise QBAuthFailedError(
          f"QuickBooks rejected the credential refresh for realm "
          f"{self.realm_id}. Reconnect the account from the connections "
          f"page to re-issue tokens.",
          recoverable=False,
        ) from e
      except requests.exceptions.RequestException as e:
        logger.warning(
          f"Transient network error during QB token refresh for realm "
          f"{self.realm_id}: {e}"
        )
        raise QBAuthFailedError(
          f"Transient network error reaching Intuit for realm "
          f"{self.realm_id}; the next sync will retry.",
          recoverable=True,
        ) from e

    self.refresh_token = self.auth_client.refresh_token
    self.access_token = self.auth_client.access_token
    logger.info(
      f"Token refresh complete: access_token={'yes' if self.access_token else 'no'}, "
      f"refresh_token={'yes' if self.refresh_token else 'no'}"
    )

    # Intuit rotates the refresh_token on every refresh; an unpersisted
    # rotation locks the connection out once the grace window expires.
    if connection_id and (
      self.refresh_token != refresh_token or self.access_token != access_token
    ):
      self._persist_rotated_tokens(qb_credentials)

    self.client = QuickBooks(
      auth_client=self.auth_client,
      refresh_token=self.refresh_token,
      company_id=self.realm_id,
      minorversion=75,
    )

  def _mark_needs_reauth(self) -> None:
    """Best-effort: the caller's QBAuthFailedError is the real signal."""
    if not self.connection_id:
      return
    try:
      from robosystems.operations.connection_service import ConnectionService

      ConnectionService.mark_connection_needs_reauth_sync(self.connection_id)
    except Exception as e:
      logger.warning(
        f"Failed to mark connection {self.connection_id} as needs_reauth "
        f"(non-fatal): {e}"
      )

  def _persist_rotated_tokens(self, prior_credentials: dict[str, Any]) -> None:
    """Best-effort write of rotated tokens, on a short-lived session."""
    if not self.connection_id:
      return
    try:
      from robosystems.database import SessionFactory
      from robosystems.models.core.connection.connection_credentials import (
        ConnectionCredentials,
      )

      session = SessionFactory()
      try:
        cred = ConnectionCredentials.get_by_connection_id(self.connection_id, session)
        if cred is None:
          logger.warning(
            f"Cannot persist rotated tokens — ConnectionCredentials not "
            f"found for connection {self.connection_id}"
          )
          return
        # Merge onto the stored bundle, not prior_credentials, so extra
        # stored keys (realm_id, scope, ...) survive.
        live = cred.get_credentials()
        updated = {
          **live,
          "refresh_token": self.refresh_token,
          "access_token": self.access_token,
        }
        cred.update_credentials(updated, session)
        logger.info(f"Persisted rotated QB tokens for connection {self.connection_id}")
      finally:
        session.close()
    except Exception as e:
      logger.warning(
        f"Failed to persist rotated QB tokens for connection "
        f"{self.connection_id} (non-fatal): {e}"
      )

  @_QB_RETRY
  def get_entity_info(self):
    from quickbooks.objects.company_info import CompanyInfo

    return CompanyInfo.all(qb=self.client)

  @_QB_RETRY
  def _fetch_accounts_page(self, start: int, page_size: int):
    """Per-page so a retry doesn't redo the whole walk."""
    from quickbooks.objects.account import Account

    return Account.query(
      f"SELECT * FROM Account WHERE Active IN (true, false) "
      f"STARTPOSITION {start} MAXRESULTS {page_size}",
      qb=self.client,
    )

  def get_accounts(self):
    # Include inactive accounts: historical lines can reference them.
    # count() in this library takes no where clause, so page until empty.
    page_size = 100
    start = 1
    all_accounts: list[dict] = []
    seen_ids: set[str] = set()
    while True:
      page = self._fetch_accounts_page(start, page_size)
      if not page:
        break
      for a in page:
        d = a.to_dict()
        ext_id = str(d.get("Id", ""))
        if ext_id and ext_id not in seen_ids:
          seen_ids.add(ext_id)
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

  @_QB_RETRY
  def get_account_by_id(self, account_id):
    from quickbooks.objects.account import Account

    return Account.get(account_id, qb=self.client).to_dict()

  @_QB_RETRY
  def get_account_by_name(self, account_name):
    from quickbooks.objects.account import Account

    return Account.filter(Name=account_name, qb=self.client)[0].to_dict()

  @_QB_RETRY
  def _paginate_one_page(
    self,
    entity_class,
    start_position: int,
    page_size: int,
    where_clause: str | None,
  ):
    """Per-page so a retry doesn't redo the whole walk."""
    if where_clause:
      return entity_class.where(
        where_clause,
        max_results=str(page_size),
        start_position=str(start_position),
        qb=self.client,
      )
    return entity_class.all(
      max_results=page_size,
      start_position=str(start_position),
      qb=self.client,
    )

  def _paginate(self, entity_class, where_clause: str | None = None):
    """``where_clause`` is QBO SQL without the WHERE keyword."""
    page_size = 100
    page = 0
    all_rows: list[dict] = []
    while True:
      start_position = page * page_size + 1
      results = self._paginate_one_page(
        entity_class, start_position, page_size, where_clause
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
    """Expense / Check / Credit Card Expense rows; their EntityRef supplies
    the counterparty for JournalReport-derived events."""
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

  @_QB_RETRY
  def get_transactions(self, start_date=None, end_date=None, testing_migration=None):
    """Fetch JournalReport, the live GL posting source.

    ``testing_migration`` (default: ``INTUIT_REPORTS_TESTING_MIGRATION``)
    routes to Intuit's v2 reporting service; a no-op once Intuit's cutover
    completes. https://medium.com/intuitdev/upcoming-changes-to-reports-apis-5083ec9aadce
    """
    params = {}
    if start_date:
      params["start_date"] = start_date
    if end_date:
      params["end_date"] = end_date
    if testing_migration is None:
      testing_migration = env.INTUIT_REPORTS_TESTING_MIGRATION
    if testing_migration:
      # Intuit checks only for the key's presence.
      params["testing_migration"] = "true"
    transactions = self.client.get_report("JournalReport", params)
    return transactions

  # -- CDC (Change Data Capture) ----------------------------------------

  def _cdc_base_url(self) -> str:
    if env.INTUIT_ENVIRONMENT.lower() == "production":
      host = "https://quickbooks.api.intuit.com"
    else:
      host = "https://sandbox-quickbooks.api.intuit.com"
    return f"{host}/v3/company/{self.realm_id}/cdc"

  @_QB_RETRY
  def cdc(
    self, changed_since: datetime | str, entities: list[str]
  ) -> tuple[dict[str, list[Any]], bool]:
    """Fetch entity changes since ``changed_since`` (naive datetimes are UTC;
    a string is passed through as-is).

    Returns ``(entities_by_type, watermark_too_old)``. Deleted entities come
    back with ``status='Deleted'`` in the same lists. ``watermark_too_old``
    means QB rejected the window (~30 days); the caller should fall back to a
    full lookback. Any other 400 raises ``HTTPError``.
    """
    if isinstance(changed_since, datetime):
      if changed_since.tzinfo is None:
        ts = changed_since.replace(tzinfo=UTC)
      else:
        ts = changed_since.astimezone(UTC)
      changed_since_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    else:
      changed_since_iso = str(changed_since)

    headers = {
      "Authorization": f"Bearer {self.access_token}",
      "Accept": "application/json",
    }
    params = {
      "entities": ",".join(entities),
      "changedSince": changed_since_iso,
    }
    url = self._cdc_base_url()
    logger.info(
      "QB CDC fetch: realm=%s entities=[%s] changedSince=%s",
      self.realm_id,
      ",".join(entities),
      changed_since_iso,
    )
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    if resp.status_code == 400:
      # Match the fault message only: other 400s are real bugs and must
      # not be masked as a watermark fallback.
      try:
        payload = resp.json()
      except ValueError:
        payload = {}
      fault = (payload.get("Fault") or {}).get("Error", [])
      messages = " | ".join(
        (e.get("Message") or "") + " " + (e.get("Detail") or "") for e in fault
      ).lower()
      if "changedsince" in messages or "30 days" in messages:
        logger.warning(
          "QB CDC rejected changedSince=%s for realm %s (window too old). "
          "Falling back to full lookback. Response: %s",
          changed_since_iso,
          self.realm_id,
          messages[:200],
        )
        return {}, True
      resp.raise_for_status()
    resp.raise_for_status()

    data = resp.json()
    # {"CDCResponse": [{"QueryResponse": [{"Customer": [...]}, ...]}], ...}
    entities_by_type: dict[str, list[Any]] = {name: [] for name in entities}
    for cdc_block in data.get("CDCResponse", []):
      for query_resp in cdc_block.get("QueryResponse", []):
        for key, value in query_resp.items():
          if key in entities_by_type and isinstance(value, list):
            entities_by_type[key].extend(value)
    return entities_by_type, False
