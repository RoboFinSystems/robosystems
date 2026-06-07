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
  """QB auth refresh failed in a way that's worth surfacing cleanly.

  Wraps both `AuthClientError` (Intuit-side credential rejection — bad,
  revoked, scope-insufficient) and transient network failures, so the
  Dagster job logs a single typed error rather than a stack-trace dump
  from the underlying intuitlib / requests stack.

  When `recoverable=True` the caller should leave the connection state
  alone (e.g., transient network blip — retry next sync). When False the
  connection has been marked `needs_reauth` and the operator must
  reconnect via OAuth.
  """

  def __init__(self, message: str, *, recoverable: bool) -> None:
    super().__init__(message)
    self.recoverable = recoverable


def _is_retryable_qb_error(exc: BaseException) -> bool:
  """Predicate for `retrying` — decides whether a QB API exception is
  worth retrying.

  Retry on:
  - Network/transport errors (`ConnectionError`, `Timeout`, generic
    `RequestException`) — always transient.
  - QuickbooksException variants whose underlying HTTP status was 429
    or 5xx. The QB SDK wraps non-200/non-Fault responses as
    ``QuickbooksException(error_code=10000)`` and embeds the original
    status in the message (`"Error returned with status code '429': ..."`).
    Match on that pattern.

  Don't retry on:
  - ``AuthorizationException`` (HTTP 401 from Intuit — auth-side,
    won't get better with retry).
  - Other ``QuickbooksException`` variants (validation, object not
    found, fault-payload application errors).
  """
  if isinstance(exc, AuthorizationException):
    return False
  if isinstance(exc, requests.exceptions.RequestException):
    # HTTPError carries response.status_code; everything else is
    # network-side and worth retrying outright.
    if isinstance(exc, requests.exceptions.HTTPError):
      status = getattr(exc.response, "status_code", None) if exc.response else None
      return status in (429, 500, 502, 503, 504)
    return True
  if isinstance(exc, QuickbooksException):
    # SDK wraps non-200 responses as `error_code=10000` with the HTTP
    # status embedded in the message text. Pattern-match because the
    # SDK doesn't surface the status code as a typed attribute.
    msg = str(exc)
    return (
      "status code '429'" in msg
      or "status code '500'" in msg
      or "status code '502'" in msg
      or "status code '503'" in msg
      or "status code '504'" in msg
    )
  return False


# Exponential backoff capped at 60s with up to 1s of jitter to spread
# concurrent syncs against the same realm. 5 attempts ≈ worst case
# 1+2+4+8+16 = 31s of backoff plus jitter before giving up.
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
    """
    Initializes the QuickBooks client using the new connection credentials system.

    Args:
      realm_id: The QuickBooks realm ID.
      qb_credentials: A dictionary containing 'refresh_token' and 'access_token'.
      connection_id: Optional connection ID used to persist rotated tokens
        back to ``ConnectionCredentials`` after Intuit rotates them on
        refresh. Without it, rotated tokens die with the
        QBClient instance and the next sync uses pre-rotation tokens —
        eventually failing once Intuit's grace window expires.
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

    # Set access token if we have it
    if self.access_token:
      self.auth_client.access_token = self.access_token

    if not refresh_token.startswith("mock_"):
      logger.info(f"Refreshing QuickBooks token for realm {self.realm_id}")
      # Wrap refresh in typed error handling. AuthClientError
      # is auth-side (revoked / expired-beyond-grace / scope-insufficient)
      # and flips the connection to needs_reauth so the operator sees a
      # "Reconnect" CTA instead of a generic "sync failed". Transient
      # network errors don't flip state — they retry next sync.
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
        # Transient — network failure between us and Intuit. Don't flip
        # connection state; the next sync will retry the refresh.
        logger.warning(
          f"Transient network error during QB token refresh for realm "
          f"{self.realm_id}: {e}"
        )
        raise QBAuthFailedError(
          f"Transient network error reaching Intuit for realm "
          f"{self.realm_id}; the next sync will retry.",
          recoverable=True,
        ) from e

    # Capture updated tokens after refresh
    self.refresh_token = self.auth_client.refresh_token
    self.access_token = self.auth_client.access_token
    logger.info(
      f"Token refresh complete: access_token={'yes' if self.access_token else 'no'}, "
      f"refresh_token={'yes' if self.refresh_token else 'no'}"
    )

    # Persist rotated tokens back to ConnectionCredentials.
    # Intuit rotates the refresh_token on every refresh; without this,
    # the rotated value dies when the QBClient instance goes out of
    # scope and the next sync uses the pre-rotation token. Eventually
    # the rotation grace window expires and the connection locks out.
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
    """Flip the connection's status to ``needs_reauth``.

    Best-effort: a failure to update the status row is logged but
    swallowed — the QBAuthFailedError raised by the caller is the
    operator-visible signal even if this side effect didn't land.
    """
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
    """Write rotated tokens back to ``ConnectionCredentials``.

    Scoped session lifecycle — opens a fresh platform-DB session just
    for this update and closes it immediately. Don't hold the session
    open for the whole sync.

    Best-effort: persistence failure is logged but swallowed. The
    QBClient still has the rotated tokens in-memory for this run; the
    next sync will refresh again and either re-rotate (and re-persist)
    or hit the rotation grace window.
    """
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
        # Pull the live bundle from the DB rather than `prior_credentials`
        # (the QBClient caller may have passed only refresh/access). Extra
        # keys (realm_id, scope, ...) that the DB carries survive the
        # rotation write.
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
    """Single page of accounts — decorated so 429/5xx retry doesn't redo
    the whole paginated walk."""
    from quickbooks.objects.account import Account

    return Account.query(
      f"SELECT * FROM Account WHERE Active IN (true, false) "
      f"STARTPOSITION {start} MAXRESULTS {page_size}",
      qb=self.client,
    )

  def get_accounts(self):
    # QB Online's Account endpoint defaults to Active=true. Historical journal
    # lines can reference accounts that were later deactivated, so we must
    # pull both active and inactive — otherwise dbt's accounting-equation
    # test fails on lines that hit accounts missing from the elements table.
    # Paginate until empty rather than calling count() first (count() in this
    # library version doesn't accept a where clause).
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
        # Dedup by Id rather than full-dict membership (O(1) vs O(n))
        # so tenants with thousands of accounts don't pay an O(n²)
        # tax on the import path.
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
    """Single page fetch for `_paginate` — decorated so 429/5xx retry
    doesn't redo the whole paginated walk."""
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

  @_QB_RETRY
  def get_transactions(self, start_date=None, end_date=None):
    """Fetch JournalReport — no longer the transactional source.

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

  # -- CDC (Change Data Capture) ----------------------------------------

  def _cdc_base_url(self) -> str:
    """QB CDC endpoint base URL — switches between sandbox + production
    per ``INTUIT_ENVIRONMENT``."""
    if env.INTUIT_ENVIRONMENT.lower() == "production":
      host = "https://quickbooks.api.intuit.com"
    else:
      host = "https://sandbox-quickbooks.api.intuit.com"
    return f"{host}/v3/company/{self.realm_id}/cdc"

  @_QB_RETRY
  def cdc(
    self, changed_since: datetime | str, entities: list[str]
  ) -> tuple[dict[str, list[Any]], bool]:
    """Fetch entity changes since ``changed_since`` via QB's CDC endpoint.

    Args:
        changed_since: tz-aware or naive datetime (naive interpreted as
            UTC), or a pre-formatted ISO 8601 string. Datetime input is
            the normal path; the string escape hatch exists for replay
            of recorded fixtures.
        entities: list of QB entity names (Invoice, Bill, Payment,
                  BillPayment, JournalEntry, SalesReceipt, Customer,
                  Vendor, Employee, Account, ...).

    Returns:
        Tuple of ``(entities_by_type, watermark_too_old)``.
        - ``entities_by_type`` maps entity name → list of raw entity dicts
          (each dict carries the QB API's representation including
          ``Id`` and ``SyncToken``). Entity types with no changes return
          empty lists. **NOTE on deletions**: CDC returns deleted entities
          with ``status='Deleted'`` in the same per-entity list. Handler
          dispatch for these is **not yet implemented** — the loader has
          no soft-delete path for QB-side deletions. Until that lands,
          a row that was deleted on the QB side after our initial sync
          will remain present in our extensions DB until the next full
          rebuild.
        - ``watermark_too_old`` is True when QB rejects the
          ``changedSince`` value (typically > 30 days back). Callers
          should reset the watermark and fall through to full lookback.

    The retry decorator handles transient 429 / 5xx; the 400 ``too old``
    response surfaces here as a non-retryable signal so the caller can
    fall through. Other 400s (malformed entity name, invalid format,
    quota faults, etc.) propagate as ``HTTPError`` rather than being
    silently masked as a watermark fallback — those are real bugs the
    operator needs to see.
    """
    if isinstance(changed_since, datetime):
      if changed_since.tzinfo is None:
        ts = changed_since.replace(tzinfo=UTC)
      else:
        ts = changed_since.astimezone(UTC)
      changed_since_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
      # Intuit accepts ISO 8601 with offset; ensure formatting matches their
      # docs (no colon in the offset is also fine, but we normalise to
      # "+0000" form which the strftime gives us).
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
      # QB returns 400 with a Fault payload when changedSince is older
      # than the supported window (~30 days). Detect by the structured
      # error message ONLY — we don't broadly treat all 400s as
      # "watermark too old" because malformed entity names, invalid
      # format, quota faults, etc. also surface as 400 and the caller
      # would silently fall back to a full lookback while masking the
      # real bug. Those propagate via raise_for_status() below.
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
      # 400 for some other reason — let it raise as an HTTPError so the
      # operator sees the actual fault instead of a silent watermark reset.
      resp.raise_for_status()
    resp.raise_for_status()

    data = resp.json()
    # CDCResponse shape:
    # {
    #   "CDCResponse": [
    #     { "QueryResponse": [ {"Customer": [...]}, {"Vendor": [...]}, ... ] }
    #   ],
    #   "time": "..."
    # }
    entities_by_type: dict[str, list[Any]] = {name: [] for name in entities}
    for cdc_block in data.get("CDCResponse", []):
      for query_resp in cdc_block.get("QueryResponse", []):
        for key, value in query_resp.items():
          if key in entities_by_type and isinstance(value, list):
            entities_by_type[key].extend(value)
    return entities_by_type, False
