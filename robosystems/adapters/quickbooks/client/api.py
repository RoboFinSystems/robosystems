import json
import random
import time
from datetime import UTC, datetime
from typing import Any

import requests
from intuitlib.client import AuthClient as _IntuitAuthClient
from intuitlib.exceptions import AuthClientError
from quickbooks import QuickBooks
from quickbooks.exceptions import AuthorizationException, QuickbooksException
from retrying import retry

from robosystems.config import env
from robosystems.logger import logger

# Neither library sets a timeout, so a stalled socket would block forever,
# on the close path while it holds the ledger transaction.
QB_TIMEOUT = (10, 120)  # (connect, read) seconds

_REFRESH_ATTEMPTS = 3
_REFRESH_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_TOKEN_LOCK_TTL_SECONDS = 60
_TOKEN_LOCK_WAIT_SECONDS = 30


def _with_default_timeout(session: requests.Session) -> None:
  """Give every request on ``session`` ``QB_TIMEOUT`` unless the call sets one."""
  send = session.request

  def request(method, url, **kwargs):
    kwargs.setdefault("timeout", QB_TIMEOUT)
    return send(method, url, **kwargs)

  session.request = request  # type: ignore[method-assign]


class AuthClient(_IntuitAuthClient):
  """intuitlib's client (a ``requests.Session``) with ``QB_TIMEOUT`` on every
  call, including the discovery fetch its constructor makes."""

  def request(self, method, url, *args, **kwargs):  # type: ignore[override]
    kwargs.setdefault("timeout", QB_TIMEOUT)
    return super().request(method, url, *args, **kwargs)


def _is_invalid_grant(error: AuthClientError) -> bool:
  """Intuit's answer for a revoked, expired or superseded refresh token.

  The only token-endpoint failure a reconnect fixes.
  """
  try:
    body = json.loads(error.content or b"{}")
  except (ValueError, TypeError):
    return False
  return isinstance(body, dict) and body.get("error") == "invalid_grant"


class QBAuthFailedError(Exception):
  """QB token refresh failed.

  ``recoverable=True``: the connection state is untouched and the next sync
  retries (a network error, a timeout, 429/5xx, or any other refusal a
  reconnect would not fix). ``False``: Intuit answered ``invalid_grant`` and
  the connection has been marked ``needs_reauth``.
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
      # Not ``if exc.response``: a Response is falsy for any 4xx/5xx.
      status = getattr(exc.response, "status_code", None)
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
      self._refresh_serialized(qb_credentials)
    else:
      self.refresh_token = self.auth_client.refresh_token
      self.access_token = self.auth_client.access_token

    self.client = QuickBooks(
      auth_client=self.auth_client,
      refresh_token=self.refresh_token,
      company_id=self.realm_id,
      minorversion=75,
    )
    if isinstance(self.client.session, requests.Session):
      _with_default_timeout(self.client.session)

  def _refresh_serialized(self, qb_credentials: dict[str, Any]) -> None:
    """Refresh under a per-connection lock, from the stored token.

    Intuit invalidates a refresh token once it issues the next one, so two
    processes refreshing the same stale token would leave the loser with
    ``invalid_grant`` on a healthy connection. The lock orders them, and
    re-reading the stored credentials under it hands the second one the
    token the first just persisted.
    """
    lock = self._acquire_token_lock()
    try:
      stored = self._read_stored_credentials()
      presented_refresh = qb_credentials["refresh_token"]
      presented_access = qb_credentials.get("access_token")
      if stored and stored.get("refresh_token"):
        # A peer may have rotated the token since the caller read it.
        presented_refresh = stored["refresh_token"]
        presented_access = stored.get("access_token") or presented_access
        self.auth_client.refresh_token = presented_refresh
        if presented_access:
          self.auth_client.access_token = presented_access

      logger.info(f"Refreshing QuickBooks token for realm {self.realm_id}")
      self._refresh_with_retry(presented_refresh)

      self.refresh_token = self.auth_client.refresh_token
      self.access_token = self.auth_client.access_token
      logger.info(
        f"Token refresh complete: access_token={'yes' if self.access_token else 'no'}, "
        f"refresh_token={'yes' if self.refresh_token else 'no'}"
      )

      # Intuit rotates the refresh_token on every refresh; an unpersisted
      # rotation locks the connection out once the grace window expires.
      if self.connection_id and (
        self.refresh_token != presented_refresh or self.access_token != presented_access
      ):
        self._persist_rotated_tokens(stored or qb_credentials)
    finally:
      if lock is not None:
        lock.release()

  def _refresh_with_retry(self, refresh_token: str) -> None:
    """One refresh, retried only where Intuit cannot have issued a token.

    429 and 5xx answers, and connect timeouts, are retried with backoff.
    Anything that can fail after the request was sent (a read timeout, a
    dropped connection) is not: Intuit may already have rotated the token,
    and a second call would present a dead one.
    """
    for attempt in range(1, _REFRESH_ATTEMPTS + 1):
      try:
        self.auth_client.refresh(refresh_token=refresh_token)
        return
      except AuthClientError as e:
        status = getattr(e, "status_code", None)
        if _is_invalid_grant(e):
          logger.warning(
            f"Intuit rejected the refresh token for realm {self.realm_id} "
            f"(invalid_grant); marking the connection needs_reauth"
          )
          self._mark_needs_reauth()
          raise QBAuthFailedError(
            f"QuickBooks rejected the credential refresh for realm "
            f"{self.realm_id}. Reconnect the account from the connections "
            f"page to re-issue tokens.",
            recoverable=False,
          ) from e
        if status in _REFRESH_RETRY_STATUSES and attempt < _REFRESH_ATTEMPTS:
          self._backoff(attempt, f"HTTP {status}")
          continue
        logger.error(
          f"QB token refresh for realm {self.realm_id} failed with HTTP "
          f"{status} (intuit_tid={getattr(e, 'intuit_tid', None)}); "
          f"connection left as is"
        )
        raise QBAuthFailedError(
          f"Intuit's token endpoint answered HTTP {status} for realm "
          f"{self.realm_id}; the next sync will retry.",
          recoverable=True,
        ) from e
      except requests.exceptions.ConnectTimeout as e:
        if attempt < _REFRESH_ATTEMPTS:
          self._backoff(attempt, "connect timeout")
          continue
        raise self._transient(e) from e
      except requests.exceptions.RequestException as e:
        raise self._transient(e) from e

  def _backoff(self, attempt: int, reason: str) -> None:
    delay = 2 ** (attempt - 1) + random.random()
    logger.warning(
      f"QB token refresh for realm {self.realm_id}: {reason}; retrying in "
      f"{delay:.1f}s (attempt {attempt}/{_REFRESH_ATTEMPTS})"
    )
    time.sleep(delay)

  def _transient(self, e: Exception) -> "QBAuthFailedError":
    logger.warning(
      f"Transient network error during QB token refresh for realm {self.realm_id}: {e}"
    )
    return QBAuthFailedError(
      f"Transient network error reaching Intuit for realm "
      f"{self.realm_id}; the next sync will retry.",
      recoverable=True,
    )

  def _acquire_token_lock(self) -> Any:
    """The per-connection refresh lock, or None to proceed unlocked.

    Valkey being down, or a holder outliving the wait, must not stop a sync
    or a close: unlocked is the behaviour before the lock existed.
    """
    if not self.connection_id:
      return None
    try:
      from robosystems.config.valkey_registry import (
        ValkeyDatabase,
        create_redis_client,
      )
      from robosystems.middleware.auth.distributed_lock import DistributedLock

      lock = DistributedLock(
        create_redis_client(ValkeyDatabase.LOCKS),
        f"qb_token:{self.connection_id}",
        ttl_seconds=_TOKEN_LOCK_TTL_SECONDS,
      )
      result = lock.acquire(blocking=True, timeout=_TOKEN_LOCK_WAIT_SECONDS)
    except Exception as e:
      logger.warning(
        f"QB token lock unavailable for connection {self.connection_id}; "
        f"refreshing unlocked: {e}"
      )
      return None
    if not result.acquired:
      logger.warning(
        f"QB token lock for connection {self.connection_id} not acquired "
        f"({result.error_message}); refreshing unlocked"
      )
      return None
    return lock

  def _read_stored_credentials(self) -> dict[str, Any] | None:
    if not self.connection_id:
      return None
    try:
      from robosystems.database import SessionFactory
      from robosystems.models.core.connection.connection_credentials import (
        ConnectionCredentials,
      )

      with SessionFactory() as session:
        cred = ConnectionCredentials.get_by_connection_id(self.connection_id, session)
        return cred.get_credentials() if cred is not None else None
    except Exception as e:
      logger.warning(
        f"Could not re-read QB credentials for connection {self.connection_id}; "
        f"using the ones passed in: {e}"
      )
      return None

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
  def get_transactions(self, start_date=None, end_date=None):
    """Fetch JournalReport, the live GL posting source."""
    params = {}
    if start_date:
      params["start_date"] = start_date
    if end_date:
      params["end_date"] = end_date
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
    back with ``status='Deleted'`` in the same lists; nothing handles them yet
    (the loader has no soft-delete path), so a QB-side delete persists here
    until the next full rebuild. ``watermark_too_old``
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
