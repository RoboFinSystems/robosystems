"""Plaid API client — Link, the Item, its accounts, and the transactions cursor.

Every call carries the platform's client id and secret in the JSON body. The
customer's per-Item ``access_token`` never expires or rotates, so there is no
token refresh. Amounts are positive for money leaving the account.
Docs: https://plaid.com/docs/api/
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from robosystems.logger import logger

HOSTS = {
  "sandbox": "https://sandbox.plaid.com",
  "production": "https://production.plaid.com",
}
PRODUCTS = ["transactions"]
COUNTRY_CODES = ["US"]
LINK_CLIENT_NAME = "RoboLedger"
# Link offers only the accounts a bank feed books: cash and cards.
ACCOUNT_FILTERS = {
  "depository": {"account_subtypes": ["all"]},
  "credit": {"account_subtypes": ["all"]},
}
MAX_DAYS_REQUESTED = 730
SYNC_PAGE_SIZE = 500
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
# The login is broken or the consent is gone: only the customer, back in
# Link, fixes it.
REAUTH_ERROR_CODES = frozenset(
  {
    "ITEM_LOGIN_REQUIRED",
    "ACCESS_NOT_GRANTED",
    "INSUFFICIENT_CREDENTIALS",
    "ITEM_LOCKED",
    "USER_SETUP_REQUIRED",
    "USER_PERMISSION_REVOKED",
    "INVALID_ACCESS_TOKEN",
    "ITEM_NOT_FOUND",
  }
)
# The Item itself is gone; update mode cannot revive it, only a fresh Link.
ITEM_GONE_ERROR_CODES = frozenset({"INVALID_ACCESS_TOKEN", "ITEM_NOT_FOUND"})
MUTATION_DURING_PAGINATION = "TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION"
HISTORY_COMPLETE = "HISTORICAL_UPDATE_COMPLETE"
# The Item's pull is still running: nothing has landed (NOT_READY), or only
# the most recent ~30 days have (INITIAL_UPDATE_COMPLETE).
PULL_PENDING_STATUSES = frozenset({"NOT_READY", "INITIAL_UPDATE_COMPLETE"})


class PlaidError(Exception):
  """Plaid refused a request; ``code`` is Plaid's ``error_code``."""

  def __init__(
    self,
    message: str,
    *,
    status_code: int | None = None,
    error_type: str | None = None,
    code: str | None = None,
    request_id: str | None = None,
  ) -> None:
    super().__init__(message)
    self.status_code = status_code
    self.error_type = error_type
    self.code = code
    self.request_id = request_id

  @property
  def needs_reauth(self) -> bool:
    return self.code in REAUTH_ERROR_CODES

  @property
  def item_gone(self) -> bool:
    return self.code in ITEM_GONE_ERROR_CODES

  @classmethod
  def from_response(cls, response: httpx.Response) -> PlaidError:
    try:
      body = response.json()
    except ValueError:
      body = None
    if not isinstance(body, dict):
      return cls(
        f"Plaid returned {response.status_code}: {response.text[:200]}",
        status_code=response.status_code,
      )
    code = body.get("error_code")
    message = body.get("error_message") or body.get("display_message") or code
    return cls(
      f"Plaid {code or response.status_code}: {message}",
      status_code=response.status_code,
      error_type=body.get("error_type"),
      code=str(code) if code else None,
      request_id=body.get("request_id"),
    )


@dataclass
class TransactionsSync:
  """Everything ``/transactions/sync`` returned from one cursor to the end."""

  added: list[dict[str, Any]] = field(default_factory=list)
  modified: list[dict[str, Any]] = field(default_factory=list)
  removed: list[dict[str, Any]] = field(default_factory=list)
  accounts: list[dict[str, Any]] = field(default_factory=list)
  next_cursor: str = ""
  update_status: str | None = None

  @property
  def ready(self) -> bool:
    """False while the Item's initial pull has not landed at Plaid."""
    return self.update_status != "NOT_READY"

  @property
  def pull_pending(self) -> bool:
    """True while Plaid is still pulling the Item's history."""
    return self.update_status in PULL_PENDING_STATUSES

  @property
  def history_complete(self) -> bool:
    """True once the whole requested history is at Plaid."""
    return self.update_status == HISTORY_COMPLETE

  def extend(self, other: TransactionsSync) -> None:
    """Fold a later page set into this one; the cursor moves to the other's."""
    self.added.extend(other.added)
    self.modified.extend(other.modified)
    self.removed.extend(other.removed)
    if other.accounts:
      self.accounts = list(other.accounts)
    if other.next_cursor:
      self.next_cursor = other.next_cursor
    self.update_status = other.update_status or self.update_status


class PlaidClient:
  def __init__(
    self,
    *,
    client_id: str,
    secret: str,
    environment: str = "sandbox",
    http: httpx.Client | None = None,
    timeout: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    max_attempts: int = 4,
  ) -> None:
    if environment not in HOSTS:
      raise ValueError(f"Unknown Plaid environment: {environment!r}")
    if not client_id or not secret:
      raise PlaidError("Plaid client id and secret are not configured")
    self.environment = environment
    self._base_url = HOSTS[environment]
    self._auth = {"client_id": client_id, "secret": secret}
    self._http = http or httpx.Client(timeout=timeout)
    self._owns_http = http is None
    self._sleep = sleep
    self._max_attempts = max_attempts

  # -- Link and the Item -----------------------------------------------------

  def create_link_token(
    self,
    *,
    client_user_id: str,
    access_token: str | None = None,
    days_requested: int | None = None,
    redirect_uri: str | None = None,
  ) -> dict[str, Any]:
    """A Link session for a new Item, or update mode on an existing one.

    With ``access_token`` Link reopens on that Item to repair its login and
    re-select accounts; products and filters belong to the Item then and are
    not re-sent.
    """
    body: dict[str, Any] = {
      "client_name": LINK_CLIENT_NAME,
      "language": "en",
      "country_codes": COUNTRY_CODES,
      "user": {"client_user_id": client_user_id},
    }
    if access_token:
      body["access_token"] = access_token
      body["update"] = {"account_selection_enabled": True}
    else:
      body["products"] = PRODUCTS
      body["account_filters"] = ACCOUNT_FILTERS
      if days_requested:
        body["transactions"] = {
          "days_requested": max(1, min(int(days_requested), MAX_DAYS_REQUESTED))
        }
    if redirect_uri:
      body["redirect_uri"] = redirect_uri
    return self._post("/link/token/create", body)

  def exchange_public_token(self, public_token: str) -> dict[str, Any]:
    """``{access_token, item_id}`` for the public token Link handed back.

    Never retried on a transport error: a public token exchanges once, and
    a retry after a lost response would fail against an Item that now
    exists with no record of it.
    """
    return self._post(
      "/item/public_token/exchange",
      {"public_token": public_token},
      retry_transport=False,
    )

  def get_item(self, access_token: str) -> dict[str, Any]:
    return self._post("/item/get", {"access_token": access_token})

  def get_accounts(self, access_token: str) -> dict[str, Any]:
    """``{accounts, item}`` from Plaid's cache — no fresh pull at the bank."""
    return self._post("/accounts/get", {"access_token": access_token})

  def get_institution(self, institution_id: str) -> dict[str, Any]:
    body = self._post(
      "/institutions/get_by_id",
      {"institution_id": institution_id, "country_codes": COUNTRY_CODES},
    )
    return dict(body.get("institution") or {})

  def remove_item(self, access_token: str) -> None:
    """End the Item: Plaid stops billing it and the access token dies."""
    self._post("/item/remove", {"access_token": access_token})

  # -- transactions ----------------------------------------------------------

  def sync_transactions(
    self, access_token: str, cursor: str | None
  ) -> TransactionsSync:
    """Page ``/transactions/sync`` from ``cursor`` to the end.

    A mutation at Plaid mid-pagination restarts from the cursor the run
    began with, as Plaid requires; the pages already read are discarded.
    """
    for _restart in range(self._max_attempts):
      result = TransactionsSync()
      page_cursor = cursor or ""
      try:
        while True:
          body: dict[str, Any] = {
            "access_token": access_token,
            "count": SYNC_PAGE_SIZE,
          }
          if page_cursor:
            body["cursor"] = page_cursor
          page = self._post("/transactions/sync", body)
          result.added.extend(page.get("added") or [])
          result.modified.extend(page.get("modified") or [])
          result.removed.extend(page.get("removed") or [])
          if page.get("accounts"):
            result.accounts = list(page["accounts"])
          result.update_status = page.get("transactions_update_status")
          page_cursor = str(page.get("next_cursor") or "")
          if not page.get("has_more"):
            result.next_cursor = page_cursor
            return result
      except PlaidError as exc:
        if exc.code != MUTATION_DURING_PAGINATION:
          raise
        logger.info("Plaid data changed mid-pagination; restarting the sync")
    raise PlaidError(
      "Plaid transactions kept changing during pagination",
      code=MUTATION_DURING_PAGINATION,
    )

  # -- sandbox ---------------------------------------------------------------

  def create_sandbox_public_token(
    self, institution_id: str, *, days_requested: int = 90
  ) -> str:
    """A public token for a new sandbox Item, bypassing the Link UI."""
    if self.environment != "sandbox":
      raise PlaidError("Sandbox public tokens exist only in the sandbox")
    body = self._post(
      "/sandbox/public_token/create",
      {
        "institution_id": institution_id,
        "initial_products": PRODUCTS,
        "options": {"transactions": {"days_requested": days_requested}},
      },
    )
    return str(body["public_token"])

  # -- transport -------------------------------------------------------------

  def close(self) -> None:
    if self._owns_http:
      self._http.close()

  def _post(
    self, path: str, body: dict[str, Any], *, retry_transport: bool = True
  ) -> dict[str, Any]:
    for attempt in range(self._max_attempts):
      try:
        response = self._http.post(
          f"{self._base_url}{path}", json={**self._auth, **body}
        )
      except httpx.HTTPError as exc:
        if retry_transport and attempt < self._max_attempts - 1:
          self._sleep(float(2**attempt))
          continue
        raise PlaidError(f"Could not reach Plaid ({path}): {exc}") from exc
      if response.status_code == 200:
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
      if (
        response.status_code in RETRYABLE_STATUSES and attempt < self._max_attempts - 1
      ):
        self._sleep(min(float(2**attempt), 30.0))
        continue
      raise PlaidError.from_response(response)
    raise PlaidError(f"Plaid request exhausted its retries ({path})")
