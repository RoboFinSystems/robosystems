"""Plaid API client — Link, the Item, its accounts, and the transactions cursor.

Plaid authenticates every call with the platform's client id and secret in the
JSON body; the customer's credential is a per-Item ``access_token`` that never
expires and never rotates (it dies only when the Item is removed or its login
breaks). So there is no token source here, only the one client.

Errors come back as a JSON body with ``error_type`` / ``error_code``. Two
classes matter to the feed: a login that needs the customer back in Link
(``ITEM_LOGIN_REQUIRED`` and friends — the connection goes ``needs_reauth``
and Link reopens in update mode on the same Item), and a transient failure
(rate limits, an institution down) the next sync retries without touching
state.

API notes, verified against the sandbox 2026-09-16: ``/transactions/sync``
answers ``transactions_update_status: NOT_READY`` with an empty page until the
Item's initial pull lands (seconds in the sandbox); amounts are positive for
money leaving the account; ``personal_finance_category`` arrives as v2.
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
    """``{access_token, item_id}`` for the public token Link handed back."""
    return self._post("/item/public_token/exchange", {"public_token": public_token})

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

  def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
    for attempt in range(self._max_attempts):
      try:
        response = self._http.post(
          f"{self._base_url}{path}", json={**self._auth, **body}
        )
      except httpx.HTTPError as exc:
        if attempt < self._max_attempts - 1:
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
