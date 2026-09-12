"""Mercury API client — the read side of the bank feed, over either credential.

Two credential modes, one client. ``oauth`` (the hosted default) holds the
partner client's tokens in ``ConnectionCredentials``: a one-hour access token
and a 30-day single-use refresh token that rotates on every refresh, so the
rotated value is written back the moment a refresh succeeds. ``api_key``
(self-hosted and local deployments only) is a personal read-only token and
never changes. Nothing below the ``TokenSource`` seam cares which.

Mercury's most common partner failure is a refresh sent without ``scope``;
the refresh here always re-sends the granted scope. ``needs_reauth`` flips
only on ``invalid_grant`` / ``invalid_scope`` / ``invalid_client`` — a network
blip or a 5xx is retried on the next sync and never strands the connection.

API notes, verified against the sandbox 2026-09-10: ``GET /transactions``
spans every account in the org, takes ``start`` / ``end`` (dates) and pages
by ``start_after`` (the last id of the previous page); there is no
``postedStart`` and no single-transaction fetch. ``/credit`` answers 403 or
404 for an org with no IO card. Docs: https://docs.mercury.com/reference
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

import httpx

from robosystems.logger import logger

PAGE_SIZE = 500
DEFAULT_SCOPE = "read offline_access"
REFRESH_LEEWAY_SECONDS = 60.0
DEFAULT_ACCESS_LIFETIME_SECONDS = 3600.0
# Refresh refusals that mean the grant is gone and only a new consent fixes
# it. Anything else (a 5xx, a timeout, a malformed body) is transient.
REAUTH_ERROR_CODES = frozenset({"invalid_grant", "invalid_scope", "invalid_client"})
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class MercuryAuthError(Exception):
  """Mercury refused the credential.

  ``recoverable=False`` means the grant is gone (the connection has been
  marked ``needs_reauth`` and the operator must reconnect); ``True`` means a
  transient failure the next sync should retry without touching state.
  """

  def __init__(self, message: str, *, recoverable: bool, code: str | None = None):
    super().__init__(message)
    self.recoverable = recoverable
    self.code = code


class TokenSource(Protocol):
  """A live bearer token for the Mercury API."""

  def token(self) -> str: ...

  def invalidate(self) -> None:
    """The API answered 401 with the current token; obtain a fresh one."""
    ...


class StaticToken:
  """A personal API token — never refreshed."""

  def __init__(self, value: str) -> None:
    if not value:
      raise ValueError("Mercury API token is empty")
    self._value = value

  def token(self) -> str:
    return self._value

  def invalidate(self) -> None:
    return None


def _error_detail(response: httpx.Response) -> tuple[str | None, str]:
  try:
    body = response.json()
  except ValueError:
    return None, response.text[:200]
  if isinstance(body, dict):
    code = body.get("error")
    detail = body.get("error_description") or body.get("error") or str(body)
    return (str(code) if code else None), str(detail)[:300]
  return None, str(body)[:200]


def refresh_access_token(
  provider: Any,
  refresh_token: str,
  scope: str | None = None,
  *,
  http: httpx.Client | None = None,
) -> dict[str, Any]:
  """Trade a refresh token for a new token set, re-sending ``scope``.

  Synchronous on purpose: it runs inside the Dagster asset. The core
  ``OAuthHandler.refresh_tokens`` is async and hides the provider's error
  code behind a 400; the code is exactly what decides ``needs_reauth`` here.
  Returns ``access_token``, ``refresh_token`` (the rotated one, or the old
  one when Mercury did not rotate), ``expires_at`` (aware datetime) and
  ``scope``.
  """
  form = {
    "grant_type": "refresh_token",
    "refresh_token": refresh_token,
    "scope": scope or DEFAULT_SCOPE,
  }
  client = http or httpx.Client(timeout=30.0)
  try:
    try:
      response = client.post(
        provider.token_url,
        data=form,
        auth=httpx.BasicAuth(provider.client_id, provider.client_secret),
        headers={"Accept": "application/json"},
      )
    except httpx.HTTPError as exc:
      raise MercuryAuthError(
        f"Could not reach Mercury to refresh the token: {exc}", recoverable=True
      ) from exc
  finally:
    if http is None:
      client.close()

  if response.status_code >= 400:
    code, detail = _error_detail(response)
    if code in REAUTH_ERROR_CODES:
      raise MercuryAuthError(
        f"Mercury refused the refresh token ({code}): {detail}. Reconnect the "
        "account from the connections page.",
        recoverable=False,
        code=code,
      )
    raise MercuryAuthError(
      f"Mercury token refresh failed ({response.status_code}): {detail}",
      recoverable=True,
      code=code,
    )

  body = response.json()
  if not isinstance(body, dict) or not body.get("access_token"):
    raise MercuryAuthError(
      "Mercury token refresh returned no access_token", recoverable=True
    )
  expires_in = body.get("expires_in")
  lifetime = (
    float(expires_in)
    if isinstance(expires_in, (int, float)) and expires_in > 0
    else DEFAULT_ACCESS_LIFETIME_SECONDS
  )
  return {
    "access_token": str(body["access_token"]),
    "refresh_token": str(body.get("refresh_token") or refresh_token),
    "expires_at": datetime.now(UTC) + timedelta(seconds=lifetime),
    "scope": str(body.get("scope") or scope or DEFAULT_SCOPE),
  }


class ConnectionTokenSource:
  """The token behind a platform connection, refreshed and persisted.

  Reads the encrypted credential bundle once, refreshes the OAuth access
  token ahead of expiry (and on a 401), and writes the rotated refresh
  token back immediately — a rotated token that dies with the process
  strands the connection on the next sync. In ``api_key`` mode the bundle's
  key is the token and nothing rotates.
  """

  def __init__(
    self,
    connection_id: str,
    *,
    provider: Any | None = None,
    http: httpx.Client | None = None,
    leeway_seconds: float = REFRESH_LEEWAY_SECONDS,
  ) -> None:
    self.connection_id = connection_id
    self._provider = provider
    self._http = http
    self._leeway = leeway_seconds
    self._credentials: dict[str, Any] | None = None

  def credentials(self) -> dict[str, Any]:
    """The decrypted bundle (tokens, ``auth_mode``, ``sync_config``)."""
    if self._credentials is None:
      self._credentials = self._load()
    return self._credentials

  def token(self) -> str:
    creds = self.credentials()
    if creds.get("auth_mode") == "api_key":
      key = creds.get("api_key")
      if not key:
        raise MercuryAuthError(
          "Mercury connection is in api_key mode but holds no key",
          recoverable=False,
        )
      return str(key)
    if self._expires_within(creds, self._leeway):
      creds = self._refresh(creds)
    access = creds.get("access_token")
    if not access:
      raise MercuryAuthError(
        "Mercury connection holds no access token; reconnect the account",
        recoverable=False,
      )
    return str(access)

  def invalidate(self) -> None:
    creds = self.credentials()
    if creds.get("auth_mode") == "api_key":
      return
    self._refresh(creds)

  # -- internals -------------------------------------------------------------

  def _load(self) -> dict[str, Any]:
    from robosystems.database import SessionFactory
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )

    with SessionFactory() as session:
      cred = ConnectionCredentials.get_by_connection_id(self.connection_id, session)
      if cred is None:
        raise MercuryAuthError(
          f"No credentials stored for connection {self.connection_id}",
          recoverable=False,
        )
      return dict(cred.get_credentials())

  @staticmethod
  def _expires_within(creds: dict[str, Any], seconds: float) -> bool:
    raw = creds.get("expires_at")
    if not raw:
      return True
    try:
      expires_at = datetime.fromisoformat(str(raw))
    except ValueError:
      return True
    if expires_at.tzinfo is None:
      expires_at = expires_at.replace(tzinfo=UTC)
    return datetime.now(UTC) + timedelta(seconds=seconds) >= expires_at

  def _provider_or_default(self) -> Any:
    if self._provider is None:
      from robosystems.operations.providers.mercury_provider import (
        mercury_oauth_provider,
      )

      self._provider = mercury_oauth_provider
    return self._provider

  def _refresh(self, creds: dict[str, Any]) -> dict[str, Any]:
    refresh_token = creds.get("refresh_token")
    if not refresh_token:
      self._mark_needs_reauth()
      raise MercuryAuthError(
        "Mercury connection holds no refresh token; reconnect the account",
        recoverable=False,
      )
    try:
      fresh = refresh_access_token(
        self._provider_or_default(),
        str(refresh_token),
        str(creds.get("scope") or DEFAULT_SCOPE),
        http=self._http,
      )
    except MercuryAuthError as exc:
      if not exc.recoverable:
        self._mark_needs_reauth()
      raise
    merged = {
      **creds,
      "access_token": fresh["access_token"],
      "refresh_token": fresh["refresh_token"],
      "expires_at": fresh["expires_at"].isoformat(),
      "scope": fresh["scope"],
    }
    self._persist(merged, fresh["expires_at"])
    self._credentials = merged
    return merged

  def _persist(self, merged: dict[str, Any], expires_at: datetime) -> None:
    from robosystems.database import SessionFactory
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )

    try:
      with SessionFactory() as session:
        cred = ConnectionCredentials.get_by_connection_id(self.connection_id, session)
        if cred is None:
          logger.warning(
            "Cannot persist rotated Mercury tokens — no credentials row for "
            f"connection {self.connection_id}"
          )
          return
        cred.update_credentials(merged, session)
        cred.update_expiry(expires_at, session)
        logger.info(
          f"Persisted rotated Mercury tokens for connection {self.connection_id}"
        )
    except Exception as exc:
      # The in-memory token still works for this run; the next refresh either
      # re-rotates and re-persists or trips the single-use rule and re-auths.
      logger.warning(
        "Failed to persist rotated Mercury tokens for connection "
        f"{self.connection_id} (non-fatal): {exc}"
      )

  def _mark_needs_reauth(self) -> None:
    try:
      from robosystems.operations.connection_service import ConnectionService

      ConnectionService.mark_connection_needs_reauth_sync(self.connection_id)
    except Exception as exc:
      logger.warning(
        f"Failed to mark connection {self.connection_id} as needs_reauth "
        f"(non-fatal): {exc}"
      )


class MercuryClient:
  """Read the bank feed: accounts, the IO card, categories, transactions."""

  def __init__(
    self,
    base_url: str,
    token_source: TokenSource,
    *,
    http: httpx.Client | None = None,
    timeout: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    max_attempts: int = 4,
  ) -> None:
    self._base_url = base_url.rstrip("/")
    self._tokens = token_source
    self._http = http or httpx.Client(timeout=timeout)
    self._owns_http = http is None
    self._sleep = sleep
    self._max_attempts = max_attempts

  def accounts(self) -> list[dict[str, Any]]:
    """Checking, savings and treasury accounts."""
    return list(self._get("/accounts").get("accounts") or [])

  def credit_accounts(self) -> list[dict[str, Any]]:
    """IO credit-card accounts; empty when the org has no card."""
    try:
      return list(self._get("/credit").get("accounts") or [])
    except httpx.HTTPStatusError as exc:
      if exc.response.status_code in (403, 404):
        return []
      raise

  def categories(self) -> list[dict[str, Any]]:
    """The org's custom transaction categories."""
    return list(self._get("/categories").get("categories") or [])

  def transactions(
    self, since: date, until: date | None = None
  ) -> list[dict[str, Any]]:
    """Every transaction across all accounts from ``since``, oldest first."""
    params: dict[str, Any] = {
      "start": since.isoformat(),
      "limit": PAGE_SIZE,
      "order": "asc",
    }
    if until is not None:
      params["end"] = until.isoformat()
    collected: list[dict[str, Any]] = []
    while True:
      page = list(self._get("/transactions", params=params).get("transactions") or [])
      collected.extend(page)
      if len(page) < PAGE_SIZE:
        return collected
      cursor = page[-1].get("id")
      if not cursor:
        raise MercuryAuthError(
          "Mercury returned a transaction page whose last row has no id; "
          "cannot continue paging",
          recoverable=True,
        )
      params["start_after"] = cursor

  def close(self) -> None:
    if self._owns_http:
      self._http.close()

  def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    refreshed = False
    for attempt in range(self._max_attempts):
      response = self._http.get(
        f"{self._base_url}{path}",
        params=params,
        headers={
          "Authorization": f"Bearer {self._tokens.token()}",
          "Accept": "application/json",
        },
      )
      if response.status_code == 401:
        if refreshed:
          raise MercuryAuthError(
            "Mercury rejected the access token after a refresh", recoverable=True
          )
        refreshed = True
        self._tokens.invalidate()
        continue
      if (
        response.status_code in RETRYABLE_STATUSES and attempt < self._max_attempts - 1
      ):
        retry_after = response.headers.get("Retry-After")
        try:
          delay = float(retry_after) if retry_after else float(2**attempt)
        except ValueError:
          delay = float(2**attempt)
        self._sleep(min(delay, 60.0))
        continue
      response.raise_for_status()
      body = response.json()
      return body if isinstance(body, dict) else {}
    raise MercuryAuthError("Mercury request exhausted its retries", recoverable=True)
