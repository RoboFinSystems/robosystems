"""Plaid connection provider — the aggregator bank feed.

A bank-feed provider (``BANK_FEED_PROVIDERS``): native accounting, so the
provider guard requires a chart of accounts and no live QuickBooks before a
connection is created. One connection is one Plaid Item — one institution
login for one customer — so a graph can hold several.

The connect flow reuses the OAuth endpoints, because Plaid Link is a consent
flow with an embedded widget instead of a redirect:

- ``oauth/init`` mints a Link token (``create_link_token``) — for a new Item,
  or in update mode on the connection's Item when its login needs repair.
- The app opens Link; Link hands back a ``public_token``.
- ``oauth/callback/plaid`` takes that token as ``code`` and completes the
  link (``complete_plaid_link``): exchange it for the Item's access token,
  refuse an Item the graph already has, store the credential encrypted,
  record the consent, and start the first sync.

The platform's Plaid client id and secret sit in Secrets Manager; the
customer's credential is the per-Item access token, never a pasted key.
Disconnect removes the Item at Plaid and runs ``purge_bank_feed`` on the graph.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy.orm import Session

from ...adapters.plaid.client import PlaidClient, PlaidError
from ...config import env
from ...logger import logger
from ...models.api.graphs.connections import PlaidConnectionConfig
from ...operations.connection_service import ConnectionService
from .bank_feed import (
  purge_bank_feed_connection,
  record_bank_feed_consent,
  record_bank_feed_purged,
)
from .types import SyncOutcome

PROVIDER = "plaid"
AUTH_MODE = "link"
# Link sessions can outlast the default ten-minute OAuth state: a bank's own
# multi-factor step happens inside the widget.
LINK_STATE_TTL_SECONDS = 1800
BOOKED_ACCOUNT_TYPES = frozenset({"depository", "credit"})


class DuplicateBankConnectionError(Exception):
  """The Item Link returned is a login the graph already has connected."""

  def __init__(self, existing_connection_id: str, institution: str | None) -> None:
    super().__init__(
      f"{institution or 'This bank'} is already connected to this graph "
      f"(connection {existing_connection_id}). Reconnect that connection instead."
    )
    self.existing_connection_id = existing_connection_id
    self.institution = institution


def plaid_client() -> PlaidClient:
  return PlaidClient(
    client_id=env.PLAID_CLIENT_ID,
    secret=env.PLAID_SECRET,
    environment=env.PLAID_ENVIRONMENT,
  )


def days_requested(since_date: str | None, today: date | None = None) -> int | None:
  """How much history Link asks Plaid to pull for a new Item."""
  if not since_date:
    return None
  today = today or date.today()
  return max((today - date.fromisoformat(since_date)).days + 1, 1)


def _sync_config(config: PlaidConnectionConfig | None) -> dict[str, Any]:
  since = config.since_date if config is not None else None
  return {"since_date": since.isoformat() if since else None}


def _credentials(connection_id: str, db: Session) -> dict[str, Any]:
  from ...models.core import ConnectionCredentials

  row = ConnectionCredentials.get_by_connection_id(connection_id, db)
  return dict(row.get_credentials()) if row is not None else {}


async def create_plaid_connection(
  entity_id: str,
  config: PlaidConnectionConfig | None,
  user_id: str,
  graph_id: str,
  db: Session,
) -> str:
  """Open a ``pending_oauth`` connection holding only the connect-time sync
  config; the Item and its access token arrive when Link completes."""
  connection_data = await ConnectionService.create_connection(
    entity_id=graph_id,
    provider=PROVIDER,
    user_id=user_id,
    credentials={"auth_mode": AUTH_MODE, "sync_config": _sync_config(config)},
    metadata={"status": "pending_oauth"},
    graph_id=graph_id,
  )
  return str(connection_data["connection_id"])


async def create_link_token(
  connection_id: str, user_id: str, db: Session
) -> dict[str, Any]:
  """A Link token for this connection.

  A connection that already holds an Item opens Link in update mode on it,
  to repair the login or re-select accounts. An Item Plaid no longer knows
  cannot be updated, so Link starts a fresh one; the callback swaps it in.
  """
  credentials = _credentials(connection_id, db)
  access_token = credentials.get("access_token")
  history = days_requested((credentials.get("sync_config") or {}).get("since_date"))
  client = plaid_client()
  try:
    try:
      return await asyncio.to_thread(
        client.create_link_token,
        client_user_id=user_id,
        access_token=access_token,
        days_requested=history,
      )
    except PlaidError as exc:
      if not (access_token and exc.item_gone):
        raise
      logger.info(
        "Plaid Item for connection %s is gone (%s); Link will create a new one",
        connection_id,
        exc.code,
      )
      return await asyncio.to_thread(
        client.create_link_token, client_user_id=user_id, days_requested=history
      )
  finally:
    client.close()


async def complete_plaid_link(
  *,
  graph_id: str,
  connection: dict[str, Any],
  connection_id: str,
  public_token: str,
  user_id: str,
  db: Session,
) -> dict[str, Any]:
  """Finish Link: exchange, de-duplicate, store, record the consent, sync."""
  from .registry import provider_registry

  credentials = _credentials(connection_id, db)
  prior_access = credentials.get("access_token")
  prior_item = credentials.get("item_id")

  client = plaid_client()
  try:
    try:
      exchanged = await asyncio.to_thread(client.exchange_public_token, public_token)
      access_token = str(exchanged["access_token"])
      item_id = str(exchanged["item_id"])
    except PlaidError:
      if not prior_access:
        raise
      # Update mode repairs the Item in place; its public token has nothing
      # new to exchange for.
      access_token, item_id = str(prior_access), str(prior_item or "")

    accounts_body = await asyncio.to_thread(client.get_accounts, access_token)
    item = dict(accounts_body.get("item") or {})
    item_id = str(item.get("item_id") or item_id)
    institution_id = item.get("institution_id")
    institution_name = item.get("institution_name")
    if not institution_name and institution_id:
      institution = await asyncio.to_thread(client.get_institution, institution_id)
      institution_name = institution.get("name")
    fingerprint = account_fingerprint(accounts_body.get("accounts") or [])

    new_item = item_id != prior_item
    if new_item:
      duplicate = find_duplicate_item(
        graph_id,
        connection_id=connection_id,
        institution_id=institution_id,
        fingerprint=fingerprint,
        db=db,
      )
      if duplicate is not None:
        await asyncio.to_thread(client.remove_item, access_token)
        raise DuplicateBankConnectionError(duplicate, institution_name)
      if prior_access:
        await _remove_item_quietly(client, str(prior_access), connection_id)
  finally:
    client.close()

  stored = await ConnectionService.update(
    connection_id=connection_id,
    user_id=user_id,
    credentials={
      **credentials,
      "auth_mode": AUTH_MODE,
      "access_token": access_token,
      "item_id": item_id,
      "institution_id": institution_id,
      "institution_name": institution_name,
      "accounts": fingerprint,
      "cursor": None if new_item else credentials.get("cursor"),
      "linked_at": datetime.now(UTC).isoformat(),
    },
    metadata={
      "item_id": item_id,
      "institution_name": institution_name,
      "entity_name": institution_name,
    },
    status="connected",
    graph_id=graph_id,
    db_session=db,
  )
  if not stored:
    raise RuntimeError(
      f"The Plaid Item for connection {connection_id} could not be stored; "
      "run Link again."
    )

  record_bank_feed_consent(
    provider=PROVIDER,
    environment=env.PLAID_ENVIRONMENT,
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    auth_mode=AUTH_MODE,
    scope="transactions",
    institution=institution_name,
  )

  first_sync = new_item or (connection.get("metadata") or {}).get("last_sync") is None
  outcome = await provider_registry.sync_connection(
    PROVIDER, connection, {"full_rebuild": True} if first_sync else None, graph_id
  )
  logger.info(
    "Plaid Link complete for connection %s (item=%s, new_item=%s); sync %s",
    connection_id,
    item_id,
    new_item,
    outcome.task_id,
  )
  return {
    "success": True,
    "message": f"{institution_name or 'Bank'} connected through Plaid",
    "connection_id": connection_id,
    "auto_sync_task_id": outcome.task_id,
  }


def account_fingerprint(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """The cash and card accounts an Item exposes, as stored with the credential."""
  return [
    {
      "account_id": acct.get("account_id"),
      "name": acct.get("name"),
      "mask": acct.get("mask"),
      "type": acct.get("type"),
      "subtype": acct.get("subtype"),
    }
    for acct in accounts
    if acct.get("type") in BOOKED_ACCOUNT_TYPES
  ]


def find_duplicate_item(
  graph_id: str,
  *,
  connection_id: str,
  institution_id: str | None,
  fingerprint: list[dict[str, Any]],
  db: Session,
) -> str | None:
  """Another live Plaid connection on the graph at the same institution
  sharing an account (same mask and subtype) — Plaid's duplicate-Item test."""
  from ...models.core.connection.connection import Connection

  if not institution_id:
    return None
  wanted = {(a.get("mask"), a.get("subtype")) for a in fingerprint if a.get("mask")}
  for other in Connection.get_all_for_graph(graph_id, db):
    if (other.provider or "").lower() != PROVIDER or str(other.id) == connection_id:
      continue
    stored = _credentials(str(other.id), db)
    if stored.get("institution_id") != institution_id:
      continue
    theirs = {
      (a.get("mask"), a.get("subtype"))
      for a in stored.get("accounts") or []
      if a.get("mask")
    }
    if not wanted or not theirs or wanted & theirs:
      return str(other.id)
  return None


async def _remove_item_quietly(
  client: PlaidClient, access_token: str, connection_id: str
) -> None:
  try:
    await asyncio.to_thread(client.remove_item, access_token)
  except PlaidError as exc:
    logger.warning(
      "Could not remove the replaced Plaid Item for connection %s: %s",
      connection_id,
      exc,
    )


async def sync_plaid_connection(
  connection: dict[str, Any], sync_options: dict[str, Any] | None, graph_id: str
) -> SyncOutcome:
  """Submit the ``plaid_sync`` Dagster job; returns its run id.

  ``sync_options`` accepts ``full_rebuild`` (replay the Item's history from
  the start of its cursor), ``since_date`` (replay from that date) and
  ``sync_lock_id``. An incremental sync continues from the stored cursor.
  """
  from robosystems.middleware.sse.dagster_monitor import submit_dagster_job_sync

  options = sync_options or {}
  connection_id = str(connection.get("connection_id", ""))
  sync_config = {
    "graph_id": graph_id,
    "connection_id": connection_id,
    "user_id": str(connection.get("user_id", "")),
    "full_rebuild": bool(options.get("full_rebuild", False)),
    "since_date": str(options.get("since_date", "") or ""),
    "sync_lock_id": str(options.get("sync_lock_id", "") or ""),
  }
  run_id = submit_dagster_job_sync(
    job_name="plaid_sync",
    run_config={"ops": {"plaid_feed": {"config": sync_config}}},
    tags={"graph_id": graph_id, "connection_id": connection_id, "pipeline": PROVIDER},
  )
  logger.info(
    f"Plaid sync submitted for graph={graph_id}, connection={connection_id}, "
    f"run_id={run_id}, full_rebuild={sync_config['full_rebuild']}"
  )
  return SyncOutcome(status="dispatched", task_id=run_id)


async def cleanup_plaid_connection(connection: dict[str, Any], graph_id: str) -> None:
  """Disconnect: remove the Item at Plaid, purge the feed, empty the credentials.

  Removal is best-effort (a dead Item must not block the disconnect); the
  purge is not — a failure surfaces and the disconnect is retried rather than
  leaving feed data behind.
  """
  from ...database import platform_session
  from ...models.core import ConnectionCredentials

  connection_id = str(connection.get("connection_id") or connection.get("id") or "")
  if not connection_id:
    logger.warning("Plaid cleanup: connection payload has no id; nothing to do")
    return

  with platform_session() as db:
    access_token = _credentials(connection_id, db).get("access_token")

  if access_token:
    client = plaid_client()
    try:
      await asyncio.to_thread(client.remove_item, str(access_token))
      logger.info(f"Removed the Plaid Item for connection {connection_id}")
    except PlaidError as exc:
      logger.warning(f"Plaid Item removal failed for {connection_id}: {exc}")
    finally:
      client.close()

  purged = purge_bank_feed_connection(
    graph_id, provider=PROVIDER, connection_id=connection_id
  )

  with platform_session() as db:
    creds = ConnectionCredentials.get_by_connection_id(connection_id, db)
    if creds:
      creds.update_credentials(
        {"auth_mode": AUTH_MODE, "revoked_at": datetime.now(UTC).isoformat()}, db
      )

  record_bank_feed_purged(
    provider=PROVIDER,
    connection=connection,
    graph_id=graph_id,
    connection_id=connection_id,
    auth_mode=AUTH_MODE,
    purged=purged,
  )
