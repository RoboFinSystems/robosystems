"""The one Dagster asset — sync the Item's cursor, link the accounts, load the changes.

``plaid_feed`` (job ``plaid_sync``) pulls the Item's accounts and every
change since the stored cursor, links a chart account to each cash and card
account, and loads the changes into the inbox. The cursor advances only after
the load commits, so a failed run replays the same window (every write is
idempotent on the event's natural key).

A full rebuild, or an explicit ``since_date``, drops the cursor and replays
the Item's whole history. A fresh Item's first pull may not have landed at
Plaid yet (``NOT_READY``); the asset waits a little, and if it still has
nothing, finishes without storing a cursor so the next sync starts over.

The body keeps the shared bank-feed discipline (``adapters/bank_feed/sync.py``).
A login the customer has to repair marks the connection ``needs_reauth`` and
fails. The job retries a run whose worker died, never one whose body failed:
the client already retries rate limits and 5xx inside the run, and a dead
login retried three times helps no one.
"""

import time
from datetime import UTC, date, datetime
from typing import Any

from dagster import (
  AssetExecutionContext,
  AssetSelection,
  Failure,
  MaterializeResult,
  asset,
  define_asset_job,
)

from robosystems.adapters.bank_feed.sync import (
  BankFeedSyncConfig,
  bootstrap_fiscal_calendar_if_needed,
  default_backfill_start,
  mark_graph_stale,
  record_failed_sync_result,
  release_sync_lock,
  update_last_sync,
)

SOURCE = "plaid"
SOURCE_LABEL = "Plaid"
INITIAL_PULL_WAIT_SECONDS = 120
INITIAL_PULL_POLL_SECONDS = 10


class PlaidSyncConfig(BankFeedSyncConfig):
  """Run configuration for ``plaid_feed``, bound by the provider."""


@asset(
  group_name="plaid_pipeline",
  description="Sync the Plaid Item and capture its changes into the ledger inbox",
  kinds={"plaid"},
  metadata={"pipeline": "plaid", "stage": "sync"},
)
def plaid_feed(
  context: AssetExecutionContext, config: PlaidSyncConfig
) -> MaterializeResult:
  try:
    return _run_plaid_sync(context, config)
  except Exception as exc:
    record_failed_sync_result(context, config, exc)
    raise
  finally:
    release_sync_lock(context, config)


plaid_sync_job = define_asset_job(
  name="plaid_sync",
  description="Plaid bank feed: sync the cursor, link accounts, capture events",
  selection=AssetSelection.assets(plaid_feed),
  tags={
    "pipeline": "plaid",
    "dagster/max_retries": "3",
    "dagster/retry_on_asset_or_op_failure": "false",
  },
)


def get_dagster_components() -> dict[str, list]:
  return {
    "assets": [plaid_feed],
    "jobs": [plaid_sync_job],
    "sensors": [],
    "schedules": [],
  }


# ── the body ─────────────────────────────────────────────────────────────────


def _run_plaid_sync(
  context: AssetExecutionContext, config: PlaidSyncConfig
) -> MaterializeResult:
  from robosystems.adapters.bank_feed.accounts import (
    build_chart_index,
    link_bank_accounts,
  )
  from robosystems.adapters.plaid.client import PlaidError
  from robosystems.adapters.plaid.pipeline.load import load_sync
  from robosystems.adapters.plaid.pipeline.transform import bank_accounts
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.providers.plaid_provider import plaid_client

  credentials = load_credentials(config.connection_id)
  access_token = credentials.get("access_token")
  if not access_token:
    raise Failure(
      description=(
        "This Plaid connection has not finished Link; there is no Item to sync."
      ),
    )
  sync_config = dict(credentials.get("sync_config") or {})
  since = since_date(config, sync_config)
  cursor = sync_cursor(config, credentials)
  item_id = credentials.get("item_id")
  context.log.info(
    f"Plaid sync for graph={config.graph_id} connection={config.connection_id} "
    f"item={item_id} from {'the stored cursor' if cursor else 'the start'}"
  )

  client = plaid_client()
  try:
    accounts_body = client.get_accounts(access_token)
    sync = client.sync_transactions(access_token, cursor)
    waited = 0
    while not cursor and not sync.ready and waited < INITIAL_PULL_WAIT_SECONDS:
      context.log.info("Plaid's first pull for this Item has not landed; waiting")
      time.sleep(INITIAL_PULL_POLL_SECONDS)
      waited += INITIAL_PULL_POLL_SECONDS
      sync = client.sync_transactions(access_token, None)
  except PlaidError as exc:
    if exc.needs_reauth:
      mark_needs_reauth(config.connection_id)
      raise Failure(
        description=f"Plaid needs the customer back in Link: {exc}",
        metadata={"plaid_error_code": exc.code or ""},
      ) from exc
    raise
  finally:
    client.close()

  institution = str(
    credentials.get("institution_name")
    or (accounts_body.get("item") or {}).get("institution_name")
    or "Bank"
  )
  accounts = bank_accounts(accounts_body.get("accounts") or [], institution=institution)
  context.log.info(
    f"Plaid returned {len(accounts)} cash and card accounts; "
    f"{len(sync.added)} added, {len(sync.modified)} modified, "
    f"{len(sync.removed)} removed (update status {sync.update_status})"
  )

  with extensions_session(config.graph_id, statement_timeout_ms=None) as session:
    link_result = link_bank_accounts(
      session,
      accounts,
      provider=SOURCE,
      connection_id=config.connection_id,
      created_by=config.user_id,
    )
    chart = build_chart_index(session)
    report = load_sync(
      session,
      graph_id=config.graph_id,
      connection_id=config.connection_id,
      item_id=item_id,
      created_by=config.user_id,
      accounts=accounts,
      sync=sync,
      account_elements=link_result.links,
      chart=chart,
      since=since,
    )
    session.commit()

  cursor_stored = bool(sync.ready and sync.next_cursor)
  if cursor_stored:
    store_cursor(config.connection_id, sync.next_cursor)

  context.log.info(
    f"Accounts: {link_result.linked} linked, {link_result.created} created. "
    f"Events: {report.events_created} captured, {report.events_existing} existing, "
    f"{report.events_updated} refreshed, {report.events_removed} removed, "
    f"{report.transfers_matched} transfers matched, {report.events_failed} failed; "
    f"skipped {dict(report.skipped)}; classified {dict(report.classification)}"
  )
  for error in report.errors:
    context.log.warning(f"Capture failed: {error}")

  summary = {
    "status": "succeeded",
    "synced_at": datetime.now(UTC).isoformat(),
    "window": {
      "since_date": since.isoformat(),
      "full_rebuild": cursor is None,
    },
    "counts": {
      **report.as_counts(),
      "accounts_linked": link_result.linked,
      "accounts_created": link_result.created,
    },
    "source_status": sync.update_status,
    "cursor_stored": cursor_stored,
    "errors": list(report.errors[:10]),
  }
  update_last_sync(context, config, summary)
  bootstrap_fiscal_calendar_if_needed(
    context, config, report.earliest_occurred_at, source_label=SOURCE_LABEL
  )
  mark_graph_stale(context, config, source_label=SOURCE_LABEL)

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "item_id": item_id or "",
      "accounts_linked": link_result.linked,
      "accounts_created": link_result.created,
      "events_captured": report.events_created,
      "events_existing": report.events_existing,
      "events_updated": report.events_updated,
      "events_removed": report.events_removed,
      "transfers_matched": report.transfers_matched,
      "events_failed": report.events_failed,
      "cursor_stored": cursor_stored,
    }
  )


def since_date(config: PlaidSyncConfig, sync_config: dict[str, Any]) -> date:
  """The earliest posting date captured: an explicit ``since_date`` for this
  run, else the connect-time one, else 1 January of last year."""
  if config.since_date:
    return date.fromisoformat(config.since_date)
  stored = sync_config.get("since_date")
  return date.fromisoformat(str(stored)) if stored else default_backfill_start()


def sync_cursor(config: PlaidSyncConfig, credentials: dict[str, Any]) -> str | None:
  """The stored cursor, unless this run replays the history (a full rebuild,
  or an explicit window)."""
  if config.full_rebuild or config.since_date:
    return None
  return credentials.get("cursor") or None


# ── credentials ──────────────────────────────────────────────────────────────


def load_credentials(connection_id: str) -> dict[str, Any]:
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )

  with SessionFactory() as session:
    row = ConnectionCredentials.get_by_connection_id(connection_id, session)
    return dict(row.get_credentials()) if row is not None else {}


def store_cursor(connection_id: str, cursor: str) -> None:
  """Advance the stored cursor, re-reading the bundle so a concurrent re-link
  that replaced the access token is never overwritten."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )

  with SessionFactory() as session:
    row = ConnectionCredentials.get_by_connection_id(connection_id, session)
    if row is None:
      return
    current = dict(row.get_credentials())
    row.update_credentials(
      {
        **current,
        "cursor": cursor,
        "cursor_updated_at": datetime.now(UTC).isoformat(),
      },
      session,
    )


def mark_needs_reauth(connection_id: str) -> None:
  from robosystems.operations.connection_service import ConnectionService

  ConnectionService.mark_connection_needs_reauth_sync(connection_id)
