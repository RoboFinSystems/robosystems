"""The ``plaid_feed`` asset (job ``plaid_sync``): sync the Item's cursor, link
a chart account per cash and card account, load the changes into the inbox.

The cursor advances only after the load commits with no failed rows, so a
failed run replays the same window; writes are idempotent on the event's
natural key. ``/transactions/sync`` never resends a window on its own.

Plaid pulls a new Item in two steps: ~30 recent days
(``INITIAL_UPDATE_COMPLETE``), then the rest (``HISTORICAL_UPDATE_COMPLETE``).
A run with only the recent window captures it but leaves the fiscal calendar
alone: bootstrapping on 30 days would close earlier months before their
transactions arrive, and the closed-period gate would then refuse them.

The job retries a run whose worker died, never one whose body failed: the
client already retries rate limits and 5xx.
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
# The first sync (no cursor) waits the long bound for Plaid's pull; a later
# run only rechecks, so a scheduled sync never holds a worker long.
PULL_WAIT_SECONDS = 600
PULL_RECHECK_SECONDS = 60
PULL_POLL_SECONDS = 10
# How many extra cursors the first run to see the history complete drains.
SETTLE_ROUNDS = 10


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
  history_seen = bool(credentials.get("history_complete_at"))
  item_id = credentials.get("item_id")
  context.log.info(
    f"Plaid sync for graph={config.graph_id} connection={config.connection_id} "
    f"item={item_id} from {'the stored cursor' if cursor else 'the start'}"
  )

  client = plaid_client()
  try:
    accounts_body = client.get_accounts(access_token)
    sync = sync_or_not_ready(client, access_token, cursor)
    waited = 0
    budget = PULL_WAIT_SECONDS if cursor is None else PULL_RECHECK_SECONDS
    while sync.pull_pending and waited < budget:
      context.log.info(
        f"Plaid is still pulling this Item's history ({sync.update_status}); waiting"
      )
      time.sleep(PULL_POLL_SECONDS)
      waited += PULL_POLL_SECONDS
      sync = sync_or_not_ready(client, access_token, cursor)
    if sync.history_complete and not history_seen:
      sync = settle_after_history(context, client, access_token, sync)
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

  if not sync.ready:
    raise Failure(
      description=(
        f"Plaid has not finished this Item's first pull after {waited} s; "
        "sync again in a few minutes."
      ),
      metadata={"plaid_update_status": sync.update_status or ""},
    )
  if not sync.history_complete:
    context.log.warning(
      f"Plaid's historical pull has not completed ({sync.update_status}): this "
      "run captures what has landed, the rest arrives on a later sync, and the "
      "fiscal calendar waits for it"
    )

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
      rekey_replaced=cursor is None,
    )
    session.commit()

  if report.events_failed:
    # The rows that captured are committed; the cursor stays where it was so
    # the next run replays this window and tries the failed rows again.
    mark_graph_stale(context, config, source_label=SOURCE_LABEL)
    for error in report.errors:
      context.log.warning(f"Capture failed: {error}")
    raise Failure(
      description=(
        f"{report.events_failed} of this window's transactions failed to "
        "capture; the cursor was not advanced, so the next sync retries them. "
        f"First: {report.errors[0] if report.errors else 'no detail'}"
      ),
      metadata={
        "events_failed": report.events_failed,
        "events_captured": report.events_created,
      },
    )

  cursor_stored = bool(sync.next_cursor)
  if cursor_stored:
    store_cursor(
      config.connection_id, sync.next_cursor, history_complete=sync.history_complete
    )

  context.log.info(
    f"Accounts: {link_result.linked} linked, {link_result.created} created. "
    f"Events: {report.events_created} captured, {report.events_existing} existing, "
    f"{report.events_updated} refreshed, {report.events_removed} removed, "
    f"{report.events_rekeyed} re-keyed, {report.transfers_matched} transfers "
    f"matched, {report.events_failed} failed; "
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
    "history_complete": sync.history_complete,
    "cursor_stored": cursor_stored,
    "errors": list(report.errors[:10]),
  }
  update_last_sync(context, config, summary)
  if sync.history_complete:
    bootstrap_fiscal_calendar_if_needed(
      context, config, report.earliest_occurred_at, source_label=SOURCE_LABEL
    )
  else:
    context.log.info(
      "Fiscal calendar bootstrap deferred until Plaid's historical pull completes"
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
      "events_rekeyed": report.events_rekeyed,
      "transfers_matched": report.transfers_matched,
      "events_failed": report.events_failed,
      "history_complete": sync.history_complete,
      "cursor_stored": cursor_stored,
    }
  )


def sync_or_not_ready(client: Any, access_token: str, cursor: str | None) -> Any:
  """One sync call, with Plaid's older way of saying "not yet" folded into
  the newer: ``PRODUCT_NOT_READY`` is an error on the wire, ``NOT_READY`` a
  status on the page, and the run waits for both the same way."""
  from robosystems.adapters.plaid.client import PlaidError, TransactionsSync

  try:
    return client.sync_transactions(access_token, cursor)
  except PlaidError as exc:
    if exc.code == "PRODUCT_NOT_READY":
      return TransactionsSync(update_status="NOT_READY")
    raise


def settle_after_history(
  context: AssetExecutionContext,
  client: Any,
  access_token: str,
  sync: Any,
) -> Any:
  """Drain what lands with the history-complete flag.

  Plaid can raise ``HISTORICAL_UPDATE_COMPLETE`` on a page holding only the
  recent window, with the history on the *next* cursor; keep pulling until a
  cursor returns nothing so the calendar opens on the whole history.
  """
  for _round in range(SETTLE_ROUNDS):
    more = client.sync_transactions(access_token, sync.next_cursor)
    if not (more.added or more.modified or more.removed):
      return sync
    context.log.info(
      f"Plaid delivered {len(more.added)} added, {len(more.modified)} modified, "
      f"{len(more.removed)} removed after the history flag; folding them in"
    )
    sync.extend(more)
  return sync


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


def store_cursor(
  connection_id: str, cursor: str, *, history_complete: bool = False
) -> None:
  """Advance the stored cursor, re-reading the bundle so a concurrent re-link
  that replaced the access token is never overwritten. The first time the
  history is complete, stamp ``history_complete_at``."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )

  with SessionFactory() as session:
    row = ConnectionCredentials.get_by_connection_id(connection_id, session)
    if row is None:
      return
    current = dict(row.get_credentials())
    now = datetime.now(UTC).isoformat()
    updated = {**current, "cursor": cursor, "cursor_updated_at": now}
    if history_complete and not current.get("history_complete_at"):
      updated["history_complete_at"] = now
    row.update_credentials(updated, session)


def mark_needs_reauth(connection_id: str) -> None:
  from robosystems.operations.connection_service import ConnectionService

  ConnectionService.mark_connection_needs_reauth_sync(connection_id)
