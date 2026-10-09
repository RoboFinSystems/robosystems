"""The one Dagster asset — pull the feed, link the accounts, capture the events.

No dbt and no DuckDB, so ``mercury_feed`` (job ``mercury_sync``) is extract,
transform and load in one body, wrapped in the shared ``bank_feed.sync``
failure handling.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any

from dagster import (
  AssetExecutionContext,
  AssetSelection,
  MaterializeResult,
  asset,
  define_asset_job,
)

from robosystems.adapters.bank_feed.sync import (
  BankFeedSyncConfig,
  bootstrap_fiscal_calendar_if_needed,
  connection_removed,
  default_backfill_start,
  last_sync,
  mark_graph_stale,
  record_failed_sync_result,
  release_sync_lock,
  removed_during_sync,
  update_last_sync,
)

SOURCE = "mercury"
SOURCE_LABEL = "Mercury"
DEFAULT_LOOKBACK_DAYS = 60


class MercurySyncConfig(BankFeedSyncConfig):
  """Run configuration for ``mercury_feed``, bound by the provider."""

  lookback_days: int = DEFAULT_LOOKBACK_DAYS


@asset(
  group_name="mercury_pipeline",
  description="Pull the Mercury feed and capture it into the ledger inbox",
  kinds={"mercury"},
  metadata={"pipeline": "mercury", "stage": "sync"},
)
def mercury_feed(
  context: AssetExecutionContext, config: MercurySyncConfig
) -> MaterializeResult:
  try:
    return _run_mercury_sync(context, config)
  except Exception as exc:
    record_failed_sync_result(context, config, exc)
    raise
  finally:
    release_sync_lock(context, config)


mercury_sync_job = define_asset_job(
  name="mercury_sync",
  description="Mercury bank feed: pull, link accounts, capture events",
  selection=AssetSelection.assets(mercury_feed),
  tags={"pipeline": "mercury", "dagster/max_retries": "3"},
)


def get_dagster_components() -> dict[str, list]:
  return {
    "assets": [mercury_feed],
    "jobs": [mercury_sync_job],
    "sensors": [],
    "schedules": [],
  }


# ── the body ─────────────────────────────────────────────────────────────────


def _run_mercury_sync(
  context: AssetExecutionContext, config: MercurySyncConfig
) -> MaterializeResult:
  from robosystems.adapters.bank_feed.accounts import (
    account_entities,
    build_chart_index,
    chart_indexes,
    link_bank_accounts,
  )
  from robosystems.adapters.mercury.client import ConnectionTokenSource, MercuryClient
  from robosystems.adapters.mercury.pipeline.load import load_feed
  from robosystems.adapters.mercury.pipeline.transform import bank_accounts
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.providers.mercury_provider import (
    mercury_oauth_provider,
  )
  from robosystems.operations.roboledger.entity_scope import ensure_entity_id

  context.log.info(
    f"Mercury sync for graph={config.graph_id} connection={config.connection_id} "
    f"full_rebuild={config.full_rebuild}"
  )

  tokens = ConnectionTokenSource(config.connection_id, provider=mercury_oauth_provider)
  sync_config = dict(tokens.credentials().get("sync_config") or {})
  include_treasury = bool(sync_config.get("include_treasury", True))
  since = _since_date(config, sync_config, last_sync(config.connection_id))
  context.log.info(f"Pulling Mercury transactions since {since.isoformat()}")

  client = MercuryClient(mercury_oauth_provider.api_base_url, tokens)
  try:
    raw: dict[str, Any] = {
      "pulled_at": datetime.now(UTC).isoformat(timespec="seconds"),
      "since": since.isoformat(),
      "accounts": client.accounts(),
      "credit_accounts": client.credit_accounts(),
      "transactions": client.transactions(since),
    }
  finally:
    client.close()
  accounts = bank_accounts(raw, include_treasury=include_treasury)
  context.log.info(
    f"Mercury returned {len(accounts)} accounts and "
    f"{len(raw['transactions'])} transactions"
  )

  with extensions_session(config.graph_id, statement_timeout_ms=None) as session:
    if connection_removed(config.connection_id):
      raise removed_during_sync(SOURCE_LABEL)
    link_result = link_bank_accounts(
      session,
      accounts,
      provider=SOURCE,
      connection_id=config.connection_id,
      created_by=config.user_id,
      entity_id=sync_config.get("entity_id") or None,
    )
    chart = build_chart_index(session)
    # Each account books to the entity whose chart it is in.
    entities = account_entities(
      session, link_result.links.values(), parent_id=ensure_entity_id(session)
    )
    by_feed_account = {
      account_id: entities[element_id]
      for account_id, element_id in link_result.links.items()
      if entities.get(element_id)
    }
    report = load_feed(
      session,
      graph_id=config.graph_id,
      connection_id=config.connection_id,
      created_by=config.user_id,
      source=SOURCE,
      raw=raw,
      account_elements=link_result.links,
      chart=chart,
      include_treasury=include_treasury,
      account_entities=by_feed_account,
      charts_by_entity=chart_indexes(session, by_feed_account.values()),
    )
    session.commit()

  context.log.info(
    f"Accounts: {link_result.linked} linked, {link_result.created} created. "
    f"Events: {report.events_created} captured, {report.events_existing} existing, "
    f"{report.events_updated} refreshed, {report.events_failed} failed; "
    f"agents created {report.agents_created}; skipped {dict(report.skipped)}; "
    f"classified {dict(report.classification)}; suggestions {dict(report.resolved)}"
  )
  for error in report.errors:
    context.log.warning(f"Capture failed: {error}")

  summary = {
    "status": "succeeded",
    "synced_at": datetime.now(UTC).isoformat(),
    "window": {
      "since_date": since.isoformat(),
      "full_rebuild": bool(config.full_rebuild),
    },
    "counts": {
      **report.as_counts(),
      "accounts_linked": link_result.linked,
      "accounts_created": link_result.created,
    },
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
      "since": since.isoformat(),
      "accounts_linked": link_result.linked,
      "accounts_created": link_result.created,
      "agents_created": report.agents_created,
      "events_captured": report.events_created,
      "events_existing": report.events_existing,
      "events_updated": report.events_updated,
      "events_failed": report.events_failed,
      "errors": len(report.errors),
    }
  )


def _since_date(
  config: MercurySyncConfig,
  sync_config: dict[str, Any],
  previous_sync: datetime | None,
) -> date:
  """The pull window's start.

  An explicit ``since_date`` wins. A full rebuild — or a connection that has
  never synced — starts at the connect-time ``since_date`` (default: two
  years back). An incremental sync looks back ``lookback_days``,
  never earlier than the connect-time start.
  """
  if config.since_date:
    return date.fromisoformat(config.since_date)
  stored = sync_config.get("since_date")
  start = date.fromisoformat(str(stored)) if stored else default_backfill_start()
  if config.full_rebuild or previous_sync is None:
    return start
  lookback = date.today() - timedelta(days=max(int(config.lookback_days), 1))
  return max(lookback, start)
