"""The one Dagster asset — pull the feed, link the accounts, capture the events.

No dbt and no DuckDB, so three assets buy nothing: the ``mercury_feed`` asset (job ``mercury_sync``) is
extract, transform and load in one body. The whole body is wrapped so any
failure — a refused token, a Mercury 5xx, a chart with no room for a new
account — is recorded on the connection (``record_sync_result``) and
releases the per-connection sync lock; a bank feed that fails silently for
30 days and then loses its refresh token is worse than no feed. The job
carries ``dagster/max_retries: 3`` from birth.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any

from dagster import (
  AssetExecutionContext,
  AssetSelection,
  Config,
  MaterializeResult,
  asset,
  define_asset_job,
)

SOURCE = "mercury"
DEFAULT_LOOKBACK_DAYS = 60


class MercurySyncConfig(Config):
  """Run configuration for ``mercury_feed``, bound by the provider."""

  graph_id: str
  connection_id: str
  user_id: str
  full_rebuild: bool = False
  lookback_days: int = DEFAULT_LOOKBACK_DAYS
  since_date: str = ""
  # The lock token the dispatcher acquired; released here so the next sync
  # can start without waiting out the lock's TTL. Empty when no lock was
  # acquired (Valkey unavailable at dispatch).
  sync_lock_id: str = ""


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
    _record_failed_sync_result(context, config, exc)
    raise
  finally:
    _release_sync_lock(context, config)


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
  from robosystems.adapters.mercury.client import ConnectionTokenSource, MercuryClient
  from robosystems.adapters.mercury.pipeline.accounts import (
    build_chart_index,
    link_bank_accounts,
  )
  from robosystems.adapters.mercury.pipeline.load import load_feed
  from robosystems.adapters.mercury.pipeline.transform import bank_accounts
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.providers.mercury_provider import (
    mercury_oauth_provider,
  )

  context.log.info(
    f"Mercury sync for graph={config.graph_id} connection={config.connection_id} "
    f"full_rebuild={config.full_rebuild}"
  )

  tokens = ConnectionTokenSource(config.connection_id, provider=mercury_oauth_provider)
  sync_config = dict(tokens.credentials().get("sync_config") or {})
  include_treasury = bool(sync_config.get("include_treasury", True))
  since = _since_date(config, sync_config, _last_sync(config.connection_id))
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
    link_result = link_bank_accounts(
      session, accounts, connection_id=config.connection_id, created_by=config.user_id
    )
    chart = build_chart_index(session)
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
  _update_last_sync(context, config, summary)
  _bootstrap_fiscal_calendar_if_needed(context, config, report.earliest_occurred_at)
  _mark_graph_stale(context, config)

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
  last_sync: datetime | None,
) -> date:
  """The pull window's start.

  An explicit ``since_date`` wins. A full rebuild — or a connection that has
  never synced — starts at the connect-time ``since_date`` (default: the first
  of January of last year). An incremental sync looks back ``lookback_days``,
  never earlier than the connect-time start.
  """
  if config.since_date:
    return date.fromisoformat(config.since_date)
  stored = sync_config.get("since_date")
  start = date.fromisoformat(str(stored)) if stored else default_backfill_start()
  if config.full_rebuild or last_sync is None:
    return start
  lookback = date.today() - timedelta(days=max(int(config.lookback_days), 1))
  return max(lookback, start)


def default_backfill_start(today: date | None = None) -> date:
  today = today or date.today()
  return date(today.year - 1, 1, 1)


def _last_sync(connection_id: str) -> datetime | None:
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  with SessionFactory() as session:
    conn = Connection.get_by_id(connection_id, session)
    return conn.last_sync if conn is not None else None


# ── connection bookkeeping (mirrors the QuickBooks load asset) ────────────────


def _release_sync_lock(
  context: AssetExecutionContext, config: MercurySyncConfig
) -> None:
  if not config.sync_lock_id:
    return
  try:
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.middleware.auth.distributed_lock import release_lock_by_id

    # The dispatcher keys every provider's lock under the `qb_sync:` prefix.
    released = release_lock_by_id(
      create_redis_client(ValkeyDatabase.LOCKS),
      lock_key=f"qb_sync:{config.connection_id}",
      lock_id=config.sync_lock_id,
    )
    context.log.info(
      f"Sync lock {'released' if released else 'release no-op'} for connection "
      f"{config.connection_id}"
    )
  except Exception as exc:
    context.log.warning(
      f"Failed to release sync lock for connection {config.connection_id} "
      f"(non-fatal — TTL is the fallback): {exc}"
    )


def _record_failed_sync_result(
  context: AssetExecutionContext, config: MercurySyncConfig, exc: Exception
) -> None:
  """Persist a failed attempt without advancing ``last_sync``."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    with SessionFactory() as session:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.record_sync_result(
          session,
          {
            "status": "failed",
            "synced_at": datetime.now(UTC).isoformat(),
            "window": {
              "since_date": config.since_date or None,
              "full_rebuild": bool(config.full_rebuild),
            },
            "error": {"code": type(exc).__name__, "message": str(exc)[:500]},
          },
        )
        context.log.info("Recorded failed sync result on connection")
  except Exception as record_exc:
    context.log.warning(f"Failed to record sync failure (non-fatal): {record_exc}")


def _update_last_sync(
  context: AssetExecutionContext, config: MercurySyncConfig, summary: dict
) -> None:
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    with SessionFactory() as session:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.update_last_sync(session, summary)
        context.log.info("Updated connection last_sync timestamp + result")
      else:
        context.log.warning(
          f"Connection {config.connection_id} not found for last_sync update"
        )
  except Exception as exc:
    context.log.warning(f"Failed to update last_sync (non-fatal): {exc}")


def _bootstrap_fiscal_calendar_if_needed(
  context: AssetExecutionContext,
  config: MercurySyncConfig,
  earliest_occurred_at: str | None,
) -> None:
  """Initialize the fiscal calendar on the first sync of a fresh company.

  A Mercury-only tenant has no history closed elsewhere, so the books open
  at the first posted month: ``closed_through`` is the period before it.
  Idempotent — a calendar that already exists (a severed QuickBooks tenant,
  or a re-sync) is left alone.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.roboledger.fiscal_calendar import (
    FiscalCalendarService,
    add_months,
    current_month_period,
    previous_period,
  )
  from robosystems.operations.roboledger.fiscal_calendar.service import (
    CalendarAlreadyInitializedError,
  )

  try:
    service = FiscalCalendarService()
    with extensions_session(config.graph_id, statement_timeout_ms=None) as session:
      existing = service.get(session, config.graph_id)
      if existing is not None and existing.initialized_at is not None:
        context.log.info("Fiscal calendar already initialized; skipping bootstrap")
        return
      current = current_month_period()
      first_period = _first_period(earliest_occurred_at, current)
      if first_period is not None:
        closed_through = previous_period(first_period)
      else:
        closed_through = previous_period(previous_period(current))
      service.initialize(
        session,
        config.graph_id,
        closed_through=closed_through,
        actor_id=config.user_id,
        actor_type="system",
        note="Auto-initialized on first Mercury sync",
      )
      start = min(closed_through, add_months(current, -23))
      inserted = service.ensure_fiscal_periods(
        session,
        config.graph_id,
        start_period=start,
        end_period=current,
        closed_through=closed_through,
      )
      session.commit()
      context.log.info(
        f"Initialized fiscal calendar on first sync: closed_through={closed_through} "
        f"({inserted} FiscalPeriod rows seeded {start} → {current})"
      )
  except CalendarAlreadyInitializedError:
    context.log.info("Fiscal calendar already initialized (race); skipping bootstrap")
  except Exception as exc:
    context.log.warning(f"Failed to bootstrap fiscal calendar (non-fatal): {exc}")


# The earliest month the books can open at. A feed can carry a placeholder
# date (the Mercury sandbox posts transactions dated year 1); a calendar
# opened there would seed thousands of periods, or fail on year 0.
EARLIEST_BOOKS_PERIOD = "1990-01"


def _first_period(earliest_occurred_at: str | None, current: str) -> str | None:
  """The first posted month, or ``None`` when it is missing, in the future,
  or implausibly early."""
  if not earliest_occurred_at or len(earliest_occurred_at) < 7:
    return None
  period = earliest_occurred_at[:7]
  if not (EARLIEST_BOOKS_PERIOD <= period < current):
    return None
  return period


def _mark_graph_stale(
  context: AssetExecutionContext, config: MercurySyncConfig
) -> None:
  try:
    from robosystems.operations.extensions.staleness import mark_graph_stale

    mark_graph_stale(config.graph_id, "connector_sync")
    context.log.info("Marked graph stale after Mercury sync")
  except Exception as exc:
    context.log.warning(f"Failed to mark graph stale (non-fatal): {exc}")
