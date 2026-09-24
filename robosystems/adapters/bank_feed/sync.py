"""The sync-result discipline every bank-feed asset keeps.

Any failure is recorded on the connection without advancing ``last_sync``,
so a silently failing feed is visible; the sync lock is released either way.
Everything past the body is best-effort and never fails the run.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dagster import AssetExecutionContext, Config

from robosystems.adapters.bank_feed.window import (
  default_backfill_start as default_backfill_start,
)

# The earliest month the books can open at. A feed can carry a placeholder
# date (the Mercury sandbox posts transactions dated year 1); a calendar
# opened there would seed thousands of periods, or fail on year 0.
EARLIEST_BOOKS_PERIOD = "1990-01"


class BankFeedSyncConfig(Config):
  """The run configuration every bank-feed asset shares, bound by its
  provider's ``sync`` hook."""

  graph_id: str
  connection_id: str
  user_id: str
  full_rebuild: bool = False
  since_date: str = ""
  # Released at the end of the run; empty when Valkey was down at dispatch.
  sync_lock_id: str = ""


def last_sync(connection_id: str) -> datetime | None:
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  with SessionFactory() as session:
    conn = Connection.get_by_id(connection_id, session)
    return conn.last_sync if conn is not None else None


def release_sync_lock(
  context: AssetExecutionContext, config: BankFeedSyncConfig
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


def record_failed_sync_result(
  context: AssetExecutionContext, config: BankFeedSyncConfig, exc: Exception
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


def update_last_sync(
  context: AssetExecutionContext,
  config: BankFeedSyncConfig,
  summary: dict[str, Any],
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


def bootstrap_fiscal_calendar_if_needed(
  context: AssetExecutionContext,
  config: BankFeedSyncConfig,
  earliest_occurred_at: str | None,
  *,
  source_label: str,
) -> None:
  """Initialize the fiscal calendar on the first sync of a fresh company.

  A bank-feed-only tenant has no history closed elsewhere, so the books open
  at the first posted month: ``closed_through`` is the period before it.
  Idempotent — a calendar that already exists (a severed QuickBooks tenant,
  a second feed, or a re-sync) is left alone.
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
      opening = first_period(earliest_occurred_at, current)
      if opening is not None:
        closed_through = previous_period(opening)
      else:
        closed_through = previous_period(previous_period(current))
      service.initialize(
        session,
        config.graph_id,
        closed_through=closed_through,
        actor_id=config.user_id,
        actor_type="system",
        note=f"Auto-initialized on first {source_label} sync",
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


def first_period(earliest_occurred_at: str | None, current: str) -> str | None:
  """The first posted month, or ``None`` when it is missing, in the future,
  or implausibly early."""
  if not earliest_occurred_at or len(earliest_occurred_at) < 7:
    return None
  period = earliest_occurred_at[:7]
  if not (EARLIEST_BOOKS_PERIOD <= period < current):
    return None
  return period


def mark_graph_stale(
  context: AssetExecutionContext, config: BankFeedSyncConfig, *, source_label: str
) -> None:
  try:
    from robosystems.operations.extensions.staleness import mark_graph_stale as mark

    mark(config.graph_id, "connector_sync")
    context.log.info(f"Marked graph stale after {source_label} sync")
  except Exception as exc:
    context.log.warning(f"Failed to mark graph stale (non-fatal): {exc}")
