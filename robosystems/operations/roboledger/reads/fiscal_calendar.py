"""Fiscal calendar reads and response assembly, shared by REST and GraphQL."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalCalendarResponse,
  FiscalPeriodSummary,
  PendingObligationDetailResponse,
)
from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.entity_scope import is_group_parent


def get_fiscal_year_start_month(session: Session) -> int:
  """The graph's fiscal year start month, defaulting to 1. Every entity's
  calendar carries the same one."""
  cal = session.query(FiscalCalendar).first()
  if cal and cal.fiscal_year_start_month:
    return int(cal.fiscal_year_start_month)
  return 1


def live_qb_connection(platform_db: Session, graph_id: str) -> Connection | None:
  """The graph's live QB connection. Disconnected and severed rows don't
  count; among the rest a connected one wins, then the most recently updated.
  """
  return (
    platform_db.query(Connection)
    .filter(
      Connection.graph_id == graph_id,
      Connection.provider == "quickbooks",
      Connection.deleted_at.is_(None),
      Connection.status.notin_(
        [ConnectionStatus.DISCONNECTED.value, ConnectionStatus.SEVERED.value]
      ),
    )
    .order_by(
      (Connection.status == "connected").desc(),
      Connection.updated_at.desc(),
    )
    .first()
  )


def qb_sync_state(platform_db: Session, graph_id: str) -> tuple[bool, datetime | None]:
  """`(has_connection, last_sync_at)` for the graph's live QB connection.

  `(True, None)` means connected but never synced, which the close gate
  treats as stale.
  """
  connection = live_qb_connection(platform_db, graph_id)
  if connection is None:
    return (False, None)
  return (True, connection.last_sync)


def entity_sync_state(
  session: Session, platform_db: Session, graph_id: str, entity_id: str | None
) -> tuple[bool, datetime | None]:
  """`qb_sync_state` for one entity. The graph's QuickBooks connection books
  for the group parent, so a subsidiary has no sync its close waits on."""
  if entity_id is None or not is_group_parent(session, entity_id):
    return (False, None)
  return qb_sync_state(platform_db, graph_id)


def build_fiscal_calendar_response(
  session: Session,
  graph_id: str,
  calendar: FiscalCalendar,
  has_sync_connection: bool,
  last_sync_at: datetime | None,
  service,
) -> FiscalCalendarResponse:
  """The calendar's own entity's periods and gate. `service` is passed in so
  a patched `FiscalCalendarService` flows through."""
  entity_id = str(calendar.entity_id)
  periods = (
    session.query(FiscalPeriod)
    .filter(
      FiscalPeriod.graph_id == graph_id,
      FiscalPeriod.entity_id == entity_id,
    )
    .order_by(FiscalPeriod.start_date)
    .all()
  )

  catch_up = service.catch_up_sequence(calendar, session=session, graph_id=graph_id)
  next_period_to_close = catch_up[0] if catch_up else None
  gate = None
  if next_period_to_close is not None:
    gate = service.closeable_gate(
      session,
      graph_id,
      next_period_to_close,
      has_sync_connection=has_sync_connection,
      last_sync_at=last_sync_at,
      entity_id=entity_id,
    )

  pending_obligation_sample = (
    [
      PendingObligationDetailResponse(
        event_id=d.event_id,
        schedule_id=d.schedule_id,
        schedule_name=d.schedule_name,
        period=d.period,
      )
      for d in gate.pending_obligation_sample
    ]
    if gate
    else []
  )
  stranded_obligation_sample = (
    [
      PendingObligationDetailResponse(
        event_id=d.event_id,
        schedule_id=d.schedule_id,
        schedule_name=d.schedule_name,
        period=d.period,
      )
      for d in gate.stranded_obligation_sample
    ]
    if gate
    else []
  )

  return FiscalCalendarResponse(
    graph_id=graph_id,
    entity_id=entity_id,
    fiscal_year_start_month=calendar.fiscal_year_start_month,
    closed_through=calendar.closed_through_period,
    close_target=calendar.close_target_period,
    gap_periods=len(catch_up),
    catch_up_sequence=catch_up,
    closeable_now=gate.is_closeable if gate else False,
    blockers=gate.blockers if gate else [],
    pending_obligation_count=gate.pending_obligation_count if gate else 0,
    pending_obligation_sample=pending_obligation_sample,
    earliest_pending_period=gate.earliest_pending_period if gate else None,
    sync_stale_days=gate.sync_stale_days if gate else None,
    stranded_obligation_count=gate.stranded_obligation_count if gate else 0,
    stranded_obligation_sample=stranded_obligation_sample,
    reconciling_item_count=gate.reconciling_item_count if gate else 0,
    reconciling_item_sample=list(gate.reconciling_item_sample) if gate else [],
    unposted_source_event_count=gate.unposted_source_event_count if gate else 0,
    unposted_source_event_sample=(
      list(gate.unposted_source_event_sample) if gate else []
    ),
    unreconciled_account_count=gate.unreconciled_account_count if gate else 0,
    unreconciled_account_sample=(
      list(gate.unreconciled_account_sample) if gate else []
    ),
    last_close_at=calendar.last_close_at,
    initialized_at=calendar.initialized_at,
    last_sync_at=last_sync_at,
    periods=[
      FiscalPeriodSummary(
        name=p.name,
        start_date=p.start_date,
        end_date=p.end_date,
        status=p.status,
        closed_at=p.closed_at,
        has_close_receipt=p.close_receipt is not None,
      )
      for p in periods
    ],
  )
