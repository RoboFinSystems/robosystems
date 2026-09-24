"""Pending-obligation promotion sweep.

Matured ``pending`` ``schedule_entry_due`` events are flipped to
``classified``; in autopilot mode (``dispatch_handlers=True``, per graph via
``Graph.auto_dispatch_obligations``) the handler also drafts the closing
entry. Called by the Dagster sensor and on demand; the caller owns the
transaction. Re-running is safe.

A co-pilot sweep can leave obligations ``classified`` with no closing entry
("stranded"), invisible to the close gate's pending count. The sweep also
finds those: autopilot dispatches them, co-pilot reports them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger import Structure
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.engine import posting_date_for_event
from robosystems.operations.event_block.python_handlers import get_python_handler
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.locking import RowLockedError, ordered_lock_column
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)


@dataclass
class PromotionResult:
  """Counts + diagnostics for a single promotion sweep."""

  graph_id: str
  classified_event_ids: list[str] = field(default_factory=list)
  dispatched_event_ids: list[str] = field(default_factory=list)
  voided_orphan_event_ids: list[str] = field(default_factory=list)
  # Classified obligations with no closing entry for their (schedule, period).
  # Autopilot also lists them in dispatched_event_ids.
  stranded_event_ids: list[str] = field(default_factory=list)
  errors: list[tuple[str, str]] = field(default_factory=list)

  @property
  def classified_count(self) -> int:
    return len(self.classified_event_ids)

  @property
  def stranded_count(self) -> int:
    return len(self.stranded_event_ids)

  @property
  def dispatched_count(self) -> int:
    return len(self.dispatched_event_ids)

  @property
  def voided_orphan_count(self) -> int:
    return len(self.voided_orphan_event_ids)

  @property
  def error_count(self) -> int:
    return len(self.errors)


def _obligation_window(event: Event) -> tuple[str, date, date] | None:
  """(schedule_id, period_start, period_end) from metadata, or None if any is
  missing or unparseable."""
  meta = event.metadata_ or {}
  schedule_id = meta.get("schedule_id")
  if not schedule_id:
    return None
  try:
    period_start = date.fromisoformat(meta.get("period_start") or "")
    period_end = date.fromisoformat(meta.get("period_end") or "")
  except ValueError:
    return None
  return str(schedule_id), period_start, period_end


def filter_stranded_obligations(session: Session, events: list[Event]) -> list[Event]:
  """Return the subset of obligation `events` with no drafted closing entry.

  Mirrors ``ScheduleService.create_closing_entry``'s reconcile: any entry,
  of any status and from any event, on the schedule within the period.
  """
  windows: dict[str, tuple[str, date, date]] = {}
  for evt in events:
    window = _obligation_window(evt)
    if window is not None:
      windows[evt.id] = window
  if not windows:
    return []

  schedule_ids = {sid for sid, _, _ in windows.values()}
  entry_dates: dict[str, list[date]] = {}
  # Exclude generated reversals: they post on the first day of the next
  # period and would falsely mark it as drafted. Keyed on the reversal link,
  # not `entry_type` (caller-authored), as in `entry_status.PRIMARY_ENTRY_SQL`.
  for entry in (
    session.query(Entry)
    .filter(
      Entry.source_structure_id.in_(schedule_ids),
      Entry.reversal_of.is_(None),
    )
    .all()
  ):
    entry_dates.setdefault(str(entry.source_structure_id), []).append(
      entry.posting_date
    )

  stranded: list[Event] = []
  for evt in events:
    window = windows.get(evt.id)
    if window is None:
      continue
    schedule_id, period_start, period_end = window
    has_entry = any(
      period_start <= posting_date <= period_end
      for posting_date in entry_dates.get(schedule_id, ())
    )
    if not has_entry:
      stranded.append(evt)
  return stranded


def find_stranded_obligations(session: Session, *, as_of: datetime) -> list[Event]:
  """Matured `classified` obligations whose closing entry was never drafted."""
  classified = (
    session.query(Event)
    .filter(
      Event.event_type == "schedule_entry_due",
      Event.status == "classified",
      Event.occurred_at <= as_of,
    )
    .order_by(Event.occurred_at.asc())
    .all()
  )
  return filter_stranded_obligations(session, classified)


def _preview_write_set(
  session: Session, candidate_filter: list
) -> tuple[list[Event], list[Event]]:
  """Unlocked read of what this sweep will write: ``pending`` candidates and
  stranded ``classified`` ones.

  Dispatched obligations stay ``classified`` forever, so only the stranded
  subset is fenced and locked, not the schedule's whole history.
  """
  preview = session.query(Event).filter(*candidate_filter).all()
  pending = [evt for evt in preview if evt.status == "pending"]
  classified = [evt for evt in preview if evt.status == "classified"]
  stranded = filter_stranded_obligations(session, classified) if classified else []
  return pending, stranded


def _fence_write_set(session: Session, write_set: list[Event]) -> list[tuple[str, str]]:
  """Take the shared period fence for every obligation autopilot will write.

  Returns ``(event_id, reason)`` for obligations in closed periods. A fence
  held exclusively by a closer propagates as retryable ``RowLockedError``.
  """
  by_date: dict[date, list[Event]] = {}
  for evt in write_set:
    posting_date = posting_date_for_event(
      effective_at=evt.effective_at,
      occurred_at=evt.occurred_at,
    )
    by_date.setdefault(posting_date, []).append(evt)

  closed: list[tuple[str, str]] = []
  for posting_date in sorted(by_date):
    try:
      assert_period_not_closed(session, posting_date)
    except ClosedPeriodError as e:
      closed.extend((evt.id, f"closed period: {e}") for evt in by_date[posting_date])
  return closed


def promote_pending_obligations(
  session: Session,
  graph_id: str,
  *,
  as_of: datetime,
  dispatch_handlers: bool = False,
  created_by: str = "system:obligation_promoter",
) -> PromotionResult:
  """Flip matured `pending` `schedule_entry_due` events to `classified`.

  ``session`` must be tenant-scoped and the caller owns commit/rollback;
  ``graph_id`` is for logging only. Per-event handler errors are collected
  in ``result.errors`` rather than raised. The caller bounds lock waits.
  """
  candidate_filter = [
    Event.event_type == "schedule_entry_due",
    Event.status.in_(("pending", "classified")),
    Event.occurred_at <= as_of,
  ]
  result = PromotionResult(graph_id=graph_id)
  # Unlocked preview of the write set, then lock exactly that.
  preview_pending, preview_stranded = _preview_write_set(session, candidate_filter)
  write_set = preview_pending + preview_stranded
  # Autopilot writes GL: fence before the row locks, matching close's order.
  # Obligations in closed periods are skipped and reported.
  if dispatch_handlers and write_set:
    closed = _fence_write_set(session, write_set)
    if closed:
      result.errors.extend(closed)
      closed_ids = {evt_id for evt_id, _ in closed}
      write_set = [evt for evt in write_set if evt.id not in closed_ids]
  if not write_set:
    return result

  # The preview loaded these rows, so the locked read must `populate_existing`
  # or it returns stale statuses; flush first (autoflush is off).
  session.flush()
  candidates = (
    session.query(Event)
    .filter(
      Event.id.in_([evt.id for evt in write_set]),
      Event.status.in_(("pending", "classified")),
    )
    # Same order as `supersede_pending_obligations` / the schedule void.
    .order_by(ordered_lock_column())
    .populate_existing()
    .with_for_update()
    .all()
  )
  # Status decided from the locked rows, not the preview.
  pending = [evt for evt in candidates if evt.status == "pending"]
  classified = [evt for evt in candidates if evt.status == "classified"]

  stranded = filter_stranded_obligations(session, classified) if classified else []

  # Orphans (schedule structure deleted) are voided in place, never drafted,
  # so they stop blocking close.
  guard_pool = pending + stranded
  if guard_pool:
    candidate_schedule_ids = {
      evt.metadata_.get("schedule_id")
      for evt in guard_pool
      if evt.metadata_ and evt.metadata_.get("schedule_id")
    }
    live_schedule_ids: set[str] = set()
    if candidate_schedule_ids:
      live_schedule_ids = {
        sid
        for (sid,) in session.query(Structure.id)
        .filter(Structure.id.in_(candidate_schedule_ids))
        .all()
      }
    orphans = [
      evt
      for evt in guard_pool
      if evt.metadata_
      and evt.metadata_.get("schedule_id")
      and evt.metadata_.get("schedule_id") not in live_schedule_ids
    ]
    if orphans:
      orphan_ids = [evt.id for evt in orphans]
      session.query(Event).filter(
        Event.id.in_(orphan_ids), Event.status.in_(("pending", "classified"))
      ).update({"status": "voided"}, synchronize_session="fetch")
      result.voided_orphan_event_ids.extend(orphan_ids)
      for evt in orphans:
        logger.warning(
          "promote_pending_obligations[%s]: voided orphan obligation %s — "
          "schedule %s no longer exists (not drafted)",
          graph_id,
          evt.id,
          evt.metadata_.get("schedule_id"),
        )
      orphan_id_set = set(orphan_ids)
      pending = [evt for evt in pending if evt.id not in orphan_id_set]
      stranded = [evt for evt in stranded if evt.id not in orphan_id_set]

  result.stranded_event_ids.extend(evt.id for evt in stranded)

  if not pending and not stranded:
    return result

  if not dispatch_handlers:
    # Co-pilot: one bulk UPDATE. Stranded ones are only reported.
    if pending:
      candidate_ids = [evt.id for evt in pending]
      # The status predicate prevents reverting a concurrently moved row.
      session.query(Event).filter(
        Event.id.in_(candidate_ids), Event.status == "pending"
      ).update({"status": "classified"}, synchronize_session="fetch")
      # Accurate only because the candidate read is locked.
      result.classified_event_ids.extend(candidate_ids)
    logger.info(
      "promote_pending_obligations[%s]: classified=%s stranded=%s (co-pilot mode)",
      graph_id,
      result.classified_count,
      result.stranded_count,
    )
    return result

  # Autopilot: mutate ORM rows so handler dispatch sees the new status.
  for event in pending:
    event.status = "classified"
    result.classified_event_ids.append(event.id)

  handler = get_python_handler("schedule_entry_due")
  if handler is None:  # pragma: no cover — registered at module import
    raise RuntimeError("schedule_entry_due handler is missing from the registry")

  # The handler's reconcile is idempotent per (schedule, period).
  for event in pending + stranded:
    try:
      typed_metadata = handler.metadata_schema.model_validate(event.metadata_ or {})
    except ValidationError as e:
      result.errors.append((event.id, f"metadata validation failed: {e}"))
      continue
    except Exception as e:  # pragma: no cover — defensive
      result.errors.append(
        (event.id, f"unexpected validation error: {type(e).__name__}: {e}")
      )
      continue

    try:
      # Savepoint per dispatch so one DB-level failure doesn't abort the
      # whole transaction.
      with session.begin_nested():
        handler.dispatch(session, event, typed_metadata, created_by)
      result.dispatched_event_ids.append(event.id)
    except HandlerMetadataValidationError as e:
      result.errors.append((event.id, f"handler validation failed: {e}"))
    except (RowLockedError, OperationalError):
      # The sweep's own condition, not one bad event: let the caller retry.
      raise
    except Exception as e:
      # One bad event must not sink the sweep; the status flip stays in the
      # session for the caller to commit or roll back.
      result.errors.append((event.id, f"dispatch raised {type(e).__name__}: {e}"))

  logger.info(
    "promote_pending_obligations[%s]: classified=%s stranded=%s dispatched=%s "
    "errors=%s (autopilot mode)",
    graph_id,
    result.classified_count,
    result.stranded_count,
    result.dispatched_count,
    result.error_count,
  )
  return result
