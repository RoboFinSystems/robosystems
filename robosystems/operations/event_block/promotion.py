"""Pending-obligation promotion — reusable core sweep.

Materialized ``pending`` ``schedule_entry_due`` events sit dormant until
their period boundary passes. At that point a sweep flips them to
``classified`` (committing to the obligation), then optionally dispatches
the ``schedule_entry_due`` Python handler to draft the closing entry on the
GL.

Two surfaces call this:

- The Dagster sensor + per-graph job (production path; runs every few
  minutes and processes whatever has matured).
- An admin CLI / REPL helper that promotes a single graph on demand
  (operator-driven during incidents or backfills).

Both paths share the same pure function: ``promote_pending_obligations``.
The session is provided by the caller; this module never opens or commits
its own transaction.

Autopilot vs co-pilot
---------------------

The ``dispatch_handlers`` flag distinguishes the two operating modes:

- ``False`` (co-pilot, default): flip status only. The operator (or
  another job) is responsible for actually drafting the entry. Useful
  when handler dispatch should be observable before it's automatic.
- ``True`` (autopilot): also call the registered Python handler so the
  draft entry lands in the GL immediately on the same tick.

Mode is selected per graph via ``Graph.auto_dispatch_obligations``, with the
``EXTENSIONS_PROMOTION_AUTO_DISPATCH`` env var as the deployment-wide default
when the column is NULL.

Idempotence
-----------

The flip is ``UPDATE … WHERE status='pending' AND occurred_at <= :as_of``
so re-running the function is safe — already-classified rows are
skipped. Handler dispatch is idempotent at the schedule-entry level
(``ScheduleService.create_closing_entry`` reconciles to the existing
draft when one already exists).

Stranded obligations
--------------------

A co-pilot sweep flips pending → classified *without* dispatching, so an
obligation can sit at ``classified`` with no closing entry ever drafted —
invisible to a later autopilot sweep that only dispatches what it flips,
and invisible to the close gate's pending count. Those are adjusting
entries a close would silently omit. The sweep therefore also scans
matured ``classified`` obligations whose (schedule, period) has no
``entries`` row — "stranded" — and dispatches them in autopilot mode
(co-pilot surfaces them on the result for the operator). The has-a-draft
test is keyed the same way ``create_closing_entry``'s reconcile is
(``source_structure_id`` + ``posting_date`` inside the period, any
status), so an obligation whose entry was drafted through a *different*
event — or already posted — is never re-dispatched.
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
  # Matured obligations found already at `classified` with no closing entry
  # drafted for their (schedule, period) — after orphan voiding. In autopilot
  # mode these were dispatched this sweep (also present in
  # dispatched_event_ids); in co-pilot mode they're surfaced so the operator
  # can re-run with dispatch_handlers=True or void them.
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
  """Extract (schedule_id, period_start, period_end) from obligation metadata.

  Returns None when any of the three is missing or unparseable — such an
  event can't be checked for a draft (and handler dispatch would reject
  its metadata anyway).
  """
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

  "Has a draft" mirrors ``ScheduleService.create_closing_entry``'s
  reconcile lookup: any ``entries`` row — draft or posted, regardless of
  which event triggered it — with ``source_structure_id`` equal to the
  obligation's schedule and ``posting_date`` inside its period window.
  Matching on the (schedule, period) key rather than
  ``triggered_by_event_id`` keeps obligations whose entry was drafted
  through a different event (or already posted) out of the stranded set.
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
  for entry in (
    session.query(Entry).filter(Entry.source_structure_id.in_(schedule_ids)).all()
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
  """Matured `classified` obligations whose closing entry was never drafted.

  The population a co-pilot sweep creates: flipped past `pending` without
  dispatch, so invisible to the close gate's pending count — adjusting
  entries a close would otherwise silently omit. The close gate counts
  these; the autopilot sweep dispatches them.
  """
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
  """Unlocked read of what this sweep will write: the ``pending`` candidates
  and the stranded ``classified`` ones.

  Dispatched obligations rest at ``classified`` for good (see
  ``schedule_entry_due``), so the classified candidate set is the schedule's
  whole history. Only the stranded subset is written; the rest is neither
  fenced nor locked — locking it would hold every historical obligation for
  the length of the sweep and fail close's publish against them, and fencing
  it would fence every period that ever held a schedule.
  """
  preview = session.query(Event).filter(*candidate_filter).all()
  pending = [evt for evt in preview if evt.status == "pending"]
  classified = [evt for evt in preview if evt.status == "classified"]
  stranded = filter_stranded_obligations(session, classified) if classified else []
  return pending, stranded


def _fence_write_set(session: Session, write_set: list[Event]) -> list[tuple[str, str]]:
  """Take the shared period fence for every obligation autopilot will write.

  Returns ``(event_id, reason)`` for obligations whose period is closed, so
  the caller can leave them out and report them. A fence that cannot be
  taken because a closer holds the exclusive side propagates as
  ``RowLockedError`` — that one is retryable and does apply to the sweep.
  """
  by_date: dict[date, list[Event]] = {}
  for evt in write_set:
    posting_date = posting_date_for_event(
      effective_at=evt.effective_at,
      occurred_at=evt.occurred_at,
    )
    by_date.setdefault(posting_date, []).append(evt)

  closed: list[tuple[str, str]] = []
  # Sorted so two sweeps fence periods in the same order. Shared fences do
  # not contend with each other, so this is tidiness rather than a
  # deadlock guard — the row locks that follow are the ones ordered for that.
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

  Also scans matured `classified` obligations with no drafted closing
  entry ("stranded" — see module docstring): autopilot dispatches them,
  co-pilot surfaces them on ``result.stranded_event_ids``.

  ``session`` must be tenant-scoped (search_path set to the target graph's
  schema) and the caller owns commit/rollback; ``graph_id`` is for logging
  only, since the data scope comes from the search_path. Events with
  ``occurred_at <= as_of`` are eligible — pass ``datetime.now(UTC)`` for a
  wall-clock sweep.

  Per-event handler errors are collected rather than raised, so a single bad
  row can't poison the sweep: those events stay at ``classified`` (the flip
  already happened) and surface in ``result.errors``.
  """
  # Locked: everything below is read-decide-write against these rows — the
  # co-pilot flip, the orphan void, and the autopilot dispatch all act on the
  # status this read observed. The sensor runs every few minutes on every
  # graph, so it races an on-demand `promote-obligations`, an inbox approval,
  # and (if a tick overruns) its own successor. See `locking` for the
  # bounded-vs-unbounded split: this function is called from both a Dagster
  # sweep and a request handler, and it is the *caller* that bounds the wait.
  candidate_filter = [
    Event.event_type == "schedule_entry_due",
    Event.status.in_(("pending", "classified")),
    Event.occurred_at <= as_of,
  ]
  result = PromotionResult(graph_id=graph_id)
  # Unlocked preview of the write set, then lock exactly that — not the whole
  # candidate set. See `_preview_write_set` for why the classified history
  # stays out of both the fence and the lock.
  preview_pending, preview_stranded = _preview_write_set(session, candidate_filter)
  write_set = preview_pending + preview_stranded
  # Autopilot writes GL, so the period fence has to come *before* the
  # event row locks. Close takes exclusive fence then event locks;
  # locking first and fencing in the handler was the inversion that
  # failed close mid-publish. An obligation whose period is already
  # closed is left out of the sweep and reported, not fatal to it.
  if dispatch_handlers and write_set:
    closed = _fence_write_set(session, write_set)
    if closed:
      result.errors.extend(closed)
      closed_ids = {evt_id for evt_id, _ in closed}
      write_set = [evt for evt in write_set if evt.id not in closed_ids]
  if not write_set:
    return result

  # The preview above put these rows in the identity map, so the locked
  # read must `populate_existing` — otherwise it takes the lock and hands
  # back the status the preview saw, and a void or approval that committed
  # in between is acted on as if it never happened. Flush first: this
  # factory is `autoflush=False`, and a refresh would otherwise discard an
  # in-flight change without a word (nothing is pending here today; this
  # keeps it a property of the read rather than of its callers).
  session.flush()
  candidates = (
    session.query(Event)
    .filter(
      Event.id.in_([evt.id for evt in write_set]),
      # Re-checked under the lock: a row that left the candidate set while
      # we waited is not returned at all — the same guard the bulk updates
      # below carry, and independent of the refresh.
      Event.status.in_(("pending", "classified")),
    )
    # Ordered so this and `supersede_pending_obligations` / the schedule
    # void — whose row sets overlap on pending obligations — can never
    # acquire in opposing orders. See `locking.ordered_lock_column`.
    .order_by(ordered_lock_column())
    .populate_existing()
    .with_for_update()
    .all()
  )
  # Re-derived from the locked rows: status is decided here, not by the
  # preview. A row that moved between the two reads (voided, approved,
  # drafted) drops out on its own.
  pending = [evt for evt in candidates if evt.status == "pending"]
  classified = [evt for evt in candidates if evt.status == "classified"]

  stranded = filter_stranded_obligations(session, classified) if classified else []

  # Orphan guard: an obligation whose schedule structure no longer exists is
  # orphaned — the schedule was deleted but its register wasn't voided. Never
  # draft an orphan into a closing entry; void it in place so it stops
  # blocking close, and surface it. This is the catch-all that stops a deleted
  # schedule from double-posting at promotion regardless of how the orphan
  # arose, and it covers stranded classified obligations too — a deleted
  # schedule's classified leftovers void rather than error at dispatch.
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
    # Co-pilot mode: single bulk UPDATE instead of per-row ORM mutation.
    # No need to keep the ORM objects in the dirty set — we don't dispatch
    # a handler that would read event.status, so the round-trip savings are
    # significant on long schedules (a 30-year mortgage is 360 rows).
    # Stranded obligations are already classified — a status flip can't help
    # them; they ride out on result.stranded_event_ids instead.
    if pending:
      candidate_ids = [evt.id for evt in pending]
      # The status predicate is not redundant with the lock above — it is the
      # invariant stated where it is relied on. Without it this UPDATE would
      # write `classified` over whatever the row now holds, so a concurrently
      # voided or committed obligation would be silently reverted to an earlier
      # state. That is a lost update, not just a redundant one, and it is the
      # same guard the orphan void a few lines up already carries.
      session.query(Event).filter(
        Event.id.in_(candidate_ids), Event.status == "pending"
      ).update({"status": "classified"}, synchronize_session="fetch")
      # Accurate because the candidate read is locked: the UPDATE's guard
      # cannot filter any of these out, so every id did in fact flip. If that
      # read ever loses its lock, this list has to come from the statement's
      # rowcount instead of being assumed.
      result.classified_event_ids.extend(candidate_ids)
    logger.info(
      "promote_pending_obligations[%s]: classified=%s stranded=%s (co-pilot mode)",
      graph_id,
      result.classified_count,
      result.stranded_count,
    )
    return result

  # Autopilot mode: mutate ORM rows so subsequent handler.dispatch calls
  # see status='classified' without an extra round trip.
  for event in pending:
    event.status = "classified"
    result.classified_event_ids.append(event.id)

  handler = get_python_handler("schedule_entry_due")
  if handler is None:  # pragma: no cover — registered at module import
    raise RuntimeError("schedule_entry_due handler is missing from the registry")

  # Dispatch newly flipped obligations AND stranded ones — the reconcile
  # inside the handler is idempotent per (schedule, period).
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
      # Each dispatch runs under its own savepoint: a database-level failure
      # inside one handler (a constraint violation, say) would otherwise abort
      # the whole transaction, every later dispatch would fail with "current
      # transaction is aborted" and be collected as if it were its own error,
      # and the caller's commit would fail — one poison obligation blocking
      # every other one on the graph, every tick.
      with session.begin_nested():
        handler.dispatch(session, event, typed_metadata, created_by)
      result.dispatched_event_ids.append(event.id)
    except HandlerMetadataValidationError as e:
      result.errors.append((event.id, f"handler validation failed: {e}"))
    except (RowLockedError, OperationalError):
      # A lock wait or a connection fault is not "one bad event" — it is the
      # sweep's own condition, and the caller's bounded wait / retry policy
      # must see it rather than a per-event error line.
      raise
    except Exception as e:
      # Catch-all so one bad event doesn't sink the sweep. The status
      # change above stays in the session — caller decides whether to
      # commit (preserving classified) or rollback (full retry next tick).
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
