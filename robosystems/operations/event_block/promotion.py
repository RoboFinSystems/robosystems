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
its own transaction. ``extensions_session`` (production) and the test
harness's stub session compose identically.

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
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.python_handlers import get_python_handler
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)


@dataclass
class PromotionResult:
  """Counts + diagnostics for a single promotion sweep."""

  graph_id: str
  classified_event_ids: list[str] = field(default_factory=list)
  dispatched_event_ids: list[str] = field(default_factory=list)
  errors: list[tuple[str, str]] = field(default_factory=list)

  @property
  def classified_count(self) -> int:
    return len(self.classified_event_ids)

  @property
  def dispatched_count(self) -> int:
    return len(self.dispatched_event_ids)

  @property
  def error_count(self) -> int:
    return len(self.errors)


def promote_pending_obligations(
  session: Session,
  graph_id: str,
  *,
  as_of: datetime,
  dispatch_handlers: bool = False,
  created_by: str = "system:obligation_promoter",
) -> PromotionResult:
  """Flip matured `pending` `schedule_entry_due` events to `classified`.

  Args:
    session: Tenant-scoped extensions session (search_path already set
      to the target graph's schema). The caller owns commit/rollback.
    graph_id: The graph this session targets, used for logging only.
      The actual data scope comes from the session's search_path.
    as_of: Cutoff timestamp. Events with ``occurred_at <= as_of`` are
      eligible. Pass ``datetime.now(UTC)`` for a wall-clock sweep.
    dispatch_handlers: When True, fires the Python handler for each
      promoted event so the draft entry materializes in the same
      transaction. When False (the default), flips status only.
    created_by: Actor recorded on any rows the handler creates.

  Returns:
    A ``PromotionResult`` with counts. Per-event handler errors are
    collected (not raised) so a single bad row can't poison the sweep
    — those events stay at ``classified`` (the flip already happened)
    and surface in ``result.errors`` for the operator to investigate.
  """
  candidates = (
    session.query(Event)
    .filter(
      Event.event_type == "schedule_entry_due",
      Event.status == "pending",
      Event.occurred_at <= as_of,
    )
    .all()
  )

  result = PromotionResult(graph_id=graph_id)

  if not candidates:
    return result

  if not dispatch_handlers:
    # Co-pilot mode: single bulk UPDATE instead of per-row ORM mutation.
    # No need to keep the ORM objects in the dirty set — we don't dispatch
    # a handler that would read event.status, so the round-trip savings are
    # significant on long schedules (a 30-year mortgage is 360 rows).
    candidate_ids = [evt.id for evt in candidates]
    session.query(Event).filter(Event.id.in_(candidate_ids)).update(
      {"status": "classified"}, synchronize_session="fetch"
    )
    result.classified_event_ids.extend(candidate_ids)
    logger.info(
      "promote_pending_obligations[%s]: classified %s events (co-pilot mode)",
      graph_id,
      result.classified_count,
    )
    return result

  # Autopilot mode: mutate ORM rows so subsequent handler.dispatch calls
  # see status='classified' without an extra round trip.
  for event in candidates:
    event.status = "classified"
    result.classified_event_ids.append(event.id)

  handler = get_python_handler("schedule_entry_due")
  if handler is None:  # pragma: no cover — registered at module import
    raise RuntimeError("schedule_entry_due handler is missing from the registry")

  for event in candidates:
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
      handler.dispatch(session, event, typed_metadata, created_by)
      result.dispatched_event_ids.append(event.id)
    except HandlerMetadataValidationError as e:
      result.errors.append((event.id, f"handler validation failed: {e}"))
    except Exception as e:
      # Catch-all so one bad event doesn't sink the sweep. The status
      # change above stays in the session — caller decides whether to
      # commit (preserving classified) or rollback (full retry next tick).
      result.errors.append((event.id, f"dispatch raised {type(e).__name__}: {e}"))

  logger.info(
    "promote_pending_obligations[%s]: classified=%s dispatched=%s errors=%s "
    "(autopilot mode)",
    graph_id,
    result.classified_count,
    result.dispatched_count,
    result.error_count,
  )
  return result
