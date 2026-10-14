"""Take the suggestion on many open bank lines at once.

One line at a time the inbox goes through `update-event-block`; a month of
feed lines is hundreds of calls. This walks the lines the filters select,
oldest first, and takes each one's suggestion through the same update, so
every check that guards a single line (the handler's veto, the period fence,
the row lock) guards the batch. A line it cannot take comes back `skipped`
with the reason; nothing is forced.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.models.api.extensions.agent import (
  AcceptSuggestionLine,
  AcceptSuggestionsRequest,
  AcceptSuggestionsResponse,
)
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.commands import (
  EventNotFoundError,
  InvalidEventTransitionError,
  update_event_block,
)
from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
  ElementResolutionError,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.classification import (
  LEARNED_EVENT_TYPES,
  SUGGESTED_ACCOUNT_NAME,
  SUGGESTED_ELEMENT_ID,
  SUGGESTION_SOURCE,
)
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.entity_scope import find_entity_id

OPEN_STATUS = "captured"

# The audit mark the batch leaves on every line it classifies.
CLASSIFIED_VIA = "classified_via"
BATCH_NAME = "accept-suggestions"


def _selection(body: AcceptSuggestionsRequest, entity_id: str | None):
  """The lines the request names: explicit ids as given (any status, so each
  can be answered), otherwise the entity's open lines that carry a
  suggestion, narrowed by the filters."""
  stmt = select(Event).where(Event.event_type.in_(LEARNED_EVENT_TYPES))
  if entity_id is not None:
    stmt = stmt.where(Event.entity_id == entity_id)
  if body.event_ids:
    stmt = stmt.where(Event.id.in_(body.event_ids))
  else:
    stmt = stmt.where(
      Event.status == OPEN_STATUS,
      Event.metadata_[SUGGESTED_ELEMENT_ID].astext.isnot(None),
    )
  if body.source is not None:
    stmt = stmt.where(Event.source == body.source)
  if body.agent_id is not None:
    stmt = stmt.where(Event.agent_id == body.agent_id)
  if body.suggestion_source is not None:
    stmt = stmt.where(
      Event.metadata_[SUGGESTION_SOURCE].astext == body.suggestion_source
    )
  if body.occurred_from is not None:
    stmt = stmt.where(
      Event.occurred_at >= datetime.combine(body.occurred_from, time.min)
    )
  if body.occurred_to is not None:
    stmt = stmt.where(
      Event.occurred_at
      < datetime.combine(body.occurred_to + timedelta(days=1), time.min)
    )
  return stmt


def _skip_before_writing(
  status: str, metadata: dict, drifted: bool
) -> tuple[str, str] | None:
  """The refusals that need no write to find; the rest come from the update."""
  if status != OPEN_STATUS:
    return "not_open", f"the line is {status}, not {OPEN_STATUS}"
  if not metadata.get(SUGGESTED_ELEMENT_ID):
    name = metadata.get(SUGGESTED_ACCOUNT_NAME)
    if name:
      return (
        "no_suggestion",
        f"the suggestion '{name}' matches no account on this chart; choose one",
      )
    return "no_suggestion", "the line carries no suggestion; choose an account"
  if drifted:
    return "source_changed", "the feed changed this line after it was captured"
  return None


def accept_suggestions(
  session: Session,
  body: AcceptSuggestionsRequest,
  created_by: str,
  *,
  graph_id: str,
) -> AcceptSuggestionsResponse:
  """Classify every selected open line to its suggestion, and post it too
  when ``commit`` is set, each through the single-line update so the same
  guards apply. Lines are taken oldest first, at most ``limit`` per call."""
  entity_id = find_entity_id(session, body.entity_id)
  stmt = _selection(body, entity_id)
  selected = int(
    session.execute(select(func.count()).select_from(stmt.subquery())).scalar() or 0
  )
  rows = (
    session.execute(
      stmt.order_by(Event.occurred_at.asc(), Event.id.asc()).limit(body.limit)
    )
    .scalars()
    .all()
  )
  # Plain values: the update commits per line, and a later rollback would
  # expire the ORM rows mid-walk.
  lines = [
    (
      str(event.id),
      str(event.status),
      dict(event.metadata_ or {}),
      bool(event.payload_drift),
      event.description,
    )
    for event in rows
  ]
  transition = "committed" if body.commit else "classified"

  results: list[AcceptSuggestionLine] = []
  for event_id, status, metadata, drifted, description in lines:
    element_id = metadata.get(SUGGESTED_ELEMENT_ID)
    skip = _skip_before_writing(status, metadata, drifted)
    if skip is not None:
      reason, detail = skip
      results.append(
        AcceptSuggestionLine(
          event_id=event_id,
          outcome="skipped",
          reason=reason,
          detail=detail,
          element_id=str(element_id) if element_id else None,
          description=description,
        )
      )
      continue
    if body.dry_run:
      results.append(
        AcceptSuggestionLine(
          event_id=event_id,
          outcome=transition,
          reason=None,
          detail=None,
          element_id=str(element_id),
          description=description,
        )
      )
      continue
    reason: str | None = None
    detail: str | None = None
    try:
      update_event_block(
        session,
        UpdateEventBlockRequest(
          event_id=event_id,
          transition_to=transition,
          metadata_patch={"accept_suggestion": True, CLASSIFIED_VIA: BATCH_NAME},
        ),
        created_by,
        graph_id=graph_id,
      )
    except ClosedPeriodError as exc:
      session.rollback()
      reason, detail = "closed_period", str(exc)
    except RowLockedError as exc:
      session.rollback()
      reason, detail = "locked", str(exc)
    except (
      InvalidEventTransitionError,
      HandlerMetadataValidationError,
      ElementResolutionError,
      EventNotFoundError,
      ValueError,
    ) as exc:
      session.rollback()
      reason, detail = "refused", str(exc)
    results.append(
      AcceptSuggestionLine(
        event_id=event_id,
        outcome="skipped" if reason else transition,
        reason=reason,
        detail=detail,
        element_id=str(element_id),
        description=description,
      )
    )

  if body.dry_run:
    session.rollback()
  return AcceptSuggestionsResponse(
    selected=selected,
    classified=sum(1 for r in results if r.outcome == "classified"),
    committed=sum(1 for r in results if r.outcome == "committed"),
    skipped=sum(1 for r in results if r.outcome == "skipped"),
    truncated=selected > len(lines),
    dry_run=body.dry_run,
    lines=results,
  )
