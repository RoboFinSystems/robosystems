"""How a month's bank lines were classified, read from the lines themselves.

Every commit stamps ``suggestion_outcome`` beside ``suggestion_source``
(`classification.learn_from_commit`), so the override rate is a count over
event metadata, with no table behind it. The share of lines that needed a
human choice is the number the product may quote, once it comes from a real
month.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.agent import (
  ClassificationSourceStats,
  ClassificationStatsResponse,
)
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.classification import (
  LEARNED_EVENT_TYPES,
  SUGGESTED_ELEMENT_ID,
  SUGGESTION_OUTCOME,
  SUGGESTION_SOURCE,
)
from robosystems.operations.roboledger.entity_scope import find_entity_id
from robosystems.operations.roboledger.fiscal_calendar.periods import (
  period_date_range,
)

OPEN_STATUSES = ("captured", "classified")
NO_SOURCE = "none"
# A suggestion recorded before the ladder named its source was the feed's.
FEED_SOURCE = "tier0"

_OUTCOMES = ("accepted", "overridden", "none", "split")


def _source_of(metadata: dict) -> str:
  source = metadata.get(SUGGESTION_SOURCE)
  if source:
    return str(source)
  return FEED_SOURCE if metadata.get(SUGGESTED_ELEMENT_ID) else NO_SOURCE


def classification_stats(
  session: Session, period: str, entity_id: str | None = None
) -> ClassificationStatsResponse:
  """Per suggestion source, how the month's committed bank lines went:
  taken as suggested, overridden, split, or classified with no suggestion
  to take. ``period`` is ``YYYY-MM`` over the lines' own dates."""
  start, end = period_date_range(period)
  entity_id = find_entity_id(session, entity_id)
  stmt = select(Event.status, Event.metadata_).where(
    Event.event_type.in_(LEARNED_EVENT_TYPES),
    Event.status.in_((*OPEN_STATUSES, "committed")),
    Event.occurred_at >= datetime.combine(start, time.min),
    Event.occurred_at < datetime.combine(end + timedelta(days=1), time.min),
  )
  if entity_id is not None:
    stmt = stmt.where(Event.entity_id == entity_id)

  open_lines = 0
  unstamped = 0
  per_source: dict[str, Counter[str]] = {}
  for status, metadata in session.execute(stmt).all():
    metadata = dict(metadata or {})
    if status in OPEN_STATUSES:
      open_lines += 1
      continue
    outcome = metadata.get(SUGGESTION_OUTCOME)
    if outcome not in _OUTCOMES:
      unstamped += 1
      continue
    per_source.setdefault(_source_of(metadata), Counter())[str(outcome)] += 1

  by_source = sorted(
    (
      ClassificationSourceStats(
        source=source,
        lines=sum(counts.values()),
        accepted=counts["accepted"],
        overridden=counts["overridden"],
        unsuggested=counts["none"],
        split=counts["split"],
      )
      for source, counts in per_source.items()
    ),
    key=lambda row: (-row.lines, row.source),
  )
  lines = sum(row.lines for row in by_source)
  accepted = sum(row.accepted for row in by_source)
  return ClassificationStatsResponse(
    period=period,
    entity_id=entity_id,
    lines=lines,
    accepted=accepted,
    overridden=sum(row.overridden for row in by_source),
    unsuggested=sum(row.unsuggested for row in by_source),
    split=sum(row.split for row in by_source),
    unstamped=unstamped,
    human_choice_share=(round((lines - accepted) / lines, 4) if lines else None),
    open_lines=open_lines,
    by_source=by_source,
  )
