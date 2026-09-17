"""Load — counterparties and captured events, through the kernel.

Runs inside one ``extensions_session`` on the tenant schema. Agents are
written directly (the loader's way, scoped to the connection); events go
through ``create_event_block_in_session`` so the envelope semantics — the
``(source, external_id)`` natural key, the source and connection checks,
the captured status — are the kernel's, not a copy. The caller commits.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions import Agent, Event
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
)

# The earliest plausible posting year. A feed can carry a placeholder date
# (the Mercury sandbox posts transactions dated year 1), and the calendar
# bootstrap must not open the books there.
PLAUSIBLE_YEAR_FLOOR = "1990"
_BATCH = 500


@dataclass
class LoadReport:
  agents_created: int = 0
  events_created: int = 0
  events_existing: int = 0
  events_updated: int = 0
  events_failed: int = 0
  errors: list[str] = field(default_factory=list)
  skipped: Counter[str] = field(default_factory=Counter)
  classification: Counter[str] = field(default_factory=Counter)
  resolved: Counter[str] = field(default_factory=Counter)
  earliest_occurred_at: str | None = None

  def as_counts(self) -> dict[str, Any]:
    return {
      "agents_created": self.agents_created,
      "events_captured": self.events_created,
      "events_existing": self.events_existing,
      "events_updated": self.events_updated,
      "events_failed": self.events_failed,
      "skipped": dict(self.skipped),
      "classified": dict(self.classification),
      "suggestions": dict(self.resolved),
    }


def earliest_plausible(events: Iterable[dict[str, Any]]) -> str | None:
  """The earliest ``occurred_at`` that is a real posting date."""
  sane = [
    str(event["occurred_at"])
    for event in events
    if str(event["occurred_at"])[:4] >= PLAUSIBLE_YEAR_FLOOR
  ]
  return min(sane) if sane else None


def ensure_agents(
  session: Session,
  agents: list[dict[str, Any]],
  *,
  source: str,
  connection_id: str,
  created_by: str,
) -> tuple[dict[str, str], int]:
  """``{external_id: agent_id}`` for every counterparty, creating the missing
  ones scoped to this connection (the agents table's unique key)."""
  rows = session.execute(
    select(Agent.external_id, Agent.id).where(
      Agent.source == source,
      Agent.connection_id == connection_id,
      Agent.external_id.is_not(None),
    )
  ).all()
  known: dict[str, str] = {str(ext): str(agent_id) for ext, agent_id in rows}
  created = 0
  now = datetime.now(UTC)
  for spec in agents:
    external_id = str(spec["external_id"])
    if external_id in known:
      continue
    agent = Agent(
      agent_type=spec["agent_type"],
      name=spec["name"],
      source=source,
      external_id=external_id,
      connection_id=connection_id,
      is_active=True,
      metadata_=dict(spec.get("metadata") or {}),
      created_at=now,
      updated_at=now,
      created_by=created_by,
    )
    session.add(agent)
    session.flush()
    known[external_id] = str(agent.id)
    created += 1
  return known, created


def existing_events(
  session: Session, source: str, external_ids: list[str]
) -> dict[str, Event]:
  found: dict[str, Event] = {}
  for start in range(0, len(external_ids), _BATCH):
    chunk = external_ids[start : start + _BATCH]
    rows = (
      session.execute(
        select(Event).where(Event.source == source, Event.external_id.in_(chunk))
      )
      .scalars()
      .all()
    )
    for event in rows:
      found[str(event.external_id)] = event
  return found


def refresh_hints(event: Event, metadata: dict[str, Any], keys: Iterable[str]) -> bool:
  """Merge the current pull's hint keys into a captured event; True if changed."""
  current = dict(event.metadata_ or {})
  changed = False
  for key in keys:
    new_value = metadata.get(key)
    if current.get(key) != new_value:
      if new_value is None:
        current.pop(key, None)
      else:
        current[key] = new_value
      changed = True
  if changed:
    event.metadata_ = current
  return changed


def capture_event(
  session: Session,
  payload: dict[str, Any],
  *,
  graph_id: str,
  created_by: str,
  report: LoadReport,
) -> bool:
  """Capture one event in its own savepoint; a bad row never stops the batch."""
  try:
    with session.begin_nested():
      create_event_block_in_session(
        session,
        CreateEventBlockRequest.model_validate(payload),
        created_by,
        graph_id=graph_id,
      )
    report.events_created += 1
    return True
  except Exception as exc:
    report.events_failed += 1
    if len(report.errors) < 10:
      report.errors.append(f"{payload.get('external_id')}: {str(exc)[:300]}")
    logger.warning(
      "Bank-feed event %s failed to capture: %s", payload.get("external_id"), exc
    )
    return False
