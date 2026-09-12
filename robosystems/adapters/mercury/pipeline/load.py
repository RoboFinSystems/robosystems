"""Load — counterparties and captured events, through the kernel.

Runs inside one ``extensions_session`` on the tenant schema. Agents are
written directly (the loader's way, scoped to the connection); events go
through ``create_event_block_in_session`` so the envelope semantics — the
``(source, external_id)`` natural key, the source and connection checks,
the captured status — are the kernel's, not a copy. The caller commits.

Re-runs are no-ops: an event already on the graph is reported as existing.
The one thing a re-run does change is the Tier-0 hint on a still-captured
event — a customer who codes a transaction in Mercury after the first sync
sees the suggestion arrive on the next one.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.adapters.mercury.pipeline.transform import (
  ChartIndex,
  TransformResult,
  bank_accounts,
  counterparties,
  own_counterparty_names,
  transform,
)
from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions import Agent, Event
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
)

# Metadata keys a later pull may change on a still-captured event.
HINT_KEYS = (
  "custom_category",
  "gl_allocations",
  "split",
  "note",
  "suggested_account_key",
  "suggested_account_name",
  "suggested_element_id",
  "classification_source",
)
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


def load_feed(
  session: Session,
  *,
  graph_id: str,
  connection_id: str,
  created_by: str,
  source: str,
  raw: dict[str, Any],
  account_elements: dict[str, str],
  chart: ChartIndex,
  include_treasury: bool = True,
) -> LoadReport:
  report = LoadReport()
  accounts = bank_accounts(raw, include_treasury=include_treasury)
  own_names = own_counterparty_names(accounts)

  agent_ids, created = _ensure_agents(
    session,
    counterparties(raw, own_names, source),
    source=source,
    connection_id=connection_id,
    created_by=created_by,
  )
  report.agents_created = created

  result: TransformResult = transform(
    raw,
    source=source,
    connection_id=connection_id,
    account_elements=account_elements,
    chart=chart,
    agent_ids=agent_ids,
    include_treasury=include_treasury,
  )
  report.skipped = result.skipped
  report.classification = result.classification
  report.resolved = result.resolved
  if result.events:
    report.earliest_occurred_at = str(result.events[0]["occurred_at"])

  existing = _existing_events(
    session, source, [str(event["external_id"]) for event in result.events]
  )
  for payload in result.events:
    prior = existing.get(str(payload["external_id"]))
    if prior is not None:
      if prior.status == "captured" and _refresh_hints(prior, payload["metadata"]):
        report.events_updated += 1
      else:
        report.events_existing += 1
      continue
    try:
      with session.begin_nested():
        create_event_block_in_session(
          session,
          CreateEventBlockRequest.model_validate(payload),
          created_by,
          graph_id=graph_id,
        )
      report.events_created += 1
    except Exception as exc:
      report.events_failed += 1
      if len(report.errors) < 10:
        report.errors.append(f"{payload.get('external_id')}: {str(exc)[:300]}")
      logger.warning(
        "Mercury event %s failed to capture: %s", payload.get("external_id"), exc
      )
  session.flush()
  return report


def _ensure_agents(
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


def _existing_events(
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


def _refresh_hints(event: Event, metadata: dict[str, Any]) -> bool:
  """Merge the current pull's hint keys into a captured event; True if changed."""
  current = dict(event.metadata_ or {})
  changed = False
  for key in HINT_KEYS:
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
