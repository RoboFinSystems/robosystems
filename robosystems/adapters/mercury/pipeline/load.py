"""Load — the Mercury pull into the inbox, through the shared bank-feed load.

Counterparties first (scoped to the connection), then every transformed event
through the event-block kernel (``adapters/bank_feed/load.py``). The caller
commits.

Re-runs are no-ops: an event already on the graph is reported as existing.
The one thing a re-run does change is the Tier-0 hint on a still-captured
event — a customer who codes a transaction in Mercury after the first sync
sees the suggestion arrive on the next one.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import ChartIndex
from robosystems.adapters.bank_feed.load import (
  LoadReport,
  capture_event,
  earliest_plausible,
  ensure_agents,
  existing_events,
  refresh_hints,
)
from robosystems.adapters.mercury.pipeline.transform import (
  TransformResult,
  bank_accounts,
  counterparties,
  own_counterparty_names,
  transform,
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

  agent_ids, created = ensure_agents(
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
  report.earliest_occurred_at = earliest_plausible(result.events)

  existing = existing_events(
    session, source, [str(event["external_id"]) for event in result.events]
  )
  for payload in result.events:
    prior = existing.get(str(payload["external_id"]))
    if prior is not None:
      if prior.status == "captured" and refresh_hints(
        prior, payload["metadata"], HINT_KEYS
      ):
        report.events_updated += 1
      else:
        report.events_existing += 1
      continue
    capture_event(
      session, payload, graph_id=graph_id, created_by=created_by, report=report
    )
  session.flush()
  return report
