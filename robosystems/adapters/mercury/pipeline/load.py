"""Load — the Mercury pull into the inbox, through the shared bank-feed load.

Counterparties first (scoped to the connection), then every transformed event
through the event-block kernel (``adapters/bank_feed/load.py``). The caller
commits.

Mercury returns every transaction in the window on every pull, so the load
reconciles what it already holds (``bank_feed.drift``): an unposted event
takes the bank's current amount, date and hints in place; a posted one whose
amount or date moved becomes a reconciling item. A line the bank later
reports failed, cancelled, blocked or reversed is retracted the same way —
deleted while unposted, flagged while posted — and a transfer leg that
arrives after its other side was captured on its own settles against it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex
from robosystems.adapters.bank_feed.drift import (
  COUNTERPART_STATUSES,
  POSTED_STATUSES,
  UNPOSTED_STATUSES,
  classified_to,
  delete_unposted,
  event_iso,
  flag_changed,
  flag_removed,
  parse_event_time,
  source_view,
)
from robosystems.adapters.bank_feed.load import (
  LoadReport,
  capture_event,
  earliest_plausible_by_entity,
  ensure_agents,
  existing_events,
  in_closed_months,
  refresh_hints,
)
from robosystems.adapters.mercury.pipeline.transform import (
  TRANSFER_WINDOW_DAYS,
  TransformResult,
  bank_accounts,
  counterparties,
  is_transfer_shaped,
  own_counterparty_names,
  pair_external_id,
  retracted_ids,
  transfer_event,
  transform,
  txn_external_id,
)
from robosystems.logger import logger
from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.models.extensions import Event

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
class MercuryLoadReport(LoadReport):
  events_removed: int = 0
  transfers_matched: int = 0
  legs_voided: int = 0
  reconciling_items: int = 0

  def as_counts(self) -> dict[str, Any]:
    return {
      **super().as_counts(),
      "events_removed": self.events_removed,
      "transfers_matched": self.transfers_matched,
      "legs_voided": self.legs_voided,
      "reconciling_items": self.reconciling_items,
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
  account_entities: dict[str, str] | None = None,
  charts_by_entity: dict[str, ChartIndex] | None = None,
) -> MercuryLoadReport:
  report = MercuryLoadReport()
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

  batch_ids = [str(txn["id"]) for txn in raw.get("transactions") or []]
  # Every id this pull could produce: each line on its own, and each line as
  # the smaller leg of a pair.
  existing = existing_events(
    session,
    source,
    [txn_external_id(i) for i in batch_ids] + [f"mercury_xfer_{i}" for i in batch_ids],
  )
  in_pairs = pair_events_by_leg(
    session, source=source, connection_id=connection_id, leg_ids=batch_ids
  )
  consumed = apply_retracted(
    session, report, retracted_ids(raw), existing=existing, in_pairs=in_pairs
  )
  if consumed:
    existing = {k: e for k, e in existing.items() if str(e.id) not in consumed}

  result: TransformResult = transform(
    raw,
    source=source,
    connection_id=connection_id,
    account_elements=account_elements,
    chart=chart,
    agent_ids=agent_ids,
    include_treasury=include_treasury,
    account_entities=account_entities,
    charts_by_entity=charts_by_entity,
    paired_on_graph=partners_on_graph(in_pairs, consumed),
    unpairable=frozenset(i for i in batch_ids if txn_external_id(i) in existing),
  )
  report.skipped = result.skipped
  report.classification = result.classification
  report.resolved = result.resolved
  report.earliest_by_entity = earliest_plausible_by_entity(result.events)

  closed = in_closed_months(
    session,
    [p for p in result.events if str(p["external_id"]) not in existing],
  )
  raw_by_id = {str(txn["id"]): txn for txn in raw.get("transactions") or []}
  by_account = {account.account_id: account for account in accounts}
  for payload in result.events:
    prior = existing.get(str(payload["external_id"]))
    if prior is not None:
      # A waiting leg merged into a pair earlier in this batch is gone.
      if str(prior.id) not in consumed:
        reconcile_existing(prior, payload, report)
      continue
    if str(payload["external_id"]) in closed:
      capture_event(
        session,
        payload,
        graph_id=graph_id,
        created_by=created_by,
        report=report,
        held=True,
      )
      continue
    metadata = payload["metadata"]
    if payload["event_type"] != "internal_transfer" and is_transfer_shaped(
      metadata.get("kind"), metadata.get("counterparty_name"), own_names
    ):
      other = find_waiting_leg(
        session,
        payload,
        source=source,
        connection_id=connection_id,
        own_names=own_names,
        consumed=consumed,
      )
      if other is not None and settle_with_counterpart(
        session,
        other,
        payload,
        raw_by_id=raw_by_id,
        accounts=by_account,
        account_elements=account_elements,
        account_entities=account_entities,
        graph_id=graph_id,
        source=source,
        connection_id=connection_id,
        created_by=created_by,
        report=report,
        consumed=consumed,
      ):
        continue
    capture_event(
      session, payload, graph_id=graph_id, created_by=created_by, report=report
    )
  session.flush()
  return report


# ── retractions ──────────────────────────────────────────────────────────────


def pair_events_by_leg(
  session: Session, *, source: str, connection_id: str, leg_ids: list[str]
) -> dict[str, Event]:
  """``{transaction_id: pair_event}`` for every id already inside a transfer
  pair on this connection, locked until the run commits."""
  found: dict[str, Event] = {}
  wanted = set(leg_ids)
  for start in range(0, len(leg_ids), _BATCH):
    chunk = leg_ids[start : start + _BATCH]
    rows = (
      session.execute(
        select(Event)
        .where(
          Event.source == source,
          Event.event_type == "internal_transfer",
          Event.metadata_["connection_id"].astext == connection_id,
          Event.metadata_["legs"].has_any(array(chunk)),
        )
        .order_by(Event.id)
        .with_for_update()
      )
      .scalars()
      .all()
    )
    for event in rows:
      for leg in (event.metadata_ or {}).get("legs") or []:
        if str(leg) in wanted:
          found[str(leg)] = event
  return found


def partners_on_graph(
  in_pairs: dict[str, Event], consumed: set[str]
) -> dict[str, str | None]:
  """Each leg already inside a live pair, mapped to its partner (``None``
  when the pair holds no other leg — one whose other side was retracted)."""
  partners: dict[str, str | None] = {}
  for leg, pair in in_pairs.items():
    if str(pair.id) in consumed:
      continue
    legs = [str(other) for other in (pair.metadata_ or {}).get("legs") or []]
    if leg not in legs:
      continue  # released from the pair by a retraction this run
    others = [other for other in legs if other != leg]
    partners[leg] = others[0] if len(legs) == 2 and others else None
  return partners


def already_retracted(event: Event) -> bool:
  """A retraction already raised (pending) or resolved — Mercury reports a
  reversed line on every pull, and it is flagged once."""
  metadata = event.metadata_ or {}
  if metadata.get("source_removed"):
    return True
  drift = metadata.get("drift_payload")
  return (
    bool(event.payload_drift)
    and isinstance(drift, dict)
    and bool(drift.get("source_removed"))
  )


def apply_retracted(
  session: Session,
  report: MercuryLoadReport,
  retracted: list[str],
  *,
  existing: dict[str, Event],
  in_pairs: dict[str, Event],
) -> set[str]:
  """Retract the lines the bank reports failed, cancelled, blocked or
  reversed; returns the ids of events deleted.

  An unposted line is deleted. A posted one becomes a reconciling item whose
  catch-up is the reversal. A pair with a retracted leg goes the same way;
  its other leg, still real at the bank, is released from it and captured on
  its own by this run.
  """
  if not retracted:
    return set()
  gone = set(retracted)
  to_delete: list[str] = []
  for txn_id in retracted:
    event = existing.get(txn_external_id(txn_id))
    if event is None:
      continue
    if event.status in UNPOSTED_STATUSES:
      to_delete.append(str(event.id))
    elif event.status in POSTED_STATUSES and not already_retracted(event):
      flag_removed(event, [txn_id])
      report.reconciling_items += 1

  pairs = {str(pair.id): pair for leg, pair in in_pairs.items() if leg in gone}
  for pair in pairs.values():
    legs = [str(leg) for leg in (pair.metadata_ or {}).get("legs") or []]
    removed = [leg for leg in legs if leg in gone]
    survivors = [leg for leg in legs if leg not in gone]
    if pair.status in UNPOSTED_STATUSES:
      to_delete.append(str(pair.id))
    elif pair.status in POSTED_STATUSES and not already_retracted(pair):
      flag_removed(pair, removed, released=survivors or None)
      report.reconciling_items += 1

  if not to_delete:
    return set()
  deleted = delete_unposted(session, to_delete)
  report.events_removed += len(deleted)
  logger.info("Mercury retractions: %d unposted events deleted", len(deleted))
  return set(deleted)


# ── lines already captured ──────────────────────────────────────────────────


def reconcile_existing(
  prior: Event, payload: dict[str, Any], report: MercuryLoadReport
) -> None:
  occurred = parse_event_time(str(payload["occurred_at"]))
  amount = int(payload["amount"])

  if prior.status in UNPOSTED_STATUSES:
    # A classified line keeps its hints: an accepted suggestion must post
    # what the operator accepted.
    changed = prior.status == "captured" and refresh_hints(
      prior, payload["metadata"], HINT_KEYS
    )
    if int(prior.amount or 0) != amount:
      prior.amount = amount
      changed = True
    if prior.occurred_at is None or prior.occurred_at.date() != occurred.date():
      prior.occurred_at = occurred
      changed = True
    if changed:
      report.events_updated += 1
    else:
      report.events_existing += 1
    return

  if prior.status in POSTED_STATUSES and not already_retracted(prior):
    known_amount, known_date = source_view(prior)
    if known_amount != amount or known_date != occurred.date():
      flag_changed(prior, payload)
      report.reconciling_items += 1
      logger.info(
        "Mercury changed posted event %s (%s): a reconciling item",
        prior.id,
        prior.external_id,
      )
      return
  report.events_existing += 1


# ── transfer legs across pulls ───────────────────────────────────────────────


def find_waiting_leg(
  session: Session,
  payload: dict[str, Any],
  *,
  source: str,
  connection_id: str,
  own_names: set[str],
  consumed: set[str],
) -> Event | None:
  """The other side of this transfer leg, captured on its own by an earlier
  pull: a line on another of this connection's accounts, opposite in sign
  and equal in size, within the transfer window, on the same entity. A
  still-captured one comes first, then a classified one, then a posted one;
  ``settle_with_counterpart`` decides what each means."""
  day = parse_event_time(str(payload["occurred_at"])).date()
  lo, hi = (
    day - timedelta(days=TRANSFER_WINDOW_DAYS),
    day + timedelta(days=TRANSFER_WINDOW_DAYS),
  )
  account_id = str(payload["metadata"].get("account_id") or "")
  same_entity = (
    [Event.entity_id == str(payload["entity_id"])] if payload.get("entity_id") else []
  )
  session.flush()
  rows = (
    session.execute(
      select(Event)
      .where(
        *same_entity,
        Event.source == source,
        Event.status.in_(COUNTERPART_STATUSES),
        Event.event_type.in_(("bank_transaction", "external_transfer")),
        Event.amount == -int(payload["amount"]),
        Event.occurred_at >= datetime.combine(lo, time.min),
        Event.occurred_at < datetime.combine(hi + timedelta(days=1), time.min),
        Event.metadata_["connection_id"].astext == connection_id,
        Event.metadata_["account_id"].astext != account_id,
      )
      .order_by(Event.id)
      .with_for_update()
      .execution_options(populate_existing=True)
    )
    .scalars()
    .all()
  )
  candidates = [
    event
    for event in rows
    if str(event.id) not in consumed
    and not already_retracted(event)
    and is_transfer_shaped(
      (event.metadata_ or {}).get("kind"),
      (event.metadata_ or {}).get("counterparty_name"),
      own_names,
    )
  ]
  if not candidates:
    return None
  rank = {"captured": 0, "classified": 1}
  return min(
    candidates,
    key=lambda event: (
      rank.get(str(event.status), 2),
      abs((event.occurred_at.date() - day).days) if event.occurred_at else 0,
      str(event.id),
    ),
  )


def settle_with_counterpart(
  session: Session,
  other: Event,
  payload: dict[str, Any],
  *,
  raw_by_id: dict[str, dict[str, Any]],
  accounts: dict[str, BankAccount],
  account_elements: dict[str, str],
  account_entities: dict[str, str] | None,
  graph_id: str,
  source: str,
  connection_id: str,
  created_by: str,
  report: MercuryLoadReport,
  consumed: set[str],
) -> bool:
  """This leg's other side is already on the graph; settle the two.

  While the other side is unposted and either unclassified or classified to
  this leg's bank account, the two merge into one ``internal_transfer``.
  When the other side is posted to this leg's bank account the movement is
  booked, so this leg is captured and voided against it. Otherwise this leg
  is captured on its own, naming its counterpart, for the operator to judge.
  Returns True when the payload needs no further capture.
  """
  mine = str(payload.get("resource_element_id") or "")
  contra = classified_to(other)
  status = str(other.status)
  if status == "captured" or (status == "classified" and contra == mine):
    merged = _merged_pair(
      other,
      payload,
      raw_by_id=raw_by_id,
      accounts=accounts,
      account_elements=account_elements,
      account_entities=account_entities,
      source=source,
      connection_id=connection_id,
    )
    if merged is not None and _replace_with_pair(
      session, other, merged, graph_id=graph_id, created_by=created_by, report=report
    ):
      consumed.add(str(other.id))
      return True
    return False

  metadata = payload["metadata"]
  metadata["counterpart_event_id"] = str(other.id)
  metadata["counterpart_status"] = status
  if not (status in POSTED_STATUSES and contra == mine):
    return False
  if not capture_event(
    session, payload, graph_id=graph_id, created_by=created_by, report=report
  ):
    return True
  external_id = str(payload["external_id"])
  captured = existing_events(session, source, [external_id]).get(external_id)
  if captured is None:
    return True
  from robosystems.operations.event_block.commands import update_event_block

  update_event_block(
    session,
    UpdateEventBlockRequest(
      event_id=str(captured.id),
      transition_to="voided",
      metadata_patch={
        "voided_reason": (
          f"The other leg of this transfer is already posted as event "
          f"{other.id}; the movement is booked."
        ),
      },
    ),
    created_by,
    graph_id=graph_id,
  )
  report.legs_voided += 1
  logger.info(
    "Mercury leg %s voided: its other side %s is already posted", external_id, other.id
  )
  return True


def _merged_pair(
  waiting: Event,
  payload: dict[str, Any],
  *,
  raw_by_id: dict[str, dict[str, Any]],
  accounts: dict[str, BankAccount],
  account_elements: dict[str, str],
  account_entities: dict[str, str] | None,
  source: str,
  connection_id: str,
) -> dict[str, Any] | None:
  """One ``internal_transfer`` from the waiting event and the new leg. The
  waiting leg comes from this pull when the window still holds it, else is
  rebuilt from what the event recorded."""
  new_id = str(payload["external_id"]).removeprefix("mercury_txn_")
  new_leg = raw_by_id.get(new_id)
  old_id = str(waiting.external_id or "").removeprefix("mercury_txn_")
  if new_leg is None or not old_id:
    return None
  metadata = waiting.metadata_ or {}
  old_leg = raw_by_id.get(old_id) or {
    "id": old_id,
    "accountId": metadata.get("account_id"),
    "amount": int(waiting.amount or 0) / 100,
    "status": metadata.get("status"),
    "postedAt": event_iso(waiting.occurred_at),
    "createdAt": metadata.get("created_at"),
    "bankDescription": metadata.get("bank_description"),
    "dashboardLink": metadata.get("dashboard_link"),
  }
  source_leg, target_leg = (
    (new_leg, old_leg) if int(payload["amount"]) < 0 else (old_leg, new_leg)
  )
  merged = transfer_event(
    source_leg,
    target_leg,
    accounts,
    account_elements,
    source,
    connection_id,
    account_entities,
  )
  # The pair books on its legs' entity; the waiting leg's stands in when the
  # new leg's account has none.
  if not merged.get("entity_id") and waiting.entity_id:
    merged["entity_id"] = waiting.entity_id
  merged["external_id"] = pair_external_id(new_id, old_id)
  return merged


class _MergeFailed(Exception):
  pass


def _replace_with_pair(
  session: Session,
  waiting: Event,
  merged: dict[str, Any],
  *,
  graph_id: str,
  created_by: str,
  report: MercuryLoadReport,
) -> bool:
  """Replace the waiting leg with the pair, atomically: if the pair cannot be
  captured, the waiting leg stays and the new leg is captured on its own."""
  failed_before = report.events_failed
  errors_before = list(report.errors)
  try:
    with session.begin_nested():
      if not delete_unposted(session, [str(waiting.id)]):
        raise _MergeFailed
      if not capture_event(
        session, merged, graph_id=graph_id, created_by=created_by, report=report
      ):
        raise _MergeFailed
  except _MergeFailed:
    report.events_failed = failed_before
    report.errors = errors_before
    return False
  report.transfers_matched += 1
  return True
