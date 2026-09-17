"""Load — one cursor's worth of Plaid changes into the inbox.

``/transactions/sync`` hands back what was ``added``, ``modified`` and
``removed`` since the last cursor. Runs inside one ``extensions_session``;
the caller commits.

- **removed** — the bank retracted a line. An unposted event (``captured`` or
  ``classified``) is deleted. A posted one is the customer's books: it is
  flagged a reconciling item (``payload_drift`` with ``source_removed`` in the
  stashed payload) and never touched. A transfer pair that loses one leg
  goes back to a single-leg event for the leg that remains.
- **added / modified** — an event already on the graph is refreshed while
  still captured (hints, amount, date, description); once classified or
  posted, a changed amount or date is recorded on the event
  (``metadata.source_change``) instead. A new transfer-shaped line whose leg
  is already waiting as a captured ``external_transfer`` on this connection
  is merged with it into one ``internal_transfer``. Everything else is
  captured through the kernel.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex
from robosystems.adapters.bank_feed.load import (
  LoadReport,
  capture_event,
  earliest_plausible,
  ensure_agents,
  existing_events,
  refresh_hints,
)
from robosystems.adapters.plaid.client import TransactionsSync
from robosystems.adapters.plaid.pipeline.transform import (
  SOURCE,
  TRANSFER_WINDOW_DAYS,
  Leg,
  counterparties,
  transfer_event,
  transform,
  txn_external_id,
  window,
)
from robosystems.logger import logger
from robosystems.models.extensions import Event
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)

# Metadata keys a later pull may change on a still-captured event.
HINT_KEYS = (
  "merchant_name",
  "merchant_entity_id",
  "counterparty_name",
  "bank_description",
  "plaid_category_primary",
  "plaid_category_detailed",
  "plaid_category_confidence",
  "suggested_account_key",
  "suggested_account_name",
  "suggested_element_id",
  "classification_source",
)
UNPOSTED_STATUSES = frozenset({"captured", "classified"})
POSTED_STATUSES = frozenset({"committed", "fulfilled"})


@dataclass
class PlaidLoadReport(LoadReport):
  events_removed: int = 0
  transfers_matched: int = 0
  reconciling_items: int = 0
  source_changes: int = 0

  def as_counts(self) -> dict[str, Any]:
    return {
      **super().as_counts(),
      "events_removed": self.events_removed,
      "transfers_matched": self.transfers_matched,
      "reconciling_items": self.reconciling_items,
      "source_changes": self.source_changes,
    }


def load_sync(
  session: Session,
  *,
  graph_id: str,
  connection_id: str,
  item_id: str | None,
  created_by: str,
  accounts: list[BankAccount],
  sync: TransactionsSync,
  account_elements: dict[str, str],
  chart: ChartIndex,
  since: date | None = None,
) -> PlaidLoadReport:
  report = PlaidLoadReport()
  by_account = {account.account_id: account for account in accounts}

  removed_ids = [
    str(entry["transaction_id"])
    for entry in sync.removed
    if entry.get("transaction_id")
  ]
  consumed = apply_removed(
    session,
    report,
    removed_ids,
    graph_id=graph_id,
    connection_id=connection_id,
    created_by=created_by,
  )

  # The later record of a transaction wins: a ``modified`` row supersedes the
  # ``added`` one when a single cursor window carries both.
  latest: dict[str, dict[str, Any]] = {}
  for txn in [*sync.added, *sync.modified]:
    latest[str(txn["transaction_id"])] = txn
  transactions = list(latest.values())
  ids = list(latest)

  singles = existing_events(session, SOURCE, [txn_external_id(i) for i in ids])
  in_pairs = pair_events_by_leg(session, connection_id=connection_id, leg_ids=ids)

  agent_ids, report.agents_created = ensure_agents(
    session,
    counterparties(transactions, account_ids=set(by_account), source=SOURCE),
    source=SOURCE,
    connection_id=connection_id,
    created_by=created_by,
  )

  result = transform(
    transactions,
    accounts=accounts,
    connection_id=connection_id,
    item_id=item_id,
    account_elements=account_elements,
    chart=chart,
    agent_ids=agent_ids,
    since=since,
    exclude=frozenset(in_pairs),
    unpairable=frozenset(i for i in ids if txn_external_id(i) in singles),
  )
  report.skipped = result.skipped
  report.classification = result.classification
  report.resolved = result.resolved
  report.earliest_occurred_at = earliest_plausible(result.events)

  for payload in result.events:
    prior = singles.get(str(payload["external_id"]))
    if prior is not None:
      if str(prior.id) not in consumed:
        reconcile_existing(prior, payload, report)
      continue
    if payload["metadata"].get("transfer_candidate"):
      waiting = find_waiting_leg(
        session, payload, connection_id=connection_id, consumed=consumed
      )
      if waiting is not None and _merge_into_pair(
        session,
        waiting,
        payload,
        graph_id=graph_id,
        connection_id=connection_id,
        item_id=item_id,
        created_by=created_by,
        report=report,
      ):
        consumed.add(str(waiting.id))
        continue
    capture_event(
      session, payload, graph_id=graph_id, created_by=created_by, report=report
    )
  session.flush()
  return report


# ── removed ──────────────────────────────────────────────────────────────────


def apply_removed(
  session: Session,
  report: PlaidLoadReport,
  removed_ids: list[str],
  *,
  graph_id: str,
  connection_id: str,
  created_by: str,
) -> set[str]:
  """Apply the bank's retractions; returns the ids of events deleted."""
  consumed: set[str] = set()
  if not removed_ids:
    return consumed
  removed = set(removed_ids)
  singles = existing_events(session, SOURCE, [txn_external_id(i) for i in removed_ids])
  to_delete: list[str] = []
  for event in singles.values():
    if event.status in UNPOSTED_STATUSES:
      to_delete.append(str(event.id))
    elif event.status in POSTED_STATUSES:
      _flag_removed(event, [str((event.metadata_ or {}).get("transaction_id"))])
      report.reconciling_items += 1

  survivors: list[dict[str, Any]] = []
  pairs = pair_events_by_leg(session, connection_id=connection_id, leg_ids=removed_ids)
  for event in {str(e.id): e for e in pairs.values()}.values():
    legs = [str(leg) for leg in (event.metadata_ or {}).get("legs") or []]
    gone = [leg for leg in legs if leg in removed]
    if event.status in UNPOSTED_STATUSES:
      to_delete.append(str(event.id))
      survivor = survivor_payload(event, gone)
      if survivor is not None:
        survivors.append(survivor)
    elif event.status in POSTED_STATUSES:
      _flag_removed(event, gone)
      report.reconciling_items += 1

  if to_delete:
    _delete_events(session, to_delete)
    consumed.update(to_delete)
    report.events_removed += len(to_delete)
  for payload in survivors:
    capture_event(
      session, payload, graph_id=graph_id, created_by=created_by, report=report
    )
  if to_delete or survivors:
    logger.info(
      "Plaid removals on connection %s: %d events deleted, %d legs restored",
      connection_id,
      len(to_delete),
      len(survivors),
    )
  return consumed


def survivor_payload(pair: Event, gone: list[str]) -> dict[str, Any] | None:
  """The single-leg event for the leg a removal leaves behind, or ``None``
  when both legs are gone."""
  metadata = dict(pair.metadata_ or {})
  legs = [str(leg) for leg in metadata.get("legs") or []]
  remaining = [leg for leg in legs if leg not in gone]
  if len(legs) != 2 or len(remaining) != 1:
    return None
  outgoing = remaining[0] == legs[0]
  side = "from" if outgoing else "to"
  amount = abs(int(pair.amount or 0))
  account_name = metadata.get(f"{side}_account_name")
  return {
    "event_type": "external_transfer",
    "event_category": "treasury",
    "event_class": "economic",
    "event_action": "transfer",
    "resource_type": "money",
    "occurred_at": _iso(pair.occurred_at),
    "source": SOURCE,
    "external_id": txn_external_id(remaining[0]),
    "amount": -amount if outgoing else amount,
    "currency": pair.currency or "USD",
    "description": str(metadata.get("bank_description") or "Transfer")[:200],
    "resource_element_id": metadata.get(f"{side}_element_id"),
    "metadata": {
      key: value
      for key, value in {
        "connection_id": metadata.get("connection_id"),
        "item_id": metadata.get("item_id"),
        "transaction_id": remaining[0],
        "status": "posted",
        "account_id": metadata.get(f"{side}_account_id"),
        "account_name": account_name,
        "bank_description": metadata.get("bank_description"),
        "transfer_candidate": True,
        "classification_source": "transfer",
      }.items()
      if value is not None
    },
    "apply_handlers": False,
  }


def _flag_removed(event: Event, transaction_ids: list[str]) -> None:
  """A posted line the bank retracted: a reconciling item, the books untouched.

  The stashed payload carries no entry, so the reconciling-item plan nets the
  posted entry against nothing — its catch-up is the reversal.
  """
  now = datetime.now(UTC).isoformat()
  metadata = dict(event.metadata_ or {})
  accepted = {
    key: value
    for key, value in metadata.items()
    if key not in ("drift_payload", "drift_detected_at")
  }
  accepted["source_removed"] = True
  accepted["source_removed_transaction_ids"] = transaction_ids
  metadata["drift_payload"] = accepted
  metadata["drift_detected_at"] = now
  event.metadata_ = metadata
  event.payload_drift = True


# ── existing events ─────────────────────────────────────────────────────────


def pair_events_by_leg(
  session: Session, *, connection_id: str, leg_ids: list[str]
) -> dict[str, Event]:
  """``{transaction_id: pair_event}`` for every id already inside a transfer
  pair on this connection."""
  found: dict[str, Event] = {}
  wanted = set(leg_ids)
  for start in range(0, len(leg_ids), 500):
    chunk = leg_ids[start : start + 500]
    rows = (
      session.execute(
        select(Event).where(
          Event.source == SOURCE,
          Event.event_type == "internal_transfer",
          Event.metadata_["connection_id"].astext == connection_id,
          Event.metadata_["legs"].has_any(array(chunk)),
        )
      )
      .scalars()
      .all()
    )
    for event in rows:
      for leg in (event.metadata_ or {}).get("legs") or []:
        if str(leg) in wanted:
          found[str(leg)] = event
  return found


def reconcile_existing(
  prior: Event, payload: dict[str, Any], report: PlaidLoadReport
) -> None:
  occurred = _parse(payload["occurred_at"])
  amount_changed = int(prior.amount or 0) != int(payload["amount"])
  date_changed = (
    prior.occurred_at is None or prior.occurred_at.date() != occurred.date()
  )

  if prior.status == "captured":
    changed = refresh_hints(prior, payload["metadata"], HINT_KEYS)
    if amount_changed:
      prior.amount = int(payload["amount"])
      changed = True
    if date_changed:
      prior.occurred_at = occurred
      changed = True
    if payload.get("description") and prior.description != payload["description"]:
      prior.description = payload["description"]
      changed = True
    if payload.get("agent_id") and not prior.agent_id:
      prior.agent_id = payload["agent_id"]
      changed = True
    if changed:
      report.events_updated += 1
    else:
      report.events_existing += 1
    return

  if amount_changed or date_changed:
    metadata = dict(prior.metadata_ or {})
    metadata["source_change"] = {
      "amount": int(payload["amount"]),
      "posted_date": payload["metadata"].get("posted_date"),
      "detected_at": datetime.now(UTC).isoformat(),
    }
    prior.metadata_ = metadata
    report.source_changes += 1
    logger.warning(
      "Plaid changed the amount or date of %s event %s (%s) after it left the inbox",
      prior.status,
      prior.id,
      prior.external_id,
    )
    return
  report.events_existing += 1


# ── transfer legs across syncs ───────────────────────────────────────────────


def find_waiting_leg(
  session: Session,
  payload: dict[str, Any],
  *,
  connection_id: str,
  consumed: set[str],
) -> Event | None:
  """A captured single-leg transfer on another of this connection's accounts,
  opposite in sign and equal in size, within the transfer window."""
  day = str(payload["occurred_at"])[:10]
  lo, hi = window(day, TRANSFER_WINDOW_DAYS)
  account_id = str(payload["metadata"].get("account_id") or "")
  rows = (
    session.execute(
      select(Event)
      .where(
        Event.source == SOURCE,
        Event.status == "captured",
        Event.event_type == "external_transfer",
        Event.amount == -int(payload["amount"]),
        Event.occurred_at >= datetime.combine(lo, time.min),
        Event.occurred_at < datetime.combine(hi + timedelta(days=1), time.min),
        Event.metadata_["connection_id"].astext == connection_id,
        Event.metadata_["transfer_candidate"].astext == "true",
        Event.metadata_["account_id"].astext != account_id,
      )
      .order_by(Event.occurred_at)
    )
    .scalars()
    .all()
  )
  candidates = [event for event in rows if str(event.id) not in consumed]
  if not candidates:
    return None
  target = _parse(payload["occurred_at"]).date()
  return min(
    candidates, key=lambda event: abs((event.occurred_at.date() - target).days)
  )


class _MergeFailed(Exception):
  pass


def _merge_into_pair(
  session: Session,
  waiting: Event,
  payload: dict[str, Any],
  *,
  graph_id: str,
  connection_id: str,
  item_id: str | None,
  created_by: str,
  report: PlaidLoadReport,
) -> bool:
  """Replace the waiting leg with the pair, atomically: if the pair cannot be
  captured, the waiting leg stays and the new leg is captured on its own."""
  merged = merge_legs(waiting, payload, connection_id=connection_id, item_id=item_id)
  failed_before = report.events_failed
  try:
    with session.begin_nested():
      _delete_events(session, [str(waiting.id)])
      if not capture_event(
        session, merged, graph_id=graph_id, created_by=created_by, report=report
      ):
        raise _MergeFailed
  except _MergeFailed:
    report.events_failed = failed_before
    report.errors = report.errors[: max(len(report.errors) - 1, 0)]
    return False
  report.transfers_matched += 1
  return True


def merge_legs(
  waiting: Event, payload: dict[str, Any], *, connection_id: str, item_id: str | None
) -> dict[str, Any]:
  """One ``internal_transfer`` from a waiting event and the new leg."""
  metadata = payload["metadata"]
  waiting_meta = dict(waiting.metadata_ or {})
  new_leg = Leg(
    transaction_id=str(metadata.get("transaction_id")),
    account_id=str(metadata.get("account_id")),
    account_name=metadata.get("account_name"),
    element_id=payload.get("resource_element_id"),
    amount=int(payload["amount"]),
    day=str(payload["occurred_at"])[:10],
    description=metadata.get("bank_description"),
  )
  old_leg = Leg(
    transaction_id=str(waiting_meta.get("transaction_id")),
    account_id=str(waiting_meta.get("account_id")),
    account_name=waiting_meta.get("account_name"),
    element_id=waiting.resource_element_id,
    amount=int(waiting.amount or 0),
    day=_iso(waiting.occurred_at)[:10],
    description=waiting_meta.get("bank_description"),
  )
  out_leg, in_leg = (new_leg, old_leg) if new_leg.amount < 0 else (old_leg, new_leg)
  return transfer_event(out_leg, in_leg, connection_id=connection_id, item_id=item_id)


def _delete_events(session: Session, event_ids: list[str]) -> None:
  session.execute(
    delete(event_dimensions).where(event_dimensions.c.event_id.in_(event_ids))
  )
  session.execute(delete(Event).where(Event.id.in_(event_ids)))


def _parse(value: str) -> datetime:
  """An event timestamp as the naive UTC the events table stores."""
  parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
  if parsed.tzinfo is not None:
    parsed = parsed.astimezone(UTC).replace(tzinfo=None)
  return parsed


def _iso(value: datetime | None) -> str:
  if value is None:
    return datetime.now(UTC).strftime("%Y-%m-%dT00:00:00Z")
  return value.strftime("%Y-%m-%dT%H:%M:%SZ")
