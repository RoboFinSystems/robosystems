"""Load — one cursor's worth of Plaid changes into the inbox.

``/transactions/sync`` hands back what was ``added``, ``modified`` and
``removed`` since the last cursor. Runs inside one ``extensions_session``;
the caller commits.

The invariant throughout: an unposted event is edited or deleted in place; a
posted one is the customer's books and is never touched, only flagged as a
reconciling item (``payload_drift``) whose stashed payload carries what the
bank now says. Plaid ids are Item-scoped, so after a re-Link a replay re-keys
the held events (``rekey_replaced_events``) rather than capturing them again.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast

from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex, name_key
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
  bank_event,
  counterparties,
  days_between,
  leg_from_transaction,
  transfer_event,
  transform,
  txn_external_id,
  window,
)
from robosystems.logger import logger
from robosystems.models.api.event_block import UpdateEventBlockRequest
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
# Reconciling-item bookkeeping that is never part of an accepted payload.
DRIFT_BOOKKEEPING_KEYS = frozenset(
  {"drift_payload", "drift_detected_at", "reconciliation_history"}
)
# What the last flag said the bank and the entry should be. A resolution
# leaves these in the live metadata; every new flag rebuilds them from
# scratch, never carries them forward — a stale entry would make the planner
# net against lines the bank no longer sends.
SOURCE_STATE_KEYS = frozenset(
  {
    "source_amount",
    "source_posted_date",
    "source_removed",
    "source_removed_transaction_ids",
    "source_legs",
    "pair_dissolved",
    "posting_date",
    "memo",
    "line_items",
    "entries",
  }
)
COUNTERPART_STATUSES = ("captured", "classified", "committed", "pending", "fulfilled")


@dataclass
class PlaidLoadReport(LoadReport):
  events_removed: int = 0
  events_rekeyed: int = 0
  transfers_matched: int = 0
  pairs_dissolved: int = 0
  legs_voided: int = 0
  reconciling_items: int = 0

  def as_counts(self) -> dict[str, Any]:
    return {
      **super().as_counts(),
      "events_removed": self.events_removed,
      "events_rekeyed": self.events_rekeyed,
      "transfers_matched": self.transfers_matched,
      "pairs_dissolved": self.pairs_dissolved,
      "legs_voided": self.legs_voided,
      "reconciling_items": self.reconciling_items,
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
  rekey_replaced: bool = False,
) -> PlaidLoadReport:
  """``rekey_replaced`` is set on a replay (no cursor): only then does every
  id the feed still has arrive in one batch, which is what makes an event
  under another Item's id safe to re-key."""
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
  # ``added`` one when a single cursor window carries both, and a retraction
  # in the same window supersedes either — a line added and removed between
  # two syncs is never captured.
  latest: dict[str, dict[str, Any]] = {}
  for txn in [*sync.added, *sync.modified]:
    latest[str(txn["transaction_id"])] = txn
  for removed_id in removed_ids:
    latest.pop(removed_id, None)
  transactions = list(latest.values())
  ids = list(latest)

  singles = existing_events(session, SOURCE, [txn_external_id(i) for i in ids])
  in_pairs = pair_events_by_leg(session, connection_id=connection_id, leg_ids=ids)

  in_window = [
    txn
    for txn in transactions
    if since is None or str(txn.get("date") or "") >= since.isoformat()
  ]
  agent_ids, report.agents_created = ensure_agents(
    session,
    counterparties(in_window, account_ids=set(by_account), source=SOURCE),
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

  if rekey_replaced:
    singles.update(
      rekey_replaced_events(
        session,
        result.events,
        known=singles,
        connection_id=connection_id,
        item_id=item_id,
        report=report,
      )
    )

  result.events.extend(
    reconcile_pairs(
      session,
      in_pairs,
      latest,
      by_account=by_account,
      account_elements=account_elements,
      chart=chart,
      agent_ids=agent_ids,
      connection_id=connection_id,
      item_id=item_id,
      report=report,
      consumed=consumed,
    )
  )

  for payload in result.events:
    prior = singles.get(str(payload["external_id"]))
    if prior is not None:
      if str(prior.id) not in consumed:
        reconcile_existing(prior, payload, report)
      continue
    if payload["metadata"].get("transfer_candidate"):
      other = find_waiting_leg(
        session, payload, connection_id=connection_id, consumed=consumed
      )
      if other is not None and settle_with_counterpart(
        session,
        other,
        payload,
        graph_id=graph_id,
        connection_id=connection_id,
        item_id=item_id,
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
    survivor = survivor_payload(event, gone)
    if event.status in UNPOSTED_STATUSES:
      to_delete.append(str(event.id))
      if survivor is not None:
        survivors.append(survivor)
    elif event.status in POSTED_STATUSES:
      # The pair's entry is reversed by the catch-up; the leg still real at
      # the bank lives on as its own line, and leaves the pair's legs so a
      # later sync finds it there and not here.
      released = [str(survivor["metadata"]["transaction_id"])] if survivor else None
      _flag_removed(event, gone, released=released)
      if survivor is not None:
        survivors.append(survivor)
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
  # The leg's own date where the pair kept it; the pair's for older pairs.
  day = str(metadata.get(f"{side}_date") or _iso(pair.occurred_at)[:10])[:10]
  return {
    "event_type": "external_transfer",
    "event_category": "treasury",
    "event_class": "economic",
    "event_action": "transfer",
    "resource_type": "money",
    "occurred_at": f"{day}T00:00:00Z",
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


def _flag_removed(
  event: Event, transaction_ids: list[str], *, released: list[str] | None = None
) -> None:
  """A posted line the bank retracted: a reconciling item, the books untouched.

  The stashed payload carries no entry, so the reconciling-item plan nets the
  posted entry against nothing — its catch-up is the reversal. ``released``
  names a pair's leg that lives on as its own line: it leaves the pair's
  ``legs``, in the stashed payload and on the row, so the pair never claims
  it again.
  """
  accepted = _accepted_payload(event)
  accepted["source_removed"] = True
  accepted["source_removed_transaction_ids"] = transaction_ids
  if released:
    _release_legs(event, accepted, released)
  _flag(event, accepted)
  if released:
    event.metadata_ = {
      **(event.metadata_ or {}),
      "legs": accepted["legs"],
      "released_legs": accepted["released_legs"],
    }


def _release_legs(event: Event, accepted: dict[str, Any], released: list[str]) -> None:
  kept = [
    str(leg)
    for leg in ((event.metadata_ or {}).get("legs") or [])
    if str(leg) not in released
  ]
  accepted["legs"] = kept
  accepted["released_legs"] = [
    *[str(leg) for leg in ((event.metadata_ or {}).get("released_legs") or [])],
    *released,
  ]


def _flag_changed(event: Event, payload: dict[str, Any]) -> None:
  """A posted line whose amount or date the bank changed: a reconciling item.

  The stashed payload records what the bank now says (``source_amount``,
  ``source_posted_date``) and, when the line was posted from its own
  classification, the entry it should now have. The reconciling-item plan
  nets the posted entry against that one, so its catch-up is the difference.
  A line posted through a tenant rule has no classification to rebuild from;
  its payload carries no entry and the plan shows the reversal, for the
  operator to re-post.
  """
  amount = int(payload["amount"])
  posted_date = str(payload["occurred_at"])[:10]
  accepted = _accepted_payload(event)
  accepted["source_amount"] = amount
  accepted["source_posted_date"] = posted_date
  accepted.update(_planned_entry(event, amount, posted_date))
  _flag(event, accepted)


def _planned_entry(event: Event, amount: int, posted_date: str) -> dict[str, Any]:
  """The entry a posted line should now have, from its own classification —
  or nothing, for a line posted through a tenant rule."""
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BankFeedMetadata,
    plan_lines,
  )
  from robosystems.operations.event_block.python_handlers.types import (
    HandlerMetadataValidationError,
  )

  try:
    lines = plan_lines(
      event_type=str(event.event_type),
      resource_element_id=event.resource_element_id,
      amount=amount,
      metadata=BankFeedMetadata.model_validate(dict(event.metadata_ or {})),
    )
  except (HandlerMetadataValidationError, ValidationError):
    lines = None
  if not lines:
    return {}
  return {
    "posting_date": posted_date,
    "memo": event.description,
    "line_items": [
      {
        "element_id": line.element_id,
        "debit_amount": line.debit_amount,
        "credit_amount": line.credit_amount,
      }
      for line in lines
    ],
  }


def _accepted_payload(event: Event) -> dict[str, Any]:
  """The live metadata less the bookkeeping and the previous flag's verdict."""
  return {
    key: value
    for key, value in (event.metadata_ or {}).items()
    if key not in DRIFT_BOOKKEEPING_KEYS and key not in SOURCE_STATE_KEYS
  }


def _flag(event: Event, accepted: dict[str, Any]) -> None:
  metadata = dict(event.metadata_ or {})
  metadata["drift_payload"] = accepted
  metadata["drift_detected_at"] = datetime.now(UTC).isoformat()
  event.metadata_ = metadata
  event.payload_drift = True


def source_view(event: Event) -> tuple[int, date | None]:
  """The amount and posting date the bank last reported for a posted line.

  A pending reconciling item's stashed payload, else an accepted one (a
  resolved item leaves its payload as the live metadata), else the event's
  own columns — so a change already raised, or already resolved, is not
  raised again on the next sync.
  """
  metadata: dict[str, Any] = {**(event.metadata_ or {})}
  drift: Any = metadata.get("drift_payload")
  view: dict[str, Any] = metadata
  if bool(event.payload_drift) and isinstance(drift, dict) and "source_amount" in drift:
    view = cast("dict[str, Any]", drift)
  amount = int(view.get("source_amount", event.amount or 0))
  stored = view.get("source_posted_date")
  if stored:
    return amount, date.fromisoformat(str(stored)[:10])
  return amount, event.occurred_at.date() if event.occurred_at else None


# ── a replaced Item ──────────────────────────────────────────────────────────

# The identity a re-key moves to the new Item. Everything else the event
# holds — its classification, its books, its trail — is untouched.
IDENTITY_KEYS = (
  "transaction_id",
  "account_id",
  "item_id",
  "connection_id",
  "legs",
  "from_account_id",
  "to_account_id",
)
# A posted line's column date can lag the date the bank last reported (a
# resolved date change); the candidate window allows for it.
REKEY_DATE_SLACK_DAYS = 7


def rekey_replaced_events(
  session: Session,
  payloads: list[dict[str, Any]],
  *,
  known: dict[str, Event],
  connection_id: str,
  item_id: str | None,
  report: PlaidLoadReport,
) -> dict[str, Event]:
  """Give events captured under an earlier Item the ids the new one uses.

  Only a replay calls this: every id the feed still has is in the batch, so
  an event that no payload identifies and that this connection may claim is
  one the new Item re-issued. Two shapes may be claimed: an event this same
  connection captured under its previous Item (the dead Item replaced in
  place), and an event with no Item at all (a disconnect purged its source
  keys; a reconnect finds it). An event that still carries another
  connection's Item is another feed's line and is never touched, whatever
  else matches. Within that, the match is on the chart account, the posting
  date and amount the bank last reported, and the bank's description where
  both sides have one — a pair also on its ``from`` account. Two lines alike
  on everything but description never cross. Returns ``{new_external_id:
  event}`` for the main loop to reconcile as existing.
  """
  fresh = [p for p in payloads if str(p["external_id"]) not in known]
  if not fresh:
    return {}
  elements = {
    str(p["resource_element_id"]) for p in fresh if p.get("resource_element_id")
  }
  days = [_parse(str(p["occurred_at"])).date() for p in fresh]
  identified = {str(event.id) for event in known.values()}
  by_key: dict[tuple[Any, ...], list[Event]] = {}
  for event in _replay_candidates(session, elements, min(days), max(days)):
    metadata = event.metadata_ or {}
    held_item = str(metadata.get("item_id") or "")
    if held_item == str(item_id or "") or str(event.id) in identified:
      continue
    ours = str(metadata.get("connection_id") or "") == connection_id
    if held_item and not ours:
      continue  # another connection's live line
    by_key.setdefault(_event_fingerprint(event), []).append(event)
  if not by_key:
    return {}

  rekeyed: dict[str, Event] = {}
  now = datetime.now(UTC).isoformat()
  pending = list(fresh)
  # Pass 1: the bank's description agrees. Pass 2: one side has none (a
  # purged line, a bank that sends none) and the account, date and amount do.
  for exact in (True, False):
    for payload in list(pending):
      pool = by_key.get(_payload_fingerprint(payload))
      match = _pick(pool or [], payload, exact=exact)
      if match is None:
        continue
      cast("list[Event]", pool).remove(match)
      _rekey(match, payload, at=now)
      rekeyed[str(payload["external_id"])] = match
      pending.remove(payload)
  if rekeyed:
    session.flush()
    report.events_rekeyed += len(rekeyed)
    logger.info(
      "Plaid replay re-keyed %d events to Item %s", len(rekeyed), item_id or ""
    )
  return rekeyed


def _replay_candidates(
  session: Session, element_ids: set[str], lo: date, hi: date
) -> list[Event]:
  """This feed's events on the batch's chart accounts around its dates.

  Source and chart account are not indexed together; the date index carries
  the scan, and a replay is rare (a re-Link, a full rebuild).
  """
  if not element_ids:
    return []
  slack = timedelta(days=REKEY_DATE_SLACK_DAYS)
  return list(
    session.execute(
      select(Event).where(
        Event.source == SOURCE,
        Event.resource_element_id.in_(sorted(element_ids)),
        Event.occurred_at >= datetime.combine(lo - slack, time.min),
        Event.occurred_at < datetime.combine(hi + slack + timedelta(days=1), time.min),
      )
    )
    .scalars()
    .all()
  )


def _event_fingerprint(event: Event) -> tuple[Any, ...]:
  metadata = event.metadata_ or {}
  if event.status in POSTED_STATUSES:
    amount, day = source_view(event)
  else:
    amount = int(event.amount or 0)
    day = event.occurred_at.date() if event.occurred_at else None
  pair = str(event.event_type) == "internal_transfer"
  return (
    pair,
    str(event.resource_element_id or ""),
    str(metadata.get("from_element_id") or "") if pair else "",
    day.isoformat() if day else "",
    amount,
  )


def _payload_fingerprint(payload: dict[str, Any]) -> tuple[Any, ...]:
  metadata = payload.get("metadata") or {}
  pair = payload.get("event_type") == "internal_transfer"
  return (
    pair,
    str(payload.get("resource_element_id") or ""),
    str(metadata.get("from_element_id") or "") if pair else "",
    str(payload["occurred_at"])[:10],
    int(payload["amount"]),
  )


def _description(metadata: dict[str, Any] | None) -> str:
  return name_key(str((metadata or {}).get("bank_description") or ""))


def _pick(pool: list[Event], payload: dict[str, Any], *, exact: bool) -> Event | None:
  wanted = _description(payload.get("metadata"))
  for event in pool:
    have = _description(event.metadata_)
    if exact and wanted and have == wanted:
      return event
    if not exact and (not wanted or not have):
      return event
  return None


def _rekey(event: Event, payload: dict[str, Any], *, at: str) -> None:
  metadata = dict(event.metadata_ or {})
  incoming = payload.get("metadata") or {}
  trail = list(metadata.get("rekeyed_from") or [])
  trail.append(
    {
      "external_id": event.external_id,
      "at": at,
      **{key: metadata[key] for key in IDENTITY_KEYS if metadata.get(key) is not None},
    }
  )
  for key in IDENTITY_KEYS:
    if key in incoming:
      metadata[key] = incoming[key]
    else:
      metadata.pop(key, None)
  metadata["rekeyed_from"] = trail
  event.metadata_ = metadata
  event.external_id = str(payload["external_id"])


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
  amount = int(payload["amount"])

  if prior.status in UNPOSTED_STATUSES:
    # A classified line keeps its hints: an accepted suggestion must post
    # what the operator accepted. A split that no longer adds up is refused
    # at commit, with the reason.
    changed = prior.status == "captured" and refresh_hints(
      prior, payload["metadata"], HINT_KEYS
    )
    if int(prior.amount or 0) != amount:
      prior.amount = amount
      changed = True
    if prior.occurred_at is None or prior.occurred_at.date() != occurred.date():
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

  if prior.status in POSTED_STATUSES:
    known_amount, known_date = source_view(prior)
    if known_amount != amount or known_date != occurred.date():
      _flag_changed(prior, payload)
      report.reconciling_items += 1
      logger.info(
        "Plaid changed posted event %s (%s): a reconciling item",
        prior.id,
        prior.external_id,
      )
      return
  report.events_existing += 1


# ── legs already inside a pair ───────────────────────────────────────────────


def stored_legs(pair: Event) -> dict[str, Leg]:
  """The two legs a captured pair recorded — each on its own date where the
  pair kept it, on the pair's date for pairs captured before it did."""
  metadata = pair.metadata_ or {}
  legs = [str(leg) for leg in metadata.get("legs") or []]
  if len(legs) != 2:
    return {}
  day = _iso(pair.occurred_at)[:10]
  amount = abs(int(pair.amount or 0))
  description = metadata.get("bank_description")
  out_id, in_id = legs
  return {
    out_id: Leg(
      transaction_id=out_id,
      account_id=str(metadata.get("from_account_id") or ""),
      account_name=metadata.get("from_account_name"),
      element_id=metadata.get("from_element_id"),
      amount=-amount,
      day=str(metadata.get("from_date") or day)[:10],
      description=description,
    ),
    in_id: Leg(
      transaction_id=in_id,
      account_id=str(metadata.get("to_account_id") or ""),
      account_name=metadata.get("to_account_name"),
      element_id=metadata.get("to_element_id"),
      amount=amount,
      day=str(metadata.get("to_date") or day)[:10],
      description=description,
    ),
  }


def legs_pair(first: Leg, second: Leg) -> bool:
  """Whether two legs are still the two sides of one transfer."""
  out_leg, in_leg = (first, second) if first.amount < 0 else (second, first)
  return (
    out_leg.amount < 0 < in_leg.amount
    and in_leg.amount == -out_leg.amount
    and in_leg.account_id != out_leg.account_id
    and abs(days_between(out_leg.day, in_leg.day)) <= TRANSFER_WINDOW_DAYS
  )


def legs_signature(legs: Iterable[Leg]) -> frozenset[tuple[str, int, str]]:
  return frozenset((leg.transaction_id, leg.amount, leg.day) for leg in legs)


def known_legs(pair: Event) -> frozenset[tuple[str, int, str]]:
  """The legs the bank last reported for a posted pair: a pending flag's,
  else a resolved one's, else the pair's own — so a change already raised or
  resolved is not raised again."""
  metadata = pair.metadata_ or {}
  drift = metadata.get("drift_payload")
  source: Any = None
  if bool(pair.payload_drift) and isinstance(drift, dict) and drift.get("source_legs"):
    source = drift["source_legs"]
  elif metadata.get("source_legs"):
    source = metadata["source_legs"]
  if isinstance(source, list):
    return frozenset(
      (
        str(leg.get("transaction_id")),
        int(leg.get("amount") or 0),
        str(leg.get("date") or "")[:10],
      )
      for leg in source
      if isinstance(leg, dict)
    )
  return legs_signature(stored_legs(pair).values())


def reconcile_pairs(
  session: Session,
  in_pairs: dict[str, Event],
  latest: dict[str, dict[str, Any]],
  *,
  by_account: dict[str, BankAccount],
  account_elements: dict[str, str],
  chart: ChartIndex,
  agent_ids: dict[str, str],
  connection_id: str,
  item_id: str | None,
  report: PlaidLoadReport,
  consumed: set[str],
) -> list[dict[str, Any]]:
  """Apply the batch's changes to legs already inside a pair.

  A pair stays a pair while its legs still match: an unposted one takes the
  new amount and dates in place; a posted one becomes a reconciling item
  whose stashed payload carries the entry it should now have. Legs that no
  longer match dissolve the pair: an unposted pair is deleted, a posted one
  is flagged with no entry (its catch-up reverses it) and its ``legs``
  emptied; either way both legs come back as single-leg payloads for the
  main loop, where one may pair anew. Returns those payloads.
  """
  by_pair: dict[str, tuple[Event, dict[str, dict[str, Any]]]] = {}
  for leg_id, pair in in_pairs.items():
    if leg_id in latest and str(pair.id) not in consumed:
      by_pair.setdefault(str(pair.id), (pair, {}))[1][leg_id] = latest[leg_id]

  released: list[dict[str, Any]] = []
  for pair, changed in by_pair.values():
    legs = stored_legs(pair)
    if not legs:
      report.events_existing += 1
      continue
    current = {
      **legs,
      **{
        leg_id: leg_from_transaction(txn, by_account, account_elements)
        for leg_id, txn in changed.items()
      },
    }
    first, second = current.values()
    still = legs_pair(first, second)
    out_leg, in_leg = (first, second) if first.amount < 0 else (second, first)

    def leg_payloads(
      pair: Event = pair,
      changed: dict[str, dict[str, Any]] = changed,
      current: dict[str, Leg] = current,
    ) -> list[dict[str, Any]]:
      out: list[dict[str, Any]] = []
      for leg_id in current:
        if leg_id in changed:
          event, _classification = bank_event(
            changed[leg_id],
            by_account,
            account_elements,
            chart,
            agent_ids,
            connection_id=connection_id,
            item_id=item_id,
          )
          out.append(event)
        else:
          single = survivor_payload(
            pair, [other for other in current if other != leg_id]
          )
          if single is not None:
            out.append(single)
      return out

    if pair.status in UNPOSTED_STATUSES:
      if still:
        if _update_pair(pair, out_leg, in_leg):
          report.events_updated += 1
        else:
          report.events_existing += 1
      else:
        released.extend(leg_payloads())
        _delete_events(session, [str(pair.id)])
        consumed.add(str(pair.id))
        report.pairs_dissolved += 1
    elif pair.status in POSTED_STATUSES:
      if known_legs(pair) == legs_signature(current.values()):
        report.events_existing += 1
      elif still:
        _flag_pair_changed(pair, out_leg, in_leg)
        report.reconciling_items += 1
      else:
        released.extend(leg_payloads())
        _flag_pair_dissolved(pair, list(current))
        report.reconciling_items += 1
        report.pairs_dissolved += 1
    else:
      report.events_existing += 1
  if by_pair:
    logger.info(
      "Plaid legs inside pairs on connection %s: %d pairs touched, %d dissolved",
      connection_id,
      len(by_pair),
      report.pairs_dissolved,
    )
  return released


def _update_pair(pair: Event, out_leg: Leg, in_leg: Leg) -> bool:
  """An unposted pair takes its legs' new amount and dates; True if changed."""
  changed = False
  magnitude = abs(in_leg.amount)
  later = max(out_leg.day, in_leg.day)
  if int(pair.amount or 0) != magnitude:
    pair.amount = magnitude
    changed = True
  if pair.occurred_at is None or _iso(pair.occurred_at)[:10] != later:
    pair.occurred_at = _parse(f"{later}T00:00:00Z")
    changed = True
  metadata = dict(pair.metadata_ or {})
  for key, value in (("from_date", out_leg.day), ("to_date", in_leg.day)):
    if metadata.get(key) != value:
      metadata[key] = value
      changed = True
  if changed:
    pair.metadata_ = metadata
  return changed


def _leg_record(leg: Leg) -> dict[str, Any]:
  return {"transaction_id": leg.transaction_id, "amount": leg.amount, "date": leg.day}


def _flag_pair_changed(pair: Event, out_leg: Leg, in_leg: Leg) -> None:
  """A posted pair whose legs moved together: a reconciling item carrying the
  transfer it should now be."""
  magnitude = abs(in_leg.amount)
  later = max(out_leg.day, in_leg.day)
  accepted = _accepted_payload(pair)
  accepted["source_legs"] = [_leg_record(out_leg), _leg_record(in_leg)]
  accepted["source_amount"] = magnitude
  accepted["source_posted_date"] = later
  accepted["from_date"] = out_leg.day
  accepted["to_date"] = in_leg.day
  accepted.update(_planned_entry(pair, magnitude, later))
  _flag(pair, accepted)


def _flag_pair_dissolved(pair: Event, leg_ids: list[str]) -> None:
  """A posted pair whose legs no longer match: a reconciling item with no
  entry — its catch-up reverses the transfer — and both legs released to
  live as their own lines."""
  accepted = _accepted_payload(pair)
  accepted["pair_dissolved"] = True
  _release_legs(pair, accepted, leg_ids)
  _flag(pair, accepted)
  pair.metadata_ = {
    **(pair.metadata_ or {}),
    "legs": accepted["legs"],
    "released_legs": accepted["released_legs"],
  }


# ── transfer legs across syncs ───────────────────────────────────────────────


def find_waiting_leg(
  session: Session,
  payload: dict[str, Any],
  *,
  connection_id: str,
  consumed: set[str],
) -> Event | None:
  """The other side of this leg, already on the graph: a single-leg transfer
  on another of this connection's accounts, opposite in sign and equal in
  size, within the transfer window. A still-captured one comes first, then
  a classified one, then a posted one; ``settle_with_counterpart`` decides
  what each means."""
  day = str(payload["occurred_at"])[:10]
  lo, hi = window(day, TRANSFER_WINDOW_DAYS)
  account_id = str(payload["metadata"].get("account_id") or "")
  rows = (
    session.execute(
      select(Event)
      .where(
        Event.source == SOURCE,
        Event.status.in_(COUNTERPART_STATUSES),
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
  rank = {"captured": 0, "classified": 1}
  return min(
    candidates,
    key=lambda event: (
      rank.get(str(event.status), 2),
      abs((event.occurred_at.date() - target).days),
    ),
  )


def classified_to(event: Event) -> str | None:
  """The one account a bank leg is classified to, or ``None`` (unclassified,
  a split, a rule-posted line)."""
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BankFeedMetadata,
    contra_allocations,
  )
  from robosystems.operations.event_block.python_handlers.types import (
    HandlerMetadataValidationError,
  )

  try:
    allocations = contra_allocations(
      BankFeedMetadata.model_validate(dict(event.metadata_ or {})),
      amount=int(event.amount or 0),
    )
  except (HandlerMetadataValidationError, ValidationError):
    return None
  if allocations and len(allocations) == 1:
    return str(allocations[0].element_id)
  return None


def settle_with_counterpart(
  session: Session,
  other: Event,
  payload: dict[str, Any],
  *,
  graph_id: str,
  connection_id: str,
  item_id: str | None,
  created_by: str,
  report: PlaidLoadReport,
  consumed: set[str],
) -> bool:
  """This leg's other side is already on the graph; settle the two.

  While the other side is unposted and either unclassified or classified to
  this leg's bank account, the two merge into one ``internal_transfer`` —
  the merged pair posts exactly the entry the operator chose. When the
  other side is posted to this leg's bank account the movement is booked,
  so this leg is captured and voided against it: a live second line would
  book it again the moment someone classified it. When the other side went
  somewhere else, this leg is captured on its own, naming its counterpart,
  for the operator to judge. Returns True when the payload needs no capture.
  """
  mine = str(payload.get("resource_element_id") or "")
  contra = classified_to(other)
  status = str(other.status)
  if status == "captured" or (status == "classified" and contra == mine):
    if _merge_into_pair(
      session,
      other,
      payload,
      graph_id=graph_id,
      connection_id=connection_id,
      item_id=item_id,
      created_by=created_by,
      report=report,
    ):
      consumed.add(str(other.id))
      return True
    return False

  metadata = payload["metadata"]
  metadata["counterpart_event_id"] = str(other.id)
  metadata["counterpart_status"] = status
  metadata["classification_source"] = "counterpart"
  booked = status in POSTED_STATUSES | {"pending"} and contra == mine
  if not booked:
    return False
  if not capture_event(
    session, payload, graph_id=graph_id, created_by=created_by, report=report
  ):
    return True
  external_id = str(payload["external_id"])
  captured = existing_events(session, SOURCE, [external_id]).get(external_id)
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
    "Plaid leg %s voided: its other side %s is already posted",
    external_id,
    other.id,
  )
  return True


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
  errors_before = list(report.errors)
  try:
    with session.begin_nested():
      _delete_events(session, [str(waiting.id)])
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
