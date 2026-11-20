"""Reconciling a bank line the feed reports again — shared by every bank feed.

An unposted event (captured or classified) is edited or deleted in place. A
posted one is the customer's books and is never touched: it is flagged as a
reconciling item (``payload_drift``), and the stashed payload carries what
the bank now says — a changed amount or date, or a retraction.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, cast

from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Event
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)

UNPOSTED_STATUSES = frozenset({"captured", "classified"})
POSTED_STATUSES = frozenset({"committed", "pending", "fulfilled"})
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


def flag_removed(
  event: Event, transaction_ids: list[str], *, released: list[str] | None = None
) -> None:
  """A posted line the bank retracted: a reconciling item, the books untouched.

  The stashed payload carries no entry, so the reconciling-item plan nets the
  posted entry against nothing — its catch-up is the reversal. ``released``
  names a pair's leg that lives on as its own line: it leaves the pair's
  ``legs``, in the stashed payload and on the row, so the pair never claims
  it again.
  """
  accepted = accepted_payload(event)
  accepted["source_removed"] = True
  accepted["source_removed_transaction_ids"] = transaction_ids
  if released:
    release_legs(event, accepted, released)
  flag(event, accepted)
  if released:
    event.metadata_ = {
      **(event.metadata_ or {}),
      "legs": accepted["legs"],
      "released_legs": accepted["released_legs"],
    }


def release_legs(event: Event, accepted: dict[str, Any], released: list[str]) -> None:
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


def flag_changed(event: Event, payload: dict[str, Any]) -> None:
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
  accepted = accepted_payload(event)
  accepted["source_amount"] = amount
  accepted["source_posted_date"] = posted_date
  accepted.update(planned_entry(event, amount, posted_date))
  flag(event, accepted)


def planned_entry(event: Event, amount: int, posted_date: str) -> dict[str, Any]:
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
  except HandlerMetadataValidationError, ValidationError:
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


def accepted_payload(event: Event) -> dict[str, Any]:
  """The live metadata less the bookkeeping and the previous flag's verdict."""
  return {
    key: value
    for key, value in (event.metadata_ or {}).items()
    if key not in DRIFT_BOOKKEEPING_KEYS and key not in SOURCE_STATE_KEYS
  }


def flag(event: Event, accepted: dict[str, Any]) -> None:
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
  except HandlerMetadataValidationError, ValidationError:
    return None
  if allocations and len(allocations) == 1:
    return str(allocations[0].element_id)
  return None


def delete_unposted(session: Session, event_ids: list[str]) -> list[str]:
  """Delete the events still unposted; returns the ids deleted. A line that
  has been posted owns ledger rows and is never deleted."""
  unposted = (Event.id.in_(event_ids), Event.status.in_(sorted(UNPOSTED_STATUSES)))
  session.execute(
    delete(event_dimensions).where(
      event_dimensions.c.event_id.in_(select(Event.id).where(*unposted))
    )
  )
  return [
    str(event_id)
    for event_id in session.execute(
      delete(Event).where(*unposted).returning(Event.id)
    ).scalars()
  ]


def parse_event_time(value: str) -> datetime:
  """An event timestamp as the naive UTC the events table stores."""
  parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
  if parsed.tzinfo is not None:
    parsed = parsed.astimezone(UTC).replace(tzinfo=None)
  return parsed


def event_iso(value: datetime | None) -> str:
  if value is None:
    return datetime.now(UTC).strftime("%Y-%m-%dT00:00:00Z")
  return value.strftime("%Y-%m-%dT%H:%M:%SZ")
