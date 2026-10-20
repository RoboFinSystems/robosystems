"""What a payment settles: the settlement link and its allocations, checked on
every write.

A payment settles invoices when money comes in and bills when it goes out:
``payment_received`` and ``bill_paid`` say so by type, a bank line by the sign
of its amount. The documents must be the payment's own entity's and not
retracted. A different counterparty is allowed, since a parent can pay a
subsidiary's bill. Allocations may not apply more than the payment's
magnitude; an overpayment shows as unapplied credit on the document instead.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import (
  OBLIGATION_EVENT_TYPES,
  DischargeAllocation,
)
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_discharge import EventDischarge

_SETTLES_BY_TYPE: dict[str, str] = {
  "payment_received": "invoice_issued",
  "bill_paid": "bill_received",
}

_RETRACTED = ("voided", "superseded")


class DischargeRefusedError(ValueError):
  """A settlement the payment cannot make."""


def settles_type(payment: Event) -> str | None:
  """The obligation type ``payment`` settles: an invoice for money in, a bill
  for money out. None for a payment with no amount to say which."""
  by_type = _SETTLES_BY_TYPE.get(str(payment.event_type))
  if by_type is not None:
    return by_type
  if not payment.amount:
    return None
  return "bill_received" if payment.amount < 0 else "invoice_issued"


def check_settlement(
  session: Session, payment: Event, obligation_ids: Sequence[str]
) -> None:
  """Refuse a settlement ``payment`` cannot make, with the reason."""
  if str(payment.event_type) in OBLIGATION_EVENT_TYPES:
    raise DischargeRefusedError(
      f"A {payment.event_type} is an obligation; only a payment settles one."
    )
  if len(set(obligation_ids)) != len(obligation_ids):
    raise DischargeRefusedError("Each obligation may be named once.")
  found = {
    str(e.id): e
    for e in session.execute(
      select(Event).where(Event.id.in_(list(obligation_ids)))
    ).scalars()
  }
  expected = settles_type(payment)
  for obligation_id in obligation_ids:
    target = found.get(obligation_id)
    if target is None or target.id == payment.id:
      raise DischargeRefusedError(f"No obligation {obligation_id!r} to settle.")
    if target.event_type not in OBLIGATION_EVENT_TYPES:
      raise DischargeRefusedError(
        f"{obligation_id} is a {target.event_type}, not an invoice_issued or a "
        "bill_received."
      )
    if expected is not None and target.event_type != expected:
      direction = "in" if expected == "invoice_issued" else "out"
      raise DischargeRefusedError(
        f"Money {direction} settles a {expected}; {obligation_id} is a "
        f"{target.event_type}."
      )
    if target.status in _RETRACTED:
      raise DischargeRefusedError(f"{obligation_id} is {target.status}.")
    if target.entity_id != payment.entity_id:
      raise DischargeRefusedError(
        f"{obligation_id} is on another entity's books; a payment settles its "
        "own entity's documents."
      )


def _clear_allocations(session: Session, payment: Event) -> None:
  session.execute(
    delete(EventDischarge).where(EventDischarge.payment_event_id == payment.id)
  )


def set_settlement_link(session: Session, payment: Event, obligation_id: str) -> None:
  """Point ``payment`` at one obligation, settled at the payment's full
  magnitude; an empty id clears the link. Either replaces its allocations."""
  if obligation_id:
    check_settlement(session, payment, [obligation_id])
  _clear_allocations(session, payment)
  payment.discharges_event_id = obligation_id or None


def allocate(
  session: Session,
  payment: Event,
  allocations: Sequence[DischargeAllocation],
  created_by: str,
) -> None:
  """Replace what ``payment`` settles with ``allocations``; an empty list
  clears it. The settlement link names the first."""
  if not allocations:
    _clear_allocations(session, payment)
    payment.discharges_event_id = None
    return
  check_settlement(session, payment, [a.obligation_event_id for a in allocations])
  applied = sum(a.amount for a in allocations)
  magnitude = abs(payment.amount or 0)
  if applied > magnitude:
    raise DischargeRefusedError(
      f"The allocations apply {applied} cents, more than the payment's {magnitude}."
    )
  _clear_allocations(session, payment)
  session.add_all(
    EventDischarge(
      payment_event_id=payment.id,
      obligation_event_id=a.obligation_event_id,
      amount=a.amount,
      created_by=created_by,
    )
    for a in allocations
  )
  payment.discharges_event_id = allocations[0].obligation_event_id
