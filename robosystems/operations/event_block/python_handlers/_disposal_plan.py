"""Read-only disposal plan computation, shared by the asset_disposed handler
and its preview."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.extensions import Structure


class ScheduleNotFoundError(LookupError):
  """Raised when a schedule structure is not found by id."""

  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Schedule not found: {structure_id}")
    self.structure_id = structure_id


@dataclass
class DisposalPlan:
  """Computed disposal plan — ready to be posted, never persisted."""

  structure_id: str
  asset_element_id: str
  credit_element_id: str
  original_amount: int  # cents
  accumulated_depreciation: int  # cents
  nbv: int  # cents (original - accumulated)
  sale_proceeds: int  # cents
  gain_loss: int  # cents, positive = gain, negative = loss
  line_items: list[dict]  # 2-4 entries, balanced


def disposal_cutoff(disposal_date: date) -> date:
  """The last schedule month the disposal keeps: the month-end on or before
  the disposal date. The plan reads accumulated depreciation through it, so
  every month after it is no longer the schedule's to post."""
  if (disposal_date + timedelta(days=1)).month != disposal_date.month:
    return disposal_date
  return disposal_date.replace(day=1) - timedelta(days=1)


def compute_disposal_plan(
  session: Session,
  *,
  structure_id: str,
  disposal_date: date,
  sale_proceeds: int,
  proceeds_element_id: str | None,
  gain_loss_element_id: str | None,
) -> DisposalPlan:
  """Compute NBV, gain/loss and the balanced line items (cents).

  Raises ``ScheduleNotFoundError`` when ``structure_id`` isn't a schedule,
  and ``ValueError`` on missing schedule metadata or inconsistent inputs.
  """
  structure = session.execute(
    select(Structure).where(
      Structure.id == structure_id,
      Structure.block_type == "schedule",
    )
  ).scalar_one_or_none()
  if structure is None:
    raise ScheduleNotFoundError(structure_id)

  mechanics = structure.artifact_mechanics or {}
  sm = mechanics.get("schedule_metadata") or {}
  et = mechanics.get("entry_template") or {}

  # A terminated schedule re-anchors original_amount to what it recognized;
  # the asset's cost is what it started from.
  original_amount = int(
    sm.get("original_amount_at_start") or sm.get("original_amount") or 0
  )
  asset_element_id = sm.get("asset_element_id")
  credit_element_id = et.get("credit_element_id")  # accumulated depreciation

  if not asset_element_id:
    raise ValueError(
      "Disposal requires schedule_metadata.asset_element_id "
      "(the balance-sheet asset element). Update the schedule first — "
      "for prepaid-amortization schedules, this is the credited prepaid "
      "element itself (the same element as entry_template.credit_element_id)."
    )
  if not credit_element_id:
    raise ValueError("Disposal requires entry_template.credit_element_id.")

  # Latest instant fact on the credited element, months before the calendar
  # included (out-of-scope facts left by truncation would give a wrong NBV). For a contra account it is
  # the rising accumulated balance; for a self-carried (prepaid-style)
  # schedule, where the credited element is the asset, it is the declining
  # remaining balance.
  acc_row = session.execute(
    text(
      "SELECT value FROM facts "
      "WHERE structure_id = :sid AND element_id = :eid "
      "AND period_type = 'instant' AND period_end <= :d "
      "AND fact_scope IN ('in_scope', 'historical') "
      "ORDER BY period_end DESC LIMIT 1"
    ),
    {"sid": structure_id, "eid": credit_element_id, "d": disposal_date},
  ).fetchone()
  carried_dollars = float(acc_row.value) if acc_row else 0.0
  carried_cents = round(carried_dollars * 100)

  self_carried = asset_element_id == credit_element_id
  if self_carried:
    # Prepaid-style: the instant fact is the remaining balance = NBV.
    nbv = carried_cents
    accumulated_depreciation = max(original_amount - nbv, 0)
  else:
    # Without the cost there is no asset to remove, and the book value would
    # come out negative.
    if original_amount <= 0:
      raise ValueError(
        "Disposal requires schedule_metadata.original_amount (the asset's "
        "cost in cents). Update the schedule first."
      )
    accumulated_depreciation = carried_cents
    nbv = original_amount - accumulated_depreciation
  gain_loss = sale_proceeds - nbv

  if sale_proceeds > 0 and not proceeds_element_id:
    raise ValueError("proceeds_element_id is required when sale_proceeds > 0.")
  # Fully depreciated with proceeds is all gain: the account is needed
  # whenever proceeds differ from book value, or the entry cannot balance.
  if gain_loss != 0 and not gain_loss_element_id:
    raise ValueError(
      "gain_loss_element_id is required when the proceeds differ from the "
      "net book value (the disposal produces a gain or loss)."
    )

  if self_carried:
    if nbv == 0 and sale_proceeds == 0:
      raise ValueError(
        "Nothing to dispose: the schedule's remaining balance is zero "
        "and no proceeds were supplied."
      )
    # One credit at the remaining balance, not a DR/CR pair on one element.
    line_items: list[dict] = []
    if nbv > 0:
      line_items.append(
        {
          "element_id": asset_element_id,
          "debit_amount": 0,
          "credit_amount": nbv,
          "description": "Derecognize asset at remaining balance",
        }
      )
  else:
    line_items = [
      {
        "element_id": credit_element_id,
        "debit_amount": accumulated_depreciation,
        "credit_amount": 0,
        "description": "Remove accumulated depreciation",
      },
      {
        "element_id": asset_element_id,
        "debit_amount": 0,
        "credit_amount": original_amount,
        "description": "Remove asset at cost",
      },
    ]
  if sale_proceeds > 0 and proceeds_element_id:
    line_items.append(
      {
        "element_id": proceeds_element_id,
        "debit_amount": sale_proceeds,
        "credit_amount": 0,
        "description": "Sale proceeds",
      }
    )
  if gain_loss > 0 and gain_loss_element_id:
    line_items.append(
      {
        "element_id": gain_loss_element_id,
        "debit_amount": 0,
        "credit_amount": gain_loss,
        "description": "Gain on disposal",
      }
    )
  elif gain_loss < 0 and gain_loss_element_id:
    line_items.append(
      {
        "element_id": gain_loss_element_id,
        "debit_amount": abs(gain_loss),
        "credit_amount": 0,
        "description": "Loss on disposal",
      }
    )

  # A first-month disposal has no accumulated depreciation to remove; the
  # entry takes no zero line.
  line_items = [li for li in line_items if li["debit_amount"] or li["credit_amount"]]
  debits = sum(li["debit_amount"] for li in line_items)
  credits = sum(li["credit_amount"] for li in line_items)
  if debits != credits:
    raise ValueError(
      f"The disposal entry does not balance (debits {debits}, credits "
      f"{credits}); check the schedule's original_amount and asset_element_id."
    )

  return DisposalPlan(
    structure_id=structure_id,
    asset_element_id=asset_element_id,
    credit_element_id=credit_element_id,
    original_amount=original_amount,
    accumulated_depreciation=accumulated_depreciation,
    nbv=nbv,
    sale_proceeds=sale_proceeds,
    gain_loss=gain_loss,
    line_items=line_items,
  )
