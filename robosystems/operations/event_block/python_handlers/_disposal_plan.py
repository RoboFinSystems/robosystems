"""Pure compute helper for asset disposal.

Extracted from `operations/roboledger/commands/schedules.py::dispose_schedule`
so both the event-driven disposal handler (`asset_disposed.py`) and the
preview path can call the same read-and-compute logic without duplicating it.

Read-only — does not flush, commit, or write any rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

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


def compute_disposal_plan(
  session: Session,
  *,
  structure_id: str,
  disposal_date: date,
  sale_proceeds: int,
  proceeds_element_id: str | None,
  gain_loss_element_id: str | None,
) -> DisposalPlan:
  """Read schedule + accumulated depreciation, compute NBV/gain/loss, build line items.

  Does not write. Safe to call from a preview path.

  Raises:
    ScheduleNotFoundError: if structure_id is not a schedule.
    ValueError: if required metadata fields are missing or the disposal is
                internally inconsistent (proceeds > 0 without proceeds_element_id, etc).
  """
  structure = session.execute(
    select(Structure).where(
      Structure.id == structure_id,
      Structure.structure_type == "schedule",
    )
  ).scalar_one_or_none()
  if structure is None:
    raise ScheduleNotFoundError(structure_id)

  mechanics = structure.artifact_mechanics or {}
  sm = mechanics.get("schedule_metadata") or {}
  et = mechanics.get("entry_template") or {}

  original_amount = int(sm.get("original_amount") or 0)
  asset_element_id = sm.get("asset_element_id")
  credit_element_id = et.get("credit_element_id")  # accumulated depreciation

  if not asset_element_id:
    raise ValueError(
      "Disposal requires schedule_metadata.asset_element_id "
      "(the balance-sheet asset element). Update the schedule first."
    )
  if not credit_element_id:
    raise ValueError("Disposal requires entry_template.credit_element_id.")

  # Accumulated depreciation = most recent cumulative instant fact up to disposal_date
  acc_row = session.execute(
    text(
      "SELECT value FROM facts "
      "WHERE structure_id = :sid AND element_id = :eid "
      "AND period_type = 'instant' AND period_end <= :d "
      "ORDER BY period_end DESC LIMIT 1"
    ),
    {"sid": structure_id, "eid": credit_element_id, "d": disposal_date},
  ).fetchone()
  accumulated_dollars = float(acc_row.value) if acc_row else 0.0
  accumulated_depreciation = round(accumulated_dollars * 100)

  nbv = original_amount - accumulated_depreciation
  gain_loss = sale_proceeds - nbv

  if sale_proceeds > 0 and not proceeds_element_id:
    raise ValueError("proceeds_element_id is required when sale_proceeds > 0.")
  if nbv != 0 and gain_loss != 0 and not gain_loss_element_id:
    raise ValueError(
      "gain_loss_element_id is required when net book value > 0 and "
      "the disposal produces a gain or loss."
    )

  line_items: list[dict] = [
    # DR accumulated depreciation (remove the contra account)
    {
      "element_id": credit_element_id,
      "debit_amount": accumulated_depreciation,
      "credit_amount": 0,
      "description": "Remove accumulated depreciation",
    },
    # CR asset at cost
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
