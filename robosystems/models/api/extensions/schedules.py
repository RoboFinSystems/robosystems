"""Schedule request and response models."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

# Mirrors the DB-level check constraint on `entries.type`:
#   CHECK (type IN ('standard','adjusting','closing','reversing'))
EntryType = Literal["standard", "adjusting", "closing", "reversing"]

# ── Requests ───────────────────────────────────────────────────────────────


class EntryTemplateRequest(BaseModel):
  debit_element_id: str = Field(
    ..., description="Element to debit (e.g., Depreciation Expense)"
  )
  credit_element_id: str = Field(
    ..., description="Element to credit (e.g., Accumulated Depreciation)"
  )
  entry_type: EntryType = Field(
    "closing", description="Entry type for generated entries"
  )
  memo_template: str = Field(
    "", description="Memo template ({structure_name} is replaced)"
  )
  auto_reverse: bool = Field(
    False,
    description="Auto-generate a reversing entry on the first day of the next period",
  )


class ScheduleMetadataRequest(BaseModel):
  method: str = Field("straight_line", description="Calculation method")
  original_amount: int = Field(0, description="Cost basis in cents")
  residual_value: int = Field(0, description="Salvage value in cents")
  useful_life_months: int = Field(0, description="Useful life in months")
  asset_element_id: str | None = Field(
    None, description="BS asset element for net book value"
  )


class CreateScheduleRequest(BaseModel):
  name: str = Field(..., description="Schedule name")
  taxonomy_id: str | None = Field(
    None, description="Taxonomy ID (auto-creates if omitted)"
  )
  element_ids: list[str] = Field(..., description="Element IDs to include")
  period_start: date = Field(..., description="First period start")
  period_end: date = Field(..., description="Last period end")
  monthly_amount: int = Field(..., description="Monthly amount in cents")
  entry_template: EntryTemplateRequest
  schedule_metadata: ScheduleMetadataRequest | None = None
  closed_through: date | None = Field(
    None,
    description=(
      "If provided, facts with period_end ≤ this date are flagged as "
      "'historical' (already reflected in opening balances, ignored by "
      "the close workflow). Used during initial ledger setup to create "
      "schedules whose early facts have already been captured elsewhere."
    ),
  )


class CreateClosingEntryRequest(BaseModel):
  posting_date: date = Field(..., description="Posting date for the entry")
  period_start: date = Field(..., description="Period start")
  period_end: date = Field(..., description="Period end")
  memo: str | None = Field(None, description="Override memo")


class ManualLineItemRequest(BaseModel):
  element_id: str = Field(..., description="Element ID (chart of accounts)")
  debit_amount: int = Field(0, ge=0, description="Debit in cents")
  credit_amount: int = Field(0, ge=0, description="Credit in cents")
  description: str | None = None


class CreateManualClosingEntryRequest(BaseModel):
  posting_date: date = Field(..., description="Posting date for the entry")
  memo: str = Field(
    ...,
    min_length=1,
    description="Memo describing the business event (e.g., 'Sale of computer to Vendor X on 3/15')",
  )
  line_items: list[ManualLineItemRequest] = Field(
    ..., min_length=1, description="Line items; must balance (total DR = total CR)"
  )
  entry_type: EntryType = Field(
    "closing",
    description="Entry type: 'closing' (default), 'adjusting', 'standard', 'reversing'",
  )


class TruncateScheduleRequest(BaseModel):
  new_end_date: date = Field(
    ...,
    description=(
      "New last-covered date for the schedule. Facts with period_start > "
      "this date are deleted (along with any stale draft entries they "
      "produced). Historical facts (already posted) are preserved."
    ),
  )
  reason: str = Field(
    ...,
    min_length=1,
    description="Required reason for the truncation (captured in audit log).",
  )


class TruncateScheduleResponse(BaseModel):
  structure_id: str
  new_end_date: date
  facts_deleted: int
  reason: str


# ── Responses ──────────────────────────────────────────────────────────────


class ScheduleSummaryResponse(BaseModel):
  structure_id: str
  name: str
  taxonomy_name: str
  entry_template: dict | None = None
  schedule_metadata: dict | None = None
  total_periods: int
  periods_with_entries: int


class ScheduleListResponse(BaseModel):
  schedules: list[ScheduleSummaryResponse]


class ScheduleFactResponse(BaseModel):
  element_id: str
  element_name: str
  value: float
  period_start: date
  period_end: date


class ScheduleFactsResponse(BaseModel):
  structure_id: str
  facts: list[ScheduleFactResponse]


class PeriodCloseItemResponse(BaseModel):
  structure_id: str
  structure_name: str
  amount: float
  status: str
  entry_id: str | None = None
  reversal_entry_id: str | None = None
  reversal_status: str | None = None


class PeriodCloseStatusResponse(BaseModel):
  fiscal_period_start: date
  fiscal_period_end: date
  period_status: str
  schedules: list[PeriodCloseItemResponse]
  total_draft: int
  total_posted: int


class ClosingEntryResponse(BaseModel):
  outcome: str = Field(
    ...,
    description=(
      "What the idempotent call did: "
      "'created' (new draft), 'unchanged' (existing draft still matches), "
      "'regenerated' (stale draft replaced with fresh one), "
      "'removed' (stale draft deleted; schedule no longer covers this period), "
      "'skipped' (nothing to do — no draft and no in-scope fact)."
    ),
  )
  entry_id: str | None = Field(
    None, description="The draft entry ID. None for 'removed' and 'skipped' outcomes."
  )
  status: str | None = Field(
    None, description="Entry status (always 'draft' when present)."
  )
  posting_date: date | None = None
  memo: str | None = None
  debit_element_id: str | None = None
  credit_element_id: str | None = None
  amount: float | None = Field(
    None, description="Entry amount in dollars. None for 'removed' and 'skipped'."
  )
  reason: str | None = Field(
    None,
    description="Explanation for 'removed' and 'skipped' outcomes.",
  )
  reversal: ClosingEntryResponse | None = None


ClosingEntryResponse.model_rebuild()


class ScheduleCreatedResponse(BaseModel):
  structure_id: str
  name: str
  taxonomy_id: str
  total_periods: int
  total_facts: int
