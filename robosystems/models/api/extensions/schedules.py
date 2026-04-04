"""Schedule request and response models."""

from datetime import date

from pydantic import BaseModel, Field

# ── Requests ───────────────────────────────────────────────────────────────


class EntryTemplateRequest(BaseModel):
  debit_element_id: str = Field(
    ..., description="Element to debit (e.g., Depreciation Expense)"
  )
  credit_element_id: str = Field(
    ..., description="Element to credit (e.g., Accumulated Depreciation)"
  )
  entry_type: str = Field("closing", description="Entry type for generated entries")
  memo_template: str = Field(
    "", description="Memo template ({structure_name} is replaced)"
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


class CreateClosingEntryRequest(BaseModel):
  posting_date: date = Field(..., description="Posting date for the entry")
  period_start: date = Field(..., description="Period start")
  period_end: date = Field(..., description="Period end")
  memo: str | None = Field(None, description="Override memo")


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


class PeriodCloseStatusResponse(BaseModel):
  fiscal_period_start: date
  fiscal_period_end: date
  period_status: str
  schedules: list[PeriodCloseItemResponse]
  total_draft: int
  total_posted: int


class ClosingEntryResponse(BaseModel):
  entry_id: str
  status: str
  posting_date: date
  memo: str
  debit_element_id: str
  credit_element_id: str
  amount: float


class ScheduleCreatedResponse(BaseModel):
  structure_id: str
  name: str
  taxonomy_id: str
  total_periods: int
  total_facts: int
