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
    ...,
    description=(
      "CoA element id to debit (e.g. Depreciation Expense). This is a "
      "chart-of-accounts element id — the `id` returned by "
      "get-unmapped-elements / get-graph-schema — NOT a taxonomy qname."
    ),
  )
  credit_element_id: str = Field(
    ...,
    description=(
      "CoA element id to credit (e.g. Accumulated Depreciation). A "
      "chart-of-accounts element id (see get-unmapped-elements), not a "
      "taxonomy qname. One template = one debit/credit pair; model a "
      "multi-account entry as several schedules."
    ),
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
  method: str = Field(
    "straight_line",
    description=(
      "Calculation method. 'straight_line' (default) distributes "
      "`monthly_amount` evenly across periods with the final period "
      "absorbing rounding. 'custom' requires `periodic_amounts` — the "
      "generator uses those values verbatim instead of computing. Other "
      "strings are labels only; fact values still come from "
      "`monthly_amount` or `periodic_amounts`."
    ),
  )
  original_amount: int = Field(0, description="Cost basis in cents")
  residual_value: int = Field(0, description="Salvage value in cents")
  useful_life_months: int = Field(0, description="Useful life in months")
  asset_element_id: str | None = Field(
    None, description="BS asset element for net book value"
  )
  periodic_amounts: list[int] | None = Field(
    None,
    description=(
      "Explicit per-period amounts in cents. When set, the generator "
      "uses these values instead of `monthly_amount` — enabling "
      "non-straight-line schedules (effective-interest bond discount "
      "amortization, day-count interest accrual, variable lease "
      "payments, pre-computed effective-yield curves, etc.). Length must "
      "match the number of monthly periods between `period_start` and "
      "`period_end`; sum must equal `original_amount` exactly. The "
      "auto-generated SumEquals rule proves Σ = original regardless of "
      "the curve shape."
    ),
  )


class CreateScheduleRequest(BaseModel):
  name: str = Field(..., description="Schedule name")
  taxonomy_id: str | None = Field(
    None, description="Taxonomy ID (auto-creates if omitted)"
  )
  element_ids: list[str] = Field(
    ...,
    description=(
      "CoA element ids the schedule touches (the `id` from "
      "get-unmapped-elements, not taxonomy qnames) — typically the same "
      "debit + credit ids used in entry_template."
    ),
  )
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
  source_transaction_id: str | None = Field(
    None,
    description=(
      "Free-form reference to the originating GL transaction (e.g. an "
      "import ID, ledger entry ID, or external system key). Stored in "
      "artifact_mechanics for audit; no FK constraint."
    ),
  )


class PromoteObligationsRequest(BaseModel):
  """On-demand trigger for the obligation-promotion sweep.

  Mirrors what the ``scheduled_obligation_promoter`` Dagster sensor does
  on its tick, but lets an interactive caller or an MCP close co-pilot
  run it now instead of waiting for the background cadence — required to
  drive a schedule-driven close to completion in a single session.
  Flips matured ``pending`` ``schedule_entry_due`` events (period boundary
  passed) to ``classified``; with ``dispatch_handlers`` it also drafts the
  closing entries in the same transaction (idempotent — reconciles to an
  existing draft).
  """

  dispatch_handlers: bool = Field(
    True,
    description=(
      "When True (default), also fire the schedule_entry_due handler for "
      "each promoted obligation so the draft closing entry materializes "
      "immediately (autopilot). When False, flip status only (co-pilot) — "
      "the draft is created separately."
    ),
  )


class PromoteObligationsResponse(BaseModel):
  """Counts from a single on-demand promotion sweep."""

  classified_count: int = Field(
    ..., description="Matured obligations flipped pending → classified."
  )
  dispatched_count: int = Field(
    ..., description="Obligations whose closing entry was drafted this run."
  )
  error_count: int = Field(
    ..., description="Per-obligation handler errors (non-fatal)."
  )
  classified_event_ids: list[str] = Field(default_factory=list)
  errors: list[dict[str, str]] = Field(
    default_factory=list,
    description="Per-obligation errors as {event_id, error}; the sweep continues past them.",
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


class CreateClosingEntryOperation(CreateClosingEntryRequest):
  """CQRS-shaped body for `POST /operations/create-closing-entry`.

  `structure_id` moves into the body so REST + MCP share a single body
  type via the registrar.
  """

  structure_id: str = Field(
    ..., description="Schedule structure the closing entry is derived from."
  )


# ── Responses ──────────────────────────────────────────────────────────────


class PeriodCloseItemResponse(BaseModel):
  """One schedule's contribution to a period close — drafted closing
  entry plus its reversal (when ``auto_reverse=True``).

  ``status`` is the closing entry's draft/posted lifecycle. The
  reversal mirrors the same shape with ``reversal_*`` fields.
  """

  structure_id: str
  structure_name: str
  amount: float
  status: str
  entry_id: str | None = None
  reversal_entry_id: str | None = None
  reversal_status: str | None = None


class PeriodCloseStatusResponse(BaseModel):
  """Period-close dashboard view — every schedule in scope for the
  period plus drafted/posted entry totals.

  Use to drive the close-period UI: schedules with ``status='draft'``
  are pending close; ``period_status`` reflects the calendar's lock
  state for the period.
  """

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
  rule_summary: dict[str, int] | None = None
  # Every schedule is backed by an event chain. Callers can use
  # `schedule_created_event_id` as the obligation-register handle and
  # `pending_event_count` as a quick sanity check that materialization
  # produced one event per period.
  schedule_created_event_id: str | None = None
  pending_event_count: int = 0


# ── Update / delete ──────────────────────────────────────────────────────


class UpdateScheduleRequest(BaseModel):
  """Update mutable fields on a schedule.

  Editable: name, entry_template, schedule_metadata (all live on the
  Structure row / its metadata_ JSONB column).

  NOT editable via this op: period_start, period_end, monthly_amount.
  Those require fact regeneration — fire an event block that terminates
  the schedule (e.g., `asset_disposed`) and create a fresh schedule via
  `create-schedule`.

  Omitted fields are left unchanged.
  """

  structure_id: str
  name: str | None = None
  entry_template: EntryTemplateRequest | None = None
  schedule_metadata: ScheduleMetadataRequest | None = None


class DeleteScheduleRequest(BaseModel):
  """Delete a schedule — cascades through facts and associations.

  Hard deletes the Structure, all Facts tied to it, and all
  Associations tied to it. This is a permanent, irreversible
  operation. For ending a schedule early without removing history,
  fire `create-event-block(event_type='asset_disposed')` instead — the
  handler truncates the schedule + posts the disposal entry atomically.
  """

  structure_id: str


# Asset disposal is an event block:
# `create-event-block(event_type='asset_disposed')`. See
# operations/event_block/python_handlers/asset_disposed.py for the
# metadata schema (AssetDisposedMetadata).
