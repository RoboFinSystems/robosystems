"""Schedule request and response models."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Mirrors the DB-level check constraint on `entries.type`:
#   CHECK (type IN ('standard','adjusting','closing','reversing'))
EntryType = Literal["standard", "adjusting", "closing", "reversing"]

# The columns of a loan schedule's amortization table a leg can post.
CurveColumn = Literal["interest", "interest_paid", "principal", "payment"]
DayCount = Literal["30/360", "actual/365", "actual/360"]
PaymentFrequency = Literal["monthly", "quarterly", "annual", "at_maturity"]

# ── Requests ───────────────────────────────────────────────────────────────


class ScheduleLegRequest(BaseModel):
  """One line of the entry a loan schedule drafts each period."""

  element_id: str = Field(..., description="CoA element id the line posts to.")
  side: Literal["debit", "credit"] = Field(..., description="The line's side.")
  amount_source: CurveColumn = Field(
    ...,
    description=(
      "The amortization-table column the line takes each period: `interest` "
      "(accrued in the period), `interest_paid` (accrued interest settled by "
      "the period's payment), `principal` (principal repaid), `payment` "
      "(interest_paid + principal)."
    ),
  )


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
      "taxonomy qname. A straight-line or custom schedule posts this one "
      "debit/credit pair; a loan schedule posts its `legs`."
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
  dimension_ids: list[str] = Field(
    default_factory=list,
    description=(
      "The management dimension members (department, class, location, "
      "project) every entry the schedule drafts carries on both lines, one "
      "per axis: a department's depreciation lands in that department."
    ),
  )
  legs: list[ScheduleLegRequest] = Field(
    default_factory=list,
    description=(
      "The lines of a loan schedule's entry (`method='effective_interest'` "
      "only), each taking one column of the amortization table; each "
      "period's lines must balance. Accrue interest and pay through the "
      "bank: debit Interest Expense and credit Accrued Interest, both "
      "`interest`. The schedule settles the payment itself: debit the note "
      "`principal`, debit Interest Expense `interest`, credit cash or a "
      "due-to `payment`. `debit_element_id` names the interest debit leg "
      "and `credit_element_id` one credit leg. Empty for every other "
      "method, which posts the debit/credit pair."
    ),
  )


class LoanTermsRequest(BaseModel):
  """The terms a loan schedule's amortization table is computed from."""

  principal: int = Field(..., gt=0, description="Principal drawn, in cents.")
  annual_rate: float = Field(
    ...,
    ge=0,
    lt=1,
    description="Annual interest rate as a fraction: 0.099 for 9.9%.",
  )
  start_date: date = Field(
    ...,
    description=(
      "The day the principal was drawn; interest accrues from it. Falls in "
      "the schedule's first month."
    ),
  )
  day_count: DayCount = Field(
    "30/360",
    description=(
      "How a period's interest counts days: `30/360` (every whole month is "
      "30 days), `actual/365` or `actual/360` (the days elapsed, over a "
      "365- or 360-day year; actual/360 is the usual bank line of credit)."
    ),
  )
  payment_frequency: PaymentFrequency = Field(
    "monthly",
    description=(
      "How often a payment falls due. A payment is applied at the end of "
      "the month it falls in."
    ),
  )
  first_payment_date: date | None = Field(
    None,
    description=(
      "The first payment's date. Omit for one frequency after the "
      "schedule's first month."
    ),
  )
  payment_amount: int | None = Field(
    None,
    gt=0,
    description=(
      "Each scheduled payment in cents, interest included. Omit for the "
      "level payment that retires the principal by `maturity_date`, or for "
      "the accrued interest when `interest_only`."
    ),
  )
  interest_only: bool = Field(
    False,
    description="Payments settle interest only; the principal is due at maturity.",
  )
  maturity_date: date | None = Field(
    None,
    description=(
      "When the remaining principal is due: it is repaid in that month, "
      "which is the schedule's last. Omit for a line of credit with no "
      "maturity, whose balance carries past the schedule."
    ),
  )
  capitalize_interest: bool = Field(
    False,
    description=(
      "Interest is added to the note rather than accrued apart (a PIK "
      "note): the balance grows by each period's interest, and payments "
      "reduce it whole."
    ),
  )
  note_element_id: str = Field(
    ...,
    description=(
      "The liability account the principal is carried in. The schedule "
      "states its balance each period, which the schedule reconciliation "
      "compares with the ledger."
    ),
  )
  current_portion_element_id: str | None = Field(
    None,
    description=(
      "A current-liability account for the principal due within twelve "
      "months. When set, each period's entry also reclasses between it and "
      "`note_element_id` so it holds the next twelve months' principal."
    ),
  )


class ScheduleMetadataRequest(BaseModel):
  method: str = Field(
    "straight_line",
    description=(
      "Calculation method. 'straight_line' (default) distributes "
      "`monthly_amount` evenly across periods with the final period "
      "absorbing rounding. 'custom' requires `periodic_amounts` — the "
      "generator uses those values verbatim instead of computing. "
      "'effective_interest' requires `loan_terms` and the template's `legs`: "
      "the generator computes the amortization table (opening balance, "
      "interest, payment, principal, closing balance) and drafts the legs "
      "from it. Other strings are labels only; fact values still come from "
      "`monthly_amount` or `periodic_amounts`."
    ),
  )
  loan_terms: LoanTermsRequest | None = Field(
    None, description="The loan's terms, for `method='effective_interest'`."
  )
  original_amount: int = Field(0, description="Cost basis in cents")
  residual_value: int = Field(
    0,
    description=(
      "Salvage value in cents. The schedule expenses `original_amount` less "
      "this; net book value ends at it."
    ),
  )
  useful_life_months: int = Field(0, description="Useful life in months")
  asset_element_id: str | None = Field(
    None, description="BS asset element for net book value"
  )
  booked_on: date | None = Field(
    None,
    description=(
      "The date the cost went on the books, when that is before the "
      "schedule's first period: a policy paid in December that starts "
      "amortizing in January, or an asset bought the month before it is "
      "placed in service. From this date until the first period the "
      "schedule carries its full cost, so the schedule reconciliation does "
      "not report that balance as unscheduled. Leave it out when the cost "
      "is booked in the first period."
    ),
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
      "`period_end`; sum must equal `original_amount` less "
      "`residual_value` exactly. The auto-generated SumEquals rule proves "
      "that total regardless of the curve shape."
    ),
  )


# Fifty years of monthly facts. Each month costs memory while the schedule is
# built, so an unbounded range let one small request exhaust the API.
MAX_SCHEDULE_MONTHS = 600


class CreateScheduleRequest(BaseModel):
  name: str = Field(..., description="Schedule name")
  entity_id: str | None = Field(
    None,
    description=(
      "The entity whose books the schedule belongs to, by id. Omit for the "
      "group parent. Its facts, obligations and closing entries land in "
      "that entity's books, against that entity's own accounts."
    ),
  )
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
  monthly_amount: int = Field(
    0,
    description=(
      "Monthly amount in cents. Required for a straight-line schedule; a "
      "custom or loan schedule takes its amounts from its curve."
    ),
  )
  entry_template: EntryTemplateRequest
  schedule_metadata: ScheduleMetadataRequest | None = None
  closed_through: date | None = Field(
    None,
    description=(
      "Watermark for onboarding. Facts with period_end ≤ this date are "
      "flagged 'historical' and their schedule_entry_due obligations are "
      "emitted 'voided', so the close workflow starts drafting at the first "
      "open period. Defaults to the calendar's history end: the last day of "
      "its closed_through month (calendar '2026-05' → '2026-05-31'), or the "
      "day before its first month when nothing has closed yet. An earlier "
      "date is raised to it, since an obligation there could never post; "
      "set a later one only to treat more months as already booked."
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

  @model_validator(mode="after")
  def _bounded_range(self) -> CreateScheduleRequest:
    if self.period_end < self.period_start:
      raise ValueError("period_end must not be before period_start")
    months = (self.period_end.year - self.period_start.year) * 12 + (
      self.period_end.month - self.period_start.month
    )
    if months >= MAX_SCHEDULE_MONTHS:
      raise ValueError(
        f"A schedule can span at most {MAX_SCHEDULE_MONTHS} months (got {months + 1})."
      )
    method = (
      self.schedule_metadata.method if self.schedule_metadata else "straight_line"
    )
    if method == "straight_line" and "monthly_amount" not in self.model_fields_set:
      raise ValueError("A straight-line schedule needs monthly_amount.")
    return self


class PreviewScheduleRequest(CreateScheduleRequest):
  """A schedule's definition, generated without being kept."""

  replaces_structure_id: str | None = Field(
    None,
    description=(
      "The schedule this definition would replace, when editing or "
      "recreating one: the preview then says what its posted entries mean "
      "for the change."
    ),
  )


class ScheduleRollForwardRow(BaseModel):
  """One period of a previewed schedule: the balance it carries, and what
  moves it."""

  period_start: date
  period_end: date
  opening: float = Field(..., description="The carried balance at the period's start.")
  amount: float = Field(
    ..., description="What the period recognizes: the entry's debit amount."
  )
  closing: float = Field(..., description="The carried balance at the period's end.")
  scope: Literal["historical", "in_scope"] = Field(
    ...,
    description=(
      "`historical` for a month already in the books' history, which the "
      "schedule records but never drafts; `in_scope` for one a close drafts."
    ),
  )
  interest: float | None = Field(None, description="Loan schedules: interest accrued.")
  payment: float | None = Field(None, description="Loan schedules: the payment.")
  principal: float | None = Field(None, description="Loan schedules: principal repaid.")
  current_portion: float | None = Field(
    None,
    description=(
      "Loan schedules with a current-portion account: the principal due in "
      "the next twelve months."
    ),
  )


class ScheduleReplacementSummary(BaseModel):
  """What an existing schedule's entries mean for replacing it."""

  structure_id: str
  name: str
  posted_entry_count: int = Field(
    ..., description="Its posted or shadowed closing entries."
  )
  posted_periods: list[str] = Field(
    default_factory=list, description="The months those entries are in (YYYY-MM)."
  )
  draft_entry_count: int = 0
  path: Literal["rebuild", "terminate_and_succeed"] = Field(
    ...,
    description=(
      "`rebuild` when nothing has posted: rebuild-schedule with the new "
      "definition replaces it in place. `terminate_and_succeed` once an "
      "entry has: terminate it after its last posted month and create the "
      "successor from the month after."
    ),
  )


class PromoteObligationsRequest(BaseModel):
  """On-demand trigger for the obligation-promotion sweep.

  Mirrors what the `scheduled_obligation_promoter` Dagster sensor does
  on its tick, but lets an interactive caller or an MCP close co-pilot
  run it now instead of waiting for the background cadence — required to
  drive a schedule-driven close to completion in a single session.
  Flips matured `pending` `schedule_entry_due` events (period boundary
  passed) to `classified`; with `dispatch_handlers` it also drafts the
  closing entries in the same transaction (idempotent — reconciles to an
  existing draft). The sweep also reaches *stranded* obligations —
  already `classified` (by an earlier flip-only sweep) but with no
  closing entry ever drafted — dispatching them in the same pass.
  """

  # A body that names none of this command's fields is not a request for it.
  model_config = ConfigDict(extra="forbid")

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
  stranded_count: int = Field(
    0,
    description=(
      "Matured obligations found already at 'classified' with no drafted "
      "closing entry. With dispatch_handlers=true they were drafted this "
      "run (included in dispatched_count); with dispatch_handlers=false "
      "they still have no draft — re-run with dispatch_handlers=true or "
      "void them."
    ),
  )
  classified_event_ids: list[str] = Field(default_factory=list)
  stranded_event_ids: list[str] = Field(
    default_factory=list,
    description="Event ids of the stranded obligations found this sweep.",
  )
  errors: list[dict[str, str]] = Field(
    default_factory=list,
    description="Per-obligation errors as {event_id, error}; the sweep continues past them.",
  )


# ── Responses ──────────────────────────────────────────────────────────────


class PeriodCloseItemResponse(BaseModel):
  """One schedule's contribution to a period close — drafted closing
  entry plus its reversal (when `auto_reverse=True`).

  `status` is the closing entry's draft/posted lifecycle (`shadowed` when a
  shadow close took the draft as an expectation and posted nothing). The
  reversal mirrors the same shape with `reversal_*` fields.
  """

  structure_id: str
  structure_name: str
  amount: float
  status: str
  entry_id: str | None = None
  reversal_entry_id: str | None = None
  reversal_status: str | None = None


class CloseReceiptResponse(BaseModel):
  """What a close actually did, stamped on the period row that close locked.

  The close result used to exist only in the HTTP response, so a close
  that SUCCEEDED but whose transport failed left no record and the
  operator reconstructed it from separate state reads. This is that
  record. Written in the same transaction as the period's `status='closed'`
  flip, so a closed period and its receipt can never disagree.

  Typed rather than a free dict because it is a contract: the GraphQL
  schema and both SDKs derive from it, and an opaque blob would push the
  shape onto every reader to rediscover.
  """

  version: int = Field(
    ...,
    description=(
      "Receipt schema version; 1 is the first shipped shape, 2 adds the shadow fields"
    ),
  )
  period: str = Field(..., description="Period this receipt is for (YYYY-MM)")
  closed_at: datetime
  closed_by: str = Field(..., description="Actor id that ran the close")
  actor_type: str = Field(
    ..., description="'user' for REST callers, 'agent' for MCP-driven closes"
  )
  was_reclose: bool = Field(
    False, description="Whether this close re-closed a previously closed period"
  )
  entries_posted: int = Field(
    0,
    description=(
      "Total entries the close transitioned to posted, across both post "
      "paths — the sum of the two fields below"
    ),
  )
  entries_published_to_qb: int = 0
  entries_posted_locally: int = 0
  target_auto_advanced: bool = False
  rule_summary: dict[str, int] | None = Field(
    None, description="Schedule rule outcomes: pass/fail/error/skipped counts"
  )
  evaluated_structure_ids: list[str] = Field(default_factory=list)
  statements_stamped: bool = False
  statement_stamp_note: str | None = None
  stamped_statement_sets: dict[str, str] = Field(
    default_factory=dict, description="structure_id -> minted fact_set_id"
  )
  statement_rule_summary: dict[str, int] | None = None
  shadow: bool = Field(
    False,
    description=(
      "The close ran in shadow: nothing written to QuickBooks, nothing "
      "posted, the drafts shadowed, the gates recorded as findings"
    ),
  )
  entries_shadowed: int = 0
  gate_findings: list[str] = Field(
    default_factory=list,
    description="Blocker codes a shadow close recorded instead of enforcing",
  )
  gate_finding_counts: dict[str, int] | None = Field(
    None, description="Each finding's size: days stale, or a count"
  )


class PeriodCloseStatusResponse(BaseModel):
  """Period-close dashboard view — every schedule in scope for the
  period plus drafted/posted entry totals.

  Use to drive the close-period UI: schedules with `status='draft'`
  are pending close; `period_status` reflects the calendar's lock
  state for the period.
  """

  fiscal_period_start: date
  fiscal_period_end: date
  period_status: str
  schedules: list[PeriodCloseItemResponse]
  total_draft: int
  total_posted: int
  close_receipt: CloseReceiptResponse | None = Field(
    None,
    description=(
      "Receipt stamped by the close that locked this period: entries_posted, "
      "the entries_published_to_qb / entries_posted_locally split, statement "
      "stamping and rule summaries, closed_at/closed_by. Null while the "
      "period is open, and null for periods closed before receipts shipped "
      "(a closed period without a receipt is not a failed close). This is "
      "the authoritative record of what a close did — read it rather than "
      "retrying when a close's response was lost in transport."
    ),
  )


class ScheduleCreatedResponse(BaseModel):
  structure_id: str
  name: str
  taxonomy_id: str
  total_periods: int
  total_facts: int
  rule_summary: dict[str, int] | None = None
  # `pending_event_count` should equal the number of periods materialized.
  schedule_created_event_id: str | None = None
  pending_event_count: int = 0
  warnings: list[str] = Field(
    default_factory=list,
    description=(
      "What the write went ahead with that deserves a look: another active "
      "schedule posting to the same accounts over the same months."
    ),
  )


# ── Update / delete ──────────────────────────────────────────────────────


class UpdateScheduleRequest(BaseModel):
  """Update mutable fields on a schedule.

  Editable: name, entry_template, schedule_metadata (all live on the
  Structure row / its metadata_ JSONB column).

  NOT editable via this op: period_start, period_end, monthly_amount.
  Those require fact regeneration — end the schedule early via
  `terminate-schedule` (no entry) or
  `create-event-block(event_type='asset_disposed')` (with a disposal
  entry), then create a fresh schedule via `create-information-block`
  (`block_type='schedule'`).

  Omitted fields are left unchanged, and that holds inside
  `schedule_metadata` too: name only the fields to change.
  """

  structure_id: str
  name: str | None = None
  entry_template: EntryTemplateRequest | None = None
  schedule_metadata: ScheduleMetadataRequest | None = None


class DeleteScheduleRequest(BaseModel):
  """Delete a schedule and everything under it. Permanent and irreversible.

  Removes the Structure, its Facts and Associations, its draft entries and
  its unposted obligations. Refused once any of the schedule's entries has
  posted, because the schedule is their support. To end a schedule early
  and keep its history, use `terminate-schedule` (no entry) or
  `create-event-block(event_type='asset_disposed')`, whose handler voids
  the remaining obligations and posts the disposal entry in one step.
  """

  structure_id: str


class TerminateScheduleRequest(BaseModel):
  """End a schedule early at a month-end cutoff — no entry is booked.

  The no-entry half of schedule retirement, for terminations whose GL
  effect is already booked (an asset transferred via a manual journal
  entry, a prepaid refunded in the source system) or where none is
  wanted. In one transaction: deletes forward facts past the cutoff,
  voids the remaining obligation chain past it (pending and classified
  rows), and rewrites the SumEquals rule to prove the truncated curve.
  History at or before the cutoff is untouched.

  When the derecognition entry still needs to be booked, use
  `create-event-block(event_type='asset_disposed')` instead — the
  disposal handler posts it atomically with the same obligation void.
  """

  structure_id: str = Field(..., description="The schedule structure to terminate.")
  new_end_date: date = Field(
    ...,
    description=(
      "Last date the schedule covers — must be the last day of a month "
      "(schedule facts are whole-month). Facts and obligations for periods "
      "starting after this date are removed/voided."
    ),
  )
  reason: str = Field(
    ...,
    min_length=1,
    description="Why the schedule is ending early — captured in the audit log.",
  )


class TerminateScheduleResponse(BaseModel):
  structure_id: str
  name: str
  new_end_date: date
  facts_deleted: int
  obligations_voided: int
  rule_updated: bool
  reason: str


class RebuildScheduleRequest(BaseModel):
  """Re-run the schedule generator in place on an existing schedule.

  Atomic alternative to delete-then-recreate: the structure id and its
  element associations are preserved, the old pending obligation chain
  is voided, the old facts + rules are deleted, and a fresh set of
  forward facts + a fresh obligation chain are regenerated from the
  schedule's stored definition (entry_template / schedule_metadata /
  monthly_amount / period bounds on the Structure's metadata).

  The historical-vs-in-scope split is re-derived from the CURRENT fiscal
  calendar (months through its `closed_through`, or before its first
  month, are history), so a rebuild re-scopes the schedule to today's
  close state. Use this to pick up a fixed generator (e.g. the
  roll-forward direction fix) without orphaning obligations.

  It is also how an edit is saved while nothing has posted: the fields below
  replace the stored definition before it regenerates, as the schedule's
  envelope reads them back (`artifact.mechanics`). Omitted fields keep their
  stored values, inside `schedule_metadata` too.
  """

  structure_id: str = Field(
    ..., description="The schedule structure to regenerate in place."
  )
  period_start: date | None = Field(None, description="A new first period start.")
  period_end: date | None = Field(None, description="A new last period end.")
  monthly_amount: int | None = Field(
    None, description="A new straight-line monthly amount, in cents."
  )
  entry_template: EntryTemplateRequest | None = Field(
    None, description="A new entry template, replacing the stored one whole."
  )
  schedule_metadata: ScheduleMetadataRequest | None = Field(
    None, description="The schedule_metadata fields to change."
  )


# Asset disposal is `create-event-block(event_type='asset_disposed')`; its
# metadata schema is AssetDisposedMetadata.
