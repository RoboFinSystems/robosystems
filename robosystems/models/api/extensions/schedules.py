"""Schedule request and response models."""

from __future__ import annotations

from datetime import date, datetime
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
      "Watermark for onboarding. Facts with period_end ≤ this date are "
      "flagged 'historical' and their schedule_entry_due obligations are "
      "emitted 'voided', so the close workflow starts drafting at the first "
      "open period. Set this to the last day of the fiscal calendar's "
      "closed_through month (calendar '2026-05' → '2026-05-31') whether "
      "those months were actually closed in RoboLedger or just "
      "baseline-watermarked at initialization. Omitting it (when prior "
      "periods exist) leaves pre-watermark periods as 'pending' obligations "
      "that block the first close."
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
  existing draft). The sweep also reaches *stranded* obligations —
  already ``classified`` (by an earlier flip-only sweep) but with no
  closing entry ever drafted — dispatching them in the same pass.
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
    ..., description="Receipt schema version; 1 is the first shipped shape"
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
  Those require fact regeneration — end the schedule early via
  `terminate-schedule` (no entry) or
  `create-event-block(event_type='asset_disposed')` (with a disposal
  entry), then create a fresh schedule via `create-information-block`
  (`block_type='schedule'`).

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
  operation. For ending a schedule early without removing history, use
  `terminate-schedule` (no entry) or
  `create-event-block(event_type='asset_disposed')` (the handler voids
  the remaining obligation chain + posts the disposal entry atomically;
  recognized facts stay as history).
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
  calendar `closed_through`, so a rebuild re-scopes the schedule to
  today's close state. Use this to pick up a fixed generator (e.g. the
  roll-forward direction fix) without orphaning obligations.
  """

  structure_id: str = Field(
    ..., description="The schedule structure to regenerate in place."
  )


# Asset disposal is an event block:
# `create-event-block(event_type='asset_disposed')`. See
# operations/event_block/python_handlers/asset_disposed.py for the
# metadata schema (AssetDisposedMetadata).
