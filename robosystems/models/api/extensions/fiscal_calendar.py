"""Fiscal calendar request and response models.

The fiscal calendar tracks where a graph's books stand in their close
cadence — the system-maintained `closed_through` pointer and the
user-settable `close_target` pointer. These models cover the ledger-scoped
endpoints that read and mutate that state.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

PERIOD_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


# ── Requests ───────────────────────────────────────────────────────────────


class InitializeLedgerRequest(BaseModel):
  """One-time setup for a graph's fiscal calendar.

  Creates the `FiscalCalendar` row, seeds `FiscalPeriod` rows from
  ``earliest_data_period`` (or 24 months ago) through the current month,
  and stamps periods on or before ``closed_through`` as already closed.
  Subsequent calls return 409 — there's no re-initialize.

  The two pointers it sets up:

  - ``closed_through`` (system-maintained): the latest period whose
    books are locked. Set on init for businesses with prior close
    history; null for a fresh start.
  - ``close_target`` (user-controlled): the goal date the user is
    closing toward. Set independently via `set-close-target`.
  """

  closed_through: str | None = Field(
    None,
    pattern=PERIOD_PATTERN,
    description=(
      "YYYY-MM period. Periods ≤ this date are treated as historical "
      "(already closed before the user joined). Set to null for a fresh "
      "business with no prior close state."
    ),
  )
  fiscal_year_start_month: int = Field(
    1,
    ge=1,
    le=12,
    description="Fiscal year start month (1-12). Defaults to calendar year.",
  )
  auto_seed_schedules: bool = Field(
    False,
    description=(
      "If true, run the SchedulerAgent to create schedules from historical "
      "BS activity. NOT YET IMPLEMENTED — returns a warning in v1."
    ),
  )
  earliest_data_period: str | None = Field(
    None,
    pattern=PERIOD_PATTERN,
    description=(
      "YYYY-MM period representing the earliest month that has transaction "
      "data. Used to create FiscalPeriod rows. Defaults to 24 months before "
      "the current month."
    ),
  )
  note: str | None = Field(
    None, description="Free-form note attached to the audit event"
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"closed_through": None, "fiscal_year_start_month": 1},
        {
          "closed_through": "2025-12",
          "fiscal_year_start_month": 1,
          "earliest_data_period": "2024-01",
          "note": "Migrating from QuickBooks; 2025 already closed there.",
        },
        {
          "closed_through": "2026-03",
          "fiscal_year_start_month": 7,
          "note": "FY runs July–June.",
        },
      ]
    }
  )


class SetCloseTargetRequest(BaseModel):
  """Set the user-controlled goal period the books should close through.

  The close target drives the catch-up sequence (every period between
  ``closed_through`` and ``close_target`` becomes a candidate for
  closing) and is auto-advanced when reached. Independent from
  ``closed_through``: setting a target doesn't close anything — call
  `close-period` for that.
  """

  period: str = Field(
    ...,
    pattern=PERIOD_PATTERN,
    description="Target period in YYYY-MM format. Must be > current `closed_through`.",
  )
  note: str | None = Field(
    None, description="Free-form note attached to the audit event"
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"period": "2026-03"},
        {
          "period": "2026-12",
          "note": "Year-end target — accelerated close required.",
        },
      ]
    }
  )


class ClosePeriodRequest(BaseModel):
  """Lock a single fiscal period — the final commit action of close.

  Closes the next period in the catch-up sequence (must be exactly
  ``closed_through + 1`` — sequence violations get rejected). Posts
  draft entries, runs balance-sheet equation check, advances
  ``closed_through`` by one, and auto-advances ``close_target`` if
  this close caught up to it. Operation rejects with 422 + structured
  ``blockers`` when gates fail (sync stale, draft entries unbalanced,
  etc).

  ``period`` is supplied separately on the wire — pass it as part of
  the operation's request body. The path identifies the graph; the
  body identifies the period.
  """

  note: str | None = Field(
    None, description="Free-form note attached to the close event"
  )
  allow_stale_sync: bool = Field(
    False,
    description=(
      "Override the sync-currency gate. Only use when you have manually "
      "verified that the source data for the period is complete."
    ),
  )


class ReopenPeriodRequest(BaseModel):
  """Decrement ``closed_through`` by one — un-locks the most recent close.

  Only the most recently closed period can be reopened (no
  reach-back). The ``reason`` is required and captured in the audit
  log. Use sparingly — reopen invalidates downstream artifacts that
  trusted the closed state (reports, shared filings).
  """

  reason: str = Field(
    ...,
    min_length=1,
    description="Required reason for the reopen (captured in audit log)",
  )
  note: str | None = Field(None, description="Additional free-form note")


# ── Responses ──────────────────────────────────────────────────────────────


class FiscalPeriodSummary(BaseModel):
  """One fiscal period row — header view used in calendar listings.

  Status lifecycle: ``open`` → ``closing`` → ``closed``. ``closing``
  is the transient state during a close run; ``closed_at`` stamps when
  the lock landed.
  """

  name: str = Field(..., description="Period name (YYYY-MM)")
  start_date: date
  end_date: date
  status: str = Field(..., description="'open' | 'closing' | 'closed'")
  closed_at: datetime | None = None


class PendingObligationDetailResponse(BaseModel):
  """One pending schedule-derived obligation blocking close.

  Surfaced on `FiscalCalendarResponse` when `pending_obligations` is in
  the blockers list so callers can name which schedules to promote.
  """

  event_id: str
  schedule_id: str | None = None
  schedule_name: str | None = None
  period: str = Field(..., description="Period in YYYY-MM format")


class FiscalCalendarResponse(BaseModel):
  """Current fiscal calendar state for a graph."""

  graph_id: str
  fiscal_year_start_month: int
  closed_through: str | None = Field(
    None, description="Latest closed period (YYYY-MM), or null if nothing closed"
  )
  close_target: str | None = Field(
    None, description="Target period the user wants closed through (YYYY-MM)"
  )
  gap_periods: int = Field(
    0,
    description=(
      "Number of periods between closed_through and close_target (inclusive "
      "of close_target). 0 means caught up."
    ),
  )
  catch_up_sequence: list[str] = Field(
    default_factory=list,
    description="Ordered list of periods that a close run would process",
  )
  closeable_now: bool = Field(
    False,
    description=(
      "Whether the next period in the catch-up sequence passes all closeable gates"
    ),
  )
  blockers: list[str] = Field(
    default_factory=list,
    description=(
      "Structured blocker codes when closeable_now is False: "
      "'sequence_violation', 'period_incomplete', 'sync_stale', "
      "'calendar_not_initialized', 'period_already_closed', "
      "'pending_obligations'"
    ),
  )
  # Detail fields for actionable blockers — populated only when the
  # corresponding code is present in `blockers`. Keeps the default
  # response shape compact while giving close agents and UIs the
  # context they need to resolve a blocker without a sidecar query.
  pending_obligation_count: int = Field(
    0,
    description=(
      "Number of pending schedule_entry_due events blocking close. Non-zero "
      "only when `pending_obligations` is in `blockers`."
    ),
  )
  pending_obligation_sample: list[PendingObligationDetailResponse] = Field(
    default_factory=list,
    description=(
      "Sample of up to 5 pending obligations (schedule_id, schedule_name, "
      "period, event_id) ordered by occurred_at. Use `list-event-blocks` "
      "with event_type=schedule_entry_due&status=pending for the full set."
    ),
  )
  earliest_pending_period: str | None = Field(
    None,
    description=(
      "Earliest period (YYYY-MM) with a pending obligation blocking close. "
      "Null when no pending_obligations blocker is active."
    ),
  )
  sync_stale_days: int | None = Field(
    None,
    description=(
      "Days the most recent sync is stale relative to the period to close. "
      "Populated only when `sync_stale` is in `blockers` and last_sync_at "
      "exists (null when there's a connection but no sync has ever run)."
    ),
  )
  last_close_at: datetime | None = None
  initialized_at: datetime | None = None
  last_sync_at: datetime | None = Field(
    None,
    description="Most recent QB sync timestamp (if connected)",
  )
  periods: list[FiscalPeriodSummary] = Field(
    default_factory=list,
    description="Fiscal period rows for this graph",
  )


class InitializeLedgerResponse(BaseModel):
  fiscal_calendar: FiscalCalendarResponse
  periods_created: int = Field(
    0, description="Number of FiscalPeriod rows created by initialization"
  )
  warnings: list[str] = Field(
    default_factory=list,
    description="Non-fatal warnings (e.g., auto_seed_schedules not implemented)",
  )


class ClosePeriodResponse(BaseModel):
  """Response from a single-period close operation."""

  fiscal_calendar: FiscalCalendarResponse
  period: str
  entries_posted: int = Field(
    0, description="Number of draft entries transitioned to posted"
  )
  target_auto_advanced: bool = Field(
    False,
    description="Whether close_target was auto-advanced because it was reached",
  )
  rule_summary: dict[str, int] | None = Field(
    None,
    description=(
      "Aggregated rule-eval outcome across every schedule Structure with "
      "facts in the closed period — keys: pass/fail/error/skipped. None when "
      "no schedules had facts in the period (auto-run on close)."
    ),
  )
  evaluated_structure_ids: list[str] = Field(
    default_factory=list,
    description=(
      "ids of schedule Structures whose rules were evaluated during the "
      "close. Pairs with rule_summary."
    ),
  )
  statements_stamped: bool = Field(
    False,
    description=(
      "Whether the close stamped the period's canonical statement "
      "FactSets (the close-time pivot). False when the tenant hasn't "
      "set up reporting yet — see statement_stamp_note."
    ),
  )
  statement_stamp_note: str | None = Field(
    None,
    description=(
      "Soft-skip reason when statements_stamped is false: "
      "no_coa_mapping | no_entity | no_statement_structures | no_taxonomy."
    ),
  )
  stamped_statement_sets: dict[str, str] = Field(
    default_factory=dict,
    description=(
      "structure_id -> fact_set_id for every canonical statement FactSet "
      "minted by this close (report_id NULL; replaced on reclose)."
    ),
  )
  statement_rule_summary: dict[str, int] | None = Field(
    None,
    description=(
      "Aggregated statement-rule verification outcome across the stamped "
      "structures — keys: pass/fail/error/skipped. None when no statement "
      "rules exist. Distinct from rule_summary (the schedule-rule pass)."
    ),
  )


# ── Draft review (read-only) ──────────────────────────────────────────────


class DraftLineItem(BaseModel):
  """A single line item within a draft entry."""

  line_item_id: str
  element_id: str
  element_code: str | None = None
  element_name: str
  debit_amount: int = Field(..., description="Debit amount in cents")
  credit_amount: int = Field(..., description="Credit amount in cents")
  description: str | None = None


class DraftEntryResponse(BaseModel):
  """A single draft entry with full line item detail for review."""

  entry_id: str
  posting_date: date
  type: str = Field(..., description="Entry type (e.g., 'closing', 'adjusting')")
  memo: str | None = None
  provenance: str | None = Field(
    None,
    description=(
      "Where the entry came from (ENTRY_PROVENANCE_VALUES): source_sync, "
      "ai_generated, manual_entry, schedule_derived, system_computed, event_handler"
    ),
  )
  source_structure_id: str | None = Field(
    None, description="Schedule structure that generated this entry (if any)"
  )
  source_structure_name: str | None = Field(
    None, description="Human-readable name of the source schedule"
  )
  line_items: list[DraftLineItem]
  total_debit: int = Field(..., description="Sum of debit amounts in cents")
  total_credit: int = Field(..., description="Sum of credit amounts in cents")
  balanced: bool = Field(..., description="True if total_debit == total_credit")
  will_publish_to_qb: bool = Field(
    False,
    description=(
      "True if closing the period will publish this draft to QuickBooks — "
      "i.e. the graph has a qb_authoritative/hybrid QB connection AND this "
      "is an RL-originated draft (schedule/manual) not already in QB. False "
      "means it posts locally only."
    ),
  )


class PeriodDraftsResponse(BaseModel):
  """All draft entries for a fiscal period, ready for review before close."""

  period: str = Field(..., description="YYYY-MM period name")
  period_start: date
  period_end: date
  draft_count: int
  total_debit: int = Field(..., description="Sum across all drafts, in cents")
  total_credit: int = Field(..., description="Sum across all drafts, in cents")
  all_balanced: bool = Field(
    ..., description="True if every draft entry has debit == credit"
  )
  qb_writeback_connection_id: str | None = Field(
    None,
    description=(
      "Id of the QuickBooks connection these drafts publish to on close, or "
      "null when the graph has no qb_authoritative/hybrid QB connection (the "
      "drafts post locally only)."
    ),
  )
  qb_write_policy: str | None = Field(
    None,
    description=(
      "write_policy of the publishing QB connection ('qb_authoritative' / "
      "'hybrid'), or null when there is no write-back connection."
    ),
  )
  qb_publish_count: int = Field(
    0,
    description="Number of drafts that will publish to QuickBooks on close.",
  )
  local_only_count: int = Field(
    0,
    description="Number of drafts that post locally only (no QB write-back).",
  )
  drafts: list[DraftEntryResponse]
