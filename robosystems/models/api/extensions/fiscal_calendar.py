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
  name: str = Field(..., description="Period name (YYYY-MM)")
  start_date: date
  end_date: date
  status: str = Field(..., description="'open' | 'closing' | 'closed'")
  closed_at: datetime | None = None


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
      "'calendar_not_initialized', 'period_already_closed'"
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
    description="Where the entry came from: 'ai_generated', 'manual_entry', etc.",
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
  drafts: list[DraftEntryResponse]
