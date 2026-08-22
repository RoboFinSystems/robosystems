"""Reconciling-item preview and resolution models.

A reconciling item is a posted event whose source-system payload changed
after it was posted: the sync captures the new payload rather than
overwriting approved books, and flags the event. These models describe the
two halves of clearing one — reading what changed, and saying what to do
about it.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

# How an operator disposes of a reconciling item. The three are the
# textbook treatments, and which is right is an accounting judgement about
# the period, not a technical one:
#
# - ``restate`` — regenerate this event's entries from the accepted payload,
#   in place. The prior months' figures change. Right when nothing external
#   binds them (the usual case inside the current fiscal year).
# - ``catch_up`` — leave the original entries alone and post the difference
#   as a new entry in an open period. Prior statements stand. Right when a
#   reporting cadence has locked the months the change would otherwise move.
# - ``acknowledge`` — record that the difference was handled outside this
#   operation (an entry authored by hand, or a change that needs no entry)
#   and clear the flag without touching the ledger.
ReconcilingItemDisposition = Literal["restate", "catch_up", "acknowledge"]


class PreviewReconcilingItemRequest(BaseModel):
  """Read what changed on a reconciling item, and what resolving it would do."""

  event_id: str = Field(..., description="Event id (evt_ prefixed) to inspect")


class ResolveReconcilingItemRequest(BaseModel):
  """Dispose of one reconciling item and clear its flag."""

  event_id: str = Field(..., description="Event id (evt_ prefixed) to resolve")
  disposition: ReconcilingItemDisposition | None = Field(
    None,
    description=(
      "How to dispose of the difference. Omit to take the default the "
      "preview reports: restate when every period the event touches is "
      "open, catch_up when any is closed."
    ),
  )
  posting_date: date | None = Field(
    None,
    description=(
      "catch_up only: when to post the catch-up entry. Defaults to the end "
      "of the earliest open period."
    ),
  )
  status: Literal["draft", "posted"] = Field(
    "draft",
    description=(
      "catch_up only: whether the catch-up entry is drafted for review at "
      "close (default) or posted immediately. A draft appears in "
      "list-period-drafts and posts locally when the period closes."
    ),
  )
  note: str | None = Field(
    None,
    description=(
      "Why this disposition. Required for acknowledge, where it is the only "
      "record of what was done instead."
    ),
  )
  reference_event_id: str | None = Field(
    None,
    description=(
      "acknowledge only: the event that already handled this difference "
      "(e.g. an alignment entry authored by hand), recorded on the trail."
    ),
  )

  @model_validator(mode="after")
  def _note_required_for_acknowledge(self) -> ResolveReconcilingItemRequest:
    if self.disposition == "acknowledge" and not (self.note or "").strip():
      raise ValueError(
        "acknowledge requires a note — it is the only record of how the "
        "difference was handled."
      )
    return self


class ReconcilingItemDeltaLine(BaseModel):
  """One account's net change between the posted entries and the new payload.

  Amounts are signed minor units in debit-positive convention: a positive
  figure is a net debit, a negative one a net credit. ``delta`` is what a
  catch-up entry would post to bring the books level.
  """

  element_id: str | None = Field(
    None, description="CoA element id; null when the account is unmapped"
  )
  element_external_id: str | None = Field(
    None, description="Source-system account id, when the line carried one"
  )
  element_code: str | None = Field(None, description="Account code")
  element_name: str | None = Field(None, description="Account name")
  prior_net: int = Field(..., description="Net of the posted entries, debit-positive")
  accepted_net: int = Field(..., description="Net of the new payload, debit-positive")
  delta: int = Field(..., description="accepted_net - prior_net")


class ReconcilingItemEntrySummary(BaseModel):
  """One entry on either side of the comparison."""

  entry_id: str | None = Field(
    None, description="Entry id; null for the accepted side, which is not posted yet"
  )
  external_id: str | None = None
  posting_date: date | None = None
  memo: str | None = None
  status: str | None = Field(
    None, description="Entry status; null on the accepted side"
  )
  total_debit: int = 0
  total_credit: int = 0


class ReconcilingItemPlan(BaseModel):
  """What changed upstream, and what each disposition would do about it."""

  event_id: str
  external_id: str | None = None
  source: str
  event_type: str
  event_status: str
  drift_detected_at: datetime | None = Field(
    None, description="When the sync first saw this difference"
  )
  default_disposition: ReconcilingItemDisposition = Field(
    ...,
    description=(
      "What resolve would do with no disposition given: restate while every "
      "affected period is open, catch_up once one is closed."
    ),
  )
  default_posting_date: date | None = Field(
    None, description="Where a catch-up entry would land by default"
  )
  affected_posting_dates: list[date] = Field(
    default_factory=list, description="Posting dates of the event's entries"
  )
  closed_periods: list[str] = Field(
    default_factory=list,
    description="Names of closed periods the event's entries sit in",
  )
  prior_entries: list[ReconcilingItemEntrySummary] = Field(default_factory=list)
  accepted_entries: list[ReconcilingItemEntrySummary] = Field(default_factory=list)
  delta: list[ReconcilingItemDeltaLine] = Field(
    default_factory=list, description="Per-account net change; empty when none"
  )
  no_gl_effect: bool = Field(
    False,
    description=(
      "The change moves no money — a memo or reference edit. catch_up posts "
      "nothing; restate still regenerates so the entries carry the new text."
    ),
  )
  restate_blockers: list[str] = Field(
    default_factory=list,
    description=(
      "Why restate is unavailable, if it is: a closed period, an entry that "
      "was reversed or is not posted, or entries from elsewhere sharing this "
      "event's transaction."
    ),
  )
  unmapped_element_external_ids: list[str] = Field(
    default_factory=list,
    description=(
      "Accounts in the new payload with no mapping in this graph. Both "
      "dispositions that write need them mapped first."
    ),
  )


class ReconcilingItemCatchUp(BaseModel):
  """The catch-up entry a resolution posted."""

  event_id: str
  entry_id: str | None = None
  transaction_id: str | None = None
  posting_date: date
  status: str


class ReconcilingItemRegenerated(BaseModel):
  """The entries a restate rebuilt."""

  transaction_ids: list[str] = Field(default_factory=list)
  entry_ids: list[str] = Field(default_factory=list)


class ResolveReconcilingItemResponse(BaseModel):
  """The outcome of resolving one reconciling item."""

  event_id: str
  external_id: str | None = None
  disposition: ReconcilingItemDisposition
  delta: list[ReconcilingItemDeltaLine] = Field(default_factory=list)
  no_gl_effect: bool = False
  catch_up: ReconcilingItemCatchUp | None = Field(
    None, description="Present when the disposition posted a catch-up entry"
  )
  regenerated: ReconcilingItemRegenerated | None = Field(
    None, description="Present when the disposition rebuilt the event's entries"
  )
  reference_event_id: str | None = None
  note: str | None = None
  resolved_at: datetime
  resolved_by: str
