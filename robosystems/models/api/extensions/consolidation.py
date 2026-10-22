"""Request and response models for the consolidating entries a group's month
needs (spec ``consolidation`` C2)."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from robosystems.models.api.extensions.journal_entries import (
  AdjustmentKind,
  JournalEntryResponse,
)

_PERIOD = Field(
  ...,
  pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
  description="The month, as YYYY-MM.",
)


class PreviewConsolidationEntriesRequest(BaseModel):
  """The month to compute the consolidating entries for."""

  period: str = _PERIOD


class ProposeConsolidationEntriesRequest(BaseModel):
  """The month to draft the consolidating entries for."""

  period: str = _PERIOD


class ConsolidationProposalLine(BaseModel):
  """One line of a proposed consolidating entry, in the consolidation
  entity's chart."""

  element_id: str = Field(..., description="The consolidation entity's account.")
  account_name: str = Field(..., description="The account's name.")
  concept: str = Field(..., description="The rs-gaap concept the account maps to.")
  debit_amount: int = Field(0, description="Debit, in cents.")
  credit_amount: int = Field(0, description="Credit, in cents.")
  flow_qname: str | None = Field(
    None,
    description=(
      "The cash flow row the line offsets, when it eliminates one: the row "
      "the member's own line produced, so the consolidated cash flow nets it."
    ),
  )


class ConsolidationProposal(BaseModel):
  """One consolidating entry the month needs: the difference between the
  balance this kind of adjustment should hold for the member at the month
  end and what the consolidation entity already holds for it."""

  adjustment_kind: AdjustmentKind = Field(..., description="What the entry adjusts.")
  adjusts_entity_id: str = Field(
    ..., description="The entity whose figures the entry adjusts."
  )
  adjusts_entity_name: str = Field(..., description="That entity's name.")
  posting_date: date = Field(
    ...,
    description="The month's last day; for an opening, the eve of the "
    "consolidation entity's first month.",
  )
  memo: str = Field(..., description="The entry's memo.")
  basis: str = Field(..., description="How the amounts were computed.")
  lines: list[ConsolidationProposalLine] = Field(
    ..., description="The entry's lines; they balance."
  )
  opening: bool = Field(
    False,
    description=(
      "Part of the consolidation entity's opening balance: what the members' "
      "history before the group's first month already needed eliminated, so "
      "the consolidated statements open on it rather than move by it. "
      "Proposing records it, posted, replacing the earlier one until the "
      "first close."
    ),
  )


class ConsolidationResidual(BaseModel):
  """A difference the engine cannot eliminate on its own: the parent's
  investment in a member against the parent's share of that member's
  capital. Its entry is withheld until the difference is explained or
  posted by hand with a basis; it is never plugged."""

  entity_id: str = Field(..., description="The member.")
  entity_name: str = Field(..., description="The member's name.")
  amount: int = Field(
    ...,
    description=(
      "Cents: the parent's investment balance less its share of the member's "
      "capital. Positive when the parent carries more than its share."
    ),
  )
  explanation: str = Field(..., description="What the two figures are.")
  cause: Literal["unexplained", "pre_acquisition"] = Field(
    "unexplained",
    description=(
      "`pre_acquisition` when the member joined the group mid-life and its "
      "acquisition is not recorded: the parent paid its sellers, not the "
      "member, so `record-acquisition` explains the difference — the "
      "consideration, with the excess over the parent's share of the "
      "member's equity at acquisition as goodwill or fair-value step-ups."
    ),
  )


class ConsolidationReconcilingItem(BaseModel):
  """Two of the group's entities whose intercompany figures do not agree.
  The lower amount is eliminated where one side matches the other; what is
  left stays in the consolidated statements until the books agree."""

  entity_ids: list[str] = Field(..., description="The two entities.")
  concept: str = Field(..., description="The rs-gaap concept left uneliminated.")
  entity_id: str = Field(..., description="The entity whose figure is left.")
  amount: int = Field(..., description="Cents left uneliminated, debit-positive.")
  description: str = Field(..., description="What does not agree.")


class PreviewConsolidationEntriesResponse(BaseModel):
  """The consolidating entries a month needs, computed from the books."""

  period: str = Field(..., description="The month, as YYYY-MM.")
  period_start: date = Field(..., description="The month's first day.")
  period_end: date = Field(..., description="The month's last day.")
  consolidation_entity_id: str = Field(
    ..., description="The entity the entries post in."
  )
  proposals: list[ConsolidationProposal] = Field(
    default_factory=list,
    description="The entries to post; empty when the month is up to date.",
  )
  residuals: list[ConsolidationResidual] = Field(
    default_factory=list,
    description="Differences withheld from the proposals until explained.",
  )
  reconciling_items: list[ConsolidationReconcilingItem] = Field(
    default_factory=list,
    description="Intercompany figures the two sides do not agree on.",
  )
  notes: list[str] = Field(
    default_factory=list,
    description="Anything the engine left out, and why.",
  )
  up_to_date: bool = Field(
    ...,
    description=(
      "True when nothing is withheld or left out and the consolidation "
      "entity holds what the month needs, posted or in the month's drafts "
      "(then `proposals` lists what the drafts hold). A closed month is up "
      "to date as posted; read `closed` first."
    ),
  )
  closed: bool = Field(
    False,
    description=(
      "True when the consolidation entity has closed the month: its entries "
      "are as posted, and nothing is recomputed."
    ),
  )
  incomplete: bool = Field(
    False,
    description=(
      "True when the engine left an entry out — a member's books it cannot "
      "read, an account the consolidation entity lacks; `notes` says which. "
      "The group close waits on it whatever the residuals."
    ),
  )
  drafted_entry_ids: list[str] = Field(
    default_factory=list,
    description=(
      "The month's drafts propose-consolidation-entries wrote, waiting for "
      "the consolidation entity's close to post them."
    ),
  )


class ProposeConsolidationEntriesResponse(BaseModel):
  """The consolidating entries drafted for a month."""

  period: str = Field(..., description="The month, as YYYY-MM.")
  consolidation_entity_id: str = Field(..., description="The entity the drafts are in.")
  closed: bool = Field(
    False, description="True when the month is closed and nothing was drafted."
  )
  entries: list[JournalEntryResponse] = Field(
    default_factory=list,
    description="The drafts written; the consolidation entity's close posts them.",
  )
  replaced: int = Field(
    0, description="Earlier drafts of this month's entries, replaced by these."
  )
  residuals: list[ConsolidationResidual] = Field(
    default_factory=list,
    description="Differences withheld from the drafts until explained.",
  )
  reconciling_items: list[ConsolidationReconcilingItem] = Field(
    default_factory=list,
    description="Intercompany figures the two sides do not agree on.",
  )
  notes: list[str] = Field(
    default_factory=list,
    description="Anything the engine left out, and why.",
  )


class SetAllocationOverrideRequest(BaseModel):
  """The outside owners' share of a member's result for one month, set by a
  person in place of the pro-rata share: a preferred return, a waterfall, a
  special allocation."""

  entity_id: str = Field(..., description="The member, consolidated in full.")
  period: str = _PERIOD
  amount: int | None = Field(
    None,
    description=(
      "The outside owners' share of the month's result, in cents; negative "
      "for a share of a loss."
    ),
  )
  percentage: float | None = Field(
    None,
    ge=0,
    le=100,
    description="The outside owners' share as a percentage of the month's result.",
  )
  basis: str | None = Field(
    None,
    description=(
      "Why the share differs from the ownership percentage; required to set one."
    ),
  )
  clear: bool = Field(
    False, description="Remove the month's override, back to the pro-rata share."
  )


class AllocationOverrideResponse(BaseModel):
  """A member's allocation override for one month."""

  entity_id: str = Field(..., description="The member.")
  period: str = Field(..., description="The month, as YYYY-MM.")
  amount: int | None = Field(None, description="The share, in cents, when set so.")
  percentage: float | None = Field(
    None, description="The share as a percentage, when set so."
  )
  basis: str | None = Field(None, description="Why it differs from pro rata.")
  set_by: str | None = Field(None, description="Who set it.")
  cleared: bool = Field(False, description="True when the override was removed.")


class AcquisitionAllocation(BaseModel):
  """Part of an acquisition's excess assigned to an identifiable asset or
  liability at fair value instead of goodwill: a step-up."""

  concept: str = Field(
    ...,
    description=(
      "The rs-gaap concept stepped up, e.g. `rs-gaap:PropertyPlantAndEquipmentNet`; "
      "the consolidation entity needs an account mapped to it."
    ),
  )
  amount: int = Field(
    ...,
    description=(
      "Cents, debit-positive: fair value less book value at acquisition. "
      "Positive raises an asset; negative raises a liability."
    ),
  )


class RecordAcquisitionRequest(BaseModel):
  """A member bought from its outside owners: what the parent paid them,
  and how the excess over its share of the member's equity at acquisition is
  allocated. The engine never invents a fair value; each comes from the
  person, with the basis."""

  entity_id: str = Field(
    ...,
    description=(
      "The member acquired, consolidated in full, with `joined_on` set and "
      "its opening balance recorded at acquisition."
    ),
  )
  consideration: int = Field(
    ...,
    gt=0,
    description=(
      "Cents the parent paid the sellers for its interest. Its investment line "
      "names the seller, not the member, which is why it is given here."
    ),
  )
  nci_fair_value: int | None = Field(
    None,
    ge=0,
    description=(
      "Cents: the fair value of the outside owners' interest at acquisition, "
      "for goodwill on the whole business (ASC 805). Omitted, the outside "
      "owners carry their share of the identifiable net assets and goodwill "
      "is the parent's alone."
    ),
  )
  allocations: list[AcquisitionAllocation] = Field(
    default_factory=list,
    description="Fair-value step-ups; what is left of the excess is goodwill.",
  )
  basis: str = Field(
    ...,
    min_length=1,
    description=(
      "Where the consideration and fair values come from: the purchase "
      "agreement, a valuation."
    ),
  )
  confirm_bargain_purchase: bool = Field(
    False,
    description=(
      "Required when the parent paid less than its share of the fair value "
      "of what it acquired. ASC 805 has the acquirer reassess before "
      "recognizing the gain; set it only after that review."
    ),
  )

  @field_validator("basis")
  @classmethod
  def _basis_says_something(cls, value: str) -> str:
    if not value.strip():
      raise ValueError("Give the basis: the purchase agreement, a valuation.")
    return value.strip()


class AcquisitionResponse(BaseModel):
  """A member's acquisition as recorded: the draft in the consolidation
  entity its close posts."""

  entity_id: str = Field(..., description="The member.")
  entity_name: str = Field(..., description="The member's name.")
  joined_on: date = Field(..., description="When it joined the group.")
  equity_at_acquisition: int = Field(
    ..., description="Cents: the member's equity in its opening balance."
  )
  parent_share: int = Field(
    ..., description="Cents: the parent's share of that equity."
  )
  consideration: int = Field(..., description="Cents paid to the sellers.")
  excess: int = Field(
    ...,
    description="Cents: consideration less the parent's share of the equity.",
  )
  goodwill: int = Field(
    ...,
    description="Cents of goodwill; negative for a bargain purchase gain.",
  )
  noncontrolling_interest: int = Field(
    0,
    description=(
      "Cents added to the outside owners' interest beyond their share of the "
      "member's book equity."
    ),
  )
  entry: JournalEntryResponse = Field(
    ..., description="The acquisition entry, a draft the close posts."
  )
  replaced: bool = Field(
    False, description="True when it replaced an earlier draft of the acquisition."
  )


class AllocationOverrideRecord(BaseModel):
  """A member's allocation override for one month, as set."""

  period: str = Field(..., description="The month, as YYYY-MM.")
  amount: int | None = Field(None, description="The share, in cents, when set so.")
  percentage: float | None = Field(
    None, description="The share as a percentage of the month's result, when set so."
  )
  basis: str | None = Field(None, description="Why it differs from pro rata.")
  set_by: str | None = Field(None, description="Who set it.")


class AllocationShareResponse(BaseModel):
  """The outside owners' share of a member's result for one month: the
  pro-rata figure its ownership gives, the override that replaces it if one
  is set, and the share the consolidating entries use."""

  entity_id: str = Field(..., description="The member.")
  entity_name: str = Field(..., description="The member's name.")
  period: str = Field(..., description="The month, as YYYY-MM.")
  consolidation_method: str = Field(
    ..., description="How the member is consolidated: full, equity, cost or none."
  )
  ownership_pct: float | None = Field(
    None, description="The parent's ownership, as a percentage."
  )
  outside_pct: float = Field(
    ..., description="The outside owners' ownership, as a percentage."
  )
  month_result: int = Field(
    ...,
    description="Cents: the member's result for the month, positive for a profit.",
  )
  pro_rata_amount: int = Field(
    ..., description="Cents: the outside owners' percentage of the month's result."
  )
  override: AllocationOverrideRecord | None = Field(
    None, description="The month's override, when one is set."
  )
  effective_amount: int = Field(
    ...,
    description=(
      "Cents: the share the consolidating entries use, the override's when set."
    ),
  )
  closed: bool = Field(
    False,
    description=(
      "True when the consolidation entity has closed the month, so the "
      "override can no longer change."
    ),
  )


class AllocationOverrideListResponse(BaseModel):
  """Every month a member's allocation override is set for."""

  entity_id: str = Field(..., description="The member.")
  overrides: list[AllocationOverrideRecord] = Field(
    default_factory=list, description="The overrides, oldest month first."
  )


class RecordOwnershipChangeRequest(BaseModel):
  """The parent buys more of a member from its outside owners, staying on the
  same consolidation method: the share it holds from a month, and what it
  paid. ASC 810 makes it an equity transaction, never a gain or loss."""

  entity_id: str = Field(..., description="The member, consolidated in full.")
  period: str = _PERIOD
  ownership_pct: float = Field(
    ...,
    gt=0,
    le=100,
    description=(
      "The parent's share from `period`, as a percentage: above its share "
      "the month before, and 50 or more, so the member stays consolidated "
      "in full."
    ),
  )
  consideration: int = Field(
    ...,
    ge=0,
    description=(
      "Cents the parent paid the outside owners for the shares. Its line "
      "names them, not the member, which is why it is given here."
    ),
  )
  basis: str = Field(
    ..., min_length=1, description="The purchase agreement, or what sets the price."
  )

  @field_validator("basis")
  @classmethod
  def _basis_says_something(cls, value: str) -> str:
    if not value.strip():
      raise ValueError(
        "Give the basis: the purchase agreement, or what sets the price."
      )
    return value.strip()


class OwnershipChangeResponse(BaseModel):
  """A change in the parent's share of a member as recorded: the draft in
  the consolidation entity its close posts."""

  entity_id: str = Field(..., description="The member.")
  entity_name: str = Field(..., description="The member's name.")
  period: str = Field(..., description="The first month the new share holds.")
  from_pct: float = Field(..., description="The parent's share the month before.")
  to_pct: float = Field(..., description="The parent's share from `period`.")
  consideration: int = Field(..., description="Cents paid to the outside owners.")
  book_value: int = Field(
    ...,
    description=(
      "Cents: the share bought of the member's capital, which the investment "
      "elimination moves from the outside owners to the parent."
    ),
  )
  earnings_transferred: int = Field(
    ...,
    description=(
      "Cents: the share bought of the member's results since it joined, moved "
      "from the outside owners' interest to the parent."
    ),
  )
  premium_transferred: int = Field(
    0,
    description=(
      "Cents: the share bought of the outside owners' interest beyond their "
      "share of book value (from the acquisition at fair value, or its "
      "step-ups), moved to the parent."
    ),
  )
  equity_adjustment: int = Field(
    ...,
    description=(
      "Cents taken from the parent's equity: the price above the carrying "
      "amount of what was bought (negative when it paid less)."
    ),
  )
  entry: JournalEntryResponse = Field(
    ..., description="The ownership change entry, a draft the close posts."
  )
  replaced: bool = Field(
    False, description="True when it replaced an earlier draft for the month."
  )
