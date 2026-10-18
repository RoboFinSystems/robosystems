"""Request and response models for the consolidating entries a group's month
needs (spec ``consolidation`` C2)."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

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
  posting_date: date = Field(..., description="The month's last day.")
  memo: str = Field(..., description="The entry's memo.")
  basis: str = Field(..., description="How the amounts were computed.")
  lines: list[ConsolidationProposalLine] = Field(
    ..., description="The entry's lines; they balance."
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
      "True when nothing is proposed, withheld or left out: the "
      "consolidation entity already holds what the month needs. A closed "
      "month is up to date as posted; read `closed` first."
    ),
  )
  closed: bool = Field(
    False,
    description=(
      "True when the consolidation entity has closed the month: its entries "
      "are as posted, and nothing is recomputed."
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
