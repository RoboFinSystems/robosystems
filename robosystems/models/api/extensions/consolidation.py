"""Request and response models for the consolidating entries a group's month
needs (spec ``consolidation`` C2)."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

from robosystems.models.api.extensions.journal_entries import AdjustmentKind


class PreviewConsolidationEntriesRequest(BaseModel):
  """The month to compute the consolidating entries for."""

  period: str = Field(
    ...,
    pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    description="The month, as YYYY-MM.",
  )


class ConsolidationProposalLine(BaseModel):
  """One line of a proposed consolidating entry, in the consolidation
  entity's chart."""

  element_id: str = Field(..., description="The consolidation entity's account.")
  account_name: str = Field(..., description="The account's name.")
  concept: str = Field(..., description="The rs-gaap concept the account maps to.")
  debit_amount: int = Field(0, description="Debit, in cents.")
  credit_amount: int = Field(0, description="Credit, in cents.")


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
      "consolidation entity already holds what the month needs."
    ),
  )
