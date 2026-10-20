"""AR/AP response models — open-balance projections derived from the event
duality chain.

AR ("Accounts Receivable") = sum of `invoice_issued` events minus
matching `payment_received` discharges. AP ("Accounts Payable") =
sum of `bill_received` events minus matching `bill_paid`
discharges. Numbers come straight from `events.amount` — no GL roll
required — so an open balance is auditable in one query.

Amounts are stored in minor currency units (cents); response models
preserve that for backend math. UI layers display-format on render.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class OpenBalanceAggregate(BaseModel):
  """Graph-wide open AR or open AP aggregate.

  `total_open_cents` is the sum of unsettled originating-event
  balances; `counterparty_count` is the number of distinct agents
  with at least one open invoice or bill. The currency is uniform
  per graph today (mixed-currency books would need a per-currency
  breakdown — out of scope for v1).
  """

  model_config = ConfigDict(from_attributes=True)

  total_open_cents: int = Field(
    ..., description="Sum of unsettled balances in minor currency units."
  )
  counterparty_count: int = Field(
    ..., description="Distinct agents with at least one open balance."
  )
  open_event_count: int = Field(
    ..., description="Distinct originating events with a nonzero open balance."
  )
  currency: str = Field("USD", description="Currency code (uniform per graph).")


class OpenBalanceByAgent(BaseModel):
  """Per-agent open balance row.

  Used for both the aging-by-counterparty list and the per-Agent
  GraphQL field. `open_balance_cents` reflects only the unsettled
  remainder (sum of originating amounts minus sum of discharges) so
  partial-payment chains net out correctly.
  """

  model_config = ConfigDict(from_attributes=True)

  agent_id: str
  open_balance_cents: int = Field(
    ...,
    description=(
      "Unsettled originating-event amounts minus discharges, in minor "
      "currency units. Positive for normal AR/AP; negative when overpaid."
    ),
  )
  open_event_count: int = Field(
    ..., description="Number of originating events with nonzero balance for this agent."
  )
  currency: str = Field("USD")


class OpenItem(BaseModel):
  """One open invoice or bill as of a date: what it was for, what has been
  settled against it by then, and what is left."""

  model_config = ConfigDict(from_attributes=True)

  event_id: str = Field(..., description="The obligation's event id.")
  entity_id: str = Field(..., description="The entity whose books hold it.")
  agent_id: str | None = Field(None, description="The counterparty.")
  occurred_on: date = Field(..., description="The date it was issued or received.")
  document_number: str | None = Field(
    None, description="The invoice or bill number, where the source has one."
  )
  description: str | None = Field(None, description="The event's description.")
  original_cents: int = Field(..., description="The obligation's amount.")
  settled_cents: int = Field(
    ..., description="Settled against it on or before the as-of date."
  )
  open_cents: int = Field(
    ...,
    description=(
      "What is left: original less settled. Negative when overpaid, an "
      "unapplied credit."
    ),
  )
  currency: str = Field("USD", description="Currency code.")


class IntercompanyPair(BaseModel):
  """What one group entity says another owes it, beside what that entity says
  it owes; consolidation eliminates both, so a difference is a disagreement."""

  creditor_entity_id: str = Field(..., description="The entity owed.")
  debtor_entity_id: str = Field(..., description="The entity that owes.")
  receivable_cents: int = Field(
    ..., description="The creditor's open receivables from the debtor."
  )
  payable_cents: int = Field(
    ..., description="The debtor's open payables to the creditor."
  )
  difference_cents: int = Field(
    ..., description="Receivable less payable; zero when the two books agree."
  )


class GroupOpenItems(BaseModel):
  """A group's open receivables or payables as of a date: the external items
  of every member consolidated in full, and the intercompany ones set apart,
  since consolidation eliminates them."""

  kind: str = Field(..., description="`receivables` or `payables`.")
  as_of: date = Field(..., description="The date the items are open at.")
  member_entity_ids: list[str] = Field(
    ..., description="The entities summed: the parent and its full members."
  )
  external: list[OpenItem] = Field(
    ..., description="Open items with a counterparty outside the group."
  )
  intercompany: list[OpenItem] = Field(
    ..., description="Open items with another summed member as counterparty."
  )
  pairs: list[IntercompanyPair] = Field(
    ..., description="Each pair of members, both sides of what they owe each other."
  )
