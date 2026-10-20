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
  due_date: date = Field(
    ...,
    description=(
      "When it falls due: the stated due date, else the date it was issued "
      "(due on receipt)."
    ),
  )
  due_date_basis: str = Field(
    ...,
    description=(
      "`stated` when the obligation carries its own due date; `defaulted` when "
      "it has none and is read as due on receipt."
    ),
  )
  days_past_due: int = Field(
    ...,
    description=(
      "Days from the due date to the as-of date; zero or negative when not yet due."
    ),
  )
  bucket: str = Field(
    ...,
    description=(
      "The aging bucket: `current`, `one_to_thirty`, `thirty_one_to_sixty`, "
      "`sixty_one_to_ninety`, `over_ninety`, or `unapplied_credit` when "
      "overpaid."
    ),
  )


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


class AgingBuckets(BaseModel):
  """Open amounts by how far past due they are at the as-of date. An
  overpayment is an unapplied credit, never netted into a bucket."""

  current_cents: int = Field(0, description="Not yet due.")
  one_to_thirty_cents: int = Field(0, description="1 to 30 days past due.")
  thirty_one_to_sixty_cents: int = Field(0, description="31 to 60 days past due.")
  sixty_one_to_ninety_cents: int = Field(0, description="61 to 90 days past due.")
  over_ninety_cents: int = Field(0, description="More than 90 days past due.")
  unapplied_credit_cents: int = Field(
    0, description="Overpayments, negative: credit owed back or still to apply."
  )
  total_cents: int = Field(
    0, description="Every bucket and the unapplied credit together."
  )


class AgedAgentRow(BaseModel):
  """One counterparty's open items, bucketed."""

  agent_id: str | None = Field(None, description="The counterparty; null when none.")
  agent_name: str | None = Field(None, description="The counterparty's name.")
  buckets: AgingBuckets = Field(..., description="Its open amounts by age.")
  open_item_count: int = Field(..., description="Its open invoices or bills.")
  oldest_due_date: date | None = Field(
    None, description="The earliest due date among its open items."
  )


class ControlAccount(BaseModel):
  """An account the open items are tied out against."""

  element_id: str = Field(..., description="The account's element id.")
  code: str | None = Field(None, description="The account code.")
  name: str = Field(..., description="The account name.")


class ControlTieOut(BaseModel):
  """One entity's open items against its receivables or payables accounts at
  the as-of date. The difference is everything that moved the accounts
  without a document behind it, or a document whose entry has not posted:
  never plugged."""

  entity_id: str = Field(..., description="The entity whose books are compared.")
  accounts: list[ControlAccount] = Field(
    ...,
    description=(
      "The control accounts: those mapped to the receivables or payables "
      "concept, and QuickBooks' own A/R or A/P accounts."
    ),
  )
  control_balance_cents: int | None = Field(
    None,
    description=(
      "The accounts' posted balance at the as-of date, in their natural sign; "
      "null when the entity has no control account."
    ),
  )
  open_items_cents: int = Field(
    ..., description="The entity's open items at the as-of date, intercompany included."
  )
  difference_cents: int | None = Field(
    None,
    description=(
      "Open items less the control balance; null when there is no control "
      "account to compare with."
    ),
  )


class AgedBalances(BaseModel):
  """Receivables or payables aged at a date: bucket totals, a row per
  counterparty, and each entity's tie-out to its control accounts. For a group,
  the items the members owe each other are set apart, since consolidation
  eliminates them."""

  kind: str = Field(..., description="`receivables` or `payables`.")
  as_of: date = Field(..., description="The date the items are aged at.")
  scope: str = Field(..., description="`entity` or `group`.")
  entity_id: str | None = Field(
    None, description="The entity read, or the group parent for a group."
  )
  member_entity_ids: list[str] = Field(
    ..., description="The entities summed: one, or the parent and its full members."
  )
  buckets: AgingBuckets = Field(
    ..., description="The totals by age, intercompany items excluded for a group."
  )
  agents: list[AgedAgentRow] = Field(
    ..., description="A row per counterparty, largest open balance first."
  )
  defaulted_due_date_count: int = Field(
    ...,
    description=("Open items with no due date of their own, aged as due on receipt."),
  )
  intercompany: list[OpenItem] = Field(
    default_factory=list,
    description="For a group: open items with another summed member as counterparty.",
  )
  pairs: list[IntercompanyPair] = Field(
    default_factory=list,
    description="For a group: each pair of members, both sides of what they owe each other.",
  )
  tie_outs: list[ControlTieOut] = Field(
    ..., description="Each summed entity's open items against its control accounts."
  )


class DischargeCandidates(BaseModel):
  """The open invoices or bills a payment may settle, best match first."""

  event_id: str = Field(..., description="The payment or bank line.")
  kind: str | None = Field(
    None,
    description=(
      "`receivables` for money in, `payables` for money out; null when the "
      "line has no amount to say which."
    ),
  )
  amount_cents: int = Field(..., description="The payment's magnitude.")
  classified_to_control: bool = Field(
    ...,
    description=(
      "Whether the line is classified to a receivables or payables control "
      "account, where a settlement is expected."
    ),
  )
  candidates: list[OpenItem] = Field(
    ...,
    description=(
      "Open items of the same entity and counterparty at the line's date: an "
      "exact amount match first, then the oldest due date."
    ),
  )
