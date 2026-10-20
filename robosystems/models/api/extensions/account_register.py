"""Account register: one entity's landed lines on one account, oldest first,
with a running balance."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

from robosystems.models.api.extensions.dimensions import LineDimension


class RegisterAccount(BaseModel):
  """The account a register reads."""

  id: str = Field(..., description="The account's id.")
  code: str | None = Field(None, description="The account's code.")
  name: str = Field(..., description="The account's name.")
  balance_type: str = Field(
    ...,
    description=(
      "`debit` or `credit`: the side the account's balance is normally on, "
      "and the sign every balance in the register is read in."
    ),
  )
  trait: str | None = Field(
    None,
    description=(
      "What the account is: asset, liability, equity, revenue, expense, gain or "
      "loss, where the chart says."
    ),
  )
  temporary: bool = Field(
    ...,
    description=(
      "True for an income or expense account, whose opening balance counts "
      "from the start of the fiscal year: the close to retained earnings is "
      "computed, never posted."
    ),
  )


class RegisterLine(BaseModel):
  """One line of the register."""

  line_id: str = Field(..., description="The line item's id.")
  entry_id: str = Field(..., description="The journal entry the line is on.")
  entry_number: str | None = Field(None, description="The entry's number.")
  posting_date: date = Field(..., description="The entry's posting date.")
  entry_type: str = Field(..., description="The entry's type.")
  provenance: str | None = Field(
    None, description="Where the entry came from: native, a sync, an adjustment."
  )
  status: str = Field(
    ...,
    description=(
      "`posted`, `reversed` (posted, then reversed by `reversed_by`), or "
      "`draft` in the register's `drafts`."
    ),
  )
  memo: str | None = Field(None, description="The entry's memo.")
  description: str | None = Field(None, description="The line's description.")
  debit: float = Field(..., description="The line's debit, in dollars.")
  credit: float = Field(..., description="The line's credit, in dollars.")
  balance: float | None = Field(
    None,
    description=(
      "The account's balance after this line, in its natural sign. Null on a "
      "draft, which is never counted in it."
    ),
  )
  triggered_by_event_id: str | None = Field(
    None, description="The event whose handler wrote the entry."
  )
  triggered_by_event_type: str | None = Field(
    None, description="That event's type, such as `invoice_issued`."
  )
  reversal_of: str | None = Field(
    None, description="The entry this one reverses, when it is a reversal."
  )
  reversed_by: str | None = Field(
    None, description="The landed entry that reverses this one, when one does."
  )
  agent_id: str | None = Field(
    None, description="The counterparty the entry's event names."
  )
  agent_name: str | None = Field(None, description="The counterparty's name.")
  flow_element_id: str | None = Field(
    None, description="The cash flow or equity movement row the line is tagged to."
  )
  dimensions: list[LineDimension] = Field(
    default_factory=list,
    description="The management dimension members the line carries.",
  )


class AccountRegisterResponse(BaseModel):
  """One account's lines over a window, each with the balance it leaves.

  `closing_balance` is `opening_balance` plus the window's lines, and equals
  the trial balance's net for the same entity, account and window added to
  the opening. A page carries its own `page_opening_balance`, so reading one
  page needs none before it.
  """

  account: RegisterAccount = Field(..., description="The account read.")
  entity_id: str | None = Field(None, description="The entity whose books were read.")
  start_date: date | None = Field(
    None, description="The window's first day; null from the first line."
  )
  end_date: date | None = Field(
    None, description="The window's last day; null through the last line."
  )
  opening_from: date | None = Field(
    None,
    description=(
      "The day the opening balance counts from: the fiscal year's start for "
      "an income or expense account, null (inception) for any other."
    ),
  )
  opening_balance: float = Field(
    ..., description="The balance before the window's first line, natural sign."
  )
  total_debits: float = Field(..., description="The window's debits, in dollars.")
  total_credits: float = Field(..., description="The window's credits, in dollars.")
  closing_balance: float = Field(
    ...,
    description=(
      "The balance after the window's last line, natural sign. A window "
      "spanning fiscal years carries an income or expense account's balance "
      "across the year end rather than resetting it."
    ),
  )
  line_count: int = Field(..., description="How many lines the window holds.")
  page_opening_balance: float = Field(
    ..., description="The balance before this page's first line, natural sign."
  )
  lines: list[RegisterLine] = Field(..., description="This page's lines, oldest first.")
  next_cursor: str | None = Field(
    None, description="Pass as `after` for the next page; null on the last."
  )
  drafts: list[RegisterLine] = Field(
    default_factory=list,
    description=(
      "With `include_drafts`, on the first page only: the window's draft "
      "lines on the account, oldest first and at most a page of them, "
      "outside every balance above."
    ),
  )
  drafts_truncated: bool = Field(
    False,
    description="True when the window holds more drafts than `drafts` lists.",
  )
  sliced: bool = Field(
    False,
    description=(
      "True when read for one management dimension member, its axis's "
      "Unallocated, or one flow row: the balances are that slice's, not the "
      "account's."
    ),
  )
