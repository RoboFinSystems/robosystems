"""The group's bank and card accounts, and the operation that moves a feed's
account from one chart account to another."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class BankAccountResponse(BaseModel):
  """One bank or card account on an entity's chart, with the feed that
  books to it, if any."""

  id: str = Field(..., description="The chart account (element id).")
  code: str | None = Field(None, description="The account's code on its chart.")
  name: str = Field(..., description="The account's name on its chart.")
  kind: str = Field(..., description="`bank` or `credit`, from the balance side.")
  balance_type: str = Field(..., description="`debit` or `credit`.")
  is_active: bool = Field(..., description="False once the account is retired.")
  entity_id: str | None = Field(
    None, description="The entity whose chart the account is in; its lines book here."
  )
  entity_name: str | None = Field(None, description="That entity's name.")
  source: str | None = Field(
    None,
    description=(
      "What writes to the account: a bank feed provider (`plaid`, `mercury`), "
      "`quickbooks` for a synced account, or null for one kept by hand."
    ),
  )
  connection_id: str | None = Field(
    None, description="The connection that writes to the account, if one does."
  )
  institution: str | None = Field(
    None, description="The bank the feed account is at, as the provider names it."
  )
  feed_account_id: str | None = Field(
    None, description="The provider's id for the account, for `link-bank-account`."
  )
  feed_account_name: str | None = Field(
    None, description="The account's name at the bank, with its mask."
  )
  feed_account_kind: str | None = Field(
    None, description="The provider's account kind (`checking`, `credit card`, ...)."
  )
  connection_status: str | None = Field(
    None, description="The connection's status (`active`, `needs_reauth`, ...)."
  )
  last_sync_at: datetime | None = Field(
    None, description="When the connection last synced successfully."
  )
  last_sync_status: str | None = Field(
    None, description="The outcome of the connection's most recent sync attempt."
  )


class BankAccountListResponse(BaseModel):
  """Every bank and card account across the group, or one entity's."""

  accounts: list[BankAccountResponse] = Field(
    ..., description="The accounts, by code then name."
  )
  total: int = Field(..., description="How many accounts are listed.")


class LinkBankAccountRequest(BaseModel):
  """Point a bank feed's account at a chart account.

  Name `element_id` for an existing active account, or `entity_id` alone to
  create one in that entity's chart. The chart the account is in decides
  whose books the feed's lines go into, so this is also how a feed account
  is bound to a subsidiary. Lines still in the inbox move with it; posted
  entries stay where they were posted.
  """

  connection_id: str = Field(..., min_length=1, description="The feed's connection.")
  account_id: str = Field(
    ..., min_length=1, description="The provider's id for the account."
  )
  element_id: str | None = Field(
    None, description="The chart account to link; its chart's entity takes the feed."
  )
  entity_id: str | None = Field(
    None,
    description=(
      "With `element_id`, the entity the account must belong to. Alone, the "
      "entity in whose chart a new account is created for the feed account."
    ),
  )

  @model_validator(mode="after")
  def _one_target(self) -> LinkBankAccountRequest:
    if not self.element_id and not self.entity_id:
      raise ValueError(
        "Name element_id (an existing account) or entity_id (a new one)."
      )
    return self


class LinkBankAccountResponse(BaseModel):
  """What `link-bank-account` did."""

  connection_id: str
  provider: str
  account_id: str
  element_id: str = Field(..., description="The chart account the feed now books to.")
  previous_element_id: str
  entity_id: str = Field(..., description="The entity the feed's lines now belong to.")
  account_created: bool = Field(
    False, description="A new account was created in the entity's chart."
  )
  events_repointed: int = Field(
    0, description="Inbox lines moved to the new account (posted entries stay)."
  )
  events_unclassified: int = Field(
    0,
    description=(
      "Lines returned to `captured`: their classification named an account "
      "in the previous entity's chart."
    ),
  )
  changed: bool = True
