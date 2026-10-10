"""Reconciliation models.

A reconciliation ties an account's period-end balance in the ledger to a
balance from somewhere outside it. These models describe the comparison:
the two sides, the difference, and which accounts tie.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, PrivateAttr

# Where the independent balance comes from. `source_ledger` is the synced
# accounting system's own trial balance, which checks the mirror of its books.
# `schedule_register` is what an account's schedules say it carries.
# `statement` is the ending balance of a statement recorded for the account.
ReconciliationMethod = Literal["source_ledger", "schedule_register", "statement"]

# Far above any real balance; keeps a typo or a non-number out of the books.
_MAX_AMOUNT = 1e13


class PreviewReconciliationsRequest(BaseModel):
  """Compare the ledger's balances at a period end with an independent source."""

  period: str = Field(
    ...,
    description="Period to compare at its last day, as YYYY-MM.",
    examples=["2026-08"],
  )
  method: ReconciliationMethod = Field(
    "source_ledger",
    description=(
      "Which check to preview. `source_ledger` compares every account with "
      "the synced accounting system's own trial balance. `schedule_register` "
      "compares each asset account a schedule carries a balance on with what "
      "its schedules say it holds. `statement` compares each account that "
      "has a statement balance recorded in the period with that balance."
    ),
  )
  include_tied: bool = Field(
    False,
    description=(
      "Also return the accounts that tie. Off by default: the differences are "
      "the work, and the counts cover the rest."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "The entity whose books to compare, by id. Omit for the group parent. "
      "`source_ledger` applies to the group parent only: QuickBooks keeps "
      "its books, not a subsidiary's."
    ),
  )


class ReconciliationComponent(BaseModel):
  """One part of an account's independent balance: what a single schedule
  says the account carries, or a recorded statement balance."""

  name: str = Field(
    ..., description="The schedule's name, or the statement and its date."
  )
  amount: float = Field(
    ..., description="What this part says the account holds, debit-positive."
  )
  structure_id: str | None = Field(
    None, description="The schedule, for a `schedule_register` part."
  )
  event_id: str | None = Field(
    None, description="The recorded balance, for a `statement` part."
  )
  document_id: str | None = Field(
    None, description="The statement document given as evidence, when one was."
  )
  note: str | None = Field(
    None,
    description=(
      "Why a schedule carries nothing (disposed of, or ended early), or the "
      "note recorded with a statement balance."
    ),
  )


class ReconciliationRow(BaseModel):
  """One account: the ledger's balance, the independent balance, the difference.

  Balances are debit-positive, so a credit balance is negative on both sides.
  """

  element_id: str | None = Field(
    None, description="The chart account; null when the ledger has none for it."
  )
  account_code: str | None = Field(None, description="The chart account's code.")
  account_name: str = Field(
    ..., description="The account's name in the ledger, else in the source."
  )
  source_account_id: str | None = Field(
    None, description="The account's id in the source system, when it has one."
  )
  statement: str | None = Field(
    None,
    description=(
      "`balance_sheet` or `income_statement`. Balance-sheet accounts are "
      "compared cumulatively to the period end; income-statement accounts "
      "from the start of the fiscal year. Null when the ledger has no account."
    ),
  )
  ledger_balance: float = Field(
    ...,
    description=(
      "What the ledger holds. For `source_ledger`, landed entries only. For "
      "`schedule_register`, the balance as the period's close will leave it: "
      "landed entries, drafts awaiting the close, and schedule entries not "
      "yet drafted."
    ),
  )
  independent_balance: float = Field(
    ..., description="What the independent source says."
  )
  difference: float = Field(..., description="Ledger minus independent.")
  status: str = Field(
    ...,
    description=(
      "`tied`: both sides agree to the cent. `different`: both know the "
      "account and disagree. `not_in_ledger`: the source reports an account "
      "the ledger has none for. `not_in_source`: the ledger holds a balance "
      "on an account the source does not have."
    ),
  )
  as_of: date | None = Field(
    None,
    description=(
      "The date both balances are stated at, when it is not the period's "
      "last day: a statement that ends mid-period is compared with the "
      "ledger at the statement's own date."
    ),
  )
  components: list[ReconciliationComponent] = Field(
    default_factory=list,
    description=(
      "Account-scope methods only: what makes up the independent balance. "
      "One entry per schedule for `schedule_register`; the recorded "
      "statement for `statement`."
    ),
  )


class ReconciliationPreviewResponse(BaseModel):
  """The comparison for one period end. Nothing is written."""

  period: str
  as_of: date = Field(..., description="The period's last day.")
  fiscal_year_start: date = Field(
    ...,
    description="Income-statement accounts are compared from this date to `as_of`.",
  )
  method: ReconciliationMethod
  source: str = Field(..., description="The system the independent side was read from.")
  report_basis: str | None = Field(
    None, description="Accounting basis the source reported on."
  )
  last_sync_at: datetime | None = Field(
    None,
    description=(
      "When the source was last synced. A difference on a sync older than the "
      "period end may be activity not yet synced, not a fault in the mirror."
    ),
  )
  accounts_compared: int = Field(
    ..., description="Accounts with a balance on either side; zero on both is left out."
  )
  accounts_tied: int
  accounts_different: int = Field(
    ..., description="Every account that does not tie, whatever the reason."
  )
  total_difference: float = Field(
    ...,
    description=(
      "Sum of the absolute differences across accounts, not a net figure: one "
      "missing transaction counts on each account it touches."
    ),
  )
  rows: list[ReconciliationRow] = Field(
    ...,
    description=(
      "Accounts that do not tie, largest difference first; tied accounts "
      "follow when `include_tied` is set."
    ),
  )
  notes: list[str] = Field(
    default_factory=list,
    description="How the comparison was made, and anything that qualifies it.",
  )
  # A fingerprint of the ledger balances a ledger-scope comparison read, kept
  # with the recorded comparison so a later read can tell the books moved.
  _ledger_digest: str | None = PrivateAttr(default=None)


# How far a reconciliation has got for a period. `not_started`: nothing has
# been compared. `stale`: the books have changed since it was compared.
# `unreconciled`: the sides differ by more than the block's materiality.
# `reconciled`: they agree within it. `reviewed`: reconciled and signed off.
ReconciliationStatus = Literal[
  "not_started", "stale", "unreconciled", "reconciled", "reviewed"
]


class RefreshReconciliationsRequest(BaseModel):
  """Compare each reconciliation at a period end and record the result."""

  period: str = Field(
    ...,
    description="Period to reconcile at its last day, as YYYY-MM.",
    examples=["2026-08"],
  )
  entity_id: str | None = Field(
    None,
    description=(
      "The entity whose books to reconcile, by id. Omit for the group parent. "
      "Each entity's reconciliations, and the close they hold, are its own."
    ),
  )


class SetReconciliationPolicyRequest(BaseModel):
  """Change how much the close cares about one reconciliation."""

  structure_id: str = Field(..., description="The reconciliation block.")
  required_for_close: bool | None = Field(
    None, description="Whether the period's close waits on it. Omit to keep."
  )
  materiality: float | None = Field(
    None,
    ge=0,
    le=_MAX_AMOUNT,
    allow_inf_nan=False,
    description=(
      "A difference up to this amount still counts as reconciled. Omit to keep."
    ),
  )
  review_required: bool | None = Field(
    None,
    description=(
      "Whether the close also waits for a sign-off, not just for the two "
      "sides to reconcile. Omit to keep."
    ),
  )
  separate_reviewer: bool | None = Field(
    None,
    description=(
      "Whether the person who signs off must be someone other than the "
      "person who ran the comparison. It can only be turned on when the "
      "graph has at least two members who can write. Omit to keep."
    ),
  )


class RecordStatementBalanceRequest(BaseModel):
  """Record the ending balance of a statement for one account."""

  element_id: str = Field(
    ...,
    description=(
      "The balance-sheet account the statement is for (a chart-of-accounts element id)."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "The entity whose books the account is in, by id. Omit for the group "
      "parent. The account must be in that entity's chart."
    ),
  )
  as_of: date = Field(
    ..., description="The statement's ending date.", examples=["2026-08-31"]
  )
  balance: float = Field(
    ...,
    ge=-_MAX_AMOUNT,
    le=_MAX_AMOUNT,
    allow_inf_nan=False,
    description=(
      "The ending balance as the statement shows it, as a positive number "
      "in the account's normal direction: money in a bank account, or the "
      "amount owed on a loan or a card. Negative for the opposite, such as "
      "an overdrawn bank account."
    ),
    examples=[18250.75],
  )
  document_id: str | None = Field(
    None,
    description=(
      "The statement itself, as a document already added with "
      "create-document. Kept on the record as evidence."
    ),
  )
  note: str | None = Field(
    None, description="Anything worth keeping with the recorded balance."
  )


class SignOffReconciliationRequest(BaseModel):
  """Sign off a reconciliation for a period as its reviewer."""

  structure_id: str = Field(..., description="The reconciliation block.")
  period: str = Field(
    ..., description="The period signed off, as YYYY-MM.", examples=["2026-08"]
  )
  note: str | None = Field(
    None, description="What the reviewer looked at, kept on the sign-off."
  )


class ReconciliationPolicyResponse(BaseModel):
  """A reconciliation's policy after a change."""

  structure_id: str
  required_for_close: bool
  materiality: float
  review_required: bool
  separate_reviewer: bool


class ReconciliationSummary(BaseModel):
  """One reconciliation's standing for a period."""

  structure_id: str = Field(..., description="The reconciliation block.")
  name: str = Field(..., description="The block's name.")
  scope: str = Field(
    ...,
    description=(
      "`ledger`: the whole ledger against one source. `account`: one account "
      "against an independent balance."
    ),
  )
  method: str = Field(
    ...,
    description=(
      "Where the independent side comes from. `source_ledger` is the synced "
      "accounting system's own trial balance. `schedule_register` is what "
      "the account's schedules say it carries. `statement` is the ending "
      "balance of a statement recorded for the account."
    ),
  )
  element_id: str | None = Field(
    None, description="The account reconciled; null for a ledger-scope block."
  )
  required_for_close: bool = Field(
    ..., description="Whether the period's close waits on this reconciliation."
  )
  materiality: float = Field(
    ..., description="A difference up to this amount still counts as reconciled."
  )
  period: str = Field(..., description="The period, as YYYY-MM.")
  as_of: date = Field(..., description="The period's last day.")
  status: str = Field(
    ...,
    description=(
      "`not_started`: not compared for this period. `stale`: the books "
      "have changed since it was compared, so run refresh-reconciliations. "
      "`unreconciled`: the sides differ by more than the materiality. "
      "`reconciled`: they agree within it. `reviewed`: reconciled and "
      "signed off."
    ),
  )
  unreconciled_difference: float | None = Field(
    None,
    description=(
      "What is left unexplained at the last comparison; null when the period "
      "has not been compared. For a ledger-scope block, the sum of every "
      "account's absolute difference. For an account-scope block, the ledger "
      "balance minus the independent one."
    ),
  )
  accounts_compared: int | None = Field(
    None, description="Ledger-scope only: accounts with a balance on either side."
  )
  accounts_different: int | None = Field(
    None, description="Ledger-scope only: accounts that do not tie."
  )
  ledger_balance: float | None = Field(
    None,
    description=(
      "Account-scope only: the account's balance at the last comparison, "
      "debit-positive, as the period's close will leave it."
    ),
  )
  independent_balance: float | None = Field(
    None,
    description=(
      "Account-scope only: what the independent source said at the last "
      "comparison, debit-positive."
    ),
  )
  balance_as_of: date | None = Field(
    None,
    description=(
      "Account-scope only: the date the two balances are stated at. The "
      "period's last day, unless a statement ended earlier in the period."
    ),
  )
  components: list[ReconciliationComponent] = Field(
    default_factory=list,
    description=(
      "Account-scope only: what makes up the independent balance. One entry "
      "per schedule for `schedule_register`; the recorded statement for "
      "`statement`."
    ),
  )
  source: str | None = Field(
    None, description="The system the independent side was read from."
  )
  compared_at: datetime | None = Field(
    None, description="When the two sides were last compared."
  )
  fact_set_id: str | None = Field(
    None, description="The FactSet holding the period's comparison."
  )
  compared_by: str | None = Field(
    None, description="The user whose action ran the last comparison."
  )
  compared_via: str | None = Field(
    None,
    description=(
      "`operation` when someone ran refresh-reconciliations; `sync` when a "
      "source sync refreshed it."
    ),
  )
  review_required: bool = Field(
    ..., description="Whether the close also waits for a sign-off."
  )
  separate_reviewer: bool = Field(
    ...,
    description=(
      "Whether the reviewer must be someone other than the person who ran "
      "the comparison."
    ),
  )
  reviewed_by: str | None = Field(
    None,
    description=(
      "The user who signed off the comparison as it stands. Null when nobody "
      "has, or when the balances changed after the sign-off."
    ),
  )
  reviewed_at: datetime | None = Field(
    None, description="When the standing sign-off was made."
  )
  self_reviewed: bool | None = Field(
    None,
    description=(
      "True when the reviewer is the person who ran the comparison they "
      "signed off. Null when there is no standing sign-off."
    ),
  )
  differences: list[ReconciliationRow] = Field(
    default_factory=list,
    description=(
      "Ledger-scope only: the accounts that did not tie at the last "
      "comparison, largest difference first."
    ),
  )


class ReconciliationListResponse(BaseModel):
  """Every reconciliation's standing for one period."""

  period: str = Field(..., description="The period, as YYYY-MM.")
  as_of: date = Field(..., description="The period's last day.")
  reconciliations: list[ReconciliationSummary] = Field(
    ..., description="One entry per reconciliation block, oldest block first."
  )
  notes: list[str] = Field(
    default_factory=list,
    description=(
      "What a refresh could not compare, such as a check skipped because its "
      "source is no longer connected. Empty on a plain read."
    ),
  )
