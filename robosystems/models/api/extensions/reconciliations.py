"""Reconciliation models.

A reconciliation ties an account's period-end balance in the ledger to a
balance from somewhere outside it. These models describe the comparison:
the two sides, the difference, and which accounts tie.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

# Where the independent balance comes from. `source_ledger` is the synced
# accounting system's own trial balance, which checks the mirror of its books.
# `schedule_register` is what an account's schedules say it carries.
ReconciliationMethod = Literal["source_ledger", "schedule_register"]


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
      "its schedules say it holds."
    ),
  )
  include_tied: bool = Field(
    False,
    description=(
      "Also return the accounts that tie. Off by default: the differences are "
      "the work, and the counts cover the rest."
    ),
  )


class ReconciliationComponent(BaseModel):
  """One part of an account's independent balance: what a single schedule
  says the account carries."""

  structure_id: str = Field(..., description="The schedule.")
  name: str = Field(..., description="The schedule's name.")
  amount: float = Field(
    ...,
    description=(
      "What the schedule says the account carries at the period end, debit-positive."
    ),
  )
  note: str | None = Field(
    None,
    description=(
      "Why the schedule carries nothing, when it has been disposed of or ended early."
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
  components: list[ReconciliationComponent] = Field(
    default_factory=list,
    description=(
      "`schedule_register` only: the schedules that make up the independent "
      "balance, one entry each."
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


# How far a reconciliation has got for a period. `not_started`: nothing has
# been compared. `unreconciled`: the sides differ by more than the block's
# materiality. `explained`: they differ, and items account for all of it.
# `reconciled`: nothing is left unexplained. `reviewed`: reconciled and
# signed off.
ReconciliationStatus = Literal[
  "not_started", "unreconciled", "explained", "reconciled", "reviewed"
]


class RefreshReconciliationsRequest(BaseModel):
  """Compare each reconciliation at a period end and record the result."""

  period: str = Field(
    ...,
    description="Period to reconcile at its last day, as YYYY-MM.",
    examples=["2026-08"],
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
      "the account's schedules say it carries."
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
      "`not_started`: not compared for this period. `unreconciled`: the "
      "sides differ by more than the materiality. `explained`: they differ "
      "and items account for all of it. `reconciled`: nothing is left "
      "unexplained. `reviewed`: reconciled and signed off."
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
  components: list[ReconciliationComponent] = Field(
    default_factory=list,
    description=(
      "Account-scope only: what makes up the independent balance. For "
      "`schedule_register`, one entry per schedule."
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
