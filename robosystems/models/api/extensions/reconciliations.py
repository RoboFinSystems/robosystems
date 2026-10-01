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
ReconciliationMethod = Literal["source_ledger"]

# `tied`: both sides agree to the cent. `different`: both sides know the
# account and disagree. `not_in_ledger`: the source reports an account the
# ledger has no chart account for. `not_in_source`: the ledger holds a
# balance on an account the source does not have.
ReconciliationRowStatus = Literal["tied", "different", "not_in_ledger", "not_in_source"]


class PreviewReconciliationsRequest(BaseModel):
  """Compare the ledger's balances at a period end with an independent source."""

  period: str = Field(
    ...,
    description="Period to compare at its last day, as YYYY-MM.",
    examples=["2026-08"],
  )
  include_tied: bool = Field(
    False,
    description=(
      "Also return the accounts that tie. Off by default: the differences are "
      "the work, and the counts cover the rest."
    ),
  )


class ReconciliationRow(BaseModel):
  """One account: the ledger's balance, the independent balance, the difference.

  Balances are debit-positive, so a credit balance is negative on both sides.
  """

  element_id: str | None = Field(
    None, description="The chart account; null when the ledger has none for it."
  )
  account_code: str | None = None
  account_name: str
  source_account_id: str | None = Field(
    None, description="The account's id in the source system, when it has one."
  )
  statement: Literal["balance_sheet", "income_statement"] | None = Field(
    None,
    description=(
      "Balance-sheet accounts are compared cumulatively to the period end; "
      "income-statement accounts from the start of the fiscal year."
    ),
  )
  ledger_balance: float
  independent_balance: float
  difference: float = Field(..., description="Ledger minus independent.")
  status: ReconciliationRowStatus


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
