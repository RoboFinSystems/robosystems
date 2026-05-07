"""Trial balance response models."""

from pydantic import BaseModel


class TrialBalanceRow(BaseModel):
  """One CoA account's debit/credit totals over the trial-balance window."""

  account_id: str
  account_code: str
  account_name: str
  trait: str | None = None
  account_type: str | None = None
  total_debits: float
  total_credits: float
  net_balance: float


class TrialBalanceResponse(BaseModel):
  """Trial balance for posted entries in a date range — every CoA
  account that had activity, plus aggregate totals.

  Ledger is balanced when ``total_debits == total_credits``. Used as
  a sanity check before close-period; failure means an unposted /
  malformed entry slipped through.
  """

  rows: list[TrialBalanceRow]
  total_debits: float
  total_credits: float
