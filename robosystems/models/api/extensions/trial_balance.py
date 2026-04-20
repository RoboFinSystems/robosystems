"""Trial balance response models."""

from pydantic import BaseModel


class TrialBalanceRow(BaseModel):
  account_id: str
  account_code: str
  account_name: str
  classification: str | None = None
  account_type: str | None = None
  total_debits: float
  total_credits: float
  net_balance: float


class TrialBalanceResponse(BaseModel):
  rows: list[TrialBalanceRow]
  total_debits: float
  total_credits: float
