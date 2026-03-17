"""Ledger API response models."""

from .accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from .summary import LedgerSummaryResponse
from .transactions import (
  LedgerEntryResponse,
  LedgerLineItemResponse,
  LedgerTransactionDetailResponse,
  LedgerTransactionListResponse,
  LedgerTransactionSummaryResponse,
)
from .trial_balance import TrialBalanceResponse, TrialBalanceRow

__all__ = [
  "AccountListResponse",
  "AccountResponse",
  "AccountTreeNode",
  "AccountTreeResponse",
  "LedgerEntryResponse",
  "LedgerLineItemResponse",
  "LedgerSummaryResponse",
  "LedgerTransactionDetailResponse",
  "LedgerTransactionListResponse",
  "LedgerTransactionSummaryResponse",
  "TrialBalanceResponse",
  "TrialBalanceRow",
  "cents_to_dollars",
]


def cents_to_dollars(cents: int) -> float:
  """Convert minor currency units (cents) to dollars."""
  return cents / 100.0
