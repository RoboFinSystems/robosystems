"""Extensions API response models (ledger, investor, etc.)."""

from .accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from .reports import (
  CreateReportRequest,
  FactRowResponse,
  RegenerateReportRequest,
  ReportListResponse,
  ReportResponse,
  StatementResponse,
  StructureSummary,
  ValidationCheckResponse,
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
  "CreateReportRequest",
  "FactRowResponse",
  "LedgerEntryResponse",
  "LedgerLineItemResponse",
  "LedgerSummaryResponse",
  "LedgerTransactionDetailResponse",
  "LedgerTransactionListResponse",
  "LedgerTransactionSummaryResponse",
  "RegenerateReportRequest",
  "ReportListResponse",
  "ReportResponse",
  "StatementResponse",
  "StructureSummary",
  "TrialBalanceResponse",
  "TrialBalanceRow",
  "ValidationCheckResponse",
  "cents_to_dollars",
]


def cents_to_dollars(cents: int | float) -> float:
  """Convert minor currency units (cents) to dollars."""
  return float(cents) / 100.0
