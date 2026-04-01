"""Extensions API response models (ledger, investor, etc.)."""

from .accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from .investor import (
  CreatePortfolioRequest,
  CreatePositionRequest,
  CreateSecurityRequest,
  HoldingResponse,
  HoldingSecuritySummary,
  HoldingsListResponse,
  PortfolioListResponse,
  PortfolioResponse,
  PositionListResponse,
  PositionResponse,
  SecurityListResponse,
  SecurityResponse,
  UpdatePortfolioRequest,
  UpdatePositionRequest,
  UpdateSecurityRequest,
)
from .reports import (
  CreateReportRequest,
  FactRowResponse,
  RegenerateReportRequest,
  ReportListResponse,
  ReportResponse,
  ShareReportRequest,
  ShareReportResponse,
  ShareResultItem,
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
  # RoboLedger
  "AccountListResponse",
  "AccountResponse",
  "AccountTreeNode",
  "AccountTreeResponse",
  # RoboInvestor
  "CreatePortfolioRequest",
  "CreatePositionRequest",
  "CreateReportRequest",
  "CreateSecurityRequest",
  "FactRowResponse",
  "HoldingResponse",
  "HoldingSecuritySummary",
  "HoldingsListResponse",
  "LedgerEntryResponse",
  "LedgerLineItemResponse",
  "LedgerSummaryResponse",
  "LedgerTransactionDetailResponse",
  "LedgerTransactionListResponse",
  "LedgerTransactionSummaryResponse",
  "PortfolioListResponse",
  "PortfolioResponse",
  "PositionListResponse",
  "PositionResponse",
  "RegenerateReportRequest",
  "ReportListResponse",
  "ReportResponse",
  "SecurityListResponse",
  "SecurityResponse",
  "ShareReportRequest",
  "ShareReportResponse",
  "ShareResultItem",
  "StatementResponse",
  "StructureSummary",
  "TrialBalanceResponse",
  "TrialBalanceRow",
  "UpdatePortfolioRequest",
  "UpdatePositionRequest",
  "UpdateSecurityRequest",
  "ValidationCheckResponse",
  "cents_to_dollars",
]


def cents_to_dollars(cents: int | float) -> float:
  """Convert minor currency units (cents) to dollars."""
  return float(cents) / 100.0
