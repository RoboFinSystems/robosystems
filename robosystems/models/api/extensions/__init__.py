"""Extensions API response models (ledger, investor, etc.)."""

from .account_rollups import (
  AccountRollupGroup,
  AccountRollupRow,
  AccountRollupsResponse,
)
from .accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from .closing_book import (
  ClosingBookCategory,
  ClosingBookItem,
  ClosingBookStructuresResponse,
)
from .investor import (
  CreatePortfolioBlockRequest,
  CreateSecurityRequest,
  DeletePortfolioBlockOperation,
  DeletePortfolioBlockResponse,
  HoldingResponse,
  HoldingSecuritySummary,
  HoldingsListResponse,
  PortfolioListResponse,
  PortfolioResponse,
  PositionListResponse,
  PositionResponse,
  SecurityListResponse,
  SecurityResponse,
  UpdatePortfolioBlockOperation,
  UpdateSecurityRequest,
)
from .reports import (
  AnalyticalStatementFactRow,
  CreateReportRequest,
  FactRowResponse,
  FinancialStatementAnalysisRequest,
  FinancialStatementAnalysisResponse,
  LiveFinancialStatementRequest,
  LiveFinancialStatementResponse,
  LiveStatementFactRow,
  RegenerateReportRequest,
  ReportListResponse,
  ReportResponse,
  ResolvedReportInfo,
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
  "AccountListResponse",
  "AccountResponse",
  "AccountRollupGroup",
  "AccountRollupRow",
  "AccountRollupsResponse",
  "AccountTreeNode",
  "AccountTreeResponse",
  "AnalyticalStatementFactRow",
  "ClosingBookCategory",
  "ClosingBookItem",
  "ClosingBookStructuresResponse",
  "CreatePortfolioBlockRequest",
  "CreateReportRequest",
  "CreateSecurityRequest",
  "DeletePortfolioBlockOperation",
  "DeletePortfolioBlockResponse",
  "FactRowResponse",
  "FinancialStatementAnalysisRequest",
  "FinancialStatementAnalysisResponse",
  "HoldingResponse",
  "HoldingSecuritySummary",
  "HoldingsListResponse",
  "LedgerEntryResponse",
  "LedgerLineItemResponse",
  "LedgerSummaryResponse",
  "LedgerTransactionDetailResponse",
  "LedgerTransactionListResponse",
  "LedgerTransactionSummaryResponse",
  "LiveFinancialStatementRequest",
  "LiveFinancialStatementResponse",
  "LiveStatementFactRow",
  "PortfolioListResponse",
  "PortfolioResponse",
  "PositionListResponse",
  "PositionResponse",
  "RegenerateReportRequest",
  "ReportListResponse",
  "ReportResponse",
  "ResolvedReportInfo",
  "SecurityListResponse",
  "SecurityResponse",
  "ShareReportRequest",
  "ShareReportResponse",
  "ShareResultItem",
  "StatementResponse",
  "StructureSummary",
  "TrialBalanceResponse",
  "TrialBalanceRow",
  "UpdatePortfolioBlockOperation",
  "UpdateSecurityRequest",
  "ValidationCheckResponse",
  "cents_to_dollars",
]


def cents_to_dollars(cents: int | float) -> float:
  """Convert minor currency units (cents) to dollars."""
  return float(cents) / 100.0
