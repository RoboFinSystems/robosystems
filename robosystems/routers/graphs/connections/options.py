"""
Connection options endpoint.
"""

from fastapi import APIRouter, Depends, Path

from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.connection import (
  ConnectionProviderInfo,
  ConnectionOptionsResponse,
)
from robosystems.models.api.common import ErrorResponse
from robosystems.config import env

router = APIRouter()


@router.get(
  "/options",
  response_model=ConnectionOptionsResponse,
  summary="List Connection Options",
  description="""Get metadata about all available data connection providers.

This endpoint returns comprehensive information about each supported provider:

**SEC EDGAR**: Public entity financial filings
- No authentication required (public data)
- 10-K, 10-Q, 8-K reports with XBRL data
- Historical and real-time filing access

**QuickBooks Online**: Full accounting system integration
- OAuth 2.0 authentication
- Chart of accounts, transactions, trial balance
- Real-time sync capabilities

**Plaid**: Bank account connections
- Secure bank authentication via Plaid Link
- Transaction history and balances
- Multi-account support

No credits are consumed for viewing connection options.""",
  operation_id="getConnectionOptions",
  responses={
    200: {
      "description": "Connection options retrieved successfully",
      "model": ConnectionOptionsResponse,
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve options", "model": ErrorResponse},
  },
)
async def get_connection_options(
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> ConnectionOptionsResponse:
  """
  Get metadata about all available connection providers.

  Returns detailed information about each provider including:
  - Authentication requirements
  - Configuration fields
  - Supported features
  - Data types available

  Only returns providers that are enabled via feature flags.
  """
  providers = []

  # SEC EDGAR provider
  if env.CONNECTION_SEC_ENABLED:
    providers.append(
      ConnectionProviderInfo(
        provider="sec",
        display_name="SEC EDGAR",
        description="Connect to SEC EDGAR database for public entity financial filings",
        auth_type="none",
        auth_flow="No authentication required - public data source",
        required_config=["cik"],
        optional_config=["entity_name"],
        features=[
          "xbrl_parsing",
          "10k_10q_import",
          "real_time_filings",
          "historical_data",
        ],
        sync_frequency="Daily for new filings",
        data_types=["10-K", "10-Q", "8-K", "DEF 14A", "20-F", "XBRL Financial Data"],
        setup_instructions="Enter the entity's 10-digit CIK (Central Index Key) number. The CIK can be found on SEC.gov.",
        documentation_url="https://www.sec.gov/edgar/searchedgar/entitysearch",
      )
    )

  # QuickBooks provider
  if env.CONNECTION_QUICKBOOKS_ENABLED:
    providers.append(
      ConnectionProviderInfo(
        provider="quickbooks",
        display_name="QuickBooks Online",
        description="Sync accounting data from QuickBooks Online",
        auth_type="oauth",
        auth_flow="OAuth 2.0 - You'll be redirected to QuickBooks to authorize access",
        required_config=["entity_id"],
        optional_config=["sync_start_date", "account_filter"],
        features=[
          "trial_balance",
          "chart_of_accounts",
          "transactions",
          "invoices",
          "bills",
          "journal_entries",
        ],
        sync_frequency="On-demand or scheduled daily",
        data_types=[
          "Chart of Accounts",
          "Trial Balance",
          "General Ledger",
          "AR/AP",
          "Bank Transactions",
        ],
        setup_instructions="Click 'Connect' to authorize access to your QuickBooks Online entity. You'll need QuickBooks admin permissions.",
        documentation_url="https://developer.intuit.com/app/developer/qbo/docs/get-started",
      )
    )

  # Plaid provider
  if env.CONNECTION_PLAID_ENABLED:
    providers.append(
      ConnectionProviderInfo(
        provider="plaid",
        display_name="Bank Connections (Plaid)",
        description="Connect to bank accounts for transaction data via Plaid",
        auth_type="link",
        auth_flow="Plaid Link - Secure bank authentication flow",
        required_config=["entity_id"],
        optional_config=["account_ids", "start_date"],
        features=[
          "bank_transactions",
          "account_balances",
          "transaction_categorization",
          "multiple_accounts",
        ],
        sync_frequency="Daily automatic updates",
        data_types=[
          "Bank Transactions",
          "Account Balances",
          "Transaction Categories",
          "Merchant Data",
        ],
        setup_instructions="Click 'Connect' to open Plaid Link and securely connect your bank accounts.",
        documentation_url="https://plaid.com/docs/",
      )
    )

  return ConnectionOptionsResponse(providers=providers, total_providers=len(providers))
