"""
Connection options endpoint.
"""

from fastapi import APIRouter, Depends, Path

from robosystems.config import env
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.connections import (
  ConnectionOptionsResponse,
  ConnectionProviderInfo,
)
from robosystems.models.core import User

router = APIRouter()


@router.get(
  "/options",
  response_model=ConnectionOptionsResponse,
  summary="List Connection Options",
  description="Returns available providers and their requirements. Only enabled providers are included (gated by feature flags). SEC requires no auth; QuickBooks requires OAuth 2.0.",
  operation_id="getConnectionOptions",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_connection_options(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> ConnectionOptionsResponse:
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

  # External integration provider (source-namespace registration)
  if env.CONNECTION_EXTERNAL_ENABLED:
    providers.append(
      ConnectionProviderInfo(
        provider="external",
        display_name="External Integration",
        description=(
          "Register a source namespace for an integration you run outside "
          "the platform. The integration writes through the public API, "
          "stamping its registered source_name on the events it emits."
        ),
        auth_type="none",
        auth_flow=(
          "No platform-held credentials — the integration authenticates to "
          "its own source system and calls the API with an API key."
        ),
        required_config=["source_name"],
        optional_config=["display_name"],
        features=["event_push"],
        sync_frequency="Push-based — the integration writes on its own schedule",
        data_types=["Events"],
        setup_instructions=(
          "Choose a source_name slug (e.g. 'salesforce'), register it, then "
          "emit events via create-event-block with source=<source_name>."
        ),
        documentation_url=None,
      )
    )

  return ConnectionOptionsResponse(providers=providers, total_providers=len(providers))
