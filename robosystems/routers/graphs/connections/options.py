"""Connection options endpoint."""

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

# Mercury's partner referral link: the catalog's "open a Mercury account" link.
MERCURY_PARTNER_URL = "https://mercury.com/partner/robosystems"


@router.get(
  "/options",
  response_model=ConnectionOptionsResponse,
  summary="List Connection Options",
  description="Returns available providers and their requirements. Only enabled providers are included (gated by feature flags). QuickBooks and Mercury require OAuth 2.0; Plaid authorizes through Plaid Link (auth_type `link`); external connections require no auth.",
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

  # Mercury: the api_key credential mode is advertised only where the
  # deployment allows it (self-hosted / local); hosted shows OAuth alone.
  if env.CONNECTION_MERCURY_ENABLED:
    api_key_mode = bool(env.MERCURY_API_KEY_CONNECTIONS_ENABLED)
    providers.append(
      ConnectionProviderInfo(
        provider="mercury",
        display_name="Mercury",
        description=(
          "Capture every posted bank transaction from Mercury into the "
          "ledger inbox with an account suggestion attached. A bank feed "
          "is native accounting: it needs a chart of accounts and cannot "
          "sit beside a live QuickBooks connection."
        ),
        auth_type="oauth",
        auth_flow=(
          "OAuth 2.0 — you'll be redirected to Mercury to authorize read-only "
          "access for this organization"
          + (
            "; or paste a personal read-only API token to connect at once"
            if api_key_mode
            else ""
          )
        ),
        required_config=[],
        optional_config=["since_date", "include_treasury"]
        + (["api_key"] if api_key_mode else []),
        features=[
          "bank_transactions",
          "internal_transfers",
          "treasury",
          "credit_card",
          "tier0_suggestions",
        ],
        sync_frequency="On-demand; a 60-day incremental window",
        data_types=["Accounts", "Transactions", "Treasury", "Cards"],
        setup_instructions=(
          "Initialize a chart of accounts first (or sever a QuickBooks "
          "connection to keep its chart), then click 'Connect' and log in "
          "to Mercury to authorize this organization. Each bank account is "
          "linked to a chart account by name, or one is added for it. "
          "Not banking with Mercury yet? Open an account through our partner "
          f"page: {MERCURY_PARTNER_URL}"
        ),
        documentation_url=MERCURY_PARTNER_URL,
      )
    )

  # Plaid: one connection per bank login.
  if env.CONNECTION_PLAID_ENABLED:
    providers.append(
      ConnectionProviderInfo(
        provider="plaid",
        display_name="Bank account (Plaid)",
        description=(
          "Capture every posted transaction from nearly any US bank or card "
          "into the ledger inbox with an account suggestion attached. A bank "
          "feed is native accounting: it needs a chart of accounts and cannot "
          "sit beside a live QuickBooks connection."
        ),
        auth_type="link",
        auth_flow=(
          "Plaid Link — sign in to your bank inside the app and choose the "
          "accounts to share (read-only). One connection per bank login."
        ),
        required_config=[],
        optional_config=["since_date"],
        features=[
          "bank_transactions",
          "internal_transfers",
          "credit_card",
          "tier0_suggestions",
          "multi_institution",
        ],
        sync_frequency="On-demand; each sync continues from the last one",
        data_types=["Accounts", "Transactions", "Cards"],
        setup_instructions=(
          "Initialize a chart of accounts first (or sever a QuickBooks "
          "connection to keep its chart), then click 'Connect' and sign in to "
          "your bank through Plaid. Each account you share is linked to a "
          "chart account by name, or one is added for it. Connect each bank "
          "login separately."
        ),
        documentation_url="https://plaid.com/how-we-handle-data/",
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
