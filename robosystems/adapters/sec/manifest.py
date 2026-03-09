"""
SEC shared repository manifest.

Declares the SEC EDGAR shared repository with its complete configuration:
identity, data source, schema, MCP capabilities, rate limits, and credit costs.
"""

from decimal import Decimal

from robosystems.adapters.base import SharedRepositoryManifest

SEC_MANIFEST = SharedRepositoryManifest(
  id="sec",
  name="SEC EDGAR Filings",
  description="SEC public company filings and XBRL financial data",
  data_source_type="sec_edgar",
  data_source_url="https://www.sec.gov/cgi-bin/browse-edgar",
  sync_frequency="daily",
  schema_extensions=("roboledger",),
  has_element_discovery=True,
  has_semantic_enrichment=True,
  sibling_subgraphs=("historical",),  # sec_historical subgraph
  rate_limits={
    "starter": {
      "queries_per_minute": 10,
      "queries_per_hour": 200,
      "queries_per_day": 2000,
      "mcp_queries_per_minute": 5,
      "mcp_queries_per_hour": 100,
      "mcp_queries_per_day": 1000,
      "agent_calls_per_minute": 2,
      "agent_calls_per_hour": 20,
      "agent_calls_per_day": 200,
      "downloads_per_month": 1,
    },
    "advanced": {
      "queries_per_minute": 50,
      "queries_per_hour": 1000,
      "queries_per_day": 10000,
      "mcp_queries_per_minute": 25,
      "mcp_queries_per_hour": 500,
      "mcp_queries_per_day": 5000,
      "agent_calls_per_minute": 10,
      "agent_calls_per_hour": 100,
      "agent_calls_per_day": 1000,
      "downloads_per_month": 4,
    },
  },
  plans={
    "starter": {
      "name": "Starter",
      "price_cents": 2900,
      "price_monthly": 29.0,
      "price_display": "$29/month",
      "monthly_credits": 5000,  # ~130 agent calls/month
      "access_level": "READ",
      "description": "Full SEC data access for individuals",
      "features": [
        "Unlimited MCP tool access",
        "API access",
        "Standard rate limits",
        "1 backup download per month",
      ],
    },
    "advanced": {
      "name": "Advanced",
      "price_cents": 9900,
      "price_monthly": 99.0,
      "price_display": "$99/month",
      "monthly_credits": 17000,  # ~450 agent calls/month
      "access_level": "READ",
      "description": "Higher throughput for production workloads",
      "features": [
        "Everything in Starter",
        "5x higher rate limits",
        "Production-ready throughput",
        "4 backup downloads per month",
      ],
    },
  },
  allowed_endpoints=(
    "query",
    "mcp",
    "agent",
    "schema",
    "status",
    "info",
    "describe",
    "download",
  ),
  blocked_endpoints=(
    "backup",
    "restore",
    "admin",
    "delete",
    "import",
    "write",
    "update",
    "create",
  ),
  credit_costs={
    "query": Decimal("0.0"),  # Included (rate-limited only)
    "mcp": Decimal("0.0"),  # Included (rate-limited only)
    "entity_lookup": Decimal("0.0"),  # Included (rate-limited only)
    "filing_fetch": Decimal("0.0"),  # Included (rate-limited only)
    "analytics": Decimal("0.0"),  # Included (rate-limited only)
    "ai_tokens": None,  # Dynamic — calculated from actual token usage
    "bulk_export": Decimal("50.0"),  # Bulk data export
  },
)
