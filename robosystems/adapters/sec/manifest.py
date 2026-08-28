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
  has_semantic_enrichment=True,
  agent_instructions=(
    "Connected to the SEC EDGAR knowledge graph (`sec`) — a curated, READ-ONLY "
    "repository of public-company XBRL financial filings. There is no general "
    "ledger here and no writes; this surface is for financial analysis and "
    "exploration.\n"
    "\n"
    "START HERE\n"
    "- `get-example-queries` — working Cypher patterns tailored to this graph's "
    "schema. Run it before writing your first query.\n"
    "- `get-graph-schema` — node types, relationships, and properties.\n"
    "\n"
    "COMMON TASKS\n"
    "- A company's financial statements → `financial-statement-analysis` (look "
    "up by ticker or CIK, with a reporting period).\n"
    "- Multidimensional fact analysis across entities / periods / dimensions → "
    "`build-fact-grid`.\n"
    '- Map a concept like "revenue" to its XBRL element qname → '
    "`resolve-element`.\n"
    "- Full-text search across filings → `search-documents`.\n"
    "- Raw graph traversal → `read-graph-cypher`.\n"
    "\n"
    "NOTES\n"
    # The `sec_historical` subgraph was advertised here until 2026-08-14 and is
    # not served: the replica stack carries `sec` only, master reads are off,
    # and the master is parked at desired 0, so every read for it fails. Do not
    # restore this line until a read actually succeeds against that graph.
    "- Raw Cypher on this graph has four rules that decide whether a number is "
    "right: `Fact {has_dimensions: false}` for consolidated totals, "
    "`Element.canonical_concept` over a single qname, a pinned `Period` shape "
    "(`duration_type` for flows, `period_type: 'instant'` for balances), and "
    "`RETURN DISTINCT` anchored on the Entity. `read-graph-cypher`'s "
    "description spells them out.\n"
    "- This is shared public data: period close, chart-of-accounts mapping, and "
    "all write operations are unavailable here."
  ),
  cypher_query_guidance=(
    "**SEC DATA RULES — a query that ignores these returns a plausible wrong "
    "number:**\n"
    "- Consolidated vs dimensional: `Fact {has_dimensions: false}` is the "
    "consolidated total; `true` is a segment / geography / member breakdown. "
    "Mixing them double-counts.\n"
    "- Canonical concepts over qnames: filter `Element.canonical_concept` "
    "('revenue', 'net_income', 'total_assets', ...) rather than one qname — "
    "filers tag the same concept differently (us-gaap:Revenues vs "
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax). "
    "`resolve-element` maps a concept to its qnames.\n"
    "- Period shape: income-statement and cash-flow facts are durations — pin "
    "`Period {duration_type: 'annual'}` (or 'quarterly'); balance-sheet facts "
    "are `Period {period_type: 'instant'}`. Quarterly, year-to-date and annual "
    "figures share end dates, so an unpinned period mixes them.\n"
    "- Anchor on the Entity (`{ticker: ...}` or CIK) or a Report first and reach "
    "`Structure`, `Element` or `Period` last — ~53k filings share a "
    "`Structure.canonical_type`, and leading with it scans them all. Always "
    "`LIMIT`.\n"
    "- `RETURN DISTINCT`: a fact is restated in later filings and sits in more "
    "than one FactSet, so an un-deduplicated result repeats rows.\n"
    "- `get-example-queries` carries these as working patterns; run it first."
  ),
  rate_limits={
    "starter": {
      "queries_per_minute": 10,
      "queries_per_hour": 200,
      "queries_per_day": 2000,
      "mcp_queries_per_minute": 5,
      "mcp_queries_per_hour": 100,
      "mcp_queries_per_day": 1000,
      "searches_per_minute": 5,
      "searches_per_hour": 50,
      "searches_per_day": 500,
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
      "searches_per_minute": 25,
      "searches_per_hour": 250,
      "searches_per_day": 2500,
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
        "MCP access from Claude, Cursor, or any MCP client",
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
    "search",
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
  },
)
