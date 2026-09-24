"""OpenAPI tags configuration for RoboSystems API.

Tag order controls the Swagger UI sidebar order. Both lists are ordered
**product-first** — lead with the knowledge graph and how you interact with it,
then data/content management, then the supporting account/meta surfaces — and
grouped with section comments so the ordering survives the next tag addition
(add new tags inside the right group, not at the end).
"""

# Main API OpenAPI tags
MAIN_API_TAGS = [
  # ── The knowledge graph — create & configure ──────────────────────────────
  {
    "name": "Graphs",
    "description": "🏗️ Graphs - Create and manage knowledge graph tenants",
  },
  {
    "name": "Graph Operations",
    "description": "⚙️ Graph lifecycle — Subgraphs, backups, tier changes, and materialization",
  },
  {
    "name": "Schema",
    "description": "📐 Schema management - Validate and manage custom graph schemas",
  },
  {
    "name": "Subgraphs",
    "description": "🌳 Subgraphs - List and inspect subgraph databases",
  },
  {
    "name": "Graph Members",
    "description": "🧑‍🤝‍🧑 Graph members - Manage per-graph member access and roles",
  },
  {
    "name": "Backup",
    "description": "💾 Backup - List, download, and inspect graph backups",
  },
  # ── Interact with the graph — query, search, AI ───────────────────────────
  {
    "name": "Operator",
    "description": "🤖 AI Operators - AI agent orchestration and execution",
  },
  {
    "name": "Query",
    "description": "🕸️ Graph queries - Execute Cypher queries on the knowledge graph",
  },
  {
    "name": "Search",
    "description": "🔎 Search - Full-text and semantic search on documents, narratives and disclosures",
  },
  {
    "name": "Memory",
    "description": "🧠 Memory - Recall, list, and inspect the graph's per-graph semantic memory store",
  },
  # ── Data & content management ─────────────────────────────────────────────
  {
    "name": "Content Operations",
    "description": "✍️ Content operations - Write content across memory, documents, and files",
  },
  {
    "name": "Connections",
    "description": "🔗 Connection management — Manage external data source integrations",
  },
  {
    "name": "Documents",
    "description": "📑 Documents - List documents for search and analysis",
  },
  {
    "name": "Files",
    "description": "📄 File management - List stored data files for generic graphs",
  },
  {
    "name": "Tables",
    "description": "🗃️ Staging tables - Table metadata for the staging layer",
  },
  # ── Domain applications — extensions ──────────────────────────────────────
  #
  # RoboLedger has one tag per workflow stage, in the order a ledger lives
  # through them; a new operation picks its stage.
  {
    "name": "GraphQL",
    "description": "🧩 GraphQL endpoint - Unified GraphQL endpoint for extensions read queries",
  },
  {
    "name": "RoboLedger: Setup",
    "description": "📒 Ledger setup - Initialize a ledger, seed its chart of accounts, and edit the reporting entity",
  },
  {
    "name": "RoboLedger: Taxonomy & Mapping",
    "description": "🏷️ Taxonomy & mapping - Curate taxonomy blocks and map the chart of accounts to their elements",
  },
  {
    "name": "RoboLedger: Information Blocks",
    "description": "🧱 Information blocks - Author the units a report is built from, and compute rules, metrics and forecasts over them",
  },
  {
    "name": "RoboLedger: Ledger & Events",
    "description": "📓 Ledger & events - Agents, business events, the handlers that post them to the GL, and journal entry corrections",
  },
  {
    "name": "RoboLedger: Fiscal Close",
    "description": "📅 Fiscal close - Close and reopen periods, and drive the schedules that feed the close",
  },
  {
    "name": "RoboLedger: Reports",
    "description": "📄 Reports - Build, rebuild, and file a financial report",
  },
  {
    "name": "RoboLedger: Report Distribution",
    "description": "📬 Report distribution - Share reports, manage publish lists, and block unwanted senders",
  },
  {
    "name": "RoboLedger: Analytical Views",
    "description": "🔬 Analytical views - Read-shaped operations over the XBRL hypercube and published reports: fact grids, statements, and disclosures",
  },
  {
    "name": "RoboInvestor",
    "description": "📈 RoboInvestor operations - Named commands for portfolio management writes and analytical views",
  },
  # ── Monitor & operate ─────────────────────────────────────────────────────
  {
    "name": "Operations",
    "description": "⏱️ Operation monitoring - Track SSE stream status and progress",
  },
  {
    "name": "Credits",
    "description": "🪙 Credits - Manage credit-based usage and allocation",
  },
  {
    "name": "Usage",
    "description": "📊 Usage - Monitor usage, metrics, and system performance",
  },
  {
    "name": "Graph Limits",
    "description": "📏 Graph limits - Storage usage, operation limits, and tier configuration",
  },
  {
    "name": "Graph Health",
    "description": "🩺 Graph health - Database health and performance metrics",
  },
  {
    "name": "Graph Info",
    "description": "ℹ️ Graph info - Database metadata and statistics",
  },
  # ── Account, teams & billing ──────────────────────────────────────────────
  {
    "name": "Org",
    "description": "🏢 Organizations - Manage organizations and team collaboration",
  },
  {
    "name": "Org Members",
    "description": "👥 Organization members - Manage team members, roles, and permissions",
  },
  {
    "name": "Org Usage",
    "description": "📈 Organization usage - Track organization-wide usage, limits, and analytics",
  },
  {
    "name": "Subscriptions",
    "description": "💳 Subscriptions - Shared repository subscription management",
  },
  {
    "name": "Billing",
    "description": "🛒 Billing - Create and manage billing checkout sessions",
  },
  {
    "name": "User",
    "description": "👤 User management - Profile, settings, and account information",
  },
  #
  # Auth keeps the sign-in story; the self-contained mechanisms below are
  # carved out. The parent is not renamed: tags are Python SDK paths, and
  # `api/auth/` must stay put.
  {
    "name": "Auth",
    "description": "🔐 Authentication - Register, sign in, manage the session, verify email, and reset passwords",
  },
  {
    "name": "Auth: Passkeys",
    "description": "🔑 Passkeys - WebAuthn registration, passwordless login, and re-authentication",
  },
  {
    "name": "Auth: MFA",
    "description": "🛡️ Multi-factor auth - Assertion options, verification, status, and recovery codes",
  },
  {
    "name": "Auth: SSO",
    "description": "🎫 Single sign-on - Cross-app token exchange, and enterprise OIDC where enabled",
  },
  # ── Platform ──────────────────────────────────────────────────────────────
  {
    "name": "Service Offerings",
    "description": "🛍️ Service offerings - View available offers and pricing",
  },
  {
    "name": "Status",
    "description": "❤️ Service status - API status and monitoring",
  },
]

# Graph API OpenAPI tags
GRAPH_API_TAGS = [
  # ── Graph — manage, schema, query ─────────────────────────────────────────
  {
    "name": "Graph Management",
    "description": "💾 Graph management - Create, list, delete, and manage graph databases",
  },
  {
    "name": "Graph Schema",
    "description": "📋 Graph schema - Retrieve and install graph schemas",
  },
  {
    "name": "Graph Query",
    "description": "🔍 Graph query - Execute Cypher queries against a specific graph database",
  },
  # ── Data & retrieval surfaces ─────────────────────────────────────────────
  {
    "name": "Tables",
    "description": "🗃️ Staging Tables - Create and query DuckDB staging tables, ingest to graph",
  },
  {
    "name": "Vector Index",
    "description": "🔮 Vector index - Build LadybugDB HNSW indexes (searched in Cypher via QUERY_VECTOR_INDEX)",
  },
  {
    "name": "Semantic Memory",
    "description": "🧠 Semantic memory - Per-graph LanceDB store for remember/recall/forget",
  },
  # ── Data movement & resources ─────────────────────────────────────────────
  {
    "name": "Backup",
    "description": "💽 Graph Backup - Create production-ready graph backups with multiple formats",
  },
  {
    "name": "Migration",
    "description": "🔄 Migration - Export and import databases for LadybugDB version upgrades",
  },
  {
    "name": "Memory",
    "description": "🧮 Memory boost - Temporarily boost RAM for staging and materialization",
  },
  # ── Monitoring ────────────────────────────────────────────────────────────
  {
    "name": "Metrics",
    "description": "📈 Graph metrics - Monitor graph usage and performance",
  },
  {
    "name": "Tasks",
    "description": "⏱️ Task management - Monitor background tasks and queue operations",
  },
  {
    "name": "Cluster Metrics",
    "description": "📊 Cluster metrics - Cluster level metrics and performance data",
  },
  {
    "name": "Cluster Info",
    "description": "🖥️ Cluster information - Get cluster metadata and configuration details",
  },
  {
    "name": "Cluster Health",
    "description": "❤️ Cluster health monitoring - Overall cluster status and health checks",
  },
]
