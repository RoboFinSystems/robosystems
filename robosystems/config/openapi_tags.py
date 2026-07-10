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
    "name": "Schema",
    "description": "📐 Schema management - Validate and manage custom graph schemas",
  },
  {
    "name": "Graph Operations",
    "description": "⚙️ Graph lifecycle — Subgraphs, backups, tier changes, and materialization",
  },
  {
    "name": "Subgraphs",
    "description": "🌳 Subgraphs - List and inspect subgraph databases",
  },
  {
    "name": "Backup",
    "description": "💾 Backup - List, download, and inspect graph backups",
  },
  # ── Interact with the graph — query, search, AI ───────────────────────────
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
  {
    "name": "MCP",
    "description": "🔌 MCP - Model Context Protocol for AI interactions with graph data",
  },
  {
    "name": "Operator",
    "description": "🤖 AI Operators - AI agent orchestration and execution",
  },
  # ── Data & content management ─────────────────────────────────────────────
  {
    "name": "Connections",
    "description": "🔗 Connections — Manage external data source integrations",
  },
  {
    "name": "Files",
    "description": "📄 File management - Upload, track, and manage data files for generic graphs",
  },
  {
    "name": "Tables",
    "description": "🗃️ Staging tables - Table metadata and SQL queries on the staging layer",
  },
  {
    "name": "Documents",
    "description": "📑 Documents - Upload, list, and manage documents for search and analysis",
  },
  {
    "name": "Content Operations",
    "description": "✍️ Content operations - Write content across memory, documents, and files",
  },
  # ── Domain applications — extensions ──────────────────────────────────────
  {
    "name": "Extensions: GraphQL",
    "description": "🧩 GraphQL endpoint - Unified GraphQL endpoint for extensions read queries",
  },
  {
    "name": "Extensions: RoboLedger",
    "description": "📒 RoboLedger operations - Named commands for accounting & reporting writes and analytical views",
  },
  {
    "name": "Extensions: RoboInvestor",
    "description": "📈 RoboInvestor operations - Named commands for portfolio management writes and analytical views",
  },
  # ── Monitor & operate ─────────────────────────────────────────────────────
  {
    "name": "Operations",
    "description": "⏱️ Operation monitoring - Track SSE stream status and progress",
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
    "name": "Service Offerings",
    "description": "🛍️ Service offerings - View available offers and pricing",
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
    "name": "Credits",
    "description": "🪙 Credits - Manage credit-based usage and allocation",
  },
  {
    "name": "User",
    "description": "👤 User management - Profile, settings, and account information",
  },
  {
    "name": "Auth",
    "description": "🔐 Authentication - Login, register, and access token management",
  },
  # ── Platform ──────────────────────────────────────────────────────────────
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
    "description": "🔮 Vector index - Build and search vector indexes (LadybugDB HNSW + LanceDB IVF-PQ)",
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
