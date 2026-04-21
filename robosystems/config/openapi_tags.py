"""OpenAPI tags configuration for RoboSystems API."""

# Main API OpenAPI tags
MAIN_API_TAGS = [
  {
    "name": "Graphs",
    "description": "🏗️ Graphs - Create and manage knowledge graph tenants",
  },
  {
    "name": "MCP",
    "description": "🔌 MCP - Model Context Protocol for AI interactions with graph data",
  },
  {
    "name": "Agent",
    "description": "🤖 AI Agents - Intelligent AI agents for analyzing and managing graph data",
  },
  {
    "name": "Query",
    "description": "🕸️ Graph queries - Execute Cypher queries on the knowledge graph",
  },
  {
    "name": "Tables",
    "description": "🗃️ Staging tables - Table metadata and SQL queries on the staging layer",
  },
  {
    "name": "Files",
    "description": "📄 File management - Upload, track, and manage data files for generic graphs",
  },
  {
    "name": "Documents",
    "description": "📑 Documents - Upload, list, and manage documents for search and analysis",
  },
  {
    "name": "Search",
    "description": "🔎 Search - Full-text and semantic search on documents, narratives and disclosures",
  },
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
  {
    "name": "Graph Operations",
    "description": "⚙️ Graph lifecycle — Subgraphs, backups, tier changes, and materialization",
  },
  {
    "name": "Connections",
    "description": "🔗 Connections — Manage external data source integrations",
  },
  {
    "name": "Subscriptions",
    "description": "💳 Subscriptions - Shared repository subscription management",
  },
  {
    "name": "Subgraphs",
    "description": "🌳 Subgraphs - List and inspect subgraph databases",
  },
  {
    "name": "Backup",
    "description": "💾 Backup - List, download, and inspect graph backups",
  },
  {
    "name": "Schema",
    "description": "📐 Schema management - Validate and manage custom graph schemas",
  },
  {
    "name": "Usage",
    "description": "📊 Usage - Monitor usage, metrics, and system performance",
  },
  {
    "name": "Credits",
    "description": "🪙 Credits - Manage credit-based usage and allocation",
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
  {
    "name": "Operations",
    "description": "⏱️ Operation monitoring - Track SSE stream status and progress",
  },
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
    "name": "Billing",
    "description": "🛒 Billing - Create and manage billing checkout sessions",
  },
  {
    "name": "User",
    "description": "👤 User management - Profile, settings, and account information",
  },
  {
    "name": "Auth",
    "description": "🔐 Authentication - Login, register, and access token management",
  },
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
  {
    "name": "Graph Management",
    "description": "💾 Graph management - Create, list, delete, and manage graph databases",
  },
  {
    "name": "Graph Query",
    "description": "🔍 Graph query - Execute Cypher queries against a specific graph database",
  },
  {
    "name": "Graph Schema",
    "description": "📋 Graph schema - Retrieve and install graph schemas",
  },
  {
    "name": "Tables",
    "description": "🗃️ Staging Tables - Create and query DuckDB staging tables, ingest to graph",
  },
  {
    "name": "Vector Index",
    "description": "🔮 Vector index - Build, search, and manage LanceDB vector indexes for similarity search",
  },
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
    "description": "🧠 Memory management - Temporarily boost memory allocation for staging and materialization",
  },
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
