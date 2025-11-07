"""OpenAPI tags configuration for RoboSystems API."""

# Main API OpenAPI tags
MAIN_API_TAGS = [
  {
    "name": "Graphs",
    "description": "🏗️ Graphs - Create and manage graph databases",
  },
  {
    "name": "Query",
    "description": "🔍 Graph queries - Execute Cypher (read only) queries against graph databases",
  },
  {
    "name": "MCP",
    "description": "🔌 MCP - Model Context Protocol for AI interactions with graph databases",
  },
  {
    "name": "Agent",
    "description": "🤖 AI Agents - Intelligent AI agents for analyzing and managing graph databases",
  },
  {
    "name": "Tables",
    "description": "🗃️ Staging tables - File upload, SQL queries, and data ingestion via DuckDB staging layer",
  },
  {
    "name": "Schema",
    "description": "📐 Schema management - Validate and manage custom graph schemas",
  },
  {
    "name": "Connections",
    "description": "🔗 Connections - Manage external service integrations and data connections",
  },
  {
    "name": "Backup",
    "description": "💾 Database backup - Create, restore, and manage graph database backups",
  },
  {
    "name": "Subgraphs",
    "description": "🌳 Subgraphs - Manage subgraph databases for version control and memory context",
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
    "name": "Subscriptions",
    "description": "💳 Subscriptions - Manage graph database subscriptions and billing",
  },
  {
    "name": "Graph Limits",
    "description": "🚧 Graph limits - Retrieve operational limits and tier-based constraints",
  },
  {
    "name": "Graph Health",
    "description": "🩺 Graph health - Monitor graph database health and performance metrics",
  },
  {
    "name": "Graph Info",
    "description": "ℹ️ Graph info - Get graph database information, statistics, and metadata",
  },
  {
    "name": "User",
    "description": "👤 User management - Profile, settings, and account information",
  },
  {
    "name": "Operations",
    "description": "⏱️ Operation monitoring - Track SSE stream status and progress",
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
    "description": "🗃️ Tables - Create and query DuckDB staging tables, ingest to graph",
  },
  {
    "name": "Copy",
    "description": "📥 Graph Copy - Copy data from S3, URLs, and other sources into graph databases",
  },
  {
    "name": "Backup",
    "description": "💽 Graph Backup - Create production-ready graph backups with multiple formats",
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
