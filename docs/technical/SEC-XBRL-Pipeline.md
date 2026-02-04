# Getting Started with the SEC XBRL Pipeline

This guide shows you how to load SEC financial data locally and query it using the RoboSystems platform.

**Quick Start:** Run `just demo-sec` to automatically set up everything and see your data in action within minutes!

## Overview

The SEC local pipeline allows you to:

- Load company financial filings (10-K, 10-Q) by ticker symbol
- Store XBRL data in a local Ladybug graph database
- Query financial data using Cypher or the MCP client
- Inspect staged data in DuckDB before ingestion
- Query SEC filings using natural language through any MCP-compatible AI tool

## Prerequisites

Before starting, ensure you have:

- Docker running locally
- RoboSystems development environment set up
- Services started with `just start`

## Quick Start

### One-Command Setup

The fastest way to get started is with the `demo-sec` command, which handles everything automatically:

```bash
# Load NVIDIA 2025 data with automatic setup and example queries
just demo-sec

# Load specific company and year
just demo-sec AAPL 2024

# Skip running example queries after setup
just demo-sec TSLA 2023 true
```

**What happens automatically:**

1. **User Setup** - Creates demo user with credentials (or reuses existing)
2. **Download** - Fetches XBRL files from SEC EDGAR
3. **Process** - XBRL processors transform data to structured format
4. **Generate** - Creates Parquet files optimized for graph ingestion
5. **Stage** - Uploads to S3 and creates DuckDB staging tables
6. **Validate** - Automatic data quality checks and deduplication
7. **Ingest** - Direct DuckDB → Ladybug via database extension
8. **Access Grant** - Grants SEC repository access to your user
9. **Config Update** - Updates credentials with SEC graph info
10. **Example Queries** - Runs preset queries showing your data (unless skipped)

**Note:** Setup and loading takes 1-5 minutes depending on the number of years of data.

**Your credentials are saved** to `examples/credentials/config.json` with your API key and user information.

### Advanced: Manual Loading

For more control over the loading process, you can use individual commands:

```bash
# Load additional companies after initial setup
just sec-load AMD 2025
just sec-load INTC 2024

# Check database health
just sec-health

# Detailed health check with statistics
just sec-health v
```

The health check shows:

- Database connectivity status
- Number of entities (companies) loaded
- Number of reports (filings) available
- Number of facts (financial metrics)
- Data coverage by year

### Behind the Scenes

The SEC pipeline uses a staging-first architecture. Data goes through DuckDB staging tables before reaching the graph database. This enables validation, transformation, and quality checks before final ingestion.

### Explore Preset Queries

The demo command automatically runs 15 preset queries showing your data. To explore the data interactively or re-run all queries:

```bash
# Interactive mode (enter queries directly, list presets, run specific ones)
just demo-sec-query
```

In interactive mode, you can:
- Type `presets` to list all available queries
- Type `preset <name>` to run a specific query (e.g., `preset entities`)
- Enter custom Cypher queries directly
- Type `help` for available commands
- Type `quit` to exit

**Available Presets:**
- `summary` - Node and relationship counts
- `entities` - Public companies with basic info
- `recent_reports` - Most recent SEC filings
- `report_types` - Report form type counts
- `financial_facts` - Sample financial facts
- `fact_dimensions` - Dimensional qualifiers on facts
- `fact_periods` - Facts by time periods
- `elements` - Most commonly used XBRL elements
- `report_structure` - Fact counts per report
- `entity_overview` - Entities with report counts
- `fact_aspects` - Facts with all aspects (Element, Period, Unit)
- `fact_with_dimensions` - Facts with dimensional context
- `taxonomy_structures` - Report taxonomy structures
- `element_hierarchy` - Element parent-child relationships
- `report_taxonomy_detail` - Complete report taxonomy structure

## Authentication Setup

The `just demo-sec` command automatically creates user credentials for you. Your API key is saved in `examples/credentials/config.json`.

**Important:** The API key (starting with `rfs`) is your authentication credential, not to be confused with the JWT Token and Authorization Header.

**Grant Access to Existing User: (Optional):**

If you need to grant SEC repository access to an existing user:

```bash
# Grant read access to SEC repository
just repo-grant-access <user_id> sec

# Check current access
just repo-check-access <user_id>
```

**Manual User Creation (Optional):**

If you need to create additional users or manage access manually:

```bash
# Create test user with SEC repository access
just demo-user --repositories sec
```

## Accessing the Data

You have multiple options for accessing and querying the loaded SEC data:

1. **[MCP Client](#option-1-mcp-client-for-ai-agents)** - AI agent integration through Claude Desktop
2. **[Direct Cypher Queries](#option-2-direct-cypher-queries-for-development)** - Command-line Cypher queries via `just graph-query`
3. **[Query Staging Tables](#option-3-query-staging-tables-for-data-validation)** - SQL queries on DuckDB staging via `just tables-query`
4. **[Python API](#option-4-python-api-for-applications)** - Programmatic access via RoboSystems Python client
5. **[G.V() Visual Explorer](#option-5-gv-visual-graph-explorer-for-interactive-exploration)** - Interactive graph visualization (recommended tool)
6. **[SQL IDE Tools](#option-6-sql-ide-tools-for-staging-data-validation)** - Direct database access with DuckDB-compatible IDEs

### Option 1: MCP Client (For AI Agents)

Access data through any MCP-compatible AI tool (Claude Desktop, Claude Code, Cursor, Cline, etc.) using the MCP protocol.

**Setup MCP Client:**

1. Run `just demo-sec` to create credentials automatically (your API key is saved to `examples/credentials/config.json`)

2. Get your API key from the credentials file:
```bash
cat examples/credentials/config.json | grep api_key
```

3. Add to your MCP tool config. For Claude Desktop:

```json
{
  "mcpServers": {
    "robosystems": {
      "command": "npx",
      "args": ["-y", "@robosystems/mcp"],
      "env": {
        "ROBOSYSTEMS_API_URL": "http://localhost:8000",
        "ROBOSYSTEMS_API_KEY": "rfsabc123xyz...",
        "ROBOSYSTEMS_GRAPH_ID": "sec"
      }
    }
  }
}
```

**Important:** Replace `rfsabc123xyz...` with your actual API key from the test user creation step.

3. Restart your MCP-compatible AI tool

4. The MCP server provides these tools:
   - `get-graph-schema` - View available node and relationship types
   - `read-graph-cypher` - Run Cypher queries
   - `discover-properties` - Explore node properties
   - `discover-common-elements` - Find financial metrics (us-gaap tags)
   - `discover-facts` - Explore fact patterns and dimensions
   - `get-example-queries` - Get sample queries

**Example MCP Usage:**

```
You: What revenue did NVIDIA report in 2024?

The AI will use:
1. get-graph-schema to understand structure
2. discover-common-elements to find "us-gaap:Revenues"
3. read-graph-cypher to query for NVIDIA's revenue facts
```

### Option 2: Direct Cypher Queries (For Development)

Query the database directly using Cypher (no authentication required for local queries):

```bash
# List all companies in the database
just graph-query sec "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"

# Get reports for a specific company
just graph-query sec "MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) WHERE e.name CONTAINS 'NVIDIA' RETURN r.form, r.filing_date LIMIT 10"

# Find revenue facts
just graph-query sec "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) WHERE el.name = 'NetIncomeLoss' RETURN el.name, f.numeric_value, f.value LIMIT 10"
```

**Interactive Query Mode:**

For exploratory analysis, you can launch an interactive REPL for running multiple queries:

```bash
# Launch interactive mode for graph queries
just graph-query-i sec

# Or for direct Ladybug database access
just lbug-query-i sec
```

In interactive mode, you can:
- Enter Cypher queries directly without command-line escaping
- Run multiple queries in sequence
- Type `health` or `info` for database status (graph-query-i only)
- Type `quit` or `exit` to leave

All results are displayed in beautiful Rich-formatted tables.

**Common Cypher Patterns:**

```cypher
-- Find all facts for a NVDA SEC CIK
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
WHERE e.cik = '0001045810'
RETURN f

-- Get balance sheet items
MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE el.name STARTS WITH 'Assets'
RETURN el.name, f.numeric_value

-- Find facts by period
MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.instant_date = '2024-01-28'
RETURN f
```

### Option 3: Query Staging Tables (For Data Validation)

Query the DuckDB staging tables to inspect data before it reaches the graph:

```bash
# List all entities in staging
just tables-query sec "SELECT * FROM Entity LIMIT 10"

# Count facts by element
just tables-query sec "SELECT el.name, COUNT(*) as fact_count FROM Fact f JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src JOIN Element el ON fhe.dst = el.identifier GROUP BY el.name ORDER BY fact_count DESC LIMIT 20"

# Check data quality - find null values
just tables-query sec "SELECT COUNT(*) as null_count FROM Entity WHERE name IS NULL"

# Explore specific company's reports
just tables-query sec "SELECT r.* FROM Report r JOIN ENTITY_HAS_REPORT ehr ON r.identifier = ehr.dst JOIN Entity e ON ehr.src = e.identifier WHERE e.name LIKE '%NVIDIA%'"

# Check available time periods
just tables-query sec "SELECT DISTINCT instant_date FROM Period ORDER BY instant_date DESC LIMIT 10"

# Find revenue facts with full details
just tables-query sec "SELECT f.value, f.numeric_value, p.instant_date, u.measure FROM Fact f JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src JOIN Element el ON fhe.dst = el.identifier JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src JOIN Period p ON fhp.dst = p.identifier JOIN FACT_HAS_UNIT fhu ON f.identifier = fhu.src JOIN Unit u ON fhu.dst = u.identifier WHERE el.name = 'Revenues' LIMIT 10"
```

**Interactive Query Mode:**

For exploratory SQL queries on staging tables, you can launch an interactive REPL:

```bash
# Launch interactive mode for staging tables (via Graph API)
just tables-query-i sec

# Or for direct DuckDB database access
just duckdb-query-i sec
```

In interactive mode, you can:
- Enter SQL queries directly without command-line escaping
- Run multiple queries in sequence
- Explore staging tables interactively
- Type `quit` or `exit` to leave

All results are displayed in beautiful Rich-formatted tables.

**Why Query Staging Tables?**
- **Pre-ingestion validation**: Check data quality before it enters the graph
- **SQL familiarity**: Use SQL joins and aggregations you already know
- **Performance**: DuckDB's columnar storage is extremely fast for analytics
- **Debugging**: Identify issues in source data before graph ingestion

**Available Staging Tables:**
- `Entity` - Companies with SEC CIK identifiers
- `Report` - 10-K and 10-Q filings
- `Fact` - Financial metrics with values
- `Element` - us-gaap metric definitions
- `Period` - Date/duration contexts
- `Unit` - Measurement units (USD, shares, etc.)
- `FactDimension` - Segments and breakdowns

### Option 4: Python API (For Applications)

Access via the RoboSystems Python client:

```python
from robosystems_client import RoboSystemsClient
import json
from pathlib import Path

# Load API key from credentials file (created by just demo-sec)
credentials_path = Path("examples/credentials/config.json")
with open(credentials_path) as f:
    credentials = json.load(f)

# Initialize client with API key
client = RoboSystemsClient(
    api_url="http://localhost:8000",
    api_key=credentials["api_key"],  # From demo-sec command
    graph_id="sec"
)

# Run Cypher query
result = client.query_graph("""
    MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
    WHERE e.name CONTAINS 'NVIDIA'
    RETURN e.name, r.form, f.numeric_value
    LIMIT 10
""")

# Get graph schema
schema = client.get_schema()
print(schema)
```

### Option 5: G.V() Graph IDE For Interactive Exploration

[G.V()](https://gdotv.com/) is a powerful graph visualization tool and our recommended partner for visualizing Ladybug databases:

**Why G.V() for Ladybug?**
- Native Ladybug database support
- Interactive graph exploration
- Real-time Cypher query visualization
- Perfect for understanding complex financial relationships

![Example graph visualization in G.V()](images/report-graph-viz.png)

*Example: Financial report graph visualized in G.V() showing entities, reports, facts, and their relationships*

**Getting Started with G.V():**

1. Visit [https://gdotv.com/](https://gdotv.com/) or download the desktop application
2. Open the application
3. Connect to your local Ladybug database:
   - **Database Path**: `./data/lbug-dbs/sec.lbug` (from RoboSystems root directory)
4. **Enable "Fetch all edges between vertices"** in settings for richer visualizations
5. Run Cypher queries and visualize results interactively

**What you can do:**
- Visualize graph relationships interactively
- Explore entity connections visually
- Debug complex graph patterns
- Understand data model structure
- Click nodes to see properties and relationships
- Export visualizations for presentations

**Example visualization queries:**
```cypher
-- Visualize a company and its reports
MATCH (e:Entity {name: 'NVIDIA CORP'})-[:ENTITY_HAS_REPORT]->(r:Report)
RETURN e, r
LIMIT 5

-- See fact relationships
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE r.form = '10-K'
RETURN r, f, el
LIMIT 20

-- Explore full entity context
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
WHERE e.cik = '0001045810'
RETURN e, r, f
LIMIT 10
```

**Visualization Tips:**
- Start with small LIMIT values (5-20 nodes) to avoid cluttering
- Use WHERE clauses to filter to specific companies or time periods
- Click nodes to see properties
- Use the layout options to organize the graph

### Option 6: SQL IDE Tools (For Staging Data Validation)

You can open the DuckDB staging database directly in any SQL IDE that supports DuckDB:

**Database Location:**
- **Path**: `./data/staging/sec.duckdb` (from RoboSystems root directory)

**Using DuckDB CLI:**

```bash
# Connect directly to the staging database
duckdb ./data/staging/sec.duckdb

# Run SQL queries interactively
D> SELECT identifier, name, industry FROM Entity LIMIT 10;
D> .tables  # List all tables
D> .schema Entity  # Show table schema
```

**Using SQL IDE Tools:**

Any SQL IDE with DuckDB support can connect to the staging database for visual exploration and querying.

**Example queries:**
```sql
-- See all companies loaded
SELECT identifier, name, industry, ticker FROM Entity;

-- Check facts by year
SELECT
  strftime(CAST(p.instant_date AS DATE), '%Y') as year,
  COUNT(DISTINCT f.identifier) as fact_count
FROM Fact f
JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src
JOIN Period p ON fhp.dst = p.identifier
WHERE p.instant_date IS NOT NULL
GROUP BY year
ORDER BY year DESC;

-- Find largest revenue figures
SELECT
  e.name,
  f.numeric_value,
  p.instant_date
FROM Fact f
JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src
JOIN Element el ON fhe.dst = el.identifier
JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src
JOIN Period p ON fhp.dst = p.identifier
JOIN REPORT_HAS_FACT rhf ON f.identifier = rhf.dst
JOIN ENTITY_HAS_REPORT ehr ON rhf.src = ehr.dst
JOIN Entity e ON ehr.src = e.identifier
WHERE el.name = 'Revenues'
ORDER BY f.numeric_value DESC
LIMIT 10;
```

**Benefits of Direct SQL Access:**
- Browse all staging tables
- Run SQL queries with full DuckDB features
- Export query results to CSV/JSON
- View table schemas and row counts
- Data quality checks before graph ingestion

## Understanding the Data Model

The SEC graph database has the following structure:

**Node Types:**

- **Entity** - Companies (e.g., NVIDIA, Apple)
- **Report** - SEC filings (10-K, 10-Q)
- **Fact** - Individual financial metrics
- **Element** - Metric definitions (us-gaap tags)
- **Period** - Time contexts (dates, durations)
- **Unit** - Measurement units (USD, shares)
- **FactDimension** - Segments (geographic, product lines)

**Relationship Types:**

- `ENTITY_HAS_REPORT` - Links companies to their filings
- `REPORT_HAS_FACT` - Links filings to financial metrics
- `FACT_HAS_ELEMENT` - Links facts to metric definitions
- `FACT_HAS_PERIOD` - Links facts to time periods
- `FACT_HAS_UNIT` - Links facts to units
- `FACT_HAS_DIMENSION` - Links facts to segments

**Example Traversal:**

```
Entity (NVIDIA)
  -[:ENTITY_HAS_REPORT]-> Report (10-K 2024)
    -[:REPORT_HAS_FACT]-> Fact ($26.97B)
      -[:FACT_HAS_ELEMENT]-> Element (us-gaap:Revenues)
      -[:FACT_HAS_PERIOD]-> Period (2024-01-28)
      -[:FACT_HAS_UNIT]-> Unit (USD)
```

## Common Tasks

### Reset Database

If you need to start fresh:

```bash
# Reset database and clear all data
just sec-reset

# Reset and reload company
just sec-reset && just sec-load NVDA 2025
```

### Load Multiple Companies

```bash
# First company with automatic setup
just demo-sec NVDA 2025

# Load additional companies (user already created)
just sec-load AMD 2025
just sec-load INTC 2025

# Verify all loaded
just sec-health v
```

### Find Available Financial Metrics

Use the MCP client's `discover-common-elements` tool or query directly:

```bash
# Find most common us-gaap elements
just graph-query sec "MATCH (el:Element) WHERE el.qname STARTS WITH 'us-gaap:' RETURN el.qname, el.name LIMIT 20"
```

Common financial metrics:

- `us-gaap:Revenues` - Total revenue
- `us-gaap:NetIncomeLoss` - Net income
- `us-gaap:Assets` - Total assets
- `us-gaap:Liabilities` - Total liabilities
- `us-gaap:StockholdersEquity` - Shareholders' equity
- `us-gaap:EarningsPerShareBasic` - EPS (basic)

### Query by Time Period

```bash
# Find all facts from a specific date
just graph-query sec "MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period) WHERE p.instant_date = '2024-12-31' RETURN COUNT(f)"

# Find facts for a fiscal year
just graph-query sec "MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period) WHERE p.instant_date >= '2023-01-01' AND p.instant_date < '2025-01-01' RETURN COUNT(f)"
```

## Troubleshooting

### Service Not Running

If you get connection errors:

```bash
# Check services are running
docker ps

# Restart if needed
just restart
```

### Database Empty

If queries return no results:

```bash
# Verify data loaded
just sec-health

# If no data, reload
just sec-load NVDA 2025
```

### Loading Failures

If loading fails:

```bash
# Check Dagster logs (pipeline orchestration)
just logs dagster-webserver

# Check API logs
just logs api

# Reset and try again
just sec-reset
just sec-load NVDA 2025
```

## Next Steps

- **Explore Preset Queries**: Run `just demo-sec-query` to see 15 curated query examples
- **Visualize with G.V()**: Use our favorite tool [G.V()](https://gdotv.com/) to explore graph relationships interactively
- **Learn Cypher**: Explore graph query patterns with [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **Validate Staging**: Use DuckDB CLI or SQL IDEs to inspect staging tables before ingestion
- **Build Analysis**: Use MCP client in Claude for financial analysis with your auto-generated API key
- **Create Reports**: Build automated reporting with Python client
- **Add More Data**: Load additional companies with `just sec-load TICKER YEAR`

## Resources

- **Dagster Pipeline**: See `/robosystems/dagster/assets/sec/` for pipeline implementation
- **SEC Adapters**: See `/robosystems/adapters/sec/` for SEC client and processors
- **Health Check**: `just sec-health v` for diagnostic information
- **G.V()**: [https://gdotv.com/](https://gdotv.com/) - Recommended graph visualization tool for Ladybug
- **Cypher Docs**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)
- **RoboSystems API**: http://localhost:8000/docs (when running locally)
- **SEC EDGAR**: [SEC Website](https://www.sec.gov/edgar)
- **DuckDB**: [Documentation](https://duckdb.org/docs/) - SQL database for staging

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
