# Getting Started with the SEC XBRL Pipeline

This guide shows you how to load SEC financial data locally and query it using the RoboSystems platform.

## Overview

The SEC local pipeline allows you to:

- Load company financial filings (10-K, 10-Q) by ticker symbol
- Store XBRL data in a local Kuzu graph database
- Query financial data using Cypher or the MCP client
- Inspect staged data in DuckDB before ingestion

## Prerequisites

Before starting, ensure you have:

- Docker running locally
- RoboSystems development environment set up
- Services started with `just start`

## Authentication Setup

For accessing the SEC data via API or MCP client, you'll need an API key.

### Create a Test User with SEC Access

```bash
# Create test user with SEC repository access
just db-create-test-user sec
```

**Important:** The API key (starting with `rfs`) is your authentication credential, not to be confused with the JWT Token and Authorization Header.

## Quick Start

### 1. Load Company Data

Load SEC filings for a company using their ticker symbol:

```bash
# Load all available years (2019-present) for NVIDIA
just sec-load NVDA

# Load specific year for Apple
just sec-load AAPL 2025

# Load Tesla data for 2023
just sec-load TSLA 2023
```

**Note:** All query examples throughout this guide use NVIDIA (NVDA) data. You can substitute any other company ticker symbol in the queries.

**What happens during loading:**

1. **Download** - Fetches XBRL files from SEC EDGAR
2. **Process** - XBRL processors transform data to structured format
3. **Generate** - Creates Parquet files optimized for graph ingestion
4. **Stage** - Uploads to S3 and creates DuckDB staging tables
5. **Validate** - Automatic data quality checks and deduplication
6. **Ingest** - Direct DuckDB → Kuzu via database extension

**Note:** Loading can take 1-5 minutes depending on the number of years.

**Behind the Scenes:**
The SEC pipeline uses a staging-first architecture. Data goes through DuckDB staging tables before reaching the graph database. This enables validation, transformation, and quality checks before final ingestion.

### 2. Verify Data Loading

Check database health and see what data is available:

```bash
# Basic health check
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

## Accessing the Data

You have multiple options for accessing and querying the loaded SEC data:

1. **[Direct Cypher Queries](#option-1-direct-cypher-queries-for-development)** - Command-line Cypher queries via `just graph-query` (no authentication)
2. **[Query Staging Tables](#option-2-query-staging-tables-for-data-validation)** - SQL queries on DuckDB staging via `just tables-query`
3. **[MCP Client](#option-3-mcp-client-for-ai-agents)** - AI agent integration through Claude Desktop
4. **[Python API](#option-4-python-api-for-applications)** - Programmatic access via RoboSystems Python client
5. **[G.V() Visual Explorer](#option-5-gv-visual-graph-explorer-for-interactive-exploration)** - Interactive graph visualization (recommended partner tool)
6. **[SQL IDE Tools](#option-6-sql-ide-tools-for-staging-data-validation)** - Direct database access with DuckDB-compatible IDEs

### Option 1: Direct Cypher Queries (For Development)

Query the database directly using Cypher (no authentication required for local queries):

```bash
# List all companies in the database
just graph-query sec "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"

# Get reports for a specific company
just graph-query sec "MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) WHERE e.name CONTAINS 'NVIDIA' RETURN r.form, r.filing_date LIMIT 10"

# Find revenue facts
just graph-query sec "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) WHERE el.name = 'NetIncomeLoss' RETURN el.name, f.numeric_value, f.value LIMIT 10"
```

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

### Option 2: Query Staging Tables (For Data Validation)

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

### Option 3: MCP Client (For AI Agents)

Access data through Claude Desktop or other AI agents using the MCP client.

**Setup MCP Client:**

1. First, create a test user and get your API key (see Authentication Setup above)

2. Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

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

3. Restart Claude Desktop

4. The MCP client provides these tools:
   - `get-graph-schema` - View available node and relationship types
   - `read-graph-cypher` - Run Cypher queries
   - `discover-properties` - Explore node properties
   - `discover-common-elements` - Find financial metrics (us-gaap tags)
   - `discover-facts` - Explore fact patterns and dimensions
   - `get-example-queries` - Get sample queries

**Example MCP Usage in Claude:**

```
You: What revenue did NVIDIA report in 2024?

Claude will use:
1. get-graph-schema to understand structure
2. discover-common-elements to find "us-gaap:Revenues"
3. read-graph-cypher to query for NVIDIA's revenue facts
```

### Option 4: Python API (For Applications)

Access via the RoboSystems Python client:

```python
from robosystems_client import RoboSystemsClient

# Initialize client with API key
client = RoboSystemsClient(
    api_url="http://localhost:8000",
    api_key="rfs_abc123xyz...",  # From test user creation
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

### Option 5: G.V() Visual Graph Explorer (For Interactive Exploration)

[G.V()](https://gdotv.com/) is a powerful graph visualization tool and our recommended partner for visualizing Kuzu databases:

**Why G.V() for Kuzu?**
- Native Kuzu database support
- Interactive graph exploration
- Real-time Cypher query visualization
- Perfect for understanding complex financial relationships
- Free and open-source

![Example graph visualization in G.V()](images/report-graph-viz.png)

*Example: Financial report graph visualized in G.V() showing entities, reports, facts, and their relationships*

**Getting Started with G.V():**

1. Visit [https://gdotv.com/](https://gdotv.com/) or download the desktop application
2. Open the application
3. Connect to your local Kuzu database:
   - **Database Path**: `./data/kuzu/sec/` (from RoboSystems root directory)
4. Run Cypher queries and visualize results interactively

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
# Load several companies for analysis
just sec-load NVDA 2025
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
# Check logs
just logs worker
just logs kuzu-api

# Reset and try again
just sec-reset
just sec-load NVDA 2025
```

## Next Steps

- **Visualize with G.V()**: Use our partner tool [G.V()](https://gdotv.com/) to explore graph relationships interactively
- **Learn Cypher**: Explore graph query patterns with [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **Validate Staging**: Use DuckDB CLI or SQL IDEs to inspect staging tables before ingestion
- **Build Analysis**: Use MCP client in Claude for financial analysis
- **Create Reports**: Build automated reporting with Python client
- **Add More Data**: Load additional companies and years

## Resources

- **G.V()**: [https://gdotv.com/](https://gdotv.com/) - Recommended graph visualization tool for Kuzu (Partner)
- **Cypher Docs**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)
- **RoboSystems API**: http://localhost:8000/docs (when running locally)
- **SEC EDGAR**: [SEC Website](https://www.sec.gov/edgar)
- **DuckDB**: [Documentation](https://duckdb.org/docs/) - SQL database for staging

## Support

- GitHub Issues: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- Documentation: See `/robosystems/tasks/sec_xbrl/README.md` for technical details
- Health Check: `just sec-health v` for diagnostic information
