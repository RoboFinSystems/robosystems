# Getting Started with the SEC XBRL Pipeline

This guide shows you how to load SEC financial data locally and query it using the RoboSystems platform.

## Overview

The SEC local pipeline allows you to:

- Load company financial filings (10-K, 10-Q) by ticker symbol
- Store XBRL data in a local Kuzu graph database
- Query financial data using Cypher or the MCP client

## Prerequisites

Before starting, ensure you have:

- Docker running locally
- RoboSystems development environment set up
- Services started with `just start` or `just compose-up robosystems`

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
just sec-load AAPL 2024

# Load Tesla data for 2023
just sec-load TSLA 2023
```

**What happens during loading:**

1. **Download** - Fetches XBRL files from SEC EDGAR
2. **Process** - Converts XBRL to parquet format
3. **Consolidate** - Combines files for optimal performance
4. **Ingest** - Loads data into Kuzu graph database

**Note:** Loading can take 1-5 minutes depending on the number of years.

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

You have three options for querying the loaded SEC data:

### Option 1: Direct Cypher Queries (For Development)

Query the database directly using Cypher (no authentication required for local queries):

```bash
# List all companies in the database
just kuzu-query sec "MATCH (e:Entity) RETURN e.name, e.identifier LIMIT 10"

# Get reports for a specific company
just kuzu-query sec "MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) WHERE e.name CONTAINS 'NVIDIA' RETURN r.form, r.filing_date LIMIT 10"

# Find revenue facts
just kuzu-query sec "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element) WHERE el.qname = 'us-gaap:Revenues' RETURN f.numeric_value, f.value LIMIT 10"
```

**Common Cypher Patterns:**

```cypher
-- Find all facts for a company
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
WHERE e.identifier = '0001045810'  -- NVIDIA's CIK
RETURN f

-- Get balance sheet items
MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE el.qname STARTS WITH 'us-gaap:Assets'
RETURN el.qname, el.name, f.numeric_value

-- Find facts by period
MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.instant_date = '2024-01-28'
RETURN f
```

### Option 2: MCP Client (For AI Agents)

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

### Option 3: Python API (For Applications)

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

**Testing with curl:**

```bash
# Use your API key in the X-API-Key header
curl -X POST "http://localhost:8000/v1/graphs/sec/query" \
  -H "X-API-Key: rfsabc123xyz..." \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (e:Entity) RETURN e.name LIMIT 5"
  }'
```

**Note:** Always use the `X-API-Key` header with your API key, not `Authorization: Bearer` tokens.

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
just sec-reset-local

# Reset and reload company
just sec-reset-local && just sec-load NVDA 2024
```

### Load Multiple Companies

```bash
# Load several companies for analysis
just sec-load NVDA 2024
just sec-load AMD 2024
just sec-load INTC 2024

# Verify all loaded
just sec-health v
```

### Find Available Financial Metrics

Use the MCP client's `discover-common-elements` tool or query directly:

```bash
# Find most common us-gaap elements
just kuzu-query sec "MATCH (el:Element) WHERE el.qname STARTS WITH 'us-gaap:' RETURN el.qname, el.name LIMIT 20"
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
just kuzu-query sec "MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period) WHERE p.instant_date = '2024-01-28' RETURN COUNT(f)"

# Find facts for a fiscal year
just kuzu-query sec "MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period) WHERE p.instant_date >= '2023-01-01' AND p.instant_date < '2024-01-01' RETURN COUNT(f)"
```

## Troubleshooting

### Service Not Running

If you get connection errors:

```bash
# Check services are running
docker ps

# Restart if needed
just restart robosystems
```

### Database Empty

If queries return no results:

```bash
# Verify data loaded
just sec-health

# If no data, reload
just sec-load NVDA 2024
```

### Loading Failures

If loading fails:

```bash
# Check logs
just logs worker
just logs kuzu-api

# Reset and try again
just sec-reset-local
just sec-load NVDA 2024
```

## Next Steps

- **Learn Cypher**: Explore graph query patterns
- **Build Analysis**: Use MCP client in Claude for financial analysis
- **Create Reports**: Build automated reporting with Python client
- **Add More Data**: Load additional companies and years

## Resources

- **Kuzu Cypher Documentation**: [Kuzu Cypher Manual](https://docs.kuzudb.com/cypher/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)
- **RoboSystems API**: http://localhost:8000/docs (when running locally)
- **SEC EDGAR**: [SEC Website](https://www.sec.gov/edgar)

## Support

- GitHub Issues: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- Documentation: See `/robosystems/tasks/sec_xbrl/README.md` for technical details
- Health Check: `just sec-health v` for diagnostic information
