# RoboSystems Examples

Comprehensive examples demonstrating RoboSystems' graph database capabilities across different domains and use cases.

## Quick Start

```bash
# Make sure RoboSystems is running
just start

# Run all demos in sequence
just demo-all

# Or run individual demos
just demo-accounting
just demo-custom-graph
just demo-sec NVDA 2025
```

## Available Demos

### 1. SEC Demo - Public Company Financial Data

Query real SEC XBRL financial data from public companies.

**Features:**
- Loads SEC 10-K/10-Q filings from EDGAR
- Processes XBRL financial statements
- Queries balance sheets, income statements, and cash flows
- Demonstrates financial fact analysis

**Usage:**
```bash
# Load and query NVIDIA 2025 financials (includes queries)
just demo-sec NVDA 2025

# Load NVIDIA data without running queries
just demo-sec NVDA 2025 true

# Query SEC data with specific examples
just demo-sec-query

# Run all available query examples
just demo-sec-query true
```

**Available Tickers:**
Any publicly traded US company with SEC filings (e.g., AAPL, MSFT, GOOGL, TSLA, NVDA)

**What It Does:**
1. Fetches SEC filing from EDGAR API
2. Processes XBRL data into graph format
3. Loads entities, elements, facts, and relationships
4. Runs example queries on financial data

**Location:** `/examples/sec_demo/`

**Documentation:** See [README.md](sec_demo/README.md) for detailed guide and query examples

### 2. Accounting Demo - Double-Entry Bookkeeping

Complete accounting system with chart of accounts, transactions, and financial statements.

**Features:**
- Chart of accounts (Assets, Liabilities, Equity, Revenue, Expenses)
- Double-entry transactions with journal entries
- Multi-month transaction history
- Trial balance and financial statement queries

**Usage:**
```bash
# Run with new graph (default)
just demo-accounting

# Run with existing graph reuse
just demo-accounting "reuse-graph"

# Specify custom API URL
just demo-accounting "new-graph" "http://api.robosystems.ai"
```

**What It Creates:**
- 1 Entity (Acme Consulting LLC - fictional consulting company)
- 20 Accounts (complete chart of accounts)
- ~30 Transactions (6 months of business activity)
- ~60 Line Items (double-entry journal entries)

**Example Data:**
- Monthly rent payments ($3,000)
- Consulting revenue ($15,000-25,000/month)
- Salary expenses ($8,000/month)
- Operating expenses (utilities, supplies, marketing)

**What It Does:**
1. Sets up user credentials (or reuses existing)
2. Creates graph database with accounting schema
3. Generates realistic accounting data
4. Uploads and ingests data via staging tables
5. Runs example queries (trial balance, income statement)

**Location:** `/examples/accounting_demo/`

**Documentation:** See [README.md](accounting_demo/README.md) for step-by-step guide

### 3. Custom Graph Demo - Generic Graph Structure

Demonstrates custom schema creation with people, companies, and projects.

**Features:**
- Custom node types (Person, Company, Project)
- Custom relationships (employment, collaboration, participation)
- Flexible schema definition via JSON
- Demonstrates generic graph capabilities

**Usage:**
```bash
# Run with new graph (default)
just demo-custom-graph

# Run with existing graph reuse
just demo-custom-graph "reuse-graph"

# Specify custom API URL
just demo-custom-graph "new-graph" "http://api.robosystems.ai"
```

**What It Creates:**
- 50 Person nodes (name, age, email, interests)
- 10 Company nodes (name, industry, location, size)
- 15 Project nodes (name, description, status, budget)
- PERSON_WORKS_FOR_COMPANY relationships (employment)
- PERSON_COLLABORATES_WITH_PERSON relationships (partnerships)
- PERSON_PARTICIPATES_IN_PROJECT relationships (project teams)

**What It Does:**
1. Sets up user credentials (or reuses existing)
2. Creates graph with custom schema from schema.json
3. Generates synthetic graph data
4. Uploads and ingests via staging tables
5. Runs example queries (org charts, collaborations, projects)

**Location:** `/examples/custom_graph_demo/`

**Documentation:** See [README.md](custom_graph_demo/README.md) for step-by-step guide

**Customization:** Edit `schema.json` to define your own node types and relationships

## Credential Management

All demos share a common credential system for authentication.

**Setup Credentials:**
```bash
# Create new user and API key
just demo-user

# Create with specific details
just demo-user --name "Your Name" --email your@email.com

# Force create new credentials
just demo-user --force
```

**Credentials Location:** `/examples/credentials/config.json`

**Shared Across Demos:** All demos use the same credentials file, so you only need to run this once.

## Demo Flags

Most demos accept flags to control behavior:

**Available Flags:**
- `new-graph` - Create a new graph (default)
- `reuse-graph` - Reuse existing graph from config
- `new-user` - Create a new user
- `reuse-user` - Reuse existing user from config

**Examples:**
```bash
# Create new graph, reuse existing user
just demo-accounting "new-graph,reuse-user"

# Reuse both graph and user
just demo-custom-graph "reuse-graph,reuse-user"

# Create new graph (default behavior)
just demo-accounting
```

## Running Individual Steps

Each demo has a `main.py` that runs all steps automatically. For manual control, you can run individual numbered scripts:

**Accounting Demo:**
```bash
cd examples/accounting_demo
uv run 01_setup_credentials.py
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

**Custom Graph Demo:**
```bash
cd examples/custom_graph_demo
uv run 01_setup_credentials.py
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

**Note:** The just commands are the recommended way to run demos as they handle all setup automatically.

## Data Ingestion Pipeline

All demos follow the same data ingestion pattern:

1. **Generate Parquet Files** - Create node and relationship data
2. **Upload to S3** - Get presigned URLs and upload files
3. **Create Staging Tables** - Load Parquet files into DuckDB staging
4. **Validate Data** - Query staging tables with SQL
5. **Ingest to Graph** - Load from DuckDB into graph database

This pipeline demonstrates the production data loading workflow used by RoboSystems.

## Understanding the Output

**Successful Demo Output:**
```
✓ User authenticated
✓ Graph created: kg1a2b3c4d5e
✓ Data generated: 6 files
✓ Files uploaded to S3
✓ Staging tables created
✓ Data ingested to graph
✓ Queries executed successfully

Example Query Results:
- Trial Balance: 20 accounts
- Income Statement: Net Income $42,000
- Balance Sheet: Total Assets $125,000
```

**Common Issues:**
- "User already exists" - Use `reuse-user` flag or run with `--force`
- "Graph already exists" - Use `reuse-graph` flag
- "API connection failed" - Ensure RoboSystems is running (`just start`)
- "Permission denied" - Check credentials in config.json

## Next Steps

After running the demos:

1. **Explore the Data:**
   - Use the query examples as templates
   - Modify queries to explore different patterns
   - Try the Graph API directly via `just graph-query`

2. **Integrate with Applications:**
   - Check out the [Python Client](https://github.com/RoboFinSystems/robosystems-python-client)
   - Try the [TypeScript Client](https://github.com/RoboFinSystems/robosystems-typescript-client)
   - Explore the [MCP Client](https://github.com/RoboFinSystems/robosystems-mcp-client)

3. **Build Your Own:**
   - Use `custom_graph_demo` as a template
   - Define your own schema in JSON
   - Generate data specific to your domain
   - Load and query via the same pipeline

## Related Documentation

- **[Main README](../README.md)** - RoboSystems overview and setup
- **[API Documentation](https://api.robosystems.ai/docs)** - REST API reference
- **[Graph API README](../robosystems/graph_api/README.md)** - Graph database system
- **[Schema System](../robosystems/schemas/README.md)** - Schema definitions
- **[Wiki](https://github.com/RoboFinSystems/robosystems/wiki)** - Detailed guides and tutorials

## Support

For issues or questions:
- [GitHub Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- Check logs: `just logs robosystems-api` or `just logs robosystems-worker`
