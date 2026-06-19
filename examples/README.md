# RoboSystems Examples

Comprehensive examples demonstrating RoboSystems' graph database capabilities across different domains and use cases.

## Quick Start

```bash
# Make sure RoboSystems is running
just start

# Run all demos in sequence (roboledger → custom-graph → sec)
just demo

# Or run individual demos
just demo-roboledger
just demo-custom-graph
just demo-sec --ticker NVDA --year 2025
just demo-seattle-method
just demo-world-online
```

## Available Demos

### 1. RoboLedger Demo - End-to-End Accounting Workflow

Full RoboLedger workflow on synthetic data for a boutique consulting firm: bulk OLTP import, taxonomy and schedule blocks, fiscal calendar, a filed annual report, and an AI-driven month-end close. All data flows in through the same HTTP API the frontend UI and MCP tools use.

**Features:**

- Generates a rolling 16-month window ending at the current month
- Creates a chart of accounts, REA Agents (customers/vendors/employees), and a typed business-event stream
- Initializes a fiscal calendar with exactly one period ready to close on first run
- Optional MappingOperator (`--ai`) to map the chart of accounts via the AI Operator (requires Bedrock)

**Usage:**

```bash
# Run the full demo (creates graph, loads data, creates schedules, uploads policies)
just demo-roboledger

# Load into an existing graph
just demo-roboledger <graph_id>

# Skeleton: create user + empty roboledger graph only (then connect a real QuickBooks sandbox manually)
just demo-roboledger --skeleton

# Use the MappingOperator instead of hardcoded mappings (requires Bedrock)
just demo-roboledger --ai

# Validate data only
just demo-roboledger --dry-run
```

**Location:** `/examples/roboledger_demo/`

**Documentation:** See [README.md](roboledger_demo/README.md) for the full walkthrough

### 2. SEC Demo - Public Company Financial Data

Query real SEC XBRL financial data from public companies.

**Features:**

- Loads SEC 10-K/10-Q filings from EDGAR
- Processes XBRL financial statements
- Queries balance sheets, income statements, and cash flows
- Demonstrates financial fact analysis

**Usage:**

```bash
# Load and query NVIDIA 2025 financials (includes queries)
just demo-sec --ticker NVDA --year 2025

# Load NVIDIA data without running queries
just demo-sec --ticker NVDA --year 2025 --skip-queries

# Query SEC data with specific examples
just demo-sec-query

# Run all available query examples
just demo-sec-query --all
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

# Create new user and graph
just demo-custom-graph --new-user --new-graph

# Skip verification queries
just demo-custom-graph --skip-queries
```

**What It Creates:**

- 50 Person nodes (name, age, email, interests)
- 10 Company nodes (name, industry, location, size)
- 15 Project nodes (name, description, status, budget)
- PERSON_WORKS_FOR_COMPANY relationships (employment)
- PERSON_WORKS_ON_PROJECT relationships (project teams)
- COMPANY_SPONSORS_PROJECT relationships (sponsorship)

**What It Does:**

1. Sets up user credentials (or reuses existing)
2. Creates graph with custom schema from schema.json
3. Generates synthetic graph data
4. Uploads and ingests via staging tables
5. Runs example queries (org charts, collaborations, projects)

**Location:** `/examples/custom_graph_demo/`

**Documentation:** See [README.md](custom_graph_demo/README.md) for step-by-step guide

**Customization:** Edit `schema.json` to define your own node types and relationships

### 4. Seattle Method Demo - Record-to-Report (Charlie Hoffman's "mini")

End-to-end accounting proof against Charlie Hoffman's Seattle Method "mini" record-to-report test case: ingest a 14-JE general journal (lemonade stand), map the chart of accounts to rs-gaap, and render a validated 4-statement report.

**Features:**

- Ingests Charlie's `mini`-tagged general-journal transactions
- Cross-taxonomy projection: `mini` → rs-gaap (reconciles 18/18 concepts; BS balances $14,450)
- Materializes a 4-Information-Block rs-gaap Report (BS / IS / CF / SE)
- Reconciliation report vs. Charlie's published reference

**Usage:**

```bash
# Run the full demo (ingest → map → render)
just demo-seattle-method

# Render the reconciliation report for an existing graph
just demo-seattle-method-reconcile

# Materialize the 4-statement rs-gaap report
just demo-seattle-method-create-report
```

**Location:** `/examples/seattle_method_demo/`

**Documentation:** See [README.md](seattle_method_demo/README.md) for the walkthrough

### 5. World Online Demo - Seattle Method at Scale

The scaled-up sibling of the `mini` demo: Charlie Hoffman's "The World Online" dataset — a real-size general ledger tagged against MINI 2026.

**Features:**

- 22,288 GL lines / 3,389 entries / 239-account chart of accounts
- Opening balances ingested as ordinary BBF transactions (`mini:OpeningBalance` as a first-class flow concept), not synthesized as a prior-period number
- Reconciles 22/23 vs. the source pivot; BS balances $0.00; trial balance balances
- Same `load_taxonomy` / `seed_mappings` / report-render helpers as the `mini` demo

**Usage:**

```bash
# Run the full demo
just demo-world-online

# Render the reconciliation report (pivot vs. SummaryOfTransactions.csv)
just demo-world-online-reconcile

# Materialize the 4-statement rs-gaap report
just demo-world-online-create-report

# Render the trial balance
just demo-world-online-trial-balance
```

**Location:** `/examples/seattle_method_world_online/`

**Documentation:** See [README.md](seattle_method_world_online/README.md) for the walkthrough

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

**Credentials Location:** `.local/config.json`

**Shared Across Demos:** All demos use the same credentials file, so you only need to run this once.

## Demo Flags

All demo recipes pass flags through to the underlying script. Default behavior reuses existing credentials and graph.

**Custom Graph Demo flags:**

- `--new-user` - Create a new user (implies `--new-graph`)
- `--new-graph` - Create a new graph
- `--skip-queries` - Skip verification queries after ingestion

**Examples:**

```bash
# Default: reuse existing user and graph, regenerate data
just demo-custom-graph

# Create new graph for existing user
just demo-custom-graph --new-graph

# Create new user and graph
just demo-custom-graph --new-user --new-graph

# Skip queries
just demo-custom-graph --skip-queries
```

## Running Individual Steps

Each demo has a `main.py` that runs all steps automatically. For manual control, you can run individual numbered scripts:

**Custom Graph Demo:**

```bash
cd examples/custom_graph_demo
uv run setup_credentials.py
uv run create_graph.py
uv run generate_data.py
uv run upload_ingest.py
uv run query_graph.py --all
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

```text
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

- "User already exists" - Default behavior reuses existing user, or use `--new-user` flag
- "Graph already exists" - Default behavior reuses existing graph, or use `--new-graph` flag
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
- Check logs: `just logs api` or `just logs worker`
