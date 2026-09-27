# Getting Started with the SEC XBRL Pipeline

This guide shows you how to load SEC financial data locally and query it using the RoboSystems platform.

**Quick Start:** Run `just demo-sec` to automatically set up everything and see your data in action within minutes!

## Overview

The SEC local pipeline allows you to:

- Load company financial filings (10-K, 10-Q) by ticker symbol
- Or pull the entire prebuilt corpus from Hugging Face with `just sec-dump`
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
# Load NVIDIA 2025 data with automatic setup and example queries (defaults)
just demo-sec

# Load specific company and year
just demo-sec --ticker AAPL --year 2024

# Skip running example queries after setup
just demo-sec --ticker TSLA --year 2023 --skip-queries
```

**What happens automatically:**

1. **User Setup** - Creates demo user with credentials (or reuses existing)
2. **Download** - Fetches XBRL files from SEC EDGAR
3. **Process** - XBRL processors transform data to structured format
4. **Generate** - Creates Parquet files optimized for graph ingestion
5. **Stage** - Uploads to S3 and creates DuckDB staging tables
6. **Validate** - Automatic data quality checks and deduplication
7. **Ingest** - Direct DuckDB → Ladybug via database extension
8. **Subscription** - Subscribes your user to the SEC shared repository (plan `starter` by default)
9. **Config Update** - Updates credentials with SEC graph info
10. **Example Queries** - Runs preset queries showing your data (unless skipped)

SEC access is a **subscription**, not a one-off grant — the shared repository is billed like any other plan. If you only want the subscription and no data:

```bash
# Subscribe only; skip the download/process/ingest pipeline entirely
just demo-sec --subscribe-only

# Or use the dedicated recipe. advanced raises rate limits and credits.
just demo-sec-subscribe                 # starter (default)
just demo-sec-subscribe advanced

# Pick the plan on a full run
just demo-sec --plan advanced
```

The SEC plans are `starter` and `advanced`. The repository-prefixed spellings `sec-starter` and `sec-advanced` are accepted too, and stored as the bare plan name.

**Note:** Setup and loading takes 1-5 minutes depending on the number of years of data.

**Your credentials are saved** to `.local/config.json` with your API key and user information.

### The full corpus, prebuilt: `just sec-dump`

`just demo-sec` builds a graph for one ticker and year from EDGAR. If you want the **entire corpus** — every XBRL filing since January 2024, ~8,400 filers and 77 million facts — the same file that backs the hosted `sec` graph is published monthly as one LadybugDB file on Hugging Face: [robosystems/sec-xbrl-knowledge-graphs](https://huggingface.co/datasets/robosystems/sec-xbrl-knowledge-graphs).

```bash
just sec-dump                # ~35 GiB download → data/lbug-dbs/sec.lbug (~128 GiB); restarts graph-api if it is running
just sec-dump --force        # replace an existing sec.lbug (for example one built by demo-sec)
just sec-dump-no-restart     # same, without the graph-api restart
```

It downloads `sec.lbug.zst` (resumable), verifies the archive's checksum and recorded size while decompressing, lands `sec.lbug` in the directory the stack mounts, removes a stale `.wal`, deletes the archive, and warns if the engine that wrote the dump differs from the `ladybug` version this checkout pins. Budget ~165 GiB free during decompression, and real time — it is 128 GiB on disk.

Then subscribe and query exactly as elsewhere in this guide — `just demo-sec-subscribe` grants access without loading anything — or read the file directly, no services needed:

```bash
just lbug-query sec "MATCH (e:Entity) RETURN count(e)"
```

The file opens with the LadybugDB release that wrote it — the `ladybug==` version this repo pinned in `pyproject.toml` when the snapshot was built (0.18.1 today). The dataset card states it (the Engine row and its *Engine version* section), each snapshot's commit title carries it, and `just sec-dump` warns when your checkout pins something else. The card also carries the snapshot date, sizes, and further example queries.

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

The demo command automatically runs 25 preset queries showing your data — 15 graph queries plus 10 document searches. To explore the data interactively or re-run all queries:

```bash
# Interactive mode (enter queries directly, list presets, run specific ones)
just demo-sec-query

# List every preset without running anything
just demo-sec-query --list
```

In interactive mode (the prompt is `sec>`), you can:
- Type `presets` to list all available queries, graph and search
- Type `preset <name>` to run a specific query (e.g., `preset entities`)
- Type `search <query>` for a full-text search across filings
- Type `search <query> @NVDA` to scope that search to one entity
- Enter custom Cypher queries directly
- Type `help` for available commands
- Type `quit` to exit

**Graph Query Presets (Cypher):**
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

**Document Search Presets (OpenSearch):**
- `risk_factors` - Risk factor disclosures across all entities
- `ai_strategy` - Mentions of AI and machine learning strategy
- `revenue_recognition` - Revenue recognition policy disclosures
- `cybersecurity` - Cybersecurity risk and incident disclosures
- `goodwill_impairment` - Goodwill and impairment discussions
- `segment_reporting` - Operating segment descriptions and breakdowns
- `mda_overview` - Management discussion and analysis highlights (10-K Item 7)
- `mda_quarterly` - Quarterly MD&A highlights (10-Q Item 2)
- `supply_chain` - Supply chain and sourcing disclosures
- `tariff_trade` - Tariff, trade policy, and geopolitical risk mentions (10-K)

## Authentication Setup

The `just demo-sec` command automatically creates user credentials for you. Your API key is saved in `.local/config.json`.

**Important:** The API key (starting with `rfs`) is your authentication credential, not to be confused with the JWT Token and Authorization Header.

**Additional Users (Optional):**

If you need to create additional users with SEC repository access:

```bash
# Create test user with SEC repository access
just demo-user --repositories sec
```

## Accessing the Data

You have multiple options for accessing and querying the loaded SEC data:

1. **[MCP Client](#option-1-mcp-client-for-ai-agents)** - AI agent integration through Claude, ChatGPT or any MCP client
2. **[Direct Cypher Queries](#option-2-direct-cypher-queries-for-development)** - Command-line Cypher queries via `just graph-query`
3. **[Query Staging Tables](#option-3-query-staging-tables-for-data-validation)** - SQL queries on DuckDB staging via `just tables-query`
4. **[Python API](#option-4-python-api-for-applications)** - Programmatic access via RoboSystems Python client
5. **[G.V() Visual Explorer](#option-5-gv-graph-ide-for-interactive-exploration)** - Interactive graph visualization (recommended tool)
6. **[SQL IDE Tools](#option-6-sql-ide-tools-for-staging-data-validation)** - Direct database access with DuckDB-compatible IDEs

### Option 1: MCP Client (For AI Agents)

Access data through Claude, ChatGPT, Cursor or any other MCP client.

**Setup MCP Client:**

1. Run `just demo-sec` to create credentials automatically (your API key is saved to `.local/config.json`)

2. Get your API key from the credentials file:
```bash
cat .local/config.json | grep api_key
```

3. Connect your client. Every graph serves the MCP Streamable HTTP transport directly — one URL plus one header. For Claude Code:

```bash
claude mcp add --transport http robosystems-sec \
  http://localhost:8000/v1/graphs/sec/mcp \
  --header "X-API-Key: <your key>"
```

For Cursor / VS Code (`mcp.json`):

```json
"robosystems-sec": {
  "url": "http://localhost:8000/v1/graphs/sec/mcp",
  "headers": { "X-API-Key": "<your key>" }
}
```

Against production the same URL shape is `https://api.robosystems.ai/v1/graphs/sec/mcp`. Clients without HTTP transport support can use the legacy [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client).

Clients with no field for a custom header — claude.ai and Claude Desktop custom connectors among them — sign in instead: add `https://api.robosystems.ai/v1/mcp`, and choose the graph on the consent screen. The `?token=` carriage that once allowed a key in the URL was retired when OAuth landed, and a credential in the query string is now rejected. See [AI Operators & MCP](AI-Operators-and-MCP).

4. Restart your MCP-compatible AI tool

5. The MCP server provides these tools:

   **Curated Tools** (no Cypher needed):
   - `financial-statement-analysis` - Structured financial statement by ticker and type (`income_statement`, `balance_sheet`, `cash_flow_statement`, `equity_statement`)
   - `build-fact-grid` - Pivot facts across entities, periods, and dimensions — the multidimensional view over the XBRL hypercube
   - `resolve-element` - Map a concept in plain words ("revenue", "operating lease liability") to the element qnames filers use for it, with fact counts
   - `disclosures` - The map of a filing's sections, one row per note or statement
   - `information-block` - One section read whole: its rows, breakdowns, footing checks and text

   `disclosures` and `information-block` run xbrlkit's `disclosures` and `information_block` functions over the filing's published holon. [`xbrlkit serve`](https://xbrlkit.com/mcp), the free local MCP server, loads that same holon for a ticker when one is published, so it reads one filing's sections the same way on your own machine. Questions across filers need the graph.

   GraphQL reads are not part of this surface: the typed GraphQL tools serve a tenant graph's operational data, and a shared repository has none.

   **Graph Tools** (general purpose):
   - `read-graph-cypher` - Run Cypher queries directly
   - `get-graph-schema` - View available node and relationship types
   - `get-graph-info` - Inspect graph topology and node/relationship counts
   - `get-example-queries` - Get sample queries for common patterns
   - `list-subgraphs` - Enumerate this repository's subgraphs and their connector URLs

   **Search Tools** (gated by `SEMANTIC_SEARCH_ENABLED`, which defaults to on):
   - `search-documents` - Full-text search across filing narratives, MD&A, risk factors, and iXBRL disclosures
   - `get-document-section` - Retrieve full text of a document found via search

**Example MCP Usage:**

```
You: What revenue did NVIDIA report in 2024?

The AI will use:
1. financial-statement-analysis with ticker=NVDA, statement_type=income_statement
   → Returns structured revenue, expenses, net income with periods
```

### Option 2: Direct Cypher Queries (For Development)

Query the local database directly using Cypher (no authentication required for local queries). Queries that read numbers follow the [four SEC query rules](SEC-Data-Model-and-Query-Rules#the-four-rules): consolidated facts (`has_dimensions: false`), `canonical_concept` over one qname, a pinned period shape, and `RETURN DISTINCT` from an `Entity` anchor. `just graph-query` rewrites single quotes to double quotes, so string literals inside the query are fine.

```bash
# List the companies in the database
just graph-query sec "MATCH (e:Entity) RETURN e.ticker, e.name, e.cik LIMIT 10"

# A company's filings
just graph-query sec "MATCH (e:Entity {ticker: 'NVDA'})-[:ENTITY_HAS_REPORT]->(r:Report) RETURN r.form, r.filing_date, r.fiscal_year_focus ORDER BY r.filing_date DESC LIMIT 10"

# Consolidated annual net income
just graph-query sec "MATCH (e:Entity {ticker: 'NVDA'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(el:Element), (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}) WHERE el.canonical_concept = 'net_income' RETURN DISTINCT el.qname AS qname, p.end_date AS end_date, f.numeric_value AS value ORDER BY end_date DESC LIMIT 5"
```

**Common Cypher Patterns:**

```cypher
-- Revenue across a company's quarters
MATCH (e:Entity {cik: '0001045810'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(el:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'quarterly'})
WHERE el.canonical_concept = 'revenue'
RETURN DISTINCT el.qname AS qname, p.start_date AS start_date, p.end_date AS end_date,
       f.numeric_value AS value
ORDER BY end_date DESC
LIMIT 8

-- Balance sheet items at a period end
MATCH (e:Entity {ticker: 'NVDA'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(el:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {period_type: 'instant'})
WHERE p.end_date = '2025-01-26'
  AND el.canonical_concept IN ['total_assets', 'total_liabilities', 'stockholders_equity']
RETURN DISTINCT el.canonical_concept AS concept, el.qname AS qname, f.numeric_value AS value

-- The income statement of one filing, through its structure
MATCH (r:Report {identifier: $report_id})-[:REPORT_HAS_FACT]->(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(el:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
RETURN DISTINCT el.canonical_concept AS concept, el.qname AS qname,
       p.end_date AS end_date, f.numeric_value AS value
ORDER BY end_date DESC
LIMIT 60
```

More worked queries, and why each rule matters, are on [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules).

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
just tables-query sec "SELECT DISTINCT end_date, period_type, duration_type FROM Period ORDER BY end_date DESC LIMIT 10"

# Consolidated annual revenue facts with full details
just tables-query sec "SELECT el.qname, f.numeric_value, p.start_date, p.end_date, u.measure FROM Fact f JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src JOIN Element el ON fhe.dst = el.identifier JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src JOIN Period p ON fhp.dst = p.identifier JOIN FACT_HAS_UNIT fhu ON f.identifier = fhu.src JOIN Unit u ON fhu.dst = u.identifier WHERE el.canonical_concept = 'revenue' AND NOT f.has_dimensions AND p.duration_type = 'annual' LIMIT 10"
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
- `Element` - us-gaap metric definitions (with `canonical_concept` from enrichment)
- `Period` - Date/duration contexts
- `Unit` - Measurement units (USD, shares, etc.)
- `Dimension` - Segments and breakdowns
- `Structure` - XBRL presentation/calculation networks (with `canonical_type`)
- `Association` - Parent-child element relationships within structures
- `Classification` - Disclosure type tags on associations (e.g., AssetsRollUp, IncomeStatement)
- `FactSet` - Logical groupings of facts by structure and statement type
- `Label` - Human-readable labels for XBRL elements
- `Reference` - Authoritative references (FASB codification links)
- `Taxonomy` - Global XBRL taxonomy definitions

### Option 4: Python API (For Applications)

Access via the RoboSystems Python client:

```python
from robosystems_client import RoboSystemsClient
from robosystems_client.api.query.execute_cypher import sync_detailed as execute_query
from robosystems_client.models.cypher_statement_request import CypherStatementRequest
import os

# An API key from robosystems.ai (Settings → API keys) on an account with an SEC subscription
client = RoboSystemsClient(
    base_url="https://api.robosystems.ai",
    token=os.environ["ROBOSYSTEMS_API_KEY"],
    auth_header_name="X-API-Key",
    prefix="",
)

# Run Cypher query
request = CypherStatementRequest(
    query="""
    MATCH (e:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
          -[:FACT_HAS_ELEMENT]->(el:Element),
          (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
    WHERE el.canonical_concept = $concept
    RETURN DISTINCT el.qname AS qname, p.end_date AS end_date, f.numeric_value AS value
    ORDER BY end_date DESC
    LIMIT 10
    """,
    parameters={"ticker": "NVDA", "concept": "revenue"},
)

response = execute_query(graph_id="sec", client=client, body=request)

# `parsed` is the decoded JSON body — a dict, not a typed model.
# Keys: success, data, columns, row_count, execution_time_ms, graph_id, timestamp, error
if response.parsed:
    print(f"Results: {response.parsed['data']}")
```

`CypherStatementRequest` also takes an optional `timeout` (seconds, 1–300, default 60).

Against the pipeline you loaded locally, set `base_url="http://localhost:8000"` and use the `api_key` that `just demo-sec` saved in `.local/config.json`.

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

-- Check facts by calendar year
SELECT
  p.calendar_year AS year,
  COUNT(DISTINCT f.identifier) AS fact_count
FROM Fact f
JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src
JOIN Period p ON fhp.dst = p.identifier
GROUP BY year
ORDER BY year DESC;

-- Largest consolidated annual revenue figures
SELECT DISTINCT
  e.ticker,
  el.qname,
  p.end_date,
  f.numeric_value
FROM Fact f
JOIN FACT_HAS_ELEMENT fhe ON f.identifier = fhe.src
JOIN Element el ON fhe.dst = el.identifier
JOIN FACT_HAS_PERIOD fhp ON f.identifier = fhp.src
JOIN Period p ON fhp.dst = p.identifier
JOIN FACT_HAS_ENTITY fhen ON f.identifier = fhen.src
JOIN Entity e ON fhen.dst = e.identifier
WHERE el.canonical_concept = 'revenue'
  AND NOT f.has_dimensions
  AND p.duration_type = 'annual'
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

The graph you load is the same model the hosted `sec` repository serves. Each filing is a `Report` of an `Entity`; each reported number is a `Fact` linked to its `Element` (the concept, with a normalized `canonical_concept`), its `Period`, its `Unit`, and any `Dimension` qualifiers. A filing's statements and notes are `Structure` nodes (with `canonical_type` on the primary statements), each holding a `FactSet` of the facts it presents, and `Association` nodes carry the parent–child arcs within a structure.

The complete node and relationship reference, the traversal patterns, the enriched properties, and the four rules every number-reading query should follow are on **[SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules)**.

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
just demo-sec --ticker NVDA --year 2025

# Load additional companies (user already created)
just sec-load AMD 2025
just sec-load INTC 2025

# Verify all loaded
just sec-health v
```

### Find Available Financial Metrics

Use the `resolve-element` MCP tool to map a concept in plain words to the qnames filers use, or query directly:

```bash
# Elements mapped to a canonical concept
just graph-query sec "MATCH (el:Element) WHERE el.canonical_concept = 'revenue' RETURN el.qname, el.canonical_confidence LIMIT 20"
```

Common financial metrics (XBRL qname → canonical concept):

- `us-gaap:Revenues` → `revenue`
- `us-gaap:NetIncomeLoss` → `net_income`
- `us-gaap:Assets` → `total_assets`
- `us-gaap:Liabilities` → `total_liabilities`
- `us-gaap:StockholdersEquity` → `stockholders_equity`
- `us-gaap:EarningsPerShareBasic` → `eps_basic`
- `us-gaap:OperatingIncomeLoss` → `operating_income`
- `us-gaap:CashAndCashEquivalentsAtCarryingValue` → `cash_and_equivalents`

### Query by Time Period

A `Period` is either an `instant` (a balance-sheet date, held in `end_date`) or a `duration` bucketed by `duration_type` (`quarterly`, `semi_annual`, `nine_months`, `annual`, `other`). Pin one or the other; see [rule 3](SEC-Data-Model-and-Query-Rules#the-four-rules).

```bash
# Consolidated balance-sheet facts at one date
just graph-query sec "MATCH (e:Entity {ticker: 'NVDA'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})-[:FACT_HAS_PERIOD]->(p:Period {period_type: 'instant'}) WHERE p.end_date = '2025-01-26' RETURN count(DISTINCT f) AS facts"

# A company's annual filings for one fiscal year
just graph-query sec "MATCH (e:Entity {ticker: 'NVDA'})-[:ENTITY_HAS_REPORT]->(r:Report) WHERE r.fiscal_year_focus = 2025 RETURN r.form, r.fiscal_period_focus, r.filing_date"
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

- **Explore Preset Queries**: Run `just demo-sec-query` to see 25 curated examples — 15 Cypher queries and 10 document searches
- **Visualize with G.V()**: Use our favorite tool [G.V()](https://gdotv.com/) to explore graph relationships interactively
- **Learn Cypher**: Explore graph query patterns with [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **Validate Staging**: Use DuckDB CLI or SQL IDEs to inspect staging tables before ingestion
- **Build Analysis**: Use an MCP client such as Claude or ChatGPT for financial analysis with your auto-generated API key
- **Create Reports**: Build automated reporting with Python client
- **Add More Data**: Load additional companies with `just sec-load TICKER YEAR`

## Resources

- **Dagster Pipeline**: See `/robosystems/adapters/sec/pipeline/` for pipeline implementation
- **SEC Adapters**: See `/robosystems/adapters/sec/` for SEC client and processors
- **Health Check**: `just sec-health v` for diagnostic information
- **G.V()**: [https://gdotv.com/](https://gdotv.com/) - Recommended graph visualization tool for Ladybug
- **Cypher Docs**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io/)
- **RoboSystems API**: `http://localhost:8000/` — Swagger UI, with try-it (when running locally)
- **SEC EDGAR**: [SEC Website](https://www.sec.gov/edgar)
- **DuckDB**: [Documentation](https://duckdb.org/docs/) - SQL database for staging
- **Search SEC narratives**: [Search & AI Retrieval](Search-and-AI-Retrieval) - Full-text and semantic search over filing text
- **XBRL taxonomy**: [Taxonomy & Frameworks](Taxonomy-and-Frameworks) - The rs-gaap and fac frameworks behind the facts
- **Query the graph**: [Querying the Analytical Graph](Querying-the-Analytical-Graph) - Ad-hoc Cypher over the SEC graph
- **Data model and rules**: [SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules) - The node model, the four query rules, and worked queries
- **MCP & operators**: [AI Operators & MCP](AI-Operators-and-MCP) - The MCP retrieval surface
- **Shared repositories**: [Shared Repositories](Shared-Repositories) - SEC as the `ladybug-shared` tier: plans, subscription, and access

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
