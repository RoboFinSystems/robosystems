# Custom Graph Schema - Complete Guide

A custom graph carries your own node and relationship types, declared in a `schema.json`; this page covers the schema format and a runnable demo.

## On the hosted platform

A custom graph is created like any other, with `POST /v1/graphs`, by passing the schema as `custom_schema` in place of `initial_entity` ([Creating a Graph](Graphs-and-Multi-Tenancy.md#creating-a-graph) covers the role and payment-method requirements):

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d "{\"metadata\": {\"graph_name\": \"my_graph\"}, \"custom_schema\": $(cat schema.json)}"
```

Creation is asynchronous: the `202` carries an `OperationEnvelope`, and the new graph id arrives with the operation's result. Data then goes in through the file pipeline — `create-file-upload`, the upload itself, `ingest-file`, and `materialize` — described on [File Uploads](File-Uploads.md), and is read with Cypher.

The rest of this page runs the custom graph demo on your own stack: it scripts those same calls end to end.

## Overview

The Custom Graph Schema demo demonstrates how to create graph databases with custom node types, properties, and relationships. This approach enables:

- **Custom Data Models**: Define any graph structure for your domain
- **Flexible Schema Design**: Nodes and relationships tailored to your needs
- **Reusable Templates**: `schema.json` as a copy-and-customize starting point
- **Type-Safe Schemas**: Property types validated when the graph is created
- **Graph-Native Queries**: Leverage Cypher for powerful data analysis
- **AI-Powered Analysis**: Query demo graph data using natural language through any MCP-compatible AI tool

**Example Domain - People, Companies, and Projects:**

- **3 Node Types**: Person, Company, Project
- **3 Relationship Types**: Employment, Project Participation, Sponsorship
- **~74 Generated Entities**: 60 people, 8 companies, 6 projects, with relationships
- **Interactive Queries**: Explore collaboration patterns, team structures, and more

**The `schema.json` file is the official RoboSystems template for custom graph schemas** - copy it and customize for your own use cases!

## Prerequisites

Before starting, ensure you have:

- Docker running locally
- RoboSystems development environment set up
- Services started with `just start`

## Quick Start

The fastest way to run the complete demo:

```bash
# Ensure RoboSystems is running
just start

# Run complete workflow
just demo-custom-graph
```

**What this does:**

1. Creates user account and API key
2. Creates a new graph database using `schema.json`
3. Generates sample data (people, companies, projects)
4. Uploads and ingests data into the graph
5. Runs verification queries with beautiful table output
6. Indexes the demo's markdown documents for search
7. Provisions a `memory` subgraph and exercises structured + semantic memory

**First run**: Takes ~1-2 minutes to complete all steps.

**Subsequent runs**: Reuses credentials and graph (~20 seconds).

**Command syntax**: `just demo-custom-graph [--flags]`
- All flags pass through to the underlying script
- Base URL is read from `ROBOSYSTEMS_API_URL` in `.env.local` (defaults to `http://localhost:8000`)
- LocalStack vs real S3 is auto-detected from the base URL (any localhost / 127.0.0.1 → LocalStack)

### Quick Start Options

```bash
# Start fresh with new user and graph
just demo-custom-graph --new-user --new-graph

# Create new graph (keep existing user)
just demo-custom-graph --new-graph

# Skip verification queries
just demo-custom-graph --skip-queries

# Combine multiple flags
just demo-custom-graph --new-user --new-graph --skip-queries
```

## The Schema Template: `schema.json`

**Location:** `examples/custom_graph_demo/schema.json`

This is the **official RoboSystems template** for creating custom graph schemas. It demonstrates best practices for schema design and serves as your starting point for any custom graph database.

### Schema Structure

```json
{
  "name": "custom_graph_demo",
  "version": "1.0.0",
  "description": "People, companies, and projects schema",
  "nodes": [
    {
      "name": "Person",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "name", "type": "STRING", "is_required": true},
        {"name": "age", "type": "INT64"},
        {"name": "title", "type": "STRING"}
      ]
    }
  ],
  "relationships": [
    {
      "name": "PERSON_WORKS_FOR_COMPANY",
      "from_node": "Person",
      "to_node": "Company",
      "properties": [
        {"name": "role", "type": "STRING"}
      ]
    }
  ],
  "metadata": {
    "domain": "custom_graph_demo"
  }
}
```

### Property Types

The demo uses `STRING`, `INT64`, and `DOUBLE`. The parser accepts every LadybugDB type, and an unknown type fails the schema before the graph is created:

| Category | Types |
| --- | --- |
| Strings | `STRING` |
| Integers | `INT8`, `INT16`, `INT32`, `INT64`, `INT128`, `UINT8`, `UINT16`, `UINT32`, `UINT64` |
| Floating point | `FLOAT`, `DOUBLE` |
| Temporal | `DATE`, `TIMESTAMP`, `INTERVAL` |
| Other scalars | `BOOLEAN`, `UUID`, `BLOB` |
| Composite | `LIST`, `MAP`, `STRUCT`, `UNION` (and fixed-size arrays such as `FLOAT[384]`) |
| Graph | `NODE`, `REL` |

The list lives in `CustomSchemaParser.VALID_TYPES`; see the [schemas README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/schemas/README.md#property-types).

### Schema Attributes

- `is_primary_key: true` — the node's key. Every node needs at least one; it becomes the table's `PRIMARY KEY`.
- `nullable: false` — marks a field required in the parsed schema. It adds no `NOT NULL` constraint to the table, so keep the field populated in your data.

`is_required`, which `schema.json` and some API examples carry, is **not read** by the parser: a property without `nullable` is nullable. Use `nullable: false` when you mean required.

Node and relationship names are checked against a short reserved list (`SystemConfig`, `SchemaVersion`, `AuditLog`, `Permission`, `Role`, `Session`, `Lock`, `Migration`, `SystemUser`, and the `SYSTEM_*` relationships).

### `extends: "base"`

The schema also takes an optional top-level `extends`. Leave it out and the graph holds exactly the types you declare. Set `"extends": "base"` and the graph is created with the **whole platform base ontology** alongside yours — `Entity`, `Period`, `Unit`, `Element`, `Label`, `Reference`, `Taxonomy`, `Dimension`, `Structure`, `Association`, `Trait`, `Classification`, `Agent`, `Event` and their relationships — and any of your nodes or relationships whose name collides with a base one is **dropped**, with only a server-side warning. It does not add properties to your own nodes. The demo's `schema.json` doesn't use it — its keys are exactly `name`, `version`, `description`, `nodes`, `relationships`, and `metadata`.

## Step-by-Step Walkthrough

The `just demo-custom-graph` command runs all 7 steps automatically. This section explains what happens during each step.

### Step 1: Setup Credentials (`setup_credentials.py`)

**What happens automatically:**
- Creates new user in PostgreSQL database
- Generates API key for authentication
- Stores credentials locally in `.local/config.json`

**Control via flags:**
```bash
just demo-custom-graph --new-user  # Force new credentials
```

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/setup_credentials.py
uv run examples/custom_graph_demo/setup_credentials.py --force  # Force new
```

### Step 2: Create Graph Database (`create_graph.py`)

**What happens automatically:**
- Reads `schema.json` from the demo directory
- Creates new Ladybug graph database with custom schema
- Registers graph with user account
- Stores graph_id in `.local/config.json`

**Control via flags:**
```bash
just demo-custom-graph --new-graph  # Force new graph
```

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/create_graph.py
uv run examples/custom_graph_demo/create_graph.py --reuse  # Reuse existing
```

**Customizing the Schema:**

The script loads `schema.json` from the same directory. To use your own schema:

```python
# In create_graph.py:
def build_custom_schema_definition() -> CustomSchemaDefinition:
  schema_file = Path(__file__).parent / "schema.json"
  # Change to: schema_file = Path(__file__).parent / "my_schema.json"
```

### Step 3: Generate Data (`generate_data.py`)

**What happens automatically:**
- Generates sample data matching the schema structure
- Creates Parquet files in `examples/custom_graph_demo/data/` directory
- Includes: Person, Company, Project nodes and their relationships
- Validates all required properties are present

**Generated data includes:**
- 60 People with realistic names, ages, titles, and interests
- 8 Companies across various industries and locations
- 6 Projects with budgets, statuses, and timelines
- Employment relationships (Person → Company)
- Project participation (Person → Project)
- Sponsorship relationships (Company → Project)

Company and project counts are derived from the person count (one company per 8 people, one project per 10, with floors of 4 and 6), so `--count 100` scales all three together.

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/generate_data.py
uv run examples/custom_graph_demo/generate_data.py --count 100  # More data
uv run examples/custom_graph_demo/generate_data.py --regenerate  # Force regenerate
```

### Step 4: Upload and Ingest (`upload_ingest.py`)

**What happens automatically:**

1. **Upload**: Files uploaded to S3 (LocalStack in development)
2. **Stage**: Data loaded into DuckDB staging tables
3. **Validate**: Automatic data quality checks
4. **Ingest**: DuckDB → Ladybug graph database via extension
5. **Verify**: Counts verified, relationships checked

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/upload_ingest.py
```

### Step 5: Query the Graph (`query_graph.py`)

**What happens automatically:**
- Executes all preset queries
- Displays results in formatted Rich tables
- Shows node counts, relationships, and analysis queries

**Control via flags:**
```bash
just demo-custom-graph --skip-queries  # Skip this step
```

**Manual execution (if needed):**
```bash
# Run all presets
uv run examples/custom_graph_demo/query_graph.py --all

# Run specific preset
uv run examples/custom_graph_demo/query_graph.py --preset people

# List the available presets and exit
uv run examples/custom_graph_demo/query_graph.py --list

# Run one Cypher query and exit
uv run examples/custom_graph_demo/query_graph.py --query "MATCH (n) RETURN count(n)"

# Interactive query mode
uv run examples/custom_graph_demo/query_graph.py
```

### Step 6: Index Documents (`upload_documents.py`)

**What happens automatically:**
- Uploads the markdown files in `examples/custom_graph_demo/documents/` into the graph's search index
- Runs a few sample searches so you can see document retrieval sitting alongside the graph data

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/upload_documents.py
```

### Step 7: Memory Subgraph (`memory_subgraph.py`)

**What happens automatically:**
- Creates a subgraph inside the graph you just built — named `memory` by default, provisioned on the `knowledge` schema extension
- Seeds Concept / Observation / Session nodes on the subgraph and traverses them with exact Cypher — the *structured* memory modality
- Stores free-text memories on the **parent** graph with `remember` and retrieves them by meaning with `recall` — the *semantic* modality

The split is deliberate. A subgraph exists to be cheap to create and throw away and is written to directly, so the API refuses semantic memory there (403) rather than keeping a second vector store in step with the parent's. Structured knowledge belongs in the subgraph; recall belongs on the parent.

**Manual execution (if needed):**
```bash
uv run examples/custom_graph_demo/memory_subgraph.py
uv run examples/custom_graph_demo/memory_subgraph.py --name mymemory
```

## Available Preset Queries

The demo includes 7 preset queries demonstrating common graph patterns:

### 1. Summary - Node Counts

View the overall graph structure:

```bash
uv run examples/custom_graph_demo/query_graph.py --preset summary
```

**Output:**
```
         Overview of graph structure
┏━━━━━━━━━┳━━━━━━━━┓
┃ label   ┃ count  ┃
┡━━━━━━━━━╇━━━━━━━━┩
│ Person  │ 60     │
│ Company │ 8      │
│ Project │ 6      │
└─────────┴────────┘
```

### 2. People - Employment Information

View all people with their roles and companies:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
    p.identifier AS person_id,
    p.name AS name,
    p.title AS role,
    c.name AS company,
    p.interests AS interests
ORDER BY name
LIMIT 25
```

### 3. Companies - Team Overview

View companies with team sizes and sponsored projects:

```cypher
MATCH (c:Company)
OPTIONAL MATCH (c)<-[:PERSON_WORKS_FOR_COMPANY]-(p:Person)
OPTIONAL MATCH (c)-[:COMPANY_SPONSORS_PROJECT]->(proj:Project)
RETURN
    c.name AS company,
    c.industry AS industry,
    c.location AS location,
    count(DISTINCT p) AS team_members,
    count(DISTINCT proj) AS sponsored_projects
ORDER BY team_members DESC
```

### 4. Projects - Sponsors and Teams

View projects with their sponsor and team size:

```cypher
MATCH (proj:Project)<-[:PERSON_WORKS_ON_PROJECT]-(p:Person)
MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN
    proj.name AS project,
    proj.status AS status,
    c.name AS sponsor,
    proj.budget AS budget,
    count(DISTINCT p) AS team_members
ORDER BY project
```

### 5. Teams - Cross-Company Collaboration

Find pairs of people from different companies working the same project — the multi-hop pattern a relational schema makes painful:

```cypher
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE p1.identifier < p2.identifier AND c1.identifier <> c2.identifier
RETURN
    proj.name AS project,
    c1.name AS company_a,
    p1.name AS teammate_a,
    c2.name AS company_b,
    p2.name AS teammate_b
ORDER BY proj.name, company_a, company_b
LIMIT 50
```

### 6. Interests - What People Work On

Aggregate the technical interests across the population:

```cypher
MATCH (p:Person)
RETURN p.interests AS interest_list, count(*) AS people
ORDER BY people DESC, interest_list ASC
LIMIT 20
```

### 7. Graphviz - A Subgraph Worth Drawing

A small, connected slice suitable for pasting into a visualization tool:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
MATCH (p)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
    p.name AS person,
    c.name AS company,
    proj.name AS project
ORDER BY proj.name, company
LIMIT 40
```

### All Available Presets

1. **summary** - Node counts by label
2. **people** - People with employment info
3. **companies** - Companies with team sizes
4. **projects** - Projects with sponsors and team members
5. **teams** - Cross-company teams on shared projects
6. **interests** - Top technical interests across the population
7. **graphviz** - A lightweight subgraph to visualize

## Interactive Query Mode

After running the demo, explore your graph data interactively:

```bash
uv run examples/custom_graph_demo/query_graph.py
```

This launches an interactive session at a `cypher>` prompt. Anything you type that isn't one of the four commands below is executed as Cypher — so a bare preset name is a syntax error, not a shortcut.

**Run Preset Queries by Name:**
```
cypher> preset people
cypher> preset projects
cypher> preset teams
```

**Execute Custom Cypher Queries:**
```
cypher> MATCH (p:Person) WHERE p.age > 40 RETURN p.name, p.age, p.title ORDER BY p.age DESC
```

**List Available Presets:**
```
cypher> presets
```

**Exit the Session:**
```
cypher> quit
```

**Features:**
- **Beautiful Tables**: All results display in Rich-formatted tables
- **Instant Feedback**: See results immediately after each query
- **Explore Freely**: Test different queries without rerunning the script
- **Learning Tool**: Great for learning Cypher query patterns

## Accessing with MCP Client (For AI Agents)

You can access the demo custom graph through Claude, ChatGPT, Cursor or any other MCP client.

**Setup MCP Client:**

1. Run `just demo-custom-graph` to create credentials automatically (your API key is saved to `.local/config.json`)

2. Get your API key and graph ID from the credentials file:
```bash
cat .local/config.json | grep -E "api_key|graph_id"
```

3. Connect your client. Every graph serves the MCP Streamable HTTP transport directly — one URL plus one header, with the graph id in the URL. For Claude Code:

```bash
claude mcp add --transport http robosystems-demo \
  http://localhost:8000/v1/graphs/<your-graph-id>/mcp \
  --header "X-API-Key: <your key>"
```

For Cursor / VS Code (`mcp.json`):

```json
"robosystems-demo": {
  "url": "http://localhost:8000/v1/graphs/<your-graph-id>/mcp",
  "headers": { "X-API-Key": "<your key>" }
}
```

Against production the same URL shape is `https://api.robosystems.ai/v1/graphs/{graph_id}/mcp`. Clients without HTTP transport support can use the legacy [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client).

A key never travels in the URL: a credential in the query string is rejected. The hosted sign-in (OAuth) path — add `https://api.robosystems.ai/v1/mcp` and choose the graph on the consent screen, as claude.ai and Claude Desktop custom connectors do (Customize → Connectors) — is on [AI Operators & MCP](AI-Operators-and-MCP.md).

4. Restart your MCP-compatible AI tool

5. The tool list is assembled per graph; the MCP `tools/list` call returns the authoritative list for yours. The ones that matter most here:

   **Schema and DDL:**
   - `get-graph-schema` - View available node and relationship types
   - `add-node-table` - Add a new node type — on a subgraph only (see [Changing the Schema After Creation](#changing-the-schema-after-creation))
   - `add-relationship-table` - Add a new relationship type — on a subgraph only

   **Querying:**
   - `read-graph-cypher` - Run Cypher reads
   - `write-graph-cypher` - Run Cypher writes — on a subgraph only; a parent graph answers `subgraph_required`
   - `get-graph-info` - Inspect graph topology and node/relationship counts

   **Subgraphs and lifecycle:**
   - `list-subgraphs`, `create-subgraph`, `delete-subgraph`
   - `create-backup`, `set-write-policy`, `sync-connection`

   **Memory** — the surface Step 7 exercises:
   - `remember`, `recall`, `update-memory`, `forget`

   **Documents** — the surface Step 6 exercises:
   - `search-documents`, `get-document-section`, `get-document`, `list-documents`
   - `create-document`, `update-document`, `delete-document`

   The list also carries `get-graphql-schema` and `query-graphql`, but they are no use here: they serve the RoboLedger and RoboInvestor read schema, and a custom graph has no operational data behind it. Your tables are read with Cypher.

   Note that `get-example-queries` is **not** served on a custom graph — it's gated on the RoboLedger extension, because the examples it returns are written against that schema. Use `get-graph-schema` instead; on a custom graph it is the tool an agent orients with.

**Example MCP Usage:**

```
You: Show me all people who work on multiple projects in the demo

The AI will use:
1. get-graph-schema to understand Person and Project node types, properties, and relationships
2. read-graph-cypher to query for multi-project contributors
```

```
You: Which companies collaborate on the same projects in the demo?

The AI will use:
1. get-graph-schema to understand the COMPANY_SPONSORS_PROJECT relationships
2. read-graph-cypher to find shared projects between companies
3. Present collaboration patterns with company names and project details
```

## Creating Your Own Custom Schema

The `schema.json` file is your template. Here's how to create your own graph database schema:

### Step 1: Copy the Template

```bash
cd examples/custom_graph_demo
cp schema.json my_custom_schema.json
```

### Step 2: Define Your Node Types

Edit `my_custom_schema.json` and define your nodes:

```json
{
  "name": "my_custom_graph",
  "version": "1.0.0",
  "description": "My custom domain graph",
  "nodes": [
    {
      "name": "Product",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "name", "type": "STRING", "nullable": false},
        {"name": "price", "type": "DOUBLE"},
        {"name": "category", "type": "STRING"},
        {"name": "in_stock", "type": "BOOLEAN"}
      ]
    },
    {
      "name": "Customer",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "name", "type": "STRING", "nullable": false},
        {"name": "email", "type": "STRING"},
        {"name": "signup_date", "type": "STRING"}
      ]
    },
    {
      "name": "Order",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "order_date", "type": "STRING", "nullable": false},
        {"name": "total_amount", "type": "DOUBLE"},
        {"name": "status", "type": "STRING"}
      ]
    }
  ]
}
```

### Step 3: Define Your Relationships

Add relationships between your nodes:

```json
{
  "relationships": [
    {
      "name": "CUSTOMER_PLACED_ORDER",
      "from_node": "Customer",
      "to_node": "Order",
      "properties": [
        {"name": "order_number", "type": "STRING"}
      ]
    },
    {
      "name": "ORDER_CONTAINS_PRODUCT",
      "from_node": "Order",
      "to_node": "Product",
      "properties": [
        {"name": "quantity", "type": "INT64"},
        {"name": "unit_price", "type": "DOUBLE"}
      ]
    }
  ]
}
```

### Step 4: Update the Graph Creation Script

Edit `create_graph.py` to use your schema:

```python
def build_custom_schema_definition() -> CustomSchemaDefinition:
  schema_file = Path(__file__).parent / "my_custom_schema.json"

  if not schema_file.exists():
    raise FileNotFoundError(f"Schema file not found: {schema_file}")

  with open(schema_file) as f:
    schema_dict = json.load(f)

  return CustomSchemaDefinition.from_dict(schema_dict)
```

### Step 5: Generate Matching Data

Update `generate_data.py` to generate data matching your schema:

```python
# Generate Product nodes
products_data = []
for i in range(100):
  products_data.append({
    "identifier": str(uuid.uuid4()),
    "name": f"Product {i}",
    "price": round(random.uniform(10.0, 500.0), 2),
    "category": random.choice(["Electronics", "Clothing", "Books"]),
    "in_stock": random.choice([True, False])
  })

# Save to Parquet
df = pd.DataFrame(products_data)
df.to_parquet("data/nodes/Product.parquet")

# Relationships: `from` and `to` hold the endpoint nodes' primary keys,
# then the relationship's own properties
pd.DataFrame([
  {"from": customer_id, "to": order_id, "order_number": "SO-1001"},
]).to_parquet("data/relationships/CUSTOMER_PLACED_ORDER.parquet")
```

One file per table, named for it: node files in `data/nodes/<NodeName>.parquet`, relationship files in `data/relationships/<RELATIONSHIP_NAME>.parquet`. A relationship file needs `from` and `to` columns — staging reads those two names to tell a relationship table from a node table — holding the primary-key values of the source and target nodes. Remove the demo's own files from both folders first: the upload step sends every parquet file it finds there.

### Step 6: Point the Ingest and Query Scripts at Your Tables

Two scripts still name the demo's tables:

- **`upload_ingest.py`** — `EXPECTED_NODE_TABLES` and `EXPECTED_REL_TABLES` near the top list `Company`, `Project`, `Person` and the three demo relationships. The script refuses to run when a listed file is missing, and its post-ingest checks count every listed table. Replace both lists with your node and relationship names.
- **`query_graph.py`** — the presets in `PRESET_QUERIES` are Cypher over `Person`, `Company`, and `Project`, so against your graph they fail or return nothing. Replace them with your own, or run ad-hoc queries with `--query` or the interactive prompt.

### Step 7: Run Your Custom Pipeline

```bash
uv run examples/custom_graph_demo/create_graph.py
uv run examples/custom_graph_demo/generate_data.py
uv run examples/custom_graph_demo/upload_ingest.py
uv run examples/custom_graph_demo/query_graph.py --all
```

## Schema Design Best Practices

### 1. Use Meaningful Node Names

```json
✅ Good: "Customer", "Product", "Order"
❌ Bad: "Node1", "Entity", "Thing"
```

### 2. Choose Appropriate Property Types

```json
{
  "properties": [
    {"name": "price", "type": "DOUBLE"},      // ✅ Numeric calculations
    {"name": "quantity", "type": "INT64"},    // ✅ Whole numbers
    {"name": "active", "type": "BOOLEAN"},    // ✅ True/false flags
    {"name": "created_at", "type": "STRING"}  // ✅ ISO date strings
  ]
}
```

### 3. Always Include Primary Keys

```json
{
  "properties": [
    {"name": "identifier", "type": "STRING", "is_primary_key": true}
  ]
}
```

### 4. Mark Required Fields

```json
{
  "properties": [
    {"name": "name", "type": "STRING", "nullable": false}
  ]
}
```

### 5. Use Descriptive Relationship Names

```json
✅ Good: "CUSTOMER_PLACED_ORDER", "PERSON_WORKS_FOR_COMPANY"
❌ Bad: "HAS", "RELATES_TO", "LINKED"
```

### 6. Add Relationship Properties

```json
{
  "name": "PERSON_WORKS_ON_PROJECT",
  "properties": [
    {"name": "hours_per_week", "type": "INT64"},
    {"name": "contribution", "type": "STRING"}
  ]
}
```

## Understanding the Data Model

### Demo Schema: People, Companies, Projects

**Node Types:**

- **Person**: Individuals with names, ages, titles, interests
- **Company**: Organizations with industries, locations, founding years
- **Project**: Work initiatives with budgets, statuses, dates

**Relationship Types:**

- **PERSON_WORKS_FOR_COMPANY**: Employment relationships with roles and start dates
- **PERSON_WORKS_ON_PROJECT**: Project participation with hours and contributions
- **COMPANY_SPONSORS_PROJECT**: Sponsorship with levels and budget commitments

### Graph Traversal Example

```
Person (Alice Johnson, Software Engineer)
  -[:PERSON_WORKS_FOR_COMPANY {role: "Senior Engineer"}]-> Company (TechCorp)
  -[:PERSON_WORKS_ON_PROJECT {hours_per_week: 40}]-> Project (Cloud Migration)
    <-[:COMPANY_SPONSORS_PROJECT {budget_committed: 500000}]- Company (TechCorp)
```

This represents: *Alice works as a Senior Engineer at TechCorp, dedicating 40 hours/week to the Cloud Migration project, which TechCorp sponsors with $500k.*

A multi-hop query follows the same edges — here, companies that collaborate through employees who share a project:

```cypher
MATCH (c1:Company)<-[:PERSON_WORKS_FOR_COMPANY]-(p:Person)
      -[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
      <-[:PERSON_WORKS_ON_PROJECT]-(p2:Person)
      -[:PERSON_WORKS_FOR_COMPANY]->(c2:Company)
WHERE c1.identifier <> c2.identifier
RETURN c1.name, c2.name, count(DISTINCT proj) AS shared_projects
```

## Visualizing with G.V()

[G.V()](https://gdotv.com/) is our recommended partner for exploring graph databases interactively.

### Getting Started

1. Visit [https://gdotv.com/](https://gdotv.com/) or download the desktop application
2. Connect to your custom graph database:
   - **Database Path**: `./data/lbug-dbs/<graph_id>.lbdb` (get graph_id from `.local/config.json`)
3. **Enable "Fetch all edges between vertices"** in settings
4. Run visualization queries to explore your data

### Visualization Query Examples

```cypher
-- Visualize people and their companies
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN p, c
LIMIT 20

-- View project teams
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN p, proj, c
LIMIT 15

-- Explore company network through shared projects
MATCH (c1:Company)-[:COMPANY_SPONSORS_PROJECT]->(proj:Project)
      <-[:COMPANY_SPONSORS_PROJECT]-(c2:Company)
WHERE c1.identifier <> c2.identifier
RETURN c1, proj, c2
LIMIT 10
```

## Changing the Schema After Creation

A graph's schema is fixed when the graph is created. The query surface blocks schema DDL (`CREATE`/`DROP`/`ALTER TABLE`) on every graph, and on a main graph Cypher is read-only: data enters through the staging pipeline (upload → ingest → materialize) against the tables declared in `schema.json`. To change a main graph's node or relationship types, edit the schema and create a new graph.

Subgraphs are the place to extend a schema live. On a subgraph, the MCP tools `add-node-table` and `add-relationship-table` add new types, and `write-graph-cypher` writes to them; on a parent graph all three answer `subgraph_required`. Create one with `POST /v1/graphs/{graph_id}/operations/create-subgraph` — `subgraph_type: "static"` starts from the platform base schema plus the parent's schema extensions — **not** a custom graph's own tables, so add those with `add-node-table` / `add-relationship-table` if you need them there — `"knowledge"` from the Concept / Observation / Session schema that Step 7 uses, and `"empty"` from nothing. Anything written this way lives in the subgraph only. See [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy.md#subgraphs).

## Troubleshooting

### Demo Fails: "Schema file not found"

**Solution:** Ensure `schema.json` exists in the demo directory:

```bash
ls examples/custom_graph_demo/schema.json
```

### Demo Fails: "No credentials found"

**Solution:** Let the demo create credentials automatically, or force new:

```bash
just demo-custom-graph --new-user
```

### Demo Fails: "Invalid schema"

**Solution:** Validate your schema JSON format:

```bash
# Check for JSON syntax errors
uv run python -m json.tool examples/custom_graph_demo/schema.json
```

### Connection Error

**Solution:** Ensure RoboSystems services are running:

```bash
just start
docker ps  # Verify containers are running
```

### Import Errors

**Solution:** Install dev dependencies:

```bash
just install
```

## Next Steps

- **Copy `schema.json`**: Use it as your template for custom schemas
- **Design Your Schema**: Define nodes and relationships for your domain
- **Generate Data**: Create Parquet files matching your schema
- **Query Your Graph**: Use Cypher to analyze your custom data
- **Visualize with G.V()**: Explore your graph structure interactively
- **Learn Cypher**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)

## Resources

- **Demo Code**: `/examples/custom_graph_demo/` in the repository
- **Schema Template**: `examples/custom_graph_demo/schema.json`
- **G.V() Graph IDE**: [https://gdotv.com/](https://gdotv.com/)
- **Cypher Docs**: [Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/)
- **RoboSystems API**: `http://localhost:8000/` — Swagger UI, with try-it
- **Graphs & Tenancy**: [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy.md) - The graph_id model, tiers, and subgraphs
- **Query the graph**: [Querying the Analytical Graph](Querying-the-Analytical-Graph.md) - Ad-hoc Cypher and staging tables
- **MCP & operators**: [AI Operators & MCP](AI-Operators-and-MCP.md) - Access your graph through MCP tools
- **Data pipelines**: [Pipeline Guide](Pipeline-Guide.md) - How data reaches a graph: the staging path and the Dagster pipelines behind it

## Support

- GitHub Issues: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- Main README: [robosystems/README.md](https://github.com/RoboFinSystems/robosystems/blob/main/README.md)
- Development Guide: [CLAUDE.md](https://github.com/RoboFinSystems/robosystems/blob/main/CLAUDE.md)
