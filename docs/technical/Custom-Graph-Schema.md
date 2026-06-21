# Custom Graph Schema - Complete Guide

This guide walks you through creating custom graph databases with RoboSystems using the **`schema.json` template**. Learn how to design, implement, and query your own graph structures for any domain.

## Overview

The Custom Graph Schema demo demonstrates how to create graph databases with custom node types, properties, and relationships. This approach enables:

- **Custom Data Models**: Define any graph structure for your domain
- **Flexible Schema Design**: Nodes and relationships tailored to your needs
- **Reusable Templates**: `schema.json` as a copy-and-customize starting point
- **Type-Safe Schemas**: Validated property types and required fields
- **Graph-Native Queries**: Leverage Cypher for powerful data analysis
- **AI-Powered Analysis**: Query demo graph data using natural language through any MCP-compatible AI tool

**Example Domain - People, Companies, and Projects:**

- **3 Node Types**: Person, Company, Project
- **3 Relationship Types**: Employment, Project Participation, Sponsorship
- **~50 Generated Entities**: Realistic sample data with relationships
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
  "extends": "base",
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

Supported data types in your schema:

- `STRING` - Text values
- `INT64` - 64-bit integers
- `DOUBLE` - Floating point numbers
- `BOOLEAN` - True/false values
- `DATE` - Date values (as STRING in ISO format)

### Schema Attributes

- `is_primary_key: true` - Unique identifier for the node
- `is_required: true` - Field must have a value
- `extends: "base"` - Inherit base schema properties (identifier, timestamps)

## Step-by-Step Walkthrough

The `just demo-custom-graph` command runs all 5 steps automatically. This section explains what happens during each step.

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
- Stores graph_id in `credentials/config.json`

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
- 50 People with realistic names, ages, titles, and interests
- 10 Companies across various industries and locations
- 15 Projects with budgets, statuses, and timelines
- Employment relationships (Person → Company)
- Project participation (Person → Project)
- Sponsorship relationships (Company → Project)

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

# Interactive query mode
uv run examples/custom_graph_demo/query_graph.py
```

## Available Preset Queries

The demo includes 10 preset queries demonstrating common graph patterns:

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
│ Person  │ 50     │
│ Company │ 10     │
│ Project │ 15     │
└─────────┴────────┘
```

### 2. People - Employment Information

View all people with their roles and companies:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
  p.name,
  p.title,
  c.name AS company,
  p.interests
ORDER BY p.name
```

### 3. Companies - Team Overview

View companies with team sizes and sponsored projects:

```cypher
MATCH (c:Company)
OPTIONAL MATCH (c)<-[:PERSON_WORKS_FOR_COMPANY]-(p:Person)
OPTIONAL MATCH (c)-[:COMPANY_SPONSORS_PROJECT]->(proj:Project)
RETURN
  c.name,
  c.industry,
  c.location,
  count(DISTINCT p) AS team_members,
  count(DISTINCT proj) AS sponsored_projects
ORDER BY team_members DESC
```

### 4. Projects - Active Projects

View active projects with team sizes and sponsors:

```cypher
MATCH (proj:Project)
WHERE proj.status = 'active'
OPTIONAL MATCH (proj)<-[:PERSON_WORKS_ON_PROJECT]-(p:Person)
OPTIONAL MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN
  proj.name,
  proj.budget,
  count(DISTINCT p) AS team_size,
  collect(DISTINCT c.name) AS sponsors
ORDER BY proj.budget DESC
```

### 5. Employment - Who Works Where

See all employment relationships:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN p.name AS person, c.name AS company, c.industry
ORDER BY c.name, p.name
```

### 6. Project Teams - Team Composition

View project teams with their members:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN
  proj.name AS project,
  proj.status,
  proj.budget,
  collect(DISTINCT p.name) AS team_members,
  collect(DISTINCT c.name) AS sponsors
ORDER BY proj.name
```

### 7. Cross-Company Collaboration

Discover cross-company project collaborations:

```cypher
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE c1.identifier <> c2.identifier AND p1.identifier < p2.identifier
RETURN
  proj.name AS project,
  c1.name AS company_a,
  c2.name AS company_b,
  count(*) AS cross_company_pairs
ORDER BY cross_company_pairs DESC
```

### All Available Presets

1. **summary** - Node and relationship counts
2. **people** - People with employment info
3. **companies** - Companies with team sizes
4. **projects** - Active projects overview
5. **employment** - Employment relationships
6. **project_teams** - Project team composition
7. **cross_company** - Cross-company collaboration
8. **company_network** - Company collaboration network
9. **person_network** - Person collaboration network
10. **industries** - Companies by industry

## Interactive Query Mode

After running the demo, explore your graph data interactively:

```bash
uv run examples/custom_graph_demo/query_graph.py
```

This launches an interactive session where you can:

**Run Preset Queries by Name:**
```
> people
> projects
> cross_company
```

**Execute Custom Cypher Queries:**
```
> MATCH (p:Person) WHERE p.age > 40 RETURN p.name, p.age, p.title ORDER BY p.age DESC
```

**List Available Presets:**
```
> presets
```

**Exit the Session:**
```
> quit
```

**Features:**
- **Beautiful Tables**: All results display in Rich-formatted tables
- **Instant Feedback**: See results immediately after each query
- **Explore Freely**: Test different queries without rerunning the script
- **Learning Tool**: Great for learning Cypher query patterns

## Accessing with MCP Client (For AI Agents)

You can access the demo custom graph through any MCP-compatible AI tool (Claude Desktop, Claude Code, Cursor, Cline, etc.) using the MCP protocol.

**Setup MCP Client:**

1. Run `just demo-custom-graph` to create credentials automatically (your API key is saved to `.local/config.json`)

2. Get your API key and graph ID from the credentials file:
```bash
cat .local/config.json | grep -E "api_key|graph_id"
```

3. Add to your MCP tool config. For Claude Desktop:

```json
{
  "mcpServers": {
    "robosystems": {
      "command": "npx",
      "args": ["-y", "@robosystems/mcp@latest"],
      "env": {
        "ROBOSYSTEMS_API_URL": "http://localhost:8000",
        "ROBOSYSTEMS_API_KEY": "rfsabc123xyz...",
        "ROBOSYSTEMS_GRAPH_ID": "your-graph-id"
      }
    }
  }
}
```

**Important:** Replace `rfsabc123xyz...` with your actual API key and `your-graph-id` with your actual graph ID from the credentials file.

4. Restart your MCP-compatible AI tool

5. The MCP server provides these tools:
   - `get-graph-schema` - View available node and relationship types
   - `read-graph-cypher` - Run Cypher queries
   - `get-graph-info` - Inspect graph topology and node/relationship counts
   - `get-example-queries` - Get sample queries

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
  "extends": "base",
  "nodes": [
    {
      "name": "Product",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "name", "type": "STRING", "is_required": true},
        {"name": "price", "type": "DOUBLE"},
        {"name": "category", "type": "STRING"},
        {"name": "in_stock", "type": "BOOLEAN"}
      ]
    },
    {
      "name": "Customer",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "name", "type": "STRING", "is_required": true},
        {"name": "email", "type": "STRING"},
        {"name": "signup_date", "type": "STRING"}
      ]
    },
    {
      "name": "Order",
      "properties": [
        {"name": "identifier", "type": "STRING", "is_primary_key": true},
        {"name": "order_date", "type": "STRING", "is_required": true},
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
```

### Step 6: Run Your Custom Pipeline

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
    {"name": "name", "type": "STRING", "is_required": true}
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
    {"name": "role", "type": "STRING"}
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

## Visualizing with G.V()

[G.V()](https://gdotv.com/) is our recommended partner for exploring graph databases interactively.

### Getting Started

1. Visit [https://gdotv.com/](https://gdotv.com/) or download the desktop application
2. Connect to your custom graph database:
   - **Database Path**: `./data/lbug-dbs/<graph_id>.lbug` (get graph_id from `credentials/config.json`)
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

## Graph Database Benefits

### 1. Flexible Data Modeling

Traditional databases require rigid schemas. Graph databases adapt easily:

- Add new node types without migration
- Add new relationships on the fly
- Extend properties as needed

### 2. Natural Relationship Representation

```cypher
-- Find colleagues who work on the same projects
MATCH (p1:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
      <-[:PERSON_WORKS_ON_PROJECT]-(p2:Person)
WHERE p1.identifier < p2.identifier
RETURN p1.name, p2.name, proj.name
```

### 3. Multi-Hop Queries

```cypher
-- Find companies that collaborate through shared employees
MATCH (c1:Company)<-[:PERSON_WORKS_FOR_COMPANY]-(p:Person)
      -[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
      <-[:PERSON_WORKS_ON_PROJECT]-(p2:Person)
      -[:PERSON_WORKS_FOR_COMPANY]->(c2:Company)
WHERE c1.identifier <> c2.identifier
RETURN c1.name, c2.name, count(DISTINCT proj) AS shared_projects
```

### 4. Real-Time Analytics

No pre-computed views needed. Analytics run instantly on current data:

```cypher
-- Real-time collaboration index
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
WITH p, count(DISTINCT proj) AS project_count
RETURN p.name, project_count
ORDER BY project_count DESC
```

## Common Use Cases

The custom schema pattern works for many domains:

### CRM Systems
- **Nodes**: Customer, Contact, Opportunity, Account
- **Relationships**: WORKS_FOR, OWNS, MANAGES

### Supply Chain
- **Nodes**: Supplier, Product, Warehouse, Order
- **Relationships**: SUPPLIES, STORES, SHIPS_TO

### Knowledge Graphs
- **Nodes**: Concept, Document, Author, Topic
- **Relationships**: MENTIONS, AUTHORED_BY, RELATED_TO

### Social Networks
- **Nodes**: User, Post, Group, Event
- **Relationships**: FOLLOWS, POSTED, MEMBER_OF, ATTENDING

### Project Management
- **Nodes**: Task, Milestone, Resource, Team
- **Relationships**: DEPENDS_ON, ASSIGNED_TO, PART_OF

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
- **RoboSystems API**: http://localhost:8000/docs
- **Graphs & Tenancy**: [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy) - The graph_id model, tiers, and subgraphs
- **Query the graph**: [Querying the Analytical Graph](Querying-the-Analytical-Graph) - Ad-hoc Cypher and staging tables
- **MCP & operators**: [AI Operators & MCP](AI-Operators-and-MCP) - Access your graph through MCP tools
- **Custom adapters**: [Pipeline Guide](Pipeline-Guide) - Ingest external data into a custom graph

## Support

- GitHub Issues: [robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues)
- Main README: [robosystems/README.md](https://github.com/RoboFinSystems/robosystems/blob/main/README.md)
- Development Guide: [CLAUDE.md](https://github.com/RoboFinSystems/robosystems/blob/main/CLAUDE.md)
