# Custom Graph Demo - Quick Start Guide

## Overview

This demo showcases RoboSystems' graph database capabilities for generic graph data. It creates a custom graph structure with:

- **Person Nodes**: Individuals with properties like name, age, and interests
- **Company Nodes**: Organizations with properties like industry and location
- **Project Nodes**: Work initiatives connecting people and companies
- **Relationships**: Custom relationships showing collaborations, employment, and project participation

## Quick Start - Run All Steps

```bash
# Make sure RoboSystems is running
just start

# Run the complete demo (all steps automatically)
just demo-custom-graph

# Or create a new graph explicitly
just demo-custom-graph "new-graph"

# Or reuse an existing graph
just demo-custom-graph "reuse-graph"
```

## What The Demo Does

When you run `just demo-custom-graph`, it automatically:

1. **Sets up credentials** - Creates a user account and API key (or reuses existing)
2. **Creates graph** - Initializes a new graph database with custom schema from `schema.json`
3. **Generates data** - Creates 50 people, 10 companies, and 15 projects with relationships
4. **Uploads & ingests** - Loads data into the graph via staging tables
5. **Runs queries** - Executes example queries to demonstrate capabilities

**Note**: The graph structure is defined in `schema.json` - you can customize this file to create your own graph schema!

## Advanced Usage - Individual Steps

The `just demo-custom-graph` command runs `main.py` which executes all steps automatically. For manual control, you can run individual steps:

### Step 1: Setup Credentials

```bash
# Using just command (recommended)
just demo-user

# Or run directly with options
cd examples/custom_graph_demo
uv run 01_setup_credentials.py --name "Your Name" --email your@email.com
```

**Output**: User created, API key generated, credentials saved to `examples/credentials/config.json`

### Step 2-5: Run All Remaining Steps

```bash
# Run the main demo script
cd examples/custom_graph_demo
uv run main.py --flags "new-graph"

# Or run each step individually
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

**Output**: Graph created with custom data, example queries executed

## Available Preset Queries

### Summary

High-level overview of node counts:

```cypher
MATCH (n)
WITH labels(n)[0] AS label, count(n) AS count
RETURN label, count
ORDER BY count DESC
```

### People

View all people, their roles, and interests:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
  p.name,
  p.title,
  c.name AS company,
  p.interests
ORDER BY p.name
```

### Company Overview

View all companies with team sizes and sponsored projects:

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

### Employment Relationships

See who works for which companies:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN p.name AS person, c.name AS company, c.industry
ORDER BY c.name, p.name
```

### Project Teams

See which projects people are working on and who sponsors them:

```cypher
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN
  proj.name AS project,
  proj.status AS status,
  proj.budget AS budget,
  collect(DISTINCT p.name) AS team_members,
  collect(DISTINCT c.name) AS sponsors
ORDER BY proj.name
```

### Cross-Company Collaboration

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

## Technical Details

### Graph Schema

The graph schema is defined in **`schema.json`** - a standalone JSON file that specifies the structure of your graph database. This file serves as a template you can copy and customize for your own use cases.

**Schema Location:** `examples/custom_graph_demo/schema.json`

**Node Types:**

- `Person`: Individual people with properties like name, age, and interests
- `Company`: Organizations with properties like industry and location
- `Project`: Work initiatives with properties like status and budget

**Relationship Types:**

- `PERSON_WORKS_FOR_COMPANY`: Employment relationships between people and companies
- `PERSON_WORKS_ON_PROJECT`: Project participation relationships
- `COMPANY_SPONSORS_PROJECT`: Sponsorship relationships between companies and projects

**Customizing the Schema:**

The `schema.json` file is the **official template** for creating custom graph schemas in RoboSystems. To create your own schema:

1. Copy the schema file: `cp schema.json my_schema.json`
2. Modify the nodes, properties, and relationships for your use case
3. Update `02_create_graph.py` to load your custom schema file
4. Generate data matching your schema structure

**Schema Format:**

```json
{
  "name": "your_schema_name",
  "version": "1.0.0",
  "description": "Your schema description",
  "extends": "base",
  "nodes": [...],
  "relationships": [...],
  "metadata": {...}
}
```

### Data Format

All data is uploaded as Parquet files for optimal performance:

- `nodes/Person.parquet`: Individual people data
- `nodes/Company.parquet`: Organization data
- `nodes/Project.parquet`: Project information
- `relationships/PERSON_WORKS_FOR_COMPANY.parquet`: Employment relationships
- `relationships/PERSON_WORKS_ON_PROJECT.parquet`: Project participation relationships
- `relationships/COMPANY_SPONSORS_PROJECT.parquet`: Sponsorship relationships

### Graph Design Principles

This demo demonstrates best practices for graph database design:

- **Node types**: Clear separation of core entities (Person, Company, Project)
- **Relationship types**: Purpose-specific connections (employment, collaboration, sponsorship)
- **Property design**: Focused attributes that support analytics and graph traversal
- **Query patterns**: Examples for aggregations, path exploration, and team analysis

## Advanced Usage

### Customize the Schema

The `schema.json` file is your starting point for creating custom graph structures. Here's how to customize it:

```bash
# 1. Copy the example schema
cd examples/custom_graph_demo
cp schema.json my_custom_schema.json

# 2. Edit my_custom_schema.json to add your own:
#    - Node types (e.g., Product, Order, Customer)
#    - Properties (e.g., price, quantity, status)
#    - Relationships (e.g., CUSTOMER_PLACED_ORDER)

# 3. Update 02_create_graph.py to load your schema:
#    Change: schema_file = Path(__file__).parent / "my_custom_schema.json"

# 4. Generate matching data and run the demo
just demo-custom-graph "new-graph"
```

**Example: Adding a new node type**

```json
{
  "name": "Product",
  "properties": [
    {"name": "identifier", "type": "STRING", "is_primary_key": true},
    {"name": "name", "type": "STRING", "is_required": true},
    {"name": "price", "type": "DOUBLE"},
    {"name": "category", "type": "STRING"}
  ]
}
```

### Custom Queries

Run custom Cypher queries against your graph:

```bash
# Using just command
just graph-query <graph_id> "MATCH (p:Person) WHERE p.interests ILIKE '%AI%' RETURN p.name, p.age"

# Or use the query script
cd examples/custom_graph_demo
uv run 05_query_graph.py --query "MATCH (p:Person) RETURN count(p)"
```

### Generate More Data

To generate more entities than the default:

```bash
cd examples/custom_graph_demo
uv run 03_generate_data.py --count 200 --regenerate
uv run 04_upload_ingest.py
```

### Interactive Mode

Enter interactive mode for ad-hoc queries:

```bash
cd examples/custom_graph_demo
uv run 05_query_graph.py

# Then type preset names or custom queries
> presets
> preset people
> preset projects
> MATCH (p:Person) RETURN count(p)
> quit
```

## Learn More

### Graph Database Benefits for Custom Applications

1. **Relationship Modeling**: Natural representation of complex relationships between entities
2. **Flexible Schema**: Easy to add new node types and relationship types as needed
3. **Pattern Matching**: Powerful query capabilities for discovering insights
4. **Real-time Traversal**: Efficient navigation through connected data
5. **Scalable Design**: Support for growing and evolving data models

### Integration Patterns

This demo shows patterns used in real RoboSystems integrations:

- **HR Systems**: Employee and organizational data
- **Project Management**: Team structures and project relationships
- **CRM Systems**: Customer and contact relationship networks
- **Knowledge Graphs**: Any domain with interconnected entities

## Troubleshooting

**Problem:** Connection error or "API unavailable"
**Solution:** Ensure RoboSystems is running:
```bash
just start
just logs robosystems-api  # Check API logs
```

**Problem:** "No credentials found"
**Solution:** Run the credential setup:
```bash
just demo-user
```

**Problem:** Demo fails with authentication error
**Solution:** Recreate credentials:
```bash
just demo-user --force
```

**Problem:** Want to start fresh with a new graph
**Solution:** Use the new-graph flag:
```bash
just demo-custom-graph "new-graph"
```

## Tips

- **`schema.json` is your template** - Copy and customize it for your own graph database schemas
- The `just demo-custom-graph` command handles all setup automatically
- Credentials are saved in `examples/credentials/config.json` and reused across all demos
- Generated data files are saved in `examples/custom_graph_demo/data/` for inspection
- Use `just graph-query <graph_id> "<cypher>"` for ad-hoc queries
- Check `just logs robosystems-api` if you encounter issues
- Review `schema.json` to understand the complete graph structure before querying

## Success!

After running the demo, you have:

1. User account & API key (shared across all demos)
2. Custom graph database with your schema
3. Sample data demonstrating the graph structure
4. Ready-to-use query examples

Explore the data further with `just graph-query` or customize the schema for your own use case!

## Support

For questions or issues:
- Check the [Examples README](../README.md) for overview of all demos
- Review the main [README.md](../../README.md) for platform documentation
- Open an issue on [GitHub](https://github.com/RoboFinSystems/robosystems/issues)
