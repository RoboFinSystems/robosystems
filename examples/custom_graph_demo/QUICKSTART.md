# Custom Graph Demo - Quick Start Guide

## Overview

This demo showcases RoboSystems' graph database capabilities for generic graph data. It creates a custom graph structure with:

- **Person Nodes**: Individuals with properties like name, age, and interests
- **Company Nodes**: Organizations with properties like industry and location
- **Project Nodes**: Work initiatives connecting people and companies
- **Relationships**: Custom relationships showing collaborations, employment, and project participation

## 🚀 Quick Start - Run All Steps

```bash
# Make sure RoboSystems is running
just start robosystems

# Navigate to the demo directory
cd examples/custom_graph_demo

# Run each step in sequence
uv run 01_setup_credentials.py
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

## 📋 Step-by-Step Guide

### Step 1: Setup Credentials

Creates a user account and API key, saves credentials to `credentials/config.json`.

```bash
uv run 01_setup_credentials.py

# Options:
uv run 01_setup_credentials.py --name "Your Name"
uv run 01_setup_credentials.py --email your@email.com
uv run 01_setup_credentials.py --force  # Create new credentials
```

**Output**: User created, API key generated, credentials saved

### Step 2: Create Graph

Creates a new generic graph database using the custom schema defined in `schema.json`.

```bash
uv run 02_create_graph.py

# Options:
uv run 02_create_graph.py --name "My Custom Graph Demo"
uv run 02_create_graph.py --reuse  # Reuse existing graph
```

**Output**: Graph created with custom schema, graph_id saved to credentials

**Note**: The graph structure is defined in `schema.json` - you can customize this file to create your own graph schema!

### Step 3: Generate Data

Generates custom graph data as Parquet files.

```bash
uv run 03_generate_data.py

# Options:
uv run 03_generate_data.py --count 100  # Generate 100 people instead of default
uv run 03_generate_data.py --regenerate  # Force regenerate
uv run 03_generate_data.py --seed 1234   # Deterministic dataset for testing
```

**Output**: 3 node and 3 relationship Parquet files created under `data/`:

- `nodes/Person.parquet` - Individual people
- `nodes/Company.parquet` - Organizations and businesses
- `nodes/Project.parquet` - Work initiatives and collaborations
- `relationships/PERSON_WORKS_FOR_COMPANY.parquet` - Employment relationships
- `relationships/PERSON_WORKS_ON_PROJECT.parquet` - Project participation relationships
- `relationships/COMPANY_SPONSORS_PROJECT.parquet` - Sponsorship relationships

### Step 4: Upload & Ingest

Uploads the Parquet files and ingests them into the graph.

```bash
uv run 04_upload_ingest.py
```

**Output**: All files uploaded, data ingested into graph, verification queries run

### Step 5: Query Graph

Run example queries or enter interactive mode.

```bash
# Run all preset queries
uv run 05_query_graph.py --all

# Run a specific preset
uv run 05_query_graph.py --preset summary
uv run 05_query_graph.py --preset people
uv run 05_query_graph.py --preset projects

# Run a custom query
uv run 05_query_graph.py --query "MATCH (n) RETURN count(n)"

# Interactive mode
uv run 05_query_graph.py
```

## 📊 Available Preset Queries

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

## 🛠️ Technical Details

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

## 🔧 Advanced Usage

### Customize the Schema

The `schema.json` file is your starting point for creating custom graph structures. Here's how to customize it:

```bash
# 1. Copy the example schema
cp schema.json my_custom_schema.json

# 2. Edit my_custom_schema.json to add your own:
#    - Node types (e.g., Product, Order, Customer)
#    - Properties (e.g., price, quantity, status)
#    - Relationships (e.g., CUSTOMER_PLACED_ORDER)

# 3. Update 02_create_graph.py to load your schema:
#    Change: schema_file = Path(__file__).parent / "my_custom_schema.json"

# 4. Generate matching data and run the pipeline
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
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

### Generate More Data

Generate more custom entities instead of the default:

```bash
uv run 03_generate_data.py --count 200 --regenerate
uv run 04_upload_ingest.py
```

### Custom Queries

Run custom Cypher queries:

```bash
# Find all people interested in 'AI'
uv run 05_query_graph.py --query "
MATCH (p:Person)
WHERE p.interests ILIKE '%AI%'
RETURN p.name, p.age, p.interests
ORDER BY p.age
"
```

### Interactive Mode

Enter interactive mode for ad-hoc queries:

```bash
uv run 05_query_graph.py

# Then type preset names or custom queries
> presets
> preset people
> preset projects
> MATCH (p:Person) RETURN count(p)
> quit
```

## 📚 Learn More

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

## 🐛 Troubleshooting

**Problem:** Script fails with "No credentials found"
**Solution:** Run step 1 first:

```bash
uv run 01_setup_credentials.py
```

**Problem:** Script fails with "No graph_id found"
**Solution:** Run step 2 first:

```bash
uv run 02_create_graph.py
```

**Problem:** Script fails with "No parquet files found"
**Solution:** Run step 3 first:

```bash
uv run 03_generate_data.py
```

**Problem:** Connection error
**Solution:** Ensure RoboSystems is running:

```bash
just start robosystems
```

**Problem:** Import errors
**Solution:** Install dev dependencies:

```bash
just install
```

## 💡 Tips

- **`schema.json` is your template** - Copy and customize it for your own graph database schemas
- All scripts can be run independently after their dependencies are met
- Credentials and data are saved locally and reused across runs
- Use `--force` or `--regenerate` flags to start fresh
- The demo uses auto-generated test data - perfect for exploring the API
- Check the generated Parquet files in `data/` to see the data structure
- Review `schema.json` to understand the complete graph structure before querying

## 🎉 Success!

After running all steps, you have:

1. ✅ User account & API key
2. ✅ Generic graph database ready for custom data
3. ✅ Sample people, companies, and projects data
4. ✅ Ready-to-use query examples

Happy querying!

## 📞 Support

For questions or issues:

- Check the main project [README.md](../../README.md)
- Review the [CLAUDE.md](../../CLAUDE.md) development guide
- Open an issue on GitHub
