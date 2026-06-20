"""
Example Queries Tool - Provides example Cypher queries for graph exploration.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool
from .constants import (
  LEDGER_STATUS_GUIDANCE,
  PERIOD_TYPE_GUIDANCE,
  QUERY_PATTERN_GUIDANCE,
)


class ExampleQueriesTool(BaseTool):
  """
  Tool for generating example Cypher queries based on the graph schema.
  """

  def _is_shared_financial_repo(self) -> bool:
    """Check if this graph is a shared repository with financial reporting schema."""
    try:
      from robosystems.config.shared_repositories import (
        get_manifest,
        is_shared_repository_or_subgraph,
        resolve_shared_repository_parent,
      )

      if is_shared_repository_or_subgraph(self.client.graph_id):
        parent_id = resolve_shared_repository_parent(self.client.graph_id)
        manifest = get_manifest(parent_id)
        return manifest is not None and "roboledger" in (
          manifest.schema_extensions or ()
        )
    except Exception as e:
      logger.debug(f"Shared repo check failed for {self.client.graph_id}: {e}")
    return False

  def get_tool_definition(self) -> dict[str, Any]:
    """Get the tool definition for example queries."""
    return {
      "name": "get-example-queries",
      "description": """Get example Cypher queries for this graph database.

**WHEN TO USE:**
- When starting to explore a new graph
- When you need query patterns for specific node types
- When learning the relationship structure
- After getting errors to see correct syntax

**RETURNS:**
List of example queries with explanations, tailored to the actual schema present in this graph.

**BENEFITS:**
- See real working queries for this specific graph
- Learn property names and relationships
- Understand query patterns that work
- Copy and modify examples for your needs

"""
      + QUERY_PATTERN_GUIDANCE
      + "\n\n"
      + PERIOD_TYPE_GUIDANCE,
      "inputSchema": {
        "type": "object",
        "properties": {
          "category": {
            "type": "string",
            "description": "Optional category filter (e.g., 'entity', 'financial', 'relationships', 'aggregations')",
          }
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute the example queries tool."""
    self._log_tool_execution("get-example-queries", arguments)
    category = arguments.get("category")
    return await self._get_example_queries(category)

  async def _get_example_queries(
    self, category: str | None = None
  ) -> list[dict[str, Any]]:
    """
    Generate example queries based on the actual graph schema.

    Args:
        category: Optional filter for query category

    Returns:
        List of example queries with descriptions
    """
    examples = []
    try:
      # Get schema to understand what's available
      schema = await self.client.get_schema()

      # Find available node types
      node_types = [item["label"] for item in schema if item["type"] == "node"]
      rel_types = [item["label"] for item in schema if item["type"] == "relationship"]

      # Basic exploration queries (always include)
      if not category or category == "exploration":
        examples.append(
          {
            "category": "exploration",
            "description": "Count all nodes by type",
            "query": "MATCH (n:Fact) RETURN 'Fact' as node_type, count(n) as count UNION ALL MATCH (n:Element) RETURN 'Element' as node_type, count(n) as count UNION ALL MATCH (n:Entity) RETURN 'Entity' as node_type, count(n) as count",
            "explanation": "Shows distribution of data across node types",
          }
        )
        examples.append(
          {
            "category": "exploration",
            "description": "Get sample nodes to understand structure",
            "query": "MATCH (n) RETURN n LIMIT 5",
            "explanation": "Returns full node objects to see all properties",
          }
        )
        examples.append(
          {
            "category": "exploration",
            "description": "Discover properties of a node type",
            "query": f"MATCH (n:{node_types[0] if node_types else 'Node'}) RETURN keys(n) as properties LIMIT 1",
            "explanation": "Use keys() to find what properties are available",
          }
        )

      # Financial reporting queries (shared repositories with roboledger schema)
      is_shared_financial = self._is_shared_financial_repo()
      if is_shared_financial and (not category or category == "financial"):
        examples.extend(
          [
            {
              "category": "financial",
              "description": "Get company information",
              "query": "MATCH (e:Entity) RETURN e.name, e.cik, e.ticker, e.sic_description LIMIT 25",
              "explanation": "Entity nodes contain company master data. Filter by ticker (e.ticker = 'MRMD') or CIK for one company — this repo holds thousands of entities, so always LIMIT or filter.",
            },
            {
              "category": "financial",
              "description": "⭐ CONSOLIDATED Revenue for one company (cross-filer robust)",
              "query": """MATCH (f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(e:Element), (f)-[:FACT_HAS_ENTITY]->(ent:Entity {ticker: 'MRMD'}), (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE e.canonical_concept = 'revenue' AND f.numeric_value IS NOT NULL
RETURN ent.ticker, e.qname, p.end_date, f.numeric_value AS revenue
ORDER BY p.end_date DESC LIMIT 10""",
              "explanation": "⚠️ has_dimensions=false drops segment breakdowns (consolidated totals only). Filter on e.canonical_concept ('revenue', 'net_income', ...) rather than a single qname — revenue is tagged us-gaap:Revenues by some filers and us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax by others, and canonical_concept normalizes across both. Use the resolve-element tool to discover concepts.",
            },
            {
              "category": "financial",
              "description": "Full income statement by ticker (fast Structure traversal)",
              "query": """MATCH (ent:Entity {ticker: 'MRMD'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
WHERE f.numeric_value IS NOT NULL
RETURN DISTINCT e.qname, f.numeric_value AS value, p.end_date
ORDER BY p.end_date DESC LIMIT 40""",
              "explanation": "Pull a whole statement via Structure.canonical_type (income_statement | balance_sheet | cash_flow_statement | equity_statement). ⚠️ ANCHOR ON THE ENTITY (or a Report) FIRST and reach Structure LAST — ~53k filings share canonical_type='income_statement', so leading the MATCH with the Structure node scans them all and times out. For balance sheets, filter Period {period_type: 'instant'} instead of duration_type.",
            },
            {
              "category": "financial",
              "description": "Revenue BY Segment (dimensional breakdown)",
              "query": """MATCH (f:Fact {has_dimensions: true})-[:FACT_HAS_ELEMENT]->(e:Element), (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
WHERE e.qname = 'us-gaap:Revenues'
  AND f.numeric_value IS NOT NULL
RETURN d.axis_uri, d.member_uri, f.numeric_value
LIMIT 10""",
              "explanation": "Use has_dimensions=true when you WANT segment/geography breakdowns",
            },
            {
              "category": "financial",
              "description": "Get facts for a specific period (annual only)",
              "query": """MATCH (f:Fact {has_dimensions: false})-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.end_date >= '2024-01-01'
  AND p.duration_type = 'annual'
RETURN f.identifier, f.numeric_value, p.end_date, p.duration_type
LIMIT 10""",
              "explanation": "Period.period_type: instant/duration/forever. Period.duration_type: quarterly/semi_annual/nine_months/annual/other",
            },
            {
              "category": "financial",
              "description": "Find reports by form type",
              "query": """MATCH (r:Report)
WHERE r.form = '10-K' OR r.form = '10-Q'
RETURN r.form, r.filing_date, r.identifier
LIMIT 10""",
              "explanation": "Report nodes contain SEC filing metadata",
            },
          ]
        )

      # Ledger-spine queries (tenant roboledger graphs that materialize the
      # OLTP general ledger). These nodes carry lifecycle status and the graph
      # keeps voided/superseded rows, so every example MUST show the live filter.
      # Key on Entry/Transaction/LineItem — NOT Event: the base REA Event table
      # exists (empty) on the SEC shared repo, so including it would surface
      # these tenant-only examples there.
      has_ledger_spine = any(
        n in node_types for n in ("Entry", "Transaction", "LineItem")
      )
      if has_ledger_spine and (not category or category == "ledger"):
        examples.extend(
          [
            {
              "category": "ledger",
              "description": "⚠️ READ FIRST — ledger status filtering",
              "info": LEDGER_STATUS_GUIDANCE,
              "explanation": "The graph keeps voided/superseded/draft rows for audit; the examples below show the required live-row filters.",
            },
            {
              "category": "ledger",
              "description": "⭐ Account balances from POSTED entries only",
              "query": """MATCH (e:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(el:Element)
WHERE e.status = 'posted'
RETURN el.qname, sum(li.debit_amount) AS debits, sum(li.credit_amount) AS credits
ORDER BY debits DESC LIMIT 25""",
              "explanation": "CRITICAL: filter `e.status = 'posted'`. The graph keeps draft/reversed entries (and entries of voided events stay as 'draft'); without this filter cancelled amounts inflate every balance.",
            },
            {
              "category": "ledger",
              "description": "Count / sum events excluding voided & superseded",
              "query": """MATCH (ev:Event)
WHERE ev.status <> 'voided' AND ev.status <> 'superseded'
RETURN ev.event_type, count(ev) AS count, sum(ev.amount) AS total
ORDER BY count DESC LIMIT 25""",
              "explanation": "Event.status keeps voided (cancelled) and superseded (replaced) rows for audit. Exclude both from counts/sums. For open obligations instead, match the positive set (e.g. status IN ['committed','fulfilled','pending']).",
            },
            {
              "category": "ledger",
              "description": "Transaction amounts via posted entries (NOT Transaction.amount)",
              "query": """MATCH (t:Transaction)-[:TRANSACTION_HAS_ENTRY]->(e:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)
WHERE e.status = 'posted'
RETURN t.number, t.date, sum(li.debit_amount) AS posted_debits
ORDER BY t.date DESC LIMIT 25""",
              "explanation": "The Transaction node exposes only a `pending` boolean, NOT the full status — a voided transaction looks identical to a live one. Aggregate realized effect through its posted Entry/LineItem instead of summing Transaction.amount directly.",
            },
          ]
        )

      # Entity-based queries (common pattern)
      if "Entity" in node_types and (not category or category == "entity"):
        examples.extend(
          [
            {
              "category": "entity",
              "description": "Find entities by name pattern",
              "query": "MATCH (e:Entity) WHERE e.name CONTAINS 'TECH' RETURN e.name, e.identifier",
              "explanation": "Use CONTAINS for substring matching",
            },
            {
              "category": "entity",
              "description": "Get entity with all its properties",
              "query": "MATCH (e:Entity) WHERE e.identifier = 'some_id' RETURN e",
              "explanation": "Return full node to see all available data",
            },
          ]
        )

      # Relationship queries
      if rel_types and (not category or category == "relationships"):
        examples.extend(
          [
            {
              "category": "relationships",
              "description": "Find all relationships from a node",
              "query": "MATCH (n)-[r]->(m) WHERE id(n) = 0 RETURN type(r) as rel_type, labels(m)[0] as target_type",
              "explanation": "Discover what a node is connected to",
            },
            {
              "category": "relationships",
              "description": "Count relationships by type",
              "query": "MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count",
              "explanation": "Understand the relationship distribution",
            },
          ]
        )

      # Aggregation examples
      if not category or category == "aggregations":
        examples.extend(
          [
            {
              "category": "aggregations",
              "description": "Group and sum values",
              "query": """MATCH (f:Fact)
WHERE f.numeric_value IS NOT NULL
RETURN 'Fact' as type, sum(f.numeric_value) as total, count(f) as count""",
              "explanation": "LadybugDB supports aggregation functions like sum(), avg(), count()",
            },
            {
              "category": "aggregations",
              "description": "Find min/max values",
              "query": """MATCH (n)
WHERE n.numeric_value IS NOT NULL
RETURN min(n.numeric_value) as min_val, max(n.numeric_value) as max_val""",
              "explanation": "Use min() and max() for range analysis",
            },
          ]
        )

      # Add note about available nodes and relationships
      examples.append(
        {
          "category": "reference",
          "description": "Available node types in this graph",
          "info": f"Node types: {', '.join(node_types)}",
          "explanation": "Use these labels in your MATCH patterns",
        }
      )
      if rel_types:
        examples.append(
          {
            "category": "reference",
            "description": "Available relationship types",
            "info": f"Relationships: {', '.join(rel_types)}",
            "explanation": "Use these in relationship patterns like -[:TYPE]->",
          }
        )

    except Exception as e:
      logger.warning(f"Error generating examples: {e}")
      # Return basic examples even if schema fetch fails
      examples = [
        {
          "category": "basic",
          "description": "Count all nodes",
          "query": "MATCH (n) RETURN COUNT(*) as total_nodes",
          "explanation": "Basic query that should always work",
        },
        {
          "category": "basic",
          "description": "Get sample data",
          "query": "MATCH (n) RETURN n LIMIT 10",
          "explanation": "Explore what's in the graph",
        },
      ]

    return examples
