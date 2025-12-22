"""MCP (Model Context Protocol) API models."""

from pydantic import BaseModel, ConfigDict, Field


class MCPToolCall(BaseModel):
  """Request model for MCP tool execution."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Count nodes by type",
          "description": "Execute a simple aggregation query to count nodes by label",
          "value": {
            "name": "read-graph-cypher",
            "arguments": {
              "query": "MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count ORDER BY count DESC",
              "parameters": {},
            },
          },
        },
        {
          "summary": "Trial balance calculation",
          "description": "Execute accounting query with parameterized period to calculate trial balance",
          "value": {
            "name": "read-graph-cypher",
            "arguments": {
              "query": "MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WITH e.name AS account, e.classification AS type, sum(li.debit_amount) AS total_debits, sum(li.credit_amount) AS total_credits RETURN account, type, total_debits, total_credits, total_debits - total_credits AS net_balance ORDER BY account",
              "parameters": {},
            },
          },
        },
        {
          "summary": "Get schema information",
          "description": "Retrieve the complete graph schema without any arguments",
          "value": {
            "name": "get-graph-schema",
            "arguments": {},
          },
        },
        {
          "summary": "Get graph statistics",
          "description": "Retrieve high-level statistics and metadata about the graph",
          "value": {
            "name": "get-graph-info",
            "arguments": {},
          },
        },
        {
          "summary": "Revenue trends with date filter",
          "description": "Execute time-series query with parameterized month for revenue analysis",
          "value": {
            "name": "read-graph-cypher",
            "arguments": {
              "query": "MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WHERE e.classification = $classification AND substring(t.date, 1, 7) = $month RETURN sum(li.credit_amount) AS total_revenue",
              "parameters": {"classification": "revenue", "month": "2025-09"},
              "timeout_override": 60,
            },
          },
        },
      ]
    }
  )

  name: str = Field(..., description="Name of the MCP tool to execute", min_length=1)
  arguments: dict[str, object] = Field(
    default_factory=dict, description="Arguments to pass to the tool"
  )


class MCPQueryRequest(BaseModel):
  """Request model for MCP Cypher query execution with safety limits."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Chart of accounts query",
          "description": "Retrieve chart of accounts with classification and normal balance",
          "value": {
            "query": "MATCH (e:Element) WHERE e.classification IS NOT NULL RETURN e.name AS account, e.classification AS type, e.balance AS normal_balance ORDER BY e.name LIMIT 20",
            "params": {},
          },
        },
        {
          "summary": "Income statement with parameters",
          "description": "Generate income statement filtering by classification types",
          "value": {
            "query": "MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WHERE e.classification IN $classifications WITH e.classification AS category, e.name AS account, sum(li.credit_amount) - sum(li.debit_amount) AS amount RETURN category, account, amount ORDER BY category, account",
            "params": {"classifications": ["revenue", "expense"]},
          },
        },
        {
          "summary": "Cash flow transactions",
          "description": "Retrieve recent cash transactions with date ordering and limit",
          "value": {
            "query": "MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WHERE e.name = $account_name RETURN t.date AS date, t.description AS description, li.debit_amount AS cash_in, li.credit_amount AS cash_out ORDER BY t.date DESC LIMIT $limit",
            "params": {"account_name": "Cash", "limit": 20},
            "timeout_override": 60,
          },
        },
        {
          "summary": "Profitability by month",
          "description": "Complex aggregation query calculating monthly profit from revenue and expenses",
          "value": {
            "query": "MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WHERE e.classification IN ['revenue', 'expense'] WITH substring(t.date, 1, 7) AS month, e.classification AS type, li.credit_amount AS credit, li.debit_amount AS debit WITH month, sum(CASE WHEN type = 'revenue' THEN credit ELSE 0 END) AS revenue, sum(CASE WHEN type = 'expense' THEN debit ELSE 0 END) AS expenses RETURN month, revenue, expenses, revenue - expenses AS profit ORDER BY month",
            "params": {},
            "timeout_override": 120,
          },
        },
        {
          "summary": "Report lineage analysis",
          "description": "Data lineage query connecting transactions to financial reports for specific period",
          "value": {
            "query": "MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e) WHERE substring(t.date, 1, 7) = $period WITH e.name AS account, count(DISTINCT t) AS transactions, count(DISTINCT li) AS line_items, count(DISTINCT f) AS facts RETURN account, transactions, line_items, facts ORDER BY transactions DESC LIMIT $limit",
            "params": {"period": "2025-09", "limit": 10},
            "timeout_override": 90,
          },
        },
      ]
    }
  )

  query: str = Field(
    ...,
    description="Cypher query to execute (read-only)",
    min_length=1,
    max_length=50000,
  )
  params: dict[str, object] = Field(
    default_factory=dict, description="Query parameters"
  )
  timeout_override: int | None = Field(
    None, description="Override default query timeout (max 300 seconds)", ge=1, le=300
  )


class MCPToolsResponse(BaseModel):
  """Response model for MCP tools listing."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Complete tool listing for entity graph",
          "description": "Full list of available MCP tools with schemas and capabilities for an entity-based knowledge graph",
          "value": {
            "tools": [
              {
                "name": "read-graph-cypher",
                "description": "Execute a read-only Cypher query against the graph database. Supports parameterized queries for safety and streaming for large result sets.",
                "inputSchema": {
                  "type": "object",
                  "properties": {
                    "query": {
                      "type": "string",
                      "description": "Cypher query to execute (read-only). Use $param syntax for parameters.",
                    },
                    "parameters": {
                      "type": "object",
                      "description": "Query parameters as key-value pairs",
                    },
                    "timeout_override": {
                      "type": "integer",
                      "description": "Override default timeout (max 300 seconds)",
                    },
                  },
                  "required": ["query"],
                },
                "capabilities": {
                  "streaming": True,
                  "progress": True,
                  "cacheable": False,
                  "timeout_seconds": 300,
                },
              },
              {
                "name": "get-graph-schema",
                "description": "Get the complete schema of the graph database including node labels, relationship types, and property definitions",
                "inputSchema": {"type": "object", "properties": {}},
                "capabilities": {
                  "streaming": False,
                  "progress": True,
                  "cacheable": True,
                  "cache_ttl_seconds": 3600,
                  "timeout_seconds": 60,
                },
              },
              {
                "name": "get-graph-info",
                "description": "Get high-level statistics and metadata about the graph database",
                "inputSchema": {"type": "object", "properties": {}},
                "capabilities": {
                  "streaming": False,
                  "progress": False,
                  "cacheable": True,
                  "cache_ttl_seconds": 300,
                  "timeout_seconds": 30,
                },
              },
            ]
          },
        },
        {
          "summary": "Minimal tool listing",
          "description": "Basic tool set available for a newly created empty graph",
          "value": {
            "tools": [
              {
                "name": "get-graph-schema",
                "description": "Get the complete schema of the graph database",
                "inputSchema": {"type": "object", "properties": {}},
                "capabilities": {
                  "streaming": False,
                  "progress": True,
                  "cacheable": True,
                  "cache_ttl_seconds": 3600,
                  "timeout_seconds": 60,
                },
              },
              {
                "name": "get-graph-info",
                "description": "Get statistics about the graph database",
                "inputSchema": {"type": "object", "properties": {}},
                "capabilities": {
                  "streaming": False,
                  "progress": False,
                  "cacheable": True,
                  "cache_ttl_seconds": 300,
                  "timeout_seconds": 30,
                },
              },
            ]
          },
        },
        {
          "summary": "Tools with extended capabilities",
          "description": "Tool listing for a graph with advanced analysis features enabled",
          "value": {
            "tools": [
              {
                "name": "read-graph-cypher",
                "description": "Execute a read-only Cypher query with streaming support",
                "inputSchema": {
                  "type": "object",
                  "properties": {
                    "query": {"type": "string"},
                    "parameters": {"type": "object"},
                  },
                  "required": ["query"],
                },
                "capabilities": {
                  "streaming": True,
                  "progress": True,
                  "cacheable": False,
                  "timeout_seconds": 300,
                },
              },
              {
                "name": "analyze-financial-statements",
                "description": "Specialized tool for analyzing financial statement relationships and metrics",
                "inputSchema": {
                  "type": "object",
                  "properties": {
                    "company_identifier": {
                      "type": "string",
                      "description": "Ticker symbol or CIK",
                    },
                    "analysis_type": {
                      "type": "string",
                      "enum": ["revenue_trends", "expense_analysis", "ratios"],
                    },
                  },
                  "required": ["company_identifier"],
                },
                "capabilities": {
                  "streaming": False,
                  "progress": False,
                  "cacheable": False,
                  "timeout_seconds": 60,
                },
              },
            ]
          },
        },
      ]
    }
  )

  tools: list[dict[str, object]] = Field(
    ..., description="List of available MCP tools with their schemas"
  )


class MCPToolResult(BaseModel):
  """Response model for MCP tool execution result."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "SEC entities query result",
          "description": "Query returned public companies from SEC shared repository",
          "value": {
            "result": {
              "content": [
                {
                  "type": "text",
                  "text": "Found 3 entities:\n- Apple Inc. (AAPL) - CIK: 0000320193\n- Microsoft Corporation (MSFT) - CIK: 0000789019\n- Alphabet Inc. (GOOGL) - CIK: 0001652044",
                }
              ],
              "rows_returned": 3,
              "execution_time_ms": 87,
            }
          },
        },
        {
          "summary": "Accounting trial balance result",
          "description": "Accounting query showing debits, credits, and net balances",
          "value": {
            "result": {
              "content": [
                {
                  "type": "text",
                  "text": "Trial Balance (5 accounts):\nCash: $40,000 DR\nRevenue: $250,000 CR\nExpenses: $85,000 DR\nAccounts Payable: $15,000 CR\nEquity: $180,000 CR",
                }
              ],
              "rows_returned": 5,
              "execution_time_ms": 234,
            }
          },
        },
        {
          "summary": "Custom graph team structure",
          "description": "Query showing cross-company project collaborations",
          "value": {
            "result": {
              "content": [
                {
                  "type": "text",
                  "text": "Cloud Platform Project - 3 companies collaborating:\n- TechCorp: Alice Johnson (Lead Engineer)\n- InnovateCo: Bob Smith (Data Architect)\n- DataSystems: Carol Davis (DevOps)",
                }
              ],
              "rows_returned": 3,
              "execution_time_ms": 156,
            }
          },
        },
        {
          "summary": "Schema retrieval result",
          "description": "Successfully retrieved accounting graph schema",
          "value": {
            "result": {
              "schema": {
                "nodes": ["Transaction", "LineItem", "Element", "Report", "Entity"],
                "relationships": [
                  "TRANSACTION_HAS_LINE_ITEM",
                  "LINE_ITEM_RELATES_TO_ELEMENT",
                  "ENTITY_HAS_REPORT",
                ],
                "node_count": 2847,
                "relationship_count": 8234,
              }
            }
          },
        },
        {
          "summary": "Empty result set",
          "description": "Query executed successfully but returned no matching results",
          "value": {
            "result": {
              "content": [
                {
                  "type": "text",
                  "text": "No results found matching the specified criteria.",
                }
              ],
              "rows_returned": 0,
              "execution_time_ms": 23,
            }
          },
        },
      ]
    }
  )

  result: dict[str, object] = Field(..., description="Result of the MCP tool execution")


class MCPSchemaResponse(BaseModel):
  """Response model for graph database schema information."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Accounting graph schema",
          "description": "Schema for accounting demo with transactions, elements, and reports",
          "value": {
            "schema": [
              {
                "label": "Transaction",
                "properties": [
                  {"name": "date", "type": "STRING"},
                  {"name": "description", "type": "STRING"},
                  {"name": "type", "type": "STRING"},
                ],
              },
              {
                "label": "LineItem",
                "properties": [
                  {"name": "debit_amount", "type": "DOUBLE"},
                  {"name": "credit_amount", "type": "DOUBLE"},
                ],
              },
              {
                "label": "Element",
                "properties": [
                  {"name": "name", "type": "STRING"},
                  {"name": "classification", "type": "STRING"},
                  {"name": "balance", "type": "STRING"},
                ],
              },
            ]
          },
        },
        {
          "summary": "SEC repository schema",
          "description": "Schema for SEC shared repository with XBRL structures",
          "value": {
            "schema": [
              {
                "label": "Entity",
                "properties": [
                  {"name": "cik", "type": "STRING"},
                  {"name": "name", "type": "STRING"},
                  {"name": "ticker", "type": "STRING"},
                  {"name": "entity_type", "type": "STRING"},
                  {"name": "industry", "type": "STRING"},
                ],
              },
              {
                "label": "Report",
                "properties": [
                  {"name": "form", "type": "STRING"},
                  {"name": "report_date", "type": "STRING"},
                  {"name": "filing_date", "type": "STRING"},
                  {"name": "accession_number", "type": "STRING"},
                ],
              },
              {
                "label": "Fact",
                "properties": [
                  {"name": "numeric_value", "type": "DOUBLE"},
                  {"name": "decimals", "type": "INT64"},
                  {"name": "fact_type", "type": "STRING"},
                ],
              },
              {
                "label": "Element",
                "properties": [
                  {"name": "name", "type": "STRING"},
                  {"name": "namespace", "type": "STRING"},
                ],
              },
            ]
          },
        },
        {
          "summary": "Custom graph schema",
          "description": "Schema for custom graph with people, companies, and projects",
          "value": {
            "schema": [
              {
                "label": "Person",
                "properties": [
                  {"name": "identifier", "type": "STRING"},
                  {"name": "name", "type": "STRING"},
                  {"name": "title", "type": "STRING"},
                  {"name": "interests", "type": "STRING"},
                ],
              },
              {
                "label": "Company",
                "properties": [
                  {"name": "identifier", "type": "STRING"},
                  {"name": "name", "type": "STRING"},
                  {"name": "industry", "type": "STRING"},
                  {"name": "location", "type": "STRING"},
                ],
              },
              {
                "label": "Project",
                "properties": [
                  {"name": "name", "type": "STRING"},
                  {"name": "status", "type": "STRING"},
                  {"name": "budget", "type": "DOUBLE"},
                ],
              },
            ]
          },
        },
        {
          "summary": "Empty graph schema",
          "description": "Schema for a newly created graph with no nodes yet",
          "value": {"schema": []},
        },
      ]
    }
  )

  schema_data: list[dict[str, object]] = Field(
    ...,
    description="Graph database schema with node types, properties, and relationships",
    alias="schema",
  )


class MCPQueryResponse(BaseModel):
  """Response model for MCP query execution."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "SEC entities query",
          "description": "Query results from SEC shared repository showing public companies",
          "value": {
            "results": [
              {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "cik": "0000320193",
                "industry": "Technology",
                "state": "CA",
                "fiscal_year_end": "0930",
              },
              {
                "ticker": "MSFT",
                "company_name": "Microsoft Corporation",
                "cik": "0000789019",
                "industry": "Technology",
                "state": "WA",
                "fiscal_year_end": "0630",
              },
            ]
          },
        },
        {
          "summary": "Accounting trial balance",
          "description": "Query results from accounting graph showing trial balance calculations",
          "value": {
            "results": [
              {
                "account": "Cash",
                "type": "asset",
                "total_debits": 125000.0,
                "total_credits": 85000.0,
                "net_balance": 40000.0,
              },
              {
                "account": "Revenue",
                "type": "revenue",
                "total_debits": 0.0,
                "total_credits": 250000.0,
                "net_balance": -250000.0,
              },
            ]
          },
        },
        {
          "summary": "Custom graph teams",
          "description": "Query results from custom graph showing cross-company project teams",
          "value": {
            "results": [
              {
                "project": "Cloud Platform",
                "company_a": "TechCorp",
                "teammate_a": "Alice Johnson",
                "company_b": "InnovateCo",
                "teammate_b": "Bob Smith",
              },
              {
                "project": "Cloud Platform",
                "company_a": "TechCorp",
                "teammate_a": "Alice Johnson",
                "company_b": "DataSystems",
                "teammate_b": "Carol Davis",
              },
            ]
          },
        },
        {
          "summary": "Financial facts aggregation",
          "description": "Query results with XBRL fact aggregations from SEC reports",
          "value": {
            "results": [
              {
                "element_name": "Revenues",
                "usage_count": 1250,
              },
              {
                "element_name": "Assets",
                "usage_count": 1100,
              },
              {
                "element_name": "Liabilities",
                "usage_count": 980,
              },
            ]
          },
        },
        {
          "summary": "Empty result set",
          "description": "Query executed successfully but returned no matching results",
          "value": {"results": []},
        },
      ]
    }
  )

  results: list[dict[str, object]] = Field(
    ..., description="Query results from graph database"
  )
