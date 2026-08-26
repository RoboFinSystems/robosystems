"""MCP (Model Context Protocol) API models.

`MCPToolCall` is the one request shape the transport shares with the tool
layer; the REST envelope models went with the REST endpoints.
"""

from pydantic import BaseModel, ConfigDict, Field


class MCPToolCall(BaseModel):
  """Request model for MCP tool execution."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "read-graph-cypher",
          "arguments": {
            "query": "MATCH (n) RETURN label(n) AS type, count(n) AS count ORDER BY count DESC",
            "parameters": {},
          },
        },
        {
          "name": "read-graph-cypher",
          "arguments": {
            "query": "MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WITH e.name AS account, e.classification AS type, sum(li.debit_amount) AS total_debits, sum(li.credit_amount) AS total_credits RETURN account, type, total_debits, total_credits, total_debits - total_credits AS net_balance ORDER BY account",
            "parameters": {},
          },
        },
        {
          "name": "get-graph-schema",
          "arguments": {},
        },
        {
          "name": "get-graph-info",
          "arguments": {},
        },
        {
          "name": "read-graph-cypher",
          "arguments": {
            "query": "MATCH (t:Transaction)-[:TRANSACTION_HAS_ENTRY]->(en:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element) WHERE e.classification = $classification AND substring(t.date, 1, 7) = $month RETURN sum(li.credit_amount) AS total_revenue",
            "parameters": {"classification": "revenue", "month": "2025-09"},
            "timeout_override": 60,
          },
        },
      ]
    }
  )

  name: str = Field(..., description="Name of the MCP tool to execute", min_length=1)
  arguments: dict[str, object] = Field(
    default_factory=dict, description="Arguments to pass to the tool"
  )
