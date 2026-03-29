"""CypherAgent — natural language to Cypher query conversion and execution.

Converts natural language questions into Cypher queries, executes them
via MCP tools, and returns formatted results. Primary agent for the
console interface.
"""

from __future__ import annotations

import json
from typing import Any

from robosystems.logger import logger
from robosystems.operations.agents.agent_context import AgentContext
from robosystems.operations.agents.agent_registry import register_agent
from robosystems.operations.agents.ai_client import AIMessage
from robosystems.operations.agents.base import (
  Agent,
  AgentCapability,
  AgentMode,
  AgentResult,
  AgentSpec,
  ExecutionProfile,
)


@register_agent("cypher")
class CypherAgent(Agent):
  """Converts natural language to Cypher queries and executes them."""

  spec = AgentSpec(
    name="Cypher Agent",
    description="Converts natural language to Cypher queries and executes them",
    capabilities=[
      AgentCapability.RAG_SEARCH,
      AgentCapability.ENTITY_ANALYSIS,
      AgentCapability.CUSTOM,
    ],
    version="1.0.0",
    requires_credits=True,
    execution_profile={
      AgentMode.QUICK: ExecutionProfile(
        min_time=2, max_time=5, avg_time=3, tool_calls=2
      ),
      AgentMode.STANDARD: ExecutionProfile(
        min_time=5, max_time=15, avg_time=10, tool_calls=3
      ),
      AgentMode.EXTENDED: ExecutionProfile(
        min_time=15, max_time=60, avg_time=30, tool_calls=5
      ),
    },
  )

  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    query_lower = query.lower()

    if any(
      kw in query_lower for kw in ["cypher", "query", "graph", "node", "relationship"]
    ):
      return 1.0

    if any(
      kw in query_lower for kw in ["show", "find", "get", "list", "count", "search"]
    ):
      return 0.9

    if "?" in query or any(
      kw in query_lower for kw in ["what", "how", "where", "when", "who"]
    ):
      return 0.8

    return 0.7

  async def run(self, ctx: AgentContext) -> AgentResult:
    await ctx.progress.report("Getting graph schema...", percent=10)
    schema = await ctx.tools.call_tool("get-graph-schema", {}, return_raw=True)

    await ctx.progress.report("Generating Cypher query...", percent=30)
    cypher_query = await self._generate_cypher(ctx, schema)

    await ctx.progress.report("Executing query...", percent=60)
    results = await ctx.tools.call_tool(
      "read-graph-cypher",
      {"query": cypher_query, "parameters": {}},
      return_raw=True,
    )

    await ctx.progress.report("Formatting results...", percent=90)
    formatted = await self._format_results(ctx, cypher_query, results)

    return AgentResult(
      content=formatted,
      metadata={
        "cypher_query": cypher_query,
        "result_count": len(results) if results else 0,
      },
      tools_called=["get-graph-schema", "read-graph-cypher"],
      confidence_score=self._calculate_confidence(cypher_query, results),
    )

  async def _generate_cypher(
    self, ctx: AgentContext, schema: list[dict[str, Any]]
  ) -> str:
    schema_text = self._format_schema_for_ai(schema)
    max_results = self._get_max_results(ctx.mode)

    system_prompt = f"""You are a Cypher query expert for RoboSystems graph databases.

SCHEMA:
{schema_text}

IMPORTANT RULES:
1. Generate ONLY the Cypher query - no explanations, no markdown formatting
2. Queries must be read-only (MATCH, RETURN, WHERE, WITH, ORDER BY, LIMIT)
3. No write operations (CREATE, SET, DELETE, MERGE, DROP)
4. Always include a LIMIT clause (max {max_results})
5. Use parameterized queries when possible
6. Handle NULL values appropriately
7. Use CONTAINS for text search, not exact matches

SCHEMA PATTERNS:
- Financial facts: MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
- Time periods: MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
- Entities: MATCH (e:Entity)-[:HAS_REPORT]->(r:Report)

Return ONLY the Cypher query, nothing else."""

    messages = []
    if ctx.history:
      for msg in ctx.history[-5:]:
        role = (
          msg.get("role", "user")
          if isinstance(msg, dict)
          else getattr(msg, "role", "user")
        )
        content = (
          msg.get("content", "")
          if isinstance(msg, dict)
          else getattr(msg, "content", "")
        )
        messages.append(AIMessage(role=role, content=content))

    messages.append(
      AIMessage(
        role="user",
        content=f"Convert this natural language query to Cypher:\n\n{ctx.query}",
      )
    )

    response = await ctx.ai.create_message(
      messages=messages,
      system=system_prompt,
      max_tokens=2000,
      temperature=0.3,
      operation_description="Cypher query generation",
    )

    cypher_query = response.content.strip()
    cypher_query = cypher_query.replace("```cypher", "").replace("```", "").strip()

    logger.info(f"Generated Cypher: {cypher_query}")
    return cypher_query

  async def _format_results(
    self,
    ctx: AgentContext,
    cypher_query: str,
    results: list[dict[str, Any]],
  ) -> str:
    if not results:
      return f"No results found for your query.\n\nGenerated Cypher:\n{cypher_query}"

    if ctx.mode == AgentMode.QUICK:
      return self._simple_format(cypher_query, results)

    results_sample = results[:10]
    results_json = json.dumps(results_sample, indent=2, default=str)

    system_prompt = """You are a helpful assistant that explains graph query results.

Format the results in a clear, concise way that directly answers the user's question.
If there are patterns or insights in the data, mention them briefly.
Keep your response focused and actionable."""

    messages = [
      AIMessage(
        role="user",
        content=f"""User asked: "{ctx.query}"

I executed this Cypher query:
{cypher_query}

Results ({len(results)} total, showing first 10):
{results_json}

Please explain these results in a clear, natural way.""",
      )
    ]

    response = await ctx.ai.create_message(
      messages=messages,
      system=system_prompt,
      max_tokens=1500,
      temperature=0.5,
      operation_description="Result formatting",
    )

    formatted = (
      f"{response.content}\n\n**Generated Cypher:**\n```cypher\n{cypher_query}\n```"
    )
    if len(results) > 10:
      formatted += f"\n\n*Showing 10 of {len(results)} results*"

    return formatted

  def _simple_format(self, cypher_query: str, results: list[dict[str, Any]]) -> str:
    formatted = f"**Generated Cypher:**\n```cypher\n{cypher_query}\n```\n\n"
    formatted += f"**Results:** {len(results)} rows\n\n"
    if results:
      sample = results[:5]
      formatted += "```json\n" + json.dumps(sample, indent=2, default=str) + "\n```"
      if len(results) > 5:
        formatted += f"\n\n*Showing 5 of {len(results)} results*"
    return formatted

  def _format_schema_for_ai(self, schema: list[dict[str, Any]]) -> str:
    formatted = []
    for item in schema[:20]:
      if item.get("type") == "node":
        props = ", ".join(
          [f"{p['name']}: {p['type']}" for p in item.get("properties", [])[:5]]
        )
        formatted.append(f"Node {item['label']}: {props}")
      elif item.get("type") == "relationship":
        formatted.append(
          f"Relationship {item['label']}: {item.get('from', '?')} -> {item.get('to', '?')}"
        )
    return "\n".join(formatted) if formatted else "Schema information not available"

  def _get_max_results(self, mode: AgentMode) -> int:
    return {
      AgentMode.QUICK: 50,
      AgentMode.STANDARD: 100,
      AgentMode.EXTENDED: 500,
    }.get(mode, 100)

  def _calculate_confidence(self, cypher_query: str, results: list | None) -> float:
    if not cypher_query or "ERROR" in cypher_query.upper():
      return 0.3
    if results is None:
      return 0.5
    if len(results) == 0:
      return 0.6
    if len(results) > 0:
      return 0.9
    return 0.7
