"""CypherOperator — agentic natural-language querying of the graph.

Drives a bounded tool-use loop (``run_tool_loop``): the model discovers the
schema, writes read-only Cypher, sees query errors and retries, then answers
in natural language. Returns the last result set as structured ``rows`` so the
console can render a real table rather than scraping the prose. Primary
operator for the console interface.

This replaced an earlier single-shot pipeline (fetch truncated schema →
generate one query → execute once → format) that never showed the model its
query errors and so failed on basic questions. The loop is the harness that
makes Claude-via-MCP robust, run in-process on Bedrock.
"""

from __future__ import annotations

from typing import Any

from robosystems.config import OperatorConfig
from robosystems.operations.operators.base import (
  ExecutionProfile,
  Operator,
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import register_operator
from robosystems.operations.operators.tool_loop import ToolLoopResult, run_tool_loop


@register_operator("cypher")
class CypherOperator(Operator):
  """Converts natural language to Cypher and answers via a tool-use loop."""

  # Read-only tool allowlist. The loop intersects this with the tools the
  # graph actually exposes (generic graphs get only schema + cypher; SEC and
  # roboledger graphs also get example queries, element resolution, GraphQL,
  # and document search). Write tools are never included.
  READ_ONLY_TOOLS = [
    "get-graph-schema",
    "read-graph-cypher",
    "get-example-queries",
    "get-graphql-schema",
    "query-graphql",
    "resolve-element",
    "search-documents",
  ]

  spec = OperatorSpec(
    name="Cypher Operator",
    description="Answers natural-language questions by querying the graph with Cypher",
    capabilities=[
      OperatorCapability.RAG_SEARCH,
      OperatorCapability.ENTITY_ANALYSIS,
      OperatorCapability.CUSTOM,
    ],
    version="2.0.0",
    requires_credits=True,
    execution_profile={
      OperatorMode.QUICK: ExecutionProfile(
        min_time=3, max_time=12, avg_time=6, tool_calls=3
      ),
      OperatorMode.STANDARD: ExecutionProfile(
        min_time=6, max_time=25, avg_time=12, tool_calls=6
      ),
      OperatorMode.EXTENDED: ExecutionProfile(
        min_time=15, max_time=90, avg_time=40, tool_calls=13
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

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    limits = OperatorConfig.get_mode_limits(ctx.mode.value)
    max_results = self._get_max_results(ctx.mode)
    # One iteration per allowed tool call, plus a final answer turn.
    max_iterations = int(limits.get("max_tools", 5)) + 1
    max_tokens = int(limits.get("max_output_tokens", 4000))
    output_mode = "answer" if ctx.extra.get("output_mode") == "answer" else "narrative"

    system = self._build_system_prompt(max_results, output_mode)

    result = await run_tool_loop(
      ctx,
      system=system,
      tool_names=self.READ_ONLY_TOOLS,
      max_iterations=max_iterations,
      max_tokens=max_tokens,
      temperature=0.3,
      operator_type="cypher",
      operation_description="Cypher query loop",
    )

    await ctx.progress.report("Done", percent=100)

    rows = result.rows or []
    return OperatorResult(
      content=result.text,
      metadata={
        # Structured outputs the console renders directly — no prose scraping.
        "cypher": result.cypher,
        "rows": rows,
        "result_count": len(rows),
        "hit_step_limit": result.hit_cap,
        "loop_iterations": result.iterations,
      },
      # De-dupe preserving order (a tool may be called several times).
      tools_called=list(dict.fromkeys(result.tools_called)),
      confidence_score=self._calculate_confidence(result),
    )

  def _build_system_prompt(self, max_results: int, output_mode: str) -> str:
    prompt = f"""You are a graph database analyst for RoboSystems. You answer the user's question by querying a LadybugDB graph with read-only Cypher.

WORKFLOW:
1. Call `get-graph-schema` first to discover node labels, relationships, and properties — never guess the schema.
2. If `get-example-queries` is available, use it for working query patterns tuned to this graph.
3. Write a read-only Cypher query and run it with `read-graph-cypher`.
4. If a query errors or returns nothing useful, read the error, fix the query, and try again. You have a limited number of steps, so be efficient — don't repeat a failing query unchanged.
5. When you have the answer, respond in natural language.

CYPHER RULES:
- Read-only only: MATCH, WHERE, WITH, RETURN, ORDER BY, LIMIT. Never CREATE, SET, DELETE, MERGE, or DROP.
- Always include a LIMIT (max {max_results}).
- Use CONTAINS for text search; guard against NULLs with IS NOT NULL.
- Anchor every query on a selective, indexed starting point (an Entity by ticker, a Report by identifier, an Element by qname) and expand from there. Never start a MATCH from an unfiltered global pattern like `(f:Fact)` or `(n)` on a large graph — it will time out.

LEDGER DATA (only when the schema has Entry / Transaction / LineItem nodes):
- The graph keeps cancelled/replaced rows. When counting or summing ledger data, filter to live rows: match Entry `status = 'posted'` for balances and debit/credit sums; exclude Event rows where `status = 'voided' OR status = 'superseded'`. Aggregate realized amounts through posted Entry/LineItem, not by summing Transaction.amount.
"""
    if output_mode == "answer":
      prompt += (
        "\nFINAL ANSWER: Give a direct, concise answer — no preamble. The raw "
        "rows are shown to the user as a table, so don't re-list them."
      )
    else:
      prompt += (
        "\nFINAL ANSWER: Briefly explain what you found in clear prose, calling "
        "out the key figures or patterns. The application shows the raw rows to "
        "the user as a table, so do NOT draw your own table or re-list every "
        "row — summarize and interpret."
      )
    return prompt

  def _get_max_results(self, mode: OperatorMode) -> int:
    return {
      OperatorMode.QUICK: 50,
      OperatorMode.STANDARD: 100,
      OperatorMode.EXTENDED: 500,
    }.get(mode, 100)

  def _calculate_confidence(self, result: ToolLoopResult) -> float:
    if result.hit_cap:
      return 0.5
    if result.rows:
      return 0.9
    return 0.6
