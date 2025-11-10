"""
Cypher Agent - Natural language to Cypher query conversion and execution.

This agent uses Claude (via Bedrock or Anthropic SDK) to convert natural language
questions into Cypher queries, executes them via MCP tools, and returns results.
"""

from typing import Dict, List, Optional, Any
import json

from robosystems.operations.agents.base import (
  BaseAgent,
  AgentMode,
  AgentCapability,
  AgentMetadata,
  AgentResponse,
)
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.operations.agents.ai_client import AIClient, AIMessage
from robosystems.logger import logger


@AgentRegistry.register("cypher")
class CypherAgent(BaseAgent):
  """
  Agent specialized in converting natural language to Cypher queries.

  This is the primary agent for the console interface, replicating the
  functionality of Claude Desktop/Code's MCP integration.
  """

  @property
  def metadata(self) -> AgentMetadata:
    """Return agent metadata."""
    return AgentMetadata(
      name="Cypher Agent",
      description="Converts natural language to Cypher queries and executes them",
      capabilities=[
        AgentCapability.RAG_SEARCH,
        AgentCapability.ENTITY_ANALYSIS,
        AgentCapability.CUSTOM,
      ],
      version="1.0.0",
      supported_modes=[
        AgentMode.QUICK,
        AgentMode.STANDARD,
        AgentMode.EXTENDED,
      ],
      author="RoboSystems",
      tags=["cypher", "query", "graph", "nlp"],
      requires_credits=True,
    )

  def __init__(self, graph_id: str, user, db_session=None):
    """Initialize Cypher agent."""
    super().__init__(graph_id, user, db_session)
    self.ai_client = AIClient()

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    callback: Optional[Any] = None,
  ) -> AgentResponse:
    """
    Convert natural language query to Cypher and execute.

    Args:
        query: Natural language question
        mode: Execution mode
        history: Conversation history
        context: Additional context
        callback: Progress callback

    Returns:
        AgentResponse with Cypher query and results
    """
    try:
      self.validate_mode(mode)

      if not self.kuzu_client:
        await self.initialize_tools()

      enhanced_context = await self.prepare_context(query, context)

      if callback:
        callback("initialization", 10, "Getting graph schema...")

      schema = await self.mcp_tools.call_tool("get-graph-schema", {}, return_raw=True)

      if callback:
        callback("analysis", 30, "Converting natural language to Cypher...")

      cypher_query = await self._generate_cypher(query, schema, history, mode)

      if callback:
        callback("execution", 60, "Executing Cypher query...")

      results = await self.mcp_tools.call_tool(
        "read-graph-cypher",
        {
          "query": cypher_query,
          "parameters": {},
        },
        return_raw=True,
      )

      if callback:
        callback("formatting", 90, "Formatting results...")

      formatted_response = await self._format_results(
        query, cypher_query, results, mode
      )

      if callback:
        callback("completion", 100, "Complete")

      response_metadata = {
        **enhanced_context,
        "cypher_query": cypher_query,
        "result_count": len(results) if results else 0,
        "backend": self.ai_client.backend,
      }

      if self._last_credit_consumption:
        response_metadata["credits_consumed"] = self._last_credit_consumption.get(
          "credits_consumed", 0
        )
        response_metadata["credits_remaining"] = self._last_credit_consumption.get(
          "remaining_balance", 0
        )

      return AgentResponse(
        content=formatted_response,
        agent_name=self.metadata.name,
        mode_used=mode,
        metadata=response_metadata,
        tokens_used=self.total_tokens_used,
        tools_called=["get-graph-schema", "read-graph-cypher"],
        confidence_score=self._calculate_confidence(cypher_query, results),
      )

    except Exception as e:
      import traceback

      logger.error(f"Cypher agent error: {str(e)}")
      logger.error(f"Full traceback:\n{traceback.format_exc()}")
      return AgentResponse(
        content=f"Query processing failed: {str(e)}",
        agent_name=self.metadata.name,
        mode_used=mode,
        tokens_used=self.total_tokens_used,
        error_details={
          "code": "QUERY_GENERATION_ERROR",
          "message": str(e),
          "traceback": traceback.format_exc(),
        },
      )

  def can_handle(self, query: str, context: Optional[Dict[str, Any]] = None) -> float:
    """
    Cypher agent can handle all queries with varying confidence.

    Args:
        query: The query to evaluate
        context: Optional context

    Returns:
        Confidence score (0.5-1.0)
    """
    query_lower = query.lower()

    if any(
      keyword in query_lower
      for keyword in ["cypher", "query", "graph", "node", "relationship"]
    ):
      return 1.0

    if any(
      keyword in query_lower
      for keyword in ["show", "find", "get", "list", "count", "search"]
    ):
      return 0.9

    if "?" in query or any(
      keyword in query_lower for keyword in ["what", "how", "where", "when", "who"]
    ):
      return 0.8

    return 0.7

  async def _generate_cypher(
    self,
    user_query: str,
    schema: List[Dict[str, Any]],
    history: Optional[List[Dict[str, Any]]],
    mode: AgentMode,
  ) -> str:
    """
    Generate Cypher query from natural language using AI.

    Args:
        user_query: Natural language question
        schema: Graph schema
        history: Conversation history
        mode: Execution mode

    Returns:
        Cypher query string
    """
    schema_text = self._format_schema_for_ai(schema)

    system_prompt = f"""You are a Cypher query expert for RoboSystems graph databases.

SCHEMA:
{schema_text}

IMPORTANT RULES:
1. Generate ONLY the Cypher query - no explanations, no markdown formatting
2. Queries must be read-only (MATCH, RETURN, WHERE, WITH, ORDER BY, LIMIT)
3. No write operations (CREATE, SET, DELETE, MERGE, DROP)
4. Always include a LIMIT clause (max {self._get_max_results(mode)})
5. Use parameterized queries when possible
6. Handle NULL values appropriately
7. Use CONTAINS for text search, not exact matches

SCHEMA PATTERNS:
- Financial facts: MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
- Time periods: MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
- Entities: MATCH (e:Entity)-[:HAS_REPORT]->(r:Report)

Return ONLY the Cypher query, nothing else."""

    messages = []

    if history:
      for msg in history[-5:]:
        if isinstance(msg, dict):
          role = msg.get("role", "user")
          content = msg.get("content", "")
        else:
          role = getattr(msg, "role", "user")
          content = getattr(msg, "content", "")

        messages.append(AIMessage(role=role, content=content))

    messages.append(
      AIMessage(
        role="user",
        content=f"Convert this natural language query to Cypher:\n\n{user_query}",
      )
    )

    response = await self.ai_client.create_message(
      messages=messages,
      system=system_prompt,
      max_tokens=2000,
      temperature=0.3,
    )

    self.track_tokens(response.input_tokens, response.output_tokens)

    credit_result = await self.consume_credits(
      response.input_tokens,
      response.output_tokens,
      response.model,
      "Cypher query generation",
    )

    if credit_result:
      self._last_credit_consumption = {
        "credits_consumed": credit_result.get("credits_consumed", 0),
        "remaining_balance": credit_result.get("remaining_balance", 0),
      }

    cypher_query = response.content.strip()

    cypher_query = cypher_query.replace("```cypher", "").replace("```", "").strip()

    logger.info(f"Generated Cypher: {cypher_query}")
    return cypher_query

  async def _format_results(
    self,
    user_query: str,
    cypher_query: str,
    results: List[Dict[str, Any]],
    mode: AgentMode,
  ) -> str:
    """
    Format query results using AI for natural language response.

    Args:
        user_query: Original natural language question
        cypher_query: Generated Cypher query
        results: Query results
        mode: Execution mode

    Returns:
        Formatted response string
    """
    if not results:
      return f"No results found for your query.\n\nGenerated Cypher:\n{cypher_query}"

    if mode == AgentMode.QUICK:
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
        content=f"""User asked: "{user_query}"

I executed this Cypher query:
{cypher_query}

Results ({len(results)} total, showing first 10):
{results_json}

Please explain these results in a clear, natural way.""",
      )
    ]

    response = await self.ai_client.create_message(
      messages=messages,
      system=system_prompt,
      max_tokens=1500,
      temperature=0.5,
    )

    self.track_tokens(response.input_tokens, response.output_tokens)

    credit_result = await self.consume_credits(
      response.input_tokens,
      response.output_tokens,
      response.model,
      "Result formatting",
    )

    if credit_result and hasattr(self, "_last_credit_consumption"):
      self._last_credit_consumption["credits_consumed"] += credit_result.get(
        "credits_consumed", 0
      )

    formatted = (
      f"{response.content}\n\n**Generated Cypher:**\n```cypher\n{cypher_query}\n```"
    )

    if len(results) > 10:
      formatted += f"\n\n*Showing 10 of {len(results)} results*"

    return formatted

  def _simple_format(self, cypher_query: str, results: List[Dict[str, Any]]) -> str:
    """Simple formatting for QUICK mode (no AI)."""
    formatted = f"**Generated Cypher:**\n```cypher\n{cypher_query}\n```\n\n"
    formatted += f"**Results:** {len(results)} rows\n\n"

    if results:
      sample = results[:5]
      formatted += "```json\n" + json.dumps(sample, indent=2, default=str) + "\n```"

      if len(results) > 5:
        formatted += f"\n\n*Showing 5 of {len(results)} results*"

    return formatted

  def _format_schema_for_ai(self, schema: List[Dict[str, Any]]) -> str:
    """Format schema for AI context."""
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
    """Get maximum result count based on mode."""
    return {
      AgentMode.QUICK: 50,
      AgentMode.STANDARD: 100,
      AgentMode.EXTENDED: 500,
    }.get(mode, 100)

  def _calculate_confidence(self, cypher_query: str, results: Optional[List]) -> float:
    """Calculate confidence in the response."""
    if not cypher_query or "ERROR" in cypher_query.upper():
      return 0.3

    if results is None:
      return 0.5

    if len(results) == 0:
      return 0.6

    if len(results) > 0:
      return 0.9

    return 0.7
