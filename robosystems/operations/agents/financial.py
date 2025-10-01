"""
Financial analysis agent implementation.

Specializes in financial analysis, SEC filings, and accounting data.
"""

import json
from typing import Dict, List, Optional, Any
from anthropic import Anthropic

from robosystems.operations.agents.base import (
  BaseAgent,
  AgentMode,
  AgentCapability,
  AgentMetadata,
  AgentResponse,
)
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.logger import logger
from robosystems.config import env


@AgentRegistry.register("financial")
class FinancialAgent(BaseAgent):
  """
  Agent specialized in financial analysis and reporting.

  Handles SEC filings, QuickBooks data, and financial metrics.
  """

  @property
  def metadata(self) -> AgentMetadata:
    """Return agent metadata."""
    return AgentMetadata(
      name="Financial Agent",
      description="Specialized in financial analysis, SEC filings, and accounting data",
      capabilities=[
        AgentCapability.FINANCIAL_ANALYSIS,
        AgentCapability.ENTITY_ANALYSIS,
        AgentCapability.TREND_ANALYSIS,
      ],
      version="2.0.0",
      supported_modes=[
        AgentMode.QUICK,
        AgentMode.STANDARD,
        AgentMode.EXTENDED,
      ],
      author="RoboSystems",
      tags=["finance", "sec", "quickbooks", "analysis"],
    )

  def __init__(self, graph_id: str, user, db_session=None):
    """Initialize financial agent."""
    super().__init__(graph_id, user, db_session)

    # Initialize Anthropic if available
    self.anthropic = None
    if env.ANTHROPIC_API_KEY:
      self.anthropic = Anthropic(api_key=env.ANTHROPIC_API_KEY)
      self.model = env.ANTHROPIC_MODEL or "claude-3-sonnet-20240229"

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    callback: Optional[Any] = None,
  ) -> AgentResponse:
    """
    Perform financial analysis.

    Args:
        query: The financial query to analyze
        mode: Execution mode
        history: Conversation history
        context: Additional context
        callback: Progress callback

    Returns:
        AgentResponse with financial analysis
    """
    try:
      # Validate mode
      self.validate_mode(mode)

      # Initialize tools if not already done
      if not self.kuzu_client:
        await self.initialize_tools()

      # Initialize credit consumption tracking
      self._last_credit_consumption = None

      # Prepare context
      enhanced_context = await self.prepare_context(query, context)

      # Get mode limits
      limits = self.get_mode_limits(mode)

      # Execute analysis based on mode
      if mode == AgentMode.QUICK:
        response_content = await self._quick_analysis(
          query, history, enhanced_context, limits
        )
      elif mode == AgentMode.EXTENDED:
        response_content = await self._extended_analysis(
          query, history, enhanced_context, limits, callback
        )
      else:  # STANDARD
        response_content = await self._standard_analysis(
          query, history, enhanced_context, limits
        )

      # Add credit consumption info to metadata if available
      if self._last_credit_consumption:
        enhanced_context["credits_consumed"] = self._last_credit_consumption.get(
          "credits_consumed", 0
        )
        enhanced_context["credits_remaining"] = self._last_credit_consumption.get(
          "remaining_balance", 0
        )

      # Return response
      return AgentResponse(
        content=response_content,
        agent_name=self.metadata.name,
        mode_used=mode,
        metadata=enhanced_context,
        tokens_used=self.total_tokens_used,
        tools_called=["kuzu_query", "financial_analysis"],
        confidence_score=self._calculate_confidence(query, response_content),
      )

    except Exception as e:
      logger.error(f"Financial agent error: {str(e)}")
      return AgentResponse(
        content=f"Financial analysis failed: {str(e)}",
        agent_name=self.metadata.name,
        mode_used=mode,
        error_details={
          "code": "ANALYSIS_ERROR",
          "message": str(e),
        },
      )

  def can_handle(self, query: str, context: Optional[Dict[str, Any]] = None) -> float:
    """
    Calculate confidence for handling financial queries.

    Args:
        query: The query to evaluate
        context: Optional context

    Returns:
        Confidence score between 0 and 1
    """
    query_lower = query.lower()

    # High confidence keywords
    high_confidence = [
      "financial",
      "revenue",
      "profit",
      "loss",
      "balance sheet",
      "income statement",
      "cash flow",
      "sec",
      "10-k",
      "10-q",
      "quickbooks",
      "accounting",
      "gaap",
      "xbrl",
      "earnings",
      "margin",
      "ebitda",
      "roi",
      "roa",
      "eps",
    ]

    # Medium confidence keywords
    medium_confidence = [
      "money",
      "cost",
      "expense",
      "income",
      "assets",
      "liabilities",
      "equity",
      "investment",
      "valuation",
      "metrics",
      "kpi",
    ]

    # Calculate score
    score = 0.0

    for keyword in high_confidence:
      if keyword in query_lower:
        score += 0.3

    for keyword in medium_confidence:
      if keyword in query_lower:
        score += 0.15

    # Check context for financial indicators
    if context:
      if context.get("domain") == "finance":
        score += 0.2
      if context.get("has_financial_data"):
        score += 0.1

    # Cap at 1.0
    return min(score, 1.0)

  async def _quick_analysis(
    self,
    query: str,
    history: Optional[List[Dict]],
    context: Dict[str, Any],
    limits: Dict[str, Any],
  ) -> str:
    """Perform quick financial analysis with limited tool calls."""
    # Quick analysis with 1-2 tool calls
    results = []

    # Get basic financial data
    if self.mcp_tools:
      try:
        # Simple query for key metrics
        result = await self.mcp_tools.call_tool(
          "read-graph-cypher",
          {
            "query": self._build_financial_query(query),
            "parameters": {},
          },
        )
        results.append(result)
      except Exception as e:
        logger.error(f"Tool call failed: {str(e)}")

    # Format response
    if results:
      return self._format_financial_response(query, results, "quick")
    else:
      return "Unable to retrieve financial data. Please try a more specific query."

  async def _standard_analysis(
    self,
    query: str,
    history: Optional[List[Dict]],
    context: Dict[str, Any],
    limits: Dict[str, Any],
  ) -> str:
    """Perform standard financial analysis."""
    # Standard analysis with 3-5 tool calls
    results = []

    if self.mcp_tools:
      # Get schema first
      schema = await self.mcp_tools.call_tool("get-graph-schema", {})

      # Query for financial data
      queries = self._generate_financial_queries(query, schema)

      for q in queries[: limits["max_tools"]]:
        try:
          result = await self.mcp_tools.call_tool(
            "read-graph-cypher",
            {"query": q, "parameters": {}},
          )
          results.append(result)
        except Exception as e:
          logger.error(f"Query failed: {str(e)}")

    # Use AI if available
    if self.anthropic and results:
      return await self._ai_financial_analysis(query, results, history)

    return self._format_financial_response(query, results, "standard")

  async def _extended_analysis(
    self,
    query: str,
    history: Optional[List[Dict]],
    context: Dict[str, Any],
    limits: Dict[str, Any],
    callback: Optional[Any] = None,
  ) -> str:
    """Perform extended financial analysis with deep research."""
    results = []

    if callback:
      callback("initialization", 10, "Starting financial analysis...")

    # Comprehensive analysis with multiple tool calls
    if self.mcp_tools:
      # Get all relevant financial data
      if callback:
        callback("data_gathering", 30, "Gathering financial data...")

      # Multiple queries for comprehensive data
      queries = self._generate_comprehensive_queries(query)

      for i, q in enumerate(queries[: limits["max_tools"]]):
        if callback:
          progress = 30 + (40 * i / len(queries))
          callback("querying", progress, f"Executing query {i + 1}/{len(queries)}")

        try:
          result = await self.mcp_tools.call_tool(
            "read-graph-cypher",
            {"query": q, "parameters": {}},
          )
          results.append(result)
        except Exception as e:
          logger.error(f"Query {i + 1} failed: {str(e)}")

    if callback:
      callback("analysis", 80, "Analyzing financial data...")

    # Deep analysis with AI
    if self.anthropic and results:
      response = await self._ai_deep_financial_analysis(query, results, history)
    else:
      response = self._format_financial_response(query, results, "extended")

    if callback:
      callback("completion", 100, "Analysis complete")

    return response

  def _build_financial_query(self, user_query: str) -> str:
    """Build a Cypher query for financial data."""
    # Simplified query builder
    if "revenue" in user_query.lower():
      return """
            MATCH (f:Fact)-[:HAS_ELEMENT]->(e:Element)
            WHERE e.name CONTAINS 'Revenue'
            RETURN f.value, f.period, e.name
            ORDER BY f.period DESC
            LIMIT 10
            """
    elif "balance" in user_query.lower():
      return """
            MATCH (f:Fact)-[:HAS_ELEMENT]->(e:Element)
            WHERE e.name CONTAINS 'Assets' OR e.name CONTAINS 'Liabilities'
            RETURN f.value, f.period, e.name
            ORDER BY f.period DESC
            LIMIT 20
            """
    else:
      return """
            MATCH (f:Fact)-[:HAS_ELEMENT]->(e:Element)
            RETURN f.value, f.period, e.name
            LIMIT 20
            """

  def _generate_financial_queries(self, user_query: str, schema: Any) -> List[str]:
    """Generate multiple queries for financial analysis."""
    queries = []

    # Add queries based on query content
    if "trend" in user_query.lower():
      queries.append("""
            MATCH (f:Fact)-[:HAS_ELEMENT]->(e:Element)
            WHERE e.name CONTAINS 'Revenue'
            RETURN f.period, sum(f.value) as total
            ORDER BY f.period
            """)

    if "compare" in user_query.lower():
      queries.append("""
            MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
            RETURN e.name, sum(f.value) as total
            ORDER BY total DESC
            LIMIT 10
            """)

    # Default query
    queries.append(self._build_financial_query(user_query))

    return queries

  def _generate_comprehensive_queries(self, user_query: str) -> List[str]:
    """Generate comprehensive queries for extended analysis."""
    queries = [
      # Get entity overview
      "MATCH (e:Entity) RETURN e LIMIT 10",
      # Get financial facts
      """
            MATCH (f:Fact)-[:HAS_ELEMENT]->(el:Element)
            RETURN f, el
            ORDER BY f.period DESC
            LIMIT 100
            """,
      # Get relationships
      """
            MATCH (e:Entity)-[r]->(n)
            RETURN type(r) as relationship, count(*) as count
            """,
    ]

    # Add specific queries based on user query
    queries.extend(self._generate_financial_queries(user_query, None))

    return queries

  def _format_financial_response(
    self, query: str, results: List[Any], mode: str
  ) -> str:
    """Format financial results into a response."""
    if not results:
      return "No financial data found for your query."

    response = f"Financial Analysis ({mode} mode):\n\n"

    for i, result in enumerate(results):
      response += f"Result {i + 1}:\n"
      if isinstance(result, list) and result:
        response += json.dumps(result[0], indent=2)[:500]
      elif isinstance(result, dict):
        response += json.dumps(result, indent=2)[:500]
      response += "\n\n"

    return response

  async def _ai_financial_analysis(
    self, query: str, data: List[Any], history: Optional[List[Dict]]
  ) -> str:
    """Use AI to analyze financial data."""
    if not self.anthropic:
      return self._format_financial_response(query, data, "standard")

    try:
      # Prepare messages
      messages = []
      if history:
        messages.extend(history)

      messages.append(
        {
          "role": "user",
          "content": f"Analyze this financial data for the query: {query}\n\nData: {json.dumps(data[:5], indent=2)[:2000]}",
        }
      )

      # Call AI
      response = self.anthropic.messages.create(
        model=self.model,
        max_tokens=2000,
        messages=messages,
        system="You are a financial analyst. Provide clear, concise financial analysis.",
      )

      # Track tokens and consume credits
      credit_result = None
      if hasattr(response, "usage"):
        self.track_tokens(response.usage.input_tokens, response.usage.output_tokens)
        credit_result = await self.consume_credits(
          response.usage.input_tokens,
          response.usage.output_tokens,
          self.model,
          "Financial analysis",
        )

      content = response.content[0].text if response.content else "Analysis failed"

      # Add credit consumption info to response if available
      if credit_result and credit_result.get("success"):
        self._last_credit_consumption = {
          "credits_consumed": credit_result.get("credits_consumed", 0),
          "remaining_balance": credit_result.get("remaining_balance", 0),
        }

      return content

    except Exception as e:
      logger.error(f"AI analysis failed: {str(e)}")
      return self._format_financial_response(query, data, "standard")

  async def _ai_deep_financial_analysis(
    self, query: str, data: List[Any], history: Optional[List[Dict]]
  ) -> str:
    """Perform deep AI analysis of financial data."""
    if not self.anthropic:
      return self._format_financial_response(query, data, "extended")

    try:
      # Prepare comprehensive messages for deep analysis
      messages = []
      if history:
        messages.extend(history)

      # More detailed prompt for extended analysis
      messages.append(
        {
          "role": "user",
          "content": f"""Perform a comprehensive financial analysis for: {query}

          Available data (sample): {json.dumps(data[:10], indent=2)[:5000]}

          Provide:
          1. Key financial metrics and trends
          2. Comparative analysis if applicable
          3. Risk factors and opportunities
          4. Strategic recommendations
          5. Data quality assessment""",
        }
      )

      # Call AI with larger token limit for extended analysis
      response = self.anthropic.messages.create(
        model=self.model,
        max_tokens=4000,
        messages=messages,
        system="""You are an expert financial analyst specializing in SEC filings
        and corporate financial analysis. Provide detailed, actionable insights
        based on the data provided. Focus on material findings and strategic implications.""",
      )

      # Track tokens and consume credits
      credit_result = None
      if hasattr(response, "usage"):
        self.track_tokens(response.usage.input_tokens, response.usage.output_tokens)
        credit_result = await self.consume_credits(
          response.usage.input_tokens,
          response.usage.output_tokens,
          self.model,
          "Extended financial analysis",
        )

      content = response.content[0].text if response.content else "Analysis failed"

      # Add credit consumption info to response if available
      if credit_result and credit_result.get("success"):
        self._last_credit_consumption = {
          "credits_consumed": credit_result.get("credits_consumed", 0),
          "remaining_balance": credit_result.get("remaining_balance", 0),
        }

      return content

    except Exception as e:
      logger.error(f"Deep AI analysis failed: {str(e)}")
      return self._format_financial_response(query, data, "extended")

  def _calculate_confidence(self, query: str, response: str) -> float:
    """Calculate confidence in the response."""
    # Simple heuristic
    if "No data" in response or "Unable to" in response:
      return 0.3
    elif len(response) > 500:
      return 0.8
    else:
      return 0.6
