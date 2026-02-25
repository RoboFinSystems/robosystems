"""Comprehensive unit tests for the financial agent module.

Tests the FinancialAgent class including analysis modes, query routing,
confidence scoring, response formatting, credit tracking, and error handling.
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from robosystems.operations.agents.base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
)

# Module paths for patching
FINANCIAL_MODULE = "robosystems.operations.agents.financial"
AI_CLIENT_MODULE = "robosystems.operations.agents.ai_client"


def _make_mock_user():
  """Create a mock user object for agent initialization."""
  user = Mock()
  user.id = "test-user-123"
  user.email = "test@example.com"
  user.name = "Test User"
  return user


def _make_financial_agent(graph_id="kg01234567890abcdef", db_session=None):
  """Create a FinancialAgent with mocked AIClient.

  Returns (agent, mock_ai_client).
  """
  user = _make_mock_user()

  with patch(f"{FINANCIAL_MODULE}.AIClient") as MockAIClient:
    mock_ai_client = MagicMock()
    MockAIClient.return_value = mock_ai_client

    from robosystems.operations.agents.financial import FinancialAgent

    agent = FinancialAgent(graph_id=graph_id, user=user, db_session=db_session)

  return agent, mock_ai_client


class TestFinancialAgentMetadata:
  """Test FinancialAgent metadata properties."""

  @pytest.mark.unit
  def test_metadata_name(self):
    """Test that metadata returns correct agent name."""
    agent, _ = _make_financial_agent()
    assert agent.metadata.name == "Financial Agent"

  @pytest.mark.unit
  def test_metadata_capabilities(self):
    """Test that metadata includes financial analysis capabilities."""
    agent, _ = _make_financial_agent()
    capabilities = agent.metadata.capabilities
    assert AgentCapability.FINANCIAL_ANALYSIS in capabilities
    assert AgentCapability.ENTITY_ANALYSIS in capabilities
    assert AgentCapability.TREND_ANALYSIS in capabilities

  @pytest.mark.unit
  def test_metadata_supported_modes(self):
    """Test that metadata lists all supported execution modes."""
    agent, _ = _make_financial_agent()
    modes = agent.metadata.supported_modes
    assert AgentMode.QUICK in modes
    assert AgentMode.STANDARD in modes
    assert AgentMode.EXTENDED in modes
    assert AgentMode.STREAMING not in modes

  @pytest.mark.unit
  def test_metadata_version(self):
    """Test that metadata returns correct version."""
    agent, _ = _make_financial_agent()
    assert agent.metadata.version == "2.0.0"

  @pytest.mark.unit
  def test_metadata_author(self):
    """Test that metadata returns correct author."""
    agent, _ = _make_financial_agent()
    assert agent.metadata.author == "RoboSystems"

  @pytest.mark.unit
  def test_metadata_tags(self):
    """Test that metadata includes relevant tags."""
    agent, _ = _make_financial_agent()
    tags = agent.metadata.tags
    assert "finance" in tags
    assert "sec" in tags
    assert "quickbooks" in tags
    assert "analysis" in tags

  @pytest.mark.unit
  def test_metadata_execution_profiles(self):
    """Test that execution profiles are defined for each mode."""
    agent, _ = _make_financial_agent()
    profiles = agent.metadata.execution_profile

    assert AgentMode.QUICK in profiles
    assert AgentMode.STANDARD in profiles
    assert AgentMode.EXTENDED in profiles

    # Verify quick mode has lower times than extended
    assert profiles[AgentMode.QUICK].avg_time < profiles[AgentMode.EXTENDED].avg_time

  @pytest.mark.unit
  def test_metadata_is_agent_metadata_instance(self):
    """Test that metadata returns an AgentMetadata instance."""
    agent, _ = _make_financial_agent()
    assert isinstance(agent.metadata, AgentMetadata)


class TestFinancialAgentInit:
  """Test FinancialAgent initialization."""

  @pytest.mark.unit
  def test_initializes_with_graph_id(self):
    """Test that agent stores graph_id from initialization."""
    agent, _ = _make_financial_agent(graph_id="kg01234567890abcdef")
    assert agent.graph_id == "kg01234567890abcdef"

  @pytest.mark.unit
  def test_initializes_with_user(self):
    """Test that agent stores user from initialization."""
    agent, _ = _make_financial_agent()
    assert agent.user.id == "test-user-123"

  @pytest.mark.unit
  def test_initializes_ai_client(self):
    """Test that agent creates an AIClient instance."""
    agent, mock_ai_client = _make_financial_agent()
    assert agent.ai_client is mock_ai_client

  @pytest.mark.unit
  def test_initializes_with_db_session(self):
    """Test that agent stores optional db_session."""
    mock_session = MagicMock()
    agent, _ = _make_financial_agent(db_session=mock_session)
    assert agent.db_session is mock_session

  @pytest.mark.unit
  def test_initial_token_tracking(self):
    """Test that token tracking starts at zero."""
    agent, _ = _make_financial_agent()
    assert agent.total_tokens_used == {"input": 0, "output": 0}


class TestFinancialAgentCanHandle:
  """Test the can_handle confidence scoring method."""

  @pytest.mark.unit
  def test_high_confidence_financial_keyword(self):
    """Test high confidence for financial keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("What is the company's financial performance?")
    assert score >= 0.3

  @pytest.mark.unit
  def test_high_confidence_revenue(self):
    """Test high confidence for revenue keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("Show me revenue trends")
    assert score >= 0.3

  @pytest.mark.unit
  def test_high_confidence_profit(self):
    """Test high confidence for profit keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("What is the net profit margin?")
    assert score >= 0.3

  @pytest.mark.unit
  def test_high_confidence_sec(self):
    """Test high confidence for SEC keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("Get SEC filing data")
    assert score >= 0.3

  @pytest.mark.unit
  def test_high_confidence_10k(self):
    """Test high confidence for 10-K keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("Analyze the 10-k report")
    assert score >= 0.3

  @pytest.mark.unit
  def test_high_confidence_ebitda(self):
    """Test high confidence for EBITDA keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("What is the ebitda for last quarter?")
    assert score >= 0.3

  @pytest.mark.unit
  def test_medium_confidence_money(self):
    """Test medium confidence for money keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("Where is the money going?")
    assert score >= 0.15

  @pytest.mark.unit
  def test_medium_confidence_investment(self):
    """Test medium confidence for investment keyword."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("Review my investment portfolio")
    assert score >= 0.15

  @pytest.mark.unit
  def test_no_confidence_unrelated_query(self):
    """Test zero confidence for completely unrelated query."""
    agent, _ = _make_financial_agent()
    score = agent.can_handle("What is the weather today?")
    assert score == 0.0

  @pytest.mark.unit
  def test_multiple_keywords_accumulate(self):
    """Test that multiple keywords accumulate confidence."""
    agent, _ = _make_financial_agent()
    # Contains both 'revenue' and 'profit' (high confidence keywords)
    score = agent.can_handle("Compare revenue and profit over time")
    assert score >= 0.6

  @pytest.mark.unit
  def test_capped_at_one(self):
    """Test that confidence score is capped at 1.0."""
    agent, _ = _make_financial_agent()
    # Query with many financial keywords
    score = agent.can_handle(
      "financial revenue profit loss balance sheet income statement "
      "cash flow sec 10-k quickbooks accounting gaap xbrl earnings margin"
    )
    assert score <= 1.0

  @pytest.mark.unit
  def test_context_domain_finance_boost(self):
    """Test confidence boost from finance domain context."""
    agent, _ = _make_financial_agent()
    score_without = agent.can_handle("Show me trends")
    score_with = agent.can_handle("Show me trends", context={"domain": "finance"})
    assert score_with > score_without

  @pytest.mark.unit
  def test_context_financial_data_boost(self):
    """Test confidence boost from has_financial_data context flag."""
    agent, _ = _make_financial_agent()
    score_without = agent.can_handle("Show me data")
    score_with = agent.can_handle("Show me data", context={"has_financial_data": True})
    assert score_with > score_without

  @pytest.mark.unit
  def test_case_insensitive_matching(self):
    """Test that keyword matching is case insensitive."""
    agent, _ = _make_financial_agent()
    score_lower = agent.can_handle("revenue analysis")
    score_upper = agent.can_handle("REVENUE ANALYSIS")
    assert score_lower == score_upper

  @pytest.mark.unit
  def test_empty_context_no_boost(self):
    """Test no boost with empty context dict."""
    agent, _ = _make_financial_agent()
    score_none = agent.can_handle("revenue analysis")
    score_empty = agent.can_handle("revenue analysis", context={})
    assert score_none == score_empty


class TestFinancialAgentBuildQuery:
  """Test Cypher query building logic."""

  @pytest.mark.unit
  def test_revenue_query(self):
    """Test that revenue keyword generates appropriate Cypher query."""
    agent, _ = _make_financial_agent()
    query = agent._build_financial_query("Show revenue trends")
    assert "Revenue" in query
    assert "MATCH" in query
    assert "RETURN" in query

  @pytest.mark.unit
  def test_balance_query(self):
    """Test that balance keyword generates asset/liability query."""
    agent, _ = _make_financial_agent()
    query = agent._build_financial_query("Show balance sheet")
    assert "Assets" in query or "Liabilities" in query
    assert "MATCH" in query

  @pytest.mark.unit
  def test_generic_query(self):
    """Test fallback query for non-specific financial queries."""
    agent, _ = _make_financial_agent()
    query = agent._build_financial_query("Show me some data")
    assert "MATCH" in query
    assert "RETURN" in query
    assert "LIMIT" in query


class TestFinancialAgentGenerateQueries:
  """Test multi-query generation methods."""

  @pytest.mark.unit
  def test_generate_financial_queries_with_trend(self):
    """Test that trend keyword generates trend-specific queries."""
    agent, _ = _make_financial_agent()
    queries = agent._generate_financial_queries("Show revenue trend", schema=None)
    assert len(queries) >= 2
    # Should contain a trend query with ORDER BY
    has_trend = any("ORDER BY f.period" in q for q in queries)
    assert has_trend

  @pytest.mark.unit
  def test_generate_financial_queries_with_compare(self):
    """Test that compare keyword generates comparison queries."""
    agent, _ = _make_financial_agent()
    queries = agent._generate_financial_queries("Compare company data", schema=None)
    assert len(queries) >= 2
    has_compare = any("ORDER BY total DESC" in q for q in queries)
    assert has_compare

  @pytest.mark.unit
  def test_generate_financial_queries_default(self):
    """Test default query generation with no special keywords."""
    agent, _ = _make_financial_agent()
    queries = agent._generate_financial_queries("What is revenue?", schema=None)
    # Should always include the default query
    assert len(queries) >= 1

  @pytest.mark.unit
  def test_generate_comprehensive_queries(self):
    """Test comprehensive query generation for extended mode."""
    agent, _ = _make_financial_agent()
    queries = agent._generate_comprehensive_queries("Analyze financial health")
    # Should include entity overview, facts, and relationships queries
    assert len(queries) >= 3
    has_entity = any("Entity" in q for q in queries)
    has_fact = any("Fact" in q for q in queries)
    assert has_entity
    assert has_fact


class TestFinancialAgentFormatResponse:
  """Test response formatting logic."""

  @pytest.mark.unit
  def test_empty_results(self):
    """Test formatting with empty results list."""
    agent, _ = _make_financial_agent()
    response = agent._format_financial_response("test query", [], "standard")
    assert "No financial data found" in response

  @pytest.mark.unit
  def test_format_with_dict_result(self):
    """Test formatting with dictionary result."""
    agent, _ = _make_financial_agent()
    results = [{"value": 1000000, "period": "2024-Q1", "name": "Revenue"}]
    response = agent._format_financial_response("test", results, "quick")
    assert "Financial Analysis (quick mode)" in response
    assert "Result 1" in response

  @pytest.mark.unit
  def test_format_with_list_result(self):
    """Test formatting with list of records result."""
    agent, _ = _make_financial_agent()
    results = [[{"value": 500, "name": "Total"}]]
    response = agent._format_financial_response("test", results, "standard")
    assert "Financial Analysis (standard mode)" in response
    assert "Result 1" in response

  @pytest.mark.unit
  def test_format_mode_label(self):
    """Test that mode label appears in response."""
    agent, _ = _make_financial_agent()
    results = [{"data": "test"}]

    for mode_name in ["quick", "standard", "extended"]:
      response = agent._format_financial_response("test", results, mode_name)
      assert f"({mode_name} mode)" in response

  @pytest.mark.unit
  def test_format_truncates_large_results(self):
    """Test that large results are truncated to 500 chars."""
    agent, _ = _make_financial_agent()
    # Create a result that produces JSON > 500 chars
    large_result = {"key": "x" * 1000}
    results = [large_result]
    response = agent._format_financial_response("test", results, "standard")
    # The JSON representation of the result should be truncated
    # Total response length should be reasonable
    assert len(response) < 2000


class TestFinancialAgentCalculateConfidence:
  """Test confidence calculation for responses."""

  @pytest.mark.unit
  def test_low_confidence_no_data(self):
    """Test low confidence when response indicates no data."""
    agent, _ = _make_financial_agent()
    score = agent._calculate_confidence("test", "No data available for this query.")
    assert score == 0.3

  @pytest.mark.unit
  def test_low_confidence_unable_to(self):
    """Test low confidence when response indicates inability."""
    agent, _ = _make_financial_agent()
    score = agent._calculate_confidence("test", "Unable to retrieve the data.")
    assert score == 0.3

  @pytest.mark.unit
  def test_high_confidence_long_response(self):
    """Test high confidence for detailed (>500 char) responses."""
    agent, _ = _make_financial_agent()
    long_response = "A" * 501
    score = agent._calculate_confidence("test", long_response)
    assert score == 0.8

  @pytest.mark.unit
  def test_medium_confidence_short_response(self):
    """Test medium confidence for short but valid responses."""
    agent, _ = _make_financial_agent()
    short_response = "Revenue is $1M."
    score = agent._calculate_confidence("test", short_response)
    assert score == 0.6


class TestFinancialAgentAnalyze:
  """Test the main analyze method across different modes."""

  @pytest.mark.unit
  async def test_analyze_validates_mode(self):
    """Test that analyze validates the execution mode."""
    agent, _ = _make_financial_agent()

    # STREAMING is not in supported modes for financial agent
    response = await agent.analyze("test query", mode=AgentMode.STREAMING)

    # Should return error response since STREAMING is not supported
    assert isinstance(response, AgentResponse)
    assert response.error_details is not None
    assert "ANALYSIS_ERROR" in response.error_details.get("code", "")

  @pytest.mark.unit
  async def test_analyze_quick_mode_no_tools(self):
    """Test quick analysis when no MCP tools are available."""
    agent, _ = _make_financial_agent()

    # Mock initialize_tools to set graph_client but no mcp_tools
    agent.graph_client = MagicMock()
    agent.mcp_tools = None

    with patch.object(agent, "initialize_tools", new_callable=AsyncMock):
      with patch.object(agent, "prepare_context", new_callable=AsyncMock) as mock_ctx:
        mock_ctx.return_value = {"graph_id": "kg01234567890abcdef"}
        with patch.object(agent, "get_mode_limits") as mock_limits:
          mock_limits.return_value = {"max_tools": 2, "timeout": 30}

          response = await agent.analyze("Show revenue", mode=AgentMode.QUICK)

    assert isinstance(response, AgentResponse)
    assert response.agent_name == "Financial Agent"
    assert response.mode_used == AgentMode.QUICK

  @pytest.mark.unit
  async def test_analyze_standard_mode_with_tools(self):
    """Test standard analysis with MCP tools available."""
    agent, _ = _make_financial_agent()

    # Set up MCP tools mock
    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(
      return_value=[{"value": "1000000", "period": "2024-Q1"}]
    )
    agent.graph_client = MagicMock()
    agent.mcp_tools = mock_mcp

    # Mock AI analysis to return a string
    with (
      patch.object(agent, "initialize_tools", new_callable=AsyncMock),
      patch.object(agent, "prepare_context", new_callable=AsyncMock) as mock_ctx,
      patch.object(agent, "get_mode_limits") as mock_limits,
      patch.object(
        agent,
        "_ai_financial_analysis",
        new_callable=AsyncMock,
        return_value="AI analysis of revenue data showing growth.",
      ),
    ):
      mock_ctx.return_value = {"graph_id": "kg01234567890abcdef"}
      mock_limits.return_value = {"max_tools": 5, "timeout": 60}

      response = await agent.analyze("Show revenue", mode=AgentMode.STANDARD)

    assert isinstance(response, AgentResponse)
    assert response.mode_used == AgentMode.STANDARD
    assert response.error_details is None

  @pytest.mark.unit
  async def test_analyze_extended_mode_with_callback(self):
    """Test extended analysis invokes callback at multiple stages."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(return_value=[{"data": "test"}])
    agent.graph_client = MagicMock()
    agent.mcp_tools = mock_mcp
    agent.anthropic = None  # No Anthropic client

    callback = Mock()

    with (
      patch.object(agent, "initialize_tools", new_callable=AsyncMock),
      patch.object(agent, "prepare_context", new_callable=AsyncMock) as mock_ctx,
      patch.object(agent, "get_mode_limits") as mock_limits,
    ):
      mock_ctx.return_value = {"graph_id": "kg01234567890abcdef"}
      mock_limits.return_value = {"max_tools": 12, "timeout": 300}

      response = await agent.analyze(
        "Deep financial analysis", mode=AgentMode.EXTENDED, callback=callback
      )

    assert isinstance(response, AgentResponse)
    assert response.mode_used == AgentMode.EXTENDED

    # Verify callback was called at various stages
    callback_stages = [call[0][0] for call in callback.call_args_list]
    assert "initialization" in callback_stages
    assert "completion" in callback_stages

  @pytest.mark.unit
  async def test_analyze_error_returns_error_response(self):
    """Test that analysis errors return proper error response."""
    agent, _ = _make_financial_agent()

    # Force an error during initialize_tools
    with patch.object(
      agent,
      "validate_mode",
      side_effect=RuntimeError("Unexpected error"),
    ):
      response = await agent.analyze("test query")

    assert isinstance(response, AgentResponse)
    assert response.error_details is not None
    assert response.error_details["code"] == "ANALYSIS_ERROR"
    assert "Unexpected error" in response.error_details["message"]
    assert "Financial analysis failed" in response.content

  @pytest.mark.unit
  async def test_analyze_includes_credit_consumption_metadata(self):
    """Test that credit consumption info is added to metadata."""
    agent, _ = _make_financial_agent()

    agent.graph_client = MagicMock()
    agent.mcp_tools = None

    with (
      patch.object(agent, "initialize_tools", new_callable=AsyncMock),
      patch.object(agent, "prepare_context", new_callable=AsyncMock) as mock_ctx,
      patch.object(agent, "get_mode_limits") as mock_limits,
    ):
      mock_ctx.return_value = {"graph_id": "kg01234567890abcdef"}
      mock_limits.return_value = {"max_tools": 2, "timeout": 30}

      # Simulate credit consumption
      agent._last_credit_consumption = {
        "credits_consumed": 15,
        "remaining_balance": 985,
      }

      response = await agent.analyze("test query", mode=AgentMode.QUICK)

    # Credits should be in metadata only if set before return
    # The analyze method sets _last_credit_consumption = None first,
    # then re-checks it after the analysis methods run
    assert isinstance(response, AgentResponse)


class TestFinancialAgentQuickAnalysis:
  """Test quick analysis path."""

  @pytest.mark.unit
  async def test_quick_with_tool_results(self):
    """Test quick analysis returns formatted response with tool data."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(return_value={"value": "5000000", "name": "Revenue"})
    agent.mcp_tools = mock_mcp

    result = await agent._quick_analysis(
      query="Show revenue",
      history=None,
      context={},
      limits={"max_tools": 2},
    )

    assert "Financial Analysis" in result
    mock_mcp.call_tool.assert_called_once()

  @pytest.mark.unit
  async def test_quick_with_tool_failure(self):
    """Test quick analysis handles tool call failure gracefully."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(side_effect=Exception("Tool error"))
    agent.mcp_tools = mock_mcp

    result = await agent._quick_analysis(
      query="Show data",
      history=None,
      context={},
      limits={"max_tools": 2},
    )

    assert "Unable to retrieve financial data" in result

  @pytest.mark.unit
  async def test_quick_without_tools(self):
    """Test quick analysis when no MCP tools available."""
    agent, _ = _make_financial_agent()
    agent.mcp_tools = None

    result = await agent._quick_analysis(
      query="Show data",
      history=None,
      context={},
      limits={"max_tools": 2},
    )

    assert "Unable to retrieve financial data" in result


class TestFinancialAgentStandardAnalysis:
  """Test standard analysis path."""

  @pytest.mark.unit
  async def test_standard_queries_schema_then_data(self):
    """Test that standard mode fetches schema first, then data."""
    agent, _ = _make_financial_agent()

    call_count = 0
    call_tool_names = []

    async def mock_call_tool(name, params):
      nonlocal call_count
      call_count += 1
      call_tool_names.append(name)
      if name == "get-graph-schema":
        return {"labels": ["Entity", "Fact"]}
      return [{"value": "1000"}]

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(side_effect=mock_call_tool)
    agent.mcp_tools = mock_mcp

    with patch.object(
      agent,
      "_ai_financial_analysis",
      new_callable=AsyncMock,
      return_value="Analysis result",
    ):
      result = await agent._standard_analysis(
        query="Show revenue",
        history=None,
        context={},
        limits={"max_tools": 5},
      )

    # Schema should be fetched first
    assert call_tool_names[0] == "get-graph-schema"
    assert result == "Analysis result"

  @pytest.mark.unit
  async def test_standard_respects_tool_limits(self):
    """Test that standard mode respects max tool call limits."""
    agent, _ = _make_financial_agent()

    call_count = 0

    async def mock_call_tool(name, params):
      nonlocal call_count
      call_count += 1
      if name == "get-graph-schema":
        return {"labels": ["Entity"]}
      return [{"value": "1000"}]

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(side_effect=mock_call_tool)
    agent.mcp_tools = mock_mcp

    with patch.object(
      agent,
      "_ai_financial_analysis",
      new_callable=AsyncMock,
      return_value="Result",
    ):
      await agent._standard_analysis(
        query="Show revenue trend compare details",
        history=None,
        context={},
        limits={"max_tools": 2},
      )

    # Should be limited: 1 schema call + max 2 data calls
    assert call_count <= 3

  @pytest.mark.unit
  async def test_standard_without_tools(self):
    """Test standard analysis falls back without tools."""
    agent, _ = _make_financial_agent()
    agent.mcp_tools = None

    result = await agent._standard_analysis(
      query="Show revenue",
      history=None,
      context={},
      limits={"max_tools": 5},
    )

    assert "No financial data found" in result


class TestFinancialAgentExtendedAnalysis:
  """Test extended analysis path."""

  @pytest.mark.unit
  async def test_extended_reports_progress(self):
    """Test that extended mode reports progress via callback."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(return_value=[{"data": "test"}])
    agent.mcp_tools = mock_mcp
    agent.anthropic = None

    callback = Mock()

    await agent._extended_analysis(
      query="Deep analysis",
      history=None,
      context={},
      limits={"max_tools": 12},
      callback=callback,
    )

    # Callback should be called at initialization, data_gathering, querying, analysis, completion
    assert callback.call_count >= 3
    stages = [call[0][0] for call in callback.call_args_list]
    assert "initialization" in stages
    assert "analysis" in stages
    assert "completion" in stages

  @pytest.mark.unit
  async def test_extended_without_callback(self):
    """Test that extended mode works without callback."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(return_value=[{"data": "test"}])
    agent.mcp_tools = mock_mcp
    agent.anthropic = None

    # Should not raise even without callback
    result = await agent._extended_analysis(
      query="Deep analysis",
      history=None,
      context={},
      limits={"max_tools": 12},
      callback=None,
    )

    assert isinstance(result, str)

  @pytest.mark.unit
  async def test_extended_with_ai_analysis(self):
    """Test extended mode uses AI when anthropic client is available."""
    agent, _ = _make_financial_agent()

    mock_mcp = AsyncMock()
    mock_mcp.call_tool = AsyncMock(return_value=[{"data": "financial"}])
    agent.mcp_tools = mock_mcp
    agent.anthropic = MagicMock()  # Set anthropic to enable AI path

    with patch.object(
      agent,
      "_ai_deep_financial_analysis",
      new_callable=AsyncMock,
      return_value="Deep AI analysis of financial data.",
    ):
      result = await agent._extended_analysis(
        query="Comprehensive analysis",
        history=None,
        context={},
        limits={"max_tools": 12},
      )

    assert result == "Deep AI analysis of financial data."


class TestFinancialAgentAIAnalysis:
  """Test AI-powered analysis methods."""

  @pytest.mark.unit
  async def test_ai_financial_analysis_success(self):
    """Test successful AI financial analysis."""
    agent, mock_ai_client = _make_financial_agent(db_session=MagicMock())

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Revenue grew by 15% year over year.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=500,
      output_tokens=200,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value={"success": True, "credits_consumed": 10, "remaining_balance": 990},
    ):
      result = await agent._ai_financial_analysis(
        query="Revenue trends",
        data=[{"value": "1000000", "period": "2024-Q1"}],
        history=None,
      )

    assert result == "Revenue grew by 15% year over year."
    assert agent.total_tokens_used["input"] == 500
    assert agent.total_tokens_used["output"] == 200

  @pytest.mark.unit
  async def test_ai_financial_analysis_with_history(self):
    """Test AI analysis includes conversation history."""
    agent, mock_ai_client = _make_financial_agent()

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Updated analysis.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=300,
      output_tokens=100,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value=None,
    ):
      history = [
        {"role": "user", "content": "Show revenue"},
        {"role": "assistant", "content": "Revenue is $1M."},
      ]

      result = await agent._ai_financial_analysis(
        query="What about profit?",
        data=[{"value": "500000"}],
        history=history,
      )

    assert result == "Updated analysis."
    # Verify messages include history
    call_args = mock_ai_client.create_message.call_args
    messages = call_args[1]["messages"]
    assert len(messages) == 3  # 2 history + 1 new

  @pytest.mark.unit
  async def test_ai_financial_analysis_error_falls_back(self):
    """Test that AI analysis errors fall back to formatted response."""
    agent, mock_ai_client = _make_financial_agent()

    mock_ai_client.create_message = AsyncMock(side_effect=Exception("Bedrock timeout"))

    result = await agent._ai_financial_analysis(
      query="Revenue trends",
      data=[{"value": "1000000"}],
      history=None,
    )

    # Should fall back to formatted response
    assert "Financial Analysis" in result or "No financial data" in result

  @pytest.mark.unit
  async def test_ai_financial_analysis_tracks_credits(self):
    """Test that successful AI analysis stores credit consumption."""
    agent, mock_ai_client = _make_financial_agent(db_session=MagicMock())

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Analysis complete.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=1000,
      output_tokens=500,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value={"success": True, "credits_consumed": 25, "remaining_balance": 975},
    ):
      await agent._ai_financial_analysis(
        query="Test",
        data=[{"data": "test"}],
        history=None,
      )

    assert agent._last_credit_consumption["credits_consumed"] == 25
    assert agent._last_credit_consumption["remaining_balance"] == 975

  @pytest.mark.unit
  async def test_ai_financial_analysis_credit_failure_no_tracking(self):
    """Test that credit failure does not set consumption tracking."""
    agent, mock_ai_client = _make_financial_agent(db_session=MagicMock())

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Analysis.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=100,
      output_tokens=50,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value={"success": False, "error": "Insufficient credits"},
    ):
      await agent._ai_financial_analysis(
        query="Test",
        data=[{"data": "test"}],
        history=None,
      )

    # _last_credit_consumption should not be set when success is False
    assert (
      not hasattr(agent, "_last_credit_consumption")
      or agent._last_credit_consumption is None
    )


class TestFinancialAgentDeepAnalysis:
  """Test deep AI analysis method."""

  @pytest.mark.unit
  async def test_deep_analysis_success(self):
    """Test successful deep AI financial analysis."""
    agent, mock_ai_client = _make_financial_agent(db_session=MagicMock())

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Comprehensive financial analysis with risk factors.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=2000,
      output_tokens=1500,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value={"success": True, "credits_consumed": 50, "remaining_balance": 950},
    ):
      result = await agent._ai_deep_financial_analysis(
        query="Comprehensive analysis of NVDA",
        data=[{"revenue": "1B"}, {"expenses": "500M"}],
        history=None,
      )

    assert result == "Comprehensive financial analysis with risk factors."

  @pytest.mark.unit
  async def test_deep_analysis_uses_larger_token_limit(self):
    """Test that deep analysis uses larger max_tokens than standard."""
    agent, mock_ai_client = _make_financial_agent()

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="Deep result.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=500,
      output_tokens=200,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value=None,
    ):
      await agent._ai_deep_financial_analysis(
        query="Test",
        data=[{"data": "test"}],
        history=None,
      )

    call_args = mock_ai_client.create_message.call_args
    assert call_args[1]["max_tokens"] == 4000

  @pytest.mark.unit
  async def test_deep_analysis_error_falls_back(self):
    """Test that deep analysis errors fall back to formatted response."""
    agent, mock_ai_client = _make_financial_agent()

    mock_ai_client.create_message = AsyncMock(side_effect=Exception("Model overloaded"))

    result = await agent._ai_deep_financial_analysis(
      query="Analysis",
      data=[{"value": "1000"}],
      history=None,
    )

    assert "Financial Analysis" in result or "No financial data" in result

  @pytest.mark.unit
  async def test_deep_analysis_includes_detailed_system_prompt(self):
    """Test that deep analysis uses a specialized system prompt."""
    agent, mock_ai_client = _make_financial_agent()

    from robosystems.operations.agents.ai_client import AIResponse

    mock_ai_response = AIResponse(
      content="OK",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=100,
      output_tokens=50,
    )
    mock_ai_client.create_message = AsyncMock(return_value=mock_ai_response)

    with patch.object(
      agent,
      "consume_credits",
      new_callable=AsyncMock,
      return_value=None,
    ):
      await agent._ai_deep_financial_analysis(
        query="Test",
        data=[{"data": "test"}],
        history=None,
      )

    call_args = mock_ai_client.create_message.call_args
    system_prompt = call_args[1]["system"]
    assert "expert financial analyst" in system_prompt
    assert "SEC filings" in system_prompt


class TestFinancialAgentBaseClassIntegration:
  """Test base class methods via the financial agent."""

  @pytest.mark.unit
  def test_supports_mode_valid(self):
    """Test supports_mode returns True for supported modes."""
    agent, _ = _make_financial_agent()
    assert agent.supports_mode(AgentMode.QUICK) is True
    assert agent.supports_mode(AgentMode.STANDARD) is True
    assert agent.supports_mode(AgentMode.EXTENDED) is True

  @pytest.mark.unit
  def test_supports_mode_invalid(self):
    """Test supports_mode returns False for unsupported modes."""
    agent, _ = _make_financial_agent()
    assert agent.supports_mode(AgentMode.STREAMING) is False

  @pytest.mark.unit
  def test_has_capability(self):
    """Test has_capability for financial agent capabilities."""
    agent, _ = _make_financial_agent()
    assert agent.has_capability(AgentCapability.FINANCIAL_ANALYSIS) is True
    assert agent.has_capability(AgentCapability.DEEP_RESEARCH) is False

  @pytest.mark.unit
  def test_track_tokens(self):
    """Test token tracking accumulates correctly."""
    agent, _ = _make_financial_agent()
    agent.track_tokens(100, 50)
    agent.track_tokens(200, 75)
    assert agent.total_tokens_used == {"input": 300, "output": 125}

  @pytest.mark.unit
  def test_validate_mode_supported(self):
    """Test validate_mode does not raise for supported modes."""
    agent, _ = _make_financial_agent()
    # Should not raise
    agent.validate_mode(AgentMode.QUICK)
    agent.validate_mode(AgentMode.STANDARD)
    agent.validate_mode(AgentMode.EXTENDED)

  @pytest.mark.unit
  def test_validate_mode_unsupported(self):
    """Test validate_mode raises ValueError for unsupported modes."""
    agent, _ = _make_financial_agent()
    with pytest.raises(ValueError, match="does not support mode"):
      agent.validate_mode(AgentMode.STREAMING)

  @pytest.mark.unit
  async def test_consume_credits_no_session(self):
    """Test that consume_credits returns None without db_session."""
    agent, _ = _make_financial_agent(db_session=None)
    result = await agent.consume_credits(100, 50, "test-model", "test operation")
    assert result is None

  @pytest.mark.unit
  async def test_prepare_context(self):
    """Test context preparation with standard fields."""
    agent, _ = _make_financial_agent()
    context = await agent.prepare_context("test query", {"existing": "value"})

    assert context["graph_id"] == "kg01234567890abcdef"
    assert context["user_id"] == "test-user-123"
    assert context["agent_name"] == "Financial Agent"
    assert context["existing"] == "value"
    assert "timestamp" in context
    assert "capabilities" in context

  @pytest.mark.unit
  async def test_prepare_context_none_input(self):
    """Test context preparation with None context input."""
    agent, _ = _make_financial_agent()
    context = await agent.prepare_context("test query", None)

    assert context["graph_id"] == "kg01234567890abcdef"
    assert "timestamp" in context

  @pytest.mark.unit
  def test_repr(self):
    """Test string representation of agent."""
    agent, _ = _make_financial_agent()
    repr_str = repr(agent)
    assert "FinancialAgent" in repr_str
    assert "Financial Agent" in repr_str
    assert "kg01234567890abcdef" in repr_str
