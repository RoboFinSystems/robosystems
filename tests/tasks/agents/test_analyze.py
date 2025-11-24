"""
Test Suite for Agent Analysis Celery Task

Tests the agent analysis task with SSE progress tracking.
"""

import pytest
from unittest.mock import MagicMock, patch, Mock, AsyncMock


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.agents.analyze.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestAnalyzeAgentTask:
  """Test cases for analyze_agent_task Celery task."""

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_successful_agent_analysis(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test successful agent analysis execution."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_user.id = "user-123"
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Test Agent"

    # Setup response
    mock_response = MagicMock()
    mock_response.content = "Analysis complete"
    mock_response.agent_name = "test-agent"
    mock_response.mode_used = MagicMock()
    mock_response.mode_used.value = "standard"
    mock_response.metadata = {"key": "value"}
    mock_response.tokens_used = 150
    mock_response.confidence_score = 0.95
    mock_response.error_details = None
    mock_response.tools_called = ["search", "analyze"]

    mock_agent.analyze = AsyncMock(return_value=mock_response)

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task
    result = analyze_agent_task_impl(
      agent_type="financial_analyst",
      graph_id="kg123456",
      request_data={
        "message": "Analyze revenue trends",
        "mode": "standard",
        "history": [],
        "context": {"period": "Q1 2024"},
      },
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify result
    assert result["content"] == "Analysis complete"
    assert result["agent_used"] == "test-agent"
    assert result["mode_used"] == "standard"
    assert result["tokens_used"] == 150
    assert result["confidence_score"] == 0.95
    assert "execution_time" in result

    # Verify SSE events emitted
    assert mock_emit.call_count >= 3  # Started, initialized, completed
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.database.get_db_session")
  def test_user_not_found(self, mock_get_db_session, mock_emit):
    """Test handling of user not found error."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session with no user
    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = None
    mock_get_db_session.return_value = iter([mock_db])

    # Execute task and expect error
    with pytest.raises(ValueError, match="User user-999 not found"):
      analyze_agent_task_impl(
        agent_type="financial_analyst",
        graph_id="kg123456",
        request_data={"message": "Test"},
        operation_id="op-123",
        user_id="user-999",
      )

    # Verify error event emitted
    error_calls = [call for call in mock_emit.call_args_list if "FAILED" in str(call)]
    assert len(error_calls) > 0
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_not_found(self, mock_get_db_session, mock_registry_class, mock_emit):
    """Test handling of agent type not found."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup registry to return None
    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = None
    mock_registry_class.return_value = mock_registry

    # Execute task and expect error
    with pytest.raises(ValueError, match="Agent type 'unknown_agent' not found"):
      analyze_agent_task_impl(
        agent_type="unknown_agent",
        graph_id="kg123456",
        request_data={"message": "Test"},
        operation_id="op-123",
        user_id="user-123",
      )

    # Verify error event emitted
    error_calls = [call for call in mock_emit.call_args_list if "FAILED" in str(call)]
    assert len(error_calls) > 0

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_with_quick_mode(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis with quick mode."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Quick Agent"

    mock_response = MagicMock()
    mock_response.content = "Quick analysis"
    mock_response.agent_name = "quick-agent"
    mock_response.mode_used = MagicMock()
    mock_response.mode_used.value = "quick"
    mock_response.metadata = {}
    mock_response.tokens_used = 50
    mock_response.confidence_score = 0.85
    mock_response.error_details = None
    mock_response.tools_called = []

    mock_agent.analyze = AsyncMock(return_value=mock_response)

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task with quick mode
    result = analyze_agent_task_impl(
      agent_type="summarizer",
      graph_id="kg123456",
      request_data={"message": "Summarize", "mode": "quick"},
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify quick mode was used
    assert result["mode_used"] == "quick"
    assert result["tokens_used"] == 50

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_with_extended_mode(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis with extended mode."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Extended Agent"

    mock_response = MagicMock()
    mock_response.content = "Extended analysis"
    mock_response.agent_name = "extended-agent"
    mock_response.mode_used = MagicMock()
    mock_response.mode_used.value = "extended"
    mock_response.metadata = {}
    mock_response.tokens_used = 500
    mock_response.confidence_score = 0.98
    mock_response.error_details = None
    mock_response.tools_called = ["search", "analyze", "verify"]

    mock_agent.analyze = AsyncMock(return_value=mock_response)

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task with extended mode
    result = analyze_agent_task_impl(
      agent_type="researcher",
      graph_id="kg123456",
      request_data={"message": "Deep analysis", "mode": "extended"},
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify extended mode was used
    assert result["mode_used"] == "extended"
    assert result["tokens_used"] == 500
    assert len(result["tools_called"]) == 3

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_with_history(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis with conversation history."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Contextual Agent"

    mock_response = MagicMock()
    mock_response.content = "Contextual analysis"
    mock_response.agent_name = "contextual-agent"
    mock_response.mode_used = MagicMock()
    mock_response.mode_used.value = "standard"
    mock_response.metadata = {}
    mock_response.tokens_used = 200
    mock_response.confidence_score = 0.92
    mock_response.error_details = None
    mock_response.tools_called = []

    mock_agent.analyze = AsyncMock(return_value=mock_response)

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task with history
    history = [
      {"role": "user", "content": "Previous question"},
      {"role": "assistant", "content": "Previous answer"},
    ]

    analyze_agent_task_impl(
      agent_type="assistant",
      graph_id="kg123456",
      request_data={
        "message": "Follow-up question",
        "mode": "standard",
        "history": history,
      },
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify analyze was called with history
    mock_agent.analyze.assert_called_once()
    call_kwargs = mock_agent.analyze.call_args[1]
    assert call_kwargs["history"] == history

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_with_progress_callback(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis with progress callback."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent that calls callback
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Progress Agent"

    async def mock_analyze(**kwargs):
      callback = kwargs.get("callback")
      if callback:
        callback("searching", 30, "Searching database")
        callback("analyzing", 60, "Analyzing results")
        callback("finalizing", 90, "Finalizing response")

      mock_response = MagicMock()
      mock_response.content = "Analysis with progress"
      mock_response.agent_name = "progress-agent"
      mock_response.mode_used = MagicMock()
      mock_response.mode_used.value = "standard"
      mock_response.metadata = {}
      mock_response.tokens_used = 100
      mock_response.confidence_score = 0.90
      mock_response.error_details = None
      mock_response.tools_called = []
      return mock_response

    mock_agent.analyze = mock_analyze

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task
    analyze_agent_task_impl(
      agent_type="progress_test",
      graph_id="kg123456",
      request_data={"message": "Test with progress"},
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify progress events were emitted
    progress_calls = [
      call
      for call in mock_emit.call_args_list
      if "RUNNING" in str(call) and "stage" in str(call)
    ]
    assert len(progress_calls) >= 3

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_failure(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis handling of execution failure."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent that raises error
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Failing Agent"
    mock_agent.analyze = AsyncMock(side_effect=Exception("Analysis failed"))

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task and expect error
    with pytest.raises(Exception, match="Analysis failed"):
      analyze_agent_task_impl(
        agent_type="failing_agent",
        graph_id="kg123456",
        request_data={"message": "Test"},
        operation_id="op-123",
        user_id="user-123",
      )

    # Verify error event emitted
    error_calls = [call for call in mock_emit.call_args_list if "FAILED" in str(call)]
    assert len(error_calls) > 0
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  @patch("robosystems.operations.agents.registry.AgentRegistry")
  @patch("robosystems.database.get_db_session")
  def test_agent_analysis_with_context(
    self, mock_get_db_session, mock_registry_class, mock_emit
  ):
    """Test agent analysis with additional context."""
    from robosystems.tasks.agents.analyze import analyze_agent_task_impl

    # Setup database session
    mock_db = MagicMock()
    mock_user = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = mock_user
    mock_get_db_session.return_value = iter([mock_db])

    # Setup agent
    mock_agent = MagicMock()
    mock_agent.metadata.name = "Context Agent"

    mock_response = MagicMock()
    mock_response.content = "Analysis with context"
    mock_response.agent_name = "context-agent"
    mock_response.mode_used = MagicMock()
    mock_response.mode_used.value = "standard"
    mock_response.metadata = {}
    mock_response.tokens_used = 175
    mock_response.confidence_score = 0.93
    mock_response.error_details = None
    mock_response.tools_called = []

    mock_agent.analyze = AsyncMock(return_value=mock_response)

    mock_registry = MagicMock()
    mock_registry.get_agent.return_value = mock_agent
    mock_registry_class.return_value = mock_registry

    # Execute task with context
    context = {"company": "NVDA", "period": "Q4 2024", "focus": "revenue"}

    analyze_agent_task_impl(
      agent_type="financial_analyst",
      graph_id="kg123456",
      request_data={
        "message": "Analyze financial performance",
        "mode": "standard",
        "context": context,
      },
      operation_id="op-123",
      user_id="user-123",
    )

    # Verify analyze was called with context
    call_kwargs = mock_agent.analyze.call_args[1]
    assert call_kwargs["context"] == context


class TestAgentAnalysisTaskFailureHandler:
  """Test cases for AgentAnalysisTask failure handler."""

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  def test_on_failure_with_operation_id_in_kwargs(self, mock_emit):
    """Test failure handler with operation_id in kwargs."""
    from robosystems.tasks.agents.analyze import AgentAnalysisTask

    task = AgentAnalysisTask()
    exc = ValueError("Test error")

    task.on_failure(
      exc=exc,
      task_id="task-123",
      args=[],
      kwargs={"operation_id": "op-456"},
      einfo=None,
    )

    # Verify error event emitted
    mock_emit.assert_called_once()
    call_args = mock_emit.call_args[1]
    assert call_args["operation_id"] == "op-456"
    assert "FAILED" in str(call_args["status"])
    assert call_args["data"]["error"] == "Test error"
    assert call_args["data"]["error_type"] == "ValueError"

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  def test_on_failure_with_operation_id_in_args(self, mock_emit):
    """Test failure handler with operation_id in args."""
    from robosystems.tasks.agents.analyze import AgentAnalysisTask

    task = AgentAnalysisTask()
    exc = RuntimeError("Another error")

    task.on_failure(
      exc=exc,
      task_id="task-789",
      args=["agent", "graph", {}, "op-789"],
      kwargs={},
      einfo=None,
    )

    # Verify error event emitted
    mock_emit.assert_called_once()
    call_args = mock_emit.call_args[1]
    assert call_args["operation_id"] == "op-789"

  @patch("robosystems.tasks.agents.analyze.emit_sse_event")
  def test_on_failure_without_operation_id(self, mock_emit):
    """Test failure handler without operation_id."""
    from robosystems.tasks.agents.analyze import AgentAnalysisTask

    task = AgentAnalysisTask()
    exc = Exception("Error without op ID")

    task.on_failure(exc=exc, task_id="task-999", args=[], kwargs={}, einfo=None)

    # Verify no SSE event emitted
    mock_emit.assert_not_called()
