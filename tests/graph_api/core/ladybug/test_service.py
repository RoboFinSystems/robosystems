"""Comprehensive unit tests for the LadybugDB service.

Covers column alias extraction, Cypher validation, query execution,
result formatting, streaming chunks, health check, cluster info,
node configuration validation, and singleton lifecycle.
"""

import os
from concurrent.futures import TimeoutError
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.ladybug.service import (
  LadybugService,
  NoOpSpan,
  NoOpTracer,
  _extract_column_aliases_from_cypher,
  _raise_database_not_found,
  validate_cypher_query,
)
from robosystems.middleware.graph.types import NodeType, RepositoryType

SERVICE_MODULE = "robosystems.graph_api.core.ladybug.service"


# ---------------------------------------------------------------------------
# _extract_column_aliases_from_cypher
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExtractColumnAliases:
  """Tests for the pure regex-based Cypher RETURN clause parser."""

  def test_simple_alias(self):
    """Extracts explicit AS alias."""
    result = _extract_column_aliases_from_cypher("MATCH (c) RETURN c AS entity")
    assert result == ["entity"]

  def test_multiple_aliases(self):
    """Extracts multiple aliases."""
    result = _extract_column_aliases_from_cypher(
      "MATCH (c) RETURN c.name AS name, c.age AS age"
    )
    assert result == ["name", "age"]

  def test_mixed_alias_and_expression(self):
    """Handles mix of aliased and unaliased columns."""
    result = _extract_column_aliases_from_cypher(
      "MATCH (c) RETURN c.name, c.age AS user_age"
    )
    assert result == ["c.name", "user_age"]

  def test_function_with_alias(self):
    """Extracts alias from function expressions."""
    result = _extract_column_aliases_from_cypher("MATCH (c) RETURN count(c) AS total")
    assert result == ["total"]

  def test_case_insensitive_return(self):
    """Handles case-insensitive RETURN keyword."""
    result = _extract_column_aliases_from_cypher("MATCH (n) return n.id AS id")
    assert result == ["id"]

  def test_case_insensitive_as(self):
    """Handles case-insensitive AS keyword."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN n.name as name")
    assert result == ["name"]

  def test_return_with_order_by(self):
    """Stops parsing at ORDER BY clause."""
    result = _extract_column_aliases_from_cypher(
      "MATCH (n) RETURN n.name AS name ORDER BY n.name"
    )
    assert result == ["name"]

  def test_return_with_limit(self):
    """Stops parsing at LIMIT clause."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN n.id AS id LIMIT 10")
    assert result == ["id"]

  def test_return_with_skip(self):
    """Stops parsing at SKIP clause."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN n.name SKIP 5")
    assert result == ["n.name"]

  def test_return_with_where(self):
    """Stops parsing at WHERE after WITH/RETURN."""
    result = _extract_column_aliases_from_cypher(
      "MATCH (n) RETURN n.name WHERE n.active = true"
    )
    assert result == ["n.name"]

  def test_no_return_clause(self):
    """Returns empty list when no RETURN clause."""
    result = _extract_column_aliases_from_cypher("CREATE (n:Person {name: 'Alice'})")
    assert result == []

  def test_empty_query(self):
    """Returns empty list for empty query."""
    result = _extract_column_aliases_from_cypher("")
    assert result == []

  def test_multiline_query(self):
    """Handles multiline queries by normalizing whitespace."""
    result = _extract_column_aliases_from_cypher(
      """MATCH (c:Company)
            RETURN c.name AS company_name,
            c.ticker AS ticker"""
    )
    assert result == ["company_name", "ticker"]

  def test_expression_without_alias(self):
    """Returns cleaned expression when no alias."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN n.name, n.age")
    assert result == ["n.name", "n.age"]

  def test_single_return_value(self):
    """Handles single return value without alias."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN n")
    assert result == ["n"]

  def test_star_return(self):
    """Handles RETURN *."""
    result = _extract_column_aliases_from_cypher("MATCH (n) RETURN *")
    assert result == ["*"]


# ---------------------------------------------------------------------------
# validate_cypher_query
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestValidateCypherQuery:
  """Tests for forbidden keyword detection in Cypher queries."""

  def test_valid_query_passes(self):
    """Valid Cypher query does not raise."""
    validate_cypher_query("MATCH (n:Person) RETURN n.name")

  def test_empty_query_raises_400(self):
    """Empty query raises 400."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query("")
    assert exc_info.value.status_code == 400
    assert "cannot be empty" in exc_info.value.detail

  def test_whitespace_only_raises_400(self):
    """Whitespace-only query raises 400."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query("   ")
    assert exc_info.value.status_code == 400

  def test_none_query_raises_400(self):
    """None query raises 400."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query(None)  # type: ignore[arg-type]
    assert exc_info.value.status_code == 400

  @pytest.mark.parametrize(
    "forbidden",
    [
      "CREATE DATABASE test",
      "DROP DATABASE test",
      "CREATE USER admin",
      "DROP USER admin",
      "ALTER USER admin",
      "SET PASSWORD admin",
      "GRANT ALL ON db TO user",
      "REVOKE ALL ON db FROM user",
      "CALL DBMS.listConfig()",
      "CALL APOC.convert.fromJsonList()",
      "LOAD CSV WITH HEADERS FROM 'file:///etc/passwd'",
    ],
  )
  def test_forbidden_keywords_raise_403(self, forbidden):
    """Forbidden keywords raise 403 Forbidden."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query(forbidden)
    assert exc_info.value.status_code == 403
    assert "forbidden keyword" in exc_info.value.detail

  def test_forbidden_keyword_case_insensitive(self):
    """Forbidden keywords detected regardless of case."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query("create database test")
    assert exc_info.value.status_code == 403

  def test_query_length_limit(self):
    """Query exceeding max length raises 400."""
    long_query = "MATCH (n) RETURN n " + "a" * 10001
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query(long_query)
    assert exc_info.value.status_code == 400
    assert "too long" in exc_info.value.detail

  def test_query_at_max_length_passes(self):
    """Query at exactly max length passes."""
    # MAX_QUERY_LENGTH is 10000
    query = "M" * 10000
    validate_cypher_query(query)  # Should not raise


@pytest.mark.unit
class TestValidateCypherQueryEngineVerbs:
  """F4: engine-adjacent verbs (bulk load / attach / install / schema DDL) are
  blocked via the shared analyzer, while ordinary graph writes still pass so
  writer instances keep working."""

  @pytest.mark.parametrize(
    "ordinary_write",
    [
      "MATCH (n:Person) RETURN n.name",
      "CREATE (n:Foo {id: 1}) RETURN n",
      "MATCH (n:Foo) SET n.x = 2 RETURN n",
      "MERGE (n:Foo {id: 1}) RETURN n",
      "MATCH (n:Foo) DETACH DELETE n",
    ],
  )
  def test_ordinary_reads_and_writes_pass(self, ordinary_write):
    """Reads and node/relationship writes are not engine-level operations."""
    validate_cypher_query(ordinary_write)  # Should not raise

  @pytest.mark.parametrize(
    "engine_verb",
    [
      "LOAD FROM '/etc/passwd' RETURN *",
      "COPY Person FROM '/tmp/data.csv'",
      "ATTACH '/other/db.kuzu' AS other",
      "INSTALL httpfs",
      "CREATE NODE TABLE Evil(id INT64, PRIMARY KEY(id))",
      "CREATE REL TABLE Knows(FROM Person TO Person)",
    ],
  )
  def test_engine_verbs_raise_403(self, engine_verb):
    """Bulk / admin / schema-DDL verbs are rejected on the ad-hoc endpoint."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query(engine_verb)
    assert exc_info.value.status_code == 403

  def test_in_string_comment_marker_cannot_hide_engine_verb(self):
    """F2 + F4 compose: an in-string `//` must not mask a trailing COPY."""
    with pytest.raises(HTTPException) as exc_info:
      validate_cypher_query("MATCH (n) WHERE n.note = '//' COPY t FROM '/x'")
    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# _raise_database_not_found
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRaiseDatabaseNotFound:
  """Tests for _raise_database_not_found behavior."""

  def test_raises_404_when_not_warming_up(self):
    """Raises 404 when replica is not warming up."""
    with patch(
      "robosystems.graph_api.routers.health.is_warming_up",
      return_value=False,
    ):
      with pytest.raises(HTTPException) as exc_info:
        _raise_database_not_found("test_db")
      assert exc_info.value.status_code == 404
      assert "test_db" in exc_info.value.detail

  def test_raises_503_when_warming_up(self):
    """Raises 503 when replica is warming up."""
    with patch(
      "robosystems.graph_api.routers.health.is_warming_up",
      return_value=True,
    ):
      with pytest.raises(HTTPException) as exc_info:
        _raise_database_not_found("test_db")
      assert exc_info.value.status_code == 503
      assert "warming" in str(exc_info.value.detail)


# ---------------------------------------------------------------------------
# NoOp tracing classes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNoOpTracing:
  """Tests for NoOpSpan and NoOpTracer."""

  def test_noop_span_context_manager(self):
    """NoOpSpan works as a context manager."""
    span = NoOpSpan()
    with span as s:
      assert s is span

  def test_noop_span_set_attribute(self):
    """NoOpSpan.set_attribute is a no-op."""
    span = NoOpSpan()
    span.set_attribute("key", "value")  # Should not raise

  def test_noop_tracer_returns_span(self):
    """NoOpTracer.start_as_current_span returns a NoOpSpan."""
    tracer = NoOpTracer()
    span = tracer.start_as_current_span("test_span")
    assert isinstance(span, NoOpSpan)


# ---------------------------------------------------------------------------
# LadybugService initialization
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLadybugServiceInit:
  """Tests for LadybugService initialization and configuration validation."""

  @pytest.fixture
  def mock_dependencies(self):
    """Mock all external dependencies for service init."""
    with (
      patch(f"{SERVICE_MODULE}.LadybugDatabaseManager") as mock_db_mgr,
      patch(f"{SERVICE_MODULE}.LadybugMetricsCollector") as mock_metrics,
    ):
      mock_db_mgr.return_value = MagicMock()
      mock_metrics.return_value = MagicMock()
      yield mock_db_mgr, mock_metrics

  def test_basic_initialization(self, mock_dependencies):
    """Service initializes with basic parameters."""
    service = LadybugService(
      base_path="/tmp/lbug",
      max_databases=100,
      read_only=False,
      node_type=NodeType.WRITER,
      repository_type=RepositoryType.ENTITY,
    )
    assert service.base_path == "/tmp/lbug"
    assert service.max_databases == 100
    assert service.read_only is False
    assert service.node_type == NodeType.WRITER
    assert service.repository_type == RepositoryType.ENTITY
    assert service.last_activity is None

  def test_writer_read_only_raises(self, mock_dependencies):
    """Writer node with read_only=True raises ConfigurationError."""
    from robosystems.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError):
      LadybugService(
        base_path="/tmp/lbug",
        read_only=True,
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )

  def test_shared_master_read_only_raises(self, mock_dependencies):
    """Shared master with read_only=True raises ConfigurationError."""
    from robosystems.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError):
      LadybugService(
        base_path="/tmp/lbug",
        read_only=True,
        node_type=NodeType.SHARED_MASTER,
        repository_type=RepositoryType.SHARED,
      )

  def test_shared_master_wrong_repo_type_raises(self, mock_dependencies):
    """Shared master with entity repo type raises ValueError."""
    with pytest.raises(ValueError, match="shared repository type"):
      LadybugService(
        base_path="/tmp/lbug",
        node_type=NodeType.SHARED_MASTER,
        repository_type=RepositoryType.ENTITY,
      )

  def test_node_id_format(self, mock_dependencies):
    """Node ID contains node type and PID."""
    service = LadybugService(
      base_path="/tmp/lbug",
      node_type=NodeType.WRITER,
      repository_type=RepositoryType.ENTITY,
    )
    assert service.node_id.startswith("lbug-writer-")
    assert str(os.getpid()) in service.node_id

  def test_get_uptime(self, mock_dependencies):
    """get_uptime returns positive value."""
    service = LadybugService(
      base_path="/tmp/lbug",
      node_type=NodeType.WRITER,
      repository_type=RepositoryType.ENTITY,
    )
    uptime = service.get_uptime()
    assert uptime >= 0

  def test_shared_master_high_max_databases_warns(self, mock_dependencies):
    """Shared master with many max_databases logs a warning."""
    # Should not raise, just warn
    service = LadybugService(
      base_path="/tmp/lbug",
      max_databases=100,
      node_type=NodeType.SHARED_MASTER,
      repository_type=RepositoryType.SHARED,
    )
    assert service.max_databases == 100


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLadybugServiceExecuteQuery:
  """Tests for execute_query with mocked connections."""

  @pytest.fixture
  def service(self):
    """Create a service with mocked dependencies."""
    with (
      patch(f"{SERVICE_MODULE}.LadybugDatabaseManager") as mock_db_mgr,
      patch(f"{SERVICE_MODULE}.LadybugMetricsCollector") as mock_metrics,
    ):
      mock_db_instance = MagicMock()
      mock_db_mgr.return_value = mock_db_instance
      mock_metrics.return_value = MagicMock()

      svc = LadybugService(
        base_path="/tmp/lbug",
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )
      yield svc

  def test_query_returns_formatted_response(self, service):
    """Valid query returns QueryResponse with data, columns, row_count."""
    from robosystems.graph_api.models.database import QueryRequest

    # Mock database listing and connection
    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.get_schema.return_value = {"name": "STRING"}

    # Simulate one row
    row_data = [("Alice",)]
    call_count = [0]

    def has_next():
      return call_count[0] < len(row_data)

    def get_next():
      row = row_data[call_count[0]]
      call_count[0] += 1
      return row

    mock_query_result.has_next = has_next
    mock_query_result.get_next = get_next

    mock_conn.execute.return_value = mock_query_result
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 30

      # Mock ThreadPoolExecutor
      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.return_value = mock_query_result
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="MATCH (n) RETURN n.name AS name",
        )
        response = service.execute_query(request)

    assert response.row_count == 1
    assert response.database == "test_db"
    assert len(response.data) == 1
    assert response.data[0]["name"] == "Alice"

  def test_query_missing_database_raises_404(self, service):
    """Query on non-existent database raises 404."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["other_db"]

    with patch(
      "robosystems.graph_api.routers.health.is_warming_up",
      return_value=False,
    ):
      request = QueryRequest(
        database="missing_db",
        cypher="MATCH (n) RETURN n",
      )

      with pytest.raises(HTTPException) as exc_info:
        service.execute_query(request)
      assert exc_info.value.status_code == 404

  def test_query_timeout_raises_http_exception(self, service):
    """Query timeout raises HTTPException (408 timeout wrapped in 500)."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 1

      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.side_effect = TimeoutError()
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="MATCH (n) RETURN n",
        )

        with pytest.raises(HTTPException) as exc_info:
          service.execute_query(request)
        # The 408 is raised but caught by the outer handler,
        # resulting in the exception propagating
        assert exc_info.value.status_code in (408, 500)
        assert "timeout" in exc_info.value.detail.lower()

  def test_binder_exception_raises_400(self, service):
    """Binder exception during query raises 400."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 30

      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError(
          "Binder exception: Cannot find property x"
        )
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="MATCH (n) RETURN n.x",
        )

        with pytest.raises(HTTPException) as exc_info:
          service.execute_query(request)
        assert exc_info.value.status_code == 400
        assert "binding error" in exc_info.value.detail.lower()

  def test_parser_exception_raises_400(self, service):
    """Parser exception during query raises 400."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 30

      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError(
          "Parser exception: Invalid syntax"
        )
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="INVALID CYPHER",
        )

        with pytest.raises(HTTPException) as exc_info:
          service.execute_query(request)
        assert exc_info.value.status_code == 400

  def test_catalog_exception_raises_400(self, service):
    """Catalog exception during query raises 400."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 30

      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError(
          "Catalog exception: Table not found"
        )
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="MATCH (n:Missing) RETURN n",
        )

        with pytest.raises(HTTPException) as exc_info:
          service.execute_query(request)
        assert exc_info.value.status_code == 400

  def test_generic_runtime_error_raises_500(self, service):
    """Generic RuntimeError during query raises 500."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    with patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning:
      mock_tuning.get_graph_query_timeout.return_value = 30

      with patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor:
        mock_executor = MagicMock()
        MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
        MockExecutor.return_value.__exit__ = MagicMock(return_value=False)

        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError("Unknown runtime error")
        mock_executor.submit.return_value = mock_future

        request = QueryRequest(
          database="test_db",
          cypher="MATCH (n) RETURN n",
        )

        with pytest.raises(HTTPException) as exc_info:
          service.execute_query(request)
        assert exc_info.value.status_code == 500

  def test_query_validates_forbidden_keywords(self, service):
    """Query execution validates for forbidden keywords."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    request = QueryRequest(
      database="test_db",
      cypher="DROP DATABASE test_db",
    )

    with pytest.raises(HTTPException) as exc_info:
      service.execute_query(request)
    assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# Streaming query execution
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLadybugServiceStreamingQuery:
  """Tests for execute_query_streaming with chunk handling."""

  @pytest.fixture
  def service(self):
    """Create a service with mocked dependencies."""
    with (
      patch(f"{SERVICE_MODULE}.LadybugDatabaseManager") as mock_db_mgr,
      patch(f"{SERVICE_MODULE}.LadybugMetricsCollector") as mock_metrics,
    ):
      mock_db_instance = MagicMock()
      mock_db_mgr.return_value = mock_db_instance
      mock_metrics.return_value = MagicMock()

      svc = LadybugService(
        base_path="/tmp/lbug",
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )
      yield svc

  def test_streaming_binder_error_yields_error_chunk(self, service):
    """Binder exception yields error chunk instead of raising."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError(
      "Binder exception: Cannot find property"
    )
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n.x")
    chunks = list(service.execute_query_streaming(request))

    assert len(chunks) == 1
    assert "error" in chunks[0]
    assert chunks[0]["error_type"] == "BinderException"
    assert chunks[0]["is_last_chunk"] is True

  def test_streaming_parser_error_yields_error_chunk(self, service):
    """Parser exception yields error chunk."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Parser exception: Invalid syntax")
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    request = QueryRequest(database="test_db", cypher="INVALID")
    chunks = list(service.execute_query_streaming(request))

    assert chunks[0]["error_type"] == "ParserException"

  def test_streaming_catalog_error_yields_error_chunk(self, service):
    """Catalog exception yields error chunk."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Catalog exception: Not found")
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    request = QueryRequest(database="test_db", cypher="MATCH (n:X) RETURN n")
    chunks = list(service.execute_query_streaming(request))

    assert chunks[0]["error_type"] == "CatalogException"

  def test_streaming_generic_runtime_error_yields_error_chunk(self, service):
    """Generic RuntimeError yields error chunk."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Some runtime error")
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")
    chunks = list(service.execute_query_streaming(request))

    assert chunks[0]["error_type"] == "RuntimeError"

  def test_streaming_unexpected_error_yields_error_chunk(self, service):
    """Unexpected exception yields error chunk."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["test_db"]

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = TypeError("unexpected type error")
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")
    chunks = list(service.execute_query_streaming(request))

    assert chunks[0]["error_type"] == "TypeError"

  def test_streaming_missing_database_raises_404(self, service):
    """Streaming on missing database raises 404."""
    from robosystems.graph_api.models.database import QueryRequest

    service.db_manager.list_databases.return_value = ["other_db"]

    with patch(
      "robosystems.graph_api.routers.health.is_warming_up", return_value=False
    ):
      request = QueryRequest(database="missing_db", cypher="MATCH (n) RETURN n")

      with pytest.raises(HTTPException) as exc_info:
        list(service.execute_query_streaming(request))
      assert exc_info.value.status_code == 404

  def _wire_streaming_result(self, service, mock_query_result):
    """Attach a mock query result to the service's connection for streaming."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_query_result
    service.db_manager.list_databases.return_value = ["test_db"]
    service.db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    service.db_manager.get_connection.return_value.__exit__ = MagicMock(
      return_value=False
    )

  def test_streaming_row_cap_truncates(self, service):
    """F5: pulling past MAX_STREAMING_ROWS stops the stream and flags truncated."""
    from robosystems.graph_api.models.database import QueryRequest

    mock_query_result = MagicMock()
    mock_query_result.get_schema.return_value = {"n": "INT64"}
    mock_query_result.has_next.return_value = True  # effectively unbounded
    counter = [0]

    def _get_next():
      counter[0] += 1
      return (counter[0],)

    mock_query_result.get_next.side_effect = _get_next
    self._wire_streaming_result(service, mock_query_result)

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")
    with patch(f"{SERVICE_MODULE}.MAX_STREAMING_ROWS", 4):
      chunks = list(service.execute_query_streaming(request, chunk_size=2))

    last = chunks[-1]
    assert last["is_last_chunk"] is True
    assert last["truncated"] is True
    delivered = sum(c.get("row_count", 0) for c in chunks if "error" not in c)
    assert delivered == 4  # capped at MAX_STREAMING_ROWS, not unbounded
    mock_query_result.close.assert_called_once()

  def test_streaming_under_cap_is_not_truncated(self, service):
    """A finite result under the cap streams fully and is not flagged."""
    from robosystems.graph_api.models.database import QueryRequest

    mock_query_result = MagicMock()
    mock_query_result.get_schema.return_value = {"n": "INT64"}
    rows = [(i,) for i in range(3)]
    idx = [0]

    def _has_next():
      return idx[0] < len(rows)

    def _get_next():
      row = rows[idx[0]]
      idx[0] += 1
      return row

    mock_query_result.has_next.side_effect = _has_next
    mock_query_result.get_next.side_effect = _get_next
    self._wire_streaming_result(service, mock_query_result)

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")
    chunks = list(service.execute_query_streaming(request, chunk_size=2))

    assert chunks[-1]["truncated"] is False
    delivered = sum(c.get("row_count", 0) for c in chunks if "error" not in c)
    assert delivered == 3

  def test_streaming_timeout_yields_timeout_chunk(self, service):
    """F5: a query that exceeds the timeout yields a QueryTimeout error chunk."""
    from robosystems.graph_api.models.database import QueryRequest

    self._wire_streaming_result(service, MagicMock())

    request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")
    with (
      patch(f"{SERVICE_MODULE}.TuningConfig") as mock_tuning,
      patch(f"{SERVICE_MODULE}.ThreadPoolExecutor") as MockExecutor,
    ):
      mock_tuning.get_graph_query_timeout.return_value = 5
      mock_executor = MagicMock()
      MockExecutor.return_value.__enter__ = MagicMock(return_value=mock_executor)
      MockExecutor.return_value.__exit__ = MagicMock(return_value=False)
      mock_future = MagicMock()
      mock_future.result.side_effect = TimeoutError()
      mock_executor.submit.return_value = mock_future

      chunks = list(service.execute_query_streaming(request))

    assert len(chunks) == 1
    assert chunks[0]["error_type"] == "QueryTimeout"
    assert chunks[0]["is_last_chunk"] is True


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLadybugServiceHealth:
  """Tests for get_cluster_health with psutil mocking."""

  @pytest.fixture
  def service(self):
    """Create a service with mocked dependencies."""
    with (
      patch(f"{SERVICE_MODULE}.LadybugDatabaseManager") as mock_db_mgr,
      patch(f"{SERVICE_MODULE}.LadybugMetricsCollector") as mock_metrics,
    ):
      mock_db_mgr.return_value = MagicMock()
      mock_metrics.return_value = MagicMock()

      svc = LadybugService(
        base_path="/tmp/lbug",
        max_databases=100,
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )
      yield svc

  def test_healthy_status(self, service):
    """Returns healthy when capacity is good and resources are low."""
    service.db_manager.list_databases.return_value = ["db1", "db2"]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 30.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=40.0)

      health = service.get_cluster_health()

    assert health.status == "healthy"
    assert health.current_databases == 2
    assert health.capacity_remaining == 98
    assert health.node_type == "writer"

  def test_full_status_when_at_capacity(self, service):
    """Returns full when no capacity remaining."""
    service.db_manager.list_databases.return_value = ["db" + str(i) for i in range(100)]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 10.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=10.0)

      health = service.get_cluster_health()

    assert health.status == "full"
    assert health.capacity_remaining == 0

  def test_warning_status_low_capacity(self, service):
    """Returns warning when capacity is low but not zero."""
    service.db_manager.list_databases.return_value = ["db" + str(i) for i in range(95)]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 10.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=10.0)

      health = service.get_cluster_health()

    assert health.status == "warning"
    assert health.capacity_remaining == 5

  def test_critical_status_high_cpu(self, service):
    """Returns critical when CPU usage is over 90%."""
    service.db_manager.list_databases.return_value = ["db1"]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 95.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=40.0)

      health = service.get_cluster_health()

    assert health.status == "critical"

  def test_critical_status_high_memory(self, service):
    """Returns critical when memory usage is over 90%."""
    service.db_manager.list_databases.return_value = ["db1"]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 30.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=95.0)

      health = service.get_cluster_health()

    assert health.status == "critical"

  def test_warning_status_high_cpu(self, service):
    """Returns warning when CPU is 75-90%."""
    service.db_manager.list_databases.return_value = ["db1"]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 80.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=40.0)

      health = service.get_cluster_health()

    assert health.status == "warning"

  def test_psutil_failure_fallback(self, service):
    """Falls back to 0% on psutil failure."""
    service.db_manager.list_databases.return_value = ["db1"]

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.side_effect = Exception("psutil error")

      health = service.get_cluster_health()

    # Falls back to 0% which is healthy
    assert health.status == "healthy"

  def test_health_includes_last_activity(self, service):
    """Health response includes last_activity when set."""
    from datetime import datetime

    service.db_manager.list_databases.return_value = []
    service.last_activity = datetime(2026, 1, 1, 12, 0, 0)

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 10.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=10.0)

      health = service.get_cluster_health()

    assert health.last_activity is not None

  def test_health_none_last_activity(self, service):
    """Health response has None last_activity when never used."""
    service.db_manager.list_databases.return_value = []

    with patch(f"{SERVICE_MODULE}.psutil") as mock_psutil:
      mock_psutil.cpu_percent.return_value = 10.0
      mock_psutil.virtual_memory.return_value = MagicMock(percent=10.0)

      health = service.get_cluster_health()

    assert health.last_activity is None


# ---------------------------------------------------------------------------
# Singleton pattern
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLadybugServiceSingleton:
  """Tests for get_ladybug_service / init_ladybug_service."""

  def test_get_before_init_raises(self):
    """get_ladybug_service raises RuntimeError before initialization."""
    import robosystems.graph_api.core.ladybug.service as svc_module

    original = svc_module._ladybug_service
    try:
      svc_module._ladybug_service = None
      with pytest.raises(RuntimeError, match="not initialized"):
        svc_module.get_ladybug_service()
    finally:
      svc_module._ladybug_service = original

  def test_init_creates_service(self):
    """init_ladybug_service creates and returns a service."""
    import robosystems.graph_api.core.ladybug.service as svc_module

    original = svc_module._ladybug_service
    try:
      svc_module._ladybug_service = None

      with (
        patch(f"{SERVICE_MODULE}.LadybugDatabaseManager"),
        patch(f"{SERVICE_MODULE}.LadybugMetricsCollector"),
      ):
        service = svc_module.init_ladybug_service("/tmp/test")

      assert service is not None
      assert svc_module._ladybug_service is service
    finally:
      svc_module._ladybug_service = original

  def test_double_init_returns_existing(self):
    """Double initialization returns the existing instance."""
    import robosystems.graph_api.core.ladybug.service as svc_module

    original = svc_module._ladybug_service
    try:
      svc_module._ladybug_service = None

      with (
        patch(f"{SERVICE_MODULE}.LadybugDatabaseManager"),
        patch(f"{SERVICE_MODULE}.LadybugMetricsCollector"),
      ):
        first = svc_module.init_ladybug_service("/tmp/test")
        second = svc_module.init_ladybug_service("/tmp/test")

      assert first is second
    finally:
      svc_module._ladybug_service = original

  def test_get_after_init_returns_service(self):
    """get_ladybug_service returns service after initialization."""
    import robosystems.graph_api.core.ladybug.service as svc_module

    original = svc_module._ladybug_service
    try:
      svc_module._ladybug_service = None

      with (
        patch(f"{SERVICE_MODULE}.LadybugDatabaseManager"),
        patch(f"{SERVICE_MODULE}.LadybugMetricsCollector"),
      ):
        expected = svc_module.init_ladybug_service("/tmp/test")
        actual = svc_module.get_ladybug_service()

      assert actual is expected
    finally:
      svc_module._ladybug_service = original
