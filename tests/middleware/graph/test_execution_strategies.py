"""Execution strategies tests."""

from robosystems.middleware.graph.execution_strategies import (
  BaseAnalyzer,
  BaseClientDetector,
  BaseExecutionStrategy,
  BaseStrategySelector,
  ResponseMode,
)


class TestBaseExecutionStrategy:
  """Tests for BaseExecutionStrategy enum."""

  def test_immediate_strategies(self):
    assert BaseExecutionStrategy.JSON_IMMEDIATE.value == "json_immediate"
    assert BaseExecutionStrategy.JSON_COMPLETE.value == "json_complete"

  def test_streaming_strategies(self):
    assert BaseExecutionStrategy.NDJSON_STREAMING.value == "ndjson_streaming"
    assert BaseExecutionStrategy.SSE_STREAMING.value == "sse_streaming"
    assert BaseExecutionStrategy.SSE_PROGRESS.value == "sse_progress"

  def test_queue_strategies(self):
    assert BaseExecutionStrategy.QUEUE_WITH_MONITORING.value == "queue_monitoring"
    assert BaseExecutionStrategy.QUEUE_SIMPLE.value == "queue_simple"


class TestResponseMode:
  """Tests for ResponseMode enum."""

  def test_modes(self):
    assert ResponseMode.AUTO.value == "auto"
    assert ResponseMode.SYNC.value == "sync"
    assert ResponseMode.ASYNC.value == "async"
    assert ResponseMode.STREAM.value == "stream"


class TestAnalyzeCypherQuery:
  """Tests for BaseAnalyzer.analyze_cypher_query."""

  def test_simple_match_return(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n:Entity) RETURN n")
    assert result["has_match"] is True
    assert result["has_limit"] is False
    assert result["estimated_rows"] == "large"

  def test_with_limit(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n LIMIT 50")
    assert result["has_limit"] is True
    assert result["limit_value"] == 50
    assert result["estimated_rows"] == "small"

  def test_medium_limit(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n LIMIT 500")
    assert result["estimated_rows"] == "medium"

  def test_large_limit(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n LIMIT 5000")
    assert result["estimated_rows"] == "large"

  def test_count_aggregation(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n:Entity) RETURN COUNT(n)")
    assert result["has_aggregation"] is True
    assert result["is_count_only"] is True
    assert result["estimated_rows"] == "small"

  def test_has_where(self):
    result = BaseAnalyzer.analyze_cypher_query(
      "MATCH (n) WHERE n.name = 'test' RETURN n LIMIT 10"
    )
    assert result["has_where"] is True

  def test_has_order_by(self):
    result = BaseAnalyzer.analyze_cypher_query(
      "MATCH (n) RETURN n ORDER BY n.name LIMIT 10"
    )
    assert result["has_order_by"] is True

  def test_shortest_path(self):
    result = BaseAnalyzer.analyze_cypher_query(
      "MATCH path = SHORTEST PATH (a)-[*]-(b) RETURN path"
    )
    assert result["has_shortest_path"] is True
    assert result["potentially_expensive"] is True

  def test_all_paths(self):
    result = BaseAnalyzer.analyze_cypher_query(
      "MATCH path = ALL PATH (a)-[*]-(b) RETURN path"
    )
    assert result["has_all_paths"] is True
    assert result["potentially_expensive"] is True

  def test_requires_streaming_large_no_agg(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n")
    assert result["requires_streaming"] is True

  def test_no_streaming_with_aggregation(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN COUNT(n)")
    assert result["requires_streaming"] is False

  def test_supports_progress(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n")
    assert result["supports_progress"] is True

  def test_parameterized_limit(self):
    result = BaseAnalyzer.analyze_cypher_query("MATCH (n) RETURN n LIMIT $limit")
    assert result["has_limit"] is True
    assert result["limit_value"] is None
    assert result["estimated_rows"] == "medium"


class TestEstimateResultSize:
  """Tests for BaseAnalyzer._estimate_result_size."""

  def test_small_limit(self):
    assert BaseAnalyzer._estimate_result_size("RETURN LIMIT", 50) == "small"

  def test_medium_limit(self):
    assert BaseAnalyzer._estimate_result_size("RETURN LIMIT", 500) == "medium"

  def test_large_limit(self):
    assert BaseAnalyzer._estimate_result_size("RETURN LIMIT", 5000) == "large"

  def test_count_no_limit(self):
    assert (
      BaseAnalyzer._estimate_result_size("RETURN COUNT( something", None) == "small"
    )

  def test_no_limit_no_count(self):
    assert BaseAnalyzer._estimate_result_size("MATCH RETURN", None) == "large"

  def test_boundary_small(self):
    assert BaseAnalyzer._estimate_result_size("LIMIT", 100) == "small"

  def test_boundary_medium(self):
    assert BaseAnalyzer._estimate_result_size("LIMIT", 1000) == "medium"


class TestBaseClientDetector:
  """Tests for BaseClientDetector."""

  def test_sse_support_from_accept(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"accept": "text/event-stream"}
    )
    assert result["supports_sse"] is True
    assert result["supports_streaming"] is True

  def test_ndjson_support(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"accept": "application/x-ndjson"}
    )
    assert result["supports_ndjson"] is True
    assert result["supports_streaming"] is True

  def test_no_streaming_support(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"accept": "application/json"}
    )
    assert result["supports_sse"] is False
    assert result["supports_ndjson"] is False
    assert result["supports_streaming"] is False

  def test_postman_detected(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"user-agent": "PostmanRuntime/7.32.3", "accept": "*/*"}
    )
    assert result["is_testing_tool"] is True
    assert result["is_interactive"] is True

  def test_curl_detected(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"user-agent": "curl/8.1.2", "accept": "*/*"}
    )
    assert result["is_testing_tool"] is True

  def test_browser_detected(self):
    result = BaseClientDetector.detect_client_capabilities(
      {"user-agent": "Mozilla/5.0 (Macintosh) Chrome/120", "accept": "text/html"}
    )
    assert result["is_browser"] is True

  def test_empty_headers(self):
    result = BaseClientDetector.detect_client_capabilities({})
    assert result["supports_sse"] is False
    assert result["is_testing_tool"] is False
    assert result["is_browser"] is False


class TestBaseStrategySelector:
  """Tests for BaseStrategySelector."""

  def test_should_use_cache_true(self):
    assert BaseStrategySelector.should_use_cache(True, True) is True

  def test_should_use_cache_not_cacheable(self):
    assert BaseStrategySelector.should_use_cache(False, True) is False

  def test_should_use_cache_not_available(self):
    assert BaseStrategySelector.should_use_cache(True, False) is False

  def test_should_use_cache_zero_ttl(self):
    assert BaseStrategySelector.should_use_cache(True, True, 0) is False

  def test_should_use_cache_positive_ttl(self):
    assert BaseStrategySelector.should_use_cache(True, True, 60) is True

  def test_should_use_cache_none_ttl(self):
    assert BaseStrategySelector.should_use_cache(True, True, None) is True

  def test_should_queue_high_load(self):
    assert (
      BaseStrategySelector.should_queue(
        queue_size=15, running_count=3, max_concurrent=5, queue_threshold=10
      )
      is True
    )

  def test_should_queue_at_capacity(self):
    assert (
      BaseStrategySelector.should_queue(queue_size=0, running_count=5, max_concurrent=5)
      is True
    )

  def test_should_not_queue_low_load(self):
    assert (
      BaseStrategySelector.should_queue(
        queue_size=2, running_count=1, max_concurrent=5, queue_threshold=10
      )
      is False
    )

  def test_select_streaming_sse_large(self):
    result = BaseStrategySelector.select_streaming_strategy(
      supports_sse=True,
      supports_ndjson=False,
      requires_streaming=True,
      is_interactive=False,
      estimated_size="large",
    )
    assert result == BaseExecutionStrategy.SSE_STREAMING

  def test_select_streaming_ndjson_large(self):
    result = BaseStrategySelector.select_streaming_strategy(
      supports_sse=False,
      supports_ndjson=True,
      requires_streaming=True,
      is_interactive=False,
      estimated_size="large",
    )
    assert result == BaseExecutionStrategy.NDJSON_STREAMING

  def test_select_streaming_medium_sse(self):
    result = BaseStrategySelector.select_streaming_strategy(
      supports_sse=True,
      supports_ndjson=False,
      requires_streaming=False,
      is_interactive=False,
      estimated_size="medium",
    )
    assert result == BaseExecutionStrategy.SSE_PROGRESS

  def test_select_streaming_no_support(self):
    result = BaseStrategySelector.select_streaming_strategy(
      supports_sse=False,
      supports_ndjson=False,
      requires_streaming=True,
      is_interactive=False,
      estimated_size="large",
    )
    assert result is None

  def test_select_streaming_small_not_required(self):
    result = BaseStrategySelector.select_streaming_strategy(
      supports_sse=True,
      supports_ndjson=True,
      requires_streaming=False,
      is_interactive=False,
      estimated_size="small",
    )
    assert result is None
