"""Tests for shared-repository query telemetry.

Covers outcome classification (status map, engine-disruption shapes, the
aggregation-fallback envelope), the shared-repository scope gate (user graphs
must never emit), and the start/end cost-line pairing.
"""

import json
from unittest.mock import Mock, patch

from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
  is_disrupted_aggregation,
  is_engine_disruption,
  log_shared_query_end,
  log_shared_query_start,
  record_shared_query_outcome,
  signal_for_status,
)

_SHARED_CHECK = (
  "robosystems.middleware.graph.utils.MultiTenantUtils.is_shared_repository_or_subgraph"
)
_AUDIT = "robosystems.security.audit_logger.SecurityAuditLogger.log_query_abuse_signal"
_TELEMETRY_LOGGER = "robosystems.middleware.graph.query_telemetry.logger"


class TestClassification:
  def test_status_map_covers_signal_statuses(self):
    assert signal_for_status(400) == "statement_rejected"
    assert signal_for_status(403) == "access_denied"
    assert signal_for_status(408) == "timeout"
    assert signal_for_status(429) == "rate_limited"
    assert signal_for_status(503) == "capacity_rejected"

  def test_status_map_drops_non_signal_statuses(self):
    assert signal_for_status(402) is None
    assert signal_for_status(404) is None
    assert signal_for_status(500) is None
    assert signal_for_status(None) is None

  def test_engine_disruption_by_exception_type(self):
    assert is_engine_disruption(ConnectionResetError("boom"))
    assert is_engine_disruption(ConnectionError("boom"))

  def test_engine_disruption_by_message(self):
    assert is_engine_disruption(RuntimeError("peer closed connection unexpectedly"))
    assert is_engine_disruption(Exception("Server disconnected without response"))

  def test_ordinary_errors_are_not_disruptions(self):
    assert not is_engine_disruption(ValueError("bad query syntax"))
    assert not is_engine_disruption(TimeoutError())

  def test_disrupted_aggregation_matches_fallback_envelope(self):
    assert is_disrupted_aggregation(
      {"success": False, "error": "Unable to aggregate results", "tool": "x"}
    )

  def test_disrupted_aggregation_ignores_other_shapes(self):
    assert not is_disrupted_aggregation({"success": True, "result": {}})
    assert not is_disrupted_aggregation({"success": False, "error": "Query failed"})
    assert not is_disrupted_aggregation(None)
    assert not is_disrupted_aggregation("Unable to aggregate results")


class TestRecordSharedQueryOutcome:
  @patch(_AUDIT)
  @patch(_SHARED_CHECK, return_value=False)
  def test_user_graph_never_records(self, mock_shared, mock_audit):
    record_shared_query_outcome("kg123abc", "user_1", status_code=408)
    mock_audit.assert_not_called()

  @patch(_AUDIT)
  @patch(_SHARED_CHECK, return_value=True)
  def test_status_code_classifies(self, mock_shared, mock_audit):
    record_shared_query_outcome(
      "sec",
      "user_1",
      status_code=429,
      api_key_prefix="rfs12345",
      endpoint="/v1/graphs/{graph_id}/query/cypher",
      source="query_cypher",
    )
    mock_audit.assert_called_once()
    kwargs = mock_audit.call_args.kwargs
    assert kwargs["signal"] == "rate_limited"
    assert kwargs["user_id"] == "user_1"
    assert kwargs["api_key_prefix"] == "rfs12345"
    assert kwargs["disruption"] is False
    assert kwargs["metadata"]["source"] == "query_cypher"
    assert kwargs["metadata"]["status_code"] == 429

  @patch(_AUDIT)
  @patch(_SHARED_CHECK, return_value=True)
  def test_explicit_signal_wins(self, mock_shared, mock_audit):
    record_shared_query_outcome(
      "sec", "user_1", signal="stream_disrupted", disruption=True, tool_name="rgc"
    )
    kwargs = mock_audit.call_args.kwargs
    assert kwargs["signal"] == "stream_disrupted"
    assert kwargs["disruption"] is True
    assert kwargs["metadata"]["tool_name"] == "rgc"

  @patch(_AUDIT)
  @patch(_SHARED_CHECK, return_value=True)
  def test_disruption_error_classifies_and_flags(self, mock_shared, mock_audit):
    record_shared_query_outcome("sec", "user_1", error=ConnectionResetError("gone"))
    kwargs = mock_audit.call_args.kwargs
    assert kwargs["signal"] == "engine_disruption"
    assert kwargs["disruption"] is True

  @patch(_AUDIT)
  @patch(_SHARED_CHECK, return_value=True)
  def test_unclassifiable_outcome_is_dropped(self, mock_shared, mock_audit):
    record_shared_query_outcome("sec", "user_1", status_code=500)
    record_shared_query_outcome("sec", "user_1", error=ValueError("syntax"))
    record_shared_query_outcome("sec", "user_1")
    mock_audit.assert_not_called()

  @patch(_AUDIT, side_effect=RuntimeError("audit down"))
  @patch(_SHARED_CHECK, return_value=True)
  def test_recording_failure_never_raises(self, mock_shared, mock_audit):
    record_shared_query_outcome("sec", "user_1", status_code=408)


class TestCostLines:
  @patch(_TELEMETRY_LOGGER)
  @patch(_SHARED_CHECK, return_value=True)
  def test_start_end_pair_share_exec_id(self, mock_shared, mock_logger):
    exec_id = log_shared_query_start(
      "sec",
      "user_1",
      api_key_prefix="rfs12345",
      source="query_cypher",
      query_length=42,
      strategy="json_complete",
    )
    assert exec_id is not None

    log_shared_query_end(
      exec_id,
      "sec",
      "user_1",
      outcome="completed",
      duration_ms=123.5,
      row_count=7,
      api_key_prefix="rfs12345",
      source="query_cypher",
    )

    assert mock_logger.info.call_count == 2
    start_msg = mock_logger.info.call_args_list[0].args[0]
    end_msg = mock_logger.info.call_args_list[1].args[0]
    assert start_msg.startswith("SHARED_QUERY_COST: ")
    start = json.loads(start_msg.split("SHARED_QUERY_COST: ", 1)[1])
    end = json.loads(end_msg.split("SHARED_QUERY_COST: ", 1)[1])
    assert start["phase"] == "start"
    assert end["phase"] == "end"
    assert start["exec_id"] == end["exec_id"] == exec_id
    assert start["user_id"] == end["user_id"] == "user_1"
    assert start["query_length"] == 42
    assert end["outcome"] == "completed"
    assert end["duration_ms"] == 123.5
    assert end["row_count"] == 7

  @patch(_TELEMETRY_LOGGER)
  @patch(_SHARED_CHECK, return_value=False)
  def test_user_graph_emits_nothing(self, mock_shared, mock_logger):
    exec_id = log_shared_query_start("kg123abc", "user_1")
    assert exec_id is None
    log_shared_query_end(exec_id, "kg123abc", "user_1", outcome="completed")
    mock_logger.info.assert_not_called()

  @patch(_TELEMETRY_LOGGER)
  @patch(_SHARED_CHECK, return_value=True)
  def test_end_with_none_exec_id_is_noop(self, mock_shared, mock_logger):
    log_shared_query_end(None, "sec", "user_1", outcome="completed")
    mock_logger.info.assert_not_called()


class TestApiKeyPrefixFromRequest:
  def test_reads_state_attribute(self):
    request = Mock()
    request.state.api_key_prefix = "rfs12345"
    assert api_key_prefix_from_request(request) == "rfs12345"

  def test_missing_attribute_returns_none(self):
    class _State:
      pass

    request = Mock()
    request.state = _State()
    assert api_key_prefix_from_request(request) is None

  def test_none_request_returns_none(self):
    assert api_key_prefix_from_request(None) is None
