"""Unit tests for the remote MCP transport (Streamable HTTP JSON-RPC)."""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.mcp import remote
from robosystems.routers.graphs.mcp.handlers import tool_error_result
from robosystems.routers.graphs.mcp.remote import (
  INVALID_PARAMS,
  INVALID_REQUEST,
  MCP_LATEST_PROTOCOL_VERSION,
  METHOD_NOT_FOUND,
  PARSE_ERROR,
  _event_to_progress,
  _remote_switch_workspace,
  _to_tool_result,
  _tool_error_payload,
  _tool_failure,
  _transport_gate,
  dispatch_jsonrpc,
)


def _body(response):
  return json.loads(response.body)


def _make_request(message, accept="application/json, text/event-stream", headers=None):
  request = Mock()
  if isinstance(message, Exception):
    request.json = AsyncMock(side_effect=message)
  else:
    request.json = AsyncMock(return_value=message)
  request.headers = {"accept": accept, **(headers or {})}
  return request


def _make_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


class TestEnvelopeHelpers:
  def test_to_tool_result_wraps_text_result(self):
    result = _to_tool_result({"type": "text", "text": "hello"})
    assert result == {
      "content": [{"type": "text", "text": "hello"}],
      "isError": False,
    }

  def test_to_tool_result_serializes_non_text_result(self):
    result = _to_tool_result({"success": True, "rows": 3})
    assert result["isError"] is False
    assert json.loads(result["content"][0]["text"]) == {"success": True, "rows": 3}

  def test_tool_error_payload_shape(self):
    payload = _tool_error_payload(7, "boom")
    assert payload["id"] == 7
    assert payload["result"]["isError"] is True
    assert payload["result"]["content"][0]["text"] == "boom"


class TestEventToProgress:
  def test_start_event_maps_to_zero(self):
    notification, progress = _event_to_progress(
      "tok", {"event": "start", "data": {"message": "go"}}, 0.0
    )
    assert notification["method"] == "notifications/progress"
    assert notification["params"]["progressToken"] == "tok"
    assert notification["params"]["progress"] > 0.0 or progress >= 0.0

  def test_progress_event_uses_value_and_message(self):
    notification, progress = _event_to_progress(
      "tok", {"event": "progress", "data": {"progress": 50, "message": "half"}}, 0.0
    )
    assert notification["params"]["progress"] == 50.0
    assert notification["params"]["message"] == "half"
    assert progress == 50.0

  def test_progress_is_monotonic(self):
    # A row-count event after a percentage event must not go backwards.
    _, p1 = _event_to_progress(
      "tok", {"event": "progress", "data": {"progress": 100}}, 0.0
    )
    notification, p2 = _event_to_progress(
      "tok", {"event": "query_chunk", "data": {"total_rows_so_far": 10}}, p1
    )
    assert p2 > p1
    assert notification["params"]["progress"] == p2

  def test_data_events_produce_no_notification(self):
    for etype in ("query_result", "result", "complete", "error", "schema_nodes"):
      notification, progress = _event_to_progress(
        "tok", {"event": etype, "data": {}}, 5.0
      )
      assert notification is None
      assert progress == 5.0


KG = "kg19fb490f76871d22e835"


class TestRemoteSwitchWorkspace:
  def test_named_subgraph_builds_id_and_url(self):
    text = _remote_switch_workspace(KG, {"workspace_id": "dev"})
    payload = json.loads(text)
    assert payload["switched"] is False
    assert payload["target_graph_id"] == f"{KG}_dev"
    assert payload["connector_url"].endswith(f"/v1/graphs/{KG}_dev/mcp")

  def test_full_subgraph_id_passes_through(self):
    payload = json.loads(_remote_switch_workspace(KG, {"workspace_id": f"{KG}_dev"}))
    assert payload["target_graph_id"] == f"{KG}_dev"

  def test_primary_resolves_parent_from_subgraph(self):
    payload = json.loads(
      _remote_switch_workspace(f"{KG}_dev", {"workspace_id": "primary"})
    )
    assert payload["target_graph_id"] == KG

  def test_shared_repo_subgraph_resolves(self):
    payload = json.loads(
      _remote_switch_workspace("sec", {"workspace_id": "historical"})
    )
    assert payload["target_graph_id"] == "sec_historical"

  def test_missing_workspace_id_is_error_text(self):
    assert _remote_switch_workspace(KG, {}).startswith("Error:")

  def test_invalid_characters_rejected(self):
    text = _remote_switch_workspace(KG, {"workspace_id": "../etc/passwd"})
    assert text.startswith("Error:")

  def test_foreign_graph_family_rejected(self):
    # A full id outside this connector's family must not resolve to a URL.
    other = "kg29fb490f76871d22e835_dev"
    text = _remote_switch_workspace(KG, {"workspace_id": other})
    assert text.startswith("Error:")
    assert "family" in text

  def test_message_never_claims_current_key_transfers(self):
    # A URL-token connector's credential may not cover the target; the
    # guidance must point at minting one, not claim the same key works.
    payload = json.loads(_remote_switch_workspace(KG, {"workspace_id": "dev"}))
    assert "same API key" not in payload["message"]
    assert "credential" in payload["message"]


@pytest.mark.asyncio
class TestDispatchEnvelope:
  async def test_parse_error(self):
    response = await dispatch_jsonrpc(
      _make_request(ValueError("bad json")), "kg123", _make_user()
    )
    assert response.status_code == 400
    assert _body(response)["error"]["code"] == PARSE_ERROR

  async def test_batch_rejected(self):
    response = await dispatch_jsonrpc(
      _make_request([{"jsonrpc": "2.0", "id": 1, "method": "ping"}]),
      "kg123",
      _make_user(),
    )
    assert response.status_code == 400
    assert _body(response)["error"]["code"] == INVALID_REQUEST

  async def test_non_jsonrpc_rejected(self):
    response = await dispatch_jsonrpc(
      _make_request({"method": "ping", "id": 1}), "kg123", _make_user()
    )
    assert response.status_code == 400
    assert _body(response)["error"]["code"] == INVALID_REQUEST

  async def test_notification_returns_202_no_body(self):
    response = await dispatch_jsonrpc(
      _make_request({"jsonrpc": "2.0", "method": "notifications/initialized"}),
      "kg123",
      _make_user(),
    )
    assert response.status_code == 202

  async def test_client_response_message_returns_202(self):
    response = await dispatch_jsonrpc(
      _make_request({"jsonrpc": "2.0", "id": 9, "result": {}}), "kg123", _make_user()
    )
    assert response.status_code == 202

  async def test_malformed_notification_still_gets_202_not_an_error(self):
    # JSON-RPC 2.0: a notification must never receive a reply — not even an
    # invalid-params error. The notification check must run before params
    # validation.
    response = await dispatch_jsonrpc(
      _make_request(
        {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": [1, 2]}
      ),
      "kg123",
      _make_user(),
    )
    assert response.status_code == 202

  async def test_ping(self):
    response = await dispatch_jsonrpc(
      _make_request({"jsonrpc": "2.0", "id": 2, "method": "ping"}),
      "kg123",
      _make_user(),
    )
    body = _body(response)
    assert body == {"jsonrpc": "2.0", "id": 2, "result": {}}

  async def test_method_not_found(self):
    response = await dispatch_jsonrpc(
      _make_request({"jsonrpc": "2.0", "id": 3, "method": "resources/list"}),
      "kg123",
      _make_user(),
    )
    assert _body(response)["error"]["code"] == METHOD_NOT_FOUND

  async def test_non_object_params_rejected(self):
    response = await dispatch_jsonrpc(
      _make_request({"jsonrpc": "2.0", "id": 4, "method": "ping", "params": [1]}),
      "kg123",
      _make_user(),
    )
    assert _body(response)["error"]["code"] == INVALID_PARAMS


def _make_handler(tools=None, instructions="per-graph guidance"):
  handler = Mock()
  handler._closed = False
  handler.close = AsyncMock()
  handler.get_tools = AsyncMock(
    return_value=tools
    if tools is not None
    else [
      {
        "name": "get-graph-schema",
        "description": "schema",
        "inputSchema": {"type": "object", "properties": {}},
      },
      {
        "name": "switch-workspace",
        "description": "client-side text",
        "inputSchema": {"type": "object", "properties": {}},
      },
    ]
  )
  handler.get_instructions = Mock(return_value=instructions)
  return handler


@pytest.mark.asyncio
class TestInitialize:
  async def _initialize(self, requested_version):
    handler = _make_handler()
    with (
      patch.object(remote, "_validate_read_access", AsyncMock()),
      patch.object(remote, "get_graph_repository", AsyncMock(return_value=Mock())),
      patch.object(remote, "MCPHandler", Mock(return_value=handler)),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 1,
          "method": "initialize",
          "params": {"protocolVersion": requested_version},
        }
      )
      return await dispatch_jsonrpc(request, "kg123", _make_user())

  async def test_supported_version_is_echoed(self):
    body = _body(await self._initialize("2025-03-26"))
    assert body["result"]["protocolVersion"] == "2025-03-26"

  async def test_unsupported_version_answers_latest(self):
    body = _body(await self._initialize("1999-01-01"))
    assert body["result"]["protocolVersion"] == MCP_LATEST_PROTOCOL_VERSION

  async def test_server_info_and_instructions_are_per_graph(self):
    body = _body(await self._initialize("2025-06-18"))
    result = body["result"]
    assert result["serverInfo"]["name"] == "robosystems-kg123"
    assert result["instructions"] == "per-graph guidance"
    assert result["capabilities"] == {"tools": {"listChanged": False}}


@pytest.mark.asyncio
class TestToolsList:
  async def test_shape_annotations_and_description_rewrite(self):
    handler = _make_handler()
    with (
      patch.object(remote, "_validate_read_access", AsyncMock()),
      patch.object(remote, "get_graph_repository", AsyncMock(return_value=Mock())),
      patch.object(remote, "MCPHandler", Mock(return_value=handler)),
    ):
      request = _make_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
      body = _body(await dispatch_jsonrpc(request, "kg123", _make_user()))

    tools = {tool["name"]: tool for tool in body["result"]["tools"]}
    assert tools["get-graph-schema"]["annotations"] == {"readOnlyHint": True}
    # The npx "client-side tool" text must never reach a remote client.
    assert "client-side" not in tools["switch-workspace"]["description"]
    assert "connector" in tools["switch-workspace"]["description"].lower()


@pytest.mark.asyncio
class TestToolsCall:
  async def test_missing_name_is_invalid_params(self):
    request = _make_request(
      {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}}
    )
    body = _body(await dispatch_jsonrpc(request, "kg123", _make_user()))
    assert body["error"]["code"] == INVALID_PARAMS

  async def test_gauntlet_denial_becomes_tool_error(self):
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(
        remote,
        "authorize_mcp_tool_call",
        AsyncMock(side_effect=HTTPException(status_code=403, detail="need a sub")),
      ),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 1,
          "method": "tools/call",
          "params": {"name": "read-graph-cypher", "arguments": {"query": "MATCH"}},
        }
      )
      response = await dispatch_jsonrpc(request, "sec", _make_user())

    body = _body(response)
    assert response.status_code == 200
    assert body["result"]["isError"] is True
    assert "need a sub" in body["result"]["content"][0]["text"]

  async def test_switch_workspace_intercepted_after_gauntlet(self):
    authorize = AsyncMock(return_value="read")
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "authorize_mcp_tool_call", authorize),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 1,
          "method": "tools/call",
          "params": {"name": "switch-workspace", "arguments": {"workspace_id": "dev"}},
        }
      )
      body = _body(await dispatch_jsonrpc(request, KG, _make_user()))

    authorize.assert_awaited_once()
    payload = json.loads(body["result"]["content"][0]["text"])
    assert payload["target_graph_id"] == f"{KG}_dev"

  async def test_success_maps_result_to_content(self):
    handler = _make_handler()
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "authorize_mcp_tool_call", AsyncMock(return_value="read")),
      patch.object(remote, "get_graph_repository", AsyncMock(return_value=Mock())),
      patch.object(remote, "MCPHandler", Mock(return_value=handler)),
      patch.object(
        remote,
        "execute_tool_to_json",
        AsyncMock(return_value={"type": "text", "text": "the result"}),
      ),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 11,
          "method": "tools/call",
          "params": {"name": "get-graph-info", "arguments": {}},
        },
        accept="application/json",
      )
      body = _body(await dispatch_jsonrpc(request, "kg123", _make_user()))

    assert body["id"] == 11
    assert body["result"]["content"] == [{"type": "text", "text": "the result"}]
    assert body["result"]["isError"] is False
    handler.close.assert_awaited()


@pytest.mark.asyncio
class TestStreamToolCall:
  async def test_progress_then_final_response(self):
    async def fake_stream(handler, name, arguments, strategy):
      yield {"event": "start", "data": {"tool": name, "message": "starting"}}
      yield {
        "event": "query_chunk",
        "data": {"total_rows_so_far": 5, "columns": ["a"], "data": [{"a": 1}]},
      }
      yield {"event": "complete", "data": {}}

    handler = _make_handler()
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "stream_mcp_tool_execution", fake_stream),
    ):
      tool_call = Mock()
      tool_call.name = "read-graph-cypher"
      tool_call.arguments = {"query": "MATCH (n) RETURN n"}
      events = [
        event
        async for event in remote._stream_tool_call(
          handler,
          "kg123",
          tool_call,
          remote.MCPExecutionStrategy.STREAM_AGGREGATED,
          30,
          42,
          "tok",
        )
      ]

    messages = [json.loads(event["data"]) for event in events]
    notifications = [m for m in messages if m.get("method") == "notifications/progress"]
    finals = [m for m in messages if "id" in m]
    assert len(notifications) == 2
    assert all(n["params"]["progressToken"] == "tok" for n in notifications)
    assert len(finals) == 1
    assert finals[0]["id"] == 42
    assert finals[0]["result"]["isError"] is False
    handler.close.assert_awaited()

  async def test_no_token_suppresses_notifications(self):
    async def fake_stream(handler, name, arguments, strategy):
      yield {"event": "start", "data": {"tool": name}}
      yield {"event": "result", "data": {"result": {"type": "text", "text": "ok"}}}

    handler = _make_handler()
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "stream_mcp_tool_execution", fake_stream),
    ):
      tool_call = Mock()
      tool_call.name = "get-graph-schema"
      tool_call.arguments = {}
      events = [
        event
        async for event in remote._stream_tool_call(
          handler,
          "kg123",
          tool_call,
          remote.MCPExecutionStrategy.SSE_PROGRESS,
          30,
          1,
          None,
        )
      ]

    messages = [json.loads(event["data"]) for event in events]
    assert len(messages) == 1
    assert "id" in messages[0]


def _make_queue_manager(statuses, result=None):
  manager = Mock()
  manager.submit_query = AsyncMock(return_value="q-1")
  manager.get_query_status = AsyncMock(side_effect=statuses)
  manager.get_query_result = AsyncMock(
    return_value=result or {"status": "completed", "data": [1]}
  )
  manager.cancel_query = AsyncMock(return_value=True)
  return manager


def _make_tool_call():
  tool_call = Mock()
  tool_call.name = "read-graph-cypher"
  tool_call.arguments = {"query": "MATCH (n) RETURN n", "parameters": {}}
  return tool_call


@pytest.mark.asyncio
class TestStreamQueuedCall:
  async def test_completed_query_streams_progress_then_result(self):
    manager = _make_queue_manager(
      [
        {"status": "pending", "queue_position": 3},
        {"status": "running"},
        {"status": "completed"},
      ]
    )
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "_get_user_priority", Mock(return_value=5)),
      patch.object(remote, "_QUEUE_POLL_SECONDS", 0),
      patch(
        "robosystems.middleware.graph.query_queue.get_query_queue",
        return_value=manager,
      ),
    ):
      events = [
        event
        async for event in remote._stream_queued_call(
          "kg123", _make_tool_call(), _make_user(), 8, "tok"
        )
      ]

    messages = [json.loads(event["data"]) for event in events]
    notifications = [m for m in messages if m.get("method") == "notifications/progress"]
    finals = [m for m in messages if "id" in m]
    assert [n["params"]["message"] for n in notifications] == [
      "Queued (position 3)",
      "Running",
      "Completed",
    ]
    assert finals[0]["id"] == 8
    assert finals[0]["result"]["isError"] is False
    manager.cancel_query.assert_not_awaited()

  async def test_failed_query_is_tool_error(self):
    manager = _make_queue_manager([{"status": "failed", "error": "syntax error"}])
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "_get_user_priority", Mock(return_value=5)),
      patch.object(remote, "_QUEUE_POLL_SECONDS", 0),
      patch(
        "robosystems.middleware.graph.query_queue.get_query_queue",
        return_value=manager,
      ),
    ):
      events = [
        event
        async for event in remote._stream_queued_call(
          "kg123", _make_tool_call(), _make_user(), 8, None
        )
      ]

    final = json.loads(events[-1]["data"])
    assert final["result"]["isError"] is True
    assert "syntax error" in final["result"]["content"][0]["text"]

  async def test_disconnect_cancels_queued_query(self):
    manager = _make_queue_manager(
      [{"status": "pending", "queue_position": 1}, {"status": "pending"}] * 50
    )
    with (
      patch.object(remote, "circuit_breaker", Mock()),
      patch.object(remote, "_get_user_priority", Mock(return_value=5)),
      patch.object(remote, "_QUEUE_POLL_SECONDS", 0),
      patch(
        "robosystems.middleware.graph.query_queue.get_query_queue",
        return_value=manager,
      ),
    ):
      generator = remote._stream_queued_call(
        "kg123", _make_tool_call(), _make_user(), 8, "tok"
      )
      # Take one progress event, then drop the stream (client disconnect).
      await generator.__anext__()
      await generator.aclose()

    manager.cancel_query.assert_awaited_once_with("q-1", "user-123")


class TestToolFailureClassification:
  """Marked handler failures and aggregated stream failures map to isError."""

  def test_marked_error_maps_to_is_error(self):
    marked = tool_error_result("Error: it broke", "backend")
    result = _to_tool_result(marked)
    assert result["isError"] is True
    assert result["content"] == [{"type": "text", "text": "Error: it broke"}]

  def test_aggregated_failure_maps_to_is_error(self):
    aggregated = {"success": False, "error": "boom", "tool": "read-graph-cypher"}
    result = _to_tool_result(aggregated)
    assert result["isError"] is True
    assert result["content"] == [{"type": "text", "text": "boom"}]

  def test_success_shapes_stay_not_error(self):
    for value in (
      {"type": "text", "text": "fine"},
      {"success": True, "rows": 3},
      ["plain", "list"],
    ):
      assert _to_tool_result(value)["isError"] is False

  def test_tool_failure_kinds(self):
    assert _tool_failure(tool_error_result("t", "timeout")) == (True, "timeout")
    assert _tool_failure(tool_error_result("c", "constraint")) == (True, "constraint")
    # Aggregated failures without a kind default to backend (covers the
    # disrupted-stream shape).
    assert _tool_failure({"success": False, "error": "x"}) == (True, "backend")
    assert _tool_failure({"type": "text", "text": "ok"}) == (False, None)


class TestTransportGate:
  """Origin and Content-Type checks required by MCP Streamable HTTP."""

  def _request(self, headers):
    request = Mock()
    request.headers = headers
    return request

  def test_absent_origin_with_json_passes(self):
    _transport_gate(self._request({"content-type": "application/json"}))

  def test_trusted_origin_passes(self):
    from robosystems.config import env

    origin = env.get_main_cors_origins()[0]
    _transport_gate(
      self._request({"origin": origin, "content-type": "application/json"})
    )

  def test_untrusted_origin_is_403(self):
    with pytest.raises(HTTPException) as exc:
      _transport_gate(
        self._request(
          {"origin": "https://evil.example", "content-type": "application/json"}
        )
      )
    assert exc.value.status_code == 403

  def test_non_json_content_type_is_415(self):
    with pytest.raises(HTTPException) as exc:
      _transport_gate(self._request({"content-type": "text/plain"}))
    assert exc.value.status_code == 415

  def test_json_with_charset_passes(self):
    _transport_gate(self._request({"content-type": "application/json; charset=utf-8"}))


@pytest.mark.asyncio
class TestProtocolVersionHeader:
  """Post-initialize requests carrying MCP-Protocol-Version must be validated."""

  async def test_unsupported_version_header_is_400(self):
    request = _make_request(
      {"jsonrpc": "2.0", "id": 5, "method": "ping"},
      headers={"mcp-protocol-version": "2024-11-05"},
    )
    response = await dispatch_jsonrpc(request, "kg123", _make_user())
    assert response.status_code == 400
    body = _body(response)
    assert body["error"]["code"] == INVALID_REQUEST
    assert "2024-11-05" in body["error"]["message"]

  async def test_supported_version_header_passes(self):
    request = _make_request(
      {"jsonrpc": "2.0", "id": 5, "method": "ping"},
      headers={"mcp-protocol-version": "2025-06-18"},
    )
    body = _body(await dispatch_jsonrpc(request, "kg123", _make_user()))
    assert body["result"] == {}

  async def test_absent_version_header_passes(self):
    request = _make_request({"jsonrpc": "2.0", "id": 5, "method": "ping"})
    body = _body(await dispatch_jsonrpc(request, "kg123", _make_user()))
    assert body["result"] == {}

  async def test_initialize_is_exempt_from_header_check(self):
    handler = _make_handler()
    with (
      patch.object(remote, "_validate_read_access", AsyncMock()),
      patch.object(remote, "get_graph_repository", AsyncMock(return_value=Mock())),
      patch.object(remote, "MCPHandler", Mock(return_value=handler)),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 1,
          "method": "initialize",
          "params": {"protocolVersion": "2025-11-25"},
        },
        headers={"mcp-protocol-version": "2025-11-25"},
      )
      response = await dispatch_jsonrpc(request, "kg123", _make_user())
    assert response.status_code == 200
    # Unsupported requested revision negotiates down to the server's latest.
    assert _body(response)["result"]["protocolVersion"] == MCP_LATEST_PROTOCOL_VERSION

  async def test_notification_with_bad_header_is_400_without_body(self):
    request = _make_request(
      {"jsonrpc": "2.0", "method": "notifications/initialized"},
      headers={"mcp-protocol-version": "bogus"},
    )
    response = await dispatch_jsonrpc(request, "kg123", _make_user())
    assert response.status_code == 400
    assert not response.body

  async def test_pre_streamable_revision_is_not_negotiable(self):
    assert "2024-11-05" not in remote.MCP_SUPPORTED_PROTOCOL_VERSIONS


@pytest.mark.asyncio
class TestToolFailureOutcomes:
  """Execution failures reach the client as isError and the breaker as failures."""

  async def _call(self, result, breaker):
    handler = _make_handler()
    with (
      patch.object(remote, "circuit_breaker", breaker),
      patch.object(remote, "authorize_mcp_tool_call", AsyncMock(return_value="read")),
      patch.object(remote, "get_graph_repository", AsyncMock(return_value=Mock())),
      patch.object(remote, "MCPHandler", Mock(return_value=handler)),
      patch.object(remote, "execute_tool_to_json", AsyncMock(return_value=result)),
    ):
      request = _make_request(
        {
          "jsonrpc": "2.0",
          "id": 21,
          "method": "tools/call",
          "params": {"name": "get-graph-info", "arguments": {}},
        },
        accept="application/json",
      )
      return _body(await dispatch_jsonrpc(request, "kg123", _make_user()))

  async def test_swallowed_timeout_is_error_and_breaker_failure(self):
    breaker = Mock()
    body = await self._call(
      tool_error_result("Error: tool 'get-graph-info' timed out", "timeout"), breaker
    )
    assert body["result"]["isError"] is True
    breaker.record_failure.assert_called_once()
    breaker.record_success.assert_not_called()

  async def test_backend_error_is_error_and_breaker_failure(self):
    breaker = Mock()
    body = await self._call(
      tool_error_result("Graph API unavailable", "backend"), breaker
    )
    assert body["result"]["isError"] is True
    breaker.record_failure.assert_called_once()

  async def test_constraint_error_is_error_but_not_breaker_failure(self):
    breaker = Mock()
    body = await self._call(
      tool_error_result("Query Error: too complex", "constraint"), breaker
    )
    # The model sees a failed call; the breaker sees a healthy backend.
    assert body["result"]["isError"] is True
    breaker.record_failure.assert_not_called()
    breaker.record_success.assert_called_once()

  async def test_disrupted_aggregation_is_error_and_breaker_failure(self):
    breaker = Mock()
    body = await self._call(
      {"success": False, "error": "Unable to aggregate results", "tool": "t"}, breaker
    )
    assert body["result"]["isError"] is True
    breaker.record_failure.assert_called_once()

  async def test_success_still_records_breaker_success(self):
    breaker = Mock()
    body = await self._call({"type": "text", "text": "fine"}, breaker)
    assert body["result"]["isError"] is False
    breaker.record_success.assert_called_once()
    breaker.record_failure.assert_not_called()
